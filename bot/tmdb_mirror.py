"""
The TMDb image mirror (B5.8): every image the app still loads from
image.tmdb.org, copied once to our own Storage with a one-year cache.

Why (decided 27/10): the app should read everything from our side and be
optimised from there. Three kinds of image still came from TMDb's CDN, each at
the one size the app asks for:
  - backdrops — media.backdrop_path, drawn at w1280;
  - title logos — media.title_logo_url, stored as TMDb's w500 URL;
  - cast and crew photos — media_credits.profile_path, drawn at w185.
Each file lands at media-images/tmdb/{size}/{file}, under TMDb's own file
name, so the app builds our address from what it already has. A TMDb file
name never changes its bytes (a new image is a new name), so a year of cache
is safe. The bytes are uploaded as TMDb served them — never re-encoded, never
resized — with the content type the CDN answered.

The ledger: public.tmdb_image_mirror (migration tmdb_image_mirror, Claude's)
holds one row per (size, file) copied ('ok', with its bytes) or answered 404
('missing', so it is not asked every hour). Two RPCs read for the script:
  - tmdb_mirror_titles(p_after, p_limit) — media rows after p_after in id
    order, tmdb_missing_at null, each with the distinct profile_path of its
    credits;
  - tmdb_mirror_missing(p_size, p_files) — which of up to 1000 files the
    ledger does not hold yet.

THE SWEEP is stateless: every run starts from the first page and walks the
catalogue in pages of PAGE_TITLES by the last id, asking the ledger which of
each page's files are new. A page already copied costs a few cheap calls; there
is no cursor to keep. Per page: the wanted files, deduplicated (a logo whose
URL is not TMDb's w500 is counted `skipped_size` and never fetched); the
missing question per size in chunks of MISSING_CHUNK; each missing file
fetched and uploaded by a pool of workers (one Supabase client per worker
thread: the shared client builds its storage client lazily, without a lock);
then the page's ledger rows, upserted on (size, file) in chunks of
LEDGER_CHUNK. The RPCs and the ledger stay on the main thread. A run that dies
after a page's uploads and before its ledger write uploads those files again
next time — an upsert of the same bytes: harmless.

Outcomes per file (posters_hd.fetch_image, B5.3's split): OK → uploaded, then
ledgered 'ok'; MISSING (404) → ledgered 'missing', no upload; ERROR (a
timeout, a 5xx, anything else), a content type this mirror does not keep, or
a failed upload → nothing recorded, asked again next run.

A time budget (TMDB_MIRROR_TIME_BUDGET_MIN, default 25, at most 80) and a
file cap (TMDB_MIRROR_MAX_FILES, default 20,000) end a run with its SUMMARY;
the next run continues where the ledger says.

FAILS FAST if the server has no RPC or ledger yet (PGRST202, 42883, 42P01,
PGRST205): one line naming the migration, exit 1. A missing RPC is met
before any upload; a ledger table missing only at the write (the RPCs there)
stops after that page's uploads, which the next run uploads again. Any other
failure of an RPC or of the ledger write (after the house's retry of the
transient ones) stops the run with what landed: its line (the error's kind and
code), the SUMMARY, exit 1. So do ERRORS_IN_A_ROW_MAX failed files in a row:
the CDN or Storage is down.

It never writes media or media_credits (it reads them through the RPC), and
never deletes a file or a ledger row.

Logs: an error line of this module names the size and the error's kind or
HTTP status — never a file, a title, a person or a URL — at most
ERROR_LINES_MAX of them a run; there is no line per success. A failed upload
also prints the house upload_image's own line (the storage path, which names
the TMDb file, and the storage error), which this module does not cap; the
breaker above bounds them.

Run via:
    python -m bot.tmdb_mirror
"""

import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import NamedTuple

import requests

from .migrate_images import poster_basename
from .posters_hd import FETCH_MISSING, FETCH_OK, ONE_YEAR_CACHE_SECONDS, fetch_image
# The house's one definition of a transient RPC failure (B3.1, B5.1).
from .supabase_client import SupabaseClient, _TRANSIENT_RPC_CODES, _TRANSIENT_RPC_ERRORS

BUCKET = "media-images"
PREFIX = "tmdb"
CDN_BASE = "https://image.tmdb.org/t/p/"
BACKDROP_SIZE, LOGO_SIZE, PROFILE_SIZE = "w1280", "w500", "w185"
SIZES = (BACKDROP_SIZE, LOGO_SIZE, PROFILE_SIZE)
CONTENT_TYPES = frozenset({"image/jpeg", "image/png", "image/svg+xml", "image/webp"})
PAGE_TITLES = 200              # tmdb_mirror_titles serves at most 500 a call
MISSING_CHUNK = 1000           # tmdb_mirror_missing reads at most 1000 files, and max-rows is 1000
LEDGER_CHUNK = 500
# Files handed to the pool between two budget checks, per worker. The last
# chunk in flight when the budget runs out is the overshoot: at worst every
# file a fetch timeout (posters_hd.FETCH_TIMEOUT_SECONDS, 30) and an upload
# timeout (storage3's default, UPLOAD_TIMEOUT_SECONDS) — 4 × 50 s ≈ 3.3 min,
# so 80 + 3.3 + ≈ 3 of setup stays inside the job's 90.
CHUNK_PER_WORKER = 4
UPLOAD_TIMEOUT_SECONDS = 20
PROGRESS_EVERY_PAGES = 25
ERROR_LINES_MAX = 20
# This many failed files in a row (every outcome but a copy or a 404) means
# the CDN or Storage is down: the run stops with what landed, red, instead of
# spending its budget — and a line from the house upload_image per failed
# upload, which no cap of this module reaches — on files that cannot land.
ERRORS_IN_A_ROW_MAX = 50
DEFAULT_TMDB_MIRROR_WORKERS = 6
MAX_TMDB_MIRROR_WORKERS = 16
DEFAULT_TMDB_MIRROR_TIME_BUDGET_MIN = 25
MAX_TMDB_MIRROR_TIME_BUDGET_MIN = 80   # inside the job's 90 minutes
DEFAULT_TMDB_MIRROR_MAX_FILES = 20000
MIRROR_MIGRATION = "tmdb_image_mirror"
LEDGER_TABLE = "tmdb_image_mirror"
_RPC_ATTEMPTS, _RPC_RETRY_DELAYS = 3, (2, 4)   # the house's transient retry (supabase_client)
_MISSING_CODES = {"PGRST202", "42883", "42P01", "PGRST205"}

COPIED, MISSING, ERROR = "copied", "missing", "error"


class FileCopy(NamedTuple):
    """What became of one file: COPIED (with its bytes), MISSING, or ERROR
    (with a short reason: the kind or the status, never the URL)."""
    outcome: str
    size: str
    file: str
    nbytes: int = 0
    reason: str | None = None


class _Stop(Exception):
    """A failure that ends the run: `missing` when the migration is not applied."""

    def __init__(self, what: str, exc: BaseException, missing: bool):
        super().__init__(what)
        self.what, self.exc, self.missing = what, exc, missing


def load_env() -> dict[str, str]:
    required = ["SUPABASE_URL", "SUPABASE_KEY"]
    config: dict[str, str] = {}
    for key in required:
        value = os.environ.get(key)
        if not value:
            print(f"[TMDB_MIRROR] ERROR: Required environment variable '{key}' is not set.")
            sys.exit(1)
        config[key] = value
    return config


def file_of(value: str | None) -> str:
    """The TMDb file a bare path (/abc.jpg) or a URL names; '' for nothing."""
    return poster_basename((value or "").split("?", 1)[0])


def logo_file(url: str | None) -> tuple[str | None, str]:
    """(size, file) of a TMDb image URL (…/t/p/{size}/{file}); (None, file)
    when it carries no size segment."""
    path = (url or "").split("?", 1)[0]
    parts = path.rsplit("/", 2)
    if "/t/p/" in path and len(parts) == 3:
        return parts[1], parts[2]
    return None, file_of(path)


def storable(file: str) -> bool:
    """What the ledger accepts as a file: 5–120 characters, no '/' or '?'."""
    return 5 <= len(file) <= 120 and "/" not in file and "?" not in file


def wanted_files(rows: list[dict]) -> tuple[dict[str, list[str]], int, list[str]]:
    """A page's files per size, deduplicated in their first order; how many
    logos are at another size than w500; the size of each name the ledger
    cannot hold (never fetched)."""
    wanted = {size: {} for size in SIZES}
    skipped, unstorable = 0, []

    def add(size, file):
        if not file:
            return
        if storable(file):
            wanted[size].setdefault(file, None)
        else:
            unstorable.append(size)

    for row in rows:
        add(BACKDROP_SIZE, file_of(row.get("backdrop_path")))
        if row.get("title_logo_url"):
            size, file = logo_file(row["title_logo_url"])
            if size == LOGO_SIZE:
                add(LOGO_SIZE, file)
            else:
                skipped += 1
        for path in row.get("profile_paths") or ():
            add(PROFILE_SIZE, file_of(path))
    return {size: list(files) for size, files in wanted.items()}, skipped, unstorable


def _texts(data) -> list[str]:
    """A setof-text RPC's answer, as PostgREST hands it back: strings, or
    one-column rows."""
    return [item if isinstance(item, str) else next(iter(item.values())) for item in data or []]


def _call(what: str, fn, sleep):
    """One server call, with the house's retry of transient failures (three
    attempts, 2 s then 4 s apart). A missing RPC or table is a _Stop marked
    missing; anything else that persists is a _Stop."""
    for attempt in range(1, _RPC_ATTEMPTS + 1):
        try:
            return fn()
        except Exception as exc:
            code = str(getattr(exc, "code", None))
            if code in _MISSING_CODES:
                raise _Stop(what, exc, missing=True) from None
            transient = isinstance(exc, _TRANSIENT_RPC_ERRORS) or code in _TRANSIENT_RPC_CODES
            if not transient or attempt >= _RPC_ATTEMPTS:
                raise _Stop(what, exc, missing=False) from None
            delay = _RPC_RETRY_DELAYS[attempt - 1]
            print(f"[TMDB_MIRROR] {what} failed ({type(exc).__name__}, attempt {attempt} of {_RPC_ATTEMPTS}) — "
                  f"retrying in {delay} s", flush=True)
            sleep(delay)


def ask_missing(db, size: str, files: list[str], sleep) -> list[str]:
    """The files of *size* the ledger does not hold, asked in chunks of MISSING_CHUNK."""
    missing: list[str] = []
    for i in range(0, len(files), MISSING_CHUNK):
        chunk = files[i:i + MISSING_CHUNK]
        missing.extend(_texts(_call("tmdb_mirror_missing", lambda: db.client.rpc(
            "tmdb_mirror_missing", {"p_size": size, "p_files": chunk}).execute().data, sleep)))
    return missing


def _content_type(raw: str | None) -> str:
    return (raw or "").split(";", 1)[0].strip().lower()


def copy_file(uploader, size: str, file: str, fetch) -> FileCopy:
    """One file: fetched once from TMDb's CDN and, when it is an image this
    mirror keeps, uploaded as served with a one-year cache."""
    got = fetch_image(f"{CDN_BASE}{size}/{file}", fetch)
    if got.outcome == FETCH_MISSING:
        return FileCopy(MISSING, size, file)
    if got.outcome != FETCH_OK:
        return FileCopy(ERROR, size, file, reason=f"fetch failed ({got.reason})")
    ctype = _content_type(got.content_type)
    if ctype not in CONTENT_TYPES:
        return FileCopy(ERROR, size, file, reason=f"content type {ctype or 'none'} is not an image this mirror keeps")
    url = uploader().upload_image(BUCKET, f"{PREFIX}/{size}/{file}", got.content, ctype,
                                  cache_seconds=ONE_YEAR_CACHE_SECONDS)
    if not url:
        return FileCopy(ERROR, size, file, reason="upload failed")
    return FileCopy(COPIED, size, file, nbytes=len(got.content))


def write_ledger(db, results: list[FileCopy], sleep) -> int:
    """The ledger rows of a page's copies and 404s, upserted on (size, file)
    in chunks of LEDGER_CHUNK. Errors are not recorded."""
    rows = [{"size": r.size, "file": r.file, "status": "ok" if r.outcome == COPIED else "missing",
             "bytes": r.nbytes if r.outcome == COPIED else None}
            for r in results if r.outcome in (COPIED, MISSING)]
    for i in range(0, len(rows), LEDGER_CHUNK):
        chunk = rows[i:i + LEDGER_CHUNK]
        _call(LEDGER_TABLE, lambda: db.client.table(LEDGER_TABLE).upsert(chunk, on_conflict="size,file").execute(),
              sleep)
    return len(rows)


def run(db, *, make_uploader, fetch=requests.get, clock, workers: int = DEFAULT_TMDB_MIRROR_WORKERS,
        budget_min: int = DEFAULT_TMDB_MIRROR_TIME_BUDGET_MIN, max_files: int = DEFAULT_TMDB_MIRROR_MAX_FILES,
        sleep=time.sleep) -> dict:
    """One sweep: every page of tmdb_mirror_titles, until the last page, the
    time budget on *clock* (aware UTC) or *max_files* attempted. *make_uploader*
    builds the Supabase client a worker thread uploads with (one per thread)."""
    stats = {"pages": 0, "titles": 0, "wanted": 0, "already": 0, COPIED: 0, MISSING: 0, "errors": 0,
             "skipped_size": 0, BACKDROP_SIZE: 0, LOGO_SIZE: 0, PROFILE_SIZE: 0, "bytes_uploaded": 0,
             "budget_hit": 0, "sweep_done": 0, "failed": None}
    started = clock()
    deadline = started.timestamp() + budget_min * 60
    attempted, printed, in_a_row = 0, 0, 0
    local = threading.local()

    def uploader():
        client = getattr(local, "client", None)
        if client is None:
            client = local.client = make_uploader()
        return client

    def out_of_budget() -> bool:
        if clock().timestamp() < deadline and attempted < max_files:
            return False
        stats["budget_hit"] = 1
        return True

    def error(reason: str, size: str) -> None:
        nonlocal printed
        stats["errors"] += 1
        printed += 1
        if printed <= ERROR_LINES_MAX:
            print(f"[TMDB_MIRROR] ERROR size={size}: {reason}", flush=True)
        elif printed == ERROR_LINES_MAX + 1:
            print("[TMDB_MIRROR] … more errors not printed (the SUMMARY counts them)", flush=True)

    print(f"[TMDB_MIRROR] Mirroring TMDb images ({BACKDROP_SIZE} backdrops, {LOGO_SIZE} logos, {PROFILE_SIZE} photos) "
          f"with {workers} worker(s), for at most {budget_min} min or {max_files} file(s)...", flush=True)
    after = None
    try:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            while not out_of_budget():
                rows = _call("tmdb_mirror_titles", lambda: db.client.rpc(
                    "tmdb_mirror_titles", {"p_after": after, "p_limit": PAGE_TITLES}).execute().data or [], sleep)
                if not rows:
                    stats["sweep_done"] = 1
                    break
                stats["pages"] += 1
                stats["titles"] += len(rows)
                wanted, skipped, unstorable = wanted_files(rows)
                stats["skipped_size"] += skipped
                for size in unstorable:
                    error("a file name the ledger cannot hold", size)
                todo = []
                for size in SIZES:
                    files = wanted[size]
                    stats["wanted"] += len(files)
                    missing = ask_missing(db, size, files, sleep) if files else []
                    stats["already"] += len(files) - len(missing)
                    todo.extend((size, f) for f in missing)

                results: list[FileCopy] = []
                step = max(1, workers * CHUNK_PER_WORKER)
                broken = False
                for i in range(0, len(todo), step):
                    if out_of_budget():
                        break
                    chunk = todo[i:i + min(step, max_files - attempted)]
                    attempted += len(chunk)
                    done = list(pool.map(lambda sf: copy_file(uploader, sf[0], sf[1], fetch), chunk))
                    results.extend(done)
                    for r in done:
                        in_a_row = in_a_row + 1 if r.outcome == ERROR else 0
                    if in_a_row >= ERRORS_IN_A_ROW_MAX:
                        broken = True   # the CDN or Storage is down: every file costs a timeout, and a line
                        break
                cut = not broken and len(results) < len(todo)   # the budget or the file cap left part of this page
                if cut:
                    stats["budget_hit"] = 1
                for r in results:
                    if r.outcome == COPIED:
                        stats[COPIED] += 1
                        stats[r.size] += 1
                        stats["bytes_uploaded"] += r.nbytes
                    elif r.outcome == MISSING:
                        stats[MISSING] += 1
                    else:
                        error(r.reason, r.size)
                write_ledger(db, results, sleep)
                if stats["pages"] % PROGRESS_EVERY_PAGES == 0:
                    print(f"[TMDB_MIRROR] progress: {stats['pages']} page(s), {stats['titles']} title(s) — "
                          f"copied {stats[COPIED]}, missing {stats[MISSING]}, errors {stats['errors']}", flush=True)
                if broken:
                    stats["failed"] = "files"
                    print(f"[TMDB_MIRROR] ERROR: {ERRORS_IN_A_ROW_MAX} files in a row failed — the CDN or Storage is "
                          "down; stopping with what landed (the next run asks again)", flush=True)
                    break
                if cut or stats["budget_hit"]:
                    break
                if len(rows) < PAGE_TITLES:
                    stats["sweep_done"] = 1
                    break
                after = rows[-1]["id"]
    except _Stop as stop:
        if stop.missing:
            print(f"[TMDB_MIRROR] ERROR: the server has no public.{stop.what} (or what it reads) — apply migration "
                  f"{MIRROR_MIGRATION} before this code runs.", flush=True)
            sys.exit(1)
        stats["failed"] = stop.what
        code = getattr(stop.exc, "code", None)
        kind = f"{type(stop.exc).__name__} {code}" if code else type(stop.exc).__name__
        print(f"[TMDB_MIRROR] ERROR: {stop.what} failed ({kind}); stopping with what landed", flush=True)
    if stats["budget_hit"]:
        print(f"[TMDB_MIRROR] budget reached ({budget_min} min or {max_files} file(s)) — stopping; "
              "the next run continues where the ledger says", flush=True)

    elapsed = (clock() - started).total_seconds()
    copied = stats[COPIED]
    avg_kb = f"{stats['bytes_uploaded'] / copied / 1024:.1f}" if copied else "-"
    per_s = f"{copied / elapsed:.1f}" if copied and elapsed > 0 else "-"
    print(
        f"[TMDB_MIRROR] SUMMARY pages={stats['pages']} titles={stats['titles']} wanted={stats['wanted']} "
        f"already={stats['already']} copied={copied} missing={stats[MISSING]} errors={stats['errors']} "
        f"skipped_size={stats['skipped_size']} {BACKDROP_SIZE}={stats[BACKDROP_SIZE]} {LOGO_SIZE}={stats[LOGO_SIZE]} "
        f"{PROFILE_SIZE}={stats[PROFILE_SIZE]} bytes_uploaded={stats['bytes_uploaded']} avg_kb={avg_kb} "
        f"files_per_s={per_s} budget_hit={stats['budget_hit']} sweep_done={stats['sweep_done']}",
        flush=True,
    )
    return stats


def _knob(name: str, default: int, low: int, high: int | None) -> int:
    """An integer from the environment, refused (exit 1) outside [low, high]."""
    raw = os.environ.get(name, str(default))
    try:
        value = int(raw)
    except ValueError:
        value = None
    if value is None or value < low or (high is not None and value > high):
        bound = f"between {low} and {high}" if high is not None else f"at least {low}"
        print(f"[TMDB_MIRROR] ERROR: {name} must be {bound} (got {raw!r}).")
        sys.exit(1)
    return value


def main(clock=None, fetch=None) -> None:
    """*clock* returns the current instant (aware UTC) and *fetch* stands in
    for requests.get; both injected by tests, the real ones otherwise."""
    print("[TMDB_MIRROR] Starting the TMDb image mirror...")
    config = load_env()
    workers = _knob("TMDB_MIRROR_WORKERS", DEFAULT_TMDB_MIRROR_WORKERS, 1, MAX_TMDB_MIRROR_WORKERS)
    budget_min = _knob("TMDB_MIRROR_TIME_BUDGET_MIN", DEFAULT_TMDB_MIRROR_TIME_BUDGET_MIN, 1,
                       MAX_TMDB_MIRROR_TIME_BUDGET_MIN)
    max_files = _knob("TMDB_MIRROR_MAX_FILES", DEFAULT_TMDB_MIRROR_MAX_FILES, 1, None)
    clock = clock if clock is not None else (lambda: datetime.now(timezone.utc))

    db = SupabaseClient(url=config["SUPABASE_URL"], key=config["SUPABASE_KEY"])
    stats = run(db, make_uploader=lambda: SupabaseClient(url=config["SUPABASE_URL"], key=config["SUPABASE_KEY"]),
                fetch=fetch if fetch is not None else requests.get, clock=clock, workers=workers,
                budget_min=budget_min, max_files=max_files)
    if stats.get("failed"):
        sys.exit(1)  # said above, after the SUMMARY: the run is red


if __name__ == "__main__":
    main()
