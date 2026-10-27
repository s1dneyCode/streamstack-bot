"""
Posters in HD for the slideshow (B5.7): an HD copy of the poster of every
title that can reach one, hosted in our own Storage.

Why: the app's slideshow crops a 2:3 poster to 9:16 for its 1080×1920 card
(it needs at least 1280×1920) and covers a 390×844 screen at 3× in its viewer.
It drew poster_url, which this repo hosts from TMDb's w500 — stretched 2.56×
on the card people post. Fran would rather the app read one more column than
call TMDb itself (27/10), so the bot makes the copy and the app reads
media.poster_hd_url, falling back to poster_url while it is null.

The set: every distinct media_id in media_list_items, together with every
non-null media_lists.cover_media_id — read in keyset pages by id (never an
offset) — then those media rows by id in chunks. A row with tmdb_missing_at
set is left out (counted `gone`); one with no poster_path is skipped (counted
`no_path`).

Pending: poster_hd_url is null, or the stem of its file differs from the stem
of poster_path (TMDb changed the poster since). The stem is the last path
segment without its extension, any `?…` ignored: get_public_url can end a URL
in a bare `?`, and the URL is stored exactly as upload_image returns it.

Per pending title, one request at a time and nothing sleeping (TMDb's image
CDN needs no key and is not the API, so there is no limiter):
  - the source is TMDb's original, image.tmdb.org/t/p/original/<file> from
    poster_path (either of its shapes) — never anything built from poster_url;
  - the fetch tells three outcomes apart (B5.3's lesson: never swallow one):
    OK (200 with bytes), MISSING (404), ERROR (a timeout, a 5xx, anything
    else);
  - larger than 1280×1920 on either side → fit inside it keeping the aspect
    (LANCZOS) and saved as a JPEG, quality 85, progressive, optimized
    (`resized`); a JPEG that already fits → uploaded byte for byte, never
    re-encoded or upscaled (`fit_as_is`); a PNG or WebP that fits → a JPEG at
    its own size (`converted`);
  - uploaded to media-images/posters-hd/{tmdb_id}/{stem}.jpg as image/jpeg
    (a versioned key per TMDb file, refresh_poster's precedent: a changed
    poster gets a new key, so no cache serves the old one) with a one-year
    cache (B5.8: that versioned key is what makes a year safe), then ONE PATCH
    by the row's id: {"poster_hd_url": <the URL upload_image returned>} —
    skipped when that is already the stored URL.
A failure at any step writes nothing for that title: counted, and it stays
pending for the next run. This script never writes poster_url or poster_path,
and never deletes a file: an HD file whose title leaves every list stays, and
so does the old file after a new stem.

A batch cap (POSTERS_HD_BATCH, default 300) and a time budget
(POSTERS_HD_TIME_BUDGET_MIN, default 20, inside the job's 30) end a run with
its SUMMARY; what is left stays pending for the next hour.

POSTERS_HD_REFRESH_EXISTING=true (a dispatch input, B5.8) makes every title
that already has an HD poster — still in the set, not gone, with a
poster_path — pending for that run, so a file made before the long cache is
uploaded again with it; its URL does not change, so nothing is written. A
title whose refresh fails is not retried by the hourly runs (for them it is
not pending): dispatch again.

FAILS FAST if the server has no media.poster_hd_url yet (42703, "column …
does not exist"): one line naming migration 20261027200000, exit 1, nothing
uploaded or written. Any other failure of the set's read: one line with the
error's kind, exit 1 (a red run), nothing written.

Logs: an error line names tmdb=… type=… and the error's kind — never a title,
a list, an owner or a URL. There is no line per success: the progress line
and the SUMMARY count them.

Run via:
    python -m bot.posters_hd
"""

import io
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import NamedTuple

import requests
from PIL import Image

from .migrate_images import poster_basename
from .supabase_client import SupabaseClient

BUCKET = "media-images"
HD_PREFIX = "posters-hd"
SOURCE_BASE = "https://image.tmdb.org/t/p/original/"
HD_BOX = (1280, 1920)          # the slideshow's 1080×1920 card needs a 9:16 crop of at least this
JPEG_QUALITY = 85
PAGE_SIZE = 1000
ID_CHUNK = 100                 # media.id is a uuid: 100 of them keep the .in_() URL near 4 KB (200 ≈ 7.9 KB)
PROGRESS_EVERY = 50
FETCH_TIMEOUT_SECONDS = 30     # an original is a few MB
DEFAULT_POSTERS_HD_BATCH = 300
DEFAULT_POSTERS_HD_TIME_BUDGET_MIN = 20
POSTER_HD_MIGRATION = "20261027200000"
# The cache an upload asks for (B5.8), through upload_image's cache_seconds:
# every object in media-images carried cacheControl = no-cache until then,
# which the app's image cache reads as zero validity. A year is safe only for
# a key versioned by its content (a TMDb file name, or its stem): a changed
# image gets a new key. bot/tmdb_mirror.py uses it too.
ONE_YEAR_CACHE_SECONDS = 31536000

# What TMDb's CDN said for a poster's original (the shape of B5.3's
# ImdbLookup): OK with its bytes; MISSING — 404; ERROR — anything else, with
# a short reason (the error's kind or the status, never the URL).
FETCH_OK, FETCH_MISSING, FETCH_ERROR = "ok", "missing", "error"
# What was done to the image before its upload.
RESIZED, FIT_AS_IS, CONVERTED = "resized", "fit_as_is", "converted"


class ImageFetch(NamedTuple):
    outcome: str
    content: bytes | None = None
    reason: str | None = None


def load_env() -> dict[str, str]:
    required = ["SUPABASE_URL", "SUPABASE_KEY"]
    config: dict[str, str] = {}
    for key in required:
        value = os.environ.get(key)
        if not value:
            print(f"[POSTER_HD] ERROR: Required environment variable '{key}' is not set.")
            sys.exit(1)
        config[key] = value
    return config


def _is_missing_column(exc: BaseException) -> bool:
    """True when PostgREST says media.poster_hd_url does not exist: the
    migration that adds it is not applied yet. A 42703 on any other column
    the set reads is not that — it is reported by its kind."""
    message = str(getattr(exc, "message", None) or exc)
    return "poster_hd_url" in message and (getattr(exc, "code", None) == "42703" or "does not exist" in message)


def file_stem(value: str | None) -> str:
    """The stem of the file a poster_path or a hosted URL names: its last path
    segment without the extension, any `?…` ignored. '' for nothing."""
    if not value:
        return ""
    return poster_basename(value.split("?", 1)[0]).rsplit(".", 1)[0]


def source_url(poster_path: str | None) -> str:
    """TMDb's original of the poster, from either shape of poster_path (the
    absolute w500 URL or the bare /<file>). '' when it names no file."""
    name = poster_basename(poster_path or "")
    return f"{SOURCE_BASE}{name}" if name else ""


def is_pending(row: dict) -> bool:
    """No HD copy yet, or one of another TMDb file than poster_path's."""
    hd = row.get("poster_hd_url")
    return not hd or file_stem(hd) != file_stem(row.get("poster_path"))


def _keyset(db, table: str, columns: str, not_null: str | None = None):
    """Every row of *table*, in keyset pages of PAGE_SIZE by id."""
    after = None
    while True:
        query = db.client.table(table).select(columns)
        if not_null:
            query = query.not_.is_(not_null, "null")
        if after is not None:
            query = query.gt("id", after)
        rows = query.order("id").limit(PAGE_SIZE).execute().data or []
        yield from rows
        if len(rows) < PAGE_SIZE:
            return
        after = rows[-1]["id"]


def read_set(db) -> list:
    """The distinct media ids that can reach a slideshow: in a list, or a
    list's cover. Sorted, so a run is reproducible."""
    ids = {r["media_id"] for r in _keyset(db, "media_list_items", "id, media_id") if r.get("media_id")}
    ids |= {r["cover_media_id"] for r in _keyset(db, "media_lists", "id, cover_media_id", not_null="cover_media_id")}
    return sorted(ids)


def read_media(db, ids: list) -> list[dict]:
    """Those media rows, by id in chunks of ID_CHUNK."""
    rows: list[dict] = []
    for i in range(0, len(ids), ID_CHUNK):
        rows.extend(
            db.client.table("media")
            .select("id, tmdb_id, media_type, poster_path, poster_hd_url, tmdb_missing_at")
            .in_("id", ids[i:i + ID_CHUNK])
            .execute()
            .data or []
        )
    return sorted(rows, key=lambda r: r["id"])


def fetch_image(url: str, fetch=requests.get) -> ImageFetch:
    """GET *url* once, with the outcome told apart."""
    try:
        response = fetch(url, timeout=FETCH_TIMEOUT_SECONDS)
    except Exception as exc:
        return ImageFetch(FETCH_ERROR, reason=type(exc).__name__)
    if response.status_code == 404:
        return ImageFetch(FETCH_MISSING)
    if response.status_code != 200:
        return ImageFetch(FETCH_ERROR, reason=f"HTTP {response.status_code}")
    if not response.content:
        return ImageFetch(FETCH_ERROR, reason="empty body")
    return ImageFetch(FETCH_OK, response.content)


def _jpeg(img: Image.Image) -> bytes:
    buf = io.BytesIO()
    img.convert("RGB").save(buf, format="JPEG", quality=JPEG_QUALITY, progressive=True, optimize=True)
    return buf.getvalue()


def hd_jpeg(data: bytes) -> tuple[bytes, str]:
    """The bytes to upload, and what was done: fit inside HD_BOX when larger
    (RESIZED), the downloaded bytes when a JPEG already fits (FIT_AS_IS), or
    a JPEG at its own size from any other format that fits (CONVERTED).
    Never upscales. Raises whatever Pillow raises on bytes it cannot read."""
    with Image.open(io.BytesIO(data)) as img:
        img.load()
        if img.width > HD_BOX[0] or img.height > HD_BOX[1]:
            # RGB first: Pillow resizes a palette or 1-bit image with NEAREST
            # whatever it is asked, and the fit is meant to be LANCZOS.
            rgb = img.convert("RGB")
            rgb.thumbnail(HD_BOX, Image.Resampling.LANCZOS)
            return _jpeg(rgb), RESIZED
        if img.format in ("JPEG", "MPO"):   # an MPO is a JPEG with more pictures after the first
            return data, FIT_AS_IS
        return _jpeg(img), CONVERTED


def _looks_like(data: bytes) -> str:
    """What bytes Pillow could not open start like — the format's name, never
    the bytes — so the exit ramp's «say which» needs no second look."""
    head = data[:32]
    if head[4:8] == b"ftyp":
        return head[8:12].decode("ascii", "replace").strip() or "ftyp"
    for magic, name in ((b"\xff\xd8\xff", "jpeg"), (b"\x89PNG", "png"), (b"GIF8", "gif"), (b"BM", "bmp")):
        if head.startswith(magic):
            return name
    if head.startswith(b"RIFF") and head[8:12] == b"WEBP":
        return "webp"
    text = head.lstrip().lower()
    if text.startswith((b"<svg", b"<?xml")):
        return "svg"
    if text.startswith((b"<!doctype", b"<html")):
        return "html"
    return "unknown"


def _error(row: dict, what: str) -> None:
    print(f"[POSTER_HD] ERROR tmdb={row['tmdb_id']} type={row['media_type']}: {what}", flush=True)


def make_hd(db, row: dict, stats: dict, fetch=requests.get) -> None:
    """One pending title: fetch, fit, upload, one PATCH by id. Any failure
    writes nothing and is counted; the title stays pending."""
    got = fetch_image(source_url(row["poster_path"]), fetch)
    if got.outcome == FETCH_MISSING:
        stats["missing"] += 1   # gone from TMDb's CDN: poster_path is stale until the nightly refreshes it
        return
    if got.outcome != FETCH_OK:
        stats["errors"] += 1
        _error(row, f"fetch failed ({got.reason})")
        return
    try:
        data, kind = hd_jpeg(got.content)
    except Exception as exc:
        stats["errors"] += 1
        _error(row, f"image unreadable ({type(exc).__name__}) — it starts like {_looks_like(got.content)}")
        return
    url = db.upload_image(BUCKET, f"{HD_PREFIX}/{row['tmdb_id']}/{file_stem(row['poster_path'])}.jpg", data,
                          "image/jpeg", cache_seconds=ONE_YEAR_CACHE_SECONDS)
    if not url:
        stats["errors"] += 1
        _error(row, "upload failed")
        return
    stats["made"] += 1
    stats[kind] += 1
    stats["bytes_uploaded"] += len(data)
    if url == row.get("poster_hd_url"):
        return  # a refresh (B5.8): the same key, so the same URL — nothing to write
    try:
        db.client.table("media").update({"poster_hd_url": url}).eq("id", row["id"]).execute()
    except Exception as exc:
        stats["errors"] += 1
        _error(row, f"write failed ({type(exc).__name__})")
        return
    stats["written"] += 1


def run(db, *, fetch=requests.get, clock, batch: int = DEFAULT_POSTERS_HD_BATCH,
        budget_min: int = DEFAULT_POSTERS_HD_TIME_BUDGET_MIN, refresh_existing: bool = False) -> dict:
    """Make every pending HD poster: up to *batch* titles, until *budget_min*
    minutes after the start on *clock* (aware UTC; injected by tests). With
    *refresh_existing*, a title that already has one is pending too."""
    stats = {"candidates": 0, "pending": 0, "made": 0, RESIZED: 0, FIT_AS_IS: 0, CONVERTED: 0, "missing": 0,
             "errors": 0, "no_path": 0, "gone": 0, "written": 0, "budget_hit": 0, "bytes_uploaded": 0}
    try:
        ids = read_set(db)
        rows = read_media(db, ids)
    except Exception as exc:
        if _is_missing_column(exc):
            print(f"[POSTER_HD] ERROR: the server has no media.poster_hd_url — apply migration "
                  f"{POSTER_HD_MIGRATION} before this code runs.", flush=True)
        else:
            print(f"[POSTER_HD] ERROR: the set's read failed ({type(exc).__name__}); stopping, nothing written.",
                  flush=True)
        sys.exit(1)

    stats["candidates"] = len(ids)
    pending = []
    for row in rows:
        if row.get("tmdb_missing_at"):
            stats["gone"] += 1
        elif not source_url(row.get("poster_path")):
            stats["no_path"] += 1
        elif refresh_existing or is_pending(row):
            pending.append(row)
    stats["pending"] = len(pending)
    print(f"[POSTER_HD] {stats['candidates']} title(s) can reach a slideshow, {stats['pending']} pending; "
          f"making up to {batch} for at most {budget_min} min...", flush=True)

    deadline = clock() + timedelta(minutes=budget_min)
    for done, row in enumerate(pending):
        if done >= batch or clock() >= deadline:
            stats["budget_hit"] = 1
            print(f"[POSTER_HD] budget reached after {done} title(s) — stopping; the rest waits for the next run",
                  flush=True)
            break
        make_hd(db, row, stats, fetch)
        if (done + 1) % PROGRESS_EVERY == 0:
            print(f"[POSTER_HD] progress: {done + 1}/{stats['pending']} — made {stats['made']}, "
                  f"missing {stats['missing']}, errors {stats['errors']}", flush=True)

    avg_kb = f"{stats['bytes_uploaded'] / stats['made'] / 1024:.1f}" if stats["made"] else "-"
    print(
        f"[POSTER_HD] SUMMARY candidates={stats['candidates']} pending={stats['pending']} made={stats['made']} "
        f"resized={stats[RESIZED]} fit_as_is={stats[FIT_AS_IS]} converted={stats[CONVERTED]} "
        f"missing={stats['missing']} errors={stats['errors']} no_path={stats['no_path']} gone={stats['gone']} "
        f"written={stats['written']} budget_hit={stats['budget_hit']} bytes_uploaded={stats['bytes_uploaded']} "
        f"avg_kb={avg_kb}",
        flush=True,
    )
    return stats


def main(clock=None, fetch=None) -> None:
    """*clock* returns the current instant (aware UTC) and *fetch* stands in
    for requests.get; both injected by tests, the real ones otherwise."""
    print("[POSTER_HD] Starting HD posters for the slideshow...")
    config = load_env()
    clock = clock if clock is not None else (lambda: datetime.now(timezone.utc))
    batch = int(os.environ.get("POSTERS_HD_BATCH", DEFAULT_POSTERS_HD_BATCH))
    budget_min = int(os.environ.get("POSTERS_HD_TIME_BUDGET_MIN", DEFAULT_POSTERS_HD_TIME_BUDGET_MIN))
    refresh_existing = os.environ.get("POSTERS_HD_REFRESH_EXISTING", "false").strip().lower() == "true"

    db = SupabaseClient(url=config["SUPABASE_URL"], key=config["SUPABASE_KEY"])
    run(db, fetch=fetch if fetch is not None else requests.get, clock=clock, batch=batch, budget_min=budget_min,
        refresh_existing=refresh_existing)


if __name__ == "__main__":
    main()
