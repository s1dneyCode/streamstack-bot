"""
The re-cache sweep (B5.10): every object in media-images that still carries
cacheControl = no-cache gets the house cache policy, by uploading its own
bytes again under its own key.

Why (decided 22/11): 625,395 objects — carousels/ 448,425, posters/ 125,898,
seasons/ 51,072 — were uploaded before any upload asked for a cache, and the
server stored no-cache on each; the app's image cache reads that as zero
validity, so every cold start revalidates every image it holds. B5.10 · 0a
makes every new upload ask the policy (bot/image_cache_policy.py: 7 days for
a key that names a slot, a year for a key that names TMDb's file). This
sweep gives the policy to the objects already there. A device that already
holds a file heals at its next revalidation: a 304 carries the new header.

Why a re-upload and not a metadata call: supabase/storage #1109 shows a copy
onto the same key with metadata.cacheControl changing storage.objects while
the Cache-Control the public URL serves never changes. A PUT of the bytes is
the reporter's working path, and it is what upload_image does. So every run
proves the SERVED header on its first object before it touches a second one.

THE WALK: the prefixes in the order given (IMAGE_RECACHE_PREFIXES, default
posters/,seasons/,carousels/; each must be one of the policy's PREFIXES, each
once). For each, keyset pages of public.image_cache_page(p_prefix, p_after,
p_limit) — names in C order, at most PAGE_LIMIT a page, each with its stored
size, mimetype and cacheControl; the function does not filter, so this script
skips what is done. A short page ends the prefix; the next starts at ''.

THE CURSOR: bot_state['image_recache_cursor'] = {"prefix", "after",
"done_at"}, written after every page once its uploads have landed (a killed
run asks that page again: its re-uploads are harmless, and its done rows are
free), and when the budget cuts a page, at the name just before the first
object it did not reach. It never passes an object whose upload failed and
was not retried yet: while one waits for the retry, the cursor stays just
before it (whatever prefix the walk has reached), so a run that ends before
its retry leaves it for the next run's walk. done_at is written when the last
prefix's last page is short and nothing waits for a retry. A run whose list
ENDS with the done cursor's prefix ends at once with drain_done=1, unless
IMAGE_RECACHE_RESTART is 'true' (the cursor goes back to the first prefix at
''); a run whose list goes on past that prefix walks on from it — the
`prefixes` input holds the carousels back, and a drain done without them is
not done for a list that has them. A stored prefix this run does not list
starts the list's first prefix, and says so. A read that fails is never taken
for "no cursor" (the house rule of get_bot_state): the run stops red, the
cursor untouched. A stored value this script did not write is refused,
unless restart.

PER ROW: cacheControl already max-age=… → `already`, nothing fetched. A key
the policy does not know → `skipped_shape`, nothing fetched. Otherwise its
public URL is fetched once, and the body is uploaded only when the status is
200 (`fetch_error`), the Content-Type is the row's mimetype (`bad_type`: an
HTML error page never becomes an image), the length is the stored size
(`size_mismatch`), and the ETag proves the bytes: a bare MD5 must be the
body's MD5 (`etag_mismatch`); a multipart one (MD5-N) cannot, so it is
counted `etag_unchecked` and the upload proceeds; any other ETag, or none,
proves nothing (`etag_mismatch`). Then upload_image(BUCKET, name, body,
mimetype, cache_seconds=the policy's), the bytes untouched → `retagged`.
A failed upload is retried once at the end of the run, while the time budget
holds; one that still fails is counted in `errors`, named (at most
FAILED_NAMES_MAX lines) and left, and the cursor moves on past it. One the
run had no time to retry is counted in `errors` too, and holds the cursor
(above). A fetch error or a refusal is counted and not retried: once the
drain is done, a dispatch with restart 'true' walks it again, and only what
is still on no-cache costs more than its page. Each object lands in one
counter: seen = already + skipped_shape + retagged + the refusals + errors.

THE GATE, once a run, on the first object uploaded (in the walk or in the
retry round): its public URL's Cache-Control is read every GATE_POLL_SECONDS
for up to GATE_WINDOW_SECONDS (the CDN's invalidation window) until it
carries max-age=<the policy's>, and the stored value is read back through
image_cache_page from the name just before it, one row. Both must say it, or
the run stops red after that one object: `[IMAGE_RECACHE] GATE served=…
stored=… for <name>`, then the SUMMARY. Until it passes, objects go one at a
time on the main thread; the workers start only once it has.

WORKERS: a pool like tmdb_mirror's (IMAGE_RECACHE_WORKERS, one Supabase
client per worker thread); the RPC, the cursor and the counters stay on the
main thread.

BUDGET: a time budget (IMAGE_RECACHE_TIME_BUDGET_MIN) and a cap on the
objects fetched (IMAGE_RECACHE_MAX_OBJECTS; `already` and `skipped_shape`
rows are free, and a retry is not fetched twice for the cap) end a run with
its cursor written and its SUMMARY, exit 0: a plan, not a failure.
ERRORS_IN_A_ROW_MAX failures in a row of the kind an outage makes — a fetch
that raised or answered 429 or 5xx, an upload that failed — mean Storage or
the CDN is down: the run stops red, without that page's cursor, so the next
run asks it again. A refusal (another status, type, size or ETag) is about
its own object: it neither adds to that count nor resets it. Every RPC and
bot_state call gets the house's retry of transient failures (3 attempts,
2 s then 4 s); one that still fails stops the run with what landed, its line
and the SUMMARY, exit 1.

It never writes media or any media_* table, never writes a URL column (a
re-upload keeps the key, so no URL changes), and never deletes a file or a
bot_state row.

Logs: no line per success. An error line names the key and the error's kind
or HTTP status (at most ERROR_LINES_MAX a run) — never a title or a URL. A
failed upload also prints the house upload_image's own line (the storage
path and the storage error), which the breaker above bounds.

Needs public.image_cache_page (applied by Claude on 22/11). Without it the
first call stops the run, naming it.

Run via:
    python -m bot.image_recache
"""

import hashlib
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import NamedTuple

import requests

from .image_cache_policy import PREFIXES, cache_seconds_for
# The house's one definition of a transient RPC failure (B3.1, B5.1).
from .supabase_client import SupabaseClient, _TRANSIENT_RPC_CODES, _TRANSIENT_RPC_ERRORS

BUCKET = "media-images"
PAGE_RPC = "image_cache_page"
PAGE_LIMIT = 1000                 # image_cache_page serves at most 1000 rows a call
CURSOR_KEY = "image_recache_cursor"
STATE = "bot_state"
DEFAULT_IMAGE_RECACHE_PREFIXES = "posters/,seasons/,carousels/"
FETCH_TIMEOUT_SECONDS = 30
# Objects handed to the pool between two budget checks, per worker. The last
# chunk in flight when the budget runs out is the overshoot: at worst every
# object a fetch timeout and an upload timeout (storage3's default, restated
# here) — 4 × 50 s ≈ 3.3 min, so 80 + 3.3 + ≈ 3 of setup stays inside the
# job's 90. The retry round starts a chunk only while the budget holds.
CHUNK_PER_WORKER = 4
UPLOAD_TIMEOUT_SECONDS = 20
GATE_POLL_SECONDS = 5
GATE_WINDOW_SECONDS = 90
ERRORS_IN_A_ROW_MAX = 50
ERROR_LINES_MAX = 50
FAILED_NAMES_MAX = 50
SKIPPED_LINES_MAX = 20
PROGRESS_EVERY_PAGES = 25
DEFAULT_IMAGE_RECACHE_WORKERS = 12     # B5.10.1: latency-bound, 2.6 to 7.7 objects a second at 6
MAX_IMAGE_RECACHE_WORKERS = 16
DEFAULT_IMAGE_RECACHE_TIME_BUDGET_MIN = 25
MAX_IMAGE_RECACHE_TIME_BUDGET_MIN = 80   # inside the job's 90 minutes
DEFAULT_IMAGE_RECACHE_MAX_OBJECTS = 40000
_RPC_ATTEMPTS, _RPC_RETRY_DELAYS = 3, (2, 4)   # the house's transient retry (supabase_client)
_MISSING_CODES = {"PGRST202", "42883"}

RETAGGED, FETCH_ERROR, BAD_TYPE = "retagged", "fetch_error", "bad_type"
SIZE_MISMATCH, ETAG_MISMATCH, ERROR = "size_mismatch", "etag_mismatch", "errors"
_OUTCOMES = (RETAGGED, FETCH_ERROR, BAD_TYPE, SIZE_MISMATCH, ETAG_MISMATCH, ERROR)
_MD5 = re.compile(r"[0-9a-fA-F]{32}")
_MULTIPART = re.compile(r"[0-9a-fA-F]{32}-[0-9]+")
CUT, STOPPED = "cut", "stopped"


class Item(NamedTuple):
    """One object to fetch: its prefix, its place in the page, its key, the
    name just before it in C order ('' first in its prefix), what the server
    stores, the policy's seconds and its public URL."""
    prefix: str
    index: int
    name: str
    prev: str
    nbytes: int | None
    mimetype: str | None
    seconds: int
    url: str


class Result(NamedTuple):
    """What became of one object: an outcome, the body bytes fetched and
    uploaded, whether its ETag was multipart, whether the failure is the kind
    an outage makes, and a short reason (a kind or a status, never a URL)."""
    outcome: str
    item: Item
    down: int = 0
    up: int = 0
    unchecked: bool = False
    outage: bool = False
    reason: str | None = None


class _Stop(Exception):
    """A failure that ends the run: `missing` when the server has no such function."""

    def __init__(self, what: str, exc: BaseException, missing: bool):
        super().__init__(what)
        self.what, self.exc, self.missing = what, exc, missing


def load_env() -> dict[str, str]:
    required = ["SUPABASE_URL", "SUPABASE_KEY"]
    config: dict[str, str] = {}
    for key in required:
        value = os.environ.get(key)
        if not value:
            print(f"[IMAGE_RECACHE] ERROR: Required environment variable '{key}' is not set.")
            sys.exit(1)
        config[key] = value
    return config


def parse_prefixes(raw: str | None) -> tuple[str, ...] | None:
    """The prefixes of a comma-separated list, in its order; None when the
    list is empty, holds an empty entry or a duplicate, or names a prefix
    the policy does not know."""
    entries = tuple(entry.strip() for entry in (raw or "").split(","))
    return entries if _known(entries) else None


def _known(prefixes) -> bool:
    prefixes = tuple(prefixes)
    return bool(prefixes) and len(set(prefixes)) == len(prefixes) and all(p in PREFIXES for p in prefixes)


def _is_cursor(value) -> bool:
    """True for a value this script writes: prefix and after as text (after
    '' or a name under prefix), done_at as text or null."""
    return (isinstance(value, dict) and isinstance(value.get("prefix"), str) and isinstance(value.get("after"), str)
            and (value["after"] == "" or value["after"].startswith(value["prefix"]))
            and "done_at" in value and (value["done_at"] is None or isinstance(value["done_at"], str)))


def _call(what: str, fn, sleep):
    """One server call, with the house's retry of transient failures (three
    attempts, 2 s then 4 s apart). A missing function is a _Stop marked
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
            print(f"[IMAGE_RECACHE] {what} failed ({type(exc).__name__}, attempt {attempt} of {_RPC_ATTEMPTS}) — "
                  f"retrying in {delay} s", flush=True)
            sleep(delay)


def _bare(content_type: str | None) -> str:
    return (content_type or "").split(";", 1)[0].strip().lower()


def _carries(header: str | None, seconds: int) -> bool:
    """True when a Cache-Control value holds the directive max-age=<seconds>."""
    return f"max-age={seconds}" in [d.strip().lower() for d in (header or "").split(",")]


def recache_one(uploader, item: Item, fetch) -> Result:
    """One object: fetched once from its public URL and, when every guard
    holds, uploaded again as it was, with the policy's cache."""
    try:
        response = fetch(item.url, timeout=FETCH_TIMEOUT_SECONDS)
    except Exception as exc:
        return Result(FETCH_ERROR, item, outage=True, reason=type(exc).__name__)
    if response.status_code != 200:
        status = response.status_code
        return Result(FETCH_ERROR, item, outage=status == 429 or status >= 500, reason=f"HTTP {status}")
    body = response.content or b""
    headers = getattr(response, "headers", None) or {}
    down = len(body)
    served_type = _bare(headers.get("Content-Type"))
    if not item.mimetype or served_type != _bare(item.mimetype):
        return Result(BAD_TYPE, item, down, reason=f"Content-Type {served_type or 'none'}, stored {item.mimetype}")
    if item.nbytes is None or down != item.nbytes:
        return Result(SIZE_MISMATCH, item, down, reason=f"{down} bytes, stored {item.nbytes}")
    tag = (headers.get("ETag") or "").strip().strip('"')
    unchecked = False
    if _MD5.fullmatch(tag):
        if hashlib.md5(body, usedforsecurity=False).hexdigest() != tag.lower():
            return Result(ETAG_MISMATCH, item, down, reason="its MD5 is not its ETag")
    elif _MULTIPART.fullmatch(tag):
        unchecked = True
    else:
        return Result(ETAG_MISMATCH, item, down, reason="an ETag that proves nothing (neither an MD5 nor MD5-N)")
    url = uploader().upload_image(BUCKET, item.name, body, item.mimetype, cache_seconds=item.seconds)
    if not url:
        return Result(ERROR, item, down, unchecked=unchecked, outage=True, reason="upload failed")
    return Result(RETAGGED, item, down, len(body), unchecked)


def gate(db, item: Item, fetch, clock, sleep) -> bool:
    """The served and the stored Cache-Control of the first object re-uploaded,
    read back; True when both carry the policy's max-age. Prints the GATE
    line, and on a failure the ERROR line that says which side failed."""
    want = item.seconds
    started = clock()
    while True:
        try:
            response = fetch(item.url, timeout=FETCH_TIMEOUT_SECONDS)
            headers = getattr(response, "headers", None) or {}
            served = (headers.get("Cache-Control") or "none") if response.status_code == 200 \
                else f"HTTP {response.status_code}"
        except Exception as exc:
            served = type(exc).__name__
        waited = (clock() - started).total_seconds()
        if _carries(served, want) or waited >= GATE_WINDOW_SECONDS:
            break
        sleep(GATE_POLL_SECONDS)
    rows = _call(PAGE_RPC, lambda: db.client.rpc(
        PAGE_RPC, {"p_prefix": item.prefix, "p_after": item.prev, "p_limit": 1}).execute().data or [], sleep)
    stored = (rows[0].get("cache_control") or "none") if rows and rows[0].get("name") == item.name else "not found"
    print(f"[IMAGE_RECACHE] GATE served={served} stored={stored} for {item.name}", flush=True)
    sides = []
    if not _carries(served, want):
        sides.append(f"its public URL still served {served} after {GATE_WINDOW_SECONDS} s")
    if not _carries(stored, want):
        sides.append(f"the server stored {stored}")
    if sides:
        print(f"[IMAGE_RECACHE] ERROR: the gate failed for {item.name} (max-age={want} wanted): {'; '.join(sides)} "
              "(supabase/storage #1109) — stopping after one object", flush=True)
    return not sides


def run(db, *, make_uploader, fetch=requests.get, clock, sleep=time.sleep, prefixes=None,
        workers: int = DEFAULT_IMAGE_RECACHE_WORKERS, budget_min: int = DEFAULT_IMAGE_RECACHE_TIME_BUDGET_MIN,
        max_objects: int = DEFAULT_IMAGE_RECACHE_MAX_OBJECTS, restart: bool = False) -> dict:
    """One sweep, from the cursor, until the last prefix is walked, the time
    budget on *clock* (aware UTC) runs out or *max_objects* are fetched.
    *make_uploader* builds the Supabase client a worker thread uploads with
    (one per thread)."""
    prefixes = tuple(prefixes) if prefixes is not None else parse_prefixes(DEFAULT_IMAGE_RECACHE_PREFIXES)
    stats = {"pages": 0, "seen": 0, "already": 0, "skipped_shape": 0, "etag_unchecked": 0,
             "bytes_down": 0, "bytes_up": 0, "budget_hit": 0, "drain_done": 0, "failed": None}
    for outcome in _OUTCOMES:
        stats[outcome] = 0
    started = clock()
    deadline = started.timestamp() + budget_min * 60
    position = {"prefix": prefixes[0] if prefixes else "", "after": ""}   # the cursor as last written or read
    written = [None]
    walk = {"fetched": 0, "in_a_row": 0, "gate_done": False, "printed": 0, "skipped": 0, "stop": None}
    deferred: list[Item] = []      # first-round upload failures, in walk order: retried at the end
    left: list[Item] = []          # retried and still failing: counted, named, left as they are
    local = threading.local()
    step = max(1, workers * CHUNK_PER_WORKER)

    def uploader():
        client = getattr(local, "client", None)
        if client is None:
            client = local.client = make_uploader()
        return client

    def out_of_time() -> bool:
        if clock().timestamp() < deadline:
            return False
        stats["budget_hit"] = 1
        return True

    def out_of_budget() -> bool:
        if walk["fetched"] >= max_objects:
            stats["budget_hit"] = 1
            return True
        return out_of_time()

    def error_line(name: str, reason: str) -> None:
        walk["printed"] += 1
        if walk["printed"] <= ERROR_LINES_MAX:
            print(f"[IMAGE_RECACHE] ERROR {name}: {reason}", flush=True)
        elif walk["printed"] == ERROR_LINES_MAX + 1:
            print("[IMAGE_RECACHE] … more errors not printed (the SUMMARY counts them)", flush=True)

    def account(result: Result, first_round: bool) -> None:
        stats["bytes_down"] += result.down
        if result.outcome == RETAGGED:
            walk["in_a_row"] = 0
        elif result.outage:
            walk["in_a_row"] += 1
        if result.outcome == ERROR and first_round:
            deferred.append(result.item)          # retried at the end of the run; counted then
            error_line(result.item.name, result.reason)
            return
        stats[result.outcome] += 1
        if result.unchecked:
            stats["etag_unchecked"] += 1
        if result.outcome == RETAGGED:
            stats["bytes_up"] += result.up
            return
        if result.outcome == ERROR:
            left.append(result.item)
        error_line(result.item.name, result.reason)

    def process(items: list[Item], pool, first_round: bool) -> tuple[int, str | None]:
        """Fetch and upload *items* in order: one at a time until the gate
        passes, then by chunks through the pool. Returns how many were done
        and why it stopped early (CUT by the budget, STOPPED by the gate, the
        breaker or a server call), or None."""
        done = 0
        while done < len(items):
            if out_of_budget() if first_round else out_of_time():
                return done, CUT
            if not walk["gate_done"]:
                item = items[done]
                done += 1
                walk["fetched"] += first_round
                result = recache_one(uploader, item, fetch)
                account(result, first_round)
                if result.outcome == RETAGGED:
                    walk["gate_done"] = True
                    try:
                        passed = gate(db, item, fetch, clock, sleep)
                    except _Stop as stop:
                        walk["stop"] = stop
                        return done, STOPPED
                    if not passed:
                        stats["failed"] = "gate"
                        return done, STOPPED
            else:
                size = min(step, max_objects - walk["fetched"]) if first_round else step
                chunk = items[done:done + size]
                done += len(chunk)
                walk["fetched"] += len(chunk) if first_round else 0
                for result in pool.map(lambda it: recache_one(uploader, it, fetch), chunk):
                    account(result, first_round)
            if walk["in_a_row"] >= ERRORS_IN_A_ROW_MAX:
                stats["failed"] = "files"
                print(f"[IMAGE_RECACHE] ERROR: {ERRORS_IN_A_ROW_MAX} objects in a row failed — Storage or the CDN is "
                      "down; stopping with what landed (the next run asks this page again)", flush=True)
                return done, STOPPED
        return done, None

    def write_cursor(prefix: str, after: str, done_at: str | None) -> None:
        value = (prefix, after, done_at)
        if value != written[0]:
            _call(STATE, lambda: db.set_bot_state(CURSOR_KEY, {"prefix": prefix, "after": after, "done_at": done_at}),
                  sleep)
            written[0] = value
        position["prefix"], position["after"] = prefix, after

    def held(prefix: str, after: str) -> tuple[str, str]:
        """Where the cursor may go: (prefix, after), or just before the first
        upload failure still waiting for its retry."""
        return (deferred[0].prefix, deferred[0].prev) if deferred else (prefix, after)

    print(f"[IMAGE_RECACHE] Re-caching media-images under {','.join(prefixes) or '(none)'} with {workers} "
          f"worker(s), for at most {budget_min} min or {max_objects} object(s)...", flush=True)
    try:
        if not _known(prefixes):
            stats["failed"] = "prefixes"
            print(f"[IMAGE_RECACHE] ERROR: the prefixes must be known to the policy ({', '.join(PREFIXES)}), "
                  "each once; refusing before any call", flush=True)
            return _summary(stats, prefixes, position, clock, started)
        index, after = 0, ""
        if restart:
            write_cursor(prefixes[0], "", None)
            print(f"[IMAGE_RECACHE] restart: the cursor goes back to {prefixes[0]} at ''", flush=True)
        else:
            stored = _call(STATE, lambda: db.get_bot_state(CURSOR_KEY), sleep)
            if stored is not None and not _is_cursor(stored):
                stats["failed"] = "cursor"
                print(f"[IMAGE_RECACHE] ERROR: bot_state['{CURSOR_KEY}'] holds a value this script did not write; "
                      "refusing (IMAGE_RECACHE_RESTART=true replaces it)", flush=True)
                return _summary(stats, prefixes, position, clock, started)
            if stored is not None:
                position["prefix"], position["after"] = stored["prefix"], stored["after"]
                written[0] = (stored["prefix"], stored["after"], stored["done_at"])
                if stored["done_at"] and stored["prefix"] == prefixes[-1]:
                    stats["drain_done"] = 1
                    print(f"[IMAGE_RECACHE] the drain is done (done_at {stored['done_at']}): nothing to do; "
                          "IMAGE_RECACHE_RESTART=true walks it again", flush=True)
                    return _summary(stats, prefixes, position, clock, started)
                if stored["prefix"] in prefixes:
                    index, after = prefixes.index(stored["prefix"]), stored["after"]
                    if stored["done_at"]:
                        print(f"[IMAGE_RECACHE] the drain was done up to {stored['prefix']}; this run's list goes on "
                              f"to {prefixes[index + 1]}", flush=True)
                else:
                    print(f"[IMAGE_RECACHE] the cursor's prefix {stored['prefix']} is not in this run's list — "
                          f"starting at {prefixes[0]}", flush=True)
                    position["prefix"], position["after"] = prefixes[0], ""

        walked = False
        walk_pos = [prefixes[index], after]     # the walk's own position: after the last object it covered
        with ThreadPoolExecutor(max_workers=workers) as pool:
            while not out_of_budget():
                prefix = prefixes[index]
                page = _call(PAGE_RPC, lambda: db.client.rpc(
                    PAGE_RPC, {"p_prefix": prefix, "p_after": after, "p_limit": PAGE_LIMIT}).execute().data or [],
                    sleep)
                stats["pages"] += 1
                kinds, items, prev = [], [], after
                for i, row in enumerate(page):
                    name = row.get("name") or ""
                    seconds = cache_seconds_for(name)
                    if (row.get("cache_control") or "").startswith("max-age="):
                        kinds.append("already")
                    elif seconds is None:
                        kinds.append("skipped_shape")
                        walk["skipped"] += 1
                        if walk["skipped"] <= SKIPPED_LINES_MAX:
                            print(f"[IMAGE_RECACHE] skipped {name}: a shape the policy does not know", flush=True)
                    else:
                        kinds.append(None)
                        items.append(Item(prefix, i, name, prev, row.get("bytes"), row.get("mimetype"), seconds,
                                          db.get_public_url(BUCKET, name)))
                    prev = name

                done, why = process(items, pool, first_round=True)
                if why == CUT:
                    covered = items[done].index
                elif why == STOPPED:
                    covered = items[done - 1].index + 1
                else:
                    covered = len(page)
                stats["seen"] += covered
                for kind in kinds[:covered]:
                    if kind:
                        stats[kind] += 1
                if walk["stop"] is not None:
                    raise walk["stop"]
                if why == STOPPED:
                    break
                last = page[covered - 1]["name"] if covered else after
                walk_pos[:] = [prefix, last]
                short = len(page) < PAGE_LIMIT and why != CUT
                walked = short and index == len(prefixes) - 1
                if walked and not deferred:
                    write_cursor(prefix, last, clock().isoformat())
                    stats["drain_done"] = 1
                    print(f"[IMAGE_RECACHE] {prefix} walked: the drain is done", flush=True)
                    break
                write_cursor(*held(prefix, last), None)
                if stats["pages"] % PROGRESS_EVERY_PAGES == 0:
                    print(f"[IMAGE_RECACHE] progress: {stats['pages']} page(s), {stats['seen']} seen — retagged "
                          f"{stats[RETAGGED]}, already {stats['already']}", flush=True)
                if walked or why == CUT:
                    break
                if short:
                    print(f"[IMAGE_RECACHE] {prefix} walked", flush=True)
                    index, after = index + 1, ""
                else:
                    after = last

            # The retry round: every first-round upload failure, once, while the time budget holds.
            if deferred and not stats["failed"]:
                retry = list(deferred)
                deferred.clear()
                done, why = process(retry, pool, first_round=False)
                if walk["stop"] is not None:
                    deferred.extend(retry[done:])
                    raise walk["stop"]
                deferred.extend(retry[done:])          # not retried: they hold the cursor for the next run
                if deferred and not stats["failed"]:
                    print(f"[IMAGE_RECACHE] {len(deferred)} failed upload(s) not retried (the time budget ran out); "
                          "the cursor stays before the first, so the next run asks them again", flush=True)
                if not stats["failed"]:
                    if walked and not deferred:
                        write_cursor(*walk_pos, clock().isoformat())
                        stats["drain_done"] = 1
                        print(f"[IMAGE_RECACHE] {walk_pos[0]} walked: the drain is done", flush=True)
                    else:
                        write_cursor(*held(*walk_pos), None)
    except _Stop as stop:
        stats["failed"] = stop.what
        if stop.missing:
            print(f"[IMAGE_RECACHE] ERROR: the server has no public.{stop.what}(p_prefix, p_after, p_limit) — "
                  "Claude applied it on 22/11; it must exist before this code runs.", flush=True)
        else:
            code = getattr(stop.exc, "code", None)
            kind = f"{type(stop.exc).__name__} {code}" if code else type(stop.exc).__name__
            print(f"[IMAGE_RECACHE] ERROR: {stop.what} failed ({kind}); stopping with what landed", flush=True)

    if deferred:
        stats[ERROR] += len(deferred)
        print(f"[IMAGE_RECACHE] not retried: {len(deferred)} failed upload(s), left for the next run:", flush=True)
        for item in deferred[:FAILED_NAMES_MAX]:
            print(f"[IMAGE_RECACHE]   {item.name}", flush=True)
        if len(deferred) > FAILED_NAMES_MAX:
            print(f"[IMAGE_RECACHE]   … and {len(deferred) - FAILED_NAMES_MAX} more", flush=True)
    if left:
        print(f"[IMAGE_RECACHE] still failing after the retry: {len(left)} object(s), left as they are:", flush=True)
        for item in left[:FAILED_NAMES_MAX]:
            print(f"[IMAGE_RECACHE]   {item.name}", flush=True)
        if len(left) > FAILED_NAMES_MAX:
            print(f"[IMAGE_RECACHE]   … and {len(left) - FAILED_NAMES_MAX} more", flush=True)
    if stats["budget_hit"]:
        print(f"[IMAGE_RECACHE] budget reached ({budget_min} min or {max_objects} object(s)) — stopping; the next "
              "run continues from the cursor", flush=True)
    return _summary(stats, prefixes, position, clock, started)


def _summary(stats: dict, prefixes, position: dict, clock, started) -> dict:
    elapsed = (clock() - started).total_seconds()
    retagged = stats[RETAGGED]
    per_s = f"{retagged / elapsed:.1f}" if retagged and elapsed > 0 else "-"
    stats["cursor"] = f"{position['prefix']}:{position['after']}"   # B5.10.1: what write_outputs hands the workflow
    print(
        f"[IMAGE_RECACHE] SUMMARY prefixes={','.join(prefixes or ())} pages={stats['pages']} seen={stats['seen']} "
        f"already={stats['already']} retagged={retagged} skipped_shape={stats['skipped_shape']} "
        f"fetch_error={stats[FETCH_ERROR]} bad_type={stats[BAD_TYPE]} size_mismatch={stats[SIZE_MISMATCH]} "
        f"etag_mismatch={stats[ETAG_MISMATCH]} etag_unchecked={stats['etag_unchecked']} errors={stats[ERROR]} "
        f"bytes_down={stats['bytes_down']} bytes_up={stats['bytes_up']} objects_per_s={per_s} "
        f"budget_hit={stats['budget_hit']} drain_done={stats['drain_done']} "
        f"cursor={stats['cursor']}",
        flush=True,
    )
    return stats


def write_outputs(stats: dict, path: str | None) -> None:
    """How the run ended, for the workflow (B5.10.1): appends — never
    truncates — budget_hit, drain_done, retagged, refused (every refusal plus
    the failed uploads: fetch_error + bad_type + size_mismatch +
    etag_mismatch + errors) and cursor, one name=value a line, to the file at
    *path* ($GITHUB_OUTPUT); its relaunch step reads them. No path (a local
    run has no $GITHUB_OUTPUT) writes nothing."""
    if not path:
        return
    refused = sum(stats[term] for term in (FETCH_ERROR, BAD_TYPE, SIZE_MISMATCH, ETAG_MISMATCH, ERROR))
    lines = (f"budget_hit={stats['budget_hit']}", f"drain_done={stats['drain_done']}",
             f"retagged={stats[RETAGGED]}", f"refused={refused}", f"cursor={stats['cursor']}")
    with open(path, "a", encoding="utf-8") as handle:
        handle.write("".join(f"{line}\n" for line in lines))


def _knob(name: str, default: int, low: int, high: int | None) -> int:
    """An integer from the environment, refused (exit 1) outside [low, high]."""
    raw = os.environ.get(name, str(default))
    try:
        value = int(raw)
    except ValueError:
        value = None
    if value is None or value < low or (high is not None and value > high):
        bound = f"between {low} and {high}" if high is not None else f"at least {low}"
        print(f"[IMAGE_RECACHE] ERROR: {name} must be {bound} (got {raw!r}).")
        sys.exit(1)
    return value


def main(clock=None, fetch=None, sleep=None) -> None:
    """*clock* returns the current instant (aware UTC), *fetch* stands in for
    requests.get and *sleep* for time.sleep; all injected by tests, the real
    ones otherwise."""
    print("[IMAGE_RECACHE] Starting the media-images re-cache sweep...")
    config = load_env()
    raw = os.environ.get("IMAGE_RECACHE_PREFIXES", DEFAULT_IMAGE_RECACHE_PREFIXES)
    prefixes = parse_prefixes(raw)
    if prefixes is None:
        print(f"[IMAGE_RECACHE] ERROR: IMAGE_RECACHE_PREFIXES must be a comma-separated list of known prefixes "
              f"({', '.join(PREFIXES)}), each once (got {raw!r}).")
        sys.exit(1)
    workers = _knob("IMAGE_RECACHE_WORKERS", DEFAULT_IMAGE_RECACHE_WORKERS, 1, MAX_IMAGE_RECACHE_WORKERS)
    budget_min = _knob("IMAGE_RECACHE_TIME_BUDGET_MIN", DEFAULT_IMAGE_RECACHE_TIME_BUDGET_MIN, 1,
                       MAX_IMAGE_RECACHE_TIME_BUDGET_MIN)
    max_objects = _knob("IMAGE_RECACHE_MAX_OBJECTS", DEFAULT_IMAGE_RECACHE_MAX_OBJECTS, 1, None)
    raw_restart = os.environ.get("IMAGE_RECACHE_RESTART", "false")
    if raw_restart.strip().lower() not in ("true", "false"):
        print(f"[IMAGE_RECACHE] ERROR: IMAGE_RECACHE_RESTART must be 'true' or 'false' (got {raw_restart!r}).")
        sys.exit(1)
    restart = raw_restart.strip().lower() == "true"
    clock = clock if clock is not None else (lambda: datetime.now(timezone.utc))

    db = SupabaseClient(url=config["SUPABASE_URL"], key=config["SUPABASE_KEY"])
    stats = run(db, make_uploader=lambda: SupabaseClient(url=config["SUPABASE_URL"], key=config["SUPABASE_KEY"]),
                fetch=fetch if fetch is not None else requests.get, clock=clock,
                sleep=sleep if sleep is not None else time.sleep, prefixes=prefixes, workers=workers,
                budget_min=budget_min, max_objects=max_objects, restart=restart)
    write_outputs(stats, os.environ.get("GITHUB_OUTPUT"))   # before the red exit: a red run's numbers are read too
    if stats.get("failed"):
        sys.exit(1)  # said above, after the SUMMARY: the run is red


if __name__ == "__main__":
    main()
