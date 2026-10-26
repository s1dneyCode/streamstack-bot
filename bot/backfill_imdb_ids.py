"""
Backfill: ask TMDb for the IMDb id of every title that has none, once, and
remember the asking (B5.3).

Why: the Trakt drain (bot/sync_trakt.py) asks /related by IMDb id in ONE
call when a title has one, and takes the resolve path — /search/tmdb, then
/related — in TWO when it does not. Run #26 sent 1,913 of its 3,000 titles
the long way for exactly that reason (most of them shows; only ~17 % of
shows had an id).

The queue lives on the server: media rows with imdb_id IS NULL AND
imdb_checked_at IS NULL AND tmdb_missing_at IS NULL (migration
20261026120000 adds imdb_checked_at — "when the bot last asked TMDb; null =
never" — and the partial index that serves exactly this set). It is read in
keyset pages of PAGE_SIZE by id (id > the last one seen, never an offset), so
the rows this run stamps leave the set without shifting the pages.

Per title, TmdbClient.lookup_imdb_id tells four outcomes apart, and the
write goes by the row's id — never by tmdb_id, which a movie and a show can
share:
  FOUND   → imdb_id and imdb_checked_at = now
  NONE    → imdb_checked_at = now (TMDb has no id for it)
  MISSING → imdb_checked_at = now, counted (404: gone from TMDb; the house's
            tmdb_missing_at has its own writers, SupabaseClient.set_tmdb_missing
            from main.py, enrich_new_titles.py and sync_changes.py — not this)
  ERROR   → nothing written, counted; the title stays queued for the next run
One PATCH per row (the logo backfills write the same way: there is no bulk
writer). TMDb is paced by TmdbClient's own limiter (20 req/s); nothing here
sleeps. A time budget (IMDB_TIME_BUDGET_MIN, default 80, inside the job's 90)
and a batch cap (IMDB_BACKFILL_BATCH, default 20,000) end the run with its
SUMMARY; the queue shrinks as rows are stamped, so the next run resumes.

FAILS FAST if the server has no media.imdb_checked_at yet (42703, "column …
does not exist"): one line naming the migration, exit 1, nothing written.

(The May version of this file read every imdb_id IS NULL row with offsets,
wrote by tmdb_id alone, slept 0.2 s a row and printed every title. B5.3
replaced it whole; only the file name and its entry point remain.)

Run via:
    python -m bot.backfill_imdb_ids
"""

import os
import sys
from datetime import datetime, timedelta, timezone

from .supabase_client import SupabaseClient
from .tmdb import IMDB_ERROR, IMDB_FOUND, IMDB_MISSING, IMDB_NONE, TmdbClient

PAGE_SIZE = 1000
PROGRESS_EVERY = 1000
DEFAULT_IMDB_BACKFILL_BATCH = 20000
DEFAULT_IMDB_TIME_BUDGET_MIN = 80
IMDB_CHECKED_MIGRATION = "20261026120000"


def load_env() -> dict[str, str]:
    required = ["TMDB_API_KEY", "SUPABASE_URL", "SUPABASE_KEY"]
    config: dict[str, str] = {}
    for key in required:
        value = os.environ.get(key)
        if not value:
            print(f"[IMDB_ID] ERROR: Required environment variable '{key}' is not set.")
            sys.exit(1)
        config[key] = value
    return config


def _queue(query):
    """The selection, shared by the page reads and the final count."""
    return query.is_("imdb_id", "null").is_("imdb_checked_at", "null").is_("tmdb_missing_at", "null")


def _is_missing_column(exc: BaseException) -> bool:
    """True when PostgREST says media.imdb_checked_at does not exist: the
    migration that adds it is not applied yet."""
    message = str(getattr(exc, "message", None) or exc)
    return getattr(exc, "code", None) == "42703" or ("imdb_checked_at" in message and "does not exist" in message)


def fetch_page(db, after_id) -> list[dict]:
    """One keyset page of the queue: id > *after_id* (none for the first),
    ordered by id."""
    query = _queue(db.client.table("media").select("id, tmdb_id, media_type"))
    if after_id is not None:
        query = query.gt("id", after_id)
    return query.order("id").limit(PAGE_SIZE).execute().data or []


def count_queue(db) -> int | None:
    """How many titles are still queued, or None when the count fails."""
    try:
        return _queue(db.client.table("media").select("id", count="exact")).limit(1).execute().count or 0
    except Exception:
        return None


def _write(db, row: dict, fields: dict) -> bool:
    """One PATCH, by the row's id. A failure is logged (tmdb id and type,
    the error's kind — no title, no URL) and the row stays queued."""
    try:
        db.client.table("media").update(fields).eq("id", row["id"]).execute()
        return True
    except Exception as exc:
        print(f"[IMDB_ID] ERROR tmdb={row['tmdb_id']} type={row['media_type']}: write failed ({type(exc).__name__})",
              flush=True)
        return False


def backfill(db, tmdb, *, clock, batch: int = DEFAULT_IMDB_BACKFILL_BATCH,
             budget_min: int = DEFAULT_IMDB_TIME_BUDGET_MIN) -> dict:
    """Drain the queue: up to *batch* titles, until *budget_min* minutes
    after the start on *clock* (aware UTC; injected by tests)."""
    stats = {"scanned": 0, "found": 0, "none": 0, "missing": 0, "errors": 0, "written": 0, "budget_hit": 0,
             "queue_failed": None}
    deadline = clock() + timedelta(minutes=budget_min)
    print(f"[IMDB_ID] Draining up to {batch} title(s) with no imdb_id and never asked, "
          f"in pages of {PAGE_SIZE}, for at most {budget_min} min...", flush=True)

    def out_of_time() -> bool:
        if clock() < deadline:
            return False
        stats["budget_hit"] = 1
        print(f"[IMDB_ID] time budget of {budget_min} min reached after {stats['scanned']} title(s) — "
              "stopping; the rest waits for the next run", flush=True)
        return True

    after = None
    while stats["scanned"] < batch and not out_of_time():
        try:
            rows = fetch_page(db, after)
        except Exception as exc:
            if _is_missing_column(exc):
                print(f"[IMDB_ID] ERROR: the server has no media.imdb_checked_at — apply migration "
                      f"{IMDB_CHECKED_MIGRATION} before this code runs.", flush=True)
                sys.exit(1)
            stats["queue_failed"] = type(exc).__name__
            print(f"[IMDB_ID] ERROR: the queue page failed ({type(exc).__name__}); stopping with what landed",
                  flush=True)
            break
        if not rows:
            break
        for row in rows:
            if stats["scanned"] >= batch or out_of_time():
                break
            stats["scanned"] += 1
            result = tmdb.lookup_imdb_id(row["tmdb_id"], row["media_type"])
            now = clock().isoformat()
            if result.outcome == IMDB_ERROR:
                stats["errors"] += 1
                print(f"[IMDB_ID] ERROR tmdb={row['tmdb_id']} type={row['media_type']}: {result.reason}", flush=True)
            else:
                fields = {"imdb_checked_at": now}
                if result.outcome == IMDB_FOUND:
                    fields = {"imdb_id": result.imdb_id, "imdb_checked_at": now}
                if _write(db, row, fields):
                    stats["written"] += 1
                    key = {IMDB_FOUND: "found", IMDB_NONE: "none", IMDB_MISSING: "missing"}[result.outcome]
                    stats[key] += 1
                else:
                    stats["errors"] += 1
            if stats["scanned"] % PROGRESS_EVERY == 0:
                print(f"[IMDB_ID] progress: {stats['scanned']} scanned — found {stats['found']}, none {stats['none']}, "
                      f"missing {stats['missing']}, errors {stats['errors']}", flush=True)
        if stats["budget_hit"] or len(rows) < PAGE_SIZE:
            break
        after = rows[-1]["id"]

    remaining = count_queue(db)
    print(
        f"[IMDB_ID] SUMMARY scanned={stats['scanned']} found={stats['found']} none={stats['none']} "
        f"missing={stats['missing']} errors={stats['errors']} written={stats['written']} "
        f"budget_hit={stats['budget_hit']} remaining={remaining if remaining is not None else '-'}",
        flush=True,
    )
    return stats


def main(clock=None) -> None:
    """*clock* returns the current instant (aware UTC); injected by tests,
    the real clock otherwise."""
    print("[IMDB_ID] Starting imdb_id backfill...")
    config = load_env()
    clock = clock if clock is not None else (lambda: datetime.now(timezone.utc))
    batch = int(os.environ.get("IMDB_BACKFILL_BATCH", DEFAULT_IMDB_BACKFILL_BATCH))
    budget_min = int(os.environ.get("IMDB_TIME_BUDGET_MIN", DEFAULT_IMDB_TIME_BUDGET_MIN))

    db = SupabaseClient(url=config["SUPABASE_URL"], key=config["SUPABASE_KEY"])
    tmdb = TmdbClient(api_key=config["TMDB_API_KEY"])
    stats = backfill(db, tmdb, clock=clock, batch=batch, budget_min=budget_min)
    if stats.get("queue_failed"):
        sys.exit(1)  # said above: the queue itself failed, the run is red


if __name__ == "__main__":
    main()
