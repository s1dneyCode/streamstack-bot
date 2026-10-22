"""
Backfill: measure the luminance over black of every title logo the
catalogue already has, and write it to media.title_logo_luminance.

Selection: title_logo_url IS NOT NULL AND title_logo_luminance IS NULL —
the logos nobody has measured yet. That puts it in the `_new` bucket: it
only ever touches rows where its own field IS NULL. The number itself has
one definition, bot/logo_luminance.py; this script only applies it.

RE-RUNNABLE BY CONSTRUCTION. The catalogue holds tens of thousands of
logos and each one is a CDN fetch, so one dispatch may not get through
them all inside the job's timeout-minutes. Every run starts again from
whatever is still NULL, so a run cut short loses nothing: dispatch it
again. The run also stops itself after TIME_BUDGET_SECONDS, inside the
job's 180 minutes, so it always reaches its last line — and that line
says how many logos are still NULL.

A logo whose measure answers None (the CDN failed, the bytes did not
decode) is left NULL and counted as unmeasurable — never written as 0,
which would read as "black" and hide a logo that may be legible. So is a
row whose write fails (a transient PostgREST error): logged, left NULL,
counted, and the run goes on — one failed PATCH must not cost a long run
its summary.

Progress lines are flushed as they print: the Backfills job does not set
PYTHONUNBUFFERED, and a three-hour run should show it is alive.

Run via:
    python -m bot.backfill_title_logo_luminance
"""

import os
import sys
import time

from .supabase_client import SupabaseClient
from .logo_luminance import luminance_for_logo

PAGE_SIZE = 1000
SLEEP_SECONDS = 0.05  # between fetches: a CDN, not the TMDb API
PROGRESS_EVERY = 100
# The Backfills job's timeout-minutes is 180. Stop starting new logos at 170
# so the summary, and the count left, always print before the job is killed.
TIME_BUDGET_SECONDS = 170 * 60


def load_env() -> dict[str, str]:
    required = ["SUPABASE_URL", "SUPABASE_KEY"]
    config: dict[str, str] = {}
    for key in required:
        value = os.environ.get(key)
        if not value:
            print(f"[LUMINANCE] ERROR: Required environment variable '{key}' is not set.")
            sys.exit(1)
        config[key] = value
    return config


def _unmeasured(query):
    """The selection, shared by the page reads and the final count:
    title_logo_url IS NOT NULL AND title_logo_luminance IS NULL."""
    return query.not_.is_("title_logo_url", "null").is_("title_logo_luminance", "null")


def fetch_unmeasured(db) -> list[dict]:
    """Every row to measure, read whole BEFORE any write.

    The updates shrink this very selection, and paginating a set while it
    empties skips rows — daily_verify.py's verify_now_playing learned that.
    """
    rows: list[dict] = []
    offset = 0
    while True:
        batch = (
            _unmeasured(db.client.table("media").select("id, title_logo_url"))
            .order("id")
            .range(offset, offset + PAGE_SIZE - 1)
            .execute()
            .data or []
        )
        rows.extend(batch)
        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return rows


def count_unmeasured(db) -> int:
    return (
        _unmeasured(db.client.table("media").select("id", count="exact"))
        .limit(1)
        .execute()
        .count or 0
    )


def backfill(db, *, measure, sleep, clock, budget_seconds: float = TIME_BUDGET_SECONDS) -> dict:
    """Measure every unmeasured logo, one update per measured row."""
    rows = fetch_unmeasured(db)
    total = len(rows)
    print(f"[LUMINANCE] {total} logo(s) to measure.", flush=True)

    started = clock()
    measured = 0
    unmeasurable = 0
    for i, row in enumerate(rows, start=1):
        if clock() - started >= budget_seconds:
            print(
                f"[LUMINANCE] Time budget reached after {i - 1}/{total} — stopping; "
                "dispatch again to continue.",
                flush=True,
            )
            break

        value = measure(row["title_logo_url"])
        if value is None:
            unmeasurable += 1  # left NULL: the next run tries again
        else:
            try:
                db.client.table("media").update({"title_logo_luminance": value}).eq("id", row["id"]).execute()
                measured += 1
            except Exception as exc:
                # One failed PATCH must not cost a long run its summary: the
                # row stays NULL and the next run tries again. Class only.
                print(f"[LUMINANCE] {row['title_logo_url']}: write failed ({type(exc).__name__})", flush=True)
                unmeasurable += 1

        if i % PROGRESS_EVERY == 0:
            print(f"[LUMINANCE] {i}/{total} — measured {measured}, unmeasurable {unmeasurable}", flush=True)
        sleep(SLEEP_SECONDS)

    left = count_unmeasured(db)
    print(
        f"[LUMINANCE] Done. measured {measured} / unmeasurable {unmeasurable} / "
        f"total {total} · {left} still NULL",
        flush=True,
    )
    return {"measured": measured, "unmeasurable": unmeasurable, "total": total, "left": left}


def main() -> None:
    print("[LUMINANCE] Starting title logo luminance backfill...")
    config = load_env()

    db = SupabaseClient(url=config["SUPABASE_URL"], key=config["SUPABASE_KEY"])
    backfill(db, measure=luminance_for_logo, sleep=time.sleep, clock=time.monotonic)


if __name__ == "__main__":
    main()
