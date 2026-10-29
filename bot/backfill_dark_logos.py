"""
Backfill: the title logos already under the app's line (B4).

Until B4 every writer kept the FIRST English logo TMDb lists, and for popular
titles that first one is often a dark variant while a light one exists: 4,495
logos measured under the line on 28/10 (Dune: Part Two 0.011, The Odyssey
0.047, House of the Dragon 0.042, Last Week Tonight 0.024, Inception 0.050),
and the app showed their text title. New titles get the one chooser when they
are written (logo_luminance.choose_logo); this corrects the rows written
before it, once.

Selection: title_logo_luminance < logo_luminance.MIN_LOGO_LUMINANCE — the
rows KNOWN to be dark. A NULL is not known to be dark and is never touched
here (backfill_title_logo_luminance.py measures those). The selection is read
whole before any write, in pages by id (the writes shrink it, and paginating a
set while it empties skips rows — backfill_title_logo_luminance.py says the
same), then walked in popularity order, id breaking ties, so the titles the
app shows first go first.

Per row: one images call through the client (TmdbClient.title_logo_candidates:
its limiter, its retries, its 401 rule) and the one chooser over the same
pool every writer uses, the stored URL's stored luminance standing in for its
fetch. It writes only when the winner is a different URL with a MEASURED
luminance higher than the stored one: one update, both keys, by id, and one
line — the title, the old URL and luminance, the new ones. The run's log is
the record of what changed. Otherwise nothing is written: the same winner,
nothing lighter, TMDb now listing no logo (or not the title: a 404), or an
error. It never writes NULL over a logo and never trades a known-dark logo
for an unmeasured one.

RE-RUNNABLE BY CONSTRUCTION. It stops starting rows after TIME_BUDGET_SECONDS
(170 minutes, inside the Backfills job's 180) and its summary still prints. A
fixed row leaves the selection; a row where every logo TMDb offers is dark
stays under the line and is checked again by the next dispatch — dispatch it
once now, and again when the app's line moves (MIN_LOGO_LUMINANCE moves with
it). It belongs to neither full sequence: it corrects populated rows once. It
writes title_logo_url, so TMDb Mirror (which runs after Backfills) copies the
new logos to our Storage.

Run via:
    python -m bot.backfill_dark_logos
"""

import os
import sys
import time

import requests

from .logo_luminance import MIN_LOGO_LUMINANCE, choose_logo, luminance_for_logo
from .supabase_client import SupabaseClient
from .tmdb import TmdbClient

PAGE_SIZE = 1000
PROGRESS_EVERY = 100
# The Backfills job's timeout-minutes is 180. Stop starting new rows at 170
# so the summary, and the count still under the line, always print.
TIME_BUDGET_SECONDS = 170 * 60
COLUMNS = "id, tmdb_id, media_type, title, popularity, title_logo_url, title_logo_luminance"


def load_env() -> dict[str, str]:
    required = ["TMDB_API_KEY", "SUPABASE_URL", "SUPABASE_KEY"]
    config: dict[str, str] = {}
    for key in required:
        value = os.environ.get(key)
        if not value:
            print(f"[DARK_LOGOS] ERROR: Required environment variable '{key}' is not set.")
            sys.exit(1)
        config[key] = value
    return config


def _dark(query):
    """The selection, shared by the page reads and the final count: the rows
    known to be under the app's line (a NULL compares to nothing)."""
    return query.lt("title_logo_luminance", MIN_LOGO_LUMINANCE)


def fetch_dark(db) -> list[dict]:
    """Every dark row, read whole before any write, in popularity order
    (highest first, id breaking ties).

    The pages are read in the order the index on (title_logo_luminance, id)
    holds (migration 20261029024854, B4.1). The first dispatch read them by
    id alone, and with no index on the luminance yet the planner walked
    media_pkey and filtered: 31,423 rows for 1,000 kept, 10.2 s, over the
    authenticator's 8 s statement timeout (57014). The index serves the
    custom plan either way; in its own order it serves a generic plan too,
    with the luminance as its index condition, instead of a pkey walk."""
    rows: list[dict] = []
    offset = 0
    while True:
        batch = (
            _dark(db.client.table("media").select(COLUMNS))
            .order("title_logo_luminance")
            .order("id")
            .range(offset, offset + PAGE_SIZE - 1)
            .execute()
            .data or []
        )
        rows.extend(batch)
        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return sorted(rows, key=lambda r: (-(r.get("popularity") or 0), r["id"]))


def count_dark(db) -> int | None:
    """How many rows are still under the line, or None when the count fails."""
    try:
        return _dark(db.client.table("media").select("id", count="exact")).limit(1).execute().count or 0
    except Exception:
        return None


def fix_row(db, tmdb, row: dict, stats: dict, measure) -> None:
    """One dark row: the chooser over TMDb's pool, and one update when it
    wins with a different URL measured lighter."""
    stats["checked"] += 1
    title = row.get("title")
    try:
        candidates = tmdb.title_logo_candidates(tmdb_id=row["tmdb_id"], media_type=row["media_type"])
    except requests.HTTPError as exc:
        if exc.response is not None and exc.response.status_code == 404:
            stats["no_logos"] += 1       # TMDb no longer lists the title, nor its logos
            return
        stats["errors"] += 1
        print(f"[DARK_LOGOS] {title}: images call failed ({type(exc).__name__})", flush=True)
        return
    except Exception as exc:
        stats["errors"] += 1
        print(f"[DARK_LOGOS] {title}: images call failed ({type(exc).__name__})", flush=True)
        return
    if not candidates:
        stats["no_logos"] += 1
        return

    stored, stored_value = row["title_logo_url"], row["title_logo_luminance"]
    url, value = choose_logo(candidates, measure=measure, known={stored: stored_value} if stored else None)
    if url == stored or value is None or value <= stored_value:
        stats["nothing_lighter"] += 1
        return
    try:
        db.client.table("media").update(
            {"title_logo_url": url, "title_logo_luminance": value}
        ).eq("id", row["id"]).execute()
    except Exception as exc:
        stats["errors"] += 1
        print(f"[DARK_LOGOS] {title}: write failed ({type(exc).__name__})", flush=True)
        return
    stats["to_light" if value >= MIN_LOGO_LUMINANCE else "to_lighter"] += 1
    print(f"[DARK_LOGOS] {title}: {stored} ({stored_value:.3f}) → {url} ({value:.3f})", flush=True)


def backfill(db, tmdb, *, measure, clock, budget_seconds: float = TIME_BUDGET_SECONDS) -> dict:
    """Check every dark row, stopping before the next row at *budget_seconds*
    on *clock* (monotonic seconds; injected by tests)."""
    stats = {"checked": 0, "to_light": 0, "to_lighter": 0, "nothing_lighter": 0, "no_logos": 0, "errors": 0}
    rows = fetch_dark(db)
    total = len(rows)
    print(f"[DARK_LOGOS] {total} title logo(s) under {MIN_LOGO_LUMINANCE} to check.", flush=True)

    started = clock()
    for i, row in enumerate(rows, start=1):
        if clock() - started >= budget_seconds:
            print(f"[DARK_LOGOS] Time budget reached after {i - 1}/{total} — stopping; dispatch again to continue.",
                  flush=True)
            break
        fix_row(db, tmdb, row, stats, measure)
        if i % PROGRESS_EVERY == 0:
            print(f"[DARK_LOGOS] {i}/{total} — to the line {stats['to_light']}, lighter {stats['to_lighter']}, "
                  f"errors {stats['errors']}", flush=True)

    left = count_dark(db)
    print(
        f"[DARK_LOGOS] SUMMARY checked={stats['checked']} to_light={stats['to_light']} "
        f"to_lighter={stats['to_lighter']} nothing_lighter={stats['nothing_lighter']} "
        f"no_logos={stats['no_logos']} errors={stats['errors']} still_dark={left if left is not None else '-'}",
        flush=True,
    )
    return stats


def main() -> None:
    print("[DARK_LOGOS] Starting the dark title logos backfill...")
    config = load_env()

    db = SupabaseClient(url=config["SUPABASE_URL"], key=config["SUPABASE_KEY"])
    tmdb = TmdbClient(api_key=config["TMDB_API_KEY"])
    backfill(db, tmdb, measure=luminance_for_logo, clock=time.monotonic)


if __name__ == "__main__":
    main()
