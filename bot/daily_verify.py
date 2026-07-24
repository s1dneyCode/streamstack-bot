"""
Daily verification bot: runs independently from the nightly bot (main.py) and
the enrichment bot (enrich_new_titles.py) to catch drift in a few high-churn
fields that can go stale between nightly runs.

Checks (in order, each independent — a failure in one does not stop the others):
  1. Now playing    — re-sync is_in_theatres against /movie/now_playing (US)
                       and force status='Released' for anything confirmed there.
  2. Upcoming dates  — re-fetch /movie/{id} for movies releasing in the next
                       60 days; release dates shift often as marketing firms up.
  3. Stale status    — re-fetch /movie|tv/{id} for any title whose release_date
                       has passed but status is still a pre-release value
                       (Planned, Post Production, In Production, Rumored).

Run via:
    python -m bot.daily_verify
"""

import os
import sys
import time
from datetime import date, timedelta

from .tmdb import TmdbClient
from .supabase_client import SupabaseClient, _PRE_RELEASE_STATUSES

BATCH_SIZE = 100


def load_env() -> dict[str, str]:
    required = ["TMDB_API_KEY", "SUPABASE_URL", "SUPABASE_KEY"]
    config: dict[str, str] = {}
    for key in required:
        value = os.environ.get(key)
        if not value:
            print(f"[DAILY VERIFY] ERROR: Required environment variable '{key}' is not set.")
            sys.exit(1)
        config[key] = value
    return config


def verify_now_playing(db: SupabaseClient, tmdb: TmdbClient) -> None:
    """Fetch TMDB now_playing (US) and sync is_in_theatres + force status=Released."""
    print("[VERIFY] Check 1: Now playing (theatres)...")

    now_playing     = tmdb.get_now_playing_movies(pages=5)
    now_playing_ids = {item["tmdb_id"] for item in now_playing}
    print(f"[VERIFY]   {len(now_playing_ids)} movies currently in theatres (US, TMDB).")

    # Mark/refresh movies confirmed in now_playing, in chunks of BATCH_SIZE ids.
    marked   = 0
    id_list  = list(now_playing_ids)
    for i in range(0, len(id_list), BATCH_SIZE):
        chunk = id_list[i:i + BATCH_SIZE]
        rows = (
            db.client.table("media")
            .select("id, tmdb_id, title, is_in_theatres, status")
            .eq("media_type", "movie")
            .in_("tmdb_id", chunk)
            .execute()
            .data or []
        )
        for row in rows:
            if not row.get("is_in_theatres") or row.get("status") != "Released":
                db.client.table("media").update({
                    "is_in_theatres": True,
                    "status": "Released",
                }).eq("id", row["id"]).execute()
                marked += 1
                if marked % 50 == 0:
                    print(f"[VERIFY]   progress: {marked} titles marked in theatres/Released")

    # Read the full "currently in theatres" set first, then clear stale flags —
    # avoids the WHERE clause shrinking mid-pagination as rows get updated.
    currently_in_theatres: list[dict] = []
    offset = 0
    while True:
        batch = (
            db.client.table("media")
            .select("id, tmdb_id, title")
            .eq("media_type", "movie")
            .eq("is_in_theatres", True)
            .range(offset, offset + BATCH_SIZE - 1)
            .execute()
            .data or []
        )
        currently_in_theatres.extend(batch)
        if len(batch) < BATCH_SIZE:
            break
        offset += BATCH_SIZE

    cleared = 0
    for row in currently_in_theatres:
        if row["tmdb_id"] not in now_playing_ids:
            db.client.table("media").update({"is_in_theatres": False}).eq("id", row["id"]).execute()
            cleared += 1
            if cleared % 50 == 0:
                print(f"[VERIFY]   progress: {cleared} titles cleared from theatres")

    print(f"[VERIFY]   Check 1 done: {marked} marked in theatres/Released, {cleared} cleared.")


def verify_upcoming_dates(db: SupabaseClient, tmdb: TmdbClient) -> None:
    """Re-fetch release dates for movies releasing in the next 60 days."""
    print("[VERIFY] Check 2: Upcoming release dates...")

    today_str = date.today().isoformat()
    horizon   = (date.today() + timedelta(days=60)).isoformat()

    targets: list[dict] = []
    offset = 0
    while True:
        batch = (
            db.client.table("media")
            .select("id, tmdb_id, title, release_date, status")
            .eq("media_type", "movie")
            .gte("release_date", today_str)
            .lte("release_date", horizon)
            .range(offset, offset + BATCH_SIZE - 1)
            .execute()
            .data or []
        )
        targets.extend(batch)
        if len(batch) < BATCH_SIZE:
            break
        offset += BATCH_SIZE

    total = len(targets)
    print(f"[VERIFY]   {total} upcoming movies to re-check.")

    changed = 0
    for i, row in enumerate(targets, start=1):
        try:
            detail = tmdb._get(f"/movie/{row['tmdb_id']}")
        except Exception as exc:
            print(f"[VERIFY]   {row['title']}: fetch failed — {exc}")
            time.sleep(0.25)
            continue

        new_date   = detail.get("release_date")
        new_status = detail.get("status")

        update_payload: dict = {}
        if new_date and new_date != row.get("release_date"):
            update_payload["release_date"]    = new_date
            update_payload["us_release_date"] = new_date
            print(f"[VERIFY] {row['title']}: date changed {row.get('release_date')} → {new_date}")

        if new_status == "Released" and row.get("status") != "Released":
            update_payload["status"] = "Released"

        if update_payload:
            db.client.table("media").update(update_payload).eq("id", row["id"]).execute()
            changed += 1

        if i % 50 == 0:
            print(f"[VERIFY]   progress: {i}/{total} processed ({changed} changed)")

        time.sleep(0.25)

    print(f"[VERIFY]   Check 2 done: {changed}/{total} updated.")


def verify_stale_status(db: SupabaseClient, tmdb: TmdbClient) -> None:
    """Fix titles where release_date has passed but status is still a pre-release value."""
    print("[VERIFY] Check 3: Stale status...")

    today_str = date.today().isoformat()

    targets: list[dict] = []
    offset = 0
    while True:
        batch = (
            db.client.table("media")
            .select("id, tmdb_id, title, media_type, status")
            .lte("release_date", today_str)
            .in_("status", list(_PRE_RELEASE_STATUSES))
            .range(offset, offset + BATCH_SIZE - 1)
            .execute()
            .data or []
        )
        targets.extend(batch)
        if len(batch) < BATCH_SIZE:
            break
        offset += BATCH_SIZE

    total = len(targets)
    print(f"[VERIFY]   {total} titles with stale status to re-check.")

    changed = 0
    for i, row in enumerate(targets, start=1):
        try:
            detail     = tmdb._get(f"/{row['media_type']}/{row['tmdb_id']}")
            new_status = detail.get("status")
        except Exception as exc:
            print(f"[VERIFY]   {row['title']}: fetch failed — {exc}")
            time.sleep(0.25)
            continue

        if new_status and new_status != row.get("status"):
            db.client.table("media").update({"status": new_status}).eq("id", row["id"]).execute()
            print(f"[VERIFY] {row['title']}: status {row.get('status')} → {new_status}")
            changed += 1

        if i % 50 == 0:
            print(f"[VERIFY]   progress: {i}/{total} processed ({changed} changed)")

        time.sleep(0.25)

    print(f"[VERIFY]   Check 3 done: {changed}/{total} updated.")


def main() -> None:
    print("[DAILY VERIFY] Starting...")
    config = load_env()

    db   = SupabaseClient(url=config["SUPABASE_URL"], key=config["SUPABASE_KEY"])
    tmdb = TmdbClient(api_key=config["TMDB_API_KEY"])

    try:
        verify_now_playing(db, tmdb)
    except Exception as exc:
        print(f"[DAILY VERIFY] Check 1 (now playing) failed: {exc}")

    try:
        verify_upcoming_dates(db, tmdb)
    except Exception as exc:
        print(f"[DAILY VERIFY] Check 2 (upcoming dates) failed: {exc}")

    try:
        verify_stale_status(db, tmdb)
    except Exception as exc:
        print(f"[DAILY VERIFY] Check 3 (stale status) failed: {exc}")

    print("[DAILY VERIFY] Done.")


if __name__ == "__main__":
    main()
