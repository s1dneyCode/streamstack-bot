"""
Backfill: populate release_date (earliest worldwide) and us_release_date
for all movies where us_release_date is currently NULL.

Run via:
    python -m bot.backfill_release_dates
"""

import os
import sys
import time

from .tmdb import TmdbClient
from .supabase_client import SupabaseClient


def load_env() -> dict[str, str]:
    required = ["TMDB_API_KEY", "SUPABASE_URL", "SUPABASE_KEY"]
    config: dict[str, str] = {}
    for key in required:
        value = os.environ.get(key)
        if not value:
            print(f"[RELEASE-DATES] ERROR: Required environment variable '{key}' is not set.")
            sys.exit(1)
        config[key] = value
    return config


def main() -> None:
    print("[RELEASE-DATES] Starting release dates backfill...")
    config = load_env()

    db   = SupabaseClient(url=config["SUPABASE_URL"], key=config["SUPABASE_KEY"])
    tmdb = TmdbClient(api_key=config["TMDB_API_KEY"])

    # Fetch all movies with us_release_date IS NULL (paginated)
    page_size = 1000
    offset    = 0
    targets: list[dict] = []
    while True:
        batch = (
            db.client.table("media")
            .select("id, tmdb_id, title")
            .eq("media_type", "movie")
            .is_("us_release_date", "null")
            .range(offset, offset + page_size - 1)
            .execute()
            .data or []
        )
        targets.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size

    total   = len(targets)
    updated = 0
    failed  = 0
    print(f"[RELEASE-DATES] {total} movies to process.")

    for i, row in enumerate(targets, start=1):
        tmdb_id = row["tmdb_id"]
        title   = row["title"]

        try:
            release_date, us_release_date = tmdb.get_movie_release_dates(tmdb_id=tmdb_id)
        except Exception as exc:
            print(f"[RELEASE-DATES] {i}/{total} {title}: fetch failed — {exc}")
            failed += 1
            time.sleep(0.25)
            continue

        if release_date or us_release_date:
            db.client.table("media").update({
                "release_date":    release_date,
                "us_release_date": us_release_date,
            }).eq("id", row["id"]).execute()
            print(f"[RELEASE-DATES] {i}/{total} {title}: release={release_date}, us={us_release_date}")
            updated += 1
        else:
            print(f"[RELEASE-DATES] {i}/{total} {title}: no release dates found")
            failed += 1

        if i % 100 == 0:
            print(f"[RELEASE-DATES] Progress: {i}/{total} ({updated} updated, {failed} failed)")

        time.sleep(0.25)

    print(f"[RELEASE-DATES] Done. {total} processed — {updated} updated, {failed} failed.")


if __name__ == "__main__":
    main()
