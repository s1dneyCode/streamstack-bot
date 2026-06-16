"""
Backfill: populate vote_average in media_seasons for all rows where it is NULL.

Calls /tv/{tmdb_id}/season/{season_number} for each season and writes the
season-level vote_average back to the DB.

Run via:
    python -m bot.backfill_season_ratings
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
            print(f"[SEASON-RATINGS] ERROR: Required environment variable '{key}' is not set.")
            sys.exit(1)
        config[key] = value
    return config


def main() -> None:
    print("[SEASON-RATINGS] Starting season ratings backfill...")
    config = load_env()

    db   = SupabaseClient(url=config["SUPABASE_URL"], key=config["SUPABASE_KEY"])
    tmdb = TmdbClient(api_key=config["TMDB_API_KEY"])

    page_size = 1000
    offset    = 0
    targets: list[dict] = []

    while True:
        batch = (
            db.client.table("media_seasons")
            .select("id, season_number, media!inner(tmdb_id, title)")
            .is_("vote_average", "null")
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
    print(f"[SEASON-RATINGS] {total} seasons to process.")

    for i, row in enumerate(targets, start=1):
        season_id     = row["id"]
        season_number = row["season_number"]
        tmdb_id       = row["media"]["tmdb_id"]
        title         = row["media"]["title"]

        try:
            data     = tmdb._get(f"/tv/{tmdb_id}/season/{season_number}")
            vote_avg = data.get("vote_average")
        except Exception as exc:
            print(f"[SEASON-RATINGS] {i}/{total} {title} S{season_number}: fetch failed — {exc}")
            failed += 1
            time.sleep(0.25)
            continue

        if vote_avg is not None:
            db.client.table("media_seasons").update({"vote_average": vote_avg}).eq("id", season_id).execute()
            updated += 1
        else:
            failed += 1

        if i % 100 == 0:
            print(f"[SEASON-RATINGS] Progress: {i}/{total} ({updated} updated, {failed} failed)")

        time.sleep(0.25)

    print(f"[SEASON-RATINGS] Done. {total} processed — {updated} updated, {failed} failed.")


if __name__ == "__main__":
    main()
