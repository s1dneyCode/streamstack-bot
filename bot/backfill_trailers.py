"""
One-time backfill: fetch and store trailers for all media titles that
currently have no entries in media_trailers.

Run via:
    python -m bot.backfill_trailers
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
            print(f"[TRAILERS] ERROR: Required environment variable '{key}' is not set.")
            sys.exit(1)
        config[key] = value
    return config


def main() -> None:
    print("[TRAILERS] Starting trailer backfill...")
    config = load_env()

    db   = SupabaseClient(url=config["SUPABASE_URL"], key=config["SUPABASE_KEY"])
    tmdb = TmdbClient(api_key=config["TMDB_API_KEY"])

    # Collect media_ids that already have at least one trailer
    trailer_rows = db.client.table("media_trailers").select("media_id").execute().data or []
    has_trailers: set[int] = {row["media_id"] for row in trailer_rows}

    # Fetch all media titles (paginated)
    page_size = 1000
    offset    = 0
    all_media: list[dict] = []
    while True:
        batch = (
            db.client.table("media")
            .select("id, tmdb_id, title, media_type")
            .range(offset, offset + page_size - 1)
            .execute()
            .data or []
        )
        all_media.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size

    needs_trailers = [row for row in all_media if row["id"] not in has_trailers]
    total = len(needs_trailers)
    print(f"[TRAILERS] {total} titles need trailer backfill.")

    total_inserted = 0
    no_trailers    = 0

    for i, row in enumerate(needs_trailers, start=1):
        videos = tmdb.get_videos(tmdb_id=row["tmdb_id"], media_type=row["media_type"])
        count  = db.upsert_trailers(media_id=row["id"], trailers=videos)

        total_inserted += count
        if count == 0:
            no_trailers += 1

        print(f"[TRAILERS] {i}/{total} {row['title']}: {count} trailer(s)")
        time.sleep(0.25)

    print(
        f"[TRAILERS] Done. {total} titles processed, "
        f"{total_inserted} trailers inserted, "
        f"{no_trailers} titles with no trailers found."
    )


if __name__ == "__main__":
    main()
