"""
One-time backfill: fetch and store credits for all media titles that
currently have no entries in media_credits.

Run via:
    python -m bot.backfill_credits
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
            print(f"[CREDITS] ERROR: Required environment variable '{key}' is not set.")
            sys.exit(1)
        config[key] = value
    return config


def main() -> None:
    print("[CREDITS] Starting credits backfill...")
    config = load_env()

    db   = SupabaseClient(url=config["SUPABASE_URL"], key=config["SUPABASE_KEY"])
    tmdb = TmdbClient(api_key=config["TMDB_API_KEY"])

    # Collect media_ids that already have at least one credit row
    credit_rows = db.client.table("media_credits").select("media_id").execute().data or []
    has_credits: set[str] = {row["media_id"] for row in credit_rows}

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

    needs_credits = [row for row in all_media if row["id"] not in has_credits]
    total = len(needs_credits)
    print(f"[CREDITS] {total} titles need credits backfill.")

    total_directors = 0
    total_writers   = 0
    total_cast      = 0
    no_credits      = 0

    for i, row in enumerate(needs_credits, start=1):
        result = tmdb.get_credits(tmdb_id=row["tmdb_id"], media_type=row["media_type"])

        directors = result["directors"]
        writers   = result["writers"]
        cast      = result["cast"]

        count = db.upsert_credits(
            media_id=row["id"],
            directors=directors,
            writers=writers,
            cast=cast,
        )

        total_directors += len(directors)
        total_writers   += len(writers)
        total_cast      += len(cast)

        if count == 0:
            no_credits += 1

        print(
            f"[CREDITS] {i}/{total} {row['title']}: "
            f"{len(directors)}d / {len(writers)}w / {len(cast)}c"
        )
        time.sleep(0.25)

    print(
        f"[CREDITS] Done. {total} titles processed — "
        f"{total_directors} directors, {total_writers} writers, {total_cast} cast inserted. "
        f"{no_credits} titles with no credits found."
    )


if __name__ == "__main__":
    main()
