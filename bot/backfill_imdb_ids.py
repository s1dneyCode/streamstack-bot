"""
One-time backfill: populate imdb_id for all rows in public.media where it is NULL.

Run via:
    python -m bot.backfill_imdb_ids
"""

import os
import sys
import time

from supabase import create_client

from .tmdb import TmdbClient


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


def main() -> None:
    print("[IMDB_ID] Starting imdb_id backfill...")
    config = load_env()

    db   = create_client(config["SUPABASE_URL"], config["SUPABASE_KEY"])
    tmdb = TmdbClient(api_key=config["TMDB_API_KEY"])

    page_size = 1000
    offset = 0
    rows: list[dict] = []

    while True:
        response = (
            db.table("media")
            .select("id, tmdb_id, title, media_type")
            .is_("imdb_id", "null")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        batch = response.data or []
        rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size

    total = len(rows)
    print(f"[IMDB_ID] {total} rows to process.")

    found = 0
    not_found = 0

    for i, row in enumerate(rows, start=1):
        tmdb_id    = row["tmdb_id"]
        title      = row["title"]
        media_type = row["media_type"]

        imdb_id = tmdb.get_imdb_id(tmdb_id=tmdb_id, media_type=media_type)

        if imdb_id:
            db.table("media").update({"imdb_id": imdb_id}).eq("tmdb_id", tmdb_id).execute()
            print(f"[IMDB_ID] {i}/{total} {title}: {imdb_id}")
            found += 1
        else:
            print(f"[IMDB_ID] {i}/{total} {title}: not found")
            not_found += 1

        time.sleep(0.2)

    print(f"[IMDB_ID] Done. {found} updated, {not_found} not found.")


if __name__ == "__main__":
    main()
