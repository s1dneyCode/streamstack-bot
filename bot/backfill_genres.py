"""
One-time backfill: populate the genre field for all rows in public.media
where genre IS NULL or genre = ''.

Run via:
    python -m bot.backfill_genres
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
            print(f"[GENRE] ERROR: Required environment variable '{key}' is not set.")
            sys.exit(1)
        config[key] = value
    return config


def main() -> None:
    print("[GENRE] Starting genre backfill...")
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
            .or_("genre.is.null,genre.eq.")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        batch = response.data or []
        rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size

    total = len(rows)
    print(f"[GENRE] {total} rows to process.")

    updated = 0
    not_found = 0

    for i, row in enumerate(rows, start=1):
        tmdb_id    = row["tmdb_id"]
        title      = row["title"]
        media_type = row["media_type"]

        try:
            data = tmdb._get(f"/{media_type}/{tmdb_id}")
            genre_str = ', '.join(g["name"] for g in data.get("genres", []))
        except Exception as exc:
            print(f"[GENRE] {i}/{total} {title}: fetch failed — {exc}")
            not_found += 1
            time.sleep(0.2)
            continue

        if genre_str:
            db.table("media").update({"genre": genre_str}).eq("tmdb_id", tmdb_id).execute()
            print(f"[GENRE] {i}/{total} {title}: {genre_str}")
            updated += 1
        else:
            print(f"[GENRE] {i}/{total} {title}: no genres found")
            not_found += 1

        time.sleep(0.2)

    print(f"[GENRE] Done. {updated} updated, {not_found} not found or failed.")


if __name__ == "__main__":
    main()
