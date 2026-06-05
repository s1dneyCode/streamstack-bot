"""
One-time backfill: populate the popularity field for all media rows
where popularity is 0 or NULL.

Run via:
    python -m bot.backfill_popularity
"""

import os
import sys
import time

import requests
from supabase import create_client

TMDB_BASE = "https://api.themoviedb.org/3"


def load_env() -> dict[str, str]:
    required = ["TMDB_API_KEY", "SUPABASE_URL", "SUPABASE_KEY"]
    config: dict[str, str] = {}
    for key in required:
        value = os.environ.get(key)
        if not value:
            print(f"[POPULARITY] ERROR: Required environment variable '{key}' is not set.")
            sys.exit(1)
        config[key] = value
    return config


def fetch_popularity(tmdb_id: int, media_type: str, api_key: str) -> float | None:
    url = f"{TMDB_BASE}/{media_type}/{tmdb_id}"
    try:
        response = requests.get(url, params={"api_key": api_key}, timeout=15)
        response.raise_for_status()
        return response.json().get("popularity", 0.0)
    except Exception as exc:
        print(f"[POPULARITY] Failed to fetch {media_type}/{tmdb_id}: {exc}")
        return None


def main() -> None:
    print("[POPULARITY] Starting popularity backfill...")
    config = load_env()

    db = create_client(config["SUPABASE_URL"], config["SUPABASE_KEY"])
    api_key = config["TMDB_API_KEY"]

    # Fetch all rows where popularity is 0 or NULL
    page_size = 1000
    offset = 0
    rows: list[dict] = []

    while True:
        response = (
            db.table("media")
            .select("id, tmdb_id, title, media_type, popularity")
            .or_("popularity.is.null,popularity.eq.0")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        batch = response.data or []
        rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size

    total = len(rows)
    print(f"[POPULARITY] {total} rows to backfill.")

    updated = 0
    for i, row in enumerate(rows, start=1):
        tmdb_id    = row["tmdb_id"]
        title      = row["title"]
        media_type = row["media_type"]

        popularity = fetch_popularity(tmdb_id, media_type, api_key)
        if popularity is not None:
            db.table("media").update({"popularity": popularity}).eq("tmdb_id", tmdb_id).execute()
            print(f"[POPULARITY] {i}/{total} {title}: {popularity}")
            updated += 1
        else:
            print(f"[POPULARITY] {i}/{total} {title}: skipped (fetch failed)")

        time.sleep(0.2)

    print(f"[POPULARITY] Done. {updated}/{total} rows updated.")


if __name__ == "__main__":
    main()
