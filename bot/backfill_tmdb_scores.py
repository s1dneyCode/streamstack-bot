"""
One-time backfill: populate tmdb_score for rows in public.media.

By default, only processes rows where tmdb_score IS NULL or 0 (cheap
enough to run after every bulk import). Pass --force to re-check every
row regardless of current tmdb_score, for periodic full refreshes.

Run via:
    python -m bot.backfill_tmdb_scores
    python -m bot.backfill_tmdb_scores --force
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
            print(f"[TMDB_SCORE] ERROR: Required environment variable '{key}' is not set.")
            sys.exit(1)
        config[key] = value
    return config


def fetch_vote_average(tmdb_id: int, media_type: str, api_key: str) -> float | None:
    url = f"{TMDB_BASE}/{media_type}/{tmdb_id}"
    try:
        response = requests.get(url, params={"api_key": api_key}, timeout=15)
        response.raise_for_status()
        return response.json().get("vote_average", 0.0)
    except Exception as exc:
        print(f"[TMDB_SCORE] Failed to fetch {media_type}/{tmdb_id}: {exc}")
        return None


def main() -> None:
    print("[TMDB_SCORE] Starting tmdb_score backfill...")
    force = "--force" in sys.argv[1:]
    config = load_env()

    db = create_client(config["SUPABASE_URL"], config["SUPABASE_KEY"])
    api_key = config["TMDB_API_KEY"]

    page_size = 1000
    offset = 0
    rows: list[dict] = []

    while True:
        query = db.table("media").select("id, tmdb_id, title, media_type")
        if not force:
            query = query.or_("tmdb_score.is.null,tmdb_score.eq.0")
        response = query.range(offset, offset + page_size - 1).execute()
        batch = response.data or []
        rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size

    total = len(rows)
    scope = "all rows (--force)" if force else "rows missing tmdb_score"
    print(f"[TMDB_SCORE] {total} {scope} to process.")

    updated = 0
    for i, row in enumerate(rows, start=1):
        tmdb_id    = row["tmdb_id"]
        title      = row["title"]
        media_type = row["media_type"]

        vote_average = fetch_vote_average(tmdb_id, media_type, api_key)
        if vote_average is not None:
            tmdb_score = round(vote_average * 10)
            db.table("media").update({"tmdb_score": tmdb_score}).eq("tmdb_id", tmdb_id).execute()
            print(f"[TMDB_SCORE] {i}/{total} {title}: {tmdb_score}")
            updated += 1
        else:
            print(f"[TMDB_SCORE] {i}/{total} {title}: skipped (fetch failed)")

        time.sleep(0.2)

    print(f"[TMDB_SCORE] Done. {updated}/{total} rows updated.")


if __name__ == "__main__":
    main()
