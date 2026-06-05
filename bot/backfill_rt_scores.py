"""
One-time backfill: populate rt_score for all movies in public.media using OMDb.

TV shows are skipped — OMDb does not provide RT scores for series.

Run via:
    python -m bot.backfill_rt_scores
"""

import os
import sys
import time

from supabase import create_client

from bot.omdb import OmdbClient


def load_env() -> dict[str, str]:
    required = ["OMDB_API_KEY", "SUPABASE_URL", "SUPABASE_KEY"]
    config: dict[str, str] = {}
    for key in required:
        value = os.environ.get(key)
        if not value:
            print(f"[RT_BACKFILL] ERROR: Required environment variable '{key}' is not set.")
            sys.exit(1)
        config[key] = value
    return config


def extract_year(release_date: str | None) -> str | None:
    if release_date and len(release_date) >= 4:
        return release_date[:4]
    return None


def main() -> None:
    print("[RT_BACKFILL] Starting RT score backfill...")
    config = load_env()

    db = create_client(config["SUPABASE_URL"], config["SUPABASE_KEY"])
    omdb = OmdbClient(api_key=config["OMDB_API_KEY"])

    page_size = 1000
    offset = 0
    rows: list[dict] = []

    while True:
        response = (
            db.table("media")
            .select("id, tmdb_id, title, media_type, release_date")
            .eq("media_type", "movie")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        batch = response.data or []
        rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size

    total = len(rows)
    print(f"[RT_BACKFILL] {total} movies to process.")

    updated = 0
    not_found = 0

    for i, row in enumerate(rows, start=1):
        tmdb_id = row["tmdb_id"]
        title   = row["title"]
        year    = extract_year(row.get("release_date"))

        score = omdb.get_rt_score(title=title, year=year)

        if score is not None:
            db.table("media").update({"rt_score": score}).eq("tmdb_id", tmdb_id).execute()
            print(f"[RT_BACKFILL] {i}/{total} {title} ({year}): {score}%")
            updated += 1
        else:
            print(f"[RT_BACKFILL] {i}/{total} {title}: not found")
            not_found += 1

        time.sleep(0.5)

    print(f"[RT_BACKFILL] Done. {updated} updated, {not_found} not found, 0 skipped (TV).")


if __name__ == "__main__":
    main()
