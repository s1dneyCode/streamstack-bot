"""
One-time backfill: populate rt_score for all movies in public.media using OMDb.

TV shows are skipped — OMDb does not provide RT scores for series.

Phase 1: fill missing imdb_ids via TMDB external_ids endpoint.
Phase 2: update RT scores using OMDb, with imdb_id as the primary lookup strategy.

Run via:
    python -m bot.backfill_rt_scores
"""

import os
import sys
import time

from .tmdb import TmdbClient
from .omdb import OmdbClient
from .supabase_client import SupabaseClient


def load_env() -> dict[str, str]:
    required = ["TMDB_API_KEY", "OMDB_API_KEY", "SUPABASE_URL", "SUPABASE_KEY"]
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

    db   = SupabaseClient(url=config["SUPABASE_URL"], key=config["SUPABASE_KEY"])
    tmdb = TmdbClient(api_key=config["TMDB_API_KEY"])
    omdb = OmdbClient(api_key=config["OMDB_API_KEY"])

    # ------------------------------------------------------------------
    # Phase 1 — fill missing imdb_ids
    # ------------------------------------------------------------------
    missing = db.get_movies_without_imdb_id()
    print(f"[RT_BACKFILL] Phase 1: filling imdb_id for {len(missing)} movies...")

    for i, row in enumerate(missing, start=1):
        tmdb_id = row["tmdb_id"]
        title   = row["title"]

        imdb_id = tmdb.get_imdb_id(tmdb_id=tmdb_id, media_type="movie")
        if imdb_id:
            db.client.table("media").update({"imdb_id": imdb_id}).eq("tmdb_id", tmdb_id).execute()
            print(f"[RT_BACKFILL] {i}/{len(missing)} {title}: imdb_id={imdb_id}")
        else:
            print(f"[RT_BACKFILL] {i}/{len(missing)} {title}: imdb_id not found")

        time.sleep(0.2)

    # ------------------------------------------------------------------
    # Phase 2 — update RT scores
    # ------------------------------------------------------------------
    page_size = 1000
    offset = 0
    rows: list[dict] = []

    while True:
        response = (
            db.client.table("media")
            .select("id, tmdb_id, title, media_type, release_date, imdb_id")
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
    print(f"[RT_BACKFILL] Phase 2: updating RT scores for {total} movies...")

    updated = 0
    not_found = 0

    for i, row in enumerate(rows, start=1):
        tmdb_id = row["tmdb_id"]
        title   = row["title"]
        year    = extract_year(row.get("release_date"))
        imdb_id = row.get("imdb_id")

        score = omdb.get_rt_score(title=title, year=year, imdb_id=imdb_id)

        if score is not None:
            db.client.table("media").update({"rt_score": score}).eq("tmdb_id", tmdb_id).execute()
            print(f"[RT_BACKFILL] {i}/{total} {title} ({year}): {score}%")
            updated += 1
        else:
            print(f"[RT_BACKFILL] {i}/{total} {title}: not found")
            not_found += 1

        time.sleep(0.5)

    print(f"[RT_BACKFILL] Done. {updated} updated, {not_found} not found, 0 skipped (TV).")


if __name__ == "__main__":
    main()
