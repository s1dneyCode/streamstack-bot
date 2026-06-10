"""
One-time backfill: fetch and store seasons, episodes, and season posters
for all TV shows that currently have no entries in media_seasons.

Run via:
    python -m bot.backfill_seasons
"""

import os
import sys
import time

import requests

from .tmdb import TmdbClient
from .supabase_client import SupabaseClient

POSTER_BASE = "https://image.tmdb.org/t/p/w500"
BUCKET      = "media-images"


def load_env() -> dict[str, str]:
    required = ["TMDB_API_KEY", "SUPABASE_URL", "SUPABASE_KEY"]
    config: dict[str, str] = {}
    for key in required:
        value = os.environ.get(key)
        if not value:
            print(f"[SEASONS] ERROR: Required environment variable '{key}' is not set.")
            sys.exit(1)
        config[key] = value
    return config


def download_image(url: str) -> bytes | None:
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        return r.content
    except Exception as exc:
        print(f"[SEASONS] Warning: failed to download {url}: {exc}")
        return None


def main() -> None:
    print("[SEASONS] Starting seasons backfill...")
    config = load_env()

    db   = SupabaseClient(url=config["SUPABASE_URL"], key=config["SUPABASE_KEY"])
    tmdb = TmdbClient(api_key=config["TMDB_API_KEY"])

    # Collect media_ids that already have at least one season row
    season_rows = db.client.table("media_seasons").select("media_id").execute().data or []
    has_seasons: set[str] = {row["media_id"] for row in season_rows}

    # Fetch all TV shows (paginated)
    page_size = 1000
    offset    = 0
    all_tv: list[dict] = []
    while True:
        batch = (
            db.client.table("media")
            .select("id, tmdb_id, title")
            .eq("media_type", "tv")
            .range(offset, offset + page_size - 1)
            .execute()
            .data or []
        )
        all_tv.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size

    needs_seasons = [row for row in all_tv if row["id"] not in has_seasons]
    total = len(needs_seasons)
    print(f"[SEASONS] {total} TV shows need seasons backfill.")

    total_seasons  = 0
    total_episodes = 0
    total_posters  = 0
    total_runtimes = 0

    for i, show in enumerate(needs_seasons, start=1):
        tmdb_id  = show["tmdb_id"]
        media_id = show["id"]
        title    = show["title"]

        seasons_raw = tmdb.get_seasons(tmdb_id=tmdb_id)
        if not seasons_raw:
            print(f"[SEASONS] {i}/{total} {title}: no seasons found")
            time.sleep(0.5)
            continue

        # Upload season posters to Supabase Storage
        posters_uploaded = 0
        for s in seasons_raw:
            poster_path = s.get("poster_path", "")
            if poster_path:
                img = download_image(f"{POSTER_BASE}{poster_path}")
                if img:
                    url = db.upload_image(
                        BUCKET,
                        f"seasons/{tmdb_id}/{s['season_number']}.jpg",
                        img,
                    )
                    if url:
                        s["poster_url"] = url
                        posters_uploaded += 1

        # Upsert seasons
        season_rows_inserted = db.upsert_seasons(media_id=media_id, seasons=seasons_raw)
        season_map = {r["season_number"]: r["id"] for r in season_rows_inserted}
        total_seasons  += len(season_rows_inserted)
        total_posters  += posters_uploaded

        # Upsert episodes for each season
        episodes_for_show = 0
        for s in seasons_raw:
            season_id = season_map.get(s["season_number"])
            if not season_id:
                continue
            episodes = tmdb.get_season_episodes(
                tmdb_id=tmdb_id, season_number=s["season_number"]
            )
            n = db.upsert_episodes(season_id=season_id, episodes=episodes)
            episodes_for_show  += n
            time.sleep(0.25)

        total_episodes += episodes_for_show

        # Update TV runtime average
        avg = db.update_tv_runtime(media_id=media_id)
        if avg is not None:
            total_runtimes += 1

        print(
            f"[SEASONS] {i}/{total} {title}: "
            f"{len(season_rows_inserted)} seasons, {episodes_for_show} episodes, "
            f"{posters_uploaded} posters, runtime={avg or 'n/a'}"
        )
        time.sleep(0.5)

    print(
        f"[SEASONS] Done. {total} shows processed — "
        f"{total_seasons} seasons, {total_episodes} episodes inserted, "
        f"{total_posters} posters uploaded, {total_runtimes} runtimes updated."
    )


if __name__ == "__main__":
    main()
