"""
StreamStack Bot — entry point.

Orchestrates the nightly pipeline:
  1. Fetch currently playing / on-air titles from TMDB.
  2. Deduplicate against what is already stored in Supabase.
  3. Enrich NEW titles only with RT scores (OMDb) and streaming info (TMDB).
  4. Persist everything to Supabase.

Run locally (requires env vars to be set in the shell):
    python bot/main.py

In production this file is invoked by the GitHub Actions nightly workflow
(.github/workflows/nightly.yml) which injects secrets as environment variables.
"""

import os
import sys

from .tmdb import TmdbClient
from .omdb import OmdbClient
from .streaming import StreamingClient
from .supabase_client import SupabaseClient


def load_env() -> dict[str, str]:
    """
    Read required environment variables and exit early with a clear message if
    any are missing.  Failing fast here is preferable to an obscure error deep
    inside an API client.
    """
    required = ["TMDB_API_KEY", "OMDB_API_KEY", "SUPABASE_URL", "SUPABASE_KEY"]
    config: dict[str, str] = {}

    for key in required:
        value = os.environ.get(key)
        if not value:
            print(f"[BOT] ERROR: Required environment variable '{key}' is not set.")
            sys.exit(1)
        config[key] = value

    return config


def extract_year(release_date: str | None) -> str | None:
    """
    Pull the 4-digit year from a YYYY-MM-DD release_date string.
    Returns None when release_date is absent — OMDb still works without it,
    just with slightly higher risk of matching a wrong-year remake.
    """
    if release_date and len(release_date) >= 4:
        return release_date[:4]
    return None


def main() -> None:
    # ------------------------------------------------------------------ #
    # Initialisation                                                       #
    # ------------------------------------------------------------------ #
    print("[BOT] Starting StreamStack nightly bot...")
    config = load_env()

    tmdb = TmdbClient(api_key=config["TMDB_API_KEY"])
    omdb = OmdbClient(api_key=config["OMDB_API_KEY"])
    streaming = StreamingClient()
    db = SupabaseClient(url=config["SUPABASE_URL"], key=config["SUPABASE_KEY"])

    # ------------------------------------------------------------------ #
    # Step 1 — Fetch now-playing movies from TMDB                         #
    # ------------------------------------------------------------------ #
    print("\n[BOT] Step 1: Fetching now-playing movies from TMDB...")
    movies = tmdb.get_now_playing_movies(pages=3)
    print(f"[BOT] Fetched {len(movies)} movies.")

    # ------------------------------------------------------------------ #
    # Step 2 — Fetch on-air TV shows from TMDB                            #
    # ------------------------------------------------------------------ #
    print("\n[BOT] Step 2: Fetching on-air TV shows from TMDB...")
    shows = tmdb.get_on_air_tv(pages=3)
    print(f"[BOT] Fetched {len(shows)} TV shows.")

    # ------------------------------------------------------------------ #
    # Step 3 — Combine and deduplicate by tmdb_id                         #
    # ------------------------------------------------------------------ #
    print("\n[BOT] Step 3: Combining and deduplicating results...")
    combined: list[dict] = []
    seen_ids: set[int] = set()

    for item in movies + shows:
        tid = item["tmdb_id"]
        if tid not in seen_ids:
            seen_ids.add(tid)
            combined.append(item)

    print(f"[BOT] {len(combined)} unique titles after deduplication.")

    # ------------------------------------------------------------------ #
    # Step 4 — Load existing tmdb_ids from Supabase                       #
    # ------------------------------------------------------------------ #
    print("\n[BOT] Step 4: Loading existing tmdb_ids from Supabase...")
    existing_ids = db.get_existing_tmdb_ids()

    # Partition into new vs. already-stored titles
    new_items = [item for item in combined if item["tmdb_id"] not in existing_ids]
    skipped = len(combined) - len(new_items)
    print(f"[BOT] {len(new_items)} new titles to process, {skipped} already in DB.")

    # ------------------------------------------------------------------ #
    # Step 5 — Enrich and persist each NEW title                          #
    # ------------------------------------------------------------------ #
    print("\n[BOT] Step 5: Enriching and saving new titles...")
    total = len(new_items)

    for index, item in enumerate(new_items, start=1):
        title = item["title"]
        tmdb_id = item["tmdb_id"]
        media_type = item["media_type"]
        year = extract_year(item.get("release_date"))

        # --- RT score from OMDb (includes built-in 1s sleep) -----------
        rt_score = omdb.get_rt_score(title=title, year=year)

        # --- Streaming providers via TMDB watch/providers ---------------
        providers = streaming.get_streaming_providers(
            tmdb_id=tmdb_id,
            media_type=media_type,
            tmdb_client=tmdb,
        )

        # --- Build the media record to upsert ---------------------------
        media_record = {
            "tmdb_id": tmdb_id,
            "title": title,
            "overview": item.get("overview"),
            "poster_path": item.get("poster_path"),
            "media_type": media_type,
            "release_date": item.get("release_date"),
            "rt_score": rt_score,
            # is_in_theatres is always False in this MVP; a separate
            # cinemas-data source would be needed to populate this correctly
            "is_in_theatres": False,
            # A title is considered "streamable now" if at least one
            # subscription service carries it in the US
            "is_streamable_now": len(providers) > 0,
        }

        # --- Persist to Supabase ----------------------------------------
        media_id = db.upsert_media(media_record)

        if media_id and providers:
            db.upsert_streaming_availability(media_id=media_id, providers=providers)

        # --- Progress log -----------------------------------------------
        score_str = f"{rt_score}%" if rt_score is not None else "N/A"
        print(f"[BOT] Processed {index}/{total}: {title} ({score_str})")

    # ------------------------------------------------------------------ #
    # Step 6 — Summary                                                     #
    # ------------------------------------------------------------------ #
    print(f"\n[BOT] Done. {total} new titles added, {skipped} skipped (already exist).")


if __name__ == "__main__":
    main()
