"""
Bulk import: fetch titles from all TMDB endpoints and insert bare media rows
into Supabase — no enrichment (no WatchMode, OMDb, trailers, credits, seasons).

Enrichment is handled afterwards by the individual backfill scripts.

Run via:
    python -m bot.bulk_import                   # default batch_size=500
    python -m bot.bulk_import --batch-size 2000
"""

import os
import sys

from .tmdb import TmdbClient
from .supabase_client import SupabaseClient

DEFAULT_BATCH_SIZE = 500
MAX_BATCH_SIZE     = 2000


def load_env() -> dict[str, str]:
    required = ["TMDB_API_KEY", "SUPABASE_URL", "SUPABASE_KEY"]
    config: dict[str, str] = {}
    for key in required:
        value = os.environ.get(key)
        if not value:
            print(f"[BULK] ERROR: Required environment variable '{key}' is not set.")
            sys.exit(1)
        config[key] = value
    return config


def parse_batch_size(args: list[str]) -> int:
    if "--batch-size" in args:
        idx = args.index("--batch-size")
        if idx + 1 >= len(args):
            print("[BULK] ERROR: --batch-size requires a value.")
            sys.exit(1)
        try:
            size = int(args[idx + 1])
        except ValueError:
            print(f"[BULK] ERROR: --batch-size must be an integer, got '{args[idx + 1]}'.")
            sys.exit(1)
        if size < 1 or size > MAX_BATCH_SIZE:
            print(f"[BULK] ERROR: --batch-size must be between 1 and {MAX_BATCH_SIZE}.")
            sys.exit(1)
        return size
    return DEFAULT_BATCH_SIZE


def main() -> None:
    batch_size = parse_batch_size(sys.argv[1:])
    print(f"[BULK] Starting bulk import (batch_size={batch_size})...")

    config = load_env()
    tmdb = TmdbClient(api_key=config["TMDB_API_KEY"])
    db   = SupabaseClient(url=config["SUPABASE_URL"], key=config["SUPABASE_KEY"])

    # ------------------------------------------------------------------ #
    # Steps 1-6e — Fetch all TMDB endpoints                               #
    # ------------------------------------------------------------------ #
    print("[BULK] Fetching genre maps...")
    movie_genre_map = tmdb.get_genre_map("movie")
    tv_genre_map    = tmdb.get_genre_map("tv")

    print("\n[BULK] Step 1: now-playing movies...")
    now_playing = tmdb.get_now_playing_movies(pages=3, genre_map=movie_genre_map)

    print("\n[BULK] Step 2: on-air TV shows...")
    on_air = tmdb.get_on_air_tv(pages=3, genre_map=tv_genre_map)

    print("\n[BULK] Step 3: upcoming movies...")
    upcoming = tmdb.get_upcoming_movies(pages=3, genre_map=movie_genre_map)

    print("\n[BULK] Step 4: popular movies...")
    popular_movies = tmdb.get_popular_movies(pages=3, genre_map=movie_genre_map)

    print("\n[BULK] Step 5: popular TV shows...")
    popular_tv = tmdb.get_popular_tv(pages=3, genre_map=tv_genre_map)

    print("\n[BULK] Step 6: top-rated movies...")
    top_rated = tmdb.get_top_rated_movies(pages=15, genre_map=movie_genre_map)

    print("\n[BULK] Step 6b: discover movies (revenue)...")
    discover_movies_revenue = tmdb.get_discover_movies_by_revenue(pages=15, genre_map=movie_genre_map)

    print("\n[BULK] Step 6c: discover movies (vote count)...")
    discover_movies_votes = tmdb.get_discover_movies_by_vote_count(pages=15, genre_map=movie_genre_map)

    print("\n[BULK] Step 6d: top-rated TV shows...")
    top_rated_tv = tmdb.get_top_rated_tv(pages=15, genre_map=tv_genre_map)

    print("\n[BULK] Step 6e: discover TV shows (vote count)...")
    discover_tv_votes = tmdb.get_discover_tv_by_vote_count(pages=15, genre_map=tv_genre_map)

    # ------------------------------------------------------------------ #
    # Step 7 — Combine and deduplicate                                     #
    # ------------------------------------------------------------------ #
    print("\n[BULK] Step 7: Combining and deduplicating...")
    combined: list[dict] = []
    seen_ids: set[int] = set()

    all_sources = (
        now_playing + on_air + upcoming + popular_movies + popular_tv
        + top_rated + discover_movies_revenue + discover_movies_votes
        + top_rated_tv + discover_tv_votes
    )

    for item in all_sources:
        tid = item["tmdb_id"]
        if tid not in seen_ids:
            seen_ids.add(tid)
            combined.append(item)

    print(f"[BULK] {len(combined)} unique titles after deduplication.")

    # ------------------------------------------------------------------ #
    # Step 8 — Filter out titles already in DB                            #
    # ------------------------------------------------------------------ #
    print("\n[BULK] Step 8: Loading existing tmdb_ids from Supabase...")
    existing_ids = db.get_existing_tmdb_ids()

    new_items = [item for item in combined if item["tmdb_id"] not in existing_ids]
    skipped   = len(combined) - len(new_items)
    print(f"[BULK] {len(new_items)} new titles to insert, {skipped} already in DB.")

    # Apply batch size cap
    batch = new_items[:batch_size]
    if len(new_items) > batch_size:
        print(f"[BULK] Capping to batch_size={batch_size} ({len(new_items) - batch_size} deferred to next run).")

    # ------------------------------------------------------------------ #
    # Step 9 — Insert bare media rows (no enrichment)                     #
    # ------------------------------------------------------------------ #
    print(f"\n[BULK] Step 9: Inserting {len(batch)} bare media rows...")
    inserted = 0
    failed   = 0

    for index, item in enumerate(batch, start=1):
        media_record = {
            "tmdb_id":     item["tmdb_id"],
            "title":       item["title"],
            "overview":    item.get("overview"),
            "poster_path": item.get("poster_path"),
            "media_type":  item["media_type"],
            "release_date": item.get("release_date"),
            "tmdb_score":  item.get("tmdb_score", 0),
            "genre":       item.get("genre", ""),
            "genres":      [g for g in item.get("genre", "").split(", ") if g],
            "imdb_id":     item.get("imdb_id"),
            "popularity":  item.get("popularity", 0.0),
            "is_in_theatres":    False,
            "is_streamable_now": False,
        }

        try:
            db.upsert_media(media_record)
            inserted += 1
            if index % 50 == 0:
                print(f"[BULK] {index}/{len(batch)} inserted...")
        except Exception as exc:
            print(f"[BULK] Failed to insert '{item['title']}': {exc}")
            failed += 1

    # ------------------------------------------------------------------ #
    # Summary                                                              #
    # ------------------------------------------------------------------ #
    print(
        f"\n[BULK] Done."
        f"\n  Fetched from TMDB:  {len(combined)} unique titles"
        f"\n  Already in DB:      {skipped}"
        f"\n  Inserted:           {inserted}"
        f"\n  Failed:             {failed}"
        f"\n  Deferred (> batch): {max(0, len(new_items) - batch_size)}"
    )


if __name__ == "__main__":
    main()
