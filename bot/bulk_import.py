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
from datetime import date, timedelta

from .tmdb import TmdbClient
from .supabase_client import SupabaseClient

DEFAULT_BATCH_SIZE = 500
MAX_BATCH_SIZE     = 10000

# Japanese is fetched separately from the rest in the historical TV discover —
# get_discover_tv_historical() applies a higher vote_count.gte (300 vs 150)
# for 'ja' since anime accumulates TMDB votes more slowly than Western/Korean
# content.
_NON_JA_LANGUAGES = "en|es|fr|de|ko|pt|it|zh"


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
    top_rated = tmdb.get_top_rated_movies(pages=100, genre_map=movie_genre_map)

    print("\n[BULK] Step 6b: discover movies (revenue)...")
    discover_movies_revenue = tmdb.get_discover_movies_by_revenue(pages=100, genre_map=movie_genre_map)

    print("\n[BULK] Step 6c: discover movies (vote count)...")
    discover_movies_votes = tmdb.get_discover_movies_by_vote_count(pages=100, genre_map=movie_genre_map)

    print("\n[BULK] Step 6d: top-rated TV shows...")
    top_rated_tv = tmdb.get_top_rated_tv(pages=100, genre_map=tv_genre_map)

    print("\n[BULK] Step 6e: discover TV shows (vote count)...")
    discover_tv_votes = tmdb.get_discover_tv_by_vote_count(pages=100, genre_map=tv_genre_map)

    # Genre-based movie endpoints (30 pages each)
    print("\n[BULK] Step 6f: discover movies by genre...")
    genre_movies_action    = tmdb.get_discover_movies_by_genre(28,    "Action",    pages=30, genre_map=movie_genre_map)
    genre_movies_comedy    = tmdb.get_discover_movies_by_genre(35,    "Comedy",    pages=30, genre_map=movie_genre_map)
    genre_movies_drama     = tmdb.get_discover_movies_by_genre(18,    "Drama",     pages=30, genre_map=movie_genre_map)
    genre_movies_horror    = tmdb.get_discover_movies_by_genre(27,    "Horror",    pages=30, genre_map=movie_genre_map)
    genre_movies_scifi     = tmdb.get_discover_movies_by_genre(878,   "Sci-Fi",    pages=30, genre_map=movie_genre_map)
    genre_movies_romance   = tmdb.get_discover_movies_by_genre(10749, "Romance",   pages=30, genre_map=movie_genre_map)
    genre_movies_thriller  = tmdb.get_discover_movies_by_genre(53,    "Thriller",  pages=30, genre_map=movie_genre_map)
    genre_movies_animation = tmdb.get_discover_movies_by_genre(16,    "Animation", pages=30, genre_map=movie_genre_map)

    # Genre-based TV endpoints (30 pages each)
    print("\n[BULK] Step 6g: discover TV shows by genre...")
    genre_tv_drama     = tmdb.get_discover_tv_by_genre(18,    "Drama",              pages=30, genre_map=tv_genre_map)
    genre_tv_comedy    = tmdb.get_discover_tv_by_genre(35,    "Comedy",             pages=30, genre_map=tv_genre_map)
    genre_tv_scifi     = tmdb.get_discover_tv_by_genre(10765, "Sci-Fi & Fantasy",   pages=30, genre_map=tv_genre_map)
    genre_tv_mystery   = tmdb.get_discover_tv_by_genre(9648,  "Mystery",            pages=30, genre_map=tv_genre_map)
    genre_tv_action    = tmdb.get_discover_tv_by_genre(10759, "Action & Adventure", pages=30, genre_map=tv_genre_map)
    genre_tv_animation = tmdb.get_discover_tv_by_genre(16,    "Animation",          pages=30, genre_map=tv_genre_map)

    # Year-based movie endpoints (30 pages each)
    print("\n[BULK] Step 6h: discover movies by year...")
    year_movies_2023 = tmdb.get_discover_movies_by_year(2023, pages=30, genre_map=movie_genre_map)
    year_movies_2022 = tmdb.get_discover_movies_by_year(2022, pages=30, genre_map=movie_genre_map)
    year_movies_2021 = tmdb.get_discover_movies_by_year(2021, pages=30, genre_map=movie_genre_map)
    year_movies_2020 = tmdb.get_discover_movies_by_year(2020, pages=30, genre_map=movie_genre_map)
    year_movies_2019 = tmdb.get_discover_movies_by_year(2019, pages=30, genre_map=movie_genre_map)
    year_movies_2018 = tmdb.get_discover_movies_by_year(2018, pages=30, genre_map=movie_genre_map)

    # Year-based TV endpoints (30 pages each)
    print("\n[BULK] Step 6i: discover TV shows by year...")
    year_tv_2023 = tmdb.get_discover_tv_by_year(2023, pages=30, genre_map=tv_genre_map)
    year_tv_2022 = tmdb.get_discover_tv_by_year(2022, pages=30, genre_map=tv_genre_map)
    year_tv_2021 = tmdb.get_discover_tv_by_year(2021, pages=30, genre_map=tv_genre_map)
    year_tv_2020 = tmdb.get_discover_tv_by_year(2020, pages=30, genre_map=tv_genre_map)
    year_tv_2019 = tmdb.get_discover_tv_by_year(2019, pages=30, genre_map=tv_genre_map)
    year_tv_2018 = tmdb.get_discover_tv_by_year(2018, pages=30, genre_map=tv_genre_map)

    # Historical TV (2000-2014), split by language: Japanese gets a higher
    # vote_count.gte floor since anime accumulates votes more slowly.
    print("\n[BULK] Step 6j: discover historical TV shows (2000-2014, non-Japanese)...")
    historical_tv_non_ja = tmdb.get_discover_tv_historical(
        date_gte="2000-01-01", date_lte="2014-12-31",
        with_original_language=_NON_JA_LANGUAGES, pages=50, genre_map=tv_genre_map,
    )

    print("\n[BULK] Step 6k: discover historical TV shows (2000-2014, Japanese)...")
    historical_tv_ja = tmdb.get_discover_tv_historical(
        date_gte="2000-01-01", date_lte="2014-12-31",
        with_original_language="ja", pages=50, genre_map=tv_genre_map,
    )

    # Provider-based discover (US, flatrate only, 50 pages each, movie + tv
    # per provider)
    print("\n[BULK] Step 6l: discover movies on Netflix...")
    provider_movies_netflix = tmdb.get_discover_by_provider("movie", 8, pages=50, genre_map=movie_genre_map)

    print("\n[BULK] Step 6m: discover TV shows on Netflix...")
    provider_tv_netflix = tmdb.get_discover_by_provider("tv", 8, pages=50, genre_map=tv_genre_map)

    print("\n[BULK] Step 6n: discover movies on Amazon Prime...")
    provider_movies_amazon = tmdb.get_discover_by_provider("movie", 9, pages=50, genre_map=movie_genre_map)

    print("\n[BULK] Step 6o: discover TV shows on Amazon Prime...")
    provider_tv_amazon = tmdb.get_discover_by_provider("tv", 9, pages=50, genre_map=tv_genre_map)

    print("\n[BULK] Step 6p: discover movies on Disney Plus...")
    provider_movies_disney = tmdb.get_discover_by_provider("movie", 337, pages=50, genre_map=movie_genre_map)

    print("\n[BULK] Step 6q: discover TV shows on Disney Plus...")
    provider_tv_disney = tmdb.get_discover_by_provider("tv", 337, pages=50, genre_map=tv_genre_map)

    print("\n[BULK] Step 6r: discover movies on Hulu...")
    provider_movies_hulu = tmdb.get_discover_by_provider("movie", 15, pages=50, genre_map=movie_genre_map)

    print("\n[BULK] Step 6s: discover TV shows on Hulu...")
    provider_tv_hulu = tmdb.get_discover_by_provider("tv", 15, pages=50, genre_map=tv_genre_map)

    print("\n[BULK] Step 6t: discover movies on HBO Max...")
    provider_movies_hbomax = tmdb.get_discover_by_provider("movie", 1899, pages=50, genre_map=movie_genre_map)

    print("\n[BULK] Step 6u: discover TV shows on HBO Max...")
    provider_tv_hbomax = tmdb.get_discover_by_provider("tv", 1899, pages=50, genre_map=tv_genre_map)

    print("\n[BULK] Step 6v: discover movies on Apple TV Plus...")
    provider_movies_appletv = tmdb.get_discover_by_provider("movie", 350, pages=50, genre_map=movie_genre_map)

    print("\n[BULK] Step 6w: discover TV shows on Apple TV Plus...")
    provider_tv_appletv = tmdb.get_discover_by_provider("tv", 350, pages=50, genre_map=tv_genre_map)

    print("\n[BULK] Step 6x: discover movies on Paramount Plus...")
    provider_movies_paramount = tmdb.get_discover_by_provider("movie", 531, pages=50, genre_map=movie_genre_map)

    print("\n[BULK] Step 6y: discover TV shows on Paramount Plus...")
    provider_tv_paramount = tmdb.get_discover_by_provider("tv", 531, pages=50, genre_map=tv_genre_map)

    print("\n[BULK] Step 6z: discover movies on Peacock...")
    provider_movies_peacock = tmdb.get_discover_by_provider("movie", 386, pages=50, genre_map=movie_genre_map)

    print("\n[BULK] Step 6aa: discover TV shows on Peacock...")
    provider_tv_peacock = tmdb.get_discover_by_provider("tv", 386, pages=50, genre_map=tv_genre_map)

    print("\n[BULK] Step 6ab: discover movies on Crunchyroll...")
    provider_movies_crunchyroll = tmdb.get_discover_by_provider("movie", 283, pages=50, genre_map=movie_genre_map)

    print("\n[BULK] Step 6ac: discover TV shows on Crunchyroll...")
    provider_tv_crunchyroll = tmdb.get_discover_by_provider("tv", 283, pages=50, genre_map=tv_genre_map)

    # HIDIVE is TV/anime only — no movie catalog
    print("\n[BULK] Step 6ad: discover TV shows on HIDIVE...")
    provider_tv_hidive = tmdb.get_discover_by_provider("tv", 430, pages=50, genre_map=tv_genre_map)

    # Language-based discover — vote_count.desc, vote_average >= 7.0, vote_count >= 100
    print("\n[BULK] Step 6ae: discover Korean TV shows (by language)...")
    lang_tv_ko = tmdb.get_discover_by_language("tv", "ko", pages=50, genre_map=tv_genre_map)

    print("\n[BULK] Step 6af: discover Korean movies (by language)...")
    lang_movies_ko = tmdb.get_discover_by_language("movie", "ko", pages=30, genre_map=movie_genre_map)

    print("\n[BULK] Step 6ag: discover Spanish TV shows (by language)...")
    lang_tv_es = tmdb.get_discover_by_language("tv", "es", pages=30, genre_map=tv_genre_map)

    print("\n[BULK] Step 6ah: discover Japanese TV shows (by language)...")
    lang_tv_ja = tmdb.get_discover_by_language("tv", "ja", pages=30, genre_map=tv_genre_map)

    print("\n[BULK] Step 6ai: discover French TV shows (by language)...")
    lang_tv_fr = tmdb.get_discover_by_language("tv", "fr", pages=20, genre_map=tv_genre_map)

    print("\n[BULK] Step 6aj: discover German TV shows (by language)...")
    lang_tv_de = tmdb.get_discover_by_language("tv", "de", pages=20, genre_map=tv_genre_map)

    print("\n[BULK] Step 6ak: discover Italian TV shows (by language)...")
    lang_tv_it = tmdb.get_discover_by_language("tv", "it", pages=20, genre_map=tv_genre_map)

    print("\n[BULK] Step 6al: discover Spanish movies (by language)...")
    lang_movies_es = tmdb.get_discover_by_language("movie", "es", pages=20, genre_map=movie_genre_map)

    print("\n[BULK] Step 6am: discover French movies (by language)...")
    lang_movies_fr = tmdb.get_discover_by_language("movie", "fr", pages=20, genre_map=movie_genre_map)

    print("\n[BULK] Step 6an: discover German movies (by language)...")
    lang_movies_de = tmdb.get_discover_by_language("movie", "de", pages=20, genre_map=movie_genre_map)

    print("\n[BULK] Step 6ao: discover Italian movies (by language)...")
    lang_movies_it = tmdb.get_discover_by_language("movie", "it", pages=20, genre_map=movie_genre_map)

    print("\n[BULK] Step 6ap: discover Filipino TV shows (by language)...")
    lang_tv_tl = tmdb.get_discover_by_language("tv", "tl", pages=20, genre_map=tv_genre_map)

    print("\n[BULK] Step 6aq: discover Filipino movies (by language)...")
    lang_movies_tl = tmdb.get_discover_by_language("movie", "tl", pages=20, genre_map=movie_genre_map)

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
        + genre_movies_action + genre_movies_comedy + genre_movies_drama
        + genre_movies_horror + genre_movies_scifi + genre_movies_romance
        + genre_movies_thriller + genre_movies_animation
        + genre_tv_drama + genre_tv_comedy + genre_tv_scifi
        + genre_tv_mystery + genre_tv_action + genre_tv_animation
        + year_movies_2023 + year_movies_2022 + year_movies_2021
        + year_movies_2020 + year_movies_2019 + year_movies_2018
        + year_tv_2023 + year_tv_2022 + year_tv_2021
        + year_tv_2020 + year_tv_2019 + year_tv_2018
        + historical_tv_non_ja + historical_tv_ja
        + provider_movies_netflix + provider_tv_netflix
        + provider_movies_amazon + provider_tv_amazon
        + provider_movies_disney + provider_tv_disney
        + provider_movies_hulu + provider_tv_hulu
        + provider_movies_hbomax + provider_tv_hbomax
        + provider_movies_appletv + provider_tv_appletv
        + provider_movies_paramount + provider_tv_paramount
        + provider_movies_peacock + provider_tv_peacock
        + provider_movies_crunchyroll + provider_tv_crunchyroll
        + provider_tv_hidive
        + lang_tv_ko + lang_movies_ko
        + lang_tv_es + lang_tv_ja + lang_tv_fr + lang_tv_de + lang_tv_it
        + lang_movies_es + lang_movies_fr + lang_movies_de + lang_movies_it
        + lang_tv_tl + lang_movies_tl
    )

    for item in all_sources:
        tid = item["tmdb_id"]
        if tid not in seen_ids:
            seen_ids.add(tid)
            combined.append(item)

    print(f"[BULK] {len(combined)} unique titles after deduplication.")

    # ------------------------------------------------------------------ #
    # Step 7b — Quality filters (language + tiered vote_count)            #
    # ------------------------------------------------------------------ #
    _ALLOWED_LANGUAGES = {'en', 'es', 'fr', 'de', 'ko', 'ja', 'pt', 'it', 'zh', 'tl'}
    _CUTOFF_DATE = date.today() - timedelta(days=365)
    _HISTORICAL_TV_EXCLUDED_GENRES = {'Kids', 'Soap', 'Talk'}

    def _passes_filters(item: dict) -> bool:
        lang = item.get("original_language")
        if lang not in _ALLOWED_LANGUAGES:
            return False

        votes = item.get("vote_count") or 0
        release = item.get("release_date")
        release_year = None
        is_recent = False
        if release:
            try:
                release_date_obj = date.fromisoformat(release)
                release_year = release_date_obj.year
                is_recent = release_date_obj >= _CUTOFF_DATE
            except ValueError:
                pass

        # Historical TV (pre-2015) needs a higher vote_count floor than the
        # standard recent/older tiers — and Japanese needs an even higher one
        # since anime accumulates TMDB votes more slowly. Also excludes
        # Kids/Soap/Talk, mirroring the without_genres filter already applied
        # at the API level in get_discover_tv_historical().
        is_historical_tv = item.get("media_type") == "tv" and release_year is not None and release_year < 2015

        if is_historical_tv:
            if lang == "ja":
                if votes < 300:
                    return False
            elif lang == "ko":
                tmdb_score = item.get("tmdb_score") or 0
                if not (votes >= 200 or (votes >= 50 and tmdb_score >= 75)):
                    return False
            elif lang == "tl":
                tmdb_score = item.get("tmdb_score") or 0
                if not (votes >= 200 or (votes >= 50 and tmdb_score >= 75)):
                    return False
            else:
                if votes < 150:
                    return False
            genre_list = [g for g in item.get("genre", "").split(", ") if g]
            if any(g in _HISTORICAL_TV_EXCLUDED_GENRES for g in genre_list):
                return False
        else:
            if lang == "ko":
                tmdb_score = item.get("tmdb_score") or 0
                if not (votes >= 200 or (votes >= 50 and tmdb_score >= 75)):
                    return False
            elif lang == "tl":
                tmdb_score = item.get("tmdb_score") or 0
                if not (votes >= 200 or (votes >= 50 and tmdb_score >= 75)):
                    return False
            else:
                threshold = 100 if is_recent else 200
                if votes < threshold:
                    return False

        if item.get("media_type") == "movie":
            runtime = item.get("runtime")
            if runtime is not None and runtime < 40:
                return False
        return True

    before_filter = len(combined)
    combined = [item for item in combined if _passes_filters(item)]
    removed = before_filter - len(combined)
    print(f"[BULK] {len(combined)} titles after quality filters ({removed} removed).")

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
