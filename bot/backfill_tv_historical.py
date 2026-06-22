"""
One-time backfill: discover and insert high-quality TV shows from 2000-2014
that are missing from the catalog.

Hits /discover/tv for two ranges (2000-2009, 2010-2014), up to 30 pages
each, filtered at the API level to vote_count >= 300, vote_average >= 7.0
(our tmdb_score >= 70), the major-language allowlist, and excluding
Kids/Talk/Soap genres. Inserts bare-ish rows the same way main.py Step 9
does (detail + watch providers, no seasons/episodes/credits/trailers) —
enrich_new_titles.py picks those up automatically the next morning since
it queries media created in the last 6 hours.

Run via:
    python -m bot.backfill_tv_historical
"""

import sys
import os

from .tmdb import TmdbClient
from .supabase_client import SupabaseClient, ALLOWED_PROVIDERS

# Mirrors _ALLOWED_LANGUAGES in bulk_import.py (source of truth) — duplicated
# here because that constant is function-local there, not importable.
_ALLOWED_LANGUAGES = {'en', 'es', 'fr', 'de', 'ko', 'ja', 'pt', 'it', 'zh'}

_YEAR_RANGES = [
    ("2000-01-01", "2009-12-31"),
    ("2010-01-01", "2014-12-31"),
]


def load_env() -> dict[str, str]:
    required = ["TMDB_API_KEY", "SUPABASE_URL", "SUPABASE_KEY"]
    config: dict[str, str] = {}
    for key in required:
        value = os.environ.get(key)
        if not value:
            print(f"[BACKFILL_TV] ERROR: Required environment variable '{key}' is not set.")
            sys.exit(1)
        config[key] = value
    return config


def main() -> None:
    print("[BACKFILL_TV] Starting historical TV backfill (2000-2014)...")
    config = load_env()

    tmdb = TmdbClient(api_key=config["TMDB_API_KEY"])
    db   = SupabaseClient(url=config["SUPABASE_URL"], key=config["SUPABASE_KEY"])

    print("[BACKFILL_TV] Fetching TV genre map...")
    tv_genre_map = tmdb.get_genre_map("tv")

    # ------------------------------------------------------------------ #
    # Step 1 — Fetch both year ranges from TMDB                           #
    # ------------------------------------------------------------------ #
    all_sources: list[dict] = []
    for date_gte, date_lte in _YEAR_RANGES:
        print(f"\n[BACKFILL_TV] Discovering TV shows {date_gte}..{date_lte}...")
        all_sources += tmdb.get_discover_tv_historical(
            date_gte=date_gte, date_lte=date_lte, pages=50, genre_map=tv_genre_map  # DIAGNOSTIC - revert after test
        )

    # ------------------------------------------------------------------ #
    # Step 2 — Dedupe by tmdb_id                                           #
    # ------------------------------------------------------------------ #
    print("\n[BACKFILL_TV] Deduplicating...")
    combined: list[dict] = []
    seen_ids: set[int] = set()
    for item in all_sources:
        tid = item["tmdb_id"]
        if tid not in seen_ids:
            seen_ids.add(tid)
            combined.append(item)
    print(f"[BACKFILL_TV] {len(combined)} unique titles fetched from TMDB.")

    # ------------------------------------------------------------------ #
    # Step 3 — Filter out titles already in DB                            #
    # ------------------------------------------------------------------ #
    print("\n[BACKFILL_TV] Loading existing tmdb_ids from Supabase...")
    existing_ids = db.get_existing_tmdb_ids()

    new_items = [item for item in combined if item["tmdb_id"] not in existing_ids]
    already_in_db = len(combined) - len(new_items)
    print(f"[BACKFILL_TV] {len(new_items)} new titles to process, {already_in_db} already in DB.")

    # ------------------------------------------------------------------ #
    # Step 4 — Enrich and persist each new title (same as main.py Step 9) #
    # ------------------------------------------------------------------ #
    total = len(new_items)
    inserted        = 0
    skipped_language = 0
    failed           = 0

    for index, item in enumerate(new_items, start=1):
        title   = item["title"]
        tmdb_id = item["tmdb_id"]

        # --- Detail fetch: status, is_limited_series, genres, etc. ------
        is_limited_series = None
        status            = None
        vote_count        = item.get("vote_count")
        original_language = item.get("original_language")
        is_documentary    = item.get("is_documentary", False)
        genres            = [g for g in item.get("genre", "").split(", ") if g]

        try:
            detail = tmdb._get(f"/tv/{tmdb_id}")
            is_limited_series = detail.get("type") == "Miniseries"
            status            = detail.get("status")
            vote_count        = detail.get("vote_count")
            original_language = detail.get("original_language")
            is_documentary    = 99 in [g["id"] for g in detail.get("genres", [])]
            genres            = [g["name"] for g in detail.get("genres", []) if g.get("name")]
        except Exception:
            pass

        # --- Language filter ---------------------------------------------
        if original_language not in _ALLOWED_LANGUAGES:
            print(f"[BACKFILL_TV] Skipping {title}: original_language={original_language} (language filter)")
            skipped_language += 1
            continue

        # --- Watch providers via TMDB watch/providers --------------------
        watch_providers = tmdb.get_watch_providers(tmdb_id=tmdb_id, media_type="tv")
        watch_providers = {
            kind: [p for p in names if p in ALLOWED_PROVIDERS]
            for kind, names in watch_providers.items()
        }
        is_streamable_now = bool(watch_providers.get("flatrate"))

        # --- Build and persist the media record -------------------------
        media_record = {
            "tmdb_id":      tmdb_id,
            "title":        title,
            "overview":     item.get("overview"),
            "poster_path":  item.get("poster_path"),
            "media_type":   "tv",
            "release_date":    item.get("release_date"),
            "us_release_date": None,
            "is_in_theatres":    False,
            "is_streamable_now": is_streamable_now,
            "popularity":   item.get("popularity", 0.0),
            "imdb_id":      item.get("imdb_id"),
            "runtime":      item.get("runtime"),
            "title_logo_url": tmdb.get_title_logo(tmdb_id=tmdb_id, media_type="tv"),
            "certification":  tmdb.get_certification(tmdb_id=tmdb_id, media_type="tv"),
            "genres":         genres,
            "vote_count":     vote_count,
            "status":         status,
            "original_language": original_language,
            "is_documentary":    is_documentary,
            "is_limited_series": is_limited_series,
        }

        try:
            media_id = db.upsert_media(media_record)
            if media_id:
                if any(watch_providers.values()):
                    db.upsert_streaming_availability(media_id=media_id, providers=watch_providers)
                db.update_streaming_last_checked(media_id)
            inserted += 1
        except Exception as exc:
            print(f"[BACKFILL_TV] Failed to insert '{title}': {exc}")
            failed += 1
            continue

        year = (item.get("release_date") or "")[:4] or "unknown"
        print(f"[BACKFILL_TV] {index}/{total}: {title} ({year})")

    # ------------------------------------------------------------------ #
    # Summary                                                              #
    # ------------------------------------------------------------------ #
    print(
        f"\n[BACKFILL_TV] Done."
        f"\n  Fetched from TMDB:          {len(combined)} unique titles"
        f"\n  Already in DB:              {already_in_db}"
        f"\n  New inserted:               {inserted}"
        f"\n  Skipped (language filter):  {skipped_language}"
        f"\n  Failed:                     {failed}"
    )


if __name__ == "__main__":
    main()
