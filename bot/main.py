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
import time
from datetime import date

from .tmdb import TmdbClient
from .omdb import OmdbClient
from .streaming import StreamingClient
from .supabase_client import SupabaseClient
from .migrate_images import migrate_poster, migrate_carousel


def load_env() -> dict[str, str]:
    """
    Read required environment variables and exit early with a clear message if
    any are missing.  Failing fast here is preferable to an obscure error deep
    inside an API client.
    """
    required = ["TMDB_API_KEY", "OMDB_API_KEY", "SUPABASE_URL", "SUPABASE_KEY", "WATCHMODE_API_KEY"]
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
    streaming = StreamingClient(api_key=config["WATCHMODE_API_KEY"])
    db = SupabaseClient(url=config["SUPABASE_URL"], key=config["SUPABASE_KEY"])

    print("[BOT] Fetching genre maps from TMDB...")
    movie_genre_map = tmdb.get_genre_map('movie')
    tv_genre_map    = tmdb.get_genre_map('tv')

    # ------------------------------------------------------------------ #
    # Step 1 — Fetch now-playing movies from TMDB                         #
    # ------------------------------------------------------------------ #
    print("\n[BOT] Step 1: Fetching now-playing movies from TMDB...")
    now_playing = tmdb.get_now_playing_movies(pages=3, genre_map=movie_genre_map)
    print(f"[BOT] Fetched {len(now_playing)} now-playing movies.")

    # ------------------------------------------------------------------ #
    # Step 2 — Fetch on-air TV shows from TMDB                            #
    # ------------------------------------------------------------------ #
    print("\n[BOT] Step 2: Fetching on-air TV shows from TMDB...")
    on_air = tmdb.get_on_air_tv(pages=3, genre_map=tv_genre_map)
    print(f"[BOT] Fetched {len(on_air)} on-air TV shows.")

    # ------------------------------------------------------------------ #
    # Step 3 — Fetch upcoming movies from TMDB                            #
    # ------------------------------------------------------------------ #
    print("\n[BOT] Step 3: Fetching upcoming movies from TMDB...")
    upcoming = tmdb.get_upcoming_movies(pages=3, genre_map=movie_genre_map)
    print(f"[BOT] Fetched {len(upcoming)} upcoming movies.")

    # ------------------------------------------------------------------ #
    # Step 4 — Fetch popular movies from TMDB                             #
    # ------------------------------------------------------------------ #
    print("\n[BOT] Step 4: Fetching popular movies from TMDB...")
    popular_movies = tmdb.get_popular_movies(pages=3, genre_map=movie_genre_map)
    print(f"[BOT] Fetched {len(popular_movies)} popular movies.")

    # ------------------------------------------------------------------ #
    # Step 5 — Fetch popular TV shows from TMDB                           #
    # ------------------------------------------------------------------ #
    print("\n[BOT] Step 5: Fetching popular TV shows from TMDB...")
    popular_tv = tmdb.get_popular_tv(pages=3, genre_map=tv_genre_map)
    print(f"[BOT] Fetched {len(popular_tv)} popular TV shows.")

    # ------------------------------------------------------------------ #
    # Step 6 — Fetch top-rated movies from TMDB                           #
    # ------------------------------------------------------------------ #
    print("\n[BOT] Step 6: Fetching top-rated movies from TMDB...")
    top_rated = tmdb.get_top_rated_movies(pages=2, genre_map=movie_genre_map)
    print(f"[BOT] Fetched {len(top_rated)} top-rated movies.")

    # ------------------------------------------------------------------ #
    # Step 7 — Combine and deduplicate by tmdb_id                         #
    # ------------------------------------------------------------------ #
    print("\n[BOT] Step 7: Combining and deduplicating results...")
    combined: list[dict] = []
    seen_ids: set[int] = set()

    for item in now_playing + on_air + upcoming + popular_movies + popular_tv + top_rated:
        tid = item["tmdb_id"]
        if tid not in seen_ids:
            seen_ids.add(tid)
            combined.append(item)

    print(f"[BOT] {len(combined)} unique titles after deduplication.")

    # ------------------------------------------------------------------ #
    # Step 8 — Load existing tmdb_ids from Supabase                       #
    # ------------------------------------------------------------------ #
    print("\n[BOT] Step 8: Loading existing tmdb_ids from Supabase...")
    existing_ids = db.get_existing_tmdb_ids()

    # Partition into new vs. already-stored titles
    new_items = [item for item in combined if item["tmdb_id"] not in existing_ids]
    skipped = len(combined) - len(new_items)
    print(f"[BOT] {len(new_items)} new titles to process, {skipped} already in DB.")

    # ------------------------------------------------------------------ #
    # Step 9 — Enrich and persist each NEW title                          #
    # ------------------------------------------------------------------ #
    print("\n[BOT] Step 9: Enriching and saving new titles...")
    total = len(new_items)

    for index, item in enumerate(new_items, start=1):
        title = item["title"]
        tmdb_id = item["tmdb_id"]
        media_type = item["media_type"]
        year = extract_year(item.get("release_date"))

        # --- RT score from OMDb (includes built-in 1s sleep) -----------
        rt_score = omdb.get_rt_score(title=title, year=year, imdb_id=item.get('imdb_id'))

        # --- Streaming providers via TMDB watch/providers ---------------
        # Fetched before the upsert so is_streamable_now is set correctly
        providers = streaming.get_streaming_providers(
            tmdb_id=tmdb_id,
            media_type=media_type,
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
            # is_in_theatres is always False for now
            "is_in_theatres": False,
            # A title is considered "streamable now" if at least one
            # subscription service carries it in the US
            "is_streamable_now": len(providers) > 0,
            "popularity": item.get("popularity", 0.0),
            "imdb_id": item.get("imdb_id"),
            "runtime": item.get("runtime"),
            "title_logo_url": tmdb.get_title_logo(tmdb_id=tmdb_id, media_type=media_type),
        }

        # --- Persist media row to Supabase ------------------------------
        db.upsert_media(media_record)

        # --- Fetch Supabase UUID then save streaming availability -------
        # Query the UUID after upsert rather than relying on the upsert
        # return value, which is not reliable across supabase-py versions
        result = (
            db.client.table("media")
            .select("id")
            .eq("tmdb_id", tmdb_id)
            .single()
            .execute()
        )
        media_uuid = result.data.get("id") if result.data else None

        print(f"[BOT] Providers for {title}: {providers}")

        if providers and media_uuid:
            db.upsert_streaming_availability(media_id=media_uuid, providers=providers)

        # --- Image migration: poster and carousel -----------------------
        try:
            migrate_poster(db, tmdb_id, title, item.get("poster_path", ""))
            migrate_carousel(db, tmdb, tmdb_id, title, media_type)
        except Exception as exc:
            print(f"[BOT] Image migration failed for {title}: {exc}")

        # --- Progress log -----------------------------------------------
        score_str = f"{rt_score}%" if rt_score is not None else "N/A"
        print(f"[BOT] Processed {index}/{total}: {title} ({score_str})")

    # ------------------------------------------------------------------ #
    # Step 10 — Re-verify streaming providers for existing titles         #
    # ------------------------------------------------------------------ #
    today = date.today()
    reverify_list = db.get_titles_to_reverify(today)
    print(f"\n[BOT] Step 10: Re-verifying streaming providers for {len(reverify_list)} existing titles...")

    reverified = 0
    for item in reverify_list:
        media_id   = item["id"]
        tmdb_id    = item["tmdb_id"]
        title      = item["title"]
        media_type = item["media_type"]

        providers = streaming.get_streaming_providers(tmdb_id=tmdb_id, media_type=media_type)

        if providers:
            # Only replace existing data when we have confirmed new results —
            # prevents wiping providers due to API failures or empty responses
            db.delete_streaming_providers(media_id)
            db.upsert_streaming_availability(media_id=media_id, providers=providers)

        db.update_streaming_last_checked(media_id)
        reverified += 1

        print(f"[BOT] Re-verified {title}: {providers if providers else '(no results — kept existing data)'}")

    print(f"[BOT] Re-verification done. {reverified} titles updated.")

    # ------------------------------------------------------------------ #
    # Step 10b — Update popularity for titles due for re-verification      #
    # ------------------------------------------------------------------ #
    print(f"\n[BOT] Step 10b: Updating popularity for {len(reverify_list)} titles...")

    for item in reverify_list:
        tmdb_id    = item["tmdb_id"]
        title      = item["title"]
        media_type = item["media_type"]

        try:
            detail = tmdb._get(f"/{media_type}/{tmdb_id}")
            popularity = detail.get("popularity", 0.0)
            db.client.table("media").update({"popularity": popularity}).eq("tmdb_id", tmdb_id).execute()
            print(f"[BOT] Updated popularity for {title}: {popularity}")
        except Exception as exc:
            print(f"[BOT] Failed to update popularity for {title}: {exc}")

        time.sleep(0.3)

    # ------------------------------------------------------------------ #
    # Step 11 — Update RT scores for movies due for re-verification       #
    # ------------------------------------------------------------------ #
    movies_to_update = [item for item in reverify_list if item["media_type"] == "movie"]
    print(f"\n[BOT] Step 11: Updating RT scores for {len(movies_to_update)} movies...")

    rt_updated = 0
    for item in movies_to_update:
        tmdb_id = item["tmdb_id"]
        title   = item["title"]
        year    = extract_year(item.get("release_date"))

        current = (
            db.client.table("media")
            .select("rt_score, imdb_id")
            .eq("tmdb_id", tmdb_id)
            .single()
            .execute()
        )
        current_score = current.data.get("rt_score") if current.data else None
        imdb_id = current.data.get("imdb_id") if current.data else None

        score = omdb.get_rt_score(title=title, year=year, imdb_id=imdb_id)

        if score is not None and score != current_score:
            db.client.table("media").update({"rt_score": score}).eq("tmdb_id", tmdb_id).execute()
            print(f"[BOT] RT score {title}: {score}%")
            rt_updated += 1

        time.sleep(0.5)

    print(f"[BOT] Step 11 done. {rt_updated} RT scores updated.")

    # ------------------------------------------------------------------ #
    # Step 11b — Retry RT scores for released movies with no score        #
    # ------------------------------------------------------------------ #
    missing_rt = db.get_movies_missing_rt_score(today)
    print(f"\n[BOT] Step 11b: Retrying RT scores for {len(missing_rt)} released movies with no score...")

    rt_recovered = 0
    for item in missing_rt:
        tmdb_id = item["tmdb_id"]
        title   = item["title"]
        year    = extract_year(item.get("release_date"))
        imdb_id = item.get("imdb_id")

        score = omdb.get_rt_score(title=title, year=year, imdb_id=imdb_id)

        if score is not None:
            db.client.table("media").update({"rt_score": score}).eq("tmdb_id", tmdb_id).execute()
            db.update_streaming_last_checked(item["id"])
            print(f"[BOT] Step 11b {title}: found {score}%")
            rt_recovered += 1
        else:
            db.update_streaming_last_checked(item["id"])
            print(f"[BOT] Step 11b {title}: still not found")

        time.sleep(0.5)

    print(f"[BOT] Step 11b done. {rt_recovered} new RT scores found.")

    # ------------------------------------------------------------------ #
    # Step 12 — Summary                                                    #
    # ------------------------------------------------------------------ #
    print(f"\n[BOT] Done. {total} new titles added, {skipped} skipped (already exist).")

    # ------------------------------------------------------------------ #
    # Step 13 — Update theatrical/streaming states                        #
    # ------------------------------------------------------------------ #
    print("\n[BOT] Step 13: Updating theatrical and streaming states...")

    theatres_count = 0
    streaming_count = 0

    movies_for_theatres = db.get_movies_to_update_theatres(today)
    for item in movies_for_theatres:
        db.update_theatres_status(item["id"], is_in_theatres=True, is_streamable_now=False)
        print(f"[BOT] Step 13 {item['title']}: marked as IN THEATRES")
        theatres_count += 1

    titles_leaving = db.get_titles_leaving_theatres()
    for item in titles_leaving:
        db.update_theatres_status(item["id"], is_in_theatres=False, is_streamable_now=True)
        print(f"[BOT] Step 13 {item['title']}: moved from IN THEATRES to STREAMING")
        streaming_count += 1

    print(f"[BOT] Step 13 done. {theatres_count} movies marked in theatres, {streaming_count} titles moved to streaming.")

    # ------------------------------------------------------------------ #
    # Step 14 — Fetch and store trailers for titles with none             #
    # ------------------------------------------------------------------ #
    print("\n[BOT] Step 14: Fetching trailers for titles with no trailers...")

    trailer_media_ids: set[int] = {
        row["media_id"]
        for row in (db.client.table("media_trailers").select("media_id").execute().data or [])
    }

    page_size = 1000
    offset = 0
    all_titles: list[dict] = []
    while True:
        batch = (
            db.client.table("media")
            .select("id, tmdb_id, title, media_type")
            .range(offset, offset + page_size - 1)
            .execute()
            .data or []
        )
        all_titles.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size

    needs_trailers = [t for t in all_titles if t["id"] not in trailer_media_ids][:50]

    step14_trailers = 0
    for item in needs_trailers:
        videos = tmdb.get_videos(tmdb_id=item["tmdb_id"], media_type=item["media_type"])
        count  = db.upsert_trailers(media_id=item["id"], trailers=videos)
        step14_trailers += count
        print(f"[BOT] Step 14 {item['title']}: {count} trailer(s) added")
        time.sleep(0.25)

    print(f"[BOT] Step 14 done. {len(needs_trailers)} titles processed, {step14_trailers} trailers added.")

    # ------------------------------------------------------------------ #
    # Step 15 — Fetch and store credits for titles with none              #
    # ------------------------------------------------------------------ #
    print("\n[BOT] Step 15: Fetching credits for titles with no credits...")

    credit_media_ids: set[str] = {
        row["media_id"]
        for row in (db.client.table("media_credits").select("media_id").execute().data or [])
    }

    offset = 0
    all_titles_for_credits: list[dict] = []
    while True:
        batch = (
            db.client.table("media")
            .select("id, tmdb_id, title, media_type")
            .range(offset, offset + page_size - 1)
            .execute()
            .data or []
        )
        all_titles_for_credits.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size

    needs_credits = [t for t in all_titles_for_credits if t["id"] not in credit_media_ids][:50]

    step15_credits = 0
    for item in needs_credits:
        result = tmdb.get_credits(tmdb_id=item["tmdb_id"], media_type=item["media_type"])
        count = db.upsert_credits(
            media_id=item["id"],
            directors=result["directors"],
            writers=result["writers"],
            cast=result["cast"],
            created_by=result["created_by"],
            producers=result["producers"],
        )
        step15_credits += count
        print(f"[BOT] Step 15 {item['title']}: {count} credit(s) added")
        time.sleep(0.25)

    print(f"[BOT] Step 15 done. {len(needs_credits)} titles processed, {step15_credits} credits inserted.")


if __name__ == "__main__":
    main()
