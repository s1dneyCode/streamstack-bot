"""
StreamStack Bot — entry point.

Orchestrates the nightly pipeline:
  1. Fetch currently playing / on-air titles from TMDB.
  2. Deduplicate against what is already stored in Supabase.
  3. Enrich NEW titles with TMDB detail data (status, genres, certifications, etc.).
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
from .supabase_client import SupabaseClient, compute_popularity_score, _PRE_RELEASE_STATUSES


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
    # Step 1b — Fetch trending movies this week from TMDB                 #
    # ------------------------------------------------------------------ #
    print("\n[BOT] Step 1b: Fetching trending movies (week) from TMDB...")
    trending_movies = tmdb.get_trending_movies(pages=3, genre_map=movie_genre_map)
    print(f"[BOT] Fetched {len(trending_movies)} trending movies.")

    # ------------------------------------------------------------------ #
    # Step 2 — Fetch on-air TV shows from TMDB                            #
    # ------------------------------------------------------------------ #
    print("\n[BOT] Step 2: Fetching on-air TV shows from TMDB...")
    on_air = tmdb.get_on_air_tv(pages=3, genre_map=tv_genre_map)
    print(f"[BOT] Fetched {len(on_air)} on-air TV shows.")

    # ------------------------------------------------------------------ #
    # Step 2b — Fetch trending TV shows this week from TMDB               #
    # ------------------------------------------------------------------ #
    print("\n[BOT] Step 2b: Fetching trending TV shows (week) from TMDB...")
    trending_tv = tmdb.get_trending_tv(pages=3, genre_map=tv_genre_map)
    print(f"[BOT] Fetched {len(trending_tv)} trending TV shows.")

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
    top_rated = tmdb.get_top_rated_movies(pages=15, genre_map=movie_genre_map)
    print(f"[BOT] Fetched {len(top_rated)} top-rated movies.")

    # ------------------------------------------------------------------ #
    # Step 6b — Discover movies by revenue                                #
    # ------------------------------------------------------------------ #
    print("\n[BOT] Step 6b: Fetching discover movies (revenue) from TMDB...")
    discover_movies_revenue = tmdb.get_discover_movies_by_revenue(pages=15, genre_map=movie_genre_map)
    print(f"[BOT] Fetched {len(discover_movies_revenue)} discover movies (revenue).")

    # ------------------------------------------------------------------ #
    # Step 6c — Discover movies by vote count                             #
    # ------------------------------------------------------------------ #
    print("\n[BOT] Step 6c: Fetching discover movies (vote count) from TMDB...")
    discover_movies_votes = tmdb.get_discover_movies_by_vote_count(pages=15, genre_map=movie_genre_map)
    print(f"[BOT] Fetched {len(discover_movies_votes)} discover movies (vote count).")

    # ------------------------------------------------------------------ #
    # Step 6d — Fetch top-rated TV shows from TMDB                        #
    # ------------------------------------------------------------------ #
    print("\n[BOT] Step 6d: Fetching top-rated TV shows from TMDB...")
    top_rated_tv = tmdb.get_top_rated_tv(pages=15, genre_map=tv_genre_map)
    print(f"[BOT] Fetched {len(top_rated_tv)} top-rated TV shows.")

    # ------------------------------------------------------------------ #
    # Step 6e — Discover TV shows by vote count                           #
    # ------------------------------------------------------------------ #
    print("\n[BOT] Step 6e: Fetching discover TV shows (vote count) from TMDB...")
    discover_tv_votes = tmdb.get_discover_tv_by_vote_count(pages=15, genre_map=tv_genre_map)
    print(f"[BOT] Fetched {len(discover_tv_votes)} discover TV shows (vote count).")

    # ------------------------------------------------------------------ #
    # Step 7 — Combine and deduplicate by tmdb_id                         #
    # ------------------------------------------------------------------ #
    print("\n[BOT] Step 7: Combining and deduplicating results...")
    combined: list[dict] = []
    seen_ids: set[int] = set()

    for item in (now_playing + trending_movies + on_air + trending_tv + upcoming
                 + popular_movies + popular_tv + top_rated
                 + discover_movies_revenue + discover_movies_votes + top_rated_tv + discover_tv_votes):
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
        title      = item["title"]
        tmdb_id    = item["tmdb_id"]
        media_type = item["media_type"]

        # --- Fetch full detail from TMDB --------------------------------
        is_limited_series = None
        status            = None
        runtime           = item.get("runtime")
        vote_count        = item.get("vote_count")
        original_language = item.get("original_language")
        is_documentary    = item.get("is_documentary", False)
        genres            = [g for g in item.get("genre", "").split(", ") if g]

        if media_type == "tv":
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
        else:
            try:
                detail = tmdb._get(f"/movie/{tmdb_id}")
                status            = detail.get("status")
                runtime           = detail.get("runtime")
                vote_count        = detail.get("vote_count")
                original_language = detail.get("original_language")
                is_documentary    = 99 in [g["id"] for g in detail.get("genres", [])]
                genres            = [g["name"] for g in detail.get("genres", []) if g.get("name")]
            except Exception:
                pass

        # --- Runtime filter: skip short films ---------------------------
        if media_type == "movie" and runtime is not None and runtime < 40:
            print(f"[BOT] Skipping {title}: runtime={runtime}min (short film filter)")
            continue

        # --- Build and persist the media record -------------------------
        media_record = {
            "tmdb_id":      tmdb_id,
            "title":        title,
            "overview":     item.get("overview"),
            "poster_path":  item.get("poster_path"),
            "media_type":   media_type,
            "release_date": item.get("release_date"),
            "is_in_theatres":    False,
            "is_streamable_now": False,
            "popularity":   item.get("popularity", 0.0),
            "imdb_id":      item.get("imdb_id"),
            "runtime":      runtime,
            "title_logo_url": tmdb.get_title_logo(tmdb_id=tmdb_id, media_type=media_type),
            "certification":  tmdb.get_certification(tmdb_id=tmdb_id, media_type=media_type),
            "genres":         genres,
            "vote_count":     vote_count,
            "status":         status,
            "original_language": original_language,
            "is_documentary":    is_documentary,
            "is_limited_series": is_limited_series,
        }

        db.upsert_media(media_record)
        print(f"[BOT] Step 9 {index}/{total}: {title} (status={status or 'unknown'})")

    # Load reverify list for Steps 10b and 11
    today = date.today()
    reverify_list = db.get_titles_to_reverify(today)
    print(f"\n[BOT] {len(reverify_list)} titles queued for periodic score and popularity updates.")

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
            popularity = detail.get("popularity", 0.0) or 0.0
            row = db.client.table("media").select("tmdb_score, rt_score, vote_count, release_date").eq("tmdb_id", tmdb_id).maybe_single().execute().data or {}
            popularity_score = compute_popularity_score(
                popularity,
                row.get("tmdb_score"),
                row.get("rt_score"),
                row.get("vote_count"),
                release_date=row.get("release_date"),
            )
            db.client.table("media").update({"popularity": popularity, "popularity_score": popularity_score}).eq("tmdb_id", tmdb_id).execute()
            print(f"[BOT] Updated popularity for {title}: {popularity} → score={popularity_score}")
        except Exception as exc:
            print(f"[BOT] Failed to update popularity for {title}: {exc}")

        time.sleep(0.3)

    # ------------------------------------------------------------------ #
    # Step 11 — Update RT scores for movies due for re-verification       #
    # ------------------------------------------------------------------ #
    movies_to_update = [
        item for item in reverify_list
        if item["media_type"] == "movie"
        and item.get("status") not in _PRE_RELEASE_STATUSES
    ]
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
            .maybe_single()
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
    # Step 13 — Update is_in_theatres using now_playing as source of truth #
    # ------------------------------------------------------------------ #
    print("\n[BOT] Step 13: Updating is_in_theatres from now_playing list...")

    now_playing_tmdb_ids = {item["tmdb_id"] for item in now_playing}

    # Movies currently flagged as in theatres in the DB
    currently_in_theatres = (
        db.client.table("media")
        .select("id, tmdb_id, title")
        .eq("media_type", "movie")
        .eq("is_in_theatres", True)
        .execute()
        .data or []
    )
    currently_in_theatres_set = {row["tmdb_id"] for row in currently_in_theatres}

    # Remove flag from movies no longer in now_playing
    removed = 0
    for row in currently_in_theatres:
        if row["tmdb_id"] not in now_playing_tmdb_ids:
            db.client.table("media").update({"is_in_theatres": False}).eq("id", row["id"]).execute()
            print(f"[BOT] Step 13 {row['title']}: removed from IN THEATRES")
            removed += 1

    # Add flag to now_playing movies that exist in the DB but aren't flagged yet
    np_in_db = (
        db.client.table("media")
        .select("id, tmdb_id, title")
        .eq("media_type", "movie")
        .in_("tmdb_id", list(now_playing_tmdb_ids))
        .execute()
        .data or []
    )
    added = 0
    for row in np_in_db:
        if row["tmdb_id"] not in currently_in_theatres_set:
            db.client.table("media").update({"is_in_theatres": True}).eq("id", row["id"]).execute()
            print(f"[BOT] Step 13 {row['title']}: marked IN THEATRES")
            added += 1

    print(f"[BOT] Step 13 done. {added} movies marked in theatres, {removed} removed from theatres.")

    # ------------------------------------------------------------------ #
    # Step 14 — Check for new seasons on Returning Series                 #
    # ------------------------------------------------------------------ #
    print("\n[BOT] Step 14: Checking for new seasons on Returning Series...")

    returning_for_seasons = (
        db.client.table("media")
        .select("id, tmdb_id, title")
        .eq("media_type", "tv")
        .eq("status", "Returning Series")
        .execute()
        .data or []
    )

    step14_shows_updated = 0
    step14_new_seasons   = 0
    step14_new_episodes  = 0

    for show in returning_for_seasons:
        media_id = show["id"]
        tmdb_id  = show["tmdb_id"]
        title    = show["title"]

        try:
            tmdb_seasons = tmdb.get_seasons(tmdb_id=tmdb_id)
        except Exception as exc:
            print(f"[BOT] Step 14 {title}: fetch failed — {exc}")
            time.sleep(0.25)
            continue

        if not tmdb_seasons:
            time.sleep(0.25)
            continue

        tmdb_season_numbers = {s["season_number"] for s in tmdb_seasons}

        stored = (
            db.client.table("media_seasons")
            .select("season_number")
            .eq("media_id", media_id)
            .execute()
            .data or []
        )
        stored_season_numbers = {r["season_number"] for r in stored}
        new_season_numbers    = tmdb_season_numbers - stored_season_numbers

        if not new_season_numbers:
            time.sleep(0.25)
            continue

        new_seasons_data = [s for s in tmdb_seasons if s["season_number"] in new_season_numbers]
        season_rows      = db.upsert_seasons(media_id=media_id, seasons=new_seasons_data)
        season_map       = {r["season_number"]: r["id"] for r in season_rows}

        eps_inserted = 0
        for s in new_seasons_data:
            season_id = season_map.get(s["season_number"])
            if not season_id:
                continue
            _, episodes = tmdb.get_season_episodes(tmdb_id=tmdb_id, season_number=s["season_number"])
            eps_inserted += db.upsert_episodes(season_id=season_id, episodes=episodes)
            time.sleep(0.25)

        step14_shows_updated += 1
        step14_new_seasons   += len(new_seasons_data)
        step14_new_episodes  += eps_inserted
        print(f"[BOT] Step 14 {title}: {len(new_seasons_data)} new season(s), {eps_inserted} episodes")
        time.sleep(0.5)

    print(f"[BOT] Step 14 done. {step14_shows_updated} shows updated, {step14_new_seasons} new seasons, {step14_new_episodes} new episodes.")

    # ------------------------------------------------------------------ #
    # Step 17 — Refresh upcoming episode air dates for active series      #
    # ------------------------------------------------------------------ #
    print("\n[BOT] Step 17: Refreshing episode air dates for active series...")

    # Fetch active TV shows: Returning Series + currently streamable (deduplicated)
    returning_series = (
        db.client.table("media")
        .select("id, tmdb_id, title")
        .eq("media_type", "tv")
        .eq("status", "Returning Series")
        .execute()
        .data or []
    )
    streamable_tv = (
        db.client.table("media")
        .select("id, tmdb_id, title")
        .eq("media_type", "tv")
        .eq("is_streamable_now", True)
        .execute()
        .data or []
    )

    seen_active: set[str] = set()
    active_series: list[dict] = []
    for show in returning_series + streamable_tv:
        if show["id"] not in seen_active:
            seen_active.add(show["id"])
            active_series.append(show)

    active_series = active_series[:50]
    print(f"[BOT] Step 17: {len(active_series)} active series queued.")

    step17_series   = 0
    step17_episodes = 0

    for show in active_series:
        media_id = show["id"]
        tmdb_id  = show["tmdb_id"]
        title    = show["title"]

        # Find the latest season stored in our DB for this show
        latest = (
            db.client.table("media_seasons")
            .select("id, season_number")
            .eq("media_id", media_id)
            .order("season_number", ascending=False)
            .limit(1)
            .execute()
            .data
        )
        if not latest:
            continue

        season_id     = latest[0]["id"]
        season_number = latest[0]["season_number"]

        # Index current episodes by episode_number for fast lookup
        current_eps: dict[int, dict] = {
            row["episode_number"]: row
            for row in (
                db.client.table("media_episodes")
                .select("id, episode_number, air_date")
                .eq("season_id", season_id)
                .execute()
                .data or []
            )
        }

        try:
            _, fresh_eps = tmdb.get_season_episodes(tmdb_id=tmdb_id, season_number=season_number)
        except Exception as exc:
            print(f"[BOT] Step 17 {title}: fetch failed — {exc}")
            time.sleep(0.25)
            continue

        updated = 0
        for ep in fresh_eps:
            ep_num   = ep.get("episode_number")
            new_date = ep.get("air_date")
            existing = current_eps.get(ep_num)

            if existing is None:
                db.upsert_episodes(season_id=season_id, episodes=[ep])
                updated += 1
            elif existing["air_date"] != new_date:
                db.client.table("media_episodes").update({"air_date": new_date}).eq("id", existing["id"]).execute()
                updated += 1

        step17_episodes += updated
        step17_series   += 1
        if updated:
            print(f"[BOT] Step 17 {title}: {updated} episode date(s) refreshed (season {season_number})")

        time.sleep(0.25)

    print(f"[BOT] Step 17 done. {step17_series} series processed, {step17_episodes} episode dates updated.")


if __name__ == "__main__":
    main()
