"""
StreamStack Bot — entry point.

Orchestrates the nightly pipeline:
  1. Fetch currently playing / on-air titles from TMDB.
  2. Deduplicate against what is already stored in Supabase.
  3. Enrich NEW titles with TMDB detail data (status, genres, certifications,
     watch providers, etc.).
  4. Persist everything to Supabase.

Run locally (requires env vars to be set in the shell):
    python bot/main.py
    python bot/main.py --limit=1000   # cap Steps 10/10b/11's re-verify set

In production this file is invoked by the GitHub Actions nightly workflow
(.github/workflows/nightly.yml) which injects secrets as environment variables.
"""

import os
import sys
from datetime import date

import requests

from .tmdb import TmdbClient
from .omdb import OmdbClient
from .supabase_client import SupabaseClient, compute_popularity_score, _PRE_RELEASE_STATUSES, ALLOWED_PROVIDERS
from .filters import ALLOWED_LANGUAGES
from .migrate_images import poster_basename, refresh_poster


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

    # Caps Steps 10/10b/11's re-verify set; falls through to the
    # REVERIFY_LIMIT env var (then a hardcoded default) when not passed —
    # see SupabaseClient.get_titles_to_reverify().
    reverify_limit = None
    for arg in sys.argv[1:]:
        if arg.startswith("--limit="):
            reverify_limit = int(arg.split("=")[1])

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
    # Step 8b — Refresh posters for existing titles seen tonight          #
    # ------------------------------------------------------------------ #
    # Relies only on poster_path already present in `combined` (from the
    # list endpoints fetched in Steps 1-7) — no extra TMDB API calls.
    print("\n[BOT] Step 8b: Refreshing posters for existing titles seen tonight...")
    existing_items = [item for item in combined if item["tmdb_id"] in existing_ids]
    poster_state = db.get_media_poster_state_by_tmdb_ids([item["tmdb_id"] for item in existing_items])

    step8b_refreshed = 0
    step8b_errors    = 0
    for item in existing_items:
        tmdb_id = item["tmdb_id"]
        try:
            stored = poster_state.get(tmdb_id)
            if stored is None:
                continue
            fresh = item.get("poster_path")
            if fresh and poster_basename(fresh) != poster_basename(stored.get("poster_path")):
                if refresh_poster(db, tmdb_id, stored["id"], fresh):
                    step8b_refreshed += 1
        except Exception as exc:
            step8b_errors += 1
            print(f"[BOT] Step 8b {tmdb_id}: failed — {exc}")

    print(
        f"[BOT] Step 8b: refreshed {step8b_refreshed} poster(s) of "
        f"{len(existing_items)} existing titles seen tonight ({step8b_errors} errors)."
    )

    # ------------------------------------------------------------------ #
    # Step 9 — Enrich and persist each NEW title                          #
    # ------------------------------------------------------------------ #
    print("\n[BOT] Step 9: Enriching and saving new titles...")
    total = len(new_items)
    step9_inserted = 0
    step9_failed   = 0

    for index, item in enumerate(new_items, start=1):
        title      = item["title"]
        tmdb_id    = item["tmdb_id"]
        media_type = item["media_type"]

        # --- Fetch full detail from TMDB, sub-resources appended in one call
        is_limited_series = None
        status            = None
        runtime           = item.get("runtime")
        vote_count        = item.get("vote_count")
        original_language = item.get("original_language")
        is_documentary    = item.get("is_documentary", False)
        genres            = [g for g in item.get("genre", "").split(", ") if g]
        release_date      = item.get("release_date")
        us_release_date   = None

        # Fed to get_certification() below — for movies this is the same
        # /release_dates payload get_movie_release_dates() also parses; for
        # TV it's /content_ratings. Defaults to {} (not None) so a total
        # fetch failure still short-circuits the downstream helpers instead
        # of triggering a second, separately-failing request.
        certification_data   = {}
        images_data           = {}
        watch_providers_data  = {}

        if media_type == "tv":
            try:
                detail = tmdb._get(
                    f"/tv/{tmdb_id}",
                    params={"append_to_response": "content_ratings,images,watch/providers"},
                )
                is_limited_series = detail.get("type") == "Miniseries"
                status            = detail.get("status")
                vote_count        = detail.get("vote_count")
                original_language = detail.get("original_language")
                is_documentary    = 99 in [g["id"] for g in detail.get("genres", [])]
                genres            = [g["name"] for g in detail.get("genres", []) if g.get("name")]
                certification_data  = detail.get("content_ratings", {})
                images_data          = detail.get("images", {})
                watch_providers_data = detail.get("watch/providers", {})
            except Exception:
                pass
        else:
            try:
                detail = tmdb._get(
                    f"/movie/{tmdb_id}",
                    params={"append_to_response": "release_dates,images,watch/providers"},
                )
                status            = detail.get("status")
                runtime           = detail.get("runtime")
                vote_count        = detail.get("vote_count")
                original_language = detail.get("original_language")
                is_documentary    = 99 in [g["id"] for g in detail.get("genres", [])]
                genres            = [g["name"] for g in detail.get("genres", []) if g.get("name")]
                certification_data  = detail.get("release_dates", {})
                images_data          = detail.get("images", {})
                watch_providers_data = detail.get("watch/providers", {})

                worldwide_date, us_release_date = tmdb.get_movie_release_dates(tmdb_id=tmdb_id, data=certification_data)
                if worldwide_date:
                    release_date = worldwide_date
            except Exception:
                pass

        # --- Runtime filter: skip short films ---------------------------
        if media_type == "movie" and runtime is not None and runtime < 40:
            print(f"[BOT] Skipping {title}: runtime={runtime}min (short film filter)")
            continue

        # --- Language filter ---------------------------------------------
        if original_language not in ALLOWED_LANGUAGES:
            print(f"[BOT] Skipping {title}: original_language={original_language} (language filter)")
            continue

        # --- Watch providers (from the appended detail payload above) ----
        watch_providers = tmdb.get_watch_providers(tmdb_id=tmdb_id, media_type=media_type, data=watch_providers_data)
        watch_providers = {
            region: {kind: [p for p in names if p in ALLOWED_PROVIDERS] for kind, names in kinds.items()}
            for region, kinds in watch_providers.items()
        }
        is_streamable_now = bool(watch_providers.get("US", {}).get("flatrate"))

        # --- Build and persist the media record -------------------------
        media_record = {
            "tmdb_id":      tmdb_id,
            "title":        title,
            "overview":     item.get("overview"),
            "poster_path":  item.get("poster_path"),
            "media_type":   media_type,
            "release_date":    release_date,
            "us_release_date": us_release_date,
            "is_in_theatres":    False,
            "is_streamable_now": is_streamable_now,
            "popularity":   item.get("popularity", 0.0),
            "imdb_id":      item.get("imdb_id"),
            "runtime":      runtime,
            "title_logo_url": tmdb.get_title_logo(tmdb_id=tmdb_id, media_type=media_type, data=images_data),
            "certification":  tmdb.get_certification(tmdb_id=tmdb_id, media_type=media_type, data=certification_data),
            "genres":         genres,
            "vote_count":     vote_count,
            "tmdb_score":     item.get("tmdb_score", 0),
            "status":         status,
            "original_language": original_language,
            "is_documentary":    is_documentary,
            "is_limited_series": is_limited_series,
        }

        media_id = db.upsert_media(media_record)
        if media_id:
            step9_inserted += 1
            if any(names for kinds in watch_providers.values() for names in kinds.values()):
                db.upsert_streaming_availability(media_id=media_id, providers=watch_providers)
            db.update_streaming_last_checked(media_id)
        else:
            step9_failed += 1
        print(f"[BOT] Step 9 {index}/{total}: {title} (status={status or 'unknown'})")

    # Load reverify list for Steps 10, 10b and 11
    today = date.today()
    reverify_list = db.get_titles_to_reverify(today, limit=reverify_limit)
    print(f"\n[BOT] {len(reverify_list)} titles queued for periodic re-verification.")

    # ------------------------------------------------------------------ #
    # Step 10 — Re-verify streaming providers for existing titles         #
    # ------------------------------------------------------------------ #
    print(f"\n[BOT] Step 10: Re-verifying streaming providers for {len(reverify_list)} existing titles...")

    reverified      = 0
    step10_missing  = 0
    step10_errors   = 0
    missing_tmdb_ids: set[int] = set()
    for item in reverify_list:
        media_id   = item["id"]
        tmdb_id    = item["tmdb_id"]
        title      = item["title"]
        media_type = item["media_type"]

        # Fetched directly (not via the get_watch_providers(data=None) path)
        # so a 404 — the title has been removed from TMDB — surfaces here
        # instead of being swallowed into an empty-providers result.
        try:
            raw_providers = tmdb._get(f"/{media_type}/{tmdb_id}/watch/providers")
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status == 404:
                db.set_tmdb_missing(media_id)
                missing_tmdb_ids.add(tmdb_id)
                step10_missing += 1
                print(f"[BOT] Step 10 {title}: 404 — removed from TMDB, flagged tmdb_missing_at.")
                continue
            print(f"[BOT] Step 10 {title}: failed ({status}) — {exc}")
            step10_errors += 1
            continue
        except Exception as exc:
            print(f"[BOT] Step 10 {title}: failed — {exc}")
            step10_errors += 1
            continue

        try:
            providers = tmdb.get_watch_providers(tmdb_id=tmdb_id, media_type=media_type, data=raw_providers)
            providers = {
                region: {kind: [p for p in names if p in ALLOWED_PROVIDERS] for kind, names in kinds.items()}
                for region, kinds in providers.items()
            }

            if any(names for kinds in providers.values() for names in kinds.values()):
                is_streamable = bool(providers.get("US", {}).get("flatrate"))
                db.sync_streaming_providers(media_id=media_id, providers=providers)
                db.client.table("media").update({"is_streamable_now": is_streamable}).eq("id", media_id).execute()

            db.update_streaming_last_checked(media_id)
            reverified += 1

            print(f"[BOT] Re-verified {title}: {providers if any(providers.values()) else '(no results — kept existing data)'}")
        except Exception as exc:
            print(f"[BOT] Step 10 {title}: failed — {exc}")
            step10_errors += 1

    print(f"[BOT] Step 10 done. {reverified} titles re-verified, {step10_missing} flagged missing, {step10_errors} errors.")

    if missing_tmdb_ids:
        reverify_list = [item for item in reverify_list if item["tmdb_id"] not in missing_tmdb_ids]
        print(f"[BOT] Excluded {len(missing_tmdb_ids)} newly-flagged-missing title(s) from Steps 10b/11.")

    # ------------------------------------------------------------------ #
    # Step 10b — Update popularity for titles due for re-verification      #
    # ------------------------------------------------------------------ #
    print(f"\n[BOT] Step 10b: Updating popularity for {len(reverify_list)} titles...")

    step10b_errors = 0
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
            step10b_errors += 1

    print(f"[BOT] Step 10b done. {step10b_errors} errors.")

    # ------------------------------------------------------------------ #
    # Step 11 — Update RT scores for movies due for re-verification       #
    # ------------------------------------------------------------------ #
    movies_to_update = [
        item for item in reverify_list
        if item["media_type"] == "movie"
        and item.get("status") not in _PRE_RELEASE_STATUSES
    ]
    print(f"\n[BOT] Step 11: Updating RT scores for {len(movies_to_update)} movies...")

    rt_updated    = 0
    step11_errors = 0
    for item in movies_to_update:
        tmdb_id = item["tmdb_id"]
        title   = item["title"]
        year    = extract_year(item.get("release_date"))

        try:
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
        except Exception as exc:
            print(f"[BOT] Step 11 {title}: failed — {exc}")
            step11_errors += 1

    print(f"[BOT] Step 11 done. {rt_updated} RT scores updated, {step11_errors} errors.")

    # ------------------------------------------------------------------ #
    # Step 11b — Retry RT scores for released movies with no score        #
    # ------------------------------------------------------------------ #
    missing_rt = db.get_movies_missing_rt_score(today)
    print(f"\n[BOT] Step 11b: Retrying RT scores for {len(missing_rt)} released movies with no score...")

    rt_recovered   = 0
    step11b_errors = 0
    for item in missing_rt:
        tmdb_id = item["tmdb_id"]
        title   = item["title"]
        year    = extract_year(item.get("release_date"))
        imdb_id = item.get("imdb_id")

        try:
            score = omdb.get_rt_score(title=title, year=year, imdb_id=imdb_id)

            if score is not None:
                db.client.table("media").update({"rt_score": score}).eq("tmdb_id", tmdb_id).execute()
                db.update_streaming_last_checked(item["id"])
                print(f"[BOT] Step 11b {title}: found {score}%")
                rt_recovered += 1
            else:
                db.update_streaming_last_checked(item["id"])
                print(f"[BOT] Step 11b {title}: still not found")
        except Exception as exc:
            print(f"[BOT] Step 11b {title}: failed — {exc}")
            step11b_errors += 1

    print(f"[BOT] Step 11b done. {rt_recovered} new RT scores found, {step11b_errors} errors.")

    # ------------------------------------------------------------------ #
    # Step 12 — Summary                                                    #
    # ------------------------------------------------------------------ #
    print(f"\n[BOT] Done. {total} new titles added, {skipped} skipped (already exist).")

    # is_in_theatres is now managed by daily_verify.py (runs at 8am UTC)

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
    step14_errors        = 0

    for show in returning_for_seasons:
        media_id = show["id"]
        tmdb_id  = show["tmdb_id"]
        title    = show["title"]

        # Whole per-show body (fetch + writes) isolated — one show's
        # exception (e.g. a season TMDB no longer has) must not abort the
        # rest of the run.
        try:
            tmdb_seasons = tmdb.get_seasons(tmdb_id=tmdb_id)
            if not tmdb_seasons:
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
                continue

            new_seasons_data = [s for s in tmdb_seasons if s["season_number"] in new_season_numbers]
            season_rows      = db.upsert_seasons(media_id=media_id, seasons=new_seasons_data)
            season_map       = {r["season_number"]: r["id"] for r in season_rows}

            eps_inserted = 0
            for s in new_seasons_data:
                season_id = season_map.get(s["season_number"])
                if not season_id:
                    continue
                # Per-season isolation too — one bad season (e.g. a 404 on
                # its episode list) shouldn't skip this show's other new
                # seasons.
                try:
                    _, episodes = tmdb.get_season_episodes(tmdb_id=tmdb_id, season_number=s["season_number"])
                    eps_inserted += db.upsert_episodes(season_id=season_id, episodes=episodes)
                except Exception as exc:
                    print(f"[BOT] Step 14 {title} season {s['season_number']}: failed — {exc}")
                    step14_errors += 1

            step14_shows_updated += 1
            step14_new_seasons   += len(new_seasons_data)
            step14_new_episodes  += eps_inserted
            print(f"[BOT] Step 14 {title}: {len(new_seasons_data)} new season(s), {eps_inserted} episodes")
        except Exception as exc:
            print(f"[BOT] Step 14 {title}: failed — {exc}")
            step14_errors += 1

    print(
        f"[BOT] Step 14 done. {step14_shows_updated} shows updated, {step14_new_seasons} new seasons, "
        f"{step14_new_episodes} new episodes, {step14_errors} errors."
    )

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
    step17_errors   = 0

    for show in active_series:
        media_id = show["id"]
        tmdb_id  = show["tmdb_id"]
        title    = show["title"]

        # Whole per-show body (fetch + writes) isolated — one show's
        # exception must not abort the rest of the run.
        try:
            # Find the latest season stored in our DB for this show
            latest = (
                db.client.table("media_seasons")
                .select("id, season_number")
                .eq("media_id", media_id)
                .order("season_number", desc=True)
                .limit(1)
                .execute()
                .data
            )
            if not latest:
                continue

            season_id     = latest[0]["id"]
            season_number = latest[0]["season_number"]

            # Confirm this season still exists on TMDB before fetching its
            # episodes — our stored "latest" season number can go stale
            # (renumbered/removed seasons) and 404 on the season
            # sub-resource even though the show itself is still valid, per
            # TMDB's own current listing rather than blindly trusting what
            # we last stored.
            tmdb_seasons = tmdb.get_seasons(tmdb_id=tmdb_id)
            if not any(s["season_number"] == season_number for s in tmdb_seasons):
                continue

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

            _, fresh_eps = tmdb.get_season_episodes(tmdb_id=tmdb_id, season_number=season_number)

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
        except Exception as exc:
            print(f"[BOT] Step 17 {title}: failed — {exc}")
            step17_errors += 1

    print(f"[BOT] Step 17 done. {step17_series} series processed, {step17_episodes} episode dates updated, {step17_errors} errors.")

    # ------------------------------------------------------------------ #
    # Step 17b — Insert new episodes for active series                    #
    # ------------------------------------------------------------------ #
    print("\n[BOT] Step 17b: Checking for new episodes on active series...")

    # Reuse active_series (Returning Series + streamable, deduped, capped at 50)
    step17b_checked  = 0
    step17b_episodes = 0
    step17b_errors   = 0

    for show in active_series:
        media_id = show["id"]
        tmdb_id  = show["tmdb_id"]
        title    = show["title"]

        # Whole per-show body (fetch + writes) isolated — one show's
        # exception must not abort the rest of the run.
        try:
            # Find the latest season stored in our DB for this show
            latest = (
                db.client.table("media_seasons")
                .select("id, season_number")
                .eq("media_id", media_id)
                .order("season_number", desc=True)
                .limit(1)
                .execute()
                .data
            )
            if not latest:
                continue

            season_id     = latest[0]["id"]
            season_number = latest[0]["season_number"]
            step17b_checked += 1

            tmdb_seasons = tmdb.get_seasons(tmdb_id=tmdb_id)
            tmdb_season = next((s for s in tmdb_seasons if s["season_number"] == season_number), None)
            if not tmdb_season:
                continue

            tmdb_episode_count = tmdb_season.get("episode_count") or 0

            stored_eps = (
                db.client.table("media_episodes")
                .select("episode_number")
                .eq("season_id", season_id)
                .execute()
                .data or []
            )

            if tmdb_episode_count <= len(stored_eps):
                continue

            _, fresh_eps = tmdb.get_season_episodes(tmdb_id=tmdb_id, season_number=season_number)

            existing_nums = {row["episode_number"] for row in stored_eps}
            new_eps = [ep for ep in fresh_eps if ep.get("episode_number") not in existing_nums]

            if new_eps:
                n = db.upsert_episodes(season_id=season_id, episodes=new_eps)
                step17b_episodes += n
                print(f"[BOT] Step 17b {title}: {n} new episode(s) inserted (season {season_number})")
        except Exception as exc:
            print(f"[BOT] Step 17b {title}: failed — {exc}")
            step17b_errors += 1

    print(f"[BOT] Step 17b done. {step17b_checked} series checked, {step17b_episodes} new episodes inserted, {step17b_errors} errors.")

    # ------------------------------------------------------------------ #
    # End-of-run summary — always reached: every per-item loop above      #
    # isolates its own exceptions rather than propagating them, so a      #
    # single bad title/show/season can never prevent this from printing. #
    # ------------------------------------------------------------------ #
    total_errors = (
        step9_failed + step10_errors + step10b_errors + step11_errors
        + step11b_errors + step14_errors + step17_errors + step17b_errors
    )
    print(
        f"\n[BOT] SUMMARY discovered={len(combined)} new_inserted={step9_inserted} "
        f"reverified={reverified} enriched={rt_updated + rt_recovered} errors={total_errors}"
    )


if __name__ == "__main__":
    main()
