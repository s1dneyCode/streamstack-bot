"""
Daily /changes sync: catches new titles and TMDB-side deletions between
nightly bot runs by walking TMDB's /movie/changes and /tv/changes feeds —
the same feed TMDB itself recommends for staying in sync without re-crawling
the whole catalog.

Window: since the last successful run (last_synced_at, persisted in
bot_state), defaulting to 24h if no prior run is recorded. If a run was
missed and more than TMDB's 14-day max has elapsed, the window is capped at
14 days — TMDB simply won't report changes further back than that.

For each changed id:
  - 404 from TMDB → the title is gone. If we have it, soft-flag it via
    tmdb_missing_at (never delete). If we don't, nothing to do.
  - Not in our DB and passes the quality filter → bare-row insert
    (enriched_at NULL; enrich_new_titles.py fills in the rest).
  - Already in our DB → reset streaming_last_checked to NULL so
    get_titles_to_reverify() re-checks its availability sooner. No forced
    re-enrichment.

Run via:
    python -m bot.sync_changes
"""

import os
import sys
from datetime import datetime, timedelta, timezone

import requests

from .tmdb import TmdbClient
from .supabase_client import SupabaseClient
from .filters import passes_sync_quality_filter

DEFAULT_WINDOW_HOURS = 24
MAX_WINDOW_DAYS = 14


def load_env() -> dict[str, str]:
    required = ["TMDB_API_KEY", "SUPABASE_URL", "SUPABASE_KEY"]
    config: dict[str, str] = {}
    for key in required:
        value = os.environ.get(key)
        if not value:
            print(f"[CHANGES] ERROR: Required environment variable '{key}' is not set.")
            sys.exit(1)
        config[key] = value
    return config


def _compute_window(db: SupabaseClient, now: datetime) -> tuple[datetime, str, str]:
    """
    Return (window_start, start_date, end_date) — window_start is the precise
    timestamp used to persist last_synced_at; start_date/end_date are the
    YYYY-MM-DD strings TMDB's /changes endpoints require (date granularity
    only, so a run partway through window_start's day re-covers that whole
    day — harmless, since re-processing an id is idempotent).
    """
    last_synced_raw = db.get_bot_state("last_synced_at")
    last_synced = None
    if last_synced_raw:
        try:
            last_synced = datetime.fromisoformat(last_synced_raw)
        except ValueError:
            print(f"[CHANGES] WARNING: could not parse stored last_synced_at={last_synced_raw!r} — defaulting to {DEFAULT_WINDOW_HOURS}h window.")

    if last_synced is None:
        window_start = now - timedelta(hours=DEFAULT_WINDOW_HOURS)
        print(f"[CHANGES] No prior last_synced_at — using default {DEFAULT_WINDOW_HOURS}h window.")
    else:
        window_start = last_synced
        cap = now - timedelta(days=MAX_WINDOW_DAYS)
        if window_start < cap:
            missed_days = (now - window_start).days
            print(f"[CHANGES] Last sync was {missed_days} day(s) ago — widening window, capped at TMDB's {MAX_WINDOW_DAYS}-day max.")
            window_start = cap

    return window_start, window_start.date().isoformat(), now.date().isoformat()


def main() -> None:
    print("[CHANGES] Starting daily /changes sync...")
    config = load_env()

    db   = SupabaseClient(url=config["SUPABASE_URL"], key=config["SUPABASE_KEY"])
    tmdb = TmdbClient(api_key=config["TMDB_API_KEY"])

    now = datetime.now(timezone.utc)
    window_start, start_date, end_date = _compute_window(db, now)
    print(f"[CHANGES] Window: {start_date} to {end_date} (start_date={window_start.isoformat()}).")

    try:
        movie_ids = tmdb.get_movie_changes(start_date, end_date)
    except Exception as exc:
        print(f"[CHANGES] ERROR: failed to fetch /movie/changes: {exc}")
        movie_ids = []

    try:
        tv_ids = tmdb.get_tv_changes(start_date, end_date)
    except Exception as exc:
        print(f"[CHANGES] ERROR: failed to fetch /tv/changes: {exc}")
        tv_ids = []

    changed = [(tmdb_id, "movie") for tmdb_id in movie_ids] + [(tmdb_id, "tv") for tmdb_id in tv_ids]
    print(f"[CHANGES] {len(movie_ids)} changed movie id(s), {len(tv_ids)} changed tv id(s) — {len(changed)} total.")

    existing_map = db.get_media_ids_by_tmdb_ids([tmdb_id for tmdb_id, _ in changed]) if changed else {}
    print(f"[CHANGES] {len(existing_map)}/{len(changed)} already in DB.")

    checked        = 0
    new_inserted   = 0
    marked_missing = 0
    errors         = 0

    for tmdb_id, media_type in changed:
        checked += 1
        media_id = existing_map.get(tmdb_id)

        try:
            detail = tmdb._get(f"/{media_type}/{tmdb_id}")
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status == 404:
                if media_id:
                    db.set_tmdb_missing(media_id)
                    marked_missing += 1
                    print(f"[CHANGES]   {media_type}/{tmdb_id}: 404 — flagged tmdb_missing_at.")
                # Not in our DB and gone from TMDB — nothing to do.
                continue
            print(f"[CHANGES]   {media_type}/{tmdb_id}: fetch failed ({status}) — {exc}")
            errors += 1
            continue
        except Exception as exc:
            print(f"[CHANGES]   {media_type}/{tmdb_id}: fetch failed — {exc}")
            errors += 1
            continue

        try:
            if media_id:
                db.reset_streaming_last_checked(media_id)
                continue

            if not passes_sync_quality_filter(detail):
                continue

            title = detail.get("title") or detail.get("name")
            media_record = {
                "tmdb_id":           tmdb_id,
                "title":             title,
                "overview":          detail.get("overview"),
                "poster_path":       detail.get("poster_path"),
                "media_type":        media_type,
                "release_date":      detail.get("release_date") or detail.get("first_air_date"),
                "status":            detail.get("status"),
                "original_language": detail.get("original_language"),
                "tmdb_score":        round((detail.get("vote_average") or 0) * 10),
                "genres":            [g["name"] for g in detail.get("genres", []) if g.get("name")],
                "imdb_id":           detail.get("imdb_id"),
                "popularity":        detail.get("popularity", 0.0),
                "vote_count":        detail.get("vote_count", 0),
                "is_in_theatres":    False,
                "is_streamable_now": False,
            }

            row_id = db.upsert_media(media_record)
            if row_id:
                new_inserted += 1
                print(f"[CHANGES]   {media_type}/{tmdb_id}: inserted bare row — {title!r}")
            else:
                errors += 1
        except Exception as exc:
            print(f"[CHANGES]   {media_type}/{tmdb_id}: processing failed — {exc}")
            errors += 1

    try:
        db.set_bot_state("last_synced_at", now.isoformat())
    except Exception as exc:
        print(f"[CHANGES] WARNING: failed to persist last_synced_at — next run will re-widen the window: {exc}")

    print(
        f"\n[CHANGES] SUMMARY checked={checked} new_inserted={new_inserted} "
        f"marked_missing={marked_missing} errors={errors}"
    )


if __name__ == "__main__":
    main()
