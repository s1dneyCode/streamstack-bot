"""
Nightly enrichment bot: runs 3 hours after the main nightly bot and populates
all secondary fields for titles added in the last 6 hours.

Steps per title (in order):
  1. Images         — upload poster + carousel to Supabase Storage
  2. Trailers       — fetch YouTube trailers from TMDB
  3. Credits        — fetch cast, directors, writers, producers
  4. Seasons        — fetch seasons + episodes (TV only)
  5. Title logos    — fetch title logo URL (skipped if already set)
  6. Certifications — fetch US certification (skipped if already set)
  7. Runtime        — fetch runtime minutes (movies only, skipped if already set)
  8. Genres         — fill genres array (skipped if already set)
  9. States         — update is_in_theatres / is_streamable_now

Run via:
    python -m bot.enrich_new_titles
"""

import os
import sys
import time
from datetime import datetime, timedelta

from .tmdb import TmdbClient
from .supabase_client import SupabaseClient
from .migrate_images import migrate_poster, migrate_carousel, download_image

POSTER_BASE = "https://image.tmdb.org/t/p/w500"
BUCKET      = "media-images"


def _full_image_url(path: str) -> str:
    """
    Build an absolute TMDB image URL from *path*.

    *path* is normally a relative TMDB path (e.g. "/abc123.jpg"), but some
    callers (e.g. media.poster_path, already normalized by tmdb.py's list
    endpoints) pass an already-absolute URL. Prepending POSTER_BASE in that
    case would produce a malformed double-prefixed URL, so this is a no-op
    when *path* is already absolute.
    """
    if not path:
        return ""
    if path.startswith("https://") or path.startswith("http://"):
        return path
    return f"{POSTER_BASE}{path}"


def load_env() -> dict[str, str]:
    required = ["TMDB_API_KEY", "SUPABASE_URL", "SUPABASE_KEY"]
    config: dict[str, str] = {}
    for key in required:
        value = os.environ.get(key)
        if not value:
            print(f"[ENRICH] ERROR: Required environment variable '{key}' is not set.")
            sys.exit(1)
        config[key] = value
    return config


def main() -> None:
    print("[ENRICH] Starting nightly enrichment bot...")
    config = load_env()

    db   = SupabaseClient(url=config["SUPABASE_URL"], key=config["SUPABASE_KEY"])
    tmdb = TmdbClient(api_key=config["TMDB_API_KEY"])

    cutoff     = datetime.utcnow() - timedelta(hours=6)
    cutoff_str = cutoff.strftime("%Y-%m-%dT%H:%M:%S")
    print(f"[ENRICH] Querying titles created since {cutoff_str} UTC...")

    new_titles = (
        db.client.table("media")
        .select(
            "id, tmdb_id, title, media_type, poster_path, release_date, "
            "is_in_theatres, is_streamable_now, title_logo_url, certification, "
            "genres, runtime"
        )
        .gte("created_at", cutoff_str)
        .execute()
        .data or []
    )
    total = len(new_titles)
    print(f"[ENRICH] {total} new title(s) to enrich.")

    if not total:
        print("[ENRICH] Nothing to do.")
        return

    stats = {
        "images":   0,
        "trailers": 0,
        "credits":  0,
        "seasons":  0,
        "logos":    0,
        "certs":    0,
        "runtimes": 0,
        "genres":   0,
        "states":   0,
    }

    for i, row in enumerate(new_titles, start=1):
        media_id    = row["id"]
        tmdb_id     = row["tmdb_id"]
        title       = row["title"]
        media_type  = row["media_type"]
        poster_path = row.get("poster_path") or ""

        print(f"\n[ENRICH] {i}/{total}: {title} ({media_type})")

        # ── Step 1: Images ──────────────────────────────────────────────
        try:
            poster_url    = None
            carousel_urls: list[str] = []
            if poster_path:
                poster_url = migrate_poster(db, tmdb_id, title, _full_image_url(poster_path))
            carousel_urls = migrate_carousel(db, tmdb, tmdb_id, title, media_type)
            if poster_url or carousel_urls:
                stats["images"] += 1
            print(
                f"[ENRICH]   images: poster={'ok' if poster_url else 'skip'}, "
                f"{len(carousel_urls)} carousel"
            )
        except Exception as exc:
            print(f"[ENRICH]   images: failed — {exc}")
        time.sleep(0.25)

        # ── Step 2: Trailers ────────────────────────────────────────────
        try:
            videos = tmdb.get_videos(tmdb_id=tmdb_id, media_type=media_type)
            n = db.upsert_trailers(media_id=media_id, trailers=videos)
            stats["trailers"] += n
            print(f"[ENRICH]   trailers: {n}")
        except Exception as exc:
            print(f"[ENRICH]   trailers: failed — {exc}")
        time.sleep(0.25)

        # ── Step 3: Credits ─────────────────────────────────────────────
        try:
            result = tmdb.get_credits(tmdb_id=tmdb_id, media_type=media_type)
            db.upsert_credits(
                media_id=media_id,
                directors=result["directors"],
                writers=result["writers"],
                cast=result["cast"],
                created_by=result["created_by"],
                producers=result["producers"],
            )
            n = (
                len(result["directors"]) + len(result["writers"])
                + len(result["cast"]) + len(result["producers"])
            )
            if n:
                stats["credits"] += 1
            print(f"[ENRICH]   credits: {n} rows")
        except Exception as exc:
            print(f"[ENRICH]   credits: failed — {exc}")
        time.sleep(0.25)

        # ── Step 4: Seasons (TV only) ────────────────────────────────────
        if media_type == "tv":
            try:
                seasons_raw = tmdb.get_seasons(tmdb_id=tmdb_id)
                for s in seasons_raw:
                    sp = s.get("poster_path", "")
                    if sp:
                        img = download_image(_full_image_url(sp))
                        if img:
                            url = db.upload_image(
                                BUCKET,
                                f"seasons/{tmdb_id}/{s['season_number']}.jpg",
                                img,
                            )
                            if url:
                                s["poster_url"] = url

                season_rows = db.upsert_seasons(media_id=media_id, seasons=seasons_raw)
                season_map  = {r["season_number"]: r["id"] for r in season_rows}
                ep_count    = 0
                for s in seasons_raw:
                    sid = season_map.get(s["season_number"])
                    if not sid:
                        continue
                    _, episodes = tmdb.get_season_episodes(
                        tmdb_id=tmdb_id, season_number=s["season_number"]
                    )
                    ep_count += db.upsert_episodes(season_id=sid, episodes=episodes)
                    time.sleep(0.25)
                db.update_tv_runtime(media_id=media_id)
                stats["seasons"] += len(season_rows)
                print(f"[ENRICH]   seasons: {len(season_rows)} seasons, {ep_count} episodes")
            except Exception as exc:
                print(f"[ENRICH]   seasons: failed — {exc}")
            time.sleep(0.25)

        # ── Step 5: Title logos ─────────────────────────────────────────
        if not row.get("title_logo_url"):
            try:
                logo_url = tmdb.get_title_logo(tmdb_id=tmdb_id, media_type=media_type)
                if logo_url:
                    db.client.table("media").update({"title_logo_url": logo_url}).eq("id", media_id).execute()
                    stats["logos"] += 1
                    print(f"[ENRICH]   logo: ok")
            except Exception as exc:
                print(f"[ENRICH]   logo: failed — {exc}")
            time.sleep(0.25)

        # ── Step 6: Certifications ──────────────────────────────────────
        if not row.get("certification"):
            try:
                cert = tmdb.get_certification(tmdb_id=tmdb_id, media_type=media_type)
                if cert:
                    db.client.table("media").update({"certification": cert}).eq("id", media_id).execute()
                    stats["certs"] += 1
                    print(f"[ENRICH]   cert: {cert}")
            except Exception as exc:
                print(f"[ENRICH]   cert: failed — {exc}")
            time.sleep(0.25)

        # ── Step 7: Runtime (movies only) ───────────────────────────────
        if media_type == "movie" and not row.get("runtime"):
            try:
                detail  = tmdb._get(f"/movie/{tmdb_id}")
                runtime = detail.get("runtime")
                if runtime:
                    db.client.table("media").update({"runtime": runtime}).eq("id", media_id).execute()
                    stats["runtimes"] += 1
                    print(f"[ENRICH]   runtime: {runtime} min")
            except Exception as exc:
                print(f"[ENRICH]   runtime: failed — {exc}")
            time.sleep(0.25)

        # ── Step 8: Genres ──────────────────────────────────────────────
        if not row.get("genres"):
            try:
                detail_data = tmdb._get(f"/{media_type}/{tmdb_id}")
                genres = [g["name"] for g in detail_data.get("genres", []) if g.get("name")]
                if genres:
                    db.client.table("media").update({"genres": genres}).eq("id", media_id).execute()
                    stats["genres"] += 1
                    print(f"[ENRICH]   genres: {genres}")
            except Exception as exc:
                print(f"[ENRICH]   genres: failed — {exc}")
            time.sleep(0.25)

        # ── Step 9: States ───────────────────────────────────────────────
        try:
            sa = (
                db.client.table("streaming_availability")
                .select("media_id")
                .eq("media_id", media_id)
                .limit(1)
                .execute()
            )
            has_providers = bool(sa.data)

            # is_in_theatres is never set here — main.py Step 13 (TMDB
            # /movie/now_playing) is the only source of truth for it.
            if has_providers:
                new_theatres, new_streamable = False, True
            else:
                new_theatres, new_streamable = False, False

            old_theatres   = row.get("is_in_theatres", False)
            old_streamable = row.get("is_streamable_now", False)

            if new_theatres != old_theatres or new_streamable != old_streamable:
                db.client.table("media").update({
                    "is_in_theatres":    new_theatres,
                    "is_streamable_now": new_streamable,
                }).eq("id", media_id).execute()
                stats["states"] += 1
                print(
                    f"[ENRICH]   state: in_theatres={new_theatres}, "
                    f"streamable={new_streamable}"
                )
        except Exception as exc:
            print(f"[ENRICH]   states: failed — {exc}")

    # ── Summary ──────────────────────────────────────────────────────────
    print(f"\n[ENRICH] ===== SUMMARY =====")
    print(f"[ENRICH] Titles processed : {total}")
    print(f"[ENRICH] Images           : {stats['images']}")
    print(f"[ENRICH] Trailers         : {stats['trailers']}")
    print(f"[ENRICH] Credits (titles) : {stats['credits']}")
    print(f"[ENRICH] Seasons inserted : {stats['seasons']}")
    print(f"[ENRICH] Title logos      : {stats['logos']}")
    print(f"[ENRICH] Certifications   : {stats['certs']}")
    print(f"[ENRICH] Runtimes         : {stats['runtimes']}")
    print(f"[ENRICH] Genres           : {stats['genres']}")
    print(f"[ENRICH] States updated   : {stats['states']}")


if __name__ == "__main__":
    main()
