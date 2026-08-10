"""
Enrichment bot: state-driven scan that populates all secondary fields for
any title that hasn't been enriched yet, regardless of when it was created.

Selection is WHERE enriched_at IS NULL AND tmdb_missing_at IS NULL, newest
created_at first, capped at --limit (or the ENRICH_LIMIT env var, default
500) titles per run. This replaces the old created_at >= NOW() - Xh
time-window query — a title missed by one run (nightly ran long,
enrichment itself failed, etc.) is no longer orphaned forever; it just
stays NULL and gets picked up by the next run. Bare rows from
bulk_import.py and sync_changes.py self-enrich the same way, no manual
full-backfill needed.

A row's enriched_at is stamped only if every step below ran without
raising — an empty result (e.g. no trailer exists on TMDB) still counts as
a completed attempt. If any step raises, enriched_at is left NULL, the
failure is logged and counted, and the row is retried on a future run
(never retried within the same run — a poison row can't loop forever). The
one exception is a 404 on the base detail fetch (title removed from TMDB
entirely) — that sets tmdb_missing_at instead, so the row is never
retried again rather than looping forever on a title that can't succeed.

Every step except States is skipped when its target is already populated,
making the script idempotent — safe to re-run without wasting API calls.

Steps per title (in order):
  0. Base detail    — existence check; a 404 here flags tmdb_missing_at and
                       skips the rest of the row (always runs)
  1. Images         — upload poster + carousel (skipped if poster_url already set)
  2. Trailers       — fetch YouTube trailers from TMDB (skipped if media_trailers rows exist)
  3. Credits        — fetch cast, directors, writers, producers (skipped if media_credits rows exist)
  4. Seasons        — fetch seasons + episodes (TV only, skipped if media_seasons rows exist)
  5. Title logos    — fetch title logo URL (skipped if already set)
  6. Certifications — fetch US certification (skipped if already set)
  7. Runtime        — fetch runtime minutes (movies only, skipped if already set)
  8. Genres         — fill genres array (skipped if already set)
  9. States         — update is_in_theatres / is_streamable_now (always runs, no API call)

Run via:
    python -m bot.enrich_new_titles
    python -m bot.enrich_new_titles --limit=1000
"""

import os
import sys
import time
from datetime import datetime, timezone

import requests

from .tmdb import TmdbClient
from .supabase_client import SupabaseClient
from .migrate_images import migrate_poster, migrate_carousel, download_image

POSTER_BASE = "https://image.tmdb.org/t/p/w500"
BUCKET      = "media-images"

DEFAULT_ENRICH_LIMIT = 500


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
    print("[ENRICH] Starting enrichment bot...")

    limit = None
    for arg in sys.argv[1:]:
        if arg.startswith("--limit="):
            limit = int(arg.split("=")[1])
    if limit is None:
        limit = int(os.environ.get("ENRICH_LIMIT", DEFAULT_ENRICH_LIMIT))

    config = load_env()

    db   = SupabaseClient(url=config["SUPABASE_URL"], key=config["SUPABASE_KEY"])
    tmdb = TmdbClient(api_key=config["TMDB_API_KEY"])

    print(f"[ENRICH] Scanning up to {limit} title(s) where enriched_at IS NULL (newest created_at first)...")

    new_titles = (
        db.client.table("media")
        .select(
            "id, tmdb_id, title, media_type, poster_path, poster_url, release_date, "
            "is_in_theatres, is_streamable_now, title_logo_url, certification, "
            "genres, runtime"
        )
        .is_("enriched_at", "null")
        .is_("tmdb_missing_at", "null")
        .order("created_at", desc=True)
        .limit(limit)
        .execute()
        .data or []
    )
    scanned = len(new_titles)
    print(f"[ENRICH] {scanned} title(s) to process this run.")

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
        "errors":   0,
        "missing":  0,
    }
    enriched_count = 0

    for i, row in enumerate(new_titles, start=1):
        media_id    = row["id"]
        tmdb_id     = row["tmdb_id"]
        title       = row["title"]
        media_type  = row["media_type"]
        poster_path = row.get("poster_path") or ""

        print(f"\n[ENRICH] {i}/{scanned}: {title} ({media_type})")

        # Tracks whether ANY step below raised for this row. Only a clean
        # sweep (no exceptions — an empty/not-found result is NOT an
        # exception) gets enriched_at stamped at the end.
        row_had_error = False

        # ── Step 0: Base detail — existence check ────────────────────────
        # A confirmed 404 means TMDB no longer has this title at all; every
        # other step below would 404 too, so flag it and move on instead of
        # burning the remaining API calls finding that out step by step.
        #
        # The payload is kept (not discarded) because it already carries
        # runtime and genres — Steps 7 and 8 reuse it instead of re-fetching
        # the identical endpoint. Initialized to {} before the try so it is
        # always defined: on a non-404 failure it stays {} and those steps
        # fall back to their own fetch, exactly as they did before.
        base_detail: dict = {}
        try:
            base_detail = tmdb._get(f"/{media_type}/{tmdb_id}")
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                db.set_tmdb_missing(media_id)
                stats["missing"] += 1
                print("[ENRICH]   base detail: 404 — gone from TMDB, flagged tmdb_missing_at (won't retry).")
                continue
            print(f"[ENRICH]   base detail: failed — {exc}")
            stats["errors"] += 1
            row_had_error = True
        except Exception as exc:
            print(f"[ENRICH]   base detail: failed — {exc}")
            stats["errors"] += 1
            row_had_error = True

        # ── Step 1: Images ──────────────────────────────────────────────
        if not row.get("poster_url"):
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
                stats["errors"] += 1
                row_had_error = True
            time.sleep(0.25)

        # ── Step 2+3: Trailers & Credits (one combined detail call) ──────
        # Cheap DB-only existence checks first; only if at least one of the
        # two is missing do we hit TMDB, and then only once — videos and
        # credits (or aggregate_credits for TV) appended to the same
        # /movie|tv/{id} call instead of two separate requests.
        try:
            has_trailers = bool(
                db.client.table("media_trailers")
                .select("media_id").eq("media_id", media_id).limit(1).execute().data
            )
            has_credits = bool(
                db.client.table("media_credits")
                .select("media_id").eq("media_id", media_id).limit(1).execute().data
            )

            append_fields = []
            if not has_trailers:
                append_fields.append("videos")
            if not has_credits:
                append_fields.append("aggregate_credits" if media_type == "tv" else "credits")

            detail: dict = {}
            if append_fields:
                detail = tmdb._get(
                    f"/{media_type}/{tmdb_id}",
                    params={"append_to_response": ",".join(append_fields)},
                )

            if not has_trailers:
                videos = tmdb.get_videos(tmdb_id=tmdb_id, media_type=media_type, data=detail.get("videos", {}))
                n = db.upsert_trailers(media_id=media_id, trailers=videos)
                stats["trailers"] += n
                print(f"[ENRICH]   trailers: {n}")

            if not has_credits:
                result = tmdb.get_credits(tmdb_id=tmdb_id, media_type=media_type, data=detail)
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
            print(f"[ENRICH]   trailers/credits: failed — {exc}")
            stats["errors"] += 1
            row_had_error = True

        # ── Step 4: Seasons (TV only) ────────────────────────────────────
        if media_type == "tv":
            try:
                existing = (
                    db.client.table("media_seasons")
                    .select("media_id")
                    .eq("media_id", media_id)
                    .limit(1)
                    .execute()
                )
                if not existing.data:
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
                    db.update_tv_runtime(media_id=media_id)
                    stats["seasons"] += len(season_rows)
                    print(f"[ENRICH]   seasons: {len(season_rows)} seasons, {ep_count} episodes")
            except Exception as exc:
                print(f"[ENRICH]   seasons: failed — {exc}")
                stats["errors"] += 1
                row_had_error = True
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
                stats["errors"] += 1
                row_had_error = True

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
                stats["errors"] += 1
                row_had_error = True

        # ── Step 7: Runtime (movies only) ───────────────────────────────
        if media_type == "movie" and not row.get("runtime"):
            try:
                # Reuse Step 0's payload (same endpoint, already carries
                # runtime); only re-fetch if Step 0 failed.
                detail  = base_detail if base_detail else tmdb._get(f"/movie/{tmdb_id}")
                runtime = detail.get("runtime")
                if runtime:
                    db.client.table("media").update({"runtime": runtime}).eq("id", media_id).execute()
                    stats["runtimes"] += 1
                    print(f"[ENRICH]   runtime: {runtime} min")
            except Exception as exc:
                print(f"[ENRICH]   runtime: failed — {exc}")
                stats["errors"] += 1
                row_had_error = True

        # ── Step 8: Genres ──────────────────────────────────────────────
        if not row.get("genres"):
            try:
                # Reuse Step 0's payload (same endpoint, already carries
                # genres); only re-fetch if Step 0 failed.
                detail_data = base_detail if base_detail else tmdb._get(f"/{media_type}/{tmdb_id}")
                genres = [g["name"] for g in detail_data.get("genres", []) if g.get("name")]
                if genres:
                    db.client.table("media").update({"genres": genres}).eq("id", media_id).execute()
                    stats["genres"] += 1
                    print(f"[ENRICH]   genres: {genres}")
            except Exception as exc:
                print(f"[ENRICH]   genres: failed — {exc}")
                stats["errors"] += 1
                row_had_error = True

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

            # is_in_theatres is never set here — daily_verify.py (TMDB
            # /movie/now_playing, runs daily at 8am UTC) is the only
            # source of truth for it.
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
            stats["errors"] += 1
            row_had_error = True

        # ── Mark this row's enrichment attempt complete ───────────────────
        if row_had_error:
            print("[ENRICH]   not marking enriched — at least one step failed, will retry next run.")
        else:
            try:
                db.client.table("media").update(
                    {"enriched_at": datetime.now(timezone.utc).isoformat()}
                ).eq("id", media_id).execute()
                enriched_count += 1
            except Exception as exc:
                print(f"[ENRICH]   failed to stamp enriched_at: {exc}")
                stats["errors"] += 1

    # ── Per-field breakdown ─────────────────────────────────────────────
    if scanned:
        print(f"\n[ENRICH] ===== FIELD BREAKDOWN =====")
        print(f"[ENRICH] Images           : {stats['images']}")
        print(f"[ENRICH] Trailers         : {stats['trailers']}")
        print(f"[ENRICH] Credits (titles) : {stats['credits']}")
        print(f"[ENRICH] Seasons inserted : {stats['seasons']}")
        print(f"[ENRICH] Title logos      : {stats['logos']}")
        print(f"[ENRICH] Certifications   : {stats['certs']}")
        print(f"[ENRICH] Runtimes         : {stats['runtimes']}")
        print(f"[ENRICH] Genres           : {stats['genres']}")
        print(f"[ENRICH] States updated   : {stats['states']}")

    # ── Summary ──────────────────────────────────────────────────────────
    remaining_null = (
        db.client.table("media")
        .select("id", count="exact")
        .is_("enriched_at", "null")
        .is_("tmdb_missing_at", "null")
        .limit(1)
        .execute()
        .count or 0
    )
    print(
        f"\n[ENRICH] SUMMARY scanned={scanned} enriched={enriched_count} "
        f"missing={stats['missing']} errors={stats['errors']} remaining_null={remaining_null}"
    )


if __name__ == "__main__":
    main()
