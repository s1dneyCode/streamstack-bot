"""
One-time backfill: populate rt_score for movies in public.media using OMDb.

TV shows are skipped — OMDb does not provide RT scores for series.

Phase 1: fill missing imdb_ids via TMDB external_ids endpoint. Since B5.4 it
asks through TmdbClient.lookup_imdb_id and writes like bot/backfill_imdb_ids.py:
by the row's id — never by tmdb_id, which a show can share with the movie —
the id and imdb_checked_at for FOUND, imdb_checked_at alone for NONE and
MISSING, nothing for ERROR.
Phase 2: update RT scores using OMDb, with imdb_id as the primary lookup strategy.

By default, Phase 2 only processes movies where rt_score IS NULL (cheap
enough to run after every bulk import). Pass --force to re-check every
movie regardless of current rt_score, for periodic full refreshes.

Run via:
    python -m bot.backfill_rt_scores
    python -m bot.backfill_rt_scores --force
"""

import os
import sys
import time
from datetime import datetime, timezone

from .tmdb import IMDB_ERROR, IMDB_FOUND, IMDB_MISSING, TmdbClient
from .omdb import OmdbClient
from .supabase_client import SupabaseClient


def load_env() -> dict[str, str]:
    required = ["TMDB_API_KEY", "OMDB_API_KEY", "SUPABASE_URL", "SUPABASE_KEY"]
    config: dict[str, str] = {}
    for key in required:
        value = os.environ.get(key)
        if not value:
            print(f"[RT_BACKFILL] ERROR: Required environment variable '{key}' is not set.")
            sys.exit(1)
        config[key] = value
    return config


def extract_year(release_date: str | None) -> str | None:
    if release_date and len(release_date) >= 4:
        return release_date[:4]
    return None


def main(clock=None) -> None:
    """*clock* returns the current instant (aware UTC), phase 1's
    imdb_checked_at; injected by tests, the real clock otherwise."""
    print("[RT_BACKFILL] Starting RT score backfill...")
    force = "--force" in sys.argv[1:]
    config = load_env()
    clock = clock if clock is not None else (lambda: datetime.now(timezone.utc))

    db   = SupabaseClient(url=config["SUPABASE_URL"], key=config["SUPABASE_KEY"])
    tmdb = TmdbClient(api_key=config["TMDB_API_KEY"])
    omdb = OmdbClient(api_key=config["OMDB_API_KEY"])

    # ------------------------------------------------------------------
    # Phase 1 — fill missing imdb_ids
    # ------------------------------------------------------------------
    missing = db.get_movies_without_imdb_id()
    print(f"[RT_BACKFILL] Phase 1: filling imdb_id for {len(missing)} movies...")

    for i, row in enumerate(missing, start=1):
        tmdb_id = row["tmdb_id"]
        title   = row["title"]

        result = tmdb.lookup_imdb_id(tmdb_id=tmdb_id, media_type="movie")
        if result.outcome == IMDB_ERROR:
            print(f"[RT_BACKFILL] {i}/{len(missing)} {title}: imdb_id lookup failed ({result.reason}) — not stamped")
        else:
            now = clock().isoformat()
            fields = {"imdb_checked_at": now}
            if result.outcome == IMDB_FOUND:
                fields = {"imdb_id": result.imdb_id, "imdb_checked_at": now}
            db.client.table("media").update(fields).eq("id", row["id"]).execute()
            if result.outcome == IMDB_FOUND:
                print(f"[RT_BACKFILL] {i}/{len(missing)} {title}: imdb_id={result.imdb_id}")
            elif result.outcome == IMDB_MISSING:
                print(f"[RT_BACKFILL] {i}/{len(missing)} {title}: gone from TMDb (404)")
            else:
                print(f"[RT_BACKFILL] {i}/{len(missing)} {title}: imdb_id not found")

        time.sleep(0.2)

    # ------------------------------------------------------------------
    # Phase 2 — update RT scores
    # ------------------------------------------------------------------
    page_size = 1000
    offset = 0
    rows: list[dict] = []

    while True:
        query = (
            db.client.table("media")
            .select("id, tmdb_id, title, media_type, release_date, imdb_id")
            .eq("media_type", "movie")
        )
        if not force:
            query = query.is_("rt_score", "null")
        response = query.range(offset, offset + page_size - 1).execute()
        batch = response.data or []
        rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size

    total = len(rows)
    scope = "all movies (--force)" if force else "movies missing rt_score"
    print(f"[RT_BACKFILL] Phase 2: updating RT scores for {total} {scope}...")

    updated = 0
    not_found = 0

    for i, row in enumerate(rows, start=1):
        tmdb_id = row["tmdb_id"]
        title   = row["title"]
        year    = extract_year(row.get("release_date"))
        imdb_id = row.get("imdb_id")

        score = omdb.get_rt_score(title=title, year=year, imdb_id=imdb_id)

        if score is not None:
            db.client.table("media").update({"rt_score": score}).eq("tmdb_id", tmdb_id).execute()
            print(f"[RT_BACKFILL] {i}/{total} {title} ({year}): {score}%")
            updated += 1
        else:
            print(f"[RT_BACKFILL] {i}/{total} {title}: not found")
            not_found += 1

        time.sleep(0.5)

    print(f"[RT_BACKFILL] Done. {updated} updated, {not_found} not found, 0 skipped (TV).")


if __name__ == "__main__":
    main()
