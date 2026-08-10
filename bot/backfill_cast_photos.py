"""
Backfill: populate media_credits.profile_path (actor/crew headshots) for
titles whose credits were ingested before that column existed.

TMDB returns profile_path in the same credits payload the bot already
fetches — it was simply discarded until now — so this re-fetches each
title's credits once and re-upserts them. Because media_credits' conflict
key is (media_id, name, role), the re-upsert UPDATES the existing person's
row with their profile_path rather than duplicating it.

Targets titles that have at least one role='cast' credit with
profile_path IS NULL. That makes the run resumable (a title already
backfilled drops out of the target set) and skips titles with no credits
at all — those have never been enriched, and enrich_new_titles.py will
populate them with profile_path included.

Run via:
    python -m bot.backfill_cast_photos
"""

import os
import sys
import time

from .tmdb import TmdbClient
from .supabase_client import SupabaseClient

PAGE_SIZE = 1000


def load_env() -> dict[str, str]:
    required = ["TMDB_API_KEY", "SUPABASE_URL", "SUPABASE_KEY"]
    config: dict[str, str] = {}
    for key in required:
        value = os.environ.get(key)
        if not value:
            print(f"[CASTPHOTOS] ERROR: Required environment variable '{key}' is not set.")
            sys.exit(1)
        config[key] = value
    return config


def fetch_targets(db: SupabaseClient) -> list[dict]:
    """
    Return media rows (id, tmdb_id, title, media_type) that still have at
    least one cast credit without a profile_path.

    Paginates media_credits (not media) because the condition lives on the
    credits table, then resolves the distinct media_ids back to their media
    rows. Chunked at 200 ids per .in_() lookup — media.id is a uuid, so a
    longer list would push the query URL toward the gateway's header limit.
    """
    print("[CASTPHOTOS] Scanning media_credits for cast rows without a profile_path...")

    media_ids: list[str] = []
    seen: set[str] = set()
    offset = 0
    while True:
        batch = (
            db.client.table("media_credits")
            .select("media_id")
            .eq("role", "cast")
            .is_("profile_path", "null")
            .range(offset, offset + PAGE_SIZE - 1)
            .execute()
            .data or []
        )
        for row in batch:
            mid = row["media_id"]
            if mid not in seen:
                seen.add(mid)
                media_ids.append(mid)
        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    print(f"[CASTPHOTOS] {len(media_ids)} distinct title(s) need cast photos.")

    rows: list[dict] = []
    chunk_size = 200
    for i in range(0, len(media_ids), chunk_size):
        chunk = media_ids[i:i + chunk_size]
        try:
            batch = (
                db.client.table("media")
                .select("id, tmdb_id, title, media_type")
                .in_("id", chunk)
                .execute()
                .data or []
            )
        except Exception as exc:
            print(f"[CASTPHOTOS] Error resolving a chunk of media ids: {exc}")
            continue
        rows.extend(batch)

    return rows


def main() -> None:
    print("[CASTPHOTOS] Starting cast photo backfill...")
    config = load_env()
    db   = SupabaseClient(url=config["SUPABASE_URL"], key=config["SUPABASE_KEY"])
    tmdb = TmdbClient(api_key=config["TMDB_API_KEY"])

    rows  = fetch_targets(db)
    total = len(rows)
    print(f"[CASTPHOTOS] {total} title(s) to process.")

    processed = 0
    credits_updated = 0
    failed = 0

    for i, row in enumerate(rows, start=1):
        media_id   = row["id"]
        tmdb_id    = row["tmdb_id"]
        title      = row["title"]
        media_type = row["media_type"]

        # Whole per-title body isolated — one bad title must not abort the run.
        try:
            # One appended detail call, same shape as enrich_new_titles.py's
            # Step 2+3: get_credits(data=...) expects the *detail* payload
            # with credits (movie) or aggregate_credits (tv) appended, since
            # TV's created_by is native to the base detail response.
            append = "aggregate_credits" if media_type == "tv" else "credits"
            detail = tmdb._get(f"/{media_type}/{tmdb_id}", params={"append_to_response": append})

            result = tmdb.get_credits(tmdb_id=tmdb_id, media_type=media_type, data=detail)
            n = db.upsert_credits(
                media_id=media_id,
                directors=result["directors"],
                writers=result["writers"],
                cast=result["cast"],
                created_by=result["created_by"],
                producers=result["producers"],
            )
            credits_updated += n
            processed += 1
        except Exception as exc:
            print(f"[CASTPHOTOS] {i}/{total} {title}: failed — {exc}")
            failed += 1

        if i % 100 == 0:
            print(f"[CASTPHOTOS] Progress: {i}/{total} processed ({credits_updated} credits upserted, {failed} failed)")

        time.sleep(0.25)

    print(
        f"\n[CASTPHOTOS] Done. {processed}/{total} titles processed, "
        f"{credits_updated} credit rows upserted, {failed} failed."
    )


if __name__ == "__main__":
    main()
