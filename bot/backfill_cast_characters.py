"""
Backfill: repopulate media_credits.character for TV shows whose cast rows
were saved with an empty character by the old get_credits bug.

TMDB's /tv/{id}/aggregate_credits has no flat "character" field — each cast
person carries a `roles` array (one entry per character, each with an
episode_count). get_credits used to read m.get("character", "") there, so
every TV cast row landed with character = "". That's now fixed (it takes
the primary role, i.e. the one with the most episodes), so this re-fetches
each show's credits and re-upserts them. On the (media_id, name, role)
conflict key that UPDATES the existing person's character in place rather
than duplicating the row; profile_path is harmlessly re-written in the
same pass.

TV-ONLY on purpose: the movie branch reads /movie/{id}/credits, whose flat
"character" field was always correct, so movie rows must not be touched.

Resumable: a show is skipped when it already has at least one non-empty
cast character — either a previous run fixed it, or the nightly re-enriched
it after the fix landed. Re-running only picks up what's still broken.

Run via:
    python -m bot.backfill_cast_characters
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
            print(f"[CASTCHARS] ERROR: Required environment variable '{key}' is not set.")
            sys.exit(1)
        config[key] = value
    return config


def fetch_targets(db: SupabaseClient) -> list[dict]:
    """
    Return every TV row (id, tmdb_id, title) in public.media, paginated.

    Unlike backfill_cast_photos, the target set isn't narrowed by a credits
    condition here: "has an empty character" can't be expressed as a
    correlated subquery through PostgREST, and filtering media_credits on
    character = '' would pull in movie rows too. The per-show resumability
    guard in main() does that filtering instead — it costs one cheap
    indexed lookup per show, which is far cheaper than a wasted TMDB call.
    """
    print("[CASTCHARS] Loading TV titles from media...")

    rows: list[dict] = []
    offset = 0
    while True:
        batch = (
            db.client.table("media")
            .select("id, tmdb_id, title")
            .eq("media_type", "tv")
            .range(offset, offset + PAGE_SIZE - 1)
            .execute()
            .data or []
        )
        rows.extend(batch)
        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    print(f"[CASTCHARS] {len(rows)} TV title(s) found.")
    return rows


def already_fixed(db: SupabaseClient, media_id: str) -> bool:
    """
    True if this show already has at least one cast credit with a non-empty
    character — i.e. nothing to do. Uses .neq("character", "") plus a NOT
    NULL guard so a row with character IS NULL (never populated) doesn't
    count as fixed.
    """
    rows = (
        db.client.table("media_credits")
        .select("id")
        .eq("media_id", media_id)
        .eq("role", "cast")
        .not_.is_("character", "null")
        .neq("character", "")
        .limit(1)
        .execute()
        .data or []
    )
    return bool(rows)


def main() -> None:
    print("[CASTCHARS] Starting cast character backfill (TV only)...")
    config = load_env()
    db   = SupabaseClient(url=config["SUPABASE_URL"], key=config["SUPABASE_KEY"])
    tmdb = TmdbClient(api_key=config["TMDB_API_KEY"])

    rows  = fetch_targets(db)
    total = len(rows)
    print(f"[CASTCHARS] {total} title(s) to process.")

    processed = 0
    skipped   = 0
    missing   = 0
    failed    = 0
    credits_updated = 0

    for i, row in enumerate(rows, start=1):
        media_id = row["id"]
        tmdb_id  = row["tmdb_id"]
        title    = row["title"]

        # Whole per-title body isolated — one bad title must not abort the run.
        try:
            # Resumability guard: skip before spending a TMDB call.
            if already_fixed(db, media_id):
                skipped += 1
                continue

            # data=None on purpose — get_credits fetches /tv/{id} (for
            # created_by) and /tv/{id}/aggregate_credits itself, and now
            # derives character from the roles array.
            result = tmdb.get_credits(tmdb_id=tmdb_id, media_type="tv", data=None)

            if not result["cast"]:
                missing += 1
                continue

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
            print(f"[CASTCHARS] {i}/{total} {title}: {len(result['cast'])} cast character(s) refreshed.")
        except Exception as exc:
            print(f"[CASTCHARS] {i}/{total} {title}: failed — {exc}")
            failed += 1

        if i % 100 == 0:
            print(
                f"[CASTCHARS] Progress: {i}/{total} "
                f"({processed} processed, {skipped} skipped, {missing} missing, {failed} failed)"
            )

        time.sleep(0.25)

    print(
        f"\n[CASTCHARS] Done. {total} TV titles scanned — {processed} processed, "
        f"{skipped} skipped (already fixed), {missing} missing (no cast returned), "
        f"{failed} failed, {credits_updated} credit rows upserted."
    )


if __name__ == "__main__":
    main()
