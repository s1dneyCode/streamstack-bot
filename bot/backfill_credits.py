"""
One-time backfill: fetch and store credits for all media titles that
currently have no entries in media_credits.

Flags:
    --media-type movie|tv   Only process titles of that type (default: all)
    --role producer         Re-run only the producer role for ALL titles
                            (upserts producer rows, ignores has_credits check)

Examples:
    python -m bot.backfill_credits
    python -m bot.backfill_credits --media-type tv
    python -m bot.backfill_credits --role producer
"""

import os
import sys
import time

from .tmdb import TmdbClient
from .supabase_client import SupabaseClient


def load_env() -> dict[str, str]:
    required = ["TMDB_API_KEY", "SUPABASE_URL", "SUPABASE_KEY"]
    config: dict[str, str] = {}
    for key in required:
        value = os.environ.get(key)
        if not value:
            print(f"[CREDITS] ERROR: Required environment variable '{key}' is not set.")
            sys.exit(1)
        config[key] = value
    return config


def main() -> None:
    args = sys.argv[1:]

    # --media-type flag
    media_type_filter: str | None = None
    if "--media-type" in args:
        idx = args.index("--media-type")
        if idx + 1 >= len(args):
            print("[CREDITS] ERROR: --media-type requires a value (movie or tv).")
            sys.exit(1)
        media_type_filter = args[idx + 1]
        if media_type_filter not in ("movie", "tv"):
            print(f"[CREDITS] ERROR: --media-type must be 'movie' or 'tv', got '{media_type_filter}'.")
            sys.exit(1)

    # --role flag
    role_filter: str | None = None
    if "--role" in args:
        idx = args.index("--role")
        if idx + 1 >= len(args):
            print("[CREDITS] ERROR: --role requires a value (e.g. producer).")
            sys.exit(1)
        role_filter = args[idx + 1]

    producer_only = role_filter == "producer"

    label_parts = []
    if media_type_filter:
        label_parts.append(f"{media_type_filter} only")
    if producer_only:
        label_parts.append("producer role backfill")
    label = f" ({', '.join(label_parts)})" if label_parts else ""

    print(f"[CREDITS] Starting credits backfill{label}...")
    config = load_env()

    db   = SupabaseClient(url=config["SUPABASE_URL"], key=config["SUPABASE_KEY"])
    tmdb = TmdbClient(api_key=config["TMDB_API_KEY"])

    # Fetch media titles (paginated), optionally filtered by media_type.
    # Default mode: left join to skip titles that already have credits.
    # producer_only mode: process all titles regardless of existing credits.
    page_size = 1000
    offset    = 0
    targets: list[dict] = []

    while True:
        if producer_only:
            query = db.client.table("media").select("id, tmdb_id, title, media_type")
        else:
            query = (
                db.client.table("media")
                .select("id, tmdb_id, title, media_type, media_credits!left(media_id)")
                .filter("media_credits.media_id", "is", "null")
            )
        if media_type_filter:
            query = query.eq("media_type", media_type_filter)
        batch = query.range(offset, offset + page_size - 1).execute().data or []
        targets.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size

    total = len(targets)
    print(f"[CREDITS] {total} titles to process.")

    total_directors  = 0
    total_writers    = 0
    total_cast       = 0
    total_created_by = 0
    total_producers  = 0
    no_credits       = 0

    for i, row in enumerate(targets, start=1):
        result = tmdb.get_credits(tmdb_id=row["tmdb_id"], media_type=row["media_type"])

        directors  = result["directors"]
        writers    = result["writers"]
        cast       = result["cast"]
        created_by = result["created_by"]
        producers  = result["producers"]

        if producer_only:
            # Only upsert producer rows; leave other roles untouched
            count = db.upsert_credits(
                media_id=row["id"],
                directors=[],
                writers=[],
                cast=[],
                created_by=[],
                producers=producers,
            )
        else:
            count = db.upsert_credits(
                media_id=row["id"],
                directors=directors,
                writers=writers,
                cast=cast,
                created_by=created_by,
                producers=producers,
            )

        total_directors  += len(directors)  if not producer_only else 0
        total_writers    += len(writers)    if not producer_only else 0
        total_cast       += len(cast)       if not producer_only else 0
        total_created_by += len(created_by) if not producer_only else 0
        total_producers  += len(producers)

        if count == 0:
            no_credits += 1

        print(
            f"[CREDITS] {i}/{total} {row['title']} ({row['media_type']}): "
            + (f"{len(producers)}p" if producer_only else
               f"{len(directors)}d / {len(writers)}w / {len(cast)}c / {len(created_by)}cb / {len(producers)}p")
        )
        time.sleep(0.25)

    if producer_only:
        print(
            f"[CREDITS] Done. {total} titles processed — "
            f"{total_producers} producers inserted. "
            f"{no_credits} titles with no producers found."
        )
    else:
        print(
            f"[CREDITS] Done. {total} titles processed — "
            f"{total_directors} directors, {total_writers} writers, "
            f"{total_cast} cast, {total_created_by} created_by, {total_producers} producers inserted. "
            f"{no_credits} titles with no credits found."
        )


if __name__ == "__main__":
    main()
