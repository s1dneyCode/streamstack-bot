"""
Backfill: populate the genres text-array column for all rows in public.media
where genres IS NULL or array_length(genres, 1) <= 1 (missing or incomplete).

Run via:
    python -m bot.backfill_genres
"""

import os
import sys
import time

from .tmdb import TmdbClient
from .supabase_client import SupabaseClient

BATCH_SIZE = 50


def load_env() -> dict[str, str]:
    required = ["TMDB_API_KEY", "SUPABASE_URL", "SUPABASE_KEY"]
    config: dict[str, str] = {}
    for key in required:
        value = os.environ.get(key)
        if not value:
            print(f"[GENRE] ERROR: Required environment variable '{key}' is not set.")
            sys.exit(1)
        config[key] = value
    return config


def fetch_targets(db: SupabaseClient) -> list[dict]:
    """Return rows where genres IS NULL or array_length(genres, 1) <= 1.

    Pass 1: DB-side filter for IS NULL.
    Pass 2: fetch non-null rows with genres included, filter in Python for
            len <= 1 (PostgREST cannot filter on array_length directly).
    """
    page_size = 1000
    rows: list[dict] = []
    seen: set[str] = set()

    # Pass 1: genres IS NULL
    offset = 0
    while True:
        batch = (
            db.client.table("media")
            .select("id, tmdb_id, title, media_type")
            .is_("genres", "null")
            .range(offset, offset + page_size - 1)
            .execute()
            .data or []
        )
        for row in batch:
            seen.add(row["id"])
            rows.append(row)
        if len(batch) < page_size:
            break
        offset += page_size

    # Pass 2: genres IS NOT NULL — Python-filter for array_length <= 1
    offset = 0
    while True:
        batch = (
            db.client.table("media")
            .select("id, tmdb_id, title, media_type, genres")
            .not_.is_("genres", "null")
            .range(offset, offset + page_size - 1)
            .execute()
            .data or []
        )
        for row in batch:
            if row["id"] not in seen and len(row.get("genres") or []) <= 1:
                seen.add(row["id"])
                rows.append({"id": row["id"], "tmdb_id": row["tmdb_id"],
                             "title": row["title"], "media_type": row["media_type"]})
        if len(batch) < page_size:
            break
        offset += page_size

    return rows


def main() -> None:
    print("[GENRE] Starting genres backfill...")
    config = load_env()

    db   = SupabaseClient(url=config["SUPABASE_URL"], key=config["SUPABASE_KEY"])
    tmdb = TmdbClient(api_key=config["TMDB_API_KEY"])

    rows  = fetch_targets(db)
    total = len(rows)
    print(f"[GENRE] {total} rows to process.")

    updated = 0
    failed  = 0

    for i, row in enumerate(rows, start=1):
        tmdb_id    = row["tmdb_id"]
        title      = row["title"]
        media_type = row["media_type"]

        try:
            data   = tmdb._get(f"/{media_type}/{tmdb_id}")
            genres = [g["name"] for g in data.get("genres", []) if g.get("name")]
        except Exception as exc:
            print(f"[GENRE] {i}/{total} {title}: fetch failed — {exc}")
            failed += 1
            time.sleep(0.25)
            continue

        if genres:
            db.client.table("media").update({"genres": genres}).eq("id", row["id"]).execute()
            updated += 1
        else:
            print(f"[GENRE] {i}/{total} {title}: no genres found")
            failed += 1

        if i % 100 == 0:
            print(f"[GENRE] Progress: {i}/{total} processed ({updated} updated, {failed} failed)")

        time.sleep(0.25)

    print(f"[GENRE] Done. {total} processed — {updated} updated, {failed} failed.")


if __name__ == "__main__":
    main()
