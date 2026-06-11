"""
Backfill: populate vote_count for all rows in public.media where vote_count IS NULL.

Calls /movie/{tmdb_id} or /tv/{tmdb_id} to retrieve vote_count.

Run via:
    python -m bot.backfill_vote_count
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
            print(f"[VCOUNT] ERROR: Required environment variable '{key}' is not set.")
            sys.exit(1)
        config[key] = value
    return config


def fetch_targets(db: SupabaseClient) -> list[dict]:
    rows: list[dict] = []
    offset = 0
    while True:
        batch = (
            db.client.table("media")
            .select("id, tmdb_id, title, media_type")
            .is_("vote_count", "null")
            .range(offset, offset + PAGE_SIZE - 1)
            .execute()
            .data or []
        )
        rows.extend(batch)
        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return rows


def main() -> None:
    print("[VCOUNT] Starting vote_count backfill...")
    config = load_env()
    db   = SupabaseClient(url=config["SUPABASE_URL"], key=config["SUPABASE_KEY"])
    tmdb = TmdbClient(api_key=config["TMDB_API_KEY"])

    rows  = fetch_targets(db)
    total = len(rows)
    print(f"[VCOUNT] {total} rows to process.")

    updated = 0
    failed  = 0

    for i, row in enumerate(rows, start=1):
        tmdb_id    = row["tmdb_id"]
        title      = row["title"]
        media_type = row["media_type"]

        try:
            data       = tmdb._get(f"/{media_type}/{tmdb_id}")
            vote_count = data.get("vote_count")
            if vote_count is not None:
                db.client.table("media").update({"vote_count": vote_count}).eq("id", row["id"]).execute()
                updated += 1
            else:
                print(f"[VCOUNT] {i}/{total} {title}: vote_count not in response")
                failed += 1
        except Exception as exc:
            print(f"[VCOUNT] {i}/{total} {title}: failed — {exc}")
            failed += 1

        if i % 100 == 0:
            print(f"[VCOUNT] Progress: {i}/{total} processed ({updated} updated, {failed} failed)")

        time.sleep(0.25)

    print(f"[VCOUNT] Done. {total} processed — {updated} updated, {failed} failed.")


if __name__ == "__main__":
    main()
