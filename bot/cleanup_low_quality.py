"""
Monthly cleanup: delete low-quality titles that have accumulated no traction.

A title is deleted only when ALL conditions are true:
  - vote_count < 10
  - release_date < today - 90 days
  - is_in_theatres = false
  - is_streamable_now = false

Run via:
    python -m bot.cleanup_low_quality
"""

import os
import sys
from datetime import date, timedelta

from .supabase_client import SupabaseClient

PAGE_SIZE = 1000


def load_env() -> dict[str, str]:
    required = ["SUPABASE_URL", "SUPABASE_KEY"]
    config: dict[str, str] = {}
    for key in required:
        value = os.environ.get(key)
        if not value:
            print(f"[CLEANUP] ERROR: Required environment variable '{key}' is not set.")
            sys.exit(1)
        config[key] = value
    return config


def fetch_targets(db: SupabaseClient, cutoff: str) -> list[dict]:
    rows: list[dict] = []
    offset = 0
    while True:
        batch = (
            db.client.table("media")
            .select("id, title, vote_count, release_date")
            .lt("vote_count", 10)
            .lt("release_date", cutoff)
            .eq("is_in_theatres", False)
            .eq("is_streamable_now", False)
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
    print("[CLEANUP] Starting low-quality title cleanup...")
    config = load_env()
    db = SupabaseClient(url=config["SUPABASE_URL"], key=config["SUPABASE_KEY"])

    cutoff = (date.today() - timedelta(days=90)).isoformat()
    rows   = fetch_targets(db, cutoff)
    total  = len(rows)
    print(f"[CLEANUP] {total} titles matched deletion criteria.")

    deleted = 0
    failed  = 0

    for row in rows:
        try:
            db.client.table("media").delete().eq("id", row["id"]).execute()
            print(f"[CLEANUP] Deleted: {row['title']} (vote_count={row.get('vote_count')}, release={row.get('release_date')})")
            deleted += 1
        except Exception as exc:
            print(f"[CLEANUP] Failed to delete id={row['id']} '{row['title']}': {exc}")
            failed += 1

    print(f"[CLEANUP] Done. {deleted} titles deleted, {failed} failed.")


if __name__ == "__main__":
    main()
