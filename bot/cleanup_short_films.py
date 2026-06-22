"""
Minimal, always-destructive cleanup: delete movies with a known runtime
under 40 minutes that aren't in theatres or streaming.

Equivalent to:
    DELETE FROM media
    WHERE media_type = 'movie'
      AND runtime < 40
      AND runtime IS NOT NULL
      AND is_in_theatres = false
      AND is_streamable_now = false

No --dry-run — intended to run unattended at the end of
full_backfill_new.yml, right after backfill_runtime.py has filled in
real runtime values for movies that slipped past bulk_import.py's
insert-time filter (which never triggers, since movie list/discover
endpoints don't return runtime).

Run via:
    python -m bot.cleanup_short_films
"""

import os
import sys

from .supabase_client import SupabaseClient

PAGE_SIZE = 1000


def load_env() -> dict[str, str]:
    required = ["SUPABASE_URL", "SUPABASE_KEY"]
    config: dict[str, str] = {}
    for key in required:
        value = os.environ.get(key)
        if not value:
            print(f"[CLEANUP_SHORT] ERROR: Required environment variable '{key}' is not set.")
            sys.exit(1)
        config[key] = value
    return config


def fetch_short_films(db: SupabaseClient) -> list[dict]:
    rows: list[dict] = []
    offset = 0
    while True:
        batch = (
            db.client.table("media")
            .select("id, title, runtime")
            .eq("media_type", "movie")
            .lt("runtime", 40)
            .not_.is_("runtime", "null")
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
    print("[CLEANUP_SHORT] Starting short film cleanup...")
    config = load_env()
    db = SupabaseClient(url=config["SUPABASE_URL"], key=config["SUPABASE_KEY"])

    rows  = fetch_short_films(db)
    total = len(rows)
    print(f"[CLEANUP_SHORT] {total} short films matched deletion criteria.")

    deleted = 0
    failed  = 0

    for row in rows:
        try:
            db.client.table("media").delete().eq("id", row["id"]).execute()
            print(f"[CLEANUP_SHORT] Deleted: {row['title']} (runtime={row.get('runtime')}min)")
            deleted += 1
        except Exception as exc:
            print(f"[CLEANUP_SHORT] Failed to delete id={row['id']} '{row['title']}': {exc}")
            failed += 1

    print(f"[CLEANUP_SHORT] Done. {deleted} short films deleted, {failed} failed.")


if __name__ == "__main__":
    main()
