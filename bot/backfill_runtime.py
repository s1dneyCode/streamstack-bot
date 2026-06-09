"""
One-time backfill: fetch and store runtime (minutes) for all movies that
currently have runtime IS NULL in the media table.

Run via:
    python -m bot.backfill_runtime
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
            print(f"[RUNTIME] ERROR: Required environment variable '{key}' is not set.")
            sys.exit(1)
        config[key] = value
    return config


def main() -> None:
    print("[RUNTIME] Starting runtime backfill...")
    config = load_env()

    db   = SupabaseClient(url=config["SUPABASE_URL"], key=config["SUPABASE_KEY"])
    tmdb = TmdbClient(api_key=config["TMDB_API_KEY"])

    # Fetch all movies with runtime IS NULL (paginated)
    page_size = 1000
    offset    = 0
    needs_runtime: list[dict] = []
    while True:
        batch = (
            db.client.table("media")
            .select("id, tmdb_id, title")
            .eq("media_type", "movie")
            .is_("runtime", "null")
            .range(offset, offset + page_size - 1)
            .execute()
            .data or []
        )
        needs_runtime.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size

    total = len(needs_runtime)
    print(f"[RUNTIME] {total} movies need runtime backfill.")

    updated    = 0
    no_runtime = 0

    for i, row in enumerate(needs_runtime, start=1):
        try:
            detail  = tmdb._get(f"/movie/{row['tmdb_id']}")
            runtime = detail.get("runtime")
        except Exception as exc:
            print(f"[RUNTIME] {i}/{total} {row['title']}: fetch error — {exc}")
            time.sleep(0.25)
            continue

        if runtime:
            db.client.table("media").update({"runtime": runtime}).eq("id", row["id"]).execute()
            print(f"[RUNTIME] {i}/{total} {row['title']}: {runtime} min")
            updated += 1
        else:
            print(f"[RUNTIME] {i}/{total} {row['title']}: no runtime found")
            no_runtime += 1

        time.sleep(0.25)

    print(
        f"[RUNTIME] Done. {total} movies processed — "
        f"{updated} updated, {no_runtime} with no runtime found."
    )


if __name__ == "__main__":
    main()
