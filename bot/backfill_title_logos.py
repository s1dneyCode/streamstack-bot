"""
One-time backfill: fetch and store title_logo_url for all media titles
where the field is currently NULL.

Run via:
    python -m bot.backfill_title_logos
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
            print(f"[LOGOS] ERROR: Required environment variable '{key}' is not set.")
            sys.exit(1)
        config[key] = value
    return config


def main() -> None:
    print("[LOGOS] Starting title logo backfill...")
    config = load_env()

    db   = SupabaseClient(url=config["SUPABASE_URL"], key=config["SUPABASE_KEY"])
    tmdb = TmdbClient(api_key=config["TMDB_API_KEY"])

    # Fetch all titles with title_logo_url IS NULL (paginated)
    page_size = 1000
    offset    = 0
    needs_logo: list[dict] = []
    while True:
        batch = (
            db.client.table("media")
            .select("id, tmdb_id, title, media_type")
            .is_("title_logo_url", "null")
            .range(offset, offset + page_size - 1)
            .execute()
            .data or []
        )
        needs_logo.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size

    total   = len(needs_logo)
    updated = 0
    missing = 0

    print(f"[LOGOS] {total} titles need logo backfill.")

    for i, row in enumerate(needs_logo, start=1):
        logo_url = tmdb.get_title_logo(tmdb_id=row["tmdb_id"], media_type=row["media_type"])

        if logo_url:
            db.client.table("media").update({"title_logo_url": logo_url}).eq("id", row["id"]).execute()
            print(f"[LOGOS] {i}/{total} {row['title']}: {logo_url}")
            updated += 1
        else:
            print(f"[LOGOS] {i}/{total} {row['title']}: no logo found")
            missing += 1

        time.sleep(0.25)

    print(
        f"[LOGOS] Done. {total} titles processed — "
        f"{updated} updated, {missing} with no logo found."
    )


if __name__ == "__main__":
    main()
