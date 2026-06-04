"""
One-shot backfill script for streaming availability data.

Designed to run once against a database that was populated before the
streaming upsert worked correctly.  Finds every media row that has no
entries in streaming_availability and fills them in via TMDB watch/providers.

Run manually:
    python -m bot.backfill_streaming

Triggered in production by the `backfill` job in nightly.yml, which is
gated to workflow_dispatch so it never runs on the cron schedule.
"""

import os
import sys
import time
import requests

from .streaming import StreamingClient
from .supabase_client import SupabaseClient


def load_env() -> dict[str, str]:
    required = ["WATCHMODE_API_KEY", "SUPABASE_URL", "SUPABASE_KEY"]
    config: dict[str, str] = {}
    for key in required:
        value = os.environ.get(key)
        if not value:
            print(f"[BACKFILL] ERROR: Required environment variable '{key}' is not set.")
            sys.exit(1)
        config[key] = value
    return config


def fetch_all_media(db: SupabaseClient) -> list[dict]:
    """Fetch id, tmdb_id, title, media_type for every row in public.media."""
    print("[BACKFILL] Fetching all media rows from Supabase...")
    rows: list[dict] = []
    page_size = 1000
    offset = 0

    while True:
        response = (
            db.client.table("media")
            .select("id, tmdb_id, title, media_type")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        page = response.data or []
        rows.extend(page)
        if len(page) < page_size:
            break
        offset += page_size

    print(f"[BACKFILL] Found {len(rows)} total media rows.")
    return rows


def fetch_covered_media_ids(db: SupabaseClient) -> set[int]:
    """Return the set of media_ids that already have streaming_availability rows."""
    print("[BACKFILL] Fetching media_ids already covered in streaming_availability...")
    covered: set[int] = set()
    page_size = 1000
    offset = 0

    while True:
        response = (
            db.client.table("streaming_availability")
            .select("media_id")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        page = response.data or []
        for row in page:
            covered.add(row["media_id"])
        if len(page) < page_size:
            break
        offset += page_size

    print(f"[BACKFILL] {len(covered)} media_ids already have streaming data.")
    return covered


def main() -> None:
    print("[BACKFILL] Starting streaming availability backfill...")
    config = load_env()

    streaming = StreamingClient(api_key=config["WATCHMODE_API_KEY"])
    db = SupabaseClient(url=config["SUPABASE_URL"], key=config["SUPABASE_KEY"])

    # ------------------------------------------------------------------ #
    # DEBUG — verify WatchMode source IDs with a known title             #
    # Remove this block once source IDs are confirmed correct.           #
    # ------------------------------------------------------------------ #
    print("[DEBUG] Testing WatchMode API with Breaking Bad (tmdb_id=1396)...")

    search_response = requests.get(
        f"{streaming.BASE_URL}/search/",
        params={
            "apiKey": config["WATCHMODE_API_KEY"],
            "search_field": "tmdb_id",
            "search_value": "1396",
            "types": "tv_series",
        },
        timeout=10,
    )
    print(f"[DEBUG] Search status: {search_response.status_code}")
    print(f"[DEBUG] Search response: {search_response.text}")

    search_data = search_response.json()
    title_results = search_data.get("title_results", [])
    if title_results:
        watchmode_id = title_results[0]["id"]
        print(f"[DEBUG] WatchMode id: {watchmode_id}")

        sources_response = requests.get(
            f"{streaming.BASE_URL}/title/{watchmode_id}/sources/",
            params={
                "apiKey": config["WATCHMODE_API_KEY"],
                "regions": "US,BR,MX",
                "types": "sub",
            },
            timeout=10,
        )
        print(f"[DEBUG] Sources status: {sources_response.status_code}")
        print(f"[DEBUG] Sources response: {sources_response.text}")
    else:
        print("[DEBUG] No title_results returned — check API key or search params.")

    print("[DEBUG] Test complete. Exiting without running backfill.")
    return
    # ------------------------------------------------------------------ #
    # END DEBUG                                                           #
    # ------------------------------------------------------------------ #

    # ------------------------------------------------------------------ #
    # Step 1 — Identify which titles need backfilling                     #
    # ------------------------------------------------------------------ #
    all_media = fetch_all_media(db)
    covered_ids = fetch_covered_media_ids(db)

    pending = [row for row in all_media if row["id"] not in covered_ids]
    print(
        f"[BACKFILL] {len(pending)} titles need streaming data, "
        f"{len(all_media) - len(pending)} already covered."
    )

    if not pending:
        print("[BACKFILL] Nothing to do. Exiting.")
        return

    # ------------------------------------------------------------------ #
    # Step 2 — Fetch providers and upsert for each pending title          #
    # ------------------------------------------------------------------ #
    total = len(pending)
    updated = 0

    for i, row in enumerate(pending, start=1):
        tmdb_id = row["tmdb_id"]
        title = row["title"]
        media_type = row["media_type"]

        providers = streaming.get_streaming_providers(tmdb_id, media_type)

        print(f"[BACKFILL] {i}/{total} {title}: {providers}")

        if providers:
            # Fetch the Supabase UUID fresh to guarantee we have the right FK
            result = (
                db.client.table("media")
                .select("id")
                .eq("tmdb_id", tmdb_id)
                .single()
                .execute()
            )
            media_uuid = result.data.get("id") if result.data else None

            if media_uuid:
                db.upsert_streaming_availability(media_id=media_uuid, providers=providers)
                updated += 1

        time.sleep(0.3)

    # ------------------------------------------------------------------ #
    # Step 3 — Summary                                                    #
    # ------------------------------------------------------------------ #
    print(f"\n[BACKFILL] Done. {updated} titles updated, {total - updated} had no providers.")


if __name__ == "__main__":
    main()
