"""
One-shot backfill script for streaming availability data.

Designed to run once against a database that was populated before the
upsert_streaming_availability bug was fixed (missing .select() call).
It finds every media row that has zero rows in streaming_availability and
fills them in via TMDB watch/providers.

Run manually:
    python -m bot.backfill_streaming

In production this is triggered by the `backfill-streaming` job in
nightly.yml, which is gated to workflow_dispatch so it never runs on cron.
"""

import os
import sys
import time

from .tmdb import TmdbClient
from .streaming import StreamingClient
from .supabase_client import SupabaseClient


def load_env() -> dict[str, str]:
    required = ["TMDB_API_KEY", "SUPABASE_URL", "SUPABASE_KEY"]
    config: dict[str, str] = {}
    for key in required:
        value = os.environ.get(key)
        if not value:
            print(f"[BACKFILL] ERROR: Required environment variable '{key}' is not set.")
            sys.exit(1)
        config[key] = value
    return config


def fetch_all_media(db: SupabaseClient) -> list[dict]:
    """
    Return every row from public.media as a list of dicts with keys:
    id, tmdb_id, title, media_type.

    Pages through the table in chunks of 1 000 to handle large catalogs
    without hitting the default result-size cap.
    """
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
    """
    Return the set of media_id values that already have at least one row in
    public.streaming_availability.  These titles are skipped by the backfill.
    """
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

    tmdb = TmdbClient(api_key=config["TMDB_API_KEY"])
    streaming = StreamingClient()
    db = SupabaseClient(url=config["SUPABASE_URL"], key=config["SUPABASE_KEY"])

    # ------------------------------------------------------------------ #
    # Step 1 — Identify which titles need backfilling                     #
    # ------------------------------------------------------------------ #
    all_media = fetch_all_media(db)
    covered_ids = fetch_covered_media_ids(db)

    # A title needs backfilling if its Supabase id is not in covered_ids.
    # Note: covered_ids may contain titles with providers=[] that were saved
    # as explicit "no providers" rows — those are already handled correctly
    # and don't need re-processing.
    pending = [row for row in all_media if row["id"] not in covered_ids]

    print(f"[BACKFILL] {len(pending)} titles need streaming data, {len(all_media) - len(pending)} already covered.")

    if not pending:
        print("[BACKFILL] Nothing to do. Exiting.")
        return

    # ------------------------------------------------------------------ #
    # Step 2 — Fetch providers and upsert for each pending title          #
    # ------------------------------------------------------------------ #
    total = len(pending)
    saved = 0

    for i, row in enumerate(pending, start=1):
        media_id = row["id"]
        tmdb_id = row["tmdb_id"]
        title = row["title"]
        media_type = row["media_type"]

        providers = streaming.get_streaming_providers(
            tmdb_id=tmdb_id,
            media_type=media_type,
            tmdb_client=tmdb,
        )

        print(f"[BACKFILL] {i}/{total} {title}: {providers}")

        if providers:
            db.upsert_streaming_availability(media_id=media_id, providers=providers)
            saved += 1

        # Avoid hammering TMDB's watch/providers endpoint — 0.5s is enough
        # headroom given the free-tier rate limit of ~40 req/s
        time.sleep(0.5)

    # ------------------------------------------------------------------ #
    # Step 3 — Summary                                                    #
    # ------------------------------------------------------------------ #
    print(
        f"\n[BACKFILL] Done. {saved}/{total} titles had streaming providers saved. "
        f"{total - saved} had no providers on any tracked service."
    )


if __name__ == "__main__":
    main()
