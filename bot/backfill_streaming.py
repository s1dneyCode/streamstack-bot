"""
Backfill: refresh streaming availability for every title in public.media
using TMDB's /movie|tv/{id}/watch/providers endpoint.

Stores flatrate (subscription), rent, and buy providers, each filtered
down to ALLOWED_PROVIDERS. Processes all titles (not just ones missing
data) so dropped providers stay in sync. is_streamable_now is true
only when flatrate providers exist.

Run manually:
    python -m bot.backfill_streaming
"""

import os
import sys
import time

from .tmdb import TmdbClient
from .supabase_client import SupabaseClient, ALLOWED_PROVIDERS


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


def main() -> None:
    print("[BACKFILL] Starting streaming availability backfill...")
    config = load_env()

    tmdb = TmdbClient(api_key=config["TMDB_API_KEY"])
    db   = SupabaseClient(url=config["SUPABASE_URL"], key=config["SUPABASE_KEY"])

    all_media = fetch_all_media(db)
    total     = len(all_media)
    updated   = 0
    streamable_count = 0

    for i, row in enumerate(all_media, start=1):
        media_id   = row["id"]
        tmdb_id    = row["tmdb_id"]
        title      = row["title"]
        media_type = row["media_type"]

        providers = tmdb.get_watch_providers(tmdb_id=tmdb_id, media_type=media_type)
        providers = {
            kind: [p for p in names if p in ALLOWED_PROVIDERS]
            for kind, names in providers.items()
        }

        if any(providers.values()):
            is_streamable = bool(providers.get("flatrate"))
            db.delete_streaming_providers(media_id)
            db.upsert_streaming_availability(media_id=media_id, providers=providers)
            db.client.table("media").update({"is_streamable_now": is_streamable}).eq("id", media_id).execute()
            updated += 1
            if is_streamable:
                streamable_count += 1
            print(f"[BACKFILL] {i}/{total} {title}: {providers}")
        else:
            print(f"[BACKFILL] {i}/{total} {title}: no providers found")

        db.update_streaming_last_checked(media_id)
        time.sleep(0.25)

    print(
        f"\n[BACKFILL] Done. {updated}/{total} titles had provider data, "
        f"{streamable_count} flagged streamable now."
    )


if __name__ == "__main__":
    main()
