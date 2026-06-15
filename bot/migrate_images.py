"""
One-time migration script that downloads posters and carousel images
from TMDB and uploads them to Supabase Storage bucket 'media-images'.
Updates poster_url and carousel_images columns in public.media.

Run via:
    python -m bot.migrate_images
"""

import os
import sys
import time

import requests

from .tmdb import TmdbClient
from .supabase_client import SupabaseClient

BACKDROP_BASE = "https://image.tmdb.org/t/p/w780"
BUCKET = "media-images"


def load_env() -> dict[str, str]:
    required = ["TMDB_API_KEY", "SUPABASE_URL", "SUPABASE_KEY"]
    config: dict[str, str] = {}
    for key in required:
        value = os.environ.get(key)
        if not value:
            print(f"[MIGRATE] ERROR: Required environment variable '{key}' is not set.")
            sys.exit(1)
        config[key] = value
    return config


def download_image(url: str) -> bytes | None:
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        return response.content
    except Exception as exc:
        print(f"[MIGRATE] Warning: failed to download {url}: {exc}")
        return None


def migrate_poster(db: SupabaseClient, tmdb_id: int, title: str, poster_path: str) -> str | None:
    """Download poster and upload to Supabase Storage. Returns public URL or None."""
    if not poster_path:
        return None

    image_bytes = download_image(poster_path)
    if not image_bytes:
        return None

    url = db.upload_image(BUCKET, f"posters/{tmdb_id}.jpg", image_bytes)
    if url:
        db.client.table("media").update({"poster_url": url}).eq("tmdb_id", tmdb_id).execute()
    return url


def migrate_carousel(db: SupabaseClient, tmdb: TmdbClient, tmdb_id: int, title: str, media_type: str) -> list[str]:
    """Fetch up to 8 backdrops from TMDB and upload to Supabase Storage. Returns list of public URLs."""
    try:
        data = tmdb._get(f"/{media_type}/{tmdb_id}/images")
    except Exception as exc:
        print(f"[MIGRATE] Warning: failed to fetch images for '{title}': {exc}")
        return []

    backdrops = data.get("backdrops", [])[:8]
    urls: list[str] = []

    for index, backdrop in enumerate(backdrops):
        file_path = backdrop.get("file_path", "")
        if not file_path:
            continue

        image_bytes = download_image(f"{BACKDROP_BASE}{file_path}")
        if not image_bytes:
            continue

        url = db.upload_image(BUCKET, f"carousels/{tmdb_id}/{index}.jpg", image_bytes)
        if url:
            urls.append(url)

    if urls:
        db.client.table("media").update({"carousel_images": urls}).eq("tmdb_id", tmdb_id).execute()

    return urls


def main() -> None:
    print("[MIGRATE] Starting image migration...")
    config = load_env()

    db   = SupabaseClient(url=config["SUPABASE_URL"], key=config["SUPABASE_KEY"])
    tmdb = TmdbClient(api_key=config["TMDB_API_KEY"])

    page_size = 1000
    offset = 0
    rows: list[dict] = []

    while True:
        response = (
            db.client.table("media")
            .select("id, tmdb_id, title, media_type, poster_path")
            .or_("poster_url.is.null,poster_url.like.https://image.tmdb.org%")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        batch = response.data or []
        rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size

    total = len(rows)
    errors = 0
    print(f"[MIGRATE] {total} titles need migration.")

    for i, row in enumerate(rows, start=1):
        tmdb_id    = row["tmdb_id"]
        title      = row["title"]
        media_type = row["media_type"]
        poster_path = row.get("poster_path", "")

        try:
            poster_url    = migrate_poster(db, tmdb_id, title, poster_path)
            carousel_urls = migrate_carousel(db, tmdb, tmdb_id, title, media_type)
            n = len(carousel_urls)
            if poster_url is None:
                errors += 1
            print(f"[MIGRATE] {i}/{total} {title} — poster {'✓' if poster_url else '✗'}, {n} carousel images ✓")
        except Exception as exc:
            errors += 1
            print(f"[MIGRATE] {i}/{total} {title} — ERROR: {exc}")

        time.sleep(0.5)

    print(f"[MIGRATE] Done. {total} titles migrated, {errors} errors.")


if __name__ == "__main__":
    main()
