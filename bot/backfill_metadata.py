"""
Backfill: populate original_language, is_documentary, and is_limited_series
for all titles where any of these fields IS NULL.

Calls /movie/{tmdb_id} or /tv/{tmdb_id} for each title and updates all three
fields in one pass.

Run via:
    python -m bot.backfill_metadata
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
            print(f"[META] ERROR: Required environment variable '{key}' is not set.")
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
            .or_("original_language.is.null,is_documentary.is.null,is_limited_series.is.null")
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
    print("[META] Starting metadata backfill...")
    config = load_env()
    db   = SupabaseClient(url=config["SUPABASE_URL"], key=config["SUPABASE_KEY"])
    tmdb = TmdbClient(api_key=config["TMDB_API_KEY"])

    rows  = fetch_targets(db)
    total = len(rows)
    print(f"[META] {total} titles to process.")

    updated_lang   = 0
    updated_doc    = 0
    updated_series = 0
    failed         = 0

    for i, row in enumerate(rows, start=1):
        tmdb_id    = row["tmdb_id"]
        title      = row["title"]
        media_type = row["media_type"]

        try:
            data = tmdb._get(f"/{media_type}/{tmdb_id}")
        except Exception as exc:
            print(f"[META] {i}/{total} {title}: fetch failed — {exc}")
            failed += 1
            time.sleep(0.25)
            continue

        payload: dict = {}

        original_language = data.get("original_language")
        if original_language is not None:
            payload["original_language"] = original_language
            updated_lang += 1

        genre_ids     = [g["id"] for g in data.get("genres", [])]
        is_documentary = 99 in genre_ids
        payload["is_documentary"] = is_documentary
        updated_doc += 1

        if media_type == "tv":
            is_limited_series = data.get("type") == "Miniseries"
            payload["is_limited_series"] = is_limited_series
            updated_series += 1

        try:
            db.client.table("media").update(payload).eq("id", row["id"]).execute()
        except Exception as exc:
            print(f"[META] {i}/{total} {title}: update failed — {exc}")
            failed += 1
            time.sleep(0.25)
            continue

        if i % 100 == 0:
            print(f"[META] Progress: {i}/{total} processed")

        time.sleep(0.25)

    print(
        f"[META] Done. {total} processed — "
        f"{updated_lang} original_language, "
        f"{updated_doc} is_documentary, "
        f"{updated_series} is_limited_series updated, "
        f"{failed} failed."
    )


if __name__ == "__main__":
    main()
