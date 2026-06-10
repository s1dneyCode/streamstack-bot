"""
One-time backfill: fetch and store US content certification for all titles
where certification is currently NULL.

Run via:
    python -m bot.backfill_certifications
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
            print(f"[CERT] ERROR: Required environment variable '{key}' is not set.")
            sys.exit(1)
        config[key] = value
    return config


def main() -> None:
    print("[CERT] Starting certification backfill...")
    config = load_env()

    db   = SupabaseClient(url=config["SUPABASE_URL"], key=config["SUPABASE_KEY"])
    tmdb = TmdbClient(api_key=config["TMDB_API_KEY"])

    # Fetch all titles with certification IS NULL (paginated)
    page_size = 1000
    offset    = 0
    needs_cert: list[dict] = []
    while True:
        batch = (
            db.client.table("media")
            .select("id, tmdb_id, title, media_type")
            .is_("certification", "null")
            .range(offset, offset + page_size - 1)
            .execute()
            .data or []
        )
        needs_cert.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size

    total   = len(needs_cert)
    updated = 0
    missing = 0

    print(f"[CERT] {total} titles need certification backfill.")

    for i, row in enumerate(needs_cert, start=1):
        cert = tmdb.get_certification(tmdb_id=row["tmdb_id"], media_type=row["media_type"])

        if cert:
            db.client.table("media").update({"certification": cert}).eq("id", row["id"]).execute()
            print(f"[CERT] {i}/{total} {row['title']} ({row['media_type']}): {cert}")
            updated += 1
        else:
            print(f"[CERT] {i}/{total} {row['title']} ({row['media_type']}): no US certification found")
            missing += 1

        time.sleep(0.25)

    print(
        f"[CERT] Done. {total} titles processed — "
        f"{updated} updated, {missing} with no certification found."
    )


if __name__ == "__main__":
    main()
