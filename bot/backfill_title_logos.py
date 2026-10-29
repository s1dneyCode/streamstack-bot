"""
One-time backfill: fetch and store title_logo_url for all media titles
where the field is currently NULL — with its luminance over black
(bot/logo_luminance.py) in the same update.

Run via:
    python -m bot.backfill_title_logos
"""

import os
import sys
import time

from .tmdb import TmdbClient
from .supabase_client import SupabaseClient
from .logo_luminance import luminance_for_logo


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


def backfill_title_logo(db, tmdb, row: dict, i: int, total: int) -> bool:
    """One row: choose the title logo and write it with its luminance.

    The logo is the chooser's (B4: choose_title_logo, the lightest at or above
    the app's line), its luminance measured once and handed back with it.
    The URL and its luminance go in ONE update; the luminance is None (SQL
    NULL) when it could not be measured, so backfill_title_logo_luminance.py
    measures it later. Returns True when a logo was written.
    """
    logo_url, luminance = tmdb.choose_title_logo(tmdb_id=row["tmdb_id"], media_type=row["media_type"],
                                                 measure=luminance_for_logo)

    if logo_url:
        db.client.table("media").update(
            {"title_logo_url": logo_url, "title_logo_luminance": luminance}
        ).eq("id", row["id"]).execute()
        note = f"luminance {luminance:.2f}" if luminance is not None else "luminance pending"
        print(f"[LOGOS] {i}/{total} {row['title']}: {logo_url} · {note}")
        return True
    print(f"[LOGOS] {i}/{total} {row['title']}: no logo found")
    return False


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
        if backfill_title_logo(db, tmdb, row, i, total):
            updated += 1
        else:
            missing += 1

        time.sleep(0.25)

    print(
        f"[LOGOS] Done. {total} titles processed — "
        f"{updated} updated, {missing} with no logo found."
    )


if __name__ == "__main__":
    main()
