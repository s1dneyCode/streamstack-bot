"""
Monthly cleanup: delete low-quality titles that have accumulated no traction.

A title is deleted when it meets ANY elimination condition and NO protection
condition.

Protection conditions (any true -> never delete):
  - is_streamable_now = true
  - is_in_theatres = true
  - release_date within the last 90 days
  - has a streaming_availability row with monetization_type IN ('rent','buy')
    AND release_date within the last 2 years

Elimination conditions (evaluated only when not protected):
  - No poster: poster_path IS NULL OR poster_path = '' (eliminate regardless
    of vote_count)
  - vote_count below a threshold tiered by release year:
      release_year >= 2019            -> eliminate if vote_count < 500
      release_year 2010-2018          -> eliminate if vote_count < 700
      release_year < 2010             -> eliminate if vote_count < 1000

Run via:
    python -m bot.cleanup_low_quality
    python -m bot.cleanup_low_quality --dry-run
"""

import os
import sys
from datetime import date, timedelta

from .supabase_client import SupabaseClient

PAGE_SIZE = 1000


def load_env() -> dict[str, str]:
    required = ["SUPABASE_URL", "SUPABASE_KEY"]
    config: dict[str, str] = {}
    for key in required:
        value = os.environ.get(key)
        if not value:
            print(f"[CLEANUP] ERROR: Required environment variable '{key}' is not set.")
            sys.exit(1)
        config[key] = value
    return config


def fetch_rent_buy_media_ids(db: SupabaseClient) -> set[str]:
    """Return media_ids with at least one rent or buy streaming_availability row."""
    ids: set[str] = set()
    offset = 0
    while True:
        batch = (
            db.client.table("streaming_availability")
            .select("media_id")
            .in_("monetization_type", ["rent", "buy"])
            .range(offset, offset + PAGE_SIZE - 1)
            .execute()
            .data or []
        )
        ids.update(row["media_id"] for row in batch)
        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return ids


def fetch_all_media(db: SupabaseClient) -> list[dict]:
    rows: list[dict] = []
    offset = 0
    while True:
        batch = (
            db.client.table("media")
            .select("id, title, vote_count, release_date, poster_path, is_streamable_now, is_in_theatres")
            .range(offset, offset + PAGE_SIZE - 1)
            .execute()
            .data or []
        )
        rows.extend(batch)
        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return rows


def _vote_threshold(release_year: int) -> int:
    if release_year >= 2019:
        return 500
    if release_year >= 2010:
        return 700
    return 1000


def evaluate(row: dict, rent_buy_ids: set[str], today: date) -> tuple[bool, int | None, bool]:
    """Return (should_delete, release_year, has_rent_buy) for a media row."""
    media_id       = row["id"]
    vote_count     = row.get("vote_count") or 0
    poster_path    = row.get("poster_path")
    is_streamable  = row.get("is_streamable_now") or False
    is_in_theatres = row.get("is_in_theatres") or False
    has_rent_buy   = media_id in rent_buy_ids

    release_date_obj: date | None = None
    if row.get("release_date"):
        try:
            release_date_obj = date.fromisoformat(row["release_date"])
        except ValueError:
            release_date_obj = None
    release_year = release_date_obj.year if release_date_obj else None

    # --- Protection conditions: any true -> never delete ------------------
    if is_streamable:
        return False, release_year, has_rent_buy
    if is_in_theatres:
        return False, release_year, has_rent_buy
    if release_date_obj and release_date_obj >= today - timedelta(days=90):
        return False, release_year, has_rent_buy
    if has_rent_buy and release_date_obj and release_date_obj >= today - timedelta(days=730):
        return False, release_year, has_rent_buy

    # --- Elimination conditions --------------------------------------------
    if not poster_path:
        return True, release_year, has_rent_buy

    if release_year is not None:
        # release_date is guaranteed > 90 days ago here (protection above),
        # satisfying the "released > 90 days" qualifier for the 2019+ tier.
        if vote_count < _vote_threshold(release_year):
            return True, release_year, has_rent_buy

    return False, release_year, has_rent_buy


def main() -> None:
    dry_run = "--dry-run" in sys.argv[1:]
    print(f"[CLEANUP] Starting low-quality title cleanup{' (dry run)' if dry_run else ''}...")
    config = load_env()
    db = SupabaseClient(url=config["SUPABASE_URL"], key=config["SUPABASE_KEY"])

    today = date.today()

    print("[CLEANUP] Fetching rent/buy streaming coverage...")
    rent_buy_ids = fetch_rent_buy_media_ids(db)

    print("[CLEANUP] Fetching all media rows...")
    all_media = fetch_all_media(db)

    to_delete: list[dict] = []
    for row in all_media:
        should_delete, release_year, has_rent_buy = evaluate(row, rent_buy_ids, today)
        if should_delete:
            to_delete.append({**row, "release_year": release_year, "has_rent_buy": has_rent_buy})

    total = len(to_delete)
    print(f"[CLEANUP] {total} titles matched deletion criteria.")
    for row in to_delete:
        print(
            f"[CLEANUP] Would delete: {row['title']} "
            f"(release_year={row['release_year']}, vote_count={row.get('vote_count')}, "
            f"is_streamable_now={row.get('is_streamable_now')}, has_rent_buy={row['has_rent_buy']})"
        )

    if dry_run:
        print(f"[CLEANUP] Dry run — no titles deleted. {total} would have been deleted.")
        return

    deleted = 0
    failed  = 0

    for row in to_delete:
        try:
            db.client.table("media").delete().eq("id", row["id"]).execute()
            print(f"[CLEANUP] Deleted: {row['title']}")
            deleted += 1
        except Exception as exc:
            print(f"[CLEANUP] Failed to delete id={row['id']} '{row['title']}': {exc}")
            failed += 1

    print(f"[CLEANUP] Done. {deleted} titles deleted, {failed} failed.")


if __name__ == "__main__":
    main()
