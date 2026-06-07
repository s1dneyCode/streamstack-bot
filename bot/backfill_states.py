"""
One-time backfill: fix is_in_theatres and is_streamable_now for all existing titles.

Rules applied:
  Movies with streaming providers   → is_streamable_now=True,  is_in_theatres=False
  Movies released, no providers     → is_in_theatres=True,     is_streamable_now=False
  Movies unreleased, no providers   → both False  (Coming Soon)
  TV with streaming providers       → is_streamable_now=True,  is_in_theatres=False
  TV released or unreleased, no providers → both False  (never set is_in_theatres for TV)

Run via:
    python -m bot.backfill_states
"""

import os
import sys
from datetime import date, timedelta

from supabase import create_client


def load_env() -> dict[str, str]:
    required = ["SUPABASE_URL", "SUPABASE_KEY"]
    config: dict[str, str] = {}
    for key in required:
        value = os.environ.get(key)
        if not value:
            print(f"[STATES] ERROR: Required environment variable '{key}' is not set.")
            sys.exit(1)
        config[key] = value
    return config


def state_label(is_in_theatres: bool, is_streamable_now: bool) -> str:
    if is_in_theatres:
        return "in_theatres"
    if is_streamable_now:
        return "streamable"
    return "neither"


def compute_state(row: dict, streaming_ids: set[int], today_str: str, ninety_days_ago_str: str) -> tuple[bool, bool]:
    """Return the correct (is_in_theatres, is_streamable_now) for a media row."""
    media_id     = row["id"]
    media_type   = row["media_type"]
    release_date = row.get("release_date") or ""

    has_providers     = media_id in streaming_ids
    recently_released = bool(release_date) and ninety_days_ago_str <= release_date <= today_str

    if has_providers:
        return False, True

    if media_type == "movie" and recently_released:
        return True, False

    return False, False


def fetch_paginated(db, table: str, select: str, filters=None) -> list[dict]:
    page_size = 1000
    offset    = 0
    rows: list[dict] = []

    while True:
        q = db.table(table).select(select)
        if filters:
            for method, args in filters:
                q = getattr(q, method)(*args)
        batch = (q.range(offset, offset + page_size - 1).execute().data or [])
        rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size

    return rows


def main() -> None:
    print("[STATES] Starting state backfill...")
    config = load_env()
    db = create_client(config["SUPABASE_URL"], config["SUPABASE_KEY"])

    today              = date.today()
    today_str          = today.isoformat()
    ninety_days_ago_str = (today - timedelta(days=90)).isoformat()

    # Fetch all distinct media_ids that have at least one streaming provider
    print("[STATES] Fetching streaming_availability...")
    sa_rows = fetch_paginated(db, "streaming_availability", "media_id")
    streaming_ids: set[int] = {row["media_id"] for row in sa_rows}
    print(f"[STATES] {len(streaming_ids)} titles have streaming providers.")

    # Fetch all media rows
    print("[STATES] Fetching all media rows...")
    media_rows = fetch_paginated(
        db, "media",
        "id, tmdb_id, title, media_type, release_date, is_in_theatres, is_streamable_now",
    )
    total = len(media_rows)
    print(f"[STATES] {total} titles to process.")

    updated = 0
    skipped = 0

    for i, row in enumerate(media_rows, start=1):
        old_in_theatres  = row["is_in_theatres"]
        old_streamable   = row["is_streamable_now"]
        new_in_theatres, new_streamable = compute_state(row, streaming_ids, today_str, ninety_days_ago_str)

        old_label = state_label(old_in_theatres, old_streamable)
        new_label = state_label(new_in_theatres, new_streamable)

        if new_in_theatres == old_in_theatres and new_streamable == old_streamable:
            skipped += 1
            continue

        db.table("media").update({
            "is_in_theatres":   new_in_theatres,
            "is_streamable_now": new_streamable,
        }).eq("id", row["id"]).execute()

        print(f"[STATES] {i}/{total} {row['title']} ({row['media_type']}): {old_label} → {new_label}")
        updated += 1

    print(f"[STATES] Done. {updated} updated, {skipped} already correct.")


if __name__ == "__main__":
    main()
