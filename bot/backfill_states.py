"""
Backfill: recompute is_streamable_now for every title, one update for each
row whose flags differ. A title is streamable when it has ANY
streaming_availability row (any region, rent and buy included).

What each mode writes today:
  --skip-theatres   is_streamable_now only; is_in_theatres is left as it is.
                    Every workflow runs it this way: backfills.yml, and since
                    B4 step 11 of full_backfill_new.yml and step 4 of
                    full_backfill_after_bulkimport.yml
                    (tests/test_workflow_backfill_states.py pins it).
  no flag           also is_in_theatres = False, on every title it updates —
                    and a title in theatres always differs from that, so a run
                    without the flag empties the New in Theatres rail until
                    daily_verify.py runs again.

is_in_theatres is daily_verify.py's (TMDb /movie/now_playing, 08:00 UTC): it
is the one writer that sets it True, and this backfill never does.

Run via:
    python -m bot.backfill_states --skip-theatres
    python -m bot.backfill_states
"""

import os
import sys

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


def compute_state(media_id: int, streaming_ids: set[int]) -> tuple[bool, bool]:
    """Return the correct (is_in_theatres, is_streamable_now) for a media row."""
    has_providers = media_id in streaming_ids
    if has_providers:
        return False, True
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
    skip_theatres = "--skip-theatres" in sys.argv[1:]
    config = load_env()
    db = create_client(config["SUPABASE_URL"], config["SUPABASE_KEY"])

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
        new_in_theatres, new_streamable = compute_state(row["id"], streaming_ids)

        if skip_theatres:
            new_in_theatres = old_in_theatres

        old_label = state_label(old_in_theatres, old_streamable)
        new_label = state_label(new_in_theatres, new_streamable)

        if new_in_theatres == old_in_theatres and new_streamable == old_streamable:
            skipped += 1
            continue

        update_payload = {"is_streamable_now": new_streamable}
        if not skip_theatres:
            update_payload["is_in_theatres"] = new_in_theatres

        db.table("media").update(update_payload).eq("id", row["id"]).execute()

        print(f"[STATES] {i}/{total} {row['title']} ({row['media_type']}): {old_label} → {new_label}")
        updated += 1

    print(f"[STATES] Done. {updated} updated, {skipped} already correct.")


if __name__ == "__main__":
    main()
