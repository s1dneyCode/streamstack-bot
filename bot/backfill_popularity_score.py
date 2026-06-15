"""
Backfill: calculate and update popularity_score for all rows in public.media.

Formula (Bayesian weighted + vote-scaled freshness decay):
    normalized_popularity = min(popularity / 500 * 100, 100)
    raw_score = (normalized_popularity * 0.3) + (tmdb_score * 0.5) + (rt_score * 0.2)
    bayesian  = (v / (v + 500)) * raw_score + (500 / (v + 500)) * 72.07
    freshness_scale = min(vote_count / 100, 1.0)   # full boost at 100+ votes
    freshness = exp(-days_since_release / 365) * freshness_scale
    popularity_score = bayesian * (0.7 + 0.3 * freshness)
    # If release_date is None: popularity_score = bayesian * 0.7

Run via:
    python -m bot.backfill_popularity_score
"""

import os
import sys

from .supabase_client import SupabaseClient, compute_popularity_score

PAGE_SIZE = 1000


def load_env() -> dict[str, str]:
    required = ["SUPABASE_URL", "SUPABASE_KEY"]
    config: dict[str, str] = {}
    for key in required:
        value = os.environ.get(key)
        if not value:
            print(f"[PSCORE] ERROR: Required environment variable '{key}' is not set.")
            sys.exit(1)
        config[key] = value
    return config


def fetch_all_rows(db: SupabaseClient) -> list[dict]:
    rows: list[dict] = []
    offset = 0
    while True:
        batch = (
            db.client.table("media")
            .select("id, title, popularity, tmdb_score, rt_score, vote_count, release_date")
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
    print("[PSCORE] Starting popularity_score backfill...")
    config = load_env()
    db = SupabaseClient(url=config["SUPABASE_URL"], key=config["SUPABASE_KEY"])

    rows = fetch_all_rows(db)
    total = len(rows)
    print(f"[PSCORE] {total} rows to process.")

    updated = 0
    failed  = 0

    for i, row in enumerate(rows, start=1):
        score = compute_popularity_score(
            row.get("popularity"),
            row.get("tmdb_score"),
            row.get("rt_score"),
            row.get("vote_count"),
            release_date=row.get("release_date"),
        )
        try:
            db.client.table("media").update({"popularity_score": score}).eq("id", row["id"]).execute()
            updated += 1
        except Exception as exc:
            print(f"[PSCORE] {i}/{total} Failed for id={row['id']} '{row.get('title')}': {exc}")
            failed += 1

        if i % 500 == 0:
            print(f"[PSCORE] Progress: {i}/{total} processed ({updated} updated, {failed} failed)")

    print(f"[PSCORE] Done. {total} processed — {updated} updated, {failed} failed.")


if __name__ == "__main__":
    main()
