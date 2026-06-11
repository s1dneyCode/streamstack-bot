"""
Backfill summary: queries Supabase and prints completion status for all backfills.

Run via:
    python -m bot.backfill_summary
"""

import os
import sys

from .supabase_client import SupabaseClient

PAGE_SIZE = 1000


def load_env() -> dict[str, str]:
    required = ["SUPABASE_URL", "SUPABASE_KEY"]
    config: dict[str, str] = {}
    for key in required:
        value = os.environ.get(key)
        if not value:
            print(f"[SUMMARY] ERROR: Required environment variable '{key}' is not set.")
            sys.exit(1)
        config[key] = value
    return config


def count_null(db, column: str, extra_eq: dict | None = None) -> int:
    """Count rows where column IS NULL, with optional equality filters."""
    q = db.client.table("media").select("id", count="exact").is_(column, "null")
    for col, val in (extra_eq or {}).items():
        q = q.eq(col, val)
    return q.limit(1).execute().count or 0


def count_eq(db, column: str, value) -> int:
    """Count media rows where column = value."""
    return (
        db.client.table("media")
        .select("id", count="exact")
        .eq(column, value)
        .limit(1)
        .execute()
        .count or 0
    )


def count_total(db) -> tuple[int, int, int]:
    total    = db.client.table("media").select("id", count="exact").limit(1).execute().count or 0
    movies   = count_eq(db, "media_type", "movie")
    tv_shows = count_eq(db, "media_type", "tv")
    return total, movies, tv_shows


def count_distinct_media_ids(db, table: str) -> int:
    """Count distinct media_id values in a related table using paginated fetch."""
    seen: set = set()
    offset = 0
    while True:
        batch = (
            db.client.table(table)
            .select("media_id")
            .range(offset, offset + PAGE_SIZE - 1)
            .execute()
            .data or []
        )
        for row in batch:
            seen.add(row["media_id"])
        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return len(seen)


def main() -> None:
    print("[SUMMARY] Querying Supabase for backfill status...")
    config = load_env()
    db = SupabaseClient(url=config["SUPABASE_URL"], key=config["SUPABASE_KEY"])

    total, total_movies, total_tv = count_total(db)

    # --- Null-fill remaining counts ---
    rem_runtime    = count_null(db, "runtime",         extra_eq={"media_type": "movie"})
    rem_vote       = count_null(db, "vote_count")
    rem_pop_score  = count_null(db, "popularity_score")
    rem_status     = count_null(db, "status")
    rem_genres     = count_null(db, "genres")
    rem_cert       = count_null(db, "certification")
    rem_logos      = count_null(db, "title_logo_url")
    rem_images     = count_null(db, "poster_url")

    # --- Related-table coverage ---
    with_trailers = count_distinct_media_ids(db, "media_trailers")
    with_credits  = count_distinct_media_ids(db, "media_credits")
    with_seasons  = count_distinct_media_ids(db, "media_seasons")

    rem_trailers = total    - with_trailers
    rem_credits  = total    - with_credits
    rem_seasons  = total_tv - with_seasons

    # --- States ---
    streamable  = count_eq(db, "is_streamable_now", True)
    in_theatres = count_eq(db, "is_in_theatres",    True)

    # --- External backfills ---
    rem_rt_scores = count_null(db, "rt_score")
    with_streaming  = count_distinct_media_ids(db, "streaming_availability")
    rem_streaming   = total - with_streaming

    # --- Print report ---
    def _row(label: str, updated: int, remaining: int, note: str) -> None:
        icon = "✅" if remaining == 0 else "⚠️ "
        print(f"{icon} {label:<22} {updated:>6} updated, {remaining:>6} remaining  ({note})")

    print()
    print("=== FULL BACKFILL SUMMARY ===")
    print(f"Total titles in catalog: {total} ({total_movies} movies / {total_tv} TV shows)")
    print()

    rows = [
        ("runtime:",         total_movies - rem_runtime,  rem_runtime,  "runtime IS NULL AND media_type = 'movie'"),
        ("vote_count:",      total - rem_vote,             rem_vote,     "vote_count IS NULL"),
        ("popularity_score:",total - rem_pop_score,        rem_pop_score,"popularity_score IS NULL"),
        ("status:",          total - rem_status,           rem_status,   "status IS NULL"),
        ("genres:",          total - rem_genres,           rem_genres,   "genres IS NULL"),
        ("certifications:",  total - rem_cert,             rem_cert,     "certification IS NULL"),
        ("title_logos:",     total - rem_logos,            rem_logos,    "title_logo_url IS NULL"),
        ("trailers:",        with_trailers,                rem_trailers, "no rows in media_trailers"),
        ("credits:",         with_credits,                 rem_credits,  "no rows in media_credits"),
        ("seasons:",         with_seasons,                 rem_seasons,  "TV shows with no rows in media_seasons"),
        ("images:",          total - rem_images,           rem_images,   "poster_url IS NULL"),
        ("states:",          streamable + in_theatres,     0,            f"{streamable} streamable now, {in_theatres} in theatres"),
    ]

    any_remaining = False
    for label, updated, remaining, note in rows:
        _row(label, updated, remaining, note)
        if remaining > 0:
            any_remaining = True

    print()
    print("=== PENDING EXTERNAL BACKFILLS (run manually) ===")
    print(f"⬜ {'rt-scores:':<22} {rem_rt_scores:>6} titles  (rt_score IS NULL)")
    print(f"⬜ {'streaming:':<22} {rem_streaming:>6} titles  (no rows in streaming_availability)")
    print()

    if any_remaining:
        print("⚠️  Some backfills are incomplete. Run full_backfill again or trigger individual backfills from backfills.yml.")
    else:
        print("✅ All backfills complete.")
    print()


if __name__ == "__main__":
    main()
