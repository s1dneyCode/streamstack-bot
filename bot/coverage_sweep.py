"""
Deep /discover coverage sweep: systematically ingests the dormant
historical long tail that /changes never touches (sync_changes.py only
sees ids TMDB reports as recently *changed* — a title nobody has touched
on TMDB since it was added, however old or obscure, never shows up there).

The space is sliced by (media_type, release-year window) and walked with a
cursor persisted in bot_state (key discover_cursor, {media_type, year,
page}): movies first, newest year to oldest, then TV the same way, down to
MIN_YEAR. Each run resumes from the cursor and processes up to
SWEEP_PAGES_PER_RUN pages (env SWEEP_PAGES_PER_RUN or --pages, default 20)
before persisting wherever it got to. A slice is exhausted when its current
page reaches TMDB's own total_pages (capped at TMDB's hard 500-page limit);
the walk then advances to the next year, or from movies into TV once movies
hit MIN_YEAR. Once TV also hits MIN_YEAR, the cursor is set to {"done":
true} and every subsequent run is a fast no-op.

Per page: /discover/{movie|tv} sorted by popularity.desc, vote_count.gte=3,
include_adult=false, no language param — passes_sync_quality_filter() is
the real gate, applied in Python directly on each discover list item
(which carries the same fields a detail payload does: poster_path,
original_language, vote_count, popularity, release_date). This avoids a
per-candidate detail fetch entirely; a discover page is a self-contained
result, not just a pointer to fetch again like /changes' bare ids are.

Every changed id (well, every discover item) is batch-checked against the
DB first — ids already present are skipped outright, only genuinely new
ones are filtered and bare-inserted (enriched_at NULL; enrich_new_titles.py
fills in the rest).

Run via:
    python -m bot.coverage_sweep
    python -m bot.coverage_sweep --pages=50
"""

import os
import sys
from datetime import date

from .tmdb import TmdbClient
from .supabase_client import SupabaseClient
from .filters import passes_sync_quality_filter, build_bare_media_record

DEFAULT_SWEEP_PAGES_PER_RUN = 20
MIN_YEAR = 1900  # oldest release year the sweep walks down to
MAX_PAGE = 500   # TMDB's hard cap on the `page` param for /discover


def load_env() -> dict[str, str]:
    required = ["TMDB_API_KEY", "SUPABASE_URL", "SUPABASE_KEY"]
    config: dict[str, str] = {}
    for key in required:
        value = os.environ.get(key)
        if not value:
            print(f"[SWEEP] ERROR: Required environment variable '{key}' is not set.")
            sys.exit(1)
        config[key] = value
    return config


def _next_slice(media_type: str, year: int, today_year: int) -> tuple[str, int] | None:
    """
    Advance the sweep cursor to the next (media_type, year) slice.

    Same media_type, one year older, while >= MIN_YEAR; once movies exhaust
    MIN_YEAR, roll into TV starting fresh at today_year (not stale from
    whenever the movie phase itself started — the "current year" that
    matters for restarting a phase is whatever year it actually is now);
    once TV exhausts MIN_YEAR too, the whole sweep is done (None).
    """
    if year - 1 >= MIN_YEAR:
        return media_type, year - 1
    if media_type == "movie":
        return "tv", today_year
    return None


def _slices_remaining(media_type: str, year: int, today_year: int) -> int:
    """Count of (media_type, year) slices left to process, including the current one."""
    remaining = year - MIN_YEAR + 1
    if media_type == "movie":
        remaining += today_year - MIN_YEAR + 1  # the full TV phase still ahead
    return remaining


def main() -> None:
    print("[SWEEP] Starting coverage sweep...")

    pages_budget = None
    for arg in sys.argv[1:]:
        if arg.startswith("--pages="):
            pages_budget = int(arg.split("=")[1])
    if pages_budget is None:
        pages_budget = int(os.environ.get("SWEEP_PAGES_PER_RUN", DEFAULT_SWEEP_PAGES_PER_RUN))

    config = load_env()
    db   = SupabaseClient(url=config["SUPABASE_URL"], key=config["SUPABASE_KEY"])
    tmdb = TmdbClient(api_key=config["TMDB_API_KEY"])

    today_year = date.today().year

    # A read failure here must NOT be treated as "no prior cursor" — that
    # would reset to a fresh start and then, at the end of the run,
    # unconditionally overwrite whatever real progress bot_state actually
    # holds. Abort cleanly instead; the next run just retries the read.
    try:
        cursor = db.get_bot_state("discover_cursor")
    except Exception as exc:
        print(f"[SWEEP] ERROR: failed to read discover_cursor — aborting without touching cursor state: {exc}")
        print("[SWEEP] SUMMARY cursor=unknown checked=0 skipped_existing=0 new_inserted=0 errors=1 slices_remaining=unknown")
        return

    if isinstance(cursor, dict) and cursor.get("done"):
        print("[SWEEP] Sweep already complete — idling.")
        print("[SWEEP] SUMMARY cursor=done checked=0 skipped_existing=0 new_inserted=0 errors=0 slices_remaining=0")
        return

    required_keys = {"media_type", "year", "page"}
    if not isinstance(cursor, dict) or not required_keys.issubset(cursor):
        if cursor is not None:
            print(f"[SWEEP] WARNING: malformed discover_cursor {cursor!r} — resetting to a fresh start.")
        else:
            print("[SWEEP] No prior cursor — starting fresh.")
        cursor = {"media_type": "movie", "year": today_year, "page": 1}
        print(f"[SWEEP] Starting at movie/{today_year}/page 1.")

    media_type = cursor["media_type"]
    year       = cursor["year"]
    page       = cursor["page"]

    # Fetched lazily per media_type actually encountered — almost every run
    # only ever touches one, and get_genre_map()'s own per-instance cache
    # means a run that does straddle the movie/tv boundary still only pays
    # for each map once.
    genre_maps: dict[str, dict[int, str]] = {}

    def genre_map_for(mt: str) -> dict[int, str]:
        if mt not in genre_maps:
            genre_maps[mt] = tmdb.get_genre_map(mt)
        return genre_maps[mt]

    checked  = 0
    errors   = 0
    seen_keys: set[tuple[str, int]] = set()  # (media_type, tmdb_id) — a run can straddle the movie→tv boundary
    candidates: list[tuple[int, str, dict]] = []  # (tmdb_id, media_type, raw discover item)

    sweep_complete = False

    for _ in range(pages_budget):
        print(f"[SWEEP] Fetching {media_type}/{year} page {page}...")
        try:
            results, total_pages = tmdb.discover_page(media_type=media_type, year=year, page=page)
            checked += len(results)
            for item in results:
                tmdb_id = item.get("id")
                key = (media_type, tmdb_id)
                if tmdb_id is not None and key not in seen_keys:
                    seen_keys.add(key)
                    candidates.append((tmdb_id, media_type, item))
        except Exception as exc:
            print(f"[SWEEP]   {media_type}/{year} page {page}: fetch failed — {exc}")
            errors += 1
            # Unknown whether more pages remain behind a failed fetch —
            # advance the page counter (capped at MAX_PAGE) and keep going
            # rather than get stuck retrying the same page forever. A
            # single skipped page is an acceptable gap in an otherwise
            # self-healing sweep; a permanently stuck cursor is not.
            if page >= MAX_PAGE:
                next_slice = _next_slice(media_type, year, today_year)
                if next_slice is None:
                    sweep_complete = True
                    break
                media_type, year = next_slice
                page = 1
            else:
                page += 1
            continue

        if page < total_pages:
            page += 1
        else:
            next_slice = _next_slice(media_type, year, today_year)
            if next_slice is None:
                print(f"[SWEEP] Slice {media_type}/{year} exhausted — all slices done.")
                sweep_complete = True
                break
            print(f"[SWEEP] Slice {media_type}/{year} exhausted — advancing to {next_slice[0]}/{next_slice[1]}.")
            media_type, year = next_slice
            page = 1

    tmdb_ids      = [c[0] for c in candidates]
    existing_map  = db.get_media_ids_by_tmdb_ids(tmdb_ids) if tmdb_ids else {}
    new_candidates = [c for c in candidates if c[0] not in existing_map]
    skipped_existing = len(candidates) - len(new_candidates)
    print(f"[SWEEP] {len(candidates)} candidate(s) this run, {skipped_existing} already in DB, {len(new_candidates)} new to check.")

    new_inserted = 0

    for tmdb_id, item_media_type, item in new_candidates:
        try:
            if not passes_sync_quality_filter(item):
                continue

            title = item.get("title") or item.get("name")
            genre_map = genre_map_for(item_media_type)
            genres = [name for gid in item.get("genre_ids", []) if (name := genre_map.get(gid))]

            media_record = build_bare_media_record(
                tmdb_id=tmdb_id,
                media_type=item_media_type,
                title=title,
                overview=item.get("overview"),
                poster_path=item.get("poster_path"),
                release_date=item.get("release_date") or item.get("first_air_date"),
                original_language=item.get("original_language"),
                vote_average=item.get("vote_average"),
                vote_count=item.get("vote_count"),
                popularity=item.get("popularity"),
                genres=genres,
            )

            row_id = db.upsert_media(media_record)
            if row_id:
                new_inserted += 1
                print(f"[SWEEP]   {item_media_type}/{tmdb_id}: inserted bare row — {title!r}")
            else:
                errors += 1
        except Exception as exc:
            print(f"[SWEEP]   {item_media_type}/{tmdb_id}: processing failed — {exc}")
            errors += 1

    if sweep_complete:
        new_cursor = {"done": True}
        slices_remaining = 0
        cursor_str = "done"
        print("[SWEEP] All slices exhausted — sweep complete, will idle from now on.")
    else:
        new_cursor = {"media_type": media_type, "year": year, "page": page}
        slices_remaining = _slices_remaining(media_type, year, today_year)
        cursor_str = f"{media_type}/{year}/{page}"

    try:
        db.set_bot_state("discover_cursor", new_cursor)
    except Exception as exc:
        print(f"[SWEEP] WARNING: failed to persist discover_cursor — next run will redo this batch: {exc}")

    print(
        f"\n[SWEEP] SUMMARY cursor={cursor_str} checked={checked} skipped_existing={skipped_existing} "
        f"new_inserted={new_inserted} errors={errors} slices_remaining={slices_remaining}"
    )


if __name__ == "__main__":
    main()
