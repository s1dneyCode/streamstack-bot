"""
Weekly Trakt sync — SHADOW MODE. Populates trakt_watchers_week,
trakt_trending_rank, trakt_anticipated_score, trakt_rating, trakt_vote_count,
trakt_synced_at, trakt_related_synced_at, and public.media_related only.
Does NOT touch compute_popularity_score, get_media_popular, or any existing
ranking logic — those are wired up separately, later, once the signal has
been judged.

Two phases:

  Phase A — list ingest (cheap, ~a handful of paginated calls per media_type).
  For movie and tv separately: pull Trakt's weekly most-watched, trending,
  and anticipated lists (up to TRAKT_TOP_N items each), merge whatever
  signals each tmdb_id appeared with, batch-match against our catalog via
  get_catalog_tmdb_index(), and write matched rows with
  update_trakt_list_signals(). A title's Trakt id/media_type is trusted
  against OUR OWN catalog's media_type for that tmdb_id before writing —
  TMDB's movie and tv id spaces are independent, so a numeric collision is
  possible; a mismatch is skipped and counted, never written to the wrong
  row. Once both media_types are done, decay_trakt_heat() nulls the three
  list-membership columns (not trakt_rating/trakt_vote_count) on any row
  that didn't get refreshed this run — i.e. it fell off every list.
  Anticipated tmdb_ids we don't have in the catalog are expected (mostly
  unreleased titles) and only logged, never inserted.

  Phase B — related-titles drain (bounded, state-driven, mirrors
  enrich_new_titles.py's enriched_at IS NULL scan). Selects up to
  TRAKT_RELATED_BATCH rows where trakt_related_synced_at IS NULL, ordered
  by popularity desc, fetches /related for each, and replaces that title's
  media_related rows.

Failure philosophy is deliberately NOT this repo's usual "log and keep
going" default: Phase A's list fetches are allowed to raise straight out of
main() (no top-level try/except) — this is a brand-new, unproven
integration, and a broken auth header or a Trakt API shape change should
fail the run loudly (red CI) rather than silently write partial/empty
signals for weeks. The one deliberate exception is Phase B's per-title
loop, which does catch and count — one show's /related lookup failing
shouldn't abort a 1500-title drain.

Run via:
    python -m bot.sync_trakt
"""

import os
import sys
from datetime import datetime, timezone

from .trakt import TraktClient
from .supabase_client import SupabaseClient

DEFAULT_TRAKT_TOP_N = 300
DEFAULT_TRAKT_RELATED_BATCH = 1500
PAGE_LIMIT = 100  # Trakt's page size per request for list endpoints

_TITLE_KEY = {"movie": "movie", "tv": "show"}


def load_env() -> dict[str, str]:
    required = ["TRAKT_CLIENT_ID", "SUPABASE_URL", "SUPABASE_KEY"]
    config: dict[str, str] = {}
    for key in required:
        value = os.environ.get(key)
        if not value:
            print(f"[TRAKT SYNC] ERROR: Required environment variable '{key}' is not set.")
            sys.exit(1)
        config[key] = value
    return config


def _title_obj(item: dict, media_type: str) -> dict:
    return item.get(_TITLE_KEY[media_type]) or {}


def _round_rating(rating) -> float | None:
    return round(rating, 2) if isinstance(rating, (int, float)) else None


def _paginate_top_n(fetch_page_fn, top_n: int, page_limit: int = PAGE_LIMIT) -> tuple[list[dict], int]:
    """
    Calls fetch_page_fn(page, limit) -> (items, page_count) repeatedly
    (page=1,2,...), accumulating items until top_n collected or Trakt's own
    page_count is exhausted, whichever comes first. Returns (items, calls).
    """
    items: list[dict] = []
    page = 1
    calls = 0
    while len(items) < top_n:
        batch, page_count = fetch_page_fn(page, page_limit)
        calls += 1
        if not batch:
            break
        items.extend(batch)
        if page >= page_count:
            break
        page += 1
    return items[:top_n], calls


def _run_phase_a(trakt: TraktClient, db: SupabaseClient, run_start_iso: str, top_n: int) -> dict:
    stats = {
        "updated": 0,
        "type_mismatches": 0,
        "unmatched_anticipated_ids": [],
        "api_calls": 0,
    }

    for media_type in ("movie", "tv"):
        label = "movies" if media_type == "movie" else "shows"
        print(f"\n[TRAKT SYNC] Phase A ({label}): fetching most-watched/trending/anticipated (top {top_n})...")

        signals_by_tmdb_id: dict[int, dict] = {}
        anticipated_tmdb_ids: set[int] = set()

        def accumulate(tmdb_id: int, **fields) -> None:
            existing = signals_by_tmdb_id.setdefault(tmdb_id, {})
            for k, v in fields.items():
                if v is not None:
                    existing[k] = v

        watched, calls = _paginate_top_n(
            lambda p, l: trakt.get_most_watched(media_type, "weekly", p, l), top_n
        )
        stats["api_calls"] += calls
        for item in watched:
            title = _title_obj(item, media_type)
            tmdb_id = (title.get("ids") or {}).get("tmdb")
            if tmdb_id is None:
                continue
            accumulate(
                tmdb_id,
                trakt_watchers_week=item.get("watcher_count"),
                trakt_rating=_round_rating(title.get("rating")),
                trakt_vote_count=title.get("votes"),
            )
        print(f"[TRAKT SYNC]   most-watched: {len(watched)} item(s), {calls} call(s).")

        trending, calls = _paginate_top_n(
            lambda p, l: trakt.get_trending(media_type, p, l), top_n
        )
        stats["api_calls"] += calls
        for rank, item in enumerate(trending, start=1):
            title = _title_obj(item, media_type)
            tmdb_id = (title.get("ids") or {}).get("tmdb")
            if tmdb_id is None:
                continue
            accumulate(tmdb_id, trakt_trending_rank=rank)
        print(f"[TRAKT SYNC]   trending: {len(trending)} item(s), {calls} call(s).")

        anticipated, calls = _paginate_top_n(
            lambda p, l: trakt.get_anticipated(media_type, p, l), top_n
        )
        stats["api_calls"] += calls
        for item in anticipated:
            title = _title_obj(item, media_type)
            tmdb_id = (title.get("ids") or {}).get("tmdb")
            if tmdb_id is None:
                continue
            anticipated_tmdb_ids.add(tmdb_id)
            accumulate(
                tmdb_id,
                trakt_anticipated_score=item.get("list_count"),
                trakt_rating=_round_rating(title.get("rating")),
                trakt_vote_count=title.get("votes"),
            )
        print(f"[TRAKT SYNC]   anticipated: {len(anticipated)} item(s), {calls} call(s).")

        all_tmdb_ids = list(signals_by_tmdb_id.keys())
        print(f"[TRAKT SYNC]   {len(all_tmdb_ids)} unique tmdb_id(s) seen across all three {label} lists.")
        idx = db.get_catalog_tmdb_index(all_tmdb_ids)

        for tmdb_id, fields in signals_by_tmdb_id.items():
            entry = idx.get(tmdb_id)
            if entry is None:
                if tmdb_id in anticipated_tmdb_ids:
                    stats["unmatched_anticipated_ids"].append(tmdb_id)
                continue
            if entry["media_type"] != media_type:
                # Same tmdb_id, different media_type already in our catalog —
                # TMDB's movie/tv id spaces are independent, so this can
                # genuinely happen. Refuse to write a movie's signals onto a
                # tv row (or vice versa) rather than silently corrupting it.
                stats["type_mismatches"] += 1
                continue
            db.update_trakt_list_signals(tmdb_id, fields, run_start_iso)
            stats["updated"] += 1

    print("\n[TRAKT SYNC] Phase A: decaying stale heat (rows not refreshed this run)...")
    db.decay_trakt_heat(run_start_iso)

    return stats


def _run_phase_b(trakt: TraktClient, db: SupabaseClient, run_start_iso: str, batch_size: int) -> dict:
    stats = {"drained": 0, "related_written": 0, "errors": 0}

    batch = db.get_titles_for_related_drain(batch_size)
    total = len(batch)
    print(f"\n[TRAKT SYNC] Phase B: draining {total} title(s) with trakt_related_synced_at IS NULL...")

    for i, row in enumerate(batch, start=1):
        media_id = row["id"]
        tmdb_id = row["tmdb_id"]
        media_type = row["media_type"]

        try:
            items = trakt.get_related(media_type, tmdb_id)

            related_rows = []
            rank = 0
            for item in items:
                related_tmdb_id = (item.get("ids") or {}).get("tmdb")
                if related_tmdb_id is None:
                    continue
                rank += 1
                related_rows.append({
                    "related_tmdb_id": related_tmdb_id,
                    "related_media_type": media_type,
                    "rank": rank,
                })

            db.replace_media_related(media_id, related_rows, run_start_iso)
            db.stamp_trakt_related_synced(media_id, run_start_iso)

            stats["drained"] += 1
            stats["related_written"] += len(related_rows)

            if i % 100 == 0:
                print(f"[TRAKT SYNC]   progress: {i}/{total} drained")
        except Exception as exc:
            print(f"[TRAKT SYNC]   tmdb_id={tmdb_id}: related drain failed — {exc}")
            stats["errors"] += 1

    return stats


def main() -> None:
    print("[TRAKT SYNC] Starting weekly Trakt sync (shadow mode)...")
    config = load_env()

    run_start_iso = datetime.now(timezone.utc).isoformat()
    top_n = int(os.environ.get("TRAKT_TOP_N", DEFAULT_TRAKT_TOP_N))
    related_batch = int(os.environ.get("TRAKT_RELATED_BATCH", DEFAULT_TRAKT_RELATED_BATCH))

    trakt = TraktClient(client_id=config["TRAKT_CLIENT_ID"])
    db = SupabaseClient(url=config["SUPABASE_URL"], key=config["SUPABASE_KEY"])

    phase_a = _run_phase_a(trakt, db, run_start_iso, top_n)
    phase_b = _run_phase_b(trakt, db, run_start_iso, related_batch)

    sample = phase_a["unmatched_anticipated_ids"][:10]
    print(
        f"\n[TRAKT SYNC] SUMMARY updated={phase_a['updated']} "
        f"type_mismatches={phase_a['type_mismatches']} "
        f"unmatched_anticipated={len(phase_a['unmatched_anticipated_ids'])} "
        f"related_drained={phase_b['drained']} related_written={phase_b['related_written']} "
        f"related_errors={phase_b['errors']} "
        f"api_calls~{phase_a['api_calls']} "
        f"unmatched_anticipated_sample={sample}"
    )


if __name__ == "__main__":
    main()
