"""
Trakt sync — daily while the related-titles backlog is re-drained, then back
to Mondays (decision 1.439; .github/workflows/trakt_sync.yml). Writes
trakt_watchers_week, trakt_trending_rank, trakt_anticipated_score,
trakt_rating, trakt_vote_count, trakt_synced_at, trakt_related_synced_at and
public.media_related.

The app reads what this writes. Home's Trending This Week rail and its hero
banner order by trakt_watchers_week (get_trending_this_week);
get_media_popular blends a trakt_watchers_week rank 50/50 with its score; the
New In Theatres and Coming Soon rails gate on trakt_watchers_week and
trakt_anticipated_score; More Like This reads media_related
(get_related_titles). Only the bot's own compute_popularity_score ignores the
trakt_* columns.

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
  that didn't get refreshed this run — i.e. it fell off every list —
  through the server's trakt_decay_heat (migration 20261025160000), 200
  rows a call while a call returns 200, at most 50 calls. Anticipated
  tmdb_ids we don't have in the catalog are expected (mostly unreleased
  titles) and only logged, never inserted.

  Phase B — related-titles drain (bounded, state-driven, mirrors
  enrich_new_titles.py's queue). Takes up to TRAKT_RELATED_BATCH titles from
  the server's trakt_related_queue(p_limit, p_before) — never synced, or
  synced before TRAKT_RELATED_FIX_DATE — in the app's order (in theatres →
  home rails → popularity → newest), served by a partial index in
  milliseconds, in slices of RELATED_SLICE. For each title: /related by its
  IMDB id when the queue row carries a well-formed one (one Trakt call),
  otherwise — or when Trakt answers 404 for that IMDB id — by the Trakt id
  resolved from our tmdb_id (TraktClient.resolve_id); then ONE write,
  trakt_replace_related, which replaces the title's media_related rows (zero
  when Trakt has none) and stamps it in one transaction. It stops itself at
  the time budget (TRAKT_TIME_BUDGET_MIN, inside the job's timeout) and
  leaves its SUMMARY line. Before B5 it asked /related with our tmdb_id —
  which Trakt read as ITS id, so most titles got nothing and the rest got
  another title's relateds.

  The Trakt client paces itself under its own budget (B5.2): at most
  TRAKT_WINDOW_CALLS requests in any TRAKT_WINDOW_SECONDS (default 420 per
  300 s), lowered to 75 % (never below 100) after a run's first 429, with
  TRAKT_RATE_LIMIT's 3.0/s bucket underneath as the burst cap. Trakt sends
  it no X-Ratelimit header (run #26: nine 429s, 300 s Retry-After each), so
  B5.1's pause on that header acts only when one arrives; a 429 still costs
  its Retry-After, and every 429 prints how many requests it took.

Failure philosophy is deliberately NOT this repo's usual "log and keep
going" default: Phase A's list fetches are allowed to raise straight out of
main() (no top-level try/except) — a broken auth header or a Trakt API
shape change should fail the run loudly (red CI) rather than silently
write partial/empty signals for weeks; a missing server function fails
fast too, naming its migration. The deliberate exceptions: Phase A's decay
logs any other error and lets Phase B run; Phase B's per-title loop catches
and counts — one show's /related lookup failing shouldn't abort a
5,000-title drain; and a queue slice that fails for good stops the drain
with what landed, prints the SUMMARY and exits 1.

Run via:
    python -m bot.sync_trakt
"""

import os
import re
import sys
from datetime import datetime, timedelta, timezone

import requests

from .trakt import DEFAULT_WINDOW_CALLS, DEFAULT_WINDOW_SECONDS, TraktClient, redact_query
from .supabase_client import SupabaseClient, is_missing_function

DEFAULT_TRAKT_TOP_N = 300
DEFAULT_TRAKT_RELATED_BATCH = 5000  # what trakt_sync.yml passes (3000 until B5.4); asked in RELATED_SLICE asks (the server caps one at 10,000)
# Decision 1.439 (24/10): every trakt_related_synced_at stamp older than this
# moment was written by the wrong /related path (our tmdb_id in Trakt's id
# slot), so the server's queue hands those titles back. A run must start after
# it, or its own stamps would never leave the queue (the startup guard).
TRAKT_RELATED_FIX_DATE = "2026-10-24T16:00:00Z"
# PostgREST trims every response to 1,000 rows, the RPC's included (B3).
RELATED_SLICE = 1000
# Phase B stops before the next title or slice once this many minutes have
# passed since the run's start (B5.1) — inside the job's timeout-minutes 90,
# so a run ends with its SUMMARY instead of being killed mid-title.
DEFAULT_TRAKT_TIME_BUDGET_MIN = 80
# The longest TRAKT_WINDOW_SECONDS a run accepts (B5.2): one budget wait lasts
# at most a window, and the job's timeout-minutes 90 leave 10 minutes past
# the 80-minute budget.
MAX_TRAKT_WINDOW_SECONDS = 600
# A well-formed IMDB id: Trakt's /related takes it in the id slot, so a title
# that has one costs one Trakt call instead of resolve + /related (B5.1).
_IMDB_ID = re.compile(r"tt\d+")
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
    try:
        decayed, calls = db.decay_trakt_heat(run_start_iso)
    except Exception as exc:
        if is_missing_function(exc):
            _fail_fast_missing("trakt_decay_heat", "20261025160000")
        raise
    print(f"[TRAKT SYNC]   decayed heat on {decayed} title(s) in {calls} call(s)")

    return stats


def _run_phase_b(trakt: TraktClient, db: SupabaseClient, run_start_iso: str, batch_size: int,
                 deadline: datetime | None = None, clock=None, budget_min: int | None = None) -> dict:
    """*deadline* (aware UTC) is the time budget's end, read on *clock* —
    both from main, injected by tests; None means no budget."""
    # scanned: titles taken from the queue — attempts, in fact: a title that
    # errors is not stamped, so a later slice of the same run serves it again
    # and it is counted again. related_by_imdb: titles whose /related Trakt
    # answered by their IMDB id. related_by_tmdb: titles that took the resolve
    # path instead (no usable imdb_id, or one Trakt does not know), resolved
    # or not. resolved / unresolved: the outcome of that path's lookup.
    # errors: any attempt that raised — at /related, the lookup or the write.
    # stuck_at: the media_id of a title this run stamped that the queue handed
    # back (the stamps are not landing), or None. queue_failed: what a slice
    # that failed for good raised (after the client's retries), or None.
    # budget_reached: the time budget stopped the drain (a plan, not a failure).
    stats = {"scanned": 0, "resolved": 0, "unresolved": 0, "related_rows": 0, "errors": 0,
             "related_by_imdb": 0, "related_by_tmdb": 0,
             "stuck_at": None, "queue_failed": None, "budget_reached": False}
    now = clock if clock is not None else (lambda: datetime.now(timezone.utc))

    def out_of_time() -> bool:
        if deadline is None or now() < deadline:
            return False
        stats["budget_reached"] = True
        print(
            f"[TRAKT SYNC] time budget of {budget_min} min reached after {stats['scanned']} title(s) — "
            "stopping this run's drain; the rest waits for the next run"
        )
        return True

    print(
        f"\n[TRAKT SYNC] Phase B: draining up to {batch_size} title(s) from trakt_related_queue "
        f"(never synced, or synced before {TRAKT_RELATED_FIX_DATE}), in slices of {RELATED_SLICE}..."
    )

    # SLICES (B3's lesson): PostgREST trims every response to 1,000 rows, so ask
    # for at most RELATED_SLICE, process and stamp that slice, and ask AGAIN —
    # each call recomputes the queue on the server, so the titles just stamped
    # drop out — until the batch is filled or a slice comes back short.
    attempted_this_run: set = set()  # every title drained this run
    stamped_this_run: set = set()    # ...whose LAST drain ended in its stamp
    drained_again: set = set()       # ...served again after a stamp: drained once more
    while stats["scanned"] < batch_size:
        if out_of_time():
            break
        want = min(batch_size - stats["scanned"], RELATED_SLICE)
        try:
            rows = _fetch_related_slice(db, want)
        except Exception as exc:
            # Failed for good — the client already retried what is transient
            # (a missing function never gets here: it exits). What landed
            # stays landed; main still prints the SUMMARY, then exits 1.
            stats["queue_failed"] = f"{type(exc).__name__}: {exc}"
            print(f"[TRAKT SYNC] ERROR: the queue slice failed — {exc}; stopping this run's drain with what landed")
            break
        if not rows:
            break
        # A title this run stamped, back in a new slice, is a stamp the queue
        # does not see. Since B5.1 the rows and the stamp land in one
        # transaction (trakt_replace_related), so it is no longer a PATCH
        # swallowed on its own — it would be a stamp the queue reads as older
        # than p_before, or a lagging read: drain it once more, the write is
        # idempotent. A title that FAILED is not one: it was never stamped, so
        # it rightly heads the next slice. (The brief's first form, the same
        # first id in two slices, fired on that too — one title that always
        # fails at the head would cut every run to one slice.)
        # Stop at once, and say which, when the stamps are not landing:
        #   - a FULL slice brings nothing this run has not tried, and some of
        #     it was stamped (a thousand lost stamps is no hiccup), or
        #   - a title comes back after its second stamp too.
        # The loop ends on its own (scanned grows every pass); this bounds the
        # waste and turns the run red. What it cannot see: a short slice is
        # drained once more, so a queue smaller than a slice, or a last ask
        # smaller than a slice (a batch between RELATED_SLICE and twice it),
        # ends green even when no stamp lands. The schedule's 5,000 is five
        # full asks.
        repeats = [r for r in rows if r["id"] in stamped_this_run]
        twice = next((r for r in repeats if r["id"] in drained_again), None)
        nothing_new = len(rows) == RELATED_SLICE and all(r["id"] in attempted_this_run for r in rows)
        if twice is not None or (repeats and nothing_new):
            first = twice if twice is not None else repeats[0]
            stats["stuck_at"] = first["id"]
            what = ("came back after its second stamp" if twice is not None
                    else "heads a full slice of titles this run already tried")
            print(
                f"[TRAKT SYNC] ERROR: media_id={first['id']} tmdb={first.get('tmdb_id')} «{first.get('title')}» "
                f"{what} — the stamps are not landing; stopping."
            )
            break
        for r in repeats:
            drained_again.add(r["id"])
            print(f"[TRAKT SYNC]   stamp did not land for media_id={r['id']} «{r.get('title')}» — draining it again")

        for row in rows:
            if out_of_time():
                break
            attempted_this_run.add(row["id"])
            if _drain_one(trakt, db, run_start_iso, row, stats):
                stamped_this_run.add(row["id"])
            else:
                stamped_this_run.discard(row["id"])  # its last drain failed: not stamped
            if stats["scanned"] % 100 == 0:
                print(f"[TRAKT SYNC]   progress: {stats['scanned']}/{batch_size} scanned")
        if stats["budget_reached"] or len(rows) < want:
            break  # out of time, or the queue is exhausted

    return stats


def _fail_fast_missing(function: str, migration: str) -> None:
    """The server has no public.<function>: one line naming the migration
    that creates it, exit 1 — B3's fetch_queue shape. The client never exits
    the process; sync_trakt does, here."""
    print(f"[TRAKT SYNC] ERROR: the server has no public.{function} — apply migration {migration} before this code runs.")
    sys.exit(1)


def _fetch_related_slice(db: SupabaseClient, want: int) -> list[dict]:
    """One slice of the queue. FAILS FAST if the server has no
    trakt_related_queue (PGRST202, "Could not find the function"): one line
    naming the migration, exit 1, nothing stamped — B3's fetch_queue shape."""
    try:
        return db.get_titles_for_related_drain(want, TRAKT_RELATED_FIX_DATE)
    except Exception as exc:
        if is_missing_function(exc):
            _fail_fast_missing("trakt_related_queue", "20261024210000")
        raise


def _write_related(db: SupabaseClient, media_id: str, rows: list[dict], run_start_iso: str) -> int:
    """One title's rows and stamp, through the one RPC. A missing function
    exits (SystemExit is not an Exception: _drain_one's catch lets it by);
    any other error is _drain_one's to count."""
    try:
        return db.write_related(media_id, rows, run_start_iso)
    except Exception as exc:
        if is_missing_function(exc):
            _fail_fast_missing("trakt_replace_related", "20261025160000")
        raise


def _related_by_imdb(trakt: TraktClient, media_type: str, imdb_id: str) -> list[dict] | None:
    """/related by IMDB id; None when Trakt answers 404 for it (it does not
    know that IMDB id), so the caller falls back to the resolve path. Any
    other error propagates: an error, never a fallback."""
    try:
        return trakt.get_related(media_type, imdb_id, limit=20)
    except requests.HTTPError as exc:
        if exc.response is not None and exc.response.status_code == 404:
            return None
        raise


def _drain_one(trakt: TraktClient, db: SupabaseClient, run_start_iso: str, row: dict, stats: dict) -> bool:
    """One title of Phase B: /related by its IMDB id, or by the Trakt id
    resolved from its tmdb_id; then its rows and stamp in one RPC. Returns
    True when the title was stamped, False when it failed."""
    media_id = row["id"]
    tmdb_id = row["tmdb_id"]
    media_type = row["media_type"]
    stats["scanned"] += 1

    try:
        # B5.1: /related takes an IMDB id in the same slot as Trakt's own id,
        # so a title the queue serves with a well-formed one is asked by it —
        # one call, no lookup. (A server still on B5's queue has no imdb_id
        # column: .get reads None, and the title takes the resolve path.)
        items = None
        imdb_id = row.get("imdb_id")
        if isinstance(imdb_id, str) and _IMDB_ID.fullmatch(imdb_id):
            items = _related_by_imdb(trakt, media_type, imdb_id)
            if items is not None:
                stats["related_by_imdb"] += 1
        if items is None:
            # B5: /related wants Trakt's own id; our tmdb_id there names
            # another title. Resolve it first — with no usable imdb_id, or one
            # Trakt answered 404 for.
            stats["related_by_tmdb"] += 1
            trakt_id = trakt.resolve_id(media_type, tmdb_id)
            if trakt_id is None:
                stats["unresolved"] += 1
                kind = "show" if media_type == "tv" else "movie"
                print(f"[TRAKT] unresolved tmdb={tmdb_id} type={kind} «{row.get('title')}»")
                # Zero rows, so a stale row from the old path can never survive.
                _write_related(db, media_id, [], run_start_iso)
                return True
            stats["resolved"] += 1
            items = trakt.get_related(media_type, trakt_id, limit=20)

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

        stats["related_rows"] += _write_related(db, media_id, related_rows, run_start_iso)
        return True
    except Exception as exc:
        print(f"[TRAKT SYNC]   tmdb_id={tmdb_id}: related drain failed — {redact_query(str(exc))}")
        stats["errors"] += 1
        return False


def main(clock=None) -> None:
    """*clock* returns the current instant (aware UTC) — the run's start, the
    time budget's checks and the client's pause all read it; injected by
    tests, the real clock otherwise."""
    print("[TRAKT SYNC] Starting Trakt sync...")
    config = load_env()

    clock = clock if clock is not None else (lambda: datetime.now(timezone.utc))
    now = clock()
    fix_date = datetime.fromisoformat(TRAKT_RELATED_FIX_DATE.replace("Z", "+00:00"))
    if now <= fix_date:
        # A stamp before the fix date counts as unsynced on the server (strict
        # <): this run's stamps would never leave trakt_related_queue. The
        # guard also refuses the instant itself.
        print(
            f"[TRAKT SYNC] ERROR: this run starts at {now.isoformat()}, not after the fix date "
            f"{TRAKT_RELATED_FIX_DATE} — its stamps would never leave trakt_related_queue; stopping."
        )
        sys.exit(1)
    run_start_iso = now.isoformat()
    top_n = int(os.environ.get("TRAKT_TOP_N", DEFAULT_TRAKT_TOP_N))
    related_batch = int(os.environ.get("TRAKT_RELATED_BATCH", DEFAULT_TRAKT_RELATED_BATCH))
    budget_min = int(os.environ.get("TRAKT_TIME_BUDGET_MIN", DEFAULT_TRAKT_TIME_BUDGET_MIN))
    # The Trakt client's own sliding-window budget (B5.2): Trakt sends no
    # X-Ratelimit header, so the client paces itself under this.
    window_calls = int(os.environ.get("TRAKT_WINDOW_CALLS", DEFAULT_WINDOW_CALLS))
    window_seconds = int(os.environ.get("TRAKT_WINDOW_SECONDS", DEFAULT_WINDOW_SECONDS))
    # Fail fast on a window that cannot work, before Phase A writes anything:
    # no calls at all, no window at all, or a window whose single wait could
    # outrun the 10 minutes between the 80-minute budget and the job's 90.
    if window_calls < 1:
        print(f"[TRAKT SYNC] ERROR: TRAKT_WINDOW_CALLS must be at least 1 (got {window_calls}).")
        sys.exit(1)
    if not 1 <= window_seconds <= MAX_TRAKT_WINDOW_SECONDS:
        print(
            f"[TRAKT SYNC] ERROR: TRAKT_WINDOW_SECONDS must be between 1 and {MAX_TRAKT_WINDOW_SECONDS} "
            f"(got {window_seconds})."
        )
        sys.exit(1)
    deadline = now + timedelta(minutes=budget_min)

    trakt = TraktClient(client_id=config["TRAKT_CLIENT_ID"], clock=clock,
                        window_calls=window_calls, window_seconds=window_seconds)
    db = SupabaseClient(url=config["SUPABASE_URL"], key=config["SUPABASE_KEY"])

    phase_a = _run_phase_a(trakt, db, run_start_iso, top_n)
    phase_b = _run_phase_b(trakt, db, run_start_iso, related_batch,
                           deadline=deadline, clock=clock, budget_min=budget_min)

    sample = phase_a["unmatched_anticipated_ids"][:10]
    print(
        f"\n[TRAKT SYNC] SUMMARY updated={phase_a['updated']} "
        f"type_mismatches={phase_a['type_mismatches']} "
        f"unmatched_anticipated={len(phase_a['unmatched_anticipated_ids'])} "
        f"related_scanned={phase_b['scanned']} related_resolved={phase_b['resolved']} "
        f"related_unresolved={phase_b['unresolved']} related_rows={phase_b['related_rows']} "
        f"related_errors={phase_b['errors']} "
        f"related_by_imdb={phase_b['related_by_imdb']} related_by_tmdb={phase_b['related_by_tmdb']} "
        f"api_calls~{phase_a['api_calls']} "
        f"{trakt.rate_limit_summary()} "
        f"unmatched_anticipated_sample={sample}"
    )
    if phase_b.get("stuck_at") or phase_b.get("queue_failed"):
        sys.exit(1)  # Phase B said why, above: the run is red, on purpose


if __name__ == "__main__":
    main()
