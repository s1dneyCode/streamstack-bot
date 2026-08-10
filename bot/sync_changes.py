"""
Daily /changes sync: catches new titles between nightly bot runs by walking
TMDB's /movie/changes and /tv/changes feeds — the same feed TMDB itself
recommends for staying in sync without re-crawling the whole catalog.

Window: since the last successful run (last_synced_at, persisted in
bot_state), defaulting to 24h if no prior run is recorded. If a run was
missed and more than TMDB's 14-day max has elapsed, the window is capped at
14 days — TMDB simply won't report changes further back than that.

Changed ids are batch-checked against the DB first, then split two ways:

  Ids NOT already in our DB — candidates for a bare-row insert:
  - 404 → never existed in our DB and already gone from TMDB. Nothing to do.
  - Passes the shared quality filter → bare-row insert (enriched_at NULL;
    enrich_new_titles.py fills in the rest).
  - Otherwise → skipped (doesn't clear the bar for a bare insert).

  Ids we ALREADY have — refreshed in place from one appended detail call
  (append_to_response=watch/providers), capped at MAX_EXISTING_REFRESH
  (default 2000) per run. This is what keeps a title's mutable fields from
  freezing at whatever they were on the day it was first ingested:
  popularity/popularity_score, tmdb_score, vote_count, status, overview,
  genres (and the derived is_documentary), runtime (movies), plus its
  streaming providers and is_streamable_now. A 404 here soft-flags
  tmdb_missing_at.

  A successful refresh also stamps streaming_last_checked, so main.py's
  Step 10 doesn't re-fetch the same title within its window. Titles that
  404'd (already soft-flagged) or errored are left un-stamped. This is safe
  only because RT retry runs off its own rt_last_checked cursor — while the
  two shared one column, stamping here evicted titles from the RT-retry
  queue that this script has no OMDb client to serve.

  release_date is deliberately NOT touched: media.release_date is the
  earliest *worldwide* date derived from /movie/{id}/release_dates (see
  main.py Step 9), not the base detail payload's value — writing it through
  would revert it.

TMDB-side deletions are caught in several places: the refresh branch above,
main.py's Step 10 weekly provider re-verify, and enrich_new_titles.py's
Step 0 base detail check for anything not yet enriched.

Run via:
    python -m bot.sync_changes
"""

import os
import sys
from datetime import datetime, timedelta, timezone
from itertools import zip_longest

import requests

from .tmdb import TmdbClient
from .supabase_client import SupabaseClient, compute_popularity_score, ALLOWED_PROVIDERS
from .filters import passes_sync_quality_filter, build_bare_media_record

DEFAULT_WINDOW_HOURS = 24
MAX_WINDOW_DAYS = 14
DEFAULT_MAX_EXISTING_REFRESH = 2000


def load_env() -> dict[str, str]:
    required = ["TMDB_API_KEY", "SUPABASE_URL", "SUPABASE_KEY"]
    config: dict[str, str] = {}
    for key in required:
        value = os.environ.get(key)
        if not value:
            print(f"[CHANGES] ERROR: Required environment variable '{key}' is not set.")
            sys.exit(1)
        config[key] = value
    return config


def _compute_window(db: SupabaseClient, now: datetime) -> tuple[datetime, str, str]:
    """
    Return (window_start, start_date, end_date) — window_start is the precise
    timestamp used to persist last_synced_at; start_date/end_date are the
    YYYY-MM-DD strings TMDB's /changes endpoints require (date granularity
    only, so a run partway through window_start's day re-covers that whole
    day — harmless, since re-processing an id is idempotent).
    """
    try:
        last_synced_raw = db.get_bot_state("last_synced_at")
    except Exception as exc:
        print(f"[CHANGES] WARNING: failed to read last_synced_at ({exc}) — defaulting to {DEFAULT_WINDOW_HOURS}h window.")
        last_synced_raw = None

    last_synced = None
    if last_synced_raw:
        try:
            last_synced = datetime.fromisoformat(last_synced_raw)
        except ValueError:
            print(f"[CHANGES] WARNING: could not parse stored last_synced_at={last_synced_raw!r} — defaulting to {DEFAULT_WINDOW_HOURS}h window.")

    if last_synced is None:
        window_start = now - timedelta(hours=DEFAULT_WINDOW_HOURS)
        print(f"[CHANGES] No prior last_synced_at — using default {DEFAULT_WINDOW_HOURS}h window.")
    else:
        window_start = last_synced
        cap = now - timedelta(days=MAX_WINDOW_DAYS)
        if window_start < cap:
            missed_days = (now - window_start).days
            print(f"[CHANGES] Last sync was {missed_days} day(s) ago — widening window, capped at TMDB's {MAX_WINDOW_DAYS}-day max.")
            window_start = cap

    return window_start, window_start.date().isoformat(), now.date().isoformat()


def main() -> None:
    print("[CHANGES] Starting daily /changes sync...")
    config = load_env()

    db   = SupabaseClient(url=config["SUPABASE_URL"], key=config["SUPABASE_KEY"])
    tmdb = TmdbClient(api_key=config["TMDB_API_KEY"])

    now = datetime.now(timezone.utc)
    window_start, start_date, end_date = _compute_window(db, now)
    print(f"[CHANGES] Window: {start_date} to {end_date} (start_date={window_start.isoformat()}).")

    try:
        movie_ids = tmdb.get_movie_changes(start_date, end_date)
    except Exception as exc:
        print(f"[CHANGES] ERROR: failed to fetch /movie/changes: {exc}")
        movie_ids = []

    try:
        tv_ids = tmdb.get_tv_changes(start_date, end_date)
    except Exception as exc:
        print(f"[CHANGES] ERROR: failed to fetch /tv/changes: {exc}")
        tv_ids = []

    changed = [(tmdb_id, "movie") for tmdb_id in movie_ids] + [(tmdb_id, "tv") for tmdb_id in tv_ids]
    checked = len(changed)
    print(f"[CHANGES] {len(movie_ids)} changed movie id(s), {len(tv_ids)} changed tv id(s) — {checked} total.")

    # get_catalog_tmdb_index (rather than get_media_ids_by_tmdb_ids) because
    # the refresh branch below needs each row's media_type as well as its id —
    # see the media_type guard there. The new-candidate partition only does a
    # key-membership test, so it behaves identically either way.
    existing_map = db.get_catalog_tmdb_index([tmdb_id for tmdb_id, _ in changed]) if changed else {}
    new_candidates = [(tmdb_id, media_type) for tmdb_id, media_type in changed if tmdb_id not in existing_map]
    existing_candidates = [(tmdb_id, mt) for (tmdb_id, mt) in changed if tmdb_id in existing_map]
    skipped_existing = checked - len(new_candidates)

    raw_cap = os.environ.get("MAX_EXISTING_REFRESH")
    try:
        max_existing_refresh = int(raw_cap) if raw_cap else DEFAULT_MAX_EXISTING_REFRESH
    except ValueError:
        print(f"[CHANGES] WARNING: MAX_EXISTING_REFRESH={raw_cap!r} is not an integer — using {DEFAULT_MAX_EXISTING_REFRESH}.")
        max_existing_refresh = DEFAULT_MAX_EXISTING_REFRESH
    if max_existing_refresh < 0:
        # A negative value would become a Python negative slice and refresh
        # nearly EVERYTHING — the opposite of a cap.
        print(f"[CHANGES] WARNING: MAX_EXISTING_REFRESH={max_existing_refresh} is negative — using {DEFAULT_MAX_EXISTING_REFRESH}.")
        max_existing_refresh = DEFAULT_MAX_EXISTING_REFRESH

    # `changed` is built movies-then-tv, so a plain head slice would drop
    # from the tv tail — deterministically, the same way every run, meaning
    # no tv title would EVER be refreshed once movie changes alone fill the
    # cap. Interleave the two so the cap samples both types proportionally.
    cap_dropped = 0
    if len(existing_candidates) > max_existing_refresh:
        movie_side = [c for c in existing_candidates if c[1] == "movie"]
        tv_side    = [c for c in existing_candidates if c[1] == "tv"]
        interleaved: list[tuple[int, str]] = []
        for a, b in zip_longest(movie_side, tv_side):
            if a is not None:
                interleaved.append(a)
            if b is not None:
                interleaved.append(b)
        cap_dropped = len(existing_candidates) - max_existing_refresh
        existing_candidates = interleaved[:max_existing_refresh]
        print(
            f"[CHANGES] {cap_dropped} existing changed title(s) over the "
            f"MAX_EXISTING_REFRESH={max_existing_refresh} cap — NOT refreshed this run "
            f"(movies/tv interleaved before slicing so neither type is starved; "
            f"the rest are still picked up by main.py's Step 10 reverify queue)."
        )

    print(
        f"[CHANGES] {skipped_existing} already in DB ({len(existing_candidates)} to refresh). "
        f"{len(new_candidates)} new candidate(s) to check."
    )

    new_inserted = 0
    errors       = 0

    for tmdb_id, media_type in new_candidates:
        try:
            detail = tmdb._get(f"/{media_type}/{tmdb_id}")
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status == 404:
                # Never existed in our DB and already gone from TMDB — nothing to do.
                continue
            print(f"[CHANGES]   {media_type}/{tmdb_id}: fetch failed ({status}) — {exc}")
            errors += 1
            continue
        except Exception as exc:
            print(f"[CHANGES]   {media_type}/{tmdb_id}: fetch failed — {exc}")
            errors += 1
            continue

        try:
            if not passes_sync_quality_filter(detail):
                continue

            title = detail.get("title") or detail.get("name")
            media_record = build_bare_media_record(
                tmdb_id=tmdb_id,
                media_type=media_type,
                title=title,
                overview=detail.get("overview"),
                poster_path=detail.get("poster_path"),
                release_date=detail.get("release_date") or detail.get("first_air_date"),
                original_language=detail.get("original_language"),
                vote_average=detail.get("vote_average"),
                vote_count=detail.get("vote_count"),
                popularity=detail.get("popularity"),
                genres=[g["name"] for g in detail.get("genres", []) if g.get("name")],
                imdb_id=detail.get("imdb_id"),
                status=detail.get("status"),
            )

            row_id = db.upsert_media(media_record)
            if row_id:
                new_inserted += 1
                print(f"[CHANGES]   {media_type}/{tmdb_id}: inserted bare row — {title!r}")
            else:
                errors += 1
        except Exception as exc:
            print(f"[CHANGES]   {media_type}/{tmdb_id}: processing failed — {exc}")
            errors += 1

    # ------------------------------------------------------------------ #
    # Refresh existing changed titles                                     #
    # ------------------------------------------------------------------ #
    # One appended detail call per title covers both the mutable scalar
    # fields and watch/providers — the same shape main.py's merged Step 10
    # uses. Without this, everything below stays frozen at whatever it was
    # when the title was first ingested.
    print(f"\n[CHANGES] Refreshing {len(existing_candidates)} existing changed title(s)...")

    refreshed        = 0
    refresh_missing  = 0
    refresh_errors   = 0
    type_mismatches  = 0

    for tmdb_id, media_type in existing_candidates:
        entry = existing_map[tmdb_id]
        media_id = entry["id"]

        # TMDB's movie and tv id spaces are independent, so a numeric
        # collision is possible and existing_map is keyed by tmdb_id alone.
        # Refuse to write a movie's payload onto a tv row (or vice versa)
        # rather than silently corrupting it — this branch WRITES, unlike
        # the new-candidate partition above which only reads the key.
        if entry["media_type"] != media_type:
            type_mismatches += 1
            continue

        try:
            detail = tmdb._get(f"/{media_type}/{tmdb_id}", params={"append_to_response": "watch/providers"})
        except requests.HTTPError as exc:
            http_status = exc.response.status_code if exc.response is not None else None
            if http_status == 404:
                db.set_tmdb_missing(media_id)
                refresh_missing += 1
                print(f"[CHANGES]   {media_type}/{tmdb_id}: 404 — removed from TMDB, flagged tmdb_missing_at.")
                continue
            print(f"[CHANGES]   {media_type}/{tmdb_id}: refresh fetch failed ({http_status}) — {exc}")
            refresh_errors += 1
            continue
        except Exception as exc:
            print(f"[CHANGES]   {media_type}/{tmdb_id}: refresh fetch failed — {exc}")
            refresh_errors += 1
            continue

        try:
            raw_providers = detail.get("watch/providers") or {}
            providers = tmdb.get_watch_providers(tmdb_id=tmdb_id, media_type=media_type, data=raw_providers)
            providers = {
                region: {kind: [p for p in names if p in ALLOWED_PROVIDERS] for kind, names in kinds.items()}
                for region, kinds in providers.items()
            }

            has_providers = any(names for kinds in providers.values() for names in kinds.values())
            if has_providers:
                is_streamable = bool(providers.get("US", {}).get("flatrate"))
                db.sync_streaming_providers(media_id=media_id, providers=providers)

            popularity   = detail.get("popularity")
            tmdb_score   = round((detail.get("vote_average") or 0) * 10)
            vote_count   = detail.get("vote_count")
            status       = detail.get("status")
            overview     = detail.get("overview")
            genres       = [g["name"] for g in detail.get("genres", []) if g.get("name")]
            runtime      = detail.get("runtime") if media_type == "movie" else None

            # is_documentary is DERIVED from the genre ids, not stored by
            # TMDB — recompute it whenever genres are refreshed below, or
            # the two columns drift permanently out of sync (this branch is
            # the only thing that mutates genres on an existing row).
            is_documentary = 99 in [g.get("id") for g in detail.get("genres", [])]

            # Read-only here: fed to compute_popularity_score but NEVER
            # written back. media.release_date is the earliest *worldwide*
            # date the pipeline derives from /movie/{id}/release_dates (see
            # main.py Step 9's get_movie_release_dates call); the base
            # detail payload's release_date is a different, less-precise
            # value, so writing it through would silently revert that.
            # main.py's Step 10 omits it for exactly this reason.
            release_date = detail.get("release_date") or detail.get("first_air_date")

            # rt_score isn't in the TMDB payload — everything else fed to
            # compute_popularity_score comes fresh from *detail*.
            row = db.client.table("media").select("rt_score").eq("id", media_id).maybe_single().execute().data or {}
            rt_score = row.get("rt_score")

            popularity_score = compute_popularity_score(
                popularity or 0.0,
                tmdb_score,
                rt_score,
                vote_count,
                release_date=release_date,
            )

            # Sparse payload. Numeric fields are gated on `is not None`;
            # overview/genres/status are gated on truthiness too, since
            # TMDB returning "" or [] for those means "nothing to say", not
            # "this title genuinely has no overview" — writing the empty
            # value through would erase good existing data.
            #
            # release_date is deliberately NOT written here — see above.
            media_update: dict = {"tmdb_score": tmdb_score}
            if has_providers:
                media_update["is_streamable_now"] = is_streamable
            # popularity and popularity_score move together or not at all —
            # writing a score derived from a fallback 0.0 while leaving the
            # popularity column at its old value would make the two columns
            # disagree about the same title.
            if popularity is not None:
                media_update["popularity"] = popularity
                media_update["popularity_score"] = popularity_score
            if vote_count is not None:
                media_update["vote_count"] = vote_count
            if runtime is not None:
                media_update["runtime"] = runtime
            if status:
                media_update["status"] = status
            if overview:
                media_update["overview"] = overview
            if genres:
                media_update["genres"] = genres
                media_update["is_documentary"] = is_documentary

            db.client.table("media").update(media_update).eq("id", media_id).execute()
            # A genuine provider re-check just happened, so stamp the
            # streaming cursor — this is what stops main.py's Step 10 from
            # re-fetching the same title again within its window. Safe now
            # that RT retry runs off its own rt_last_checked cursor; while
            # the two shared one column, stamping here evicted titles from
            # the RT-retry queue that this script has no OMDb client to
            # serve. Only reached on the success path: a 404 continues above
            # (already soft-flagged via set_tmdb_missing) and an error falls
            # to the except below, so neither gets stamped.
            db.update_streaming_last_checked(media_id)
            refreshed += 1
        except Exception as exc:
            print(f"[CHANGES]   {media_type}/{tmdb_id}: refresh failed — {exc}")
            refresh_errors += 1

    print(
        f"[CHANGES] Refresh done. {refreshed} refreshed, {refresh_missing} flagged missing, "
        f"{type_mismatches} skipped (media_type mismatch), {refresh_errors} errors."
    )

    try:
        db.set_bot_state("last_synced_at", now.isoformat())
    except Exception as exc:
        print(f"[CHANGES] WARNING: failed to persist last_synced_at — next run will re-widen the window: {exc}")

    print(
        f"\n[CHANGES] SUMMARY checked={checked} skipped_existing={skipped_existing} "
        f"new_inserted={new_inserted} errors={errors} "
        f"refreshed={refreshed} refresh_missing={refresh_missing} refresh_errors={refresh_errors} "
        f"refresh_type_mismatches={type_mismatches} refresh_cap_dropped={cap_dropped}"
    )


if __name__ == "__main__":
    main()
