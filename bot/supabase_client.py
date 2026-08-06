"""
Supabase persistence layer.

All database writes go through this module.  The rest of the bot treats it as
an opaque "save this thing" interface and never constructs raw SQL.

Expected schema (create once in the Supabase dashboard / migrations):

    public.media
        id              uuid    PRIMARY KEY DEFAULT gen_random_uuid()
                                      -- corrected 2026-08 — this column is uuid, not the
                                      -- bigint this docstring previously (and wrongly)
                                      -- documented; confirmed via the Flutter app's own
                                      -- FK declarations (media_id uuid references media(id))
        tmdb_id         bigint  UNIQUE NOT NULL
        title           text
        overview        text
        poster_path     text
        media_type      text          -- 'movie' | 'tv'
        release_date    date          -- earliest worldwide release date
        us_release_date date          -- earliest US theatrical/digital date (movies only)
        rt_score        smallint      -- 0-100 or NULL
        is_in_theatres  boolean       -- always False for now
        is_streamable_now boolean
        enriched_at     timestamptz   -- NULL until enrich_new_titles.py completes a row
                                      -- with no step raising; drives its state scan
        tmdb_missing_at timestamptz   -- NULL normally; set (never cleared automatically)
                                      -- when TMDB 404s a tmdb_id — soft-flag, row is
                                      -- NEVER deleted. Excluded from reverify and the
                                      -- enrichment scan once set.
        trakt_watchers_week     int         -- NULL unless currently on a Trakt weekly-watched
                                             -- list; cleared (not the row) when it drops off
        trakt_trending_rank     int         -- NULL unless currently on Trakt's trending list;
                                             -- 1-based rank, cleared when it drops off
        trakt_anticipated_score int         -- NULL unless currently on Trakt's anticipated
                                             -- list (list_count); cleared when it drops off
        trakt_rating            numeric(4,2) -- Trakt's all-time rating; NOT cleared on decay —
                                             -- unlike the three fields above, this isn't a
                                             -- weekly-list membership signal
        trakt_vote_count        int         -- Trakt's all-time vote count; also not cleared
        trakt_synced_at         timestamptz -- last time any Trakt list write touched this row;
                                             -- sync_trakt.py Phase A decays the three list
                                             -- signals above on any row where this predates
                                             -- the current run (i.e. it fell off every list)
        trakt_related_synced_at timestamptz -- NULL until sync_trakt.py Phase B populates this
                                             -- row's media_related rows; drives that phase's
                                             -- state scan, same pattern as enriched_at
        created_at      timestamptz   DEFAULT now()
        updated_at      timestamptz   DEFAULT now()

    public.streaming_availability
        id                bigint      PRIMARY KEY GENERATED ALWAYS AS IDENTITY
        media_id          uuid        REFERENCES media(id) ON DELETE CASCADE
        provider_name     text
        region            text
        monetization_type text        -- 'flatrate' | 'rent' | 'buy'
        first_seen_at     timestamptz DEFAULT now()  -- set once, never overwritten on update
        UNIQUE (media_id, provider_name, region, monetization_type)

    public.bot_state
        key        text  PRIMARY KEY
        value      jsonb
        updated_at timestamptz DEFAULT now()
        -- Generic key/value store for cross-run bot state. Known keys:
        --   last_synced_at   (sync_changes.py)    {"last_synced_at": "<iso ts>"}
        --   discover_cursor  (coverage_sweep.py)  {"media_type", "year", "page"}
        --                                         or {"done": true} once exhausted

    public.media_related
        media_id           uuid        REFERENCES media(id)  -- the title these are related to
        related_tmdb_id    bigint      -- NOT a media(tmdb_id) FK — the related title may not
                                       -- be in our catalog at all
        related_media_type text        -- 'movie' | 'tv'
        rank               int         -- 1-based order as Trakt returned it
        source             text        -- always 'trakt' for now
        synced_at          timestamptz
        PRIMARY KEY (media_id, related_media_type, related_tmdb_id)
        -- Populated by sync_trakt.py Phase B, one full replace per title
        -- (see SupabaseClient.replace_media_related) — never a partial update.
"""

import math
import os
from datetime import date, datetime, timedelta, timezone

from supabase import create_client, Client

_BAYES_M = 500    # minimum votes threshold
_BAYES_C = 72.07  # global mean score

_DEFAULT_REVERIFY_LIMIT = 3000

_PRE_RELEASE_STATUSES = frozenset({'In Production', 'Post Production', 'Planned', 'Rumored'})

ALLOWED_PROVIDERS = frozenset({
    'Netflix', 'Netflix Standard with Ads',
    'Amazon Prime Video', 'Amazon Prime Video with Ads',
    'HBO Max', 'HBO Max Amazon Channel',
    'Disney Plus',
    'Hulu',
    'Paramount Plus Premium', 'Paramount Plus Essential',
    'Paramount+ Amazon Channel', 'Paramount+ Roku Premium Channel',
    'Paramount Plus Apple TV Channel',
    'Peacock Premium', 'Peacock Premium Plus',
    'Apple TV Plus',
    'Crunchyroll', 'Crunchyroll Amazon Channel',
    'AMC+', 'AMC+ Amazon Channel', 'AMC Plus Apple TV Channel',
    'MGM Plus', 'MGM Plus Roku Premium Channel', 'MGM+ Amazon Channel',
    'Amazon Video', 'Apple TV Store',
    'HiDive', 'Hidive Amazon Channel',
})


def compute_popularity_score(
    popularity: float,
    tmdb_score: int,
    rt_score: int | None,
    vote_count: int | None,
    release_date: str | None = None,
) -> float:
    normalized = min((popularity or 0.0) / 500 * 100, 100)

    if rt_score is None:
        # No RT score yet — redistribute its 5% weight to tmdb_score
        raw = (normalized * 0.3) + ((tmdb_score or 0) * 0.70)
    else:
        raw = (normalized * 0.3) + ((tmdb_score or 0) * 0.65) + (rt_score * 0.05)

    v = vote_count or 0
    if v == 0:
        bayesian = raw
    else:
        bayesian = (v / (v + _BAYES_M)) * raw + (_BAYES_M / (v + _BAYES_M)) * _BAYES_C

    days = None
    if release_date:
        try:
            rd = datetime.fromisoformat(release_date)
            days = (datetime.now() - rd).days
        except (ValueError, TypeError):
            pass

    if days is None:
        freshness = 0
    elif days <= 30:
        freshness = min((vote_count or 0) / 200.0, 1.0) if vote_count else 0
    else:
        freshness_scale = min((vote_count or 0) / 100.0, 1.0) if vote_count else 0
        freshness = math.exp(-days / 365) * freshness_scale

    return round(bayesian * (0.7 + 0.3 * freshness), 2)


def _dedupe_by_conflict_key(rows: list[dict], key_fields: tuple[str, ...]) -> list[dict]:
    """
    Keep only the first row per unique combination of *key_fields* values.

    Postgres rejects an upsert batch outright if it contains two rows with
    the same on_conflict key ("ON CONFLICT DO UPDATE cannot affect row a
    second time") — the whole batch is lost, not just the duplicate — so
    this must run before every upsert() call below that's keyed on
    caller-supplied data (TMDB's own lists aren't guaranteed unique).
    """
    seen: set[tuple] = set()
    deduped: list[dict] = []
    for row in rows:
        key = tuple(row[f] for f in key_fields)
        if key not in seen:
            seen.add(key)
            deduped.append(row)
    return deduped


def _dedupe_credit_rows(rows: list[dict]) -> list[dict]:
    """
    Same purpose as _dedupe_by_conflict_key, keyed on (media_id, name,
    role) — but when TMDB lists the same person twice for a title (dual
    cast roles, duplicate crew entries), keep whichever duplicate carries
    richer data instead of an arbitrary one: a defined `order` (lower
    wins) beats a missing one, then a non-empty `character` beats an
    empty one. Ties keep the first occurrence.
    """
    def _richness(row: dict) -> tuple:
        order = row.get("order")
        has_order = order is not None
        return (not has_order, order if has_order else 0, not bool(row.get("character")))

    best: dict[tuple, dict] = {}
    for row in rows:
        key = (row["media_id"], row["name"], row["role"])
        if key not in best or _richness(row) < _richness(best[key]):
            best[key] = row
    return list(best.values())


class SupabaseClient:
    """Wraps the Supabase Python SDK for the bot's read/write operations."""

    def __init__(self, url: str, key: str) -> None:
        # create_client returns a synchronous client; the bot is single-threaded
        # so we don't need the async variant
        self.client: Client = create_client(url, key)

    # ------------------------------------------------------------------
    # Write operations
    # ------------------------------------------------------------------

    def upsert_media(self, media_dict: dict) -> int | None:
        """
        Insert or update a row in public.media.

        Uses tmdb_id as the conflict key so re-running the bot on the same day
        updates scores/providers rather than duplicating rows.

        Returns the internal `id` of the upserted row (needed by
        upsert_streaming_availability) or None on error.

        media_dict must contain:
            tmdb_id, title, overview, poster_path, media_type,
            release_date, rt_score, is_in_theatres, is_streamable_now
        """
        print(f"[Supabase] Upserting media: {media_dict.get('title')} (tmdb_id={media_dict.get('tmdb_id')})")

        try:
            popularity   = media_dict.get("popularity", 0.0) or 0.0
            tmdb_score   = media_dict.get("tmdb_score", 0) or 0
            rt_score     = media_dict.get("rt_score")
            vote_count   = media_dict.get("vote_count")
            status       = media_dict.get("status")
            if status in _PRE_RELEASE_STATUSES:
                rt_score = None
            popularity_score = compute_popularity_score(
                popularity, tmdb_score, rt_score, vote_count,
                release_date=media_dict.get("release_date"),
            )

            upsert_payload = {**media_dict, "popularity": popularity, "tmdb_score": tmdb_score, "imdb_id": media_dict.get("imdb_id", None), "vote_count": vote_count, "status": status, "popularity_score": popularity_score}
            if status in _PRE_RELEASE_STATUSES:
                upsert_payload["rt_score"] = None
            if media_dict.get("media_type") == "movie":
                upsert_payload["runtime"] = media_dict.get("runtime")
            upsert_payload["title_logo_url"]    = media_dict.get("title_logo_url")
            upsert_payload["certification"]     = media_dict.get("certification")
            upsert_payload["genres"]            = media_dict.get("genres") or []
            upsert_payload["original_language"] = media_dict.get("original_language")
            upsert_payload["is_documentary"]    = media_dict.get("is_documentary")
            upsert_payload["is_limited_series"] = media_dict.get("is_limited_series")
            upsert_payload["us_release_date"]   = media_dict.get("us_release_date")
            self.client.table("media").upsert(upsert_payload, on_conflict="tmdb_id").execute()

            # Fetch the row id in a separate query — chaining .select() after
            # .upsert() is not supported in supabase-py 2.x
            result = (
                self.client.table("media")
                .select("id")
                .eq("tmdb_id", media_dict["tmdb_id"])
                .maybe_single()
                .execute()
            )

            row_id = result.data.get("id") if result.data else None
            print(f"[Supabase] media upsert OK → id={row_id}")
            return row_id

        except Exception as exc:
            print(f"[Supabase] Error upserting media '{media_dict.get('title')}': {exc}")
            return None

    def upsert_streaming_availability(self, media_id: int, providers: dict[str, dict[str, list[str]]]) -> None:
        """
        Insert streaming availability rows for a given media record that
        don't already exist.

        Each (media_id, provider_name, region, monetization_type) tuple is
        upserted independently with ignore_duplicates=True: on conflict
        (row already exists) PostgREST does nothing and leaves the existing
        row — including first_seen_at — completely untouched. Only rows
        that don't already exist get a fresh INSERT.

        first_seen_at is also deliberately left out of the row payload. The
        column has a DB-level DEFAULT now(), so a true INSERT still gets
        stamped with the current time, but since ignore_duplicates=True
        means existing rows are never updated at all, first_seen_at always
        reflects when a title first appeared on that platform.

        Note: this method only ADDS rows — it never removes providers a
        title lost. Use sync_streaming_providers() when you need to also
        clean up stale rows.

        Parameters
        ----------
        media_id   Internal `id` from the media table (FK).
        providers  Dict as returned by TmdbClient.get_watch_providers(), e.g.
                   {'US': {'flatrate': ['Netflix'], 'rent': ['Apple TV'], 'buy': [...]},
                    'MX': {'flatrate': [...], 'rent': [...], 'buy': [...]}, ...}.
        """
        rows = [
            {"media_id": media_id, "provider_name": name, "region": region, "monetization_type": kind}
            for region, kinds in (providers or {}).items()
            for kind, names in kinds.items()
            for name in names
        ]

        if not rows:
            print(f"[Supabase] No streaming providers to upsert for media_id={media_id}.")
            return

        try:
            self.client.table("streaming_availability").upsert(
                rows,
                on_conflict="media_id,provider_name,region,monetization_type",
                ignore_duplicates=True,
            ).execute()
            print(f"[Supabase] Upserted {len(rows)} streaming row(s) for media_id={media_id}.")

        except Exception as exc:
            print(f"[Supabase] Error upserting streaming availability for media_id={media_id}: {exc}")

    def sync_streaming_providers(self, media_id: int, providers: dict[str, dict[str, list[str]]]) -> None:
        """
        Reconcile streaming_availability for media_id with freshly fetched
        *providers*, without the delete-then-reinsert pattern that used to
        reset first_seen_at on every re-verification run.

        Stale rows — present in the DB but absent from *providers* (the
        title lost that platform) — are deleted individually by id. Rows
        that are still current are left completely untouched: new ones are
        inserted via upsert_streaming_availability()'s ignore_duplicates=True
        upsert, and ones that already exist are simply skipped on conflict,
        preserving their original first_seen_at.

        Parameters
        ----------
        media_id   Internal `id` from the media table (FK).
        providers  Dict as returned by TmdbClient.get_watch_providers(), e.g.
                   {'US': {'flatrate': ['Netflix'], 'rent': ['Apple TV'], 'buy': [...]},
                    'MX': {'flatrate': [...], 'rent': [...], 'buy': [...]}, ...}.
        """
        try:
            current = (
                self.client.table("streaming_availability")
                .select("id, provider_name, region, monetization_type")
                .eq("media_id", media_id)
                .execute()
                .data or []
            )
        except Exception as exc:
            print(f"[Supabase] Error fetching current streaming rows for media_id={media_id}: {exc}")
            current = []

        new_combos = {
            (name, region, kind)
            for region, kinds in (providers or {}).items()
            for kind, names in kinds.items()
            for name in names
        }

        stale_ids = [
            row["id"] for row in current
            if (row["provider_name"], row["region"], row["monetization_type"]) not in new_combos
        ]

        if stale_ids:
            try:
                self.client.table("streaming_availability").delete().in_("id", stale_ids).execute()
                print(f"[Supabase] Removed {len(stale_ids)} stale streaming row(s) for media_id={media_id}.")
            except Exception as exc:
                print(f"[Supabase] Error removing stale streaming rows for media_id={media_id}: {exc}")

        self.upsert_streaming_availability(media_id=media_id, providers=providers)

    def update_streaming_last_checked(self, media_id: int) -> None:
        """Stamp streaming_last_checked = now() on the media row."""
        now = datetime.now(timezone.utc).isoformat()
        try:
            self.client.table("media").update({"streaming_last_checked": now}).eq("id", media_id).execute()
        except Exception as exc:
            print(f"[Supabase] Error updating streaming_last_checked for media_id={media_id}: {exc}")

    def upsert_seasons(self, media_id: str, seasons: list[dict]) -> list[dict]:
        """
        Upsert season rows into media_seasons.
        Conflict key: (media_id, season_number). Deduped by that key before
        upserting (see _dedupe_by_conflict_key) — low risk in practice
        since TMDB's own seasons array is one entry per season number, but
        cheap insurance against a data anomaly poisoning the whole batch.
        Returns the upserted rows (with id) for use by upsert_episodes.
        Raises on failure — never returns [] to mean "failed silently."
        """
        if not seasons:
            return []
        rows = [
            {
                "media_id":      media_id,
                "season_number": s["season_number"],
                "name":          s.get("name", ""),
                "episode_count": s.get("episode_count"),
                "air_date":      s.get("air_date") or None,
                "poster_url":    s.get("poster_url"),
                "vote_average":  s.get("vote_average"),
            }
            for s in seasons
        ]
        rows = _dedupe_by_conflict_key(rows, ("media_id", "season_number"))
        try:
            self.client.table("media_seasons").upsert(
                rows, on_conflict="media_id,season_number"
            ).execute()
            result = (
                self.client.table("media_seasons")
                .select("id, season_number")
                .eq("media_id", media_id)
                .execute()
            )
            print(f"[Supabase] Upserted {len(rows)} season(s) for media_id={media_id}.")
            return result.data or []
        except Exception as exc:
            print(f"[Supabase] Error upserting seasons for media_id={media_id}: {exc}")
            raise

    def upsert_episodes(self, season_id: str, episodes: list[dict]) -> int:
        """
        Upsert episode rows into media_episodes.
        Conflict key: (season_id, episode_number). Deduped by that key
        before upserting (see _dedupe_by_conflict_key) — low risk in
        practice since TMDB's own episodes array is one entry per episode
        number, but cheap insurance against a data anomaly poisoning the
        whole batch.
        Returns rows upserted. Raises on failure — never returns 0 to mean
        "failed silently."
        """
        if not episodes:
            return 0
        rows = [
            {
                "season_id":      season_id,
                "episode_number": ep["episode_number"],
                "name":           ep.get("name", ""),
                "runtime":        ep.get("runtime"),
                "air_date":       ep.get("air_date") or None,
            }
            for ep in episodes
            if ep.get("episode_number") is not None
        ]
        if not rows:
            return 0
        rows = _dedupe_by_conflict_key(rows, ("season_id", "episode_number"))
        try:
            self.client.table("media_episodes").upsert(
                rows, on_conflict="season_id,episode_number"
            ).execute()
            print(f"[Supabase] Upserted {len(rows)} episode(s) for season_id={season_id}.")
            return len(rows)
        except Exception as exc:
            print(f"[Supabase] Error upserting episodes for season_id={season_id}: {exc}")
            raise

    def update_tv_runtime(self, media_id: str) -> int | None:
        """
        Compute average runtime across all episodes for a TV show and update media.runtime.
        Skips null/0 values. Returns the computed average or None if no data.
        """
        try:
            rows = (
                self.client.table("media_episodes")
                .select("runtime, media_seasons!inner(media_id)")
                .eq("media_seasons.media_id", media_id)
                .execute()
                .data or []
            )
            runtimes = [r["runtime"] for r in rows if r.get("runtime")]
            if not runtimes:
                return None
            avg = round(sum(runtimes) / len(runtimes))
            self.client.table("media").update({"runtime": avg}).eq("id", media_id).execute()
            return avg
        except Exception as exc:
            print(f"[Supabase] Error updating TV runtime for media_id={media_id}: {exc}")
            return None

    def upsert_credits(
        self,
        media_id: str,
        directors: list[dict],
        writers: list[dict],
        cast: list[dict],
        created_by: list[dict] | None = None,
        producers: list[dict] | None = None,
    ) -> int:
        """
        Upsert credit rows into media_credits.
        Roles: "director", "writer", "cast", "created_by", "producer".
        Conflict key: (media_id, name, role). TMDB sometimes lists the same
        person twice (dual cast roles, duplicate crew entries) — writers
        and producers are already deduped upstream in TmdbClient.get_credits,
        but directors/cast/created_by aren't, so this is deduped by conflict
        key here too (see _dedupe_credit_rows), keeping the richer duplicate.
        This is the confirmed root cause of ~40 titles losing ALL credits at
        once: Postgres rejects the whole upsert batch if it contains two
        rows with the same conflict key, not just the duplicate row.
        Returns total rows upserted. Raises on failure — never returns 0 to
        mean "failed silently."
        """
        rows: list[dict] = []

        for p in directors:
            rows.append({"media_id": media_id, "name": p["name"], "role": "director", "order": None, "character": None})

        for i, p in enumerate(writers):
            rows.append({"media_id": media_id, "name": p["name"], "role": "writer", "order": p.get("order", i + 1), "character": None})

        for p in cast:
            rows.append({"media_id": media_id, "name": p["name"], "role": "cast", "order": p.get("order"), "character": p.get("character", "")})

        for p in (created_by or []):
            rows.append({"media_id": media_id, "name": p["name"], "role": "created_by", "order": None, "character": None})

        for p in (producers or []):
            rows.append({"media_id": media_id, "name": p["name"], "role": "producer", "order": None, "character": None})

        if not rows:
            return 0

        rows = _dedupe_credit_rows(rows)

        try:
            self.client.table("media_credits").upsert(
                rows,
                on_conflict="media_id,name,role",
            ).execute()
            print(f"[Supabase] Upserted {len(rows)} credit(s) for media_id={media_id}.")
            return len(rows)
        except Exception as exc:
            print(f"[Supabase] Error upserting credits for media_id={media_id}: {exc}")
            raise

    def upsert_trailers(self, media_id: int, trailers: list[dict]) -> int:
        """
        Upsert trailer rows into media_trailers for the given media row.
        Conflict key: (media_id, youtube_key). Deduped by that key before
        upserting (see _dedupe_by_conflict_key) — TMDB's /videos endpoint
        can plausibly list the same underlying video twice (duplicate
        entries with the same key), so this isn't just defensive.
        Returns the number of rows upserted. Raises on failure — never
        returns 0 to mean "failed silently."
        """
        if not trailers:
            return 0

        rows = [
            {
                "media_id":     media_id,
                "youtube_key":  v["key"],
                "name":         v.get("name", ""),
                "type":         v.get("type", ""),
                "published_at": v.get("published_at"),
            }
            for v in trailers
            if v.get("key")
        ]

        if not rows:
            return 0

        rows = _dedupe_by_conflict_key(rows, ("media_id", "youtube_key"))

        try:
            self.client.table("media_trailers").upsert(
                rows,
                on_conflict="media_id,youtube_key",
            ).execute()
            print(f"[Supabase] Upserted {len(rows)} trailer(s) for media_id={media_id}.")
            return len(rows)
        except Exception as exc:
            print(f"[Supabase] Error upserting trailers for media_id={media_id}: {exc}")
            raise

    def upload_image(self, bucket: str, path: str, image_bytes: bytes, content_type: str = "image/jpeg") -> str | None:
        """Upload image_bytes to Supabase Storage and return the public URL, or None on failure."""
        try:
            self.client.storage.from_(bucket).upload(
                path=path,
                file=image_bytes,
                file_options={"content-type": content_type, "upsert": "true"},
            )
            return self.get_public_url(bucket, path)
        except Exception as exc:
            print(f"[Supabase] Error uploading image to {bucket}/{path}: {exc}")
            return None

    def get_public_url(self, bucket: str, path: str) -> str:
        """Return the public URL for a file stored in Supabase Storage."""
        return self.client.storage.from_(bucket).get_public_url(path)

    def get_bot_state(self, key: str) -> dict | list | str | int | float | bool | None:
        """
        Return the jsonb value stored under *key* in bot_state, or None if
        the key genuinely has no stored value yet.

        Raises on a read failure (network, auth, etc.) instead of also
        returning None for that case — a caller that can't tell "no prior
        state" apart from "the read failed" risks silently resetting state
        it should have left untouched (e.g. a cursor) on a transient blip.
        Callers that want the old graceful-degrade-to-default behavior
        should catch this explicitly.

        Deliberately uses a plain .select() rather than .maybe_single() —
        in this supabase-py version, .execute() on a maybe_single() query
        returns None (not a response object) for zero matching rows, which
        turned the perfectly normal "key not set yet" case into an
        AttributeError on row.data that looked like a genuine failure to
        every caller. A plain .select() always returns a response object
        with a .data list, empty or not, so "zero rows" and "the request
        itself failed" stay distinguishable.
        """
        response = (
            self.client.table("bot_state")
            .select("value")
            .eq("key", key)
            .execute()
        )
        rows = response.data or []
        return rows[0]["value"] if rows else None

    def set_bot_state(self, key: str, value) -> None:
        """Upsert *value* (any JSON-serializable object) under *key* in bot_state."""
        try:
            self.client.table("bot_state").upsert(
                {"key": key, "value": value, "updated_at": datetime.now(timezone.utc).isoformat()},
                on_conflict="key",
            ).execute()
        except Exception as exc:
            print(f"[Supabase] Error writing bot_state[{key}]: {exc}")
            raise

    def get_media_ids_by_tmdb_ids(self, tmdb_ids: list[int]) -> dict[int, int]:
        """
        Return a {tmdb_id: internal id} map for whichever of *tmdb_ids*
        already exist in public.media. Used by sync_changes.py to tell
        which changed ids are new inserts vs. existing rows in one batch
        instead of a query per id.

        Chunked at 200 ids per request to stay well under PostgREST's URL
        length limit for `.in_()` filters.
        """
        mapping: dict[int, int] = {}
        chunk_size = 200
        for i in range(0, len(tmdb_ids), chunk_size):
            chunk = tmdb_ids[i:i + chunk_size]
            try:
                response = (
                    self.client.table("media")
                    .select("id, tmdb_id")
                    .in_("tmdb_id", chunk)
                    .execute()
                )
            except Exception as exc:
                print(f"[Supabase] Error looking up media ids for a chunk of tmdb_ids: {exc}")
                continue
            for row in response.data or []:
                mapping[row["tmdb_id"]] = row["id"]
        return mapping

    def set_tmdb_missing(self, media_id: int) -> None:
        """
        Stamp tmdb_missing_at = now() on the media row — soft-flag a title
        TMDB 404s on. Never deletes the row, and tmdb_missing_at is never
        cleared automatically once set.
        """
        now = datetime.now(timezone.utc).isoformat()
        try:
            self.client.table("media").update({"tmdb_missing_at": now}).eq("id", media_id).execute()
        except Exception as exc:
            print(f"[Supabase] Error setting tmdb_missing_at for media_id={media_id}: {exc}")

    # ------------------------------------------------------------------
    # Trakt integration (shadow mode) — sync_trakt.py
    # ------------------------------------------------------------------

    def get_catalog_tmdb_index(self, tmdb_ids: list[int]) -> dict[int, dict]:
        """
        Return {tmdb_id: {"id": <uuid>, "media_type": <str>}} for whichever
        of *tmdb_ids* already exist in public.media. Used by sync_trakt.py
        to know which Trakt list items we actually have (and log the
        misses) before writing any trakt_* column, and to guard against
        writing a movie's Trakt signals onto a same-numbered tv row or vice
        versa (tmdb_id is unique per media_type at TMDB, not globally, but
        this table's tmdb_id column is a single UNIQUE constraint).

        Chunked at 200 ids per request, same as get_media_ids_by_tmdb_ids.
        """
        index: dict[int, dict] = {}
        chunk_size = 200
        for i in range(0, len(tmdb_ids), chunk_size):
            chunk = tmdb_ids[i:i + chunk_size]
            try:
                response = (
                    self.client.table("media")
                    .select("id, tmdb_id, media_type")
                    .in_("tmdb_id", chunk)
                    .execute()
                )
            except Exception as exc:
                print(f"[Supabase] Error looking up catalog index for a chunk of tmdb_ids: {exc}")
                continue
            for row in response.data or []:
                index[row["tmdb_id"]] = {"id": row["id"], "media_type": row["media_type"]}
        return index

    def update_trakt_list_signals(self, tmdb_id: int, fields: dict, synced_at_iso: str) -> None:
        """
        Narrow update of Trakt list-derived signal columns on an existing
        media row, keyed by tmdb_id — never touches any other column
        (unlike upsert_media, which would reset every field not present in
        its payload).

        *fields* may contain any subset of: trakt_watchers_week,
        trakt_trending_rank, trakt_anticipated_score, trakt_rating,
        trakt_vote_count. Always stamps trakt_synced_at = synced_at_iso so
        decay_trakt_heat() can later tell this row was refreshed this run.
        """
        payload = {**fields, "trakt_synced_at": synced_at_iso}
        try:
            self.client.table("media").update(payload).eq("tmdb_id", tmdb_id).execute()
        except Exception as exc:
            print(f"[Supabase] Error updating trakt list signals for tmdb_id={tmdb_id}: {exc}")

    def decay_trakt_heat(self, run_start_iso: str) -> None:
        """
        Null out trakt_watchers_week, trakt_trending_rank, and
        trakt_anticipated_score on any row whose trakt_synced_at predates
        *run_start_iso* — i.e. it didn't appear on any Trakt list this run,
        so its old heat numbers are stale. A row with trakt_synced_at IS
        NULL (never synced) is correctly left alone: NULL < anything is
        NULL, not true, in Postgres, so .lt() never matches it.

        trakt_rating/trakt_vote_count (Trakt's all-time rating, not a
        weekly list-membership signal) are deliberately left untouched —
        falling off the weekly watched/trending/anticipated lists doesn't
        mean the rating became stale.
        """
        try:
            self.client.table("media").update({
                "trakt_watchers_week": None,
                "trakt_trending_rank": None,
                "trakt_anticipated_score": None,
            }).lt("trakt_synced_at", run_start_iso).execute()
        except Exception as exc:
            print(f"[Supabase] Error decaying stale trakt heat: {exc}")

    def get_titles_for_related_drain(self, limit: int) -> list[dict]:
        """
        Return up to *limit* media rows (id, tmdb_id, media_type) where
        trakt_related_synced_at IS NULL, ordered by popularity desc — the
        state-driven drain queue for sync_trakt.py's related-titles phase.
        Mirrors enrich_new_titles.py's enriched_at IS NULL scan, including
        the tmdb_missing_at exclusion so a soft-flagged-dead title isn't
        drained forever.
        """
        return (
            self.client.table("media")
            .select("id, tmdb_id, media_type")
            .is_("trakt_related_synced_at", "null")
            .is_("tmdb_missing_at", "null")
            .order("popularity", desc=True)
            .limit(limit)
            .execute()
            .data or []
        )

    def replace_media_related(self, media_id_uuid: str, rows: list[dict], synced_at_iso: str) -> None:
        """
        Replace all media_related rows for *media_id_uuid* with *rows* — a
        delete-then-insert so titles that drop out of Trakt's related list
        don't linger. Deletes even when *rows* is empty, so a title with
        zero current related results still correctly clears out stale rows
        from a prior sync.

        *rows* items must contain: related_tmdb_id, related_media_type,
        rank; source='trakt' and synced_at=synced_at_iso are added here.

        Raises on failure (unlike most narrow updates in this module) —
        the caller (sync_trakt.py) must not stamp trakt_related_synced_at
        for a title whose replace didn't actually succeed, or it would
        never be retried.
        """
        try:
            self.client.table("media_related").delete().eq("media_id", media_id_uuid).execute()
            if rows:
                payload = [
                    {
                        "media_id": media_id_uuid,
                        "related_tmdb_id": r["related_tmdb_id"],
                        "related_media_type": r["related_media_type"],
                        "rank": r["rank"],
                        "source": "trakt",
                        "synced_at": synced_at_iso,
                    }
                    for r in rows
                ]
                self.client.table("media_related").insert(payload).execute()
        except Exception as exc:
            print(f"[Supabase] Error replacing media_related for media_id={media_id_uuid}: {exc}")
            raise

    def stamp_trakt_related_synced(self, media_id_uuid: str, synced_at_iso: str) -> None:
        """Set trakt_related_synced_at on the media row by internal id."""
        try:
            self.client.table("media").update(
                {"trakt_related_synced_at": synced_at_iso}
            ).eq("id", media_id_uuid).execute()
        except Exception as exc:
            print(f"[Supabase] Error stamping trakt_related_synced_at for media_id={media_id_uuid}: {exc}")

    # ------------------------------------------------------------------
    # Read operations
    # ------------------------------------------------------------------

    def get_titles_to_reverify(self, today: date, limit: int | None = None) -> list[dict]:
        """
        Return titles whose streaming availability should be re-checked today.

        Movie / general categories:
          - Coming Soon          (release_date > today)              — every 7 days or never checked
          - In Theatres recent   (in theatres, released ≤ 60d ago)   — every 7 days
          - In Theatres old      (in theatres, released > 60d ago)   — every 7 days
          - Streamable           (is_streamable_now = true)          — every 7 days
          - Recent (90-365d ago) critical streaming window           — every 7 days or never checked
          - Old titles           (released > 365d ago)               — every 90 days or never checked

        TV-specific categories (no providers yet):
          - TV released ≤ 30 days ago, not streamable               — every 3 days
          - TV released 30-90 days ago, not streamable              — every 7 days
          - TV released > 90 days ago, not streamable               — every 7 days

        Every bucket also excludes tmdb_missing_at IS NOT NULL — a title
        TMDB no longer has (soft-flagged by sync_changes.py or the
        enrichment scan, never deleted) has no availability to re-verify.

        Bounded to *limit* titles total (defaults to the REVERIFY_LIMIT env
        var if set, else _DEFAULT_REVERIFY_LIMIT). Every bucket query is
        ordered by streaming_last_checked ascending (nulls/never-checked
        first) and capped at *limit* server-side, so a bucket that exceeds
        Supabase's implicit page size can't silently drop its stalest rows
        in favor of arbitrary ones. After merging, the combined set is
        re-sorted the same way and truncated to *limit* — the titles that
        miss the cut are simply the freshest-checked ones, which wait one
        more day; nothing is lost, and this keeps nightly runtime from
        growing unbounded as the catalog grows.
        """
        if limit is None:
            limit = int(os.environ.get("REVERIFY_LIMIT", _DEFAULT_REVERIFY_LIMIT))

        cols = "id, tmdb_id, title, media_type, release_date, is_in_theatres, status, streaming_last_checked"
        today_str        = today.isoformat()
        three_days_ago   = (today - timedelta(days=3)).isoformat()
        seven_days_ago   = (today - timedelta(days=7)).isoformat()
        thirty_days_ago  = (today - timedelta(days=30)).isoformat()
        sixty_days_ago   = (today - timedelta(days=60)).isoformat()
        ninety_days_ago  = (today - timedelta(days=90)).isoformat()
        one_year_ago     = (today - timedelta(days=365)).isoformat()

        results: list[dict] = []
        seen: set[int] = set()

        def _add(rows: list[dict]) -> None:
            for row in rows:
                if row["id"] not in seen:
                    seen.add(row["id"])
                    results.append(row)

        try:
            # Coming Soon — null or stale after 7 days
            _add((self.client.table("media").select(cols)
                .gt("release_date", today_str)
                .or_(f"streaming_last_checked.is.null,streaming_last_checked.lt.{seven_days_ago}")
                .is_("tmdb_missing_at", "null")
                .order("streaming_last_checked", desc=False, nullsfirst=True)
                .limit(limit)
                .execute()).data or [])

            # In Theatres, recent release — stale after 7 days
            _add((self.client.table("media").select(cols)
                .eq("is_in_theatres", True)
                .lte("release_date", today_str)
                .gte("release_date", sixty_days_ago)
                .lt("streaming_last_checked", seven_days_ago)
                .is_("tmdb_missing_at", "null")
                .order("streaming_last_checked", desc=False, nullsfirst=True)
                .limit(limit)
                .execute()).data or [])

            # In Theatres, older release — stale after 7 days
            _add((self.client.table("media").select(cols)
                .eq("is_in_theatres", True)
                .lt("release_date", sixty_days_ago)
                .lt("streaming_last_checked", seven_days_ago)
                .is_("tmdb_missing_at", "null")
                .order("streaming_last_checked", desc=False, nullsfirst=True)
                .limit(limit)
                .execute()).data or [])

            # Streamable titles — stale after 7 days
            _add((self.client.table("media").select(cols)
                .eq("is_streamable_now", True)
                .lt("streaming_last_checked", seven_days_ago)
                .is_("tmdb_missing_at", "null")
                .order("streaming_last_checked", desc=False, nullsfirst=True)
                .limit(limit)
                .execute()).data or [])

            # Recent titles (90-365d ago) — critical streaming window, stale after 7 days
            _add((self.client.table("media").select(cols)
                .lt("release_date", ninety_days_ago)
                .gte("release_date", one_year_ago)
                .or_(f"streaming_last_checked.is.null,streaming_last_checked.lt.{seven_days_ago}")
                .is_("tmdb_missing_at", "null")
                .order("streaming_last_checked", desc=False, nullsfirst=True)
                .limit(limit)
                .execute()).data or [])

            # Old titles (> 365d ago) — stale after 90 days
            _add((self.client.table("media").select(cols)
                .lt("release_date", one_year_ago)
                .or_(f"streaming_last_checked.is.null,streaming_last_checked.lt.{ninety_days_ago}")
                .is_("tmdb_missing_at", "null")
                .order("streaming_last_checked", desc=False, nullsfirst=True)
                .limit(limit)
                .execute()).data or [])

            # TV: released in last 30 days, not yet streamable — stale after 3 days
            _add((self.client.table("media").select(cols)
                .eq("media_type", "tv")
                .eq("is_streamable_now", False)
                .lte("release_date", today_str)
                .gte("release_date", thirty_days_ago)
                .or_(f"streaming_last_checked.is.null,streaming_last_checked.lt.{three_days_ago}")
                .is_("tmdb_missing_at", "null")
                .order("streaming_last_checked", desc=False, nullsfirst=True)
                .limit(limit)
                .execute()).data or [])

            # TV: released 30-90 days ago, not yet streamable — stale after 7 days
            _add((self.client.table("media").select(cols)
                .eq("media_type", "tv")
                .eq("is_streamable_now", False)
                .lt("release_date", thirty_days_ago)
                .gte("release_date", ninety_days_ago)
                .or_(f"streaming_last_checked.is.null,streaming_last_checked.lt.{seven_days_ago}")
                .is_("tmdb_missing_at", "null")
                .order("streaming_last_checked", desc=False, nullsfirst=True)
                .limit(limit)
                .execute()).data or [])

            # TV: older than 90 days, not yet streamable — stale after 7 days
            _add((self.client.table("media").select(cols)
                .eq("media_type", "tv")
                .eq("is_streamable_now", False)
                .lt("release_date", ninety_days_ago)
                .or_(f"streaming_last_checked.is.null,streaming_last_checked.lt.{seven_days_ago}")
                .is_("tmdb_missing_at", "null")
                .order("streaming_last_checked", desc=False, nullsfirst=True)
                .limit(limit)
                .execute()).data or [])

        except Exception as exc:
            print(f"[Supabase] Error fetching titles to reverify: {exc}")

        # Global re-sort across all buckets (each was only locally sorted),
        # nulls/never-checked first, then cap — the titles cut here are
        # simply the freshest-checked, which wait one more day.
        results.sort(key=lambda r: r.get("streaming_last_checked") or "")
        if len(results) > limit:
            print(f"[Supabase] {len(results)} titles stale, capping to the {limit} oldest-checked tonight.")
            results = results[:limit]

        print(f"[Supabase] {len(results)} titles queued for re-verification.")
        return results

    def get_movies_missing_rt_score(self, today: date) -> list[dict]:
        """
        Return released movies with no RT score that are due for a retry.

        Conditions:
          - media_type = 'movie'
          - rt_score = 0 OR rt_score IS NULL
          - release_date <= today (already released)
          - streaming_last_checked IS NULL OR < today - 7 days (retry cap)
        """
        today_str        = today.isoformat()
        seven_days_ago   = (today - timedelta(days=7)).isoformat()

        try:
            response = (
                self.client.table("media")
                .select("id, tmdb_id, title, release_date, imdb_id")
                .eq("media_type", "movie")
                .or_("rt_score.eq.0,rt_score.is.null")
                .lte("release_date", today_str)
                .or_(f"streaming_last_checked.is.null,streaming_last_checked.lt.{seven_days_ago}")
                .execute()
            )
            rows = response.data or []
        except Exception as exc:
            print(f"[Supabase] Error fetching movies missing RT score: {exc}")
            rows = []

        print(f"[Supabase] {len(rows)} released movies with missing RT score queued for retry.")
        return rows

    def get_movies_without_imdb_id(self) -> list[dict]:
        """Return movies where imdb_id IS NULL, for backfilling."""
        rows: list[dict] = []
        page_size = 1000
        offset = 0

        while True:
            try:
                response = (
                    self.client.table("media")
                    .select("id, tmdb_id, title")
                    .eq("media_type", "movie")
                    .is_("imdb_id", "null")
                    .range(offset, offset + page_size - 1)
                    .execute()
                )
            except Exception as exc:
                print(f"[Supabase] Error fetching movies without imdb_id: {exc}")
                break

            batch = response.data or []
            rows.extend(batch)
            if len(batch) < page_size:
                break
            offset += page_size

        print(f"[Supabase] Found {len(rows)} movies without imdb_id.")
        return rows

    def get_existing_tmdb_ids(self) -> set[int]:
        """
        Return the set of all tmdb_ids already present in public.media.

        Used in main.py to skip OMDb / provider lookups for titles we have
        already processed, preserving API quota on re-runs.

        Fetches in pages of 1 000 rows to handle large tables safely — the
        Supabase client's default page size is 1 000 and silently truncates
        larger result sets without a range header.
        """
        print("[Supabase] Loading existing tmdb_ids...")

        existing: set[int] = set()
        page_size = 1000
        offset = 0

        while True:
            try:
                response = (
                    self.client.table("media")
                    .select("tmdb_id")
                    .range(offset, offset + page_size - 1)
                    .execute()
                )
            except Exception as exc:
                print(f"[Supabase] Error fetching existing ids at offset {offset}: {exc}")
                break

            rows = response.data or []
            for row in rows:
                existing.add(row["tmdb_id"])

            # Stop when we receive fewer rows than the page size — last page
            if len(rows) < page_size:
                break

            offset += page_size

        print(f"[Supabase] Found {len(existing)} existing tmdb_ids in DB.")
        return existing
