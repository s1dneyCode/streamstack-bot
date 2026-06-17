"""
Supabase persistence layer.

All database writes go through this module.  The rest of the bot treats it as
an opaque "save this thing" interface and never constructs raw SQL.

Expected schema (create once in the Supabase dashboard / migrations):

    public.media
        id              bigint  PRIMARY KEY GENERATED ALWAYS AS IDENTITY
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
        created_at      timestamptz   DEFAULT now()
        updated_at      timestamptz   DEFAULT now()

    public.streaming_availability
        id           bigint  PRIMARY KEY GENERATED ALWAYS AS IDENTITY
        media_id     bigint  REFERENCES media(id) ON DELETE CASCADE
        provider_name text
        region       text
        UNIQUE (media_id, provider_name, region)
"""

import math
from datetime import date, datetime, timedelta, timezone

from supabase import create_client, Client

_BAYES_M = 500    # minimum votes threshold
_BAYES_C = 72.07  # global mean score

_PRE_RELEASE_STATUSES = frozenset({'In Production', 'Post Production', 'Planned', 'Rumored'})


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

    def upsert_streaming_availability(self, media_id: int, providers: list[str]) -> None:
        """
        Persist streaming availability rows for a given media record.

        Each (media_id, provider_name, region) triple is upserted independently
        so partial provider lists on subsequent runs don't wipe existing rows —
        only the rows that come back from the API are touched.

        Parameters
        ----------
        media_id   Internal `id` from the media table (FK).
        providers  List of canonical provider name strings (e.g. ['Netflix']).
        """
        if not providers:
            print(f"[Supabase] No streaming providers to upsert for media_id={media_id}.")
            return

        rows = [
            {"media_id": media_id, "provider_name": provider, "region": "US"}
            for provider in providers
        ]

        try:
            self.client.table("streaming_availability").upsert(
                rows,
                on_conflict="media_id,provider_name,region",
            ).execute()
            print(f"[Supabase] Upserted {len(rows)} streaming row(s) for media_id={media_id}.")

        except Exception as exc:
            print(f"[Supabase] Error upserting streaming availability for media_id={media_id}: {exc}")

    def delete_streaming_providers(self, media_id: int) -> None:
        """Delete all streaming_availability rows for a media row before re-inserting."""
        try:
            self.client.table("streaming_availability").delete().eq("media_id", media_id).execute()
            print(f"[Supabase] Deleted streaming rows for media_id={media_id}.")
        except Exception as exc:
            print(f"[Supabase] Error deleting streaming rows for media_id={media_id}: {exc}")

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
        Conflict key: (media_id, season_number).
        Returns the upserted rows (with id) for use by upsert_episodes.
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
            return []

    def upsert_episodes(self, season_id: str, episodes: list[dict]) -> int:
        """
        Upsert episode rows into media_episodes.
        Conflict key: (season_id, episode_number). Returns rows upserted.
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
        try:
            self.client.table("media_episodes").upsert(
                rows, on_conflict="season_id,episode_number"
            ).execute()
            print(f"[Supabase] Upserted {len(rows)} episode(s) for season_id={season_id}.")
            return len(rows)
        except Exception as exc:
            print(f"[Supabase] Error upserting episodes for season_id={season_id}: {exc}")
            return 0

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
        Conflict key: (media_id, name, role). Returns total rows upserted.
        """
        rows: list[dict] = []

        for p in directors:
            rows.append({"media_id": media_id, "name": p["name"], "role": "director", "order": None, "character": None})

        for i, p in enumerate(writers):
            rows.append({"media_id": media_id, "name": p["name"], "role": "writer", "order": p.get("order", i + 1), "character": None})

        for p in cast:
            rows.append({"media_id": media_id, "name": p["name"], "role": "cast", "order": None, "character": p.get("character", "")})

        for p in (created_by or []):
            rows.append({"media_id": media_id, "name": p["name"], "role": "created_by", "order": None, "character": None})

        for p in (producers or []):
            rows.append({"media_id": media_id, "name": p["name"], "role": "producer", "order": None, "character": None})

        if not rows:
            return 0

        try:
            self.client.table("media_credits").upsert(
                rows,
                on_conflict="media_id,name,role",
            ).execute()
            print(f"[Supabase] Upserted {len(rows)} credit(s) for media_id={media_id}.")
            return len(rows)
        except Exception as exc:
            print(f"[Supabase] Error upserting credits for media_id={media_id}: {exc}")
            return 0

    def upsert_trailers(self, media_id: int, trailers: list[dict]) -> int:
        """
        Upsert trailer rows into media_trailers for the given media row.
        Conflict key: (media_id, youtube_key). Returns the number of rows upserted.
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

        try:
            self.client.table("media_trailers").upsert(
                rows,
                on_conflict="media_id,youtube_key",
            ).execute()
            print(f"[Supabase] Upserted {len(rows)} trailer(s) for media_id={media_id}.")
            return len(rows)
        except Exception as exc:
            print(f"[Supabase] Error upserting trailers for media_id={media_id}: {exc}")
            return 0

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

    # ------------------------------------------------------------------
    # Read operations
    # ------------------------------------------------------------------

    def get_titles_to_reverify(self, today: date) -> list[dict]:
        """
        Return titles whose streaming availability should be re-checked today.

        Movie / general categories:
          - Coming Soon          (release_date > today)              — every 7 days or never checked
          - In Theatres recent   (in theatres, released ≤ 60d ago)   — every 7 days
          - In Theatres old      (in theatres, released > 60d ago)   — every 30 days
          - Streamable           (is_streamable_now = true)          — every 30 days
          - Old titles           (released > 365d ago)               — every 90 days

        TV-specific categories (no providers yet):
          - TV released ≤ 30 days ago, not streamable               — every 3 days
          - TV released 30-90 days ago, not streamable              — every 7 days
          - TV released > 90 days ago, not streamable               — every 30 days
        """
        cols = "id, tmdb_id, title, media_type, release_date, is_in_theatres, status"
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
                .execute()).data or [])

            # In Theatres, recent release — stale after 7 days
            _add((self.client.table("media").select(cols)
                .eq("is_in_theatres", True)
                .lte("release_date", today_str)
                .gte("release_date", sixty_days_ago)
                .lt("streaming_last_checked", seven_days_ago)
                .execute()).data or [])

            # In Theatres, older release — stale after 30 days
            _add((self.client.table("media").select(cols)
                .eq("is_in_theatres", True)
                .lt("release_date", sixty_days_ago)
                .lt("streaming_last_checked", thirty_days_ago)
                .execute()).data or [])

            # Streamable titles — stale after 30 days
            _add((self.client.table("media").select(cols)
                .eq("is_streamable_now", True)
                .lt("streaming_last_checked", thirty_days_ago)
                .execute()).data or [])

            # Old titles — stale after 90 days
            _add((self.client.table("media").select(cols)
                .lt("release_date", one_year_ago)
                .lt("streaming_last_checked", ninety_days_ago)
                .execute()).data or [])

            # TV: released in last 30 days, not yet streamable — stale after 3 days
            _add((self.client.table("media").select(cols)
                .eq("media_type", "tv")
                .eq("is_streamable_now", False)
                .lte("release_date", today_str)
                .gte("release_date", thirty_days_ago)
                .or_(f"streaming_last_checked.is.null,streaming_last_checked.lt.{three_days_ago}")
                .execute()).data or [])

            # TV: released 30-90 days ago, not yet streamable — stale after 7 days
            _add((self.client.table("media").select(cols)
                .eq("media_type", "tv")
                .eq("is_streamable_now", False)
                .lt("release_date", thirty_days_ago)
                .gte("release_date", ninety_days_ago)
                .or_(f"streaming_last_checked.is.null,streaming_last_checked.lt.{seven_days_ago}")
                .execute()).data or [])

            # TV: older than 90 days, not yet streamable — stale after 30 days
            _add((self.client.table("media").select(cols)
                .eq("media_type", "tv")
                .eq("is_streamable_now", False)
                .lt("release_date", ninety_days_ago)
                .or_(f"streaming_last_checked.is.null,streaming_last_checked.lt.{thirty_days_ago}")
                .execute()).data or [])

        except Exception as exc:
            print(f"[Supabase] Error fetching titles to reverify: {exc}")

        print(f"[Supabase] {len(results)} titles queued for re-verification.")
        return results

    def get_movies_to_update_theatres(self, today: date) -> list[dict]:
        """
        Return recently released movies with no theatrical or streaming status set yet.

        Only movies released within the last 90 days are considered in-theatres.
        Older movies with no providers are left in a neutral state (both false).
        TV shows are excluded; is_in_theatres is never set for them.
        """
        today_str          = today.isoformat()
        ninety_days_ago    = (today - timedelta(days=90)).isoformat()
        try:
            response = (
                self.client.table("media")
                .select("id, tmdb_id, title, media_type")
                .eq("media_type", "movie")
                .lte("release_date", today_str)
                .gte("release_date", ninety_days_ago)
                .eq("is_in_theatres", False)
                .eq("is_streamable_now", False)
                .execute()
            )
            rows = response.data or []
        except Exception as exc:
            print(f"[Supabase] Error fetching movies to update theatres: {exc}")
            rows = []

        print(f"[Supabase] {len(rows)} movies to mark as in theatres.")
        return rows

    def get_titles_leaving_theatres(self) -> list[dict]:
        """
        Return titles that are marked in-theatres but now have streaming availability.

        Uses an inner join so only titles with at least one streaming_availability
        row are returned.
        """
        try:
            response = (
                self.client.table("media")
                .select("id, tmdb_id, title, media_type, streaming_availability(media_id)")
                .eq("is_in_theatres", True)
                .execute()
            )
            rows = [row for row in (response.data or []) if row.get("streaming_availability")]
        except Exception as exc:
            print(f"[Supabase] Error fetching titles leaving theatres: {exc}")
            rows = []

        print(f"[Supabase] {len(rows)} titles moving from theatres to streaming.")
        return rows

    def update_theatres_status(self, media_id: int, is_in_theatres: bool, is_streamable_now: bool) -> None:
        """Update is_in_theatres and is_streamable_now for a media row."""
        try:
            self.client.table("media").update({
                "is_in_theatres": is_in_theatres,
                "is_streamable_now": is_streamable_now,
            }).eq("id", media_id).execute()
        except Exception as exc:
            print(f"[Supabase] Error updating theatre status for media_id={media_id}: {exc}")

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
