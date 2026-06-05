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
        release_date    date
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

from datetime import date, datetime, timedelta, timezone

from supabase import create_client, Client


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
            upsert_payload = {**media_dict, "popularity": media_dict.get("popularity", 0.0), "tmdb_score": media_dict.get("tmdb_score", 0), "imdb_id": media_dict.get("imdb_id", None)}
            self.client.table("media").upsert(upsert_payload, on_conflict="tmdb_id").execute()

            # Fetch the row id in a separate query — chaining .select() after
            # .upsert() is not supported in supabase-py 2.x
            result = (
                self.client.table("media")
                .select("id")
                .eq("tmdb_id", media_dict["tmdb_id"])
                .single()
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

    # ------------------------------------------------------------------
    # Read operations
    # ------------------------------------------------------------------

    def get_titles_to_reverify(self, today: date) -> list[dict]:
        """
        Return titles whose streaming availability should be re-checked today.

        Five categories with different re-check frequencies:
          - Coming Soon          (release_date > today)              — every 7 days or never checked
          - In Theatres recent   (in theatres, released ≤ 60d ago)   — every 7 days
          - In Theatres old      (in theatres, released > 60d ago)   — every 30 days
          - Streamable           (is_streamable_now = true)          — every 30 days
          - Old titles           (released > 365d ago)               — every 90 days
        """
        cols = "id, tmdb_id, title, media_type, release_date, is_in_theatres"
        today_str        = today.isoformat()
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

        except Exception as exc:
            print(f"[Supabase] Error fetching titles to reverify: {exc}")

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
