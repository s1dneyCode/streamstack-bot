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
            self.client.table("media").upsert(media_dict, on_conflict="tmdb_id").execute()

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

    # ------------------------------------------------------------------
    # Read operations
    # ------------------------------------------------------------------

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
