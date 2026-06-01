"""
Streaming availability module.

MVP implementation: delegates to TMDB's watch/providers endpoint rather than
integrating a dedicated streaming-availability API.  The public interface is
intentionally kept API-agnostic so we can swap the data source later without
touching main.py or supabase_client.py.

When the dedicated API is introduced:
1. Add the new client class here.
2. Replace the body of get_streaming_providers while keeping its signature.
3. Remove the tmdb_client parameter (or make it optional) as it will no longer
   be needed.
"""

from bot.tmdb import TmdbClient

# Maps raw TMDB provider names to the canonical names we store in the database.
# Any provider whose name is NOT a key here is silently ignored — we only track
# the major subscription services that users actually care about.
PROVIDER_MAP: dict[str, str] = {
    "Netflix": "Netflix",
    "Amazon Prime Video": "Prime",
    "Max": "Max",
    "Disney Plus": "Disney+",
    "Apple TV Plus": "Apple TV",
}


class StreamingClient:
    """
    Resolves streaming availability for a given title.

    Currently backed by TMDB watch providers; designed to be re-backed by a
    dedicated service (e.g. Streaming Availability API by Movie of the Night)
    in a future iteration.
    """

    def get_streaming_providers(
        self,
        tmdb_id: int,
        media_type: str,
        tmdb_client: TmdbClient,
    ) -> list[str]:
        """
        Return the list of *known* streaming providers that carry this title in
        the US.

        Fetches raw provider names from TMDB, then filters and maps them
        through PROVIDER_MAP.  Providers not present in PROVIDER_MAP are
        dropped so we don't pollute the database with obscure or short-lived
        services.

        Parameters
        ----------
        tmdb_id      TMDB numeric identifier for the title.
        media_type   'movie' or 'tv' — used to build the TMDB endpoint path.
        tmdb_client  An initialised TmdbClient instance (injected to avoid
                     creating a second HTTP client inside this module).

        Returns
        -------
        A deduplicated list of canonical provider name strings, e.g.
        ['Netflix', 'Prime'].  Empty list when not available anywhere.
        """
        raw_providers = tmdb_client.get_watch_providers(tmdb_id, media_type)

        # Translate raw names to canonical names, skipping unknowns
        canonical: list[str] = []
        for raw_name in raw_providers:
            mapped = PROVIDER_MAP.get(raw_name)
            if mapped and mapped not in canonical:
                canonical.append(mapped)

        if canonical:
            print(f"[Streaming] {tmdb_id} available on: {', '.join(canonical)}")
        else:
            print(f"[Streaming] {tmdb_id} not found on any tracked provider.")

        return canonical
