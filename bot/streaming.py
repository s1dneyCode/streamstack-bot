"""
Streaming availability client — WatchMode API.

Reference: https://api.watchmode.com/docs/
Two-step lookup per title:
  1. GET /search/?search_field=tmdb_id  →  resolve to a WatchMode title id
  2. GET /title/{id}/sources/           →  get subscription sources
Each call to get_streaming_providers costs 2 API credits.
"""

import time
import requests

BASE_URL = "https://api.watchmode.com/v1"


class StreamingClient:
    """Fetches subscription streaming availability from the WatchMode API."""

    # WatchMode source IDs for tracked platforms across US / BR / MX.
    # Multiple IDs can map to the same canonical name (one per region).
    TRACKED_SOURCES: dict[int, str] = {
        203: "Netflix",    # Netflix US
        26:  "Prime",      # Amazon Prime Video US
        387: "Max",        # Max US
        372: "Disney+",    # Disney Plus US
        371: "Apple TV",   # Apple TV Plus US
        23:  "Netflix",    # Netflix BR
        119: "Prime",      # Amazon Prime Video BR
        484: "Disney+",    # Disney Plus BR
        619: "Apple TV",   # Apple TV Plus BR
        725: "Max",        # Max BR
        188: "Netflix",    # Netflix MX
        631: "Prime",      # Amazon Prime Video MX
        658: "Disney+",    # Disney Plus MX
        659: "Apple TV",   # Apple TV Plus MX
        726: "Max",        # Max MX
    }

    def __init__(self, api_key: str) -> None:
        self.api_key = api_key

    def get_streaming_providers(self, tmdb_id: int, media_type: str) -> list[str]:
        """
        Return deduplicated canonical provider names for a title in US/BR/MX.

        Parameters
        ----------
        tmdb_id     TMDB numeric ID for the title.
        media_type  'movie' or 'tv'.

        Returns
        -------
        List of canonical provider name strings, e.g. ['Netflix', 'Prime'].
        Returns [] on any error or when no tracked providers carry the title.
        Costs 2 API credits per call (search + sources).
        """
        try:
            # ---------------------------------------------------------- #
            # Step 1 — Resolve TMDB id → WatchMode title id              #
            # ---------------------------------------------------------- #
            search_type = "movie" if media_type == "movie" else "tv_series"
            search_field = "tmdb_movie_id" if media_type == "movie" else "tmdb_tv_id"
            search_params = {
                "apiKey": self.api_key,
                "search_field": search_field,
                "search_value": str(tmdb_id),
                "types": search_type,
            }
            search_response = self._get(f"{BASE_URL}/search/", params=search_params)
            if search_response is None:
                return []

            if search_response.status_code != 200:
                print(f"[Streaming] Search failed for tmdb_id={tmdb_id}: {search_response.status_code}")
                return []

            title_results = search_response.json().get("title_results", [])
            if not title_results:
                print(f"[Streaming] No WatchMode match for tmdb_id={tmdb_id}")
                return []

            watchmode_id = title_results[0]["id"]

            # ---------------------------------------------------------- #
            # Step 2 — Fetch subscription sources for the WatchMode id   #
            # ---------------------------------------------------------- #
            sources_params = {
                "apiKey": self.api_key,
                "regions": "US,BR,MX",
                "types": "sub",  # subscription only, not rent/buy
            }
            sources_response = self._get(
                f"{BASE_URL}/title/{watchmode_id}/sources/", params=sources_params
            )
            if sources_response is None:
                return []

            if sources_response.status_code != 200:
                print(f"[Streaming] Sources failed for watchmode_id={watchmode_id}: {sources_response.status_code}")
                return []

            # Build deduplicated list of canonical names for tracked sources only
            providers: list[str] = []
            for source in sources_response.json():
                source_id = source.get("source_id")
                name = self.TRACKED_SOURCES.get(source_id)
                if name and name not in providers:
                    providers.append(name)

            print(f"[Streaming] tmdb_id={tmdb_id}: {providers}")
            time.sleep(2)
            return providers

        except Exception as exc:
            print(f"[Streaming] Error for tmdb_id={tmdb_id}: {exc}")
            return []

    def _get(self, url: str, params: dict) -> requests.Response | None:
        """
        GET request with 429 backoff: waits 10s and retries up to 2 times
        before giving up and returning None.
        """
        for attempt in range(1, 3):
            response = requests.get(url, params=params, timeout=10)
            if response.status_code != 429:
                return response
            print(f"[Streaming] 429 rate limit hit — waiting 10s (attempt {attempt}/2)...")
            time.sleep(10)
        print(f"[Streaming] Giving up after 2 retries for {url}")
        return None
