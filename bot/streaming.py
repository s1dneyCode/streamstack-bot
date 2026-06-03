"""
Streaming availability client — Streaming Availability API by Movie of the Night.

Reference: https://docs.movieofthenight.com/
Endpoint: GET /v4/shows/{type}/{tmdb_id}?country=us
"""

import time
import requests

BASE_URL = "https://api.movieofthenight.com/v4"

# Maps Streaming Availability API service slugs to the canonical names stored
# in the database.  Services not listed here are silently ignored.
PROVIDER_MAP: dict[str, str] = {
    "netflix": "Netflix",
    "prime": "Prime",
    "max": "Max",
    "disney": "Disney+",
    "apple": "Apple TV",
    "hulu": "Hulu",
}


class StreamingClient:
    """Fetches subscription streaming availability from the Movie of the Night API."""

    def __init__(self, api_key: str) -> None:
        self.api_key = api_key
        self.headers = {"X-API-Key": api_key}

    def test_single_title(self) -> None:
        # Test with Breaking Bad (TMDB id: 1396, type: tv)
        url = f"{BASE_URL}/shows/series/1396"
        params = {"country": "us"}
        response = requests.get(url, headers=self.headers, params=params)
        print(f"[DEBUG] Status: {response.status_code}")
        print(f"[DEBUG] Response: {response.text[:500]}")

    def get_streaming_providers(self, tmdb_id: int, media_type: str) -> list[str]:
        """
        Return canonical provider names that carry this title on subscription
        in the US.

        Parameters
        ----------
        tmdb_id     TMDB numeric ID for the title.
        media_type  'movie' or 'tv' — 'tv' is mapped to 'series' for the API.

        Returns
        -------
        Deduplicated list of canonical provider names (e.g. ['Netflix', 'Hulu']),
        or [] when the title is not found or has no tracked subscription services.
        """
        # The API uses 'series' for TV shows, not 'tv'
        api_type = "series" if media_type == "tv" else "movie"
        url = f"{BASE_URL}/shows/{api_type}/{tmdb_id}"
        params = {"country": "us"}

        last_exc: Exception | None = None
        for attempt in range(1, 4):
            try:
                response = requests.get(
                    url, headers=self.headers, params=params, timeout=15
                )

                # 404 means the title simply isn't in the API — not a transient error
                if response.status_code == 404:
                    print(f"[Streaming] {tmdb_id} not found in Streaming Availability API.")
                    return []

                response.raise_for_status()
                data = response.json()
                break

            except requests.RequestException as exc:
                last_exc = exc
                print(f"[Streaming] Request failed for {tmdb_id} (attempt {attempt}/3): {exc}")
                if attempt < 3:
                    time.sleep(2)
        else:
            print(f"[Streaming] All retries exhausted for {tmdb_id}: {last_exc}")
            return []

        # The API returns streamingInfo keyed by country code
        streaming_info = data.get("result", {}).get("streamingInfo", {}).get("us", [])

        providers: list[str] = []
        for entry in streaming_info:
            # Only include subscription-type access, not rent/buy/free
            if entry.get("streamingType") != "subscription":
                continue
            service = entry.get("service", "")
            mapped = PROVIDER_MAP.get(service)
            if mapped and mapped not in providers:
                providers.append(mapped)

        print(f"[Streaming] {tmdb_id}: {providers if providers else 'no tracked providers'}")
        return providers
