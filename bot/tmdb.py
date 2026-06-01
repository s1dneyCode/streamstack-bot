"""
TMDB (The Movie Database) API client.

Handles all communication with api.themoviedb.org v3.  Each public method
returns plain Python dicts / lists so the rest of the bot stays decoupled from
the raw API response shape.

Reference: https://developer.themoviedb.org/reference/intro/getting-started
"""

import time
import requests
from bot.utils import format_date, clean_text

# Base URL for all TMDB v3 endpoints
TMDB_BASE = "https://api.themoviedb.org/3"

# Prefix used to build absolute poster URLs from TMDB's relative path strings
POSTER_BASE = "https://image.tmdb.org/t/p/w500"


class TmdbClient:
    """Thin wrapper around the TMDB REST API."""

    def __init__(self, api_key: str) -> None:
        # The API key is passed as a query parameter on every request
        self.api_key = api_key

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get(self, path: str, params: dict | None = None, retries: int = 3) -> dict:
        """
        Perform a GET request against *path* (relative to TMDB_BASE), merging
        *params* with the mandatory api_key parameter.

        Retries up to *retries* times with a 2-second pause between attempts so
        that transient network errors or TMDB rate-limit responses (429) don't
        immediately crash the bot.  Raises the final exception if all attempts
        are exhausted.
        """
        url = f"{TMDB_BASE}{path}"
        merged_params = {"api_key": self.api_key, **(params or {})}

        last_exc: Exception | None = None
        for attempt in range(1, retries + 1):
            try:
                response = requests.get(url, params=merged_params, timeout=15)
                response.raise_for_status()
                return response.json()
            except requests.RequestException as exc:
                last_exc = exc
                print(
                    f"[TMDB] Request failed (attempt {attempt}/{retries}): {exc}"
                )
                if attempt < retries:
                    time.sleep(2)

        raise last_exc  # type: ignore[misc]

    # ------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------

    def get_now_playing_movies(self, pages: int = 3) -> list[dict]:
        """
        Fetch movies currently playing in US cinemas.

        Iterates over *pages* pages of the /movie/now_playing endpoint and
        deduplicates results by tmdb_id so that a title appearing on multiple
        pages is only represented once in the returned list.

        Returns a list of normalised dicts:
            tmdb_id       int
            title         str
            overview      str
            poster_path   str  (full URL or empty string)
            media_type    'movie'
            release_date  str (YYYY-MM-DD) or None
            vote_average  float
        """
        seen_ids: set[int] = set()
        results: list[dict] = []

        for page in range(1, pages + 1):
            print(f"[TMDB] Fetching page {page}/{pages} of now_playing...")
            data = self._get(
                "/movie/now_playing",
                params={"region": "US", "language": "en-US", "page": page},
            )

            for item in data.get("results", []):
                tmdb_id = item.get("id")
                if tmdb_id in seen_ids:
                    continue
                seen_ids.add(tmdb_id)

                # Build an absolute poster URL; use empty string when absent
                raw_poster = item.get("poster_path") or ""
                poster_url = f"{POSTER_BASE}{raw_poster}" if raw_poster else ""

                results.append(
                    {
                        "tmdb_id": tmdb_id,
                        "title": item.get("title", ""),
                        "overview": clean_text(item.get("overview")),
                        "poster_path": poster_url,
                        "media_type": "movie",
                        "release_date": format_date(item.get("release_date")),
                        "vote_average": item.get("vote_average", 0.0),
                    }
                )

        print(f"[TMDB] Collected {len(results)} unique movies.")
        return results

    def get_on_air_tv(self, pages: int = 3) -> list[dict]:
        """
        Fetch TV shows currently airing (within the next 7 days per TMDB).

        Same structure as get_now_playing_movies but uses the /tv/on_the_air
        endpoint.  Note that TV responses use `name` and `first_air_date`
        instead of `title` and `release_date`.

        Returns the same normalised dict shape with media_type='tv'.
        """
        seen_ids: set[int] = set()
        results: list[dict] = []

        for page in range(1, pages + 1):
            print(f"[TMDB] Fetching page {page}/{pages} of on_the_air...")
            data = self._get(
                "/tv/on_the_air",
                params={"language": "en-US", "page": page},
            )

            for item in data.get("results", []):
                tmdb_id = item.get("id")
                if tmdb_id in seen_ids:
                    continue
                seen_ids.add(tmdb_id)

                raw_poster = item.get("poster_path") or ""
                poster_url = f"{POSTER_BASE}{raw_poster}" if raw_poster else ""

                results.append(
                    {
                        "tmdb_id": tmdb_id,
                        "title": item.get("name", ""),
                        "overview": clean_text(item.get("overview")),
                        "poster_path": poster_url,
                        "media_type": "tv",
                        "release_date": format_date(item.get("first_air_date")),
                        "vote_average": item.get("vote_average", 0.0),
                    }
                )

        print(f"[TMDB] Collected {len(results)} unique TV shows.")
        return results

    def get_upcoming_movies(self, pages: int = 3) -> list[dict]:
        """Fetch movies with a US release date in the near future."""
        seen_ids: set[int] = set()
        results: list[dict] = []

        for page in range(1, pages + 1):
            print(f"[TMDB] Fetching page {page}/{pages} of upcoming movies...")
            data = self._get(
                "/movie/upcoming",
                params={"region": "US", "language": "en-US", "page": page},
            )

            for item in data.get("results", []):
                tmdb_id = item.get("id")
                if tmdb_id in seen_ids:
                    continue
                seen_ids.add(tmdb_id)

                raw_poster = item.get("poster_path") or ""
                poster_url = f"{POSTER_BASE}{raw_poster}" if raw_poster else ""

                results.append(
                    {
                        "tmdb_id": tmdb_id,
                        "title": item.get("title", ""),
                        "overview": clean_text(item.get("overview")),
                        "poster_path": poster_url,
                        "media_type": "movie",
                        "release_date": format_date(item.get("release_date")),
                        "vote_average": item.get("vote_average", 0.0),
                    }
                )

        print(f"[TMDB] Collected {len(results)} unique upcoming movies.")
        return results

    def get_popular_movies(self, pages: int = 3) -> list[dict]:
        """Fetch the most-viewed movies on TMDB right now."""
        seen_ids: set[int] = set()
        results: list[dict] = []

        for page in range(1, pages + 1):
            print(f"[TMDB] Fetching page {page}/{pages} of popular movies...")
            data = self._get(
                "/movie/popular",
                params={"region": "US", "language": "en-US", "page": page},
            )

            for item in data.get("results", []):
                tmdb_id = item.get("id")
                if tmdb_id in seen_ids:
                    continue
                seen_ids.add(tmdb_id)

                raw_poster = item.get("poster_path") or ""
                poster_url = f"{POSTER_BASE}{raw_poster}" if raw_poster else ""

                results.append(
                    {
                        "tmdb_id": tmdb_id,
                        "title": item.get("title", ""),
                        "overview": clean_text(item.get("overview")),
                        "poster_path": poster_url,
                        "media_type": "movie",
                        "release_date": format_date(item.get("release_date")),
                        "vote_average": item.get("vote_average", 0.0),
                    }
                )

        print(f"[TMDB] Collected {len(results)} unique popular movies.")
        return results

    def get_popular_tv(self, pages: int = 3) -> list[dict]:
        """Fetch the most-viewed TV shows on TMDB right now."""
        seen_ids: set[int] = set()
        results: list[dict] = []

        for page in range(1, pages + 1):
            print(f"[TMDB] Fetching page {page}/{pages} of popular TV...")
            data = self._get(
                "/tv/popular",
                params={"language": "en-US", "page": page},
            )

            for item in data.get("results", []):
                tmdb_id = item.get("id")
                if tmdb_id in seen_ids:
                    continue
                seen_ids.add(tmdb_id)

                raw_poster = item.get("poster_path") or ""
                poster_url = f"{POSTER_BASE}{raw_poster}" if raw_poster else ""

                results.append(
                    {
                        "tmdb_id": tmdb_id,
                        "title": item.get("name", ""),
                        "overview": clean_text(item.get("overview")),
                        "poster_path": poster_url,
                        "media_type": "tv",
                        "release_date": format_date(item.get("first_air_date")),
                        "vote_average": item.get("vote_average", 0.0),
                    }
                )

        print(f"[TMDB] Collected {len(results)} unique popular TV shows.")
        return results

    def get_top_rated_movies(self, pages: int = 2) -> list[dict]:
        """Fetch highest-rated movies on TMDB."""
        seen_ids: set[int] = set()
        results: list[dict] = []

        for page in range(1, pages + 1):
            print(f"[TMDB] Fetching page {page}/{pages} of top rated movies...")
            data = self._get(
                "/movie/top_rated",
                params={"region": "US", "language": "en-US", "page": page},
            )

            for item in data.get("results", []):
                tmdb_id = item.get("id")
                if tmdb_id in seen_ids:
                    continue
                seen_ids.add(tmdb_id)

                raw_poster = item.get("poster_path") or ""
                poster_url = f"{POSTER_BASE}{raw_poster}" if raw_poster else ""

                results.append(
                    {
                        "tmdb_id": tmdb_id,
                        "title": item.get("title", ""),
                        "overview": clean_text(item.get("overview")),
                        "poster_path": poster_url,
                        "media_type": "movie",
                        "release_date": format_date(item.get("release_date")),
                        "vote_average": item.get("vote_average", 0.0),
                    }
                )

        print(f"[TMDB] Collected {len(results)} unique top rated movies.")
        return results

    def get_watch_providers(self, tmdb_id: int, media_type: str) -> list[str]:
        """
        Return the names of US flatrate (subscription) streaming providers for
        a given title.

        TMDB's watch/providers endpoint groups providers by monetisation type
        (flatrate, rent, buy, etc.).  We only care about *flatrate* because
        that corresponds to "included with subscription" services like Netflix
        or Max — not one-off rentals.

        Returns an empty list when no US flatrate data is available.
        """
        print(f"[TMDB] Fetching watch providers for {media_type} {tmdb_id}...")

        try:
            data = self._get(f"/{media_type}/{tmdb_id}/watch/providers")
        except Exception as exc:
            print(f"[TMDB] Could not fetch providers for {tmdb_id}: {exc}")
            return []

        us_data = data.get("results", {}).get("US", {})
        flatrate_entries = us_data.get("flatrate", [])

        # Each entry is a dict like {"provider_name": "Netflix", ...}
        return [entry["provider_name"] for entry in flatrate_entries if "provider_name" in entry]
