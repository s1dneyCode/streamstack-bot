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

_PRODUCER_JOBS = {"Producer", "Executive Producer"}


def _extract_producers(crew: list[dict]) -> list[dict]:
    seen: set[str] = set()
    result: list[dict] = []
    for m in crew:
        if m.get("job") in _PRODUCER_JOBS and m.get("name") and m["name"] not in seen:
            seen.add(m["name"])
            result.append({"name": m["name"]})
    return result


class TmdbClient:
    """Thin wrapper around the TMDB REST API."""

    def __init__(self, api_key: str) -> None:
        # The API key is passed as a query parameter on every request
        self.api_key = api_key
        self._genre_cache: dict[str, dict[int, str]] = {}

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

    def get_genre_map(self, media_type: str) -> dict[int, str]:
        """Return a genre_id → genre_name mapping for movie or tv. Cached per instance."""
        if media_type in self._genre_cache:
            return self._genre_cache[media_type]

        endpoint = "/genre/movie/list" if media_type == "movie" else "/genre/tv/list"
        try:
            data = self._get(endpoint, params={"language": "en-US"})
            genre_map = {g["id"]: g["name"] for g in data.get("genres", [])}
        except Exception as exc:
            print(f"[TMDB] Failed to fetch genre map for {media_type}: {exc}")
            genre_map = {}

        self._genre_cache[media_type] = genre_map
        print(f"[TMDB] Loaded {len(genre_map)} genres for {media_type}.")
        return genre_map

    def get_now_playing_movies(self, pages: int = 3, genre_map: dict[int, str] | None = None) -> list[dict]:
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
        genre_map = genre_map or {}
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

                genre_names = [genre_map.get(gid, '') for gid in item.get('genre_ids', [])]
                genre_str = ', '.join(filter(None, genre_names))

                results.append(
                    {
                        "tmdb_id": tmdb_id,
                        "title": item.get("title", ""),
                        "overview": clean_text(item.get("overview")),
                        "poster_path": poster_url,
                        "media_type": "movie",
                        "release_date": format_date(item.get("release_date")),
                        "vote_average": item.get("vote_average", 0.0),
                        "tmdb_score": round(item.get("vote_average", 0) * 10),
                        "genre": genre_str,
                        "imdb_id": item.get("imdb_id", None),
                        "popularity": item.get("popularity", 0.0),
                        "vote_count": item.get("vote_count"),
                    }
                )

        print(f"[TMDB] Collected {len(results)} unique movies.")
        return results

    def get_on_air_tv(self, pages: int = 3, genre_map: dict[int, str] | None = None) -> list[dict]:
        """
        Fetch TV shows currently airing (within the next 7 days per TMDB).

        Same structure as get_now_playing_movies but uses the /tv/on_the_air
        endpoint.  Note that TV responses use `name` and `first_air_date`
        instead of `title` and `release_date`.

        Returns the same normalised dict shape with media_type='tv'.
        """
        genre_map = genre_map or {}
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

                genre_names = [genre_map.get(gid, '') for gid in item.get('genre_ids', [])]
                genre_str = ', '.join(filter(None, genre_names))

                results.append(
                    {
                        "tmdb_id": tmdb_id,
                        "title": item.get("name", ""),
                        "overview": clean_text(item.get("overview")),
                        "poster_path": poster_url,
                        "media_type": "tv",
                        "release_date": format_date(item.get("first_air_date")),
                        "vote_average": item.get("vote_average", 0.0),
                        "tmdb_score": round(item.get("vote_average", 0) * 10),
                        "genre": genre_str,
                        "imdb_id": item.get("imdb_id", None),
                        "popularity": item.get("popularity", 0.0),
                        "vote_count": item.get("vote_count"),
                    }
                )

        print(f"[TMDB] Collected {len(results)} unique TV shows.")
        return results

    def get_upcoming_movies(self, pages: int = 3, genre_map: dict[int, str] | None = None) -> list[dict]:
        """Fetch movies with a US release date in the near future."""
        genre_map = genre_map or {}
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

                genre_names = [genre_map.get(gid, '') for gid in item.get('genre_ids', [])]
                genre_str = ', '.join(filter(None, genre_names))

                results.append(
                    {
                        "tmdb_id": tmdb_id,
                        "title": item.get("title", ""),
                        "overview": clean_text(item.get("overview")),
                        "poster_path": poster_url,
                        "media_type": "movie",
                        "release_date": format_date(item.get("release_date")),
                        "vote_average": item.get("vote_average", 0.0),
                        "tmdb_score": round(item.get("vote_average", 0) * 10),
                        "genre": genre_str,
                        "imdb_id": item.get("imdb_id", None),
                        "popularity": item.get("popularity", 0.0),
                        "vote_count": item.get("vote_count"),
                    }
                )

        print(f"[TMDB] Collected {len(results)} unique upcoming movies.")
        return results

    def get_popular_movies(self, pages: int = 3, genre_map: dict[int, str] | None = None) -> list[dict]:
        """Fetch the most-viewed movies on TMDB right now."""
        genre_map = genre_map or {}
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

                genre_names = [genre_map.get(gid, '') for gid in item.get('genre_ids', [])]
                genre_str = ', '.join(filter(None, genre_names))

                results.append(
                    {
                        "tmdb_id": tmdb_id,
                        "title": item.get("title", ""),
                        "overview": clean_text(item.get("overview")),
                        "poster_path": poster_url,
                        "media_type": "movie",
                        "release_date": format_date(item.get("release_date")),
                        "vote_average": item.get("vote_average", 0.0),
                        "tmdb_score": round(item.get("vote_average", 0) * 10),
                        "genre": genre_str,
                        "imdb_id": item.get("imdb_id", None),
                        "popularity": item.get("popularity", 0.0),
                        "vote_count": item.get("vote_count"),
                    }
                )

        print(f"[TMDB] Collected {len(results)} unique popular movies.")
        return results

    def get_popular_tv(self, pages: int = 3, genre_map: dict[int, str] | None = None) -> list[dict]:
        """Fetch the most-viewed TV shows on TMDB right now."""
        genre_map = genre_map or {}
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

                genre_names = [genre_map.get(gid, '') for gid in item.get('genre_ids', [])]
                genre_str = ', '.join(filter(None, genre_names))

                results.append(
                    {
                        "tmdb_id": tmdb_id,
                        "title": item.get("name", ""),
                        "overview": clean_text(item.get("overview")),
                        "poster_path": poster_url,
                        "media_type": "tv",
                        "release_date": format_date(item.get("first_air_date")),
                        "vote_average": item.get("vote_average", 0.0),
                        "tmdb_score": round(item.get("vote_average", 0) * 10),
                        "genre": genre_str,
                        "imdb_id": item.get("imdb_id", None),
                        "popularity": item.get("popularity", 0.0),
                        "vote_count": item.get("vote_count"),
                    }
                )

        print(f"[TMDB] Collected {len(results)} unique popular TV shows.")
        return results

    def get_top_rated_movies(self, pages: int = 15, genre_map: dict[int, str] | None = None) -> list[dict]:
        """Fetch highest-rated movies on TMDB."""
        genre_map = genre_map or {}
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

                genre_names = [genre_map.get(gid, '') for gid in item.get('genre_ids', [])]
                genre_str = ', '.join(filter(None, genre_names))

                results.append(
                    {
                        "tmdb_id": tmdb_id,
                        "title": item.get("title", ""),
                        "overview": clean_text(item.get("overview")),
                        "poster_path": poster_url,
                        "media_type": "movie",
                        "release_date": format_date(item.get("release_date")),
                        "vote_average": item.get("vote_average", 0.0),
                        "tmdb_score": round(item.get("vote_average", 0) * 10),
                        "genre": genre_str,
                        "imdb_id": item.get("imdb_id", None),
                        "popularity": item.get("popularity", 0.0),
                        "vote_count": item.get("vote_count"),
                    }
                )

        print(f"[TMDB] Collected {len(results)} unique top rated movies.")
        return results

    def get_discover_movies_by_revenue(self, pages: int = 15, genre_map: dict[int, str] | None = None) -> list[dict]:
        """Fetch highest-grossing movies via /discover/movie sorted by revenue."""
        genre_map = genre_map or {}
        seen_ids: set[int] = set()
        results: list[dict] = []

        for page in range(1, pages + 1):
            print(f"[TMDB] Fetching page {page}/{pages} of discover movies (revenue)...")
            data = self._get(
                "/discover/movie",
                params={"sort_by": "revenue.desc", "language": "en-US", "page": page},
            )

            for item in data.get("results", []):
                tmdb_id = item.get("id")
                if tmdb_id in seen_ids:
                    continue
                seen_ids.add(tmdb_id)

                raw_poster = item.get("poster_path") or ""
                poster_url = f"{POSTER_BASE}{raw_poster}" if raw_poster else ""

                genre_names = [genre_map.get(gid, '') for gid in item.get('genre_ids', [])]
                genre_str = ', '.join(filter(None, genre_names))

                results.append(
                    {
                        "tmdb_id": tmdb_id,
                        "title": item.get("title", ""),
                        "overview": clean_text(item.get("overview")),
                        "poster_path": poster_url,
                        "media_type": "movie",
                        "release_date": format_date(item.get("release_date")),
                        "vote_average": item.get("vote_average", 0.0),
                        "tmdb_score": round(item.get("vote_average", 0) * 10),
                        "genre": genre_str,
                        "imdb_id": item.get("imdb_id", None),
                        "popularity": item.get("popularity", 0.0),
                        "vote_count": item.get("vote_count"),
                    }
                )

        print(f"[TMDB] Collected {len(results)} unique discover movies (revenue).")
        return results

    def get_discover_movies_by_vote_count(self, pages: int = 15, genre_map: dict[int, str] | None = None) -> list[dict]:
        """Fetch most-voted movies via /discover/movie sorted by vote_count."""
        genre_map = genre_map or {}
        seen_ids: set[int] = set()
        results: list[dict] = []

        for page in range(1, pages + 1):
            print(f"[TMDB] Fetching page {page}/{pages} of discover movies (vote count)...")
            data = self._get(
                "/discover/movie",
                params={"sort_by": "vote_count.desc", "language": "en-US", "page": page},
            )

            for item in data.get("results", []):
                tmdb_id = item.get("id")
                if tmdb_id in seen_ids:
                    continue
                seen_ids.add(tmdb_id)

                raw_poster = item.get("poster_path") or ""
                poster_url = f"{POSTER_BASE}{raw_poster}" if raw_poster else ""

                genre_names = [genre_map.get(gid, '') for gid in item.get('genre_ids', [])]
                genre_str = ', '.join(filter(None, genre_names))

                results.append(
                    {
                        "tmdb_id": tmdb_id,
                        "title": item.get("title", ""),
                        "overview": clean_text(item.get("overview")),
                        "poster_path": poster_url,
                        "media_type": "movie",
                        "release_date": format_date(item.get("release_date")),
                        "vote_average": item.get("vote_average", 0.0),
                        "tmdb_score": round(item.get("vote_average", 0) * 10),
                        "genre": genre_str,
                        "imdb_id": item.get("imdb_id", None),
                        "popularity": item.get("popularity", 0.0),
                        "vote_count": item.get("vote_count"),
                    }
                )

        print(f"[TMDB] Collected {len(results)} unique discover movies (vote count).")
        return results

    def get_top_rated_tv(self, pages: int = 15, genre_map: dict[int, str] | None = None) -> list[dict]:
        """Fetch highest-rated TV shows on TMDB."""
        genre_map = genre_map or {}
        seen_ids: set[int] = set()
        results: list[dict] = []

        for page in range(1, pages + 1):
            print(f"[TMDB] Fetching page {page}/{pages} of top rated TV...")
            data = self._get(
                "/tv/top_rated",
                params={"language": "en-US", "page": page},
            )

            for item in data.get("results", []):
                tmdb_id = item.get("id")
                if tmdb_id in seen_ids:
                    continue
                seen_ids.add(tmdb_id)

                raw_poster = item.get("poster_path") or ""
                poster_url = f"{POSTER_BASE}{raw_poster}" if raw_poster else ""

                genre_names = [genre_map.get(gid, '') for gid in item.get('genre_ids', [])]
                genre_str = ', '.join(filter(None, genre_names))

                results.append(
                    {
                        "tmdb_id": tmdb_id,
                        "title": item.get("name", ""),
                        "overview": clean_text(item.get("overview")),
                        "poster_path": poster_url,
                        "media_type": "tv",
                        "release_date": format_date(item.get("first_air_date")),
                        "vote_average": item.get("vote_average", 0.0),
                        "tmdb_score": round(item.get("vote_average", 0) * 10),
                        "genre": genre_str,
                        "imdb_id": item.get("imdb_id", None),
                        "popularity": item.get("popularity", 0.0),
                        "vote_count": item.get("vote_count"),
                    }
                )

        print(f"[TMDB] Collected {len(results)} unique top rated TV shows.")
        return results

    def get_discover_tv_by_vote_count(self, pages: int = 15, genre_map: dict[int, str] | None = None) -> list[dict]:
        """Fetch most-voted TV shows via /discover/tv sorted by vote_count."""
        genre_map = genre_map or {}
        seen_ids: set[int] = set()
        results: list[dict] = []

        for page in range(1, pages + 1):
            print(f"[TMDB] Fetching page {page}/{pages} of discover TV (vote count)...")
            data = self._get(
                "/discover/tv",
                params={"sort_by": "vote_count.desc", "language": "en-US", "page": page},
            )

            for item in data.get("results", []):
                tmdb_id = item.get("id")
                if tmdb_id in seen_ids:
                    continue
                seen_ids.add(tmdb_id)

                raw_poster = item.get("poster_path") or ""
                poster_url = f"{POSTER_BASE}{raw_poster}" if raw_poster else ""

                genre_names = [genre_map.get(gid, '') for gid in item.get('genre_ids', [])]
                genre_str = ', '.join(filter(None, genre_names))

                results.append(
                    {
                        "tmdb_id": tmdb_id,
                        "title": item.get("name", ""),
                        "overview": clean_text(item.get("overview")),
                        "poster_path": poster_url,
                        "media_type": "tv",
                        "release_date": format_date(item.get("first_air_date")),
                        "vote_average": item.get("vote_average", 0.0),
                        "tmdb_score": round(item.get("vote_average", 0) * 10),
                        "genre": genre_str,
                        "imdb_id": item.get("imdb_id", None),
                        "popularity": item.get("popularity", 0.0),
                        "vote_count": item.get("vote_count"),
                    }
                )

        print(f"[TMDB] Collected {len(results)} unique discover TV shows (vote count).")
        return results

    def get_discover_movies_by_genre(
        self,
        genre_id: int,
        genre_name: str,
        pages: int = 30,
        genre_map: dict[int, str] | None = None,
    ) -> list[dict]:
        """Fetch movies for a specific genre via /discover/movie sorted by popularity."""
        genre_map = genre_map or {}
        seen_ids: set[int] = set()
        results: list[dict] = []

        for page in range(1, pages + 1):
            print(f"[TMDB] Fetching page {page}/{pages} of discover movies (genre: {genre_name})...")
            data = self._get(
                "/discover/movie",
                params={
                    "sort_by": "popularity.desc",
                    "with_genres": genre_id,
                    "language": "en-US",
                    "page": page,
                },
            )

            for item in data.get("results", []):
                tmdb_id = item.get("id")
                if tmdb_id in seen_ids:
                    continue
                seen_ids.add(tmdb_id)

                raw_poster = item.get("poster_path") or ""
                poster_url = f"{POSTER_BASE}{raw_poster}" if raw_poster else ""

                genre_names = [genre_map.get(gid, '') for gid in item.get('genre_ids', [])]
                genre_str = ', '.join(filter(None, genre_names))

                results.append(
                    {
                        "tmdb_id": tmdb_id,
                        "title": item.get("title", ""),
                        "overview": clean_text(item.get("overview")),
                        "poster_path": poster_url,
                        "media_type": "movie",
                        "release_date": format_date(item.get("release_date")),
                        "vote_average": item.get("vote_average", 0.0),
                        "tmdb_score": round(item.get("vote_average", 0) * 10),
                        "genre": genre_str,
                        "imdb_id": item.get("imdb_id", None),
                        "popularity": item.get("popularity", 0.0),
                        "vote_count": item.get("vote_count"),
                    }
                )

        print(f"[TMDB] Collected {len(results)} unique discover movies (genre: {genre_name}).")
        return results

    def get_discover_tv_by_genre(
        self,
        genre_id: int,
        genre_name: str,
        pages: int = 30,
        genre_map: dict[int, str] | None = None,
    ) -> list[dict]:
        """Fetch TV shows for a specific genre via /discover/tv sorted by popularity."""
        genre_map = genre_map or {}
        seen_ids: set[int] = set()
        results: list[dict] = []

        for page in range(1, pages + 1):
            print(f"[TMDB] Fetching page {page}/{pages} of discover TV (genre: {genre_name})...")
            data = self._get(
                "/discover/tv",
                params={
                    "sort_by": "popularity.desc",
                    "with_genres": genre_id,
                    "language": "en-US",
                    "page": page,
                },
            )

            for item in data.get("results", []):
                tmdb_id = item.get("id")
                if tmdb_id in seen_ids:
                    continue
                seen_ids.add(tmdb_id)

                raw_poster = item.get("poster_path") or ""
                poster_url = f"{POSTER_BASE}{raw_poster}" if raw_poster else ""

                genre_names = [genre_map.get(gid, '') for gid in item.get('genre_ids', [])]
                genre_str = ', '.join(filter(None, genre_names))

                results.append(
                    {
                        "tmdb_id": tmdb_id,
                        "title": item.get("name", ""),
                        "overview": clean_text(item.get("overview")),
                        "poster_path": poster_url,
                        "media_type": "tv",
                        "release_date": format_date(item.get("first_air_date")),
                        "vote_average": item.get("vote_average", 0.0),
                        "tmdb_score": round(item.get("vote_average", 0) * 10),
                        "genre": genre_str,
                        "imdb_id": item.get("imdb_id", None),
                        "popularity": item.get("popularity", 0.0),
                        "vote_count": item.get("vote_count"),
                    }
                )

        print(f"[TMDB] Collected {len(results)} unique discover TV shows (genre: {genre_name}).")
        return results

    def get_discover_movies_by_year(
        self,
        year: int,
        pages: int = 30,
        genre_map: dict[int, str] | None = None,
    ) -> list[dict]:
        """Fetch movies for a specific release year via /discover/movie sorted by popularity."""
        genre_map = genre_map or {}
        seen_ids: set[int] = set()
        results: list[dict] = []

        for page in range(1, pages + 1):
            print(f"[TMDB] Fetching page {page}/{pages} of discover movies (year: {year})...")
            data = self._get(
                "/discover/movie",
                params={
                    "sort_by": "popularity.desc",
                    "primary_release_year": year,
                    "language": "en-US",
                    "page": page,
                },
            )

            for item in data.get("results", []):
                tmdb_id = item.get("id")
                if tmdb_id in seen_ids:
                    continue
                seen_ids.add(tmdb_id)

                raw_poster = item.get("poster_path") or ""
                poster_url = f"{POSTER_BASE}{raw_poster}" if raw_poster else ""

                genre_names = [genre_map.get(gid, '') for gid in item.get('genre_ids', [])]
                genre_str = ', '.join(filter(None, genre_names))

                results.append(
                    {
                        "tmdb_id": tmdb_id,
                        "title": item.get("title", ""),
                        "overview": clean_text(item.get("overview")),
                        "poster_path": poster_url,
                        "media_type": "movie",
                        "release_date": format_date(item.get("release_date")),
                        "vote_average": item.get("vote_average", 0.0),
                        "tmdb_score": round(item.get("vote_average", 0) * 10),
                        "genre": genre_str,
                        "imdb_id": item.get("imdb_id", None),
                        "popularity": item.get("popularity", 0.0),
                        "vote_count": item.get("vote_count"),
                    }
                )

        print(f"[TMDB] Collected {len(results)} unique discover movies (year: {year}).")
        return results

    def get_discover_tv_by_year(
        self,
        year: int,
        pages: int = 30,
        genre_map: dict[int, str] | None = None,
    ) -> list[dict]:
        """Fetch TV shows for a specific first-air year via /discover/tv sorted by popularity."""
        genre_map = genre_map or {}
        seen_ids: set[int] = set()
        results: list[dict] = []

        for page in range(1, pages + 1):
            print(f"[TMDB] Fetching page {page}/{pages} of discover TV (year: {year})...")
            data = self._get(
                "/discover/tv",
                params={
                    "sort_by": "popularity.desc",
                    "first_air_date_year": year,
                    "language": "en-US",
                    "page": page,
                },
            )

            for item in data.get("results", []):
                tmdb_id = item.get("id")
                if tmdb_id in seen_ids:
                    continue
                seen_ids.add(tmdb_id)

                raw_poster = item.get("poster_path") or ""
                poster_url = f"{POSTER_BASE}{raw_poster}" if raw_poster else ""

                genre_names = [genre_map.get(gid, '') for gid in item.get('genre_ids', [])]
                genre_str = ', '.join(filter(None, genre_names))

                results.append(
                    {
                        "tmdb_id": tmdb_id,
                        "title": item.get("name", ""),
                        "overview": clean_text(item.get("overview")),
                        "poster_path": poster_url,
                        "media_type": "tv",
                        "release_date": format_date(item.get("first_air_date")),
                        "vote_average": item.get("vote_average", 0.0),
                        "tmdb_score": round(item.get("vote_average", 0) * 10),
                        "genre": genre_str,
                        "imdb_id": item.get("imdb_id", None),
                        "popularity": item.get("popularity", 0.0),
                        "vote_count": item.get("vote_count"),
                    }
                )

        print(f"[TMDB] Collected {len(results)} unique discover TV shows (year: {year}).")
        return results

    def get_credits(self, tmdb_id: int, media_type: str) -> dict:
        """
        Return credits for a title, split by media_type.

        Movies  → /movie/{id}/credits
                  directors (job==Director), writers (Writer/Screenplay/Story), cast (top-20)

        TV      → /tv/{id} (created_by) + /tv/{id}/aggregate_credits (cast top-20)
                  created_by from show detail, no writers, cast (top-20)
        """
        if media_type == "movie":
            try:
                data = self._get(f"/movie/{tmdb_id}/credits")
            except Exception as exc:
                print(f"[TMDB] Could not fetch credits for movie/{tmdb_id}: {exc}")
                return {"directors": [], "writers": [], "cast": [], "created_by": [], "producers": []}

            crew = data.get("crew", [])

            directors = [
                {"name": m["name"]}
                for m in crew
                if m.get("job") == "Director"
            ]

            writer_priority = {"Writer": 1, "Screenplay": 2, "Story": 2}
            writers_raw = [
                {"name": m["name"], "order": writer_priority[m["job"]]}
                for m in crew
                if m.get("job") in writer_priority
            ]
            seen_writer: set[str] = set()
            writers: list[dict] = []
            for w in sorted(writers_raw, key=lambda x: x["order"]):
                if w["name"] not in seen_writer:
                    seen_writer.add(w["name"])
                    writers.append(w)

            cast_raw = data.get("cast", [])
            cast = [
                {"name": m.get("name", ""), "character": m.get("character", "")}
                for m in sorted(cast_raw, key=lambda x: x.get("order", 9999))
            ][:20]

            producers = _extract_producers(crew)
            return {"directors": directors, "writers": writers, "cast": cast, "created_by": [], "producers": producers}

        else:
            # TV: fetch show detail for created_by, then aggregate_credits for cast + crew
            created_by: list[dict] = []
            try:
                detail = self._get(f"/tv/{tmdb_id}")
                created_by = [
                    {"name": p["name"]}
                    for p in detail.get("created_by", [])
                    if p.get("name")
                ]
            except Exception as exc:
                print(f"[TMDB] Could not fetch TV detail for {tmdb_id}: {exc}")

            cast: list[dict] = []
            producers: list[dict] = []
            try:
                agg = self._get(f"/tv/{tmdb_id}/aggregate_credits")
                cast_raw = agg.get("cast", [])
                cast = [
                    {"name": m.get("name", ""), "character": m.get("character", "")}
                    for m in sorted(cast_raw, key=lambda x: x.get("order", 9999))
                ][:20]
                producers = _extract_producers(agg.get("crew", []))
            except Exception as exc:
                print(f"[TMDB] Could not fetch aggregate_credits for tv/{tmdb_id}: {exc}")

            return {"directors": [], "writers": [], "cast": cast, "created_by": created_by, "producers": producers}

    def get_seasons(self, tmdb_id: int) -> list[dict]:
        """Return seasons for a TV show, excluding season_number == 0 (Specials)."""
        try:
            data = self._get(f"/tv/{tmdb_id}")
        except Exception as exc:
            print(f"[TMDB] Could not fetch TV detail for {tmdb_id}: {exc}")
            return []
        return [s for s in data.get("seasons", []) if s.get("season_number", 0) != 0]

    def get_season_episodes(self, tmdb_id: int, season_number: int) -> list[dict]:
        """Return episodes for a specific season with episode_number, name, runtime, air_date."""
        try:
            data = self._get(f"/tv/{tmdb_id}/season/{season_number}")
        except Exception as exc:
            print(f"[TMDB] Could not fetch season {season_number} for tv/{tmdb_id}: {exc}")
            return []
        return [
            {
                "episode_number": ep.get("episode_number"),
                "name":           ep.get("name", ""),
                "runtime":        ep.get("runtime"),
                "air_date":       ep.get("air_date"),
            }
            for ep in data.get("episodes", [])
        ]

    def get_certification(self, tmdb_id: int, media_type: str) -> str | None:
        """
        Return the US content certification for a title.

        Movies  → /movie/{id}/release_dates: find iso_3166_1=='US', take the
                  first release_date entry with a non-empty certification.
        TV      → /tv/{id}/content_ratings: find iso_3166_1=='US', take rating.
        """
        try:
            if media_type == "movie":
                data = self._get(f"/movie/{tmdb_id}/release_dates")
                for country in data.get("results", []):
                    if country.get("iso_3166_1") == "US":
                        for entry in country.get("release_dates", []):
                            cert = entry.get("certification", "").strip()
                            if cert:
                                return cert
            else:
                data = self._get(f"/tv/{tmdb_id}/content_ratings")
                for country in data.get("results", []):
                    if country.get("iso_3166_1") == "US":
                        rating = country.get("rating", "").strip()
                        return rating if rating else None
        except Exception as exc:
            print(f"[TMDB] Could not fetch certification for {media_type}/{tmdb_id}: {exc}")
        return None

    def get_title_logo(self, tmdb_id: int, media_type: str) -> str | None:
        """
        Return a full image URL for the title's logo, preferring English.
        Falls back to the first logo in any language if no English logo exists.
        Returns None if no logos are available.
        """
        try:
            data = self._get(f"/{media_type}/{tmdb_id}/images")
        except Exception as exc:
            print(f"[TMDB] Could not fetch images for {media_type}/{tmdb_id}: {exc}")
            return None

        logos = data.get("logos", [])
        if not logos:
            return None

        en_logos = [l for l in logos if l.get("iso_639_1") == "en"]
        chosen = en_logos[0] if en_logos else logos[0]
        file_path = chosen.get("file_path", "")
        return f"{POSTER_BASE}{file_path}" if file_path else None

    def get_videos(self, tmdb_id: int, media_type: str) -> list[dict]:
        """Return official YouTube trailers and teasers for a title from TMDB."""
        try:
            data = self._get(f"/{media_type}/{tmdb_id}/videos")
        except Exception as exc:
            print(f"[TMDB] Could not fetch videos for {media_type}/{tmdb_id}: {exc}")
            return []

        return [
            v for v in data.get("results", [])
            if v.get("official")
            and v.get("site") == "YouTube"
            and v.get("type") in ("Trailer", "Teaser")
        ]

    def get_imdb_id(self, tmdb_id: int, media_type: str) -> str | None:
        """Return the IMDb ID for a given TMDB title, or None if unavailable."""
        try:
            data = self._get(f"/{media_type}/{tmdb_id}/external_ids")
            return data.get("imdb_id") or None
        except Exception as exc:
            print(f"[TMDB] Could not fetch external_ids for {media_type}/{tmdb_id}: {exc}")
            return None

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
