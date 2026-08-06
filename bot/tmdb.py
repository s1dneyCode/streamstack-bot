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
from bot.rate_limiter import RateLimiter

# Base URL for all TMDB v3 endpoints
TMDB_BASE = "https://api.themoviedb.org/3"

# Prefix used to build absolute poster URLs from TMDB's relative path strings
POSTER_BASE = "https://image.tmdb.org/t/p/w500"

# Conservative throttle — TMDB's documented limit is much higher (~50 req/s);
# this keeps us well clear of it without needing per-call time.sleep()s.
TMDB_RATE_LIMIT = 20.0  # requests/second

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
        self._limiter = RateLimiter(rate=TMDB_RATE_LIMIT)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get(self, path: str, params: dict | None = None, retries: int = 3) -> dict:
        """
        Perform a GET request against *path* (relative to TMDB_BASE), merging
        *params* with the mandatory api_key parameter.

        Throttled to TMDB_RATE_LIMIT req/s via a shared token-bucket limiter
        (self._limiter) before every attempt, including retries.

        Retries up to *retries* times — but only for errors that can
        genuinely succeed on a later attempt: 429 (honoring the Retry-After
        header instead of the flat 2-second pause used elsewhere), 5xx
        server errors, and network/timeout errors. Non-429 4xx client
        errors (404 Not Found, 401 Unauthorized, etc.) are raised
        immediately on the first attempt — a missing season or a bad key
        will never succeed just by asking again, so retrying it is pure
        wasted time. Raises the final exception if all attempts are
        exhausted (or immediately, for a non-retryable 4xx).
        """
        url = f"{TMDB_BASE}{path}"
        merged_params = {"api_key": self.api_key, **(params or {})}

        last_exc: Exception | None = None
        for attempt in range(1, retries + 1):
            self._limiter.acquire()
            try:
                response = requests.get(url, params=merged_params, timeout=15)
                if response.status_code == 429:
                    retry_after = response.headers.get("Retry-After")
                    wait = float(retry_after) if retry_after else 2.0
                    print(f"[TMDB] Rate limited (429, attempt {attempt}/{retries}) — waiting {wait}s...")
                    last_exc = requests.HTTPError(f"429 Too Many Requests: {url}")
                    if attempt < retries:
                        time.sleep(wait)
                    continue
                response.raise_for_status()
                return response.json()
            except requests.RequestException as exc:
                status = exc.response.status_code if isinstance(exc, requests.HTTPError) and exc.response is not None else None
                if status is not None and 400 <= status < 500:
                    # Non-retryable client error — fail fast, no point burning
                    # the remaining attempts on a request that can't succeed.
                    print(f"[TMDB] {status} for {url} — not retrying (client error).")
                    raise

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
                        "vote_count":        item.get("vote_count"),
                        "status":            item.get("status"),
                        "original_language": item.get("original_language"),
                        "is_documentary":    99 in item.get("genre_ids", []),
                        "is_limited_series": None,
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
                        "vote_count":        item.get("vote_count"),
                        "status":            item.get("status"),
                        "original_language": item.get("original_language"),
                        "is_documentary":    99 in item.get("genre_ids", []),
                        "is_limited_series": None,
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
                        "vote_count":        item.get("vote_count"),
                        "status":            item.get("status"),
                        "original_language": item.get("original_language"),
                        "is_documentary":    99 in item.get("genre_ids", []),
                        "is_limited_series": None,
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
                        "vote_count":        item.get("vote_count"),
                        "status":            item.get("status"),
                        "original_language": item.get("original_language"),
                        "is_documentary":    99 in item.get("genre_ids", []),
                        "is_limited_series": None,
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
                        "vote_count":        item.get("vote_count"),
                        "status":            item.get("status"),
                        "original_language": item.get("original_language"),
                        "is_documentary":    99 in item.get("genre_ids", []),
                        "is_limited_series": None,
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
                        "vote_count":        item.get("vote_count"),
                        "status":            item.get("status"),
                        "original_language": item.get("original_language"),
                        "is_documentary":    99 in item.get("genre_ids", []),
                        "is_limited_series": None,
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
                        "vote_count":        item.get("vote_count"),
                        "status":            item.get("status"),
                        "original_language": item.get("original_language"),
                        "is_documentary":    99 in item.get("genre_ids", []),
                        "is_limited_series": None,
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
                        "vote_count":        item.get("vote_count"),
                        "status":            item.get("status"),
                        "original_language": item.get("original_language"),
                        "is_documentary":    99 in item.get("genre_ids", []),
                        "is_limited_series": None,
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
                        "vote_count":        item.get("vote_count"),
                        "status":            item.get("status"),
                        "original_language": item.get("original_language"),
                        "is_documentary":    99 in item.get("genre_ids", []),
                        "is_limited_series": None,
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
                        "vote_count":        item.get("vote_count"),
                        "status":            item.get("status"),
                        "original_language": item.get("original_language"),
                        "is_documentary":    99 in item.get("genre_ids", []),
                        "is_limited_series": None,
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
                        "vote_count":        item.get("vote_count"),
                        "status":            item.get("status"),
                        "original_language": item.get("original_language"),
                        "is_documentary":    99 in item.get("genre_ids", []),
                        "is_limited_series": None,
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
        vote_count_gte: int | None = None,
        with_original_language: str | None = None,
    ) -> list[dict]:
        """Fetch TV shows for a specific genre via /discover/tv sorted by popularity."""
        genre_map = genre_map or {}
        seen_ids: set[int] = set()
        results: list[dict] = []

        for page in range(1, pages + 1):
            print(f"[TMDB] Fetching page {page}/{pages} of discover TV (genre: {genre_name})...")
            params = {
                "sort_by": "popularity.desc",
                "with_genres": genre_id,
                "language": "en-US",
                "page": page,
            }
            if vote_count_gte is not None:
                params["vote_count.gte"] = vote_count_gte
            if with_original_language is not None:
                params["with_original_language"] = with_original_language

            data = self._get("/discover/tv", params=params)

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
                        "vote_count":        item.get("vote_count"),
                        "status":            item.get("status"),
                        "original_language": item.get("original_language"),
                        "is_documentary":    99 in item.get("genre_ids", []),
                        "is_limited_series": None,
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
                        "vote_count":        item.get("vote_count"),
                        "status":            item.get("status"),
                        "original_language": item.get("original_language"),
                        "is_documentary":    99 in item.get("genre_ids", []),
                        "is_limited_series": None,
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
                        "vote_count":        item.get("vote_count"),
                        "status":            item.get("status"),
                        "original_language": item.get("original_language"),
                        "is_documentary":    99 in item.get("genre_ids", []),
                        "is_limited_series": None,
                    }
                )

        print(f"[TMDB] Collected {len(results)} unique discover TV shows (year: {year}).")
        return results

    def get_discover_tv_historical(
        self,
        date_gte: str,
        date_lte: str,
        with_original_language: str,
        pages: int = 50,
        genre_map: dict[int, str] | None = None,
    ) -> list[dict]:
        """
        Fetch high-quality TV shows first aired within [date_gte, date_lte]
        via /discover/tv, sorted by vote_count desc, restricted to
        *with_original_language* (a single TMDB language code, or a
        pipe-separated list of codes).

        Filters applied at the API level: vote_average >= 7.5, excludes
        Kids/Talk/Soap genres (10762/10767/10766). vote_count threshold is
        300 when with_original_language == 'ja' and 150 otherwise — anime
        accumulates TMDB votes more slowly than Western/Korean content, so
        a flat 150 floor let in too much noise for Japanese-only requests.
        Used by bulk_import.py's historical TV discover (Steps 6j/6k).
        """
        genre_map = genre_map or {}
        vote_count_gte = 300 if with_original_language == "ja" else 150
        seen_ids: set[int] = set()
        results: list[dict] = []

        for page in range(1, pages + 1):
            print(
                f"[TMDB] Fetching page {page}/{pages} of discover TV "
                f"({date_gte}..{date_lte}, lang={with_original_language})..."
            )
            data = self._get(
                "/discover/tv",
                params={
                    "sort_by": "vote_count.desc",
                    "first_air_date.gte": date_gte,
                    "first_air_date.lte": date_lte,
                    "vote_count.gte": vote_count_gte,
                    "vote_average.gte": 7.5,
                    "with_original_language": with_original_language,
                    "without_genres": "10762,10767,10766",
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
                        "vote_count":        item.get("vote_count"),
                        "status":            item.get("status"),
                        "original_language": item.get("original_language"),
                        "is_documentary":    99 in item.get("genre_ids", []),
                        "is_limited_series": None,
                    }
                )

        print(
            f"[TMDB] Collected {len(results)} unique discover TV shows "
            f"({date_gte}..{date_lte}, lang={with_original_language})."
        )
        return results

    def get_discover_by_provider(
        self,
        media_type: str,
        provider_id: int,
        pages: int = 50,
        genre_map: dict[int, str] | None = None,
    ) -> list[dict]:
        """
        Fetch movies or TV shows available via a specific watch provider
        through /discover/{media_type}, sorted by popularity desc.

        US region, flatrate (subscription) only — no rent/buy. *media_type*
        must be 'movie' or 'tv'.
        """
        genre_map = genre_map or {}
        endpoint = "/discover/movie" if media_type == "movie" else "/discover/tv"
        seen_ids: set[int] = set()
        results: list[dict] = []

        for page in range(1, pages + 1):
            print(f"[TMDB] Fetching page {page}/{pages} of discover {media_type} (provider={provider_id})...")
            data = self._get(
                endpoint,
                params={
                    "with_watch_providers": provider_id,
                    "watch_region": "US",
                    "watch_monetization_types": "flatrate",
                    "sort_by": "popularity.desc",
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

                if media_type == "movie":
                    title = item.get("title", "")
                    release_date = format_date(item.get("release_date"))
                else:
                    title = item.get("name", "")
                    release_date = format_date(item.get("first_air_date"))

                results.append(
                    {
                        "tmdb_id": tmdb_id,
                        "title": title,
                        "overview": clean_text(item.get("overview")),
                        "poster_path": poster_url,
                        "media_type": media_type,
                        "release_date": release_date,
                        "vote_average": item.get("vote_average", 0.0),
                        "tmdb_score": round(item.get("vote_average", 0) * 10),
                        "genre": genre_str,
                        "imdb_id": item.get("imdb_id", None),
                        "popularity": item.get("popularity", 0.0),
                        "vote_count":        item.get("vote_count"),
                        "status":            item.get("status"),
                        "original_language": item.get("original_language"),
                        "is_documentary":    99 in item.get("genre_ids", []),
                        "is_limited_series": None,
                    }
                )

        print(f"[TMDB] Collected {len(results)} unique discover {media_type} (provider={provider_id}).")
        return results

    def get_discover_by_language(
        self,
        media_type: str,
        language: str,
        pages: int = 30,
        genre_map: dict[int, str] | None = None,
    ) -> list[dict]:
        """
        Fetch movies or TV shows with a specific original language via
        /discover/{media_type}, sorted by vote_count desc.

        Applies vote_average.gte=7.0 and vote_count.gte=100 as API-level
        pre-filters to reduce noise. *media_type* must be 'movie' or 'tv'.
        """
        genre_map = genre_map or {}
        endpoint = "/discover/movie" if media_type == "movie" else "/discover/tv"
        seen_ids: set[int] = set()
        results: list[dict] = []

        for page in range(1, pages + 1):
            print(f"[TMDB] Fetching page {page}/{pages} of discover {media_type} (language={language})...")
            data = self._get(
                endpoint,
                params={
                    "with_original_language": language,
                    "sort_by": "vote_count.desc",
                    "vote_average.gte": 7.0,
                    "vote_count.gte": 100,
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

                genre_names = [genre_map.get(gid, '') for gid in item.get("genre_ids", [])]
                genre_str = ", ".join(filter(None, genre_names))

                if media_type == "movie":
                    title = item.get("title", "")
                    release_date = format_date(item.get("release_date"))
                else:
                    title = item.get("name", "")
                    release_date = format_date(item.get("first_air_date"))

                results.append(
                    {
                        "tmdb_id":           tmdb_id,
                        "title":             title,
                        "overview":          clean_text(item.get("overview")),
                        "poster_path":       poster_url,
                        "media_type":        media_type,
                        "release_date":      release_date,
                        "vote_average":      item.get("vote_average", 0.0),
                        "tmdb_score":        round(item.get("vote_average", 0) * 10),
                        "genre":             genre_str,
                        "imdb_id":           item.get("imdb_id", None),
                        "popularity":        item.get("popularity", 0.0),
                        "vote_count":        item.get("vote_count"),
                        "status":            item.get("status"),
                        "original_language": item.get("original_language"),
                        "is_documentary":    99 in item.get("genre_ids", []),
                        "is_limited_series": None,
                    }
                )

        print(f"[TMDB] Collected {len(results)} unique discover {media_type} (language={language}).")
        return results

    def get_trending_movies(self, pages: int = 3, genre_map: dict[int, str] | None = None) -> list[dict]:
        """Fetch trending movies this week via /trending/movie/week."""
        genre_map = genre_map or {}
        seen_ids: set[int] = set()
        results: list[dict] = []

        for page in range(1, pages + 1):
            print(f"[TMDB] Fetching page {page}/{pages} of trending movies (week)...")
            data = self._get(
                "/trending/movie/week",
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
                        "vote_count":        item.get("vote_count"),
                        "status":            item.get("status"),
                        "original_language": item.get("original_language"),
                        "is_documentary":    99 in item.get("genre_ids", []),
                        "is_limited_series": None,
                    }
                )

        print(f"[TMDB] Collected {len(results)} unique trending movies.")
        return results

    def get_trending_tv(self, pages: int = 3, genre_map: dict[int, str] | None = None) -> list[dict]:
        """Fetch trending TV shows this week via /trending/tv/week."""
        genre_map = genre_map or {}
        seen_ids: set[int] = set()
        results: list[dict] = []

        for page in range(1, pages + 1):
            print(f"[TMDB] Fetching page {page}/{pages} of trending TV (week)...")
            data = self._get(
                "/trending/tv/week",
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
                        "vote_count":        item.get("vote_count"),
                        "status":            item.get("status"),
                        "original_language": item.get("original_language"),
                        "is_documentary":    99 in item.get("genre_ids", []),
                        "is_limited_series": None,
                    }
                )

        print(f"[TMDB] Collected {len(results)} unique trending TV shows.")
        return results

    def get_credits(self, tmdb_id: int, media_type: str, data: dict | None = None) -> dict:
        """
        Return credits for a title, split by media_type.

        Movies  → /movie/{id}/credits
                  directors (job==Director), writers (Writer/Screenplay/Story), cast (top-20)

        TV      → /tv/{id} (created_by) + /tv/{id}/aggregate_credits (cast top-20)
                  created_by from show detail, no writers, cast (top-20)

        Unlike this client's other get_*(data=...) helpers, *data* here is
        the *detail* payload (whatever /movie or /tv/{id} call the caller
        already made), not an isolated sub-resource response — because
        TV's created_by isn't a separate sub-resource, it's already a
        field on the base show detail, so there's no standalone
        "created_by endpoint" payload to hand in on its own:
          Movies  → detail with an appended "credits" key
                    (append_to_response=credits).
          TV      → detail with created_by (native on the base /tv/{id}
                    response) and an appended "aggregate_credits" key
                    (append_to_response=aggregate_credits).
        """
        if media_type == "movie":
            if data is not None:
                movie_credits = data.get("credits", {})
            else:
                try:
                    movie_credits = self._get(f"/movie/{tmdb_id}/credits")
                except Exception as exc:
                    print(f"[TMDB] Could not fetch credits for movie/{tmdb_id}: {exc}")
                    return {"directors": [], "writers": [], "cast": [], "created_by": [], "producers": []}

            crew = movie_credits.get("crew", [])

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

            cast_raw = movie_credits.get("cast", [])
            cast = [
                {"name": m.get("name", ""), "character": m.get("character", ""), "order": m.get("order")}
                for m in sorted(cast_raw, key=lambda x: x.get("order", 9999))
            ][:20]

            producers = _extract_producers(crew)
            return {"directors": directors, "writers": writers, "cast": cast, "created_by": [], "producers": producers}

        else:
            created_by: list[dict] = []
            agg: dict = {}

            if data is not None:
                created_by = [
                    {"name": p["name"]}
                    for p in data.get("created_by", [])
                    if p.get("name")
                ]
                agg = data.get("aggregate_credits", {})
            else:
                # TV: fetch show detail for created_by, then aggregate_credits for cast + crew
                try:
                    detail = self._get(f"/tv/{tmdb_id}")
                    created_by = [
                        {"name": p["name"]}
                        for p in detail.get("created_by", [])
                        if p.get("name")
                    ]
                except Exception as exc:
                    print(f"[TMDB] Could not fetch TV detail for {tmdb_id}: {exc}")

                try:
                    agg = self._get(f"/tv/{tmdb_id}/aggregate_credits")
                except Exception as exc:
                    print(f"[TMDB] Could not fetch aggregate_credits for tv/{tmdb_id}: {exc}")
                    agg = {}

            cast_raw = agg.get("cast", [])
            cast = [
                {"name": m.get("name", ""), "character": m.get("character", ""), "order": m.get("order")}
                for m in sorted(cast_raw, key=lambda x: x.get("order", 9999))
            ][:20]
            producers = _extract_producers(agg.get("crew", []))

            return {"directors": [], "writers": [], "cast": cast, "created_by": created_by, "producers": producers}

    def get_seasons(self, tmdb_id: int) -> list[dict]:
        """Return seasons for a TV show, excluding season_number == 0 (Specials)."""
        try:
            data = self._get(f"/tv/{tmdb_id}")
        except Exception as exc:
            print(f"[TMDB] Could not fetch TV detail for {tmdb_id}: {exc}")
            return []
        return [s for s in data.get("seasons", []) if s.get("season_number", 0) != 0]

    def get_season_episodes(
        self, tmdb_id: int, season_number: int
    ) -> tuple[float | None, list[dict]]:
        """Return (vote_average, episodes) for a specific season.

        vote_average is the season-level rating from TMDB.
        episodes contain episode_number, name, runtime, air_date.
        """
        try:
            data = self._get(f"/tv/{tmdb_id}/season/{season_number}")
        except Exception as exc:
            print(f"[TMDB] Could not fetch season {season_number} for tv/{tmdb_id}: {exc}")
            return None, []
        episodes = [
            {
                "episode_number": ep.get("episode_number"),
                "name":           ep.get("name", ""),
                "runtime":        ep.get("runtime"),
                "air_date":       ep.get("air_date"),
            }
            for ep in data.get("episodes", [])
        ]
        return data.get("vote_average"), episodes

    def get_certification(self, tmdb_id: int, media_type: str, data: dict | None = None) -> str | None:
        """
        Return the US content certification for a title.

        Movies  → /movie/{id}/release_dates: find iso_3166_1=='US', take the
                  first release_date entry with a non-empty certification.
        TV      → /tv/{id}/content_ratings: find iso_3166_1=='US', take rating.

        Pass a pre-fetched payload via *data* to avoid a second request when
        the caller already has one — the movie's /release_dates response
        (see get_movie_release_dates) or the TV show's /content_ratings
        response, matching whichever endpoint *media_type* would hit.
        """
        try:
            if media_type == "movie":
                if data is None:
                    data = self._get(f"/movie/{tmdb_id}/release_dates")
                for country in data.get("results", []):
                    if country.get("iso_3166_1") == "US":
                        for entry in country.get("release_dates", []):
                            cert = entry.get("certification", "").strip()
                            if cert:
                                return cert
            else:
                if data is None:
                    data = self._get(f"/tv/{tmdb_id}/content_ratings")
                for country in data.get("results", []):
                    if country.get("iso_3166_1") == "US":
                        rating = country.get("rating", "").strip()
                        return rating if rating else None
        except Exception as exc:
            print(f"[TMDB] Could not fetch certification for {media_type}/{tmdb_id}: {exc}")
        return None

    def get_movie_release_dates(self, tmdb_id: int, data: dict | None = None) -> tuple[str | None, str | None]:
        """
        Return (release_date, us_release_date) for a movie from
        /movie/{id}/release_dates.

        release_date     → earliest release date across ALL regions/types
                            (worldwide premiere, used as the canonical date).
        us_release_date   → earliest US release date with type == 3
                            (Theatrical) or type == 4 (Digital).

        Pass a pre-fetched /movie/{id}/release_dates payload via *data* to
        avoid a second request when the caller already has one (see
        get_certification).
        """
        if data is None:
            try:
                data = self._get(f"/movie/{tmdb_id}/release_dates")
            except Exception as exc:
                print(f"[TMDB] Could not fetch release dates for movie/{tmdb_id}: {exc}")
                return None, None

        all_dates: list[str] = []
        us_dates: list[str] = []

        for country in data.get("results", []):
            for entry in country.get("release_dates", []):
                raw = entry.get("release_date")
                if not raw:
                    continue
                date_str = raw[:10]
                all_dates.append(date_str)
                if country.get("iso_3166_1") == "US" and entry.get("type") in (3, 4):
                    us_dates.append(date_str)

        release_date    = min(all_dates) if all_dates else None
        us_release_date = min(us_dates) if us_dates else None
        return release_date, us_release_date

    def get_watch_providers(self, tmdb_id: int, media_type: str, data: dict | None = None) -> dict[str, list[str]]:
        """
        Return US watch providers for a title from
        /movie|tv/{id}/watch/providers.

        Returns a dict with keys 'flatrate' (subscription), 'rent', and
        'buy', each mapping to a list of provider_name strings. A missing
        category in the API response returns an empty list for that key.
        Returns all-empty lists when there is no US entry or on error.

        Pass a pre-fetched /movie|tv/{id}/watch/providers payload via *data*
        to avoid a second request when the caller already has one (e.g. via
        append_to_response=watch/providers on the detail call).
        """
        if data is None:
            try:
                data = self._get(f"/{media_type}/{tmdb_id}/watch/providers")
            except Exception as exc:
                print(f"[TMDB] Could not fetch watch providers for {media_type}/{tmdb_id}: {exc}")
                return {"flatrate": [], "rent": [], "buy": []}

        us = data.get("results", {}).get("US") or {}

        return {
            kind: [p["provider_name"] for p in us.get(kind, []) if p.get("provider_name")]
            for kind in ("flatrate", "rent", "buy")
        }

    def get_title_logo(self, tmdb_id: int, media_type: str, data: dict | None = None) -> str | None:
        """
        Return a full image URL for the title's logo, preferring English.
        Falls back to the first logo in any language if no English logo exists.
        Returns None if no logos are available.

        Pass a pre-fetched /movie|tv/{id}/images payload via *data* to avoid
        a second request when the caller already has one (e.g. via
        append_to_response=images on the detail call).
        """
        if data is None:
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

    def get_videos(self, tmdb_id: int, media_type: str, data: dict | None = None) -> list[dict]:
        """
        Return official YouTube trailers and teasers for a title from TMDB.

        Pass a pre-fetched /movie|tv/{id}/videos payload via *data* to avoid
        a second request when the caller already has one (e.g. via
        append_to_response=videos on the detail call).
        """
        if data is None:
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

    def get_movie_changes(self, start_date: str, end_date: str) -> list[int]:
        """
        Return every distinct movie tmdb_id that changed between
        *start_date* and *end_date* (YYYY-MM-DD, inclusive) via
        /movie/changes, fully paginated. TMDB caps this range at 14 days.
        """
        return self._get_changed_ids("/movie/changes", start_date, end_date)

    def get_tv_changes(self, start_date: str, end_date: str) -> list[int]:
        """
        Return every distinct TV tmdb_id that changed between *start_date*
        and *end_date* (YYYY-MM-DD, inclusive) via /tv/changes, fully
        paginated. TMDB caps this range at 14 days.
        """
        return self._get_changed_ids("/tv/changes", start_date, end_date)

    def _get_changed_ids(self, endpoint: str, start_date: str, end_date: str) -> list[int]:
        ids: list[int] = []
        seen: set[int] = set()
        page = 1
        total_pages = 1
        while page <= total_pages:
            data = self._get(
                endpoint,
                params={"start_date": start_date, "end_date": end_date, "page": page},
            )
            for item in data.get("results", []):
                tmdb_id = item.get("id")
                if tmdb_id is not None and tmdb_id not in seen:
                    seen.add(tmdb_id)
                    ids.append(tmdb_id)
            total_pages = data.get("total_pages") or 1
            print(f"[TMDB] {endpoint}: page {page}/{total_pages} ({start_date} to {end_date}), {len(ids)} id(s) so far...")
            page += 1
        return ids
