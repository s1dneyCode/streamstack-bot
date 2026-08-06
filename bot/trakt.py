"""
Trakt API client.

Handles all communication with api.trakt.tv v2. Each public method returns
plain Python dicts/lists so the rest of the bot stays decoupled from the raw
API response shape — mirrors bot/tmdb.py's TmdbClient in spirit, with two
structural differences Trakt's API forces:

  - Auth is via headers (trakt-api-key, trakt-api-version, Content-Type),
    not a query param like TMDB's api_key.
  - List-endpoint pagination is reported via the X-Pagination-Page-Count
    response header, not a JSON body field — see _get_list().

Reference: https://trakt.docs.apiary.io/
"""

import os
import sys
import time

import requests

from .rate_limiter import RateLimiter

TRAKT_BASE = "https://api.trakt.tv"

# Trakt's documented limit is ~1000 GET/5min (~3.3/s); stay a bit under it.
TRAKT_RATE_LIMIT = 3.0  # requests/second

_MEDIA_TYPE_PATH = {"movie": "movies", "tv": "shows"}


def load_env() -> dict[str, str]:
    required = ["TRAKT_CLIENT_ID"]
    config: dict[str, str] = {}
    for key in required:
        value = os.environ.get(key)
        if not value:
            print(f"[TRAKT] ERROR: Required environment variable '{key}' is not set.")
            sys.exit(1)
        config[key] = value
    return config


class TraktClient:
    """Thin wrapper around the Trakt REST API v2."""

    def __init__(self, client_id: str) -> None:
        self.client_id = client_id
        self._headers = {
            "trakt-api-key": client_id,
            "trakt-api-version": "2",
            "Content-Type": "application/json",
        }
        self._limiter = RateLimiter(rate=TRAKT_RATE_LIMIT)

    @staticmethod
    def _path_segment(media_type: str) -> str:
        try:
            return _MEDIA_TYPE_PATH[media_type]
        except KeyError:
            raise ValueError(f"Unknown media_type: {media_type!r} (expected 'movie' or 'tv')")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _request(self, path: str, params: dict | None = None, retries: int = 3) -> requests.Response:
        """
        Perform a GET request against *path* (relative to TRAKT_BASE), with
        the auth headers attached.

        Throttled to TRAKT_RATE_LIMIT req/s via a shared token-bucket
        limiter (self._limiter) before every attempt, including retries.

        Same retry policy as TmdbClient._get(): retries 429 (honoring
        Retry-After), 5xx, and network/timeout errors up to *retries*
        times; a non-429 4xx is raised immediately on the first attempt —
        it can't succeed just by asking again. Raises the final exception
        if all attempts are exhausted (or immediately, for a non-retryable
        4xx).

        Returns the raw response (not .json()) so callers can also read
        response headers — needed by _get_list() for Trakt's
        header-based pagination.
        """
        url = f"{TRAKT_BASE}{path}"

        last_exc: Exception | None = None
        for attempt in range(1, retries + 1):
            self._limiter.acquire()
            try:
                response = requests.get(url, headers=self._headers, params=params or {}, timeout=15)
                if response.status_code == 429:
                    retry_after = response.headers.get("Retry-After")
                    wait = float(retry_after) if retry_after else 2.0
                    print(f"[TRAKT] Rate limited (429, attempt {attempt}/{retries}) — waiting {wait}s...")
                    last_exc = requests.HTTPError(f"429 Too Many Requests: {url}")
                    if attempt < retries:
                        time.sleep(wait)
                    continue
                response.raise_for_status()
                return response
            except requests.RequestException as exc:
                status = exc.response.status_code if isinstance(exc, requests.HTTPError) and exc.response is not None else None
                if status is not None and 400 <= status < 500:
                    print(f"[TRAKT] {status} for {url} — not retrying (client error).")
                    raise

                last_exc = exc
                print(f"[TRAKT] Request failed (attempt {attempt}/{retries}): {exc}")
                if attempt < retries:
                    time.sleep(2)

        raise last_exc  # type: ignore[misc]

    def _get(self, path: str, params: dict | None = None) -> dict | list:
        """GET *path*, returning the parsed JSON body only."""
        return self._request(path, params).json()

    def _get_list(self, path: str, params: dict | None = None) -> tuple[list, int]:
        """
        GET a paginated list endpoint. Returns (items, page_count) — Trakt
        reports the total page count via the X-Pagination-Page-Count
        response header, not a JSON body field.

        A present-but-malformed header (e.g. mangled by a proxy) degrades
        to page_count=1 rather than raising — this arrives after
        _request() already returned a successful response, so it's not a
        request failure the retry loop should ever see.
        """
        response = self._request(path, params)
        raw_page_count = response.headers.get("X-Pagination-Page-Count")
        try:
            page_count = int(raw_page_count) if raw_page_count else 1
        except ValueError:
            print(f"[TRAKT] Malformed X-Pagination-Page-Count header ({raw_page_count!r}) for {path} — treating as a single page.")
            page_count = 1
        return response.json(), page_count

    # ------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------

    def get_most_watched(
        self, media_type: str, period: str = "weekly", page: int = 1, limit: int = 100
    ) -> tuple[list[dict], int]:
        """
        One page of /movies|shows/watched/{period}, extended=full (adds
        rating/votes to the embedded movie/show object). Each item exposes
        watcher_count, play_count, and the movie/show object with ids.tmdb.
        """
        segment = self._path_segment(media_type)
        return self._get_list(
            f"/{segment}/watched/{period}",
            params={"extended": "full", "page": page, "limit": limit},
        )

    def get_trending(self, media_type: str, page: int = 1, limit: int = 100) -> tuple[list[dict], int]:
        """One page of /movies|shows/trending. Each item exposes watchers + ids.tmdb."""
        segment = self._path_segment(media_type)
        return self._get_list(f"/{segment}/trending", params={"page": page, "limit": limit})

    def get_anticipated(self, media_type: str, page: int = 1, limit: int = 100) -> tuple[list[dict], int]:
        """
        One page of /movies|shows/anticipated, extended=full. Each item
        exposes list_count + the movie/show object with ids.tmdb, rating, votes.
        """
        segment = self._path_segment(media_type)
        return self._get_list(
            f"/{segment}/anticipated",
            params={"extended": "full", "page": page, "limit": limit},
        )

    def get_related(self, media_type: str, trakt_id_or_slug, limit: int = 20) -> list[dict]:
        """
        Related titles for a single movie/show. *trakt_id_or_slug* accepts
        any of Trakt's interchangeable id types (trakt id, slug, imdb id,
        or — what we actually pass — our own tmdb_id). Not paginated via
        headers like the list endpoints; Trakt just returns up to *limit*
        items directly.
        """
        segment = self._path_segment(media_type)
        return self._get(f"/{segment}/{trakt_id_or_slug}/related", params={"limit": limit})
