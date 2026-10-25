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

import json
import os
import sys
import time
from datetime import datetime, timezone

import requests

from .rate_limiter import RateLimiter

TRAKT_BASE = "https://api.trakt.tv"

# Trakt's documented limit is ~1000 GET/5min (~3.3/s); stay a bit under it.
# B5.1: at this pace run #23 still hit eight 429s (Retry-After 300 s each),
# so the wall is not where this comment says. The client now reads Trakt's own
# X-Ratelimit header and pauses before the wall (TRAKT_RATELIMIT_FLOOR); the
# pause, not a slower clock, is what prevents a 300 s penalty.
TRAKT_RATE_LIMIT = 3.0  # requests/second

# When X-Ratelimit says this many requests or fewer remain in the window, the
# next request first waits until the window's `until` (1–300 s).
TRAKT_RATELIMIT_FLOOR = 25
_PAUSE_MIN_SECONDS = 1.0
_PAUSE_MAX_SECONDS = 300.0

_MEDIA_TYPE_PATH = {"movie": "movies", "tv": "shows"}
_SEARCH_TYPE = {"movie": "movie", "tv": "show"}  # Trakt's ?type= and result key


def _header_names(response: requests.Response) -> list[str]:
    """A response's header NAMES, sorted — never their values."""
    return sorted(response.headers.keys(), key=str.lower)


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

    def __init__(self, client_id: str, clock=None, monotonic=None) -> None:
        """*clock* returns the current instant (aware UTC) for the pause
        before Trakt's wall; *monotonic* returns seconds for measuring the
        wall between 429s. Both are injected by tests; the real clocks
        otherwise."""
        self.client_id = client_id
        self._headers = {
            "trakt-api-key": client_id,
            "trakt-api-version": "2",
            "Content-Type": "application/json",
        }
        self._limiter = RateLimiter(rate=TRAKT_RATE_LIMIT)
        self._clock = clock if clock is not None else (lambda: datetime.now(timezone.utc))
        # X-Ratelimit, as Trakt reports it (B5.1): the first header's window,
        # the pause the last one asked for, and what the run counted.
        self._window: tuple[str, int, int] | None = None  # (name, limit, period) of the first header
        self._pause: dict | None = None                    # set when remaining <= the floor
        self._header_warned = False
        self._pauses = 0
        self._429s = 0
        # The wall, measured (B5.2): Trakt sends this client no X-Ratelimit
        # (run #26), so every 429 says how many requests, and how many
        # monotonic seconds, it took since the previous one.
        self._monotonic = monotonic if monotonic is not None else (lambda: time.monotonic())
        self._requests = 0                     # requests sent, every attempt
        self._first_sent: float | None = None  # monotonic time of the first one
        self._last_429_at: float | None = None
        self._requests_at_last_429 = 0
        self._walls: list[int] = []            # requests between two 429s (the first: since the start)
        self._responses = 0

    def wall_estimate(self) -> int | None:
        """The fewest requests this run made between two 429s (the first
        counted from the start), or None when no 429 came."""
        return min(self._walls) if self._walls else None

    def rate_limit_summary(self) -> str:
        """For the SUMMARY line: Trakt's window as its first header named it,
        and how many pauses and 429s this run had."""
        limit = f"{self._window[1]}/{self._window[2]}s" if self._window else "unknown"
        return f"trakt_limit={limit} trakt_pauses={self._pauses} trakt_429={self._429s}"

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

        Every response's X-Ratelimit header is read (B5.1): at
        TRAKT_RATELIMIT_FLOOR remaining or fewer, the next attempt first
        waits until the header's `until`. A 429 still honours Retry-After.
        """
        url = f"{TRAKT_BASE}{path}"

        last_exc: Exception | None = None
        for attempt in range(1, retries + 1):
            self._pause_before_the_wall()
            self._limiter.acquire()
            try:
                sent_at = self._monotonic()
                self._requests += 1
                if self._first_sent is None:
                    self._first_sent = sent_at
                response = requests.get(url, headers=self._headers, params=params or {}, timeout=15)
                self._responses += 1
                if self._responses == 1:
                    print(f"[TRAKT] First response headers: {_header_names(response)}")
                self._read_rate_limit(response)
                if response.status_code == 429:
                    self._429s += 1
                    self._measure_429(response, sent_at)
                    retry_after = response.headers.get("Retry-After")
                    wait = float(retry_after) if retry_after else 2.0
                    print(f"[TRAKT] Rate limited (429, attempt {attempt}/{retries}) — waiting {wait}s...")
                    last_exc = requests.HTTPError(f"429 Too Many Requests: {url}")
                    if attempt < retries:
                        time.sleep(wait)
                        self._pause = None  # Retry-After was the wait; do not wait twice
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

    def _measure_429(self, response: requests.Response, sent_at: float) -> None:
        """One line per 429: requests since the start and since the previous
        429, the monotonic seconds between them, the Retry-After and the
        response's header NAMES — the only values printed are Retry-After's
        (and X-Ratelimit's, by _read_rate_limit)."""
        since_previous = self._requests - self._requests_at_last_429
        mark = self._last_429_at if self._last_429_at is not None else self._first_sent
        seconds = sent_at - mark
        self._walls.append(since_previous)
        self._requests_at_last_429, self._last_429_at = self._requests, sent_at
        print(
            f"[TRAKT] 429 #{self._429s} — requests since start={self._requests}, "
            f"since previous 429={since_previous} over {seconds:.1f} s (monotonic), "
            f"Retry-After={response.headers.get('Retry-After') or '-'}, headers={_header_names(response)}"
        )

    def _read_rate_limit(self, response: requests.Response) -> None:
        """Parse X-Ratelimit — JSON like {"name": "UNAUTHED_API_GET_LIMIT",
        "period": 300, "limit": 1000, "remaining": N, "until": "<ISO 8601>"}.
        A missing or malformed header is logged once a run and changes
        nothing; only this header's value is ever printed."""
        raw = response.headers.get("X-Ratelimit")
        try:
            info = json.loads(raw)
            remaining = int(info["remaining"])
            limit = int(info["limit"])
            period = int(info["period"])
            until_raw = str(info["until"])
            until = datetime.fromisoformat(until_raw.replace("Z", "+00:00"))
        except (TypeError, ValueError, KeyError, AttributeError):
            if not self._header_warned:
                self._header_warned = True
                shown = "missing" if raw is None else f"malformed: {raw[:80]!r}"
                print(f"[TRAKT] X-Ratelimit header {shown} — pacing at {TRAKT_RATE_LIMIT}/s only.")
            return
        if until.tzinfo is None:
            until = until.replace(tzinfo=timezone.utc)
        if self._window is None:
            self._window = (str(info.get("name")), limit, period)
        self._pause = (
            {"until": until, "until_raw": until_raw, "remaining": remaining, "limit": limit, "period": period}
            if remaining <= TRAKT_RATELIMIT_FLOOR else None
        )

    def _pause_before_the_wall(self) -> None:
        """When the last header asked for it, wait until its `until`, bounded
        to 1–300 s, on the injected clock — once per pause."""
        pause, self._pause = self._pause, None
        if pause is None:
            return
        wait = (pause["until"] - self._clock()).total_seconds()
        wait = min(max(wait, _PAUSE_MIN_SECONDS), _PAUSE_MAX_SECONDS)
        print(
            f"[TRAKT] Near the limit (remaining={pause['remaining']} of {pause['limit']} per {pause['period']} s) "
            f"— pausing {wait:.0f}s until {pause['until_raw']}"
        )
        self._pauses += 1
        time.sleep(wait)

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

    def resolve_id(self, media_type: str, tmdb_id: int) -> int | None:
        """
        Trakt's own id for our *tmdb_id*, through Trakt's ID lookup
        (GET /search/tmdb/{tmdb_id}?type=movie|show). Each result is
        {"type": "movie"|"show", "movie"|"show": {"ids": {"trakt", "tmdb", …}}}.

        The FIRST result whose type is the one asked AND whose ids.tmdb is
        *tmdb_id* wins. A tmdb id can name a movie and a show at once (1416
        is a movie and Grey's Anatomy), so the type is the filter, never a
        tiebreak. None when the list is empty, nothing matches, or Trakt
        answers 404; any other error propagates as _request() raises it.
        """
        self._path_segment(media_type)  # validates media_type the same way
        kind = _SEARCH_TYPE[media_type]
        try:
            results = self._get(f"/search/tmdb/{tmdb_id}", params={"type": kind})
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                return None
            raise
        for result in results or []:
            if result.get("type") != kind:
                continue
            # Each result carries its title object under its OWN type's key.
            ids = (result.get(result.get("type")) or {}).get("ids") or {}
            if ids.get("tmdb") == tmdb_id:
                return ids.get("trakt")
        return None

    def get_related(self, media_type: str, trakt_id: int | str, limit: int = 20) -> list[dict]:
        """
        Related titles for a single movie/show. *trakt_id* is any of the
        three forms Trakt takes in that slot: a Trakt id (int, from
        resolve_id()), a Trakt slug, or an IMDB id ("tt0111161") — never our
        tmdb_id (B5). Trakt reads a bare number in this path as ITS id, so a
        tmdb_id here answers with whatever other title Trakt has under that
        number. (This used to say the parameter took "our own tmdb_id" —
        that was the bug.) An IMDB id Trakt does not know answers 404, raised
        like any 4xx. Not paginated via headers like the list endpoints;
        Trakt just returns up to *limit* items directly.
        """
        segment = self._path_segment(media_type)
        return self._get(f"/{segment}/{trakt_id}/related", params={"limit": limit})
