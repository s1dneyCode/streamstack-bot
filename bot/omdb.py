"""
OMDb (Open Movie Database) API client.

OMDb aggregates metadata from multiple sources, including Rotten Tomatoes
critic scores.  We use it solely for that RT score; TMDB is our source of
truth for everything else.

This account is on OMDb's PAID tier: 100,000 requests/day, not the free
tier's 1,000. daily_quota below is therefore a per-process runaway-loop
backstop rather than a scarce-resource ration — see the comment on
DEFAULT_DAILY_QUOTA for why per-process is sufficient here and where it
doesn't hold. Overridable via the OMDB_DAILY_QUOTA env var.

Reference: https://www.omdbapi.com/
"""

import os
import requests
import re

from bot.rate_limiter import RateLimiter

OMDB_BASE = "http://www.omdbapi.com/"

# Conservative throttle — OMDb doesn't publish a documented per-second cap.
OMDB_RATE_LIMIT = 5.0  # requests/second

# Runaway-loop backstop, NOT quota rationing — the paid tier's real ceiling
# is 100,000/day.
#
# Two entry points call OMDb: the nightly (bot/main.py, on cron) and the
# manual RT backfill (bot/backfill_rt_scores.py, workflow_dispatch-only, no
# cron). Only the nightly is scheduled, so no two OMDb processes run
# automatically on the same day.
#
# This is a PER-PROCESS backstop, not a cross-process guarantee: manually
# triggering the backfill on the same calendar day as a nightly gives each
# process its own budget (up to 2x90k against the 100k real ceiling). Not a
# risk in practice — total unique RT work is bounded by the ~9k-movie
# missing-RT backlog, so per-run usage stays well under the cap and a
# same-day pair stays under 100k. If that changes, lower OMDB_DAILY_QUOTA
# or add cross-process accounting.
#
# Previously 950, sized for the free tier's 1,000/day wall — that throttled
# the bot to ~1% of available quota and starved Step 11b, since Step 11
# alone (~600+ movies) could trip the cap before 11b ever ran.
DEFAULT_DAILY_QUOTA = 90000


class OmdbClient:
    """Fetches Rotten Tomatoes scores from the OMDb API."""

    def __init__(self, api_key: str, daily_quota: int | None = None) -> None:
        self.api_key = api_key
        # Precedence: explicit arg (tests can force a value) > env
        # OMDB_DAILY_QUOTA > DEFAULT_DAILY_QUOTA.
        self.daily_quota = (
            daily_quota if daily_quota is not None
            else int(os.environ.get("OMDB_DAILY_QUOTA", DEFAULT_DAILY_QUOTA))
        )
        self._requests_made = 0
        self._quota_exhausted = False
        self._limiter = RateLimiter(rate=OMDB_RATE_LIMIT)

    @property
    def quota_exhausted(self) -> bool:
        """
        True once this run has burned its OMDb daily quota. Read-only.

        get_rt_score() returns None both for "genuinely not found" and for
        "quota gone", so callers that treat a None as a definitive answer
        (e.g. by stamping a re-check timestamp) should consult this first
        and stop instead — every further call is a guaranteed no-op.
        """
        return self._quota_exhausted

    def get_rt_score(self, title: str, year: str | None = None, imdb_id: str | None = None) -> int | None:
        """
        Attempts to find the RT score using multiple search strategies in order:
        1. By IMDb ID (most precise)
        2. By exact title + year
        3. By title without year
        4. By simplified title (removes subtitles, leading articles, possessives)
        Returns score as int (e.g. 88) or None if not found in any strategy —
        including when the run's OMDb quota has been exhausted, so callers
        can't tell that case apart from "genuinely not found" (see
        _quota_exhausted / the "Quota exhausted" log line).
        """
        if self._quota_exhausted:
            return None

        strategies: list[tuple[dict, str]] = []

        if imdb_id:
            strategies.append(({'i': imdb_id}, 'IMDb ID'))

        if year:
            strategies.append(({'t': title, 'y': year}, 'title + year'))

        strategies.append(({'t': title}, 'title'))

        simplified = self._simplify_title(title)
        if simplified != title:
            if year:
                strategies.append(({'t': simplified, 'y': year}, 'simplified title + year'))
            strategies.append(({'t': simplified}, 'simplified title'))

        for params, strategy_name in strategies:
            if self._requests_made >= self.daily_quota:
                self._quota_exhausted = True
                print(
                    f"[OMDb] Quota exhausted ({self._requests_made}/{self.daily_quota} "
                    "requests this run) — stopping OMDb lookups for the rest of this run."
                )
                return None

            params['apikey'] = self.api_key
            self._limiter.acquire()
            self._requests_made += 1
            try:
                response = requests.get(OMDB_BASE, params=params, timeout=10)
                data = response.json()
                if data.get('Response') == 'True':
                    for rating in data.get('Ratings', []):
                        if rating.get('Source') == 'Rotten Tomatoes':
                            value = rating.get('Value', '')
                            if value and value != 'N/A':
                                score = int(value.replace('%', ''))
                                print(f"[OMDb] Found RT score for '{title}' via {strategy_name}: {score}%")
                                return score
            except Exception:
                pass

        print(f"[OMDb] No RT score found for '{title}' after all strategies.")
        return None

    def _simplify_title(self, title: str) -> str:
        """Removes possessives, subtitles after ':', and leading articles."""
        simplified = re.sub(r"^[\w\s]+'s\s+", '', title)
        simplified = re.sub(r'[\:\-].*$', '', simplified).strip()
        simplified = re.sub(r'^(The|A|An)\s+', '', simplified, flags=re.IGNORECASE)
        return simplified.strip()
