"""
OMDb (Open Movie Database) API client.

OMDb aggregates metadata from multiple sources, including Rotten Tomatoes
critic scores.  We use it solely for that RT score; TMDB is our source of
truth for everything else.

Free tier is limited to 1 000 requests/day.

Reference: https://www.omdbapi.com/
"""

import re
import time
import requests

OMDB_BASE = "http://www.omdbapi.com/"


class OmdbClient:
    """Fetches Rotten Tomatoes scores from the OMDb API."""

    def __init__(self, api_key: str) -> None:
        self.api_key = api_key

    def get_rt_score(self, title: str, year: str | None = None, imdb_id: str | None = None) -> int | None:
        """
        Attempts to find the RT score using multiple search strategies in order:
        1. By IMDb ID (most precise)
        2. By exact title + year
        3. By title without year
        4. By simplified title (removes subtitles, leading articles, possessives)
        Returns score as int (e.g. 88) or None if not found in any strategy.
        """
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
            params['apikey'] = self.api_key
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
            time.sleep(0.3)

        print(f"[OMDb] No RT score found for '{title}' after all strategies.")
        return None

    def _simplify_title(self, title: str) -> str:
        """Removes possessives, subtitles after ':', and leading articles."""
        simplified = re.sub(r"^[\w\s]+'s\s+", '', title)
        simplified = re.sub(r'[\:\-].*$', '', simplified).strip()
        simplified = re.sub(r'^(The|A|An)\s+', '', simplified, flags=re.IGNORECASE)
        return simplified.strip()
