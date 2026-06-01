"""
OMDb (Open Movie Database) API client.

OMDb aggregates metadata from multiple sources, including Rotten Tomatoes
critic scores.  We use it solely for that RT score; TMDB is our source of
truth for everything else.

Free tier is limited to 1 000 requests/day, so we add a 1-second sleep
between calls to avoid accidentally exhausting the quota in a single run.

Reference: https://www.omdbapi.com/
"""

import time
import requests

OMDB_BASE = "http://www.omdbapi.com/"


class OmdbClient:
    """Fetches Rotten Tomatoes scores from the OMDb API."""

    def __init__(self, api_key: str) -> None:
        self.api_key = api_key

    def get_rt_score(self, title: str, year: str | None) -> int | None:
        """
        Look up the Rotten Tomatoes critic score for *title* (optionally
        filtered by *year* to reduce false-positive matches on remakes).

        OMDb returns a `Ratings` array where each element has `Source` and
        `Value` fields.  The Rotten Tomatoes entry looks like:
            {"Source": "Rotten Tomatoes", "Value": "88%"}

        We strip the "%" suffix and return the integer so callers can store
        it as a plain number.

        Returns None when:
        - The OMDb API returns an error ("Response": "False")
        - No Rotten Tomatoes entry exists in the Ratings array
        - The request itself fails for any reason
        """
        print(f"[OMDb] Fetching RT score for {title} ({year})...")

        params: dict = {"t": title, "apikey": self.api_key}
        if year:
            # Providing the year narrows the match and reduces wrong-film hits
            params["y"] = year

        try:
            response = requests.get(OMDB_BASE, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as exc:
            print(f"[OMDb] Request failed for '{title}': {exc}")
            # Honour the rate-limit sleep even on failure so we don't hammer
            # the endpoint with rapid retries during a flaky network period
            time.sleep(1)
            return None

        # OMDb signals errors in the JSON body rather than via HTTP status
        if data.get("Response") == "False":
            print(f"[OMDb] No results for '{title}': {data.get('Error')}")
            time.sleep(1)
            return None

        # Walk the Ratings array looking for the Rotten Tomatoes source
        for rating in data.get("Ratings", []):
            if rating.get("Source") == "Rotten Tomatoes":
                raw_value = rating.get("Value", "")
                # Strip the trailing "%" and convert to int (e.g. "88%" → 88)
                try:
                    score = int(raw_value.replace("%", "").strip())
                    print(f"[OMDb] RT score for '{title}': {score}%")
                    time.sleep(1)
                    return score
                except ValueError:
                    print(f"[OMDb] Could not parse RT value '{raw_value}' for '{title}'")
                    break

        print(f"[OMDb] No Rotten Tomatoes rating found for '{title}'")
        time.sleep(1)
        return None
