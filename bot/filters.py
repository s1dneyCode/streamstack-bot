"""
Shared title-quality filtering pieces used across ingestion entry points
(bulk_import.py, main.py, sync_changes.py).
"""

from datetime import date

# Single source of truth for which original languages the catalog accepts.
# Previously duplicated as bulk_import.py's function-local _ALLOWED_LANGUAGES
# and main.py's module-level copy of the same set — both now import this
# instead of maintaining their own copy.
ALLOWED_LANGUAGES = {'en', 'es', 'fr', 'de', 'ko', 'ja', 'pt', 'it', 'zh', 'tl'}

# sync_changes.py's quality bar for the daily /changes feed. Deliberately
# much more permissive than bulk_import.py's tiered vote-count filter:
# /changes surfaces effectively every id TMDB touched that day — including
# a lot of low-quality/spam titles — not a curated discover list, so this
# only needs to keep out the obvious junk, not do fine-grained curation.
SYNC_MIN_VOTE_COUNT = 3
SYNC_MIN_POPULARITY = 2.0
SYNC_RECENCY_DAYS   = 730


def passes_sync_quality_filter(detail: dict) -> bool:
    """
    Return True if a /movie or /tv detail payload (as returned by
    TmdbClient._get, not the normalized list-endpoint shape) clears the
    bar for a bare insert via sync_changes.py's daily /changes sync.

    Requires a poster, an allowed original_language, include_adult=false
    (movies only — TV detail payloads don't carry an `adult` field, so
    this check is a no-op for TV), and at least one of: vote_count >= 3,
    popularity >= 2.0, or released within the last 730 days.
    """
    if not detail.get("poster_path"):
        return False
    if detail.get("original_language") not in ALLOWED_LANGUAGES:
        return False
    if detail.get("adult"):
        return False

    vote_count = detail.get("vote_count") or 0
    popularity = detail.get("popularity") or 0.0

    release_date_str = detail.get("release_date") or detail.get("first_air_date")
    is_recent = False
    if release_date_str:
        try:
            release_date_obj = date.fromisoformat(release_date_str[:10])
            is_recent = (date.today() - release_date_obj).days <= SYNC_RECENCY_DAYS
        except ValueError:
            pass

    return vote_count >= SYNC_MIN_VOTE_COUNT or popularity >= SYNC_MIN_POPULARITY or is_recent
