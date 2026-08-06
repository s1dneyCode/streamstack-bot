"""
Shared title-quality filtering and bare-insert-building pieces used across
ingestion entry points (bulk_import.py, main.py, sync_changes.py,
coverage_sweep.py).
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


def passes_sync_quality_filter(item: dict) -> bool:
    """
    Return True if a title payload clears the bar for a bare insert.

    Accepts either shape: a /movie or /tv detail payload (as returned by
    TmdbClient._get) used by sync_changes.py's daily /changes sync, or a
    raw /discover/{movie|tv} list-item used by coverage_sweep.py's
    historical sweep — both carry the same field names this checks
    (poster_path, original_language, adult, vote_count, popularity,
    release_date/first_air_date), so no per-caller branching is needed.

    Requires a poster, an allowed original_language, include_adult=false
    (movies only — TV payloads don't carry an `adult` field, so this check
    is a no-op for TV), and at least one of: vote_count >= 3,
    popularity >= 2.0, or released within the last 730 days.
    """
    if not item.get("poster_path"):
        return False
    if item.get("original_language") not in ALLOWED_LANGUAGES:
        return False
    if item.get("adult"):
        return False

    vote_count = item.get("vote_count") or 0
    popularity = item.get("popularity") or 0.0

    release_date_str = item.get("release_date") or item.get("first_air_date")
    is_recent = False
    if release_date_str:
        try:
            release_date_obj = date.fromisoformat(release_date_str[:10])
            is_recent = (date.today() - release_date_obj).days <= SYNC_RECENCY_DAYS
        except ValueError:
            pass

    return vote_count >= SYNC_MIN_VOTE_COUNT or popularity >= SYNC_MIN_POPULARITY or is_recent


def build_bare_media_record(
    tmdb_id: int,
    media_type: str,
    title: str,
    overview: str | None,
    poster_path: str | None,
    release_date: str | None,
    original_language: str | None,
    vote_average: float | None,
    vote_count: int | None,
    popularity: float | None,
    genres: list[str],
    imdb_id: str | None = None,
    status: str | None = None,
) -> dict:
    """
    Build the common bare-row payload for a brand-new title: enriched_at is
    left unset (NULL until enrich_new_titles.py's state scan picks it up),
    is_in_theatres/is_streamable_now default False, and everything else
    (images, credits, seasons, logos, certifications, runtime) is deferred
    to enrichment.

    Shared by sync_changes.py (fields extracted from a /movie|tv detail
    payload) and coverage_sweep.py (fields extracted from a /discover list
    item, genres resolved via a genre_id → name map since discover items
    only carry genre_ids) so the two bare-insert shapes can't drift apart.
    """
    return {
        "tmdb_id":           tmdb_id,
        "title":             title,
        "overview":          overview,
        "poster_path":       poster_path,
        "media_type":        media_type,
        "release_date":      release_date,
        "status":            status,
        "original_language": original_language,
        "tmdb_score":        round((vote_average or 0) * 10),
        "genres":            genres,
        "imdb_id":           imdb_id,
        "popularity":        popularity or 0.0,
        "vote_count":        vote_count or 0,
        "is_in_theatres":    False,
        "is_streamable_now": False,
    }
