"""
Shared helper utilities used across the bot modules.
None of these functions call external APIs — they are pure data-transformation
helpers that any module can import without introducing circular dependencies.
"""

from datetime import date, timedelta
import re


def format_date(date_str: str | None) -> str | None:
    """
    Normalize an arbitrary date string to YYYY-MM-DD.

    Tries the two most common formats returned by TMDB (YYYY-MM-DD and
    YYYY-MM-DDTHH:MM:SSZ).  Returns None for empty / unparseable input so
    callers can decide how to handle missing dates rather than storing garbage.
    """
    if not date_str:
        return None

    # Strip time component if present (e.g. "2024-03-15T00:00:00.000Z")
    date_part = date_str.split("T")[0].strip()

    # Validate the resulting YYYY-MM-DD fragment
    if re.match(r"^\d{4}-\d{2}-\d{2}$", date_part):
        return date_part

    return None


def clean_text(text: str | None) -> str | None:
    """
    Collapse all runs of whitespace (spaces, tabs, newlines) into a single
    space and strip leading / trailing whitespace.

    TMDB overview fields sometimes contain newline-separated paragraphs; this
    flattens them into a single clean string suitable for database storage.
    Returns None when given None so callers don't need null-checks everywhere.
    """
    if text is None:
        return None

    return re.sub(r"\s+", " ", text).strip()


def get_date_n_days_ago(n: int) -> str:
    """
    Return the calendar date that was *n* days before today as a YYYY-MM-DD
    string.  Used to build date-range filters when querying APIs for "recent"
    content without hardcoding a specific date.
    """
    target = date.today() - timedelta(days=n)
    return target.strftime("%Y-%m-%d")


def chunk_list(lst: list, size: int) -> list[list]:
    """
    Split *lst* into consecutive sub-lists of at most *size* elements.

    The last chunk will be smaller than *size* when len(lst) is not a multiple
    of *size*.  Useful for batching Supabase upserts or OMDb requests so we
    never send an unbounded payload in a single call.

    Example:
        chunk_list([1, 2, 3, 4, 5], 2) → [[1, 2], [3, 4], [5]]
    """
    return [lst[i : i + size] for i in range(0, len(lst), size)]
