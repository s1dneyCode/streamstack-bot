"""
The house cache policy for media-images (B5.10): what Cache-Control each
object deserves, from the shape of its key alone.

Until B5.8 no upload asked for a cache, and the server stored no-cache on
every object, which the app's image cache (flutter_cache_manager) reads as
zero validity: every cold start revalidated every image it held. B5.8 gave
the TMDb-file keys a year. This module is the one table for every key:

  - a key that names TMDb's FILE — a changed image is a new key, so its bytes
    never change — a year (ONE_YEAR_CACHE_SECONDS, B5.8's rule):
        posters/<tmdb_id>/<stem>.jpg      migrate_images.refresh_poster
        posters-hd/<tmdb_id>/<stem>.jpg   posters_hd
        tmdb/w<size>/<file>               tmdb_mirror
  - a key that names a SLOT — the bytes under it can change (backfill_seasons
    re-uploads every active show's) — 7 days (SEVEN_DAYS_CACHE_SECONDS,
    Fran's number, 22/11: a changed image is at most a week stale on a
    device):
        posters/<tmdb_id>.jpg             migrate_images.migrate_poster
        carousels/<tmdb_id>/<index>.jpg   migrate_images.migrate_carousel
        seasons/<tmdb_id>/<season>.jpg    enrich_new_titles, backfill_seasons
  - anything else: None — unknown, never guessed. A caller that gets None
    sends today's request (the server stores no-cache); bot/image_recache.py
    skips the object and counts it.

Pure: no client, no network. The uploaders pass
cache_seconds=cache_seconds_for(<the key they upload>).
"""

import re

ONE_YEAR_CACHE_SECONDS = 31536000
SEVEN_DAYS_CACHE_SECONDS = 604800

# A segment the policy accepts as a stem or a file: anything but a slash or a
# query mark (tmdb_mirror's ledger holds no name with either).
_SEGMENT = r"[^/?]+"
_SHAPES = (
    (re.compile(rf"posters/\d+/{_SEGMENT}\.jpg"), ONE_YEAR_CACHE_SECONDS),
    (re.compile(rf"posters-hd/\d+/{_SEGMENT}\.jpg"), ONE_YEAR_CACHE_SECONDS),
    (re.compile(rf"tmdb/w\d+/{_SEGMENT}"), ONE_YEAR_CACHE_SECONDS),
    (re.compile(r"posters/\d+\.jpg"), SEVEN_DAYS_CACHE_SECONDS),
    (re.compile(r"carousels/\d+/\d+\.jpg"), SEVEN_DAYS_CACHE_SECONDS),
    (re.compile(r"seasons/\d+/\d+\.jpg"), SEVEN_DAYS_CACHE_SECONDS),
)
# The first segment of every shape above: the prefixes a sweep may walk.
PREFIXES = ("posters/", "posters-hd/", "carousels/", "seasons/", "tmdb/")


def cache_seconds_for(name: str) -> int | None:
    """The max-age, in seconds, the object at *name* (a key inside the
    bucket) deserves; None for a shape this policy does not know."""
    for shape, seconds in _SHAPES:
        if shape.fullmatch(name or ""):
            return seconds
    return None
