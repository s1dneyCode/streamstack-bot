"""
Unit tests for the one title-logo chooser (RUN B4 · 0b): the lightest logo
TMDb offers, at or above the app's line.

Until B4 every writer kept the FIRST English logo TMDb lists, and for popular
titles that first one is often a dark variant while a light one exists (Dune:
Part Two 0.011, House of the Dragon 0.042): the app showed their text title
for the bot's choice, not for want of a logo. The rule now lives in
logo_luminance.choose_logo, reached by every writer through
TmdbClient.choose_title_logo; the line is logo_luminance.MIN_LOGO_LUMINANCE,
the app's kTitleLogoMinLuminance.

Fakes only: logos built in memory with Pillow out of opaque white and black
pixels, so their luminance over black is exact (1 white in 10 is 0.1); the
real luminance_for_logo measures them through an injected fetch that records
every URL it is asked for; a TMDb client whose _get answers an images
payload. No network. Run from the repo root:
    python -m unittest discover -s tests -t . -v
"""

import contextlib
import io
import types
import unittest

from PIL import Image

from bot import logo_luminance
from bot.logo_luminance import luminance_for_logo
from bot.tmdb import POSTER_BASE, TmdbClient


def _png(white: int, total: int) -> bytes:
    """A logo whose luminance over black is exactly white/total: `white`
    opaque white pixels, the rest opaque black, on a transparent ground."""
    width = 100
    img = Image.new("RGBA", (width, (total + width - 1) // width), (0, 0, 0, 0))
    px = img.load()
    for i in range(total):
        px[i % width, i // width] = (255, 255, 255, 255) if i < white else (0, 0, 0, 255)
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


# The luminances these tests need, exact.
LUM = {0.011: (11, 1000), 0.6: (6, 10), 0.10: (1, 10), 0.0999: (999, 10000), 0.047: (47, 1000),
       0.042: (42, 1000), 0.05: (5, 100), 0.0: (0, 10), 0.9: (9, 10)}


class _Cdn:
    """Stands in for requests.get: an image per file, an error for the rest;
    records every URL asked."""

    def __init__(self, logos: dict):
        # file -> luminance (a key of LUM), or None for a logo that cannot be measured (HTTP 500)
        self.logos, self.urls = dict(logos), []

    def __call__(self, url, timeout=None):
        self.urls.append(url)
        name = url.rsplit("/", 1)[-1]
        value = self.logos.get(name)
        if value is None:
            return types.SimpleNamespace(status_code=500, content=b"")
        return types.SimpleNamespace(status_code=200, content=_png(*LUM[value]))

    @property
    def files(self):
        return [u.rsplit("/", 1)[-1] for u in self.urls]


def _url(name: str) -> str:
    return f"{POSTER_BASE}/{name}"


def _choose(spec: list, known=None):
    """choose_logo over logos named a0, a1, … with the given luminances
    (None: unmeasurable), measured by the real luminance_for_logo."""
    names = [f"a{i}.png" for i in range(len(spec))]
    cdn = _Cdn(dict(zip(names, spec)))
    with contextlib.redirect_stdout(io.StringIO()):
        url, value = logo_luminance.choose_logo([_url(n) for n in names],
                                                measure=lambda u: luminance_for_logo(u, fetch=cdn), known=known)
    return (url.rsplit("/", 1)[-1] if url else None), value, cdn.files


# ── the line and the walk ─────────────────────────────────────────────────

class ChooseLogoTest(unittest.TestCase):

    def test_dunes_shape_the_light_second_wins_with_its_luminance(self):
        self.assertEqual(_choose([0.011, 0.6]), ("a1.png", 0.6, ["a0.png", "a1.png"]))

    def test_the_line_is_at_or_above_0_10(self):
        # A walk: 0.0999 is passed over, 0.10 wins.
        self.assertEqual(_choose([0.05, 0.0999, 0.10, 0.6])[:2], ("a2.png", 0.10))
        # The first candidate: 0.10 wins at once; 0.0999 sends the walk on.
        self.assertEqual(_choose([0.10, 0.6]), ("a0.png", 0.10, ["a0.png"]))
        self.assertEqual(_choose([0.0999, 0.6])[:2], ("a1.png", 0.6))
        self.assertEqual(logo_luminance.MIN_LOGO_LUMINANCE, 0.10)

    def test_a_first_candidate_that_cannot_be_measured_wins_and_nothing_else_is_fetched(self):
        self.assertEqual(_choose([None, 0.6, 0.9]), ("a0.png", None, ["a0.png"]))
        # Positive control: a known-dark first one does walk on.
        self.assertEqual(_choose([0.011, 0.6, 0.9])[:2], ("a1.png", 0.6))

    def test_an_unmeasurable_one_after_a_dark_first_is_passed_over(self):
        self.assertEqual(_choose([0.011, None, 0.6]), ("a2.png", 0.6, ["a0.png", "a1.png", "a2.png"]))

    def test_all_dark_the_lightest_wins_and_a_tie_keeps_the_earlier(self):
        self.assertEqual(_choose([0.011, 0.047, 0.042])[:2], ("a1.png", 0.047))      # not the first
        self.assertEqual(_choose([0.042, 0.047, 0.047])[:2], ("a1.png", 0.047))      # the earlier of the tie
        self.assertEqual(_choose([0.047, 0.042, 0.011])[:2], ("a0.png", 0.047))      # nothing lighter: the first
        self.assertEqual(_choose([0.0, 0.0])[:2], ("a0.png", 0.0))                   # a measured zero is a number

    def test_three_unmeasurable_in_a_row_end_the_walk(self):
        name, value, fetched = _choose([0.011, None, None, None, 0.6])
        self.assertEqual((name, value), ("a0.png", 0.011))
        self.assertEqual(fetched, ["a0.png", "a1.png", "a2.png", "a3.png"])       # the light one never fetched
        self.assertEqual(logo_luminance.UNMEASURED_IN_A_ROW_MAX, 3)
        # Positive controls: two in a row do not end it, and a measured one resets the count.
        self.assertEqual(_choose([0.011, None, None, 0.6])[:2], ("a3.png", 0.6))
        self.assertEqual(_choose([0.011, None, None, 0.042, None, None, 0.6])[:2], ("a6.png", 0.6))

    def test_a_light_first_candidate_is_one_fetch(self):
        self.assertEqual(_choose([0.6, 0.9, 0.011]), ("a0.png", 0.6, ["a0.png"]))

    def test_no_candidate_is_no_logo_and_no_fetch(self):
        self.assertEqual(_choose([]), (None, None, []))

    def test_a_known_luminance_stands_in_for_its_fetch(self):
        # The dark backlog's stored luminance: no fetch for the stored URL.
        name, value, fetched = _choose([0.011, 0.6], known={_url("a0.png"): 0.011})
        self.assertEqual((name, value, fetched), ("a1.png", 0.6, ["a1.png"]))


# ── the pool, and the one call every writer makes ────────────────────────

def _tmdb(images: dict):
    """A real TmdbClient whose _get answers *images* and records the paths."""
    client = TmdbClient.__new__(TmdbClient)
    client.paths = []

    def _get(path, params=None, **kwargs):
        client.paths.append(path)
        if isinstance(images, BaseException):
            raise images
        return images

    client._get = _get
    return client


def _logo(name: str, lang="en") -> dict:
    return {"file_path": f"/{name}", "iso_639_1": lang}


class ChooseTitleLogoTest(unittest.TestCase):

    def _choose(self, images, lums, data=None):
        cdn = _Cdn(lums)
        tmdb = _tmdb(images)
        with contextlib.redirect_stdout(io.StringIO()):
            url, value = tmdb.choose_title_logo(tmdb_id=693134, media_type="movie", data=data,
                                                measure=lambda u: luminance_for_logo(u, fetch=cdn))
        return (url.rsplit("/", 1)[-1] if url else None), value, cdn.files, tmdb.paths

    def test_the_pool_is_the_english_logos_in_tmdbs_order(self):
        images = {"logos": [_logo("fr.png", "fr"), _logo("dark.png"), _logo("de.png", "de"), _logo("light.png")]}
        name, value, fetched, paths = self._choose(images, {"fr.png": 0.9, "dark.png": 0.011, "de.png": 0.9,
                                                            "light.png": 0.6})
        self.assertEqual((name, value), ("light.png", 0.6))
        self.assertEqual(fetched, ["dark.png", "light.png"])        # the other languages are not candidates
        self.assertEqual(paths, ["/movie/693134/images"])           # one images call

    def test_no_english_logo_the_same_rule_over_every_logo(self):
        images = {"logos": [_logo("fr.png", "fr"), _logo("xx.png", None), _logo("de.png", "de")]}
        name, value, fetched, _ = self._choose(images, {"fr.png": 0.011, "xx.png": 0.042, "de.png": 0.6})
        self.assertEqual((name, value, fetched), ("de.png", 0.6, ["fr.png", "xx.png", "de.png"]))

    def test_a_prefetched_payload_is_not_fetched_again(self):
        images = {"logos": [_logo("light.png")]}
        name, value, _, paths = self._choose(RuntimeError("must not fetch"), {"light.png": 0.6}, data=images)
        self.assertEqual((name, value, paths), ("light.png", 0.6, []))

    def test_no_logos_and_a_failed_images_call_are_no_logo(self):
        self.assertEqual(self._choose({"logos": []}, {})[:3], (None, None, []))
        name, value, fetched, paths = self._choose(RuntimeError("boom"), {})
        self.assertEqual((name, value, fetched, paths), (None, None, [], ["/movie/693134/images"]))

    def test_a_logo_without_a_file_path_is_not_a_candidate(self):
        images = {"logos": [{"iso_639_1": "en", "file_path": ""}, _logo("light.png")]}
        self.assertEqual(self._choose(images, {"light.png": 0.6})[:3], ("light.png", 0.6, ["light.png"]))

    def test_the_candidates_are_full_w500_urls_in_order(self):
        tmdb = _tmdb({"logos": [_logo("a.png"), _logo("b.svg"), _logo("c.png", "fr")]})
        self.assertEqual(tmdb.title_logo_candidates(tmdb_id=1, media_type="tv"),
                         [f"{POSTER_BASE}/a.png", f"{POSTER_BASE}/b.svg"])
        self.assertEqual(tmdb.paths, ["/tv/1/images"])

    def test_the_401_rule_is_not_swallowed(self):
        with self.assertRaises(SystemExit):
            self._choose(SystemExit(1), {})


if __name__ == "__main__":
    unittest.main()
