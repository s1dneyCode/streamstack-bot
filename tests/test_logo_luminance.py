"""
Unit tests for bot.logo_luminance — the one definition of a title logo's
luminance over black.

No network and no fixtures on disk: every PNG is generated here by Pillow,
and `fetch` is a fake that records what it was asked and returns a canned
response. The URLs use .invalid hosts, so even a missed fake cannot reach
the network. Run from the repo root:
    python -m unittest discover -s tests -t . -v
"""

import contextlib
import io
import unittest

import requests
from PIL import Image

from bot.logo_luminance import luminance_for_logo, luminance_of_image

# Tolerances, stated once. Values are pinned to the 4th decimal (the
# board's numbers); the alpha-128 case to 0.005 because 128/255 = 0.50196.
TOL = 0.001
TOL_ALPHA = 0.005

PNG_URL = "https://image.tmdb.invalid/t/p/w500/logo.png"
PNG_URL_W300 = "https://image.tmdb.invalid/t/p/w300/logo.png"
SVG_URL = "https://image.tmdb.invalid/t/p/w500/logo.svg"


def _png(color, size=(8, 8)) -> bytes:
    """An RGBA PNG of one colour, as bytes."""
    buf = io.BytesIO()
    Image.new("RGBA", size, color).save(buf, format="PNG")
    return buf.getvalue()


def _half_alpha_white_on_transparent() -> bytes:
    """8×8, fully transparent, with a 4×4 block of white at alpha 128."""
    img = Image.new("RGBA", (8, 8), (0, 0, 0, 0))
    img.paste(Image.new("RGBA", (4, 4), (255, 255, 255, 128)), (2, 2))
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


WHITE = _png((255, 255, 255, 255))


class _Response:
    def __init__(self, status_code: int = 200, content: bytes = b""):
        self.status_code = status_code
        self.content = content


class _FakeFetch:
    """Stands in for requests.get: records (url, kwargs), answers canned."""

    def __init__(self, response: _Response | None = None, error: Exception | None = None):
        self.response = response
        self.error = error
        self.calls = []

    def __call__(self, url, **kwargs):
        self.calls.append((url, kwargs))
        if self.error is not None:
            raise self.error
        return self.response


def _measure(url: str, fetch: _FakeFetch) -> tuple[float | None, str]:
    """luminance_for_logo(url, fetch), plus everything it printed."""
    out = io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
        value = luminance_for_logo(url, fetch=fetch)
    return value, out.getvalue()


class _NearTestCase(unittest.TestCase):

    def assertNear(self, value, expected: float, tol: float):
        """A float within `tol` of `expected` — never assertEqual on a float."""
        self.assertIsInstance(value, float, f"expected a number near {expected}, got {value!r}")
        self.assertAlmostEqual(value, expected, delta=tol)


class LuminanceOfImageTest(_NearTestCase):

    def test_opaque_white_is_one(self):
        self.assertNear(luminance_of_image(WHITE), 1.0, TOL)

    def test_opaque_black_is_zero(self):
        self.assertNear(luminance_of_image(_png((0, 0, 0, 255))), 0.0, TOL)

    def test_opaque_pure_red_is_its_rec709_weight(self):
        self.assertNear(luminance_of_image(_png((255, 0, 0, 255))), 0.2126, TOL)

    def test_srgb_grey_128_is_linearised(self):
        # The naive (unlinearised) answer would be 128/255 = 0.502.
        self.assertNear(
            luminance_of_image(_png((128, 128, 128, 255))), 0.2158, TOL
        )

    def test_half_alpha_white_is_composited_over_black(self):
        # Composited over black: 128/255 × 1.0 = 0.502. An alpha-WEIGHTED
        # mean would answer 1.0; counting the transparent ground would
        # answer 0.125. Both must miss.
        self.assertNear(
            luminance_of_image(_half_alpha_white_on_transparent()), 0.502, TOL_ALPHA
        )

    def test_16_bit_grey_is_scaled_not_clipped(self):
        # Pillow opens a 16-bit greyscale PNG as mode I;16, and its
        # convert("RGBA") CLIPS every value above 255 to white — a dark logo
        # would read as 1.0, the one wrong answer this column must never give.
        def grey16(value: int) -> bytes:
            buf = io.BytesIO()
            Image.new("I;16", (8, 8), value).save(buf, format="PNG")
            return buf.getvalue()

        self.assertNear(luminance_of_image(grey16(0x8080)), 0.2158, TOL)  # 128·257
        self.assertNear(luminance_of_image(grey16(0x2000)), 0.0144, TOL)  # ≈ 8-bit 32
        self.assertNear(luminance_of_image(grey16(0xFFFF)), 1.0, TOL)
        # Control: the same grey at 8 bits reads the same number.
        buf = io.BytesIO()
        Image.new("L", (8, 8), 128).save(buf, format="PNG")
        self.assertNear(luminance_of_image(buf.getvalue()), 0.2158, TOL)

    def test_16_bit_grey_keeps_its_transparency_key(self):
        # A white 4×4 on a black 8×8 ground; with tRNS = black the ground is
        # transparent and drops out of the mean, without it the ground counts.
        img = Image.new("I;16", (8, 8), 0)
        img.paste(Image.new("I;16", (4, 4), 0xFFFF), (2, 2))
        keyed, opaque = io.BytesIO(), io.BytesIO()
        img.save(keyed, format="PNG", transparency=0)
        img.save(opaque, format="PNG")

        self.assertNear(luminance_of_image(keyed.getvalue()), 1.0, TOL)
        self.assertNear(luminance_of_image(opaque.getvalue()), 16 / 64, TOL)  # control

    def test_fully_transparent_image_is_none(self):
        self.assertIsNone(luminance_of_image(_png((255, 255, 255, 0))))
        # Control: the same image made opaque has something to read.
        self.assertNear(luminance_of_image(_png((255, 255, 255, 255))), 1.0, TOL)

    def test_garbage_bytes_are_none(self):
        self.assertIsNone(luminance_of_image(b"definitely not an image"))
        self.assertIsNone(luminance_of_image(b""))
        # Control: real image bytes decode.
        self.assertNear(luminance_of_image(WHITE), 1.0, TOL)


class LuminanceForLogoTest(_NearTestCase):

    def test_svg_answers_one_without_fetching(self):
        fetch = _FakeFetch(_Response(200, WHITE))

        value, out = _measure(SVG_URL, fetch)

        self.assertNear(value, 1.0, TOL)
        self.assertEqual(fetch.calls, [])
        self.assertEqual(out, "")  # a success logs nothing
        # Control: the same fake IS called for a raster logo, so an empty
        # call list above means "not fetched", not "the fake is deaf"; and a
        # failing raster does log, so the empty output is not a deaf capture.
        _measure(PNG_URL, fetch)
        self.assertEqual(len(fetch.calls), 1)
        _, out = _measure(PNG_URL, _FakeFetch(_Response(404, b"")))
        self.assertEqual(len(out.splitlines()), 1)

    def test_tmdb_w500_is_fetched_as_w300_with_a_10_second_timeout(self):
        fetch = _FakeFetch(_Response(200, WHITE))

        value, out = _measure(PNG_URL, fetch)

        self.assertEqual(fetch.calls, [(PNG_URL_W300, {"timeout": 10})])
        self.assertNear(value, 1.0, TOL)
        self.assertEqual(out, "")
        # Control: the capture does see a line when the same fetch fails.
        _, out = _measure(PNG_URL, _FakeFetch(_Response(500, b"")))
        self.assertEqual(len(out.splitlines()), 1)

    def test_url_without_a_size_segment_is_fetched_as_is(self):
        url = "https://cdn.invalid/logos/abc.png"
        fetch = _FakeFetch(_Response(200, WHITE))

        _measure(url, fetch)

        self.assertEqual(fetch.calls, [(url, {"timeout": 10})])

    def test_404_is_none_with_one_log_line(self):
        value, out = _measure(PNG_URL, _FakeFetch(_Response(404, b"")))

        self.assertIsNone(value)
        lines = out.splitlines()
        self.assertEqual(len(lines), 1, out)
        self.assertTrue(lines[0].startswith(f"[LUMINANCE] {PNG_URL}: "), lines[0])
        self.assertIn("404", lines[0])
        # Control: the same URL answering 200 measures, and logs nothing.
        value, out = _measure(PNG_URL, _FakeFetch(_Response(200, WHITE)))
        self.assertNear(value, 1.0, TOL)
        self.assertEqual(out, "")

    def test_a_raise_inside_fetch_is_none_not_a_raise(self):
        for error in (requests.Timeout("read timed out"), RuntimeError("boom")):
            with self.subTest(error=type(error).__name__):
                value, out = _measure(PNG_URL, _FakeFetch(error=error))

                self.assertIsNone(value)
                lines = out.splitlines()
                self.assertEqual(len(lines), 1, out)
                self.assertTrue(lines[0].startswith(f"[LUMINANCE] {PNG_URL}: "), lines[0])
        # Control: a fetch that does not raise measures.
        value, _ = _measure(PNG_URL, _FakeFetch(_Response(200, WHITE)))
        self.assertNear(value, 1.0, TOL)

    def test_undecodable_body_is_none_with_one_log_line(self):
        value, out = _measure(PNG_URL, _FakeFetch(_Response(200, b"<html>oops</html>")))

        self.assertIsNone(value)
        lines = out.splitlines()
        self.assertEqual(len(lines), 1, out)
        self.assertTrue(lines[0].startswith(f"[LUMINANCE] {PNG_URL}: "), lines[0])
        # Control: a decodable body measures.
        value, _ = _measure(PNG_URL, _FakeFetch(_Response(200, _png((255, 0, 0, 255)))))
        self.assertNear(value, 0.2126, TOL)


if __name__ == "__main__":
    unittest.main()
