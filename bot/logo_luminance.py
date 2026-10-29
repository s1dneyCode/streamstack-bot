"""
The one definition of a title logo's luminance over black — and, since B4,
of which logo a title gets.

The app draws title logos on a black ground, and its detail screen falls back
to the text title when a logo's relative luminance over that ground is below
MIN_LOGO_LUMINANCE. The app cannot decode an image in its test suite, so the
measure is data: the bot computes it here and writes it to
media.title_logo_luminance beside title_logo_url, and the app reads a number.
Every writer of that column calls luminance_for_logo(); there is no second
formula anywhere. Every writer of title_logo_url chooses through
choose_logo(); there is no second rule anywhere.
"""

import io
import re

import requests
from PIL import Image

# The line the app draws a title logo at: its detail screen shows the logo
# when the luminance over black is at least this, the text title otherwise —
# kTitleLogoMinLuminance in the app's
# lib/features/detail/presentation/screens/detail_screen.dart (the row 7
# rule). The two move together: change one, change the other, and dispatch
# the dark-logos backfill again.
MIN_LOGO_LUMINANCE = 0.10
# After a known-dark first candidate, this many unmeasurable logos in a row
# end the walk: the CDN is failing, and each one more is a fetch for nothing.
UNMEASURED_IN_A_ROW_MAX = 3


def luminance_of_image(data: bytes) -> float | None:
    """Mean relative luminance of a logo composited over black, 0..1.

    WHY OVER BLACK: black is the ground the app draws the logo on, and the
    board's threshold is stated against it. A pixel at alpha a with colour
    (r, g, b) shows as a/255 × L(r, g, b) over black, so a half-transparent
    white reads as grey (0.502), not as white. That is the compositing, and
    it is why this is not an alpha-WEIGHTED mean (which would say 1.0).

    L is the Rec. 709 relative luminance over LINEARISED sRGB:
        L = 0.2126 R + 0.7152 G + 0.0722 B,
    each channel c = value/255 through the sRGB transfer:
        c/12.92 below 0.04045, else ((c + 0.055)/1.055) ** 2.4.

    The mean runs over the pixels whose alpha is greater than zero: the
    fully transparent ground around a logo is not part of what is read.
    Returns None when no pixel has alpha (nothing to read) or the bytes do
    not decode. No numpy: the loop over a 300-wide logo is small.
    """
    try:
        with Image.open(io.BytesIO(data)) as img:
            if img.mode.startswith("I"):
                # 16-bit grey (mode I;16): convert("RGBA") would CLIP every
                # value above 255 to white, so a dark logo would read as 1.0.
                # Scale to 8 bits first, carrying a tRNS key to the nearest
                # 8-bit step.
                key = img.info.get("transparency")
                grey = img.convert("I").point(lambda v: v * (1 / 257) + 0.5).convert("L")
                if isinstance(key, int):
                    grey.info["transparency"] = round(key / 257)
                rgba = grey.convert("RGBA")
            else:
                rgba = img.convert("RGBA")
    except Exception:
        return None

    # The sRGB transfer, once per 8-bit value.
    linear = [
        c / 12.92 if c < 0.04045 else ((c + 0.055) / 1.055) ** 2.4
        for c in (v / 255 for v in range(256))
    ]

    pixels = rgba.tobytes()
    total = 0.0
    visible = 0
    for r, g, b, a in zip(pixels[0::4], pixels[1::4], pixels[2::4], pixels[3::4]):
        if a:
            total += a / 255 * (0.2126 * linear[r] + 0.7152 * linear[g] + 0.0722 * linear[b])
            visible += 1
    return total / visible if visible else None


def luminance_for_logo(url: str, fetch=requests.get) -> float | None:
    """The luminance of the logo at `url`, or None if it cannot be measured.

    An .svg answers 1.0 without fetching: the app tints SVG logos white. A
    raster is fetched at TMDb's w300 rendition when the URL carries a TMDb
    size segment (/w500/ → /w300/; the STORED URL never changes, this only
    picks what is downloaded), with a 10-second timeout, and measured by
    luminance_of_image().

    Never raises. A non-200, a timeout, any exception, or bytes with nothing
    to read answer None with one log line `[LUMINANCE] <url>: <reason>`: a
    logo that cannot be measured now is measured by the next run, and must
    not crash the writer that called this.
    """
    if url.split("?", 1)[0].lower().endswith(".svg"):
        return 1.0

    fetch_url = re.sub(r"/t/p/(?:w\d+|h\d+|original)/", "/t/p/w300/", url, count=1)
    try:
        response = fetch(fetch_url, timeout=10)
        status, body = response.status_code, response.content
    except Exception as exc:
        print(f"[LUMINANCE] {url}: fetch failed ({type(exc).__name__})")
        return None
    if status != 200:
        print(f"[LUMINANCE] {url}: HTTP {status}")
        return None

    value = luminance_of_image(body)
    if value is None:
        print(f"[LUMINANCE] {url}: nothing to read (undecodable, or no visible pixel)")
    return value


def choose_logo(candidates: list[str], measure=luminance_for_logo,
                known: dict[str, float] | None = None) -> tuple[str | None, float | None]:
    """The title logo to write, and its luminance, out of *candidates* in
    TMDb's order (B4) — the lightest TMDb offers, at or above the app's line.

    The first candidate is measured. At or above MIN_LOGO_LUMINANCE, or
    unmeasurable (None), it wins exactly as before B4: one fetch, the same
    answer. Known dark: the rest are walked in order and the first one
    measured at or above the line wins. An unmeasurable one is passed over —
    a known-dark logo shows the text title, an unknown one could be an
    invisible logo — and UNMEASURED_IN_A_ROW_MAX of them in a row end the
    walk. If nothing wins, the lightest measured one does, TMDb's order
    breaking a tie, so the first stays when nothing is lighter.

    The winner comes back with its luminance, so the writer writes both keys
    in one payload and no URL is fetched twice. *known* maps a URL to a
    luminance already measured (the stored one), which stands in for its
    fetch. No candidates: (None, None), and nothing is fetched."""
    if not candidates:
        return None, None
    known = known or {}

    def luminance(url):
        return known[url] if url in known else measure(url)

    first = candidates[0]
    best, best_value = first, luminance(first)
    if best_value is None or best_value >= MIN_LOGO_LUMINANCE:
        return best, best_value
    unmeasured = 0
    for url in candidates[1:]:
        value = luminance(url)
        if value is None:
            unmeasured += 1
            if unmeasured >= UNMEASURED_IN_A_ROW_MAX:
                break
            continue
        unmeasured = 0
        if value >= MIN_LOGO_LUMINANCE:
            return url, value
        if value > best_value:
            best, best_value = url, value
    return best, best_value
