"""
The one definition of a title logo's luminance over black.

The app draws title logos on a black ground, and the board's rule (E188 ·
T1-06 § 6a, case D) falls back to the text title when a logo's relative
luminance over that ground is below 0.25. The app cannot decode an image
in its test suite, so the measure is data: the bot computes it here and
writes it to media.title_logo_luminance beside title_logo_url, and the app
reads a number. Every writer of that column calls luminance_for_logo();
there is no second formula anywhere.
"""

import io
import re

import requests
from PIL import Image


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
