"""
Unit tests for upload_image's long cache (RUN B5.8 · 0a).

Every object in media-images carried cacheControl = no-cache (503,371 of them,
measured 27/10): upload_image sent no cache of its own, and the app's image
cache reads no-cache as zero validity. upload_image gains an optional
cache_seconds; without it the request is today's, byte for byte (every
existing caller unchanged); with it, the upload asks for that cache — a year
for the files whose keys are versioned by TMDb file.

The pin is the request that reaches the wire: a REAL SupabaseClient, offline
(any socket connect fails the test), whose request is caught at httpx's
transport — never storage3's internals, which change between versions. The
multipart boundary is random per request and is the only thing normalised.
Run from the repo root:
    python -m unittest discover -s tests -t . -v
"""

import contextlib
import re
import socket
import unittest
from unittest import mock

import httpx

from bot.posters_hd import ONE_YEAR_CACHE_SECONDS
from bot.supabase_client import SupabaseClient

FAKE_URL = "https://project.supabase.invalid"
FAKE_KEY = "fake-header.fake-payload.fake-signature"
BUCKET, PATH, DATA = "media-images", "tmdb/w185/abc123.jpg", b"\xff\xd8\xff\xe0fake-jpeg-bytes"


def _no_network(*args, **kwargs):
    raise AssertionError("a test opened a socket")


@contextlib.contextmanager
def _wire():
    """A real SupabaseClient whose every HTTP request is caught, read and
    answered 200 at httpx's transport."""
    caught = []

    def handle_request(self, request):
        request.read()
        caught.append(request)
        return httpx.Response(200, json={"Key": f"{BUCKET}/{PATH}"}, request=request)

    with mock.patch.object(socket.socket, "connect", _no_network), \
            mock.patch.object(socket, "getaddrinfo", _no_network), \
            mock.patch.object(socket, "create_connection", _no_network), \
            mock.patch.object(httpx.HTTPTransport, "handle_request", handle_request), \
            contextlib.redirect_stdout(None):
        yield SupabaseClient(url=FAKE_URL, key=FAKE_KEY), caught


def _normalised(request) -> tuple:
    """(method, url, headers, body), the random multipart boundary replaced."""
    boundary = re.search(r"boundary=([0-9A-Za-z]+)", request.headers["content-type"]).group(1)
    headers = tuple(sorted((k.lower(), v.replace(boundary, "BOUNDARY")) for k, v in request.headers.items()))
    return request.method, str(request.url), headers, request.content.replace(boundary.encode(), b"BOUNDARY")


def _form_field(request, name: str) -> str | None:
    """The value of a multipart form field, or None when the body has none."""
    match = re.search(rb'name="' + name.encode() + rb'"\r\n\r\n(.*?)\r\n', request.content, re.S)
    return match.group(1).decode() if match else None


class UploadImageCacheTest(unittest.TestCase):

    def test_a_call_without_the_cache_sends_todays_request_byte_for_byte(self):
        with _wire() as (db, caught):
            url = db.upload_image(BUCKET, PATH, DATA, "image/jpeg")
            # Today's call, as upload_image made it until B5.8.
            db.client.storage.from_(BUCKET).upload(path=PATH, file=DATA,
                                                  file_options={"content-type": "image/jpeg", "upsert": "true"})
        self.assertEqual(len(caught), 2)
        self.assertEqual(_normalised(caught[0]), _normalised(caught[1]))
        self.assertIsNone(_form_field(caught[0], "cacheControl"))   # the server stores no-cache, as today
        self.assertTrue(url.startswith(f"{FAKE_URL}/storage/v1/object/public/{BUCKET}/{PATH}"))

    def test_the_long_cache_reaches_the_request(self):
        with _wire() as (db, caught):
            url = db.upload_image(BUCKET, PATH, DATA, "image/jpeg", cache_seconds=ONE_YEAR_CACHE_SECONDS)
            db.upload_image(BUCKET, PATH, DATA, "image/jpeg")
        long, default = caught
        # The multipart field the server stores as the object's cacheControl, and the header.
        self.assertEqual(_form_field(long, "cacheControl"), "31536000")
        self.assertEqual(long.headers["cache-control"], "max-age=31536000")
        # Positive control: the call without it carries neither.
        self.assertIsNone(_form_field(default, "cacheControl"))
        self.assertNotEqual(default.headers.get("cache-control"), "max-age=31536000")
        # Everything else is the same request: the content type, the upsert, the path.
        self.assertEqual(long.headers["x-upsert"], "true")
        self.assertIn(b"Content-Type: image/jpeg", long.content)
        self.assertEqual(str(long.url), f"{FAKE_URL}/storage/v1/object/{BUCKET}/{PATH}")
        self.assertTrue(url.startswith(f"{FAKE_URL}/storage/v1/object/public/{BUCKET}/{PATH}"))

    def test_only_the_cache_differs_between_the_two_requests(self):
        with _wire() as (db, caught):
            db.upload_image(BUCKET, PATH, DATA, "image/jpeg", cache_seconds=ONE_YEAR_CACHE_SECONDS)
            db.upload_image(BUCKET, PATH, DATA, "image/jpeg")
        self.assertEqual(len(caught), 2)

        def without_the_cache(request):
            method, url, headers, body = _normalised(request)
            headers = tuple(h for h in headers if h[0] not in ("cache-control", "content-length"))
            body = re.sub(rb'--BOUNDARY\r\nContent-Disposition: form-data; name="cacheControl"\r\n\r\n[^\r]*\r\n', b"", body)
            return method, url, headers, body

        self.assertEqual(without_the_cache(caught[0]), without_the_cache(caught[1]))
        self.assertNotEqual(_normalised(caught[0]), _normalised(caught[1]))   # positive control: they did differ

    def test_one_year_is_31536000_seconds(self):
        self.assertEqual(ONE_YEAR_CACHE_SECONDS, 365 * 24 * 3600)


if __name__ == "__main__":
    unittest.main()
