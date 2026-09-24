"""Venue-only Python start-up hook for the credential connection test oracle.

When VENUE_PROVIDER_STUB_PORT is set, the Python plane resolves a few
provider hostnames to fixed public addresses (so the real
_validate_external_url runs its real checks without DNS) and sends every
request for those hosts to a stub on 127.0.0.1:<port>, whichever HTTP library
made it (httpx for the probes and the LaunchDarkly connector, requests for the
GitHub App token exchange). Nothing in src/ is touched.
"""

import os
import socket

_PORT = os.environ.get("VENUE_PROVIDER_STUB_PORT")

if _PORT:
    _FAKE_ADDRESSES = {
        "provider.example.test": "93.184.216.34",
        "api.github.com": "140.82.112.5",
        "gitlab.com": "172.65.251.78",
    }
    _real_getaddrinfo = socket.getaddrinfo

    def _getaddrinfo(host, *args, **kwargs):
        if host in _FAKE_ADDRESSES:
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (_FAKE_ADDRESSES[host], 0))]
        return _real_getaddrinfo(host, *args, **kwargs)

    socket.getaddrinfo = _getaddrinfo

    _STUBBED_HOSTS = {
        "provider.example.test",
        "api.github.com",
        "gitlab.com",
        "api.linear.app",
        "app.launchdarkly.com",
    }

    import httpx

    class _StubTransport(httpx.AsyncHTTPTransport):
        async def handle_async_request(self, request):
            if request.url.host in _STUBBED_HOSTS:
                request.url = request.url.copy_with(scheme="http", host="127.0.0.1", port=int(_PORT))
            return await super().handle_async_request(request)

    _orig_init = httpx.AsyncClient.__init__

    def _init(self, *args, **kwargs):
        kwargs.setdefault("transport", _StubTransport())
        _orig_init(self, *args, **kwargs)

    httpx.AsyncClient.__init__ = _init

    import requests
    from urllib.parse import urlsplit, urlunsplit

    _orig_send = requests.Session.send

    def _send(self, prepared, **kwargs):
        parts = urlsplit(prepared.url)
        if parts.hostname in _STUBBED_HOSTS:
            prepared.url = urlunsplit(("http", f"127.0.0.1:{_PORT}", parts.path, parts.query, parts.fragment))
        return _orig_send(self, prepared, **kwargs)

    requests.Session.send = _send
