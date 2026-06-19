from __future__ import annotations

import httpx


def make_hardened_async_httpx_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        follow_redirects=False,
        trust_env=False,
        timeout=60.0,
    )


def make_hardened_httpx_client() -> httpx.Client:
    return httpx.Client(
        follow_redirects=False,
        trust_env=False,
        timeout=60.0,
    )
