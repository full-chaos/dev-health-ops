from __future__ import annotations

import httpx

import httpx2


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


def make_hardened_async_httpx2_client() -> httpx2.AsyncClient:
    return httpx2.AsyncClient(
        follow_redirects=False,
        trust_env=False,
        timeout=60.0,
    )


def make_hardened_httpx2_client() -> httpx2.Client:
    return httpx2.Client(
        follow_redirects=False,
        trust_env=False,
        timeout=60.0,
    )
