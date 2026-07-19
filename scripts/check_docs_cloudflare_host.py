#!/usr/bin/env python3
"""Smoke-test the deployed documentation host and one legacy redirect."""

from __future__ import annotations

import argparse
import csv
import sys
from dataclasses import dataclass
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlparse
from urllib.request import HTTPRedirectHandler, Request, build_opener


@dataclass(frozen=True)
class ResponseSummary:
    status: int
    headers: dict[str, str]
    body: bytes


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _request(url: str, *, follow_redirects: bool = True) -> ResponseSummary:
    request = Request(url, headers={"User-Agent": "dev-health-docs-smoke/1"})
    opener = build_opener() if follow_redirects else build_opener(NoRedirect())
    try:
        with opener.open(request, timeout=20) as response:
            return ResponseSummary(
                status=response.status,
                headers={key.lower(): value for key, value in response.headers.items()},
                body=response.read(),
            )
    except HTTPError as exc:
        return ResponseSummary(
            status=exc.code,
            headers={key.lower(): value for key, value in exc.headers.items()},
            body=exc.read(),
        )


def _first_redirect(path: Path) -> tuple[str, str]:
    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        for row in reader:
            source = (row.get("source_path") or "").strip()
            target = (row.get("target_path") or "").strip()
            if source and target:
                return source, target
    raise ValueError(f"redirect manifest has no rows: {path}")


def _origin(value: str) -> str:
    parsed = urlparse(value)
    if parsed.scheme != "https" or not parsed.netloc:
        raise ValueError(f"expected an HTTPS origin: {value}")
    return f"{parsed.scheme}://{parsed.netloc}/"


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Smoke-test a deployed documentation host."
    )
    parser.add_argument("--origin", required=True)
    parser.add_argument("--redirects", type=Path, required=True)
    args = parser.parse_args(argv)

    try:
        origin = _origin(args.origin)
        redirect_source, redirect_target = _first_redirect(args.redirects)
    except (OSError, ValueError, csv.Error) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    errors: list[str] = []
    checks = {
        "/": 200,
        "/use/": 200,
        "/reference/metrics/weighting-and-aggregation/": 200,
        "/phase-11-intentional-missing-page/": 404,
    }
    home: ResponseSummary | None = None

    try:
        for path, expected_status in checks.items():
            result = _request(urljoin(origin, path.lstrip("/")))
            if path == "/":
                home = result
            if result.status != expected_status:
                errors.append(
                    f"{path}: expected HTTP {expected_status}, received {result.status}"
                )

        if home is not None:
            if home.headers.get("x-content-type-options", "").lower() != "nosniff":
                errors.append("/: X-Content-Type-Options is not nosniff")
            if (
                home.headers.get("referrer-policy", "").lower()
                != "strict-origin-when-cross-origin"
            ):
                errors.append(
                    "/: Referrer-Policy is not strict-origin-when-cross-origin"
                )
            if "noindex" in home.headers.get("x-robots-tag", "").lower():
                errors.append(
                    "/: production response unexpectedly has X-Robots-Tag noindex"
                )
            if b"Dev Health" not in home.body:
                errors.append(
                    "/: response body does not contain the Dev Health site identity"
                )

        redirect_result = _request(
            urljoin(origin, redirect_source.lstrip("/")), follow_redirects=False
        )
        if redirect_result.status not in {301, 308}:
            errors.append(
                f"{redirect_source}: expected a permanent redirect, received "
                f"{redirect_result.status}"
            )
        location = redirect_result.headers.get("location", "")
        expected_location = urljoin(origin, redirect_target.lstrip("/"))
        actual_location = urljoin(origin, location)
        if actual_location.rstrip("/") != expected_location.rstrip("/"):
            errors.append(
                f"{redirect_source}: expected Location {expected_location}, found {location}"
            )
    except (URLError, TimeoutError, OSError) as exc:
        errors.append(f"request failed: {exc}")

    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1

    print(
        "Cloudflare host smoke checks passed for canonical pages, 404 behavior, "
        "security headers, and a legacy redirect"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
