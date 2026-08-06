#!/usr/bin/env python3
"""Runs INSIDE the ``ask-dev-acceptance-api`` container, never on the host.

Invoked by ``db_verify.verify_world_digest_via_exec`` as ``docker compose
... exec -T api python -m scripts.acceptance.corpus._inner_world_digest
--manifest ... --sink ... --postgres-uri ...`` (team-lead ruling
2026-08-06: WORLD_DIGEST verification, ruling D2, is one of exactly two
harness concerns allowed through the container boundary -- see
``compose_context.py``'s module docstring).

Thin CLI wrapper over ``scripts.acceptance.corpus.world_digest_guard.
verify_world_digest`` (which is itself a thin wrapper over
``dev_health_ops.fixtures.world``) -- reused rather than reimplemented, so
this script and the pure-Python unit-tested comparison logic
(``require_world_digest_match``) can never quietly diverge. Prints exactly
ONE JSON line to stdout; all diagnostics go to stderr.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys

from scripts.acceptance.corpus.world_digest_guard import verify_world_digest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--sink", required=True)
    parser.add_argument("--postgres-uri", required=True)
    parser.add_argument("--digest-path", default=None)
    args = parser.parse_args(argv)

    try:
        verification = asyncio.run(
            verify_world_digest(
                args.manifest,
                sink=args.sink,
                postgres_uri=args.postgres_uri,
                digest_path=args.digest_path,
            )
        )
    except Exception as exc:  # noqa: BLE001 -- reported as structured JSON, not a traceback the host must scrape
        print(f"world digest verification raised: {exc}", file=sys.stderr)
        return 1

    print(
        json.dumps(
            {
                "pinned_digest": verification.pinned_digest,
                "live_digest": verification.live_digest,
                "matched": verification.matched,
                "drifted_components": list(verification.drifted_components),
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
