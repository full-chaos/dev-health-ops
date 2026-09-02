"""The Go test harness must run the ClickHouse build production runs.

WHY THIS EXISTS (CHAOS-4854)
----------------------------
The harness pulled `sha256:1d1f6508` = **26.6.1.1193**, one minor BELOW the
`26.7` floor, while `deploy/docker-compose/compose.production.yml` ran
`sha256:d7556a38` = **26.7.1.1315**. Nothing detected the gap because nothing
compared them: both are digest pins in different files, each individually
correct-looking.

A version gap is not cosmetic here. CHAOS-4549 measured a multi-arm
`JOIN ... ON (... OR ...)` accepted on 26.7 and rejected on 24.8 with
`Code: 403 Unsupported JOIN ON conditions`, under every analyzer setting. **A
version gap changes what SQL is accepted at all**, so a harness on a different
build than production is not proving the thing it exists to prove.

WHAT THIS ASSERTS, AND WHY AS A TEST
------------------------------------
That the two digests are equal. The rule was going to be recorded as a comment;
a comment cannot fail. This is the same distinction that produced CHAOS-4834's
vacuous green -- a stated invariant with no producer is indistinguishable from a
satisfied one.

DELIBERATELY NOT ASSERTED
-------------------------
The *version string*. Resolving a digest to `com.clickhouse.build.version`
requires a registry round trip, which would make this test network-dependent and
fail closed on an outage -- and the property that matters is equality of the two
pins, which is checkable offline. Note for anyone extending it: read
`com.clickhouse.build.version`, NOT `org.opencontainers.image.version`, which
reports the Ubuntu base (22.04) on every ClickHouse image.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
HARNESS = REPO_ROOT / "internal" / "testsupport" / "containers" / "harness.go"
PRODUCTION_COMPOSE = REPO_ROOT / "deploy" / "docker-compose" / "compose.production.yml"

_HARNESS_PIN = re.compile(
    r'ClickHouseImage\s*=\s*"clickhouse/clickhouse-server@(sha256:[0-9a-f]{64})"'
)
_COMPOSE_PIN = re.compile(
    r"image:\s*clickhouse/clickhouse-server[^@\s]*@(sha256:[0-9a-f]{64})"
)


def _harness_digest() -> str:
    match = _HARNESS_PIN.search(HARNESS.read_text(encoding="utf-8"))
    assert match, (
        f"no ClickHouseImage digest pin found in {HARNESS.name}. If the constant "
        "was renamed or the pin moved to a tag, this guard is silently dead -- "
        "update the pattern rather than deleting the test."
    )
    return match.group(1)


def _production_digest() -> str:
    match = _COMPOSE_PIN.search(PRODUCTION_COMPOSE.read_text(encoding="utf-8"))
    assert match, (
        f"no ClickHouse digest pin found in {PRODUCTION_COMPOSE.name}. If "
        "production stopped digest-pinning, this guard cannot compare and the "
        "harness has no reference to match -- decide deliberately, do not delete."
    )
    return match.group(1)


def test_both_pins_are_findable() -> None:
    """Guard the guard: neither side may go unreadable and pass vacuously.

    If a rename made either pattern match nothing, an equality assertion on two
    empty strings would pass. The helpers above assert rather than return None,
    and this exercises both so the failure is loud at the right place.
    """
    assert _harness_digest().startswith("sha256:")
    assert _production_digest().startswith("sha256:")


def test_harness_clickhouse_digest_matches_production() -> None:
    """The harness and production must pull the same ClickHouse build."""
    harness = _harness_digest()
    production = _production_digest()

    assert harness == production, (
        "the Go test harness and production run DIFFERENT ClickHouse builds:\n"
        f"  harness    {HARNESS.relative_to(REPO_ROOT)}: {harness}\n"
        f"  production {PRODUCTION_COMPOSE.relative_to(REPO_ROOT)}: {production}\n"
        "The harness exists to prove parity against production's engine, and a "
        "version gap changes what SQL is accepted (CHAOS-4549: a multi-arm "
        "`JOIN ... ON (... OR ...)` is accepted on 26.7 and rejected on 24.8). "
        "Bump the harness to production's digest, or -- if production moved "
        "deliberately and the harness should lag -- record that decision here "
        "rather than deleting this assertion.\n"
        "Resolve either digest's version with `com.clickhouse.build.version`; "
        "`org.opencontainers.image.version` reports the Ubuntu base (22.04)."
    )
