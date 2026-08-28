"""Regression coverage for the Go integration-package inventory gate."""

from __future__ import annotations

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_integration_coverage_inventory_completes_and_stays_nonempty() -> None:
    """A large inventory must not deadlock before the first package is printed.

    Bash implements a here-string as a producer that fills a pipe before the
    consumer starts. In environments whose pipe capacity is smaller than this
    repository's inventory, that producer blocks forever. Running the public
    discovery verb exercises the real inventory size and the same shell seam
    used by ``local_validate.sh``.
    """

    result = subprocess.run(
        ["bash", "ci/check_go.sh", "integration-coverage"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert "  RUN  internal/providersync" in result.stdout
    # CHAOS-3092 P0 added internal/testsupport/computeparity (27 -> 28): the
    # whole-table parity harness is integration-tagged because it provisions
    # two migrated scratch stores in a real container.
    # CHAOS-4194 added internal/streamhandlers (28 -> 29): the project
    # membership sink is proved against the real migration chain in a real
    # container, so the package grew its first -tags integration file.
    # CHAOS-4226 added internal/cacheinvalidation (29 -> 30): the per-org
    # cache epoch bump is proved against a real Valkey container.
    # CHAOS-4366 added cmd/query-api/internal/routeswitch (30 -> 31): the
    # go_api_registry-backed PostgresSwitch is proved against a real
    # Postgres testcontainer.
    # CHAOS-4367 added cmd/query-api (31 -> 32): the featureFlags Wave-1
    # canary's HTTP-level reachability test
    # (query_route_integration_test.go) is proved against a real Postgres
    # testcontainer + the real gqlgen/routeswitch/PostgresSwitch wiring.
    assert "32 package(s) discovered, 0 denylisted, 32 will run" in result.stdout
