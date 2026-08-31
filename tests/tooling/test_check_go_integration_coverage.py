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
    # CHAOS-4506 added cmd/query-api/internal/analytics (32 -> 33): the
    # NaN-class live proof (nan_class_live_test.go) is proved against a
    # real ClickHouse container -- the analytics package's first
    # -tags integration file.
    # CHAOS-4643 denylisted cmd/query-api/internal/analytics (discovered
    # count stays 33; denylisted 0 -> 1, will-run 33 -> 32). CI's
    # integration-shard job gives every other package its own ClickHouse via
    # testcontainers, but nan_class_live_test.go dials an externally supplied
    # CLICKHOUSE_URI instead (mirroring the Python dual-run slot harness) and
    # .github/workflows/go.yml never sets that var -- the enrolled test
    # skipped on every CI run and the skip reported as a pass. It is a
    # discretionary, slot-only proof per orchestrator ruling 2026-08-29 (see
    # the file's own STATUS header); denylisting it stops the shard from
    # implying coverage it structurally cannot deliver.
    assert "33 package(s) discovered, 1 denylisted, 32 will run" in result.stdout
    # Name the denylisted package explicitly (SET DIFFERENCE), not just the
    # count -- a bare count is exactly what let CHAOS-4643's own literal drift
    # 31 -> 32 -> 33 unnoticed.
    assert "  SKIP cmd/query-api/internal/analytics: " in result.stdout
    assert "  RUN  cmd/query-api/internal/analytics" not in result.stdout
