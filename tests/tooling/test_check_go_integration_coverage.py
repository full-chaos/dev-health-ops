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
    # CHAOS-4684 added cmd/query-api/internal/hotspots (33 -> 34 discovered,
    # 32 -> 33 will run): the argMax(<col>, (day, computed_at)) tie-break
    # regression guard runs against a real ClickHouse container.
    # CHAOS-4730 un-denylisted cmd/query-api/internal/analytics (34
    # discovered unchanged, 33 -> 34 will run, 1 -> 0 denylisted): the
    # CHAOS-4643 premise (this package's ONLY integration file could never
    # run in CI) no longer holds -- the SETTINGS max_execution_time =
    # {timeout:UInt64} bound-parameter defect was fixed package-wide, and
    # the package now has two REAL Testcontainers-backed regression tests
    # (breakdown_seeded_integration_test.go,
    # investmentquality_seeded_integration_test.go). nan_class_live_test.go
    # and investmentquality_live_test.go stay deliberately opt-in-live,
    # each skipping with a message naming the env var it needs -- that
    # pattern is not what CHAOS-4643 objected to; its complaint was a
    # package whose ENTIRE integration coverage was a permanent, silent
    # skip.
    # CHAOS-4655 added cmd/query-api/internal/workgraph (34 -> 35
    # discovered, 34 -> 35 will run): the batch-membership pair-bound-match
    # fix needed a real-engine red/green proof against a real ClickHouse
    # container.
    # CHAOS-4441 added internal/jobs/investment/chquery (35 -> 36
    # discovered, 35 -> 36 will run): the ClickHouse read side of the native
    # investment materializer. Its correctness claims -- dedup-before-filter
    # on a ReplacingMergeTree, type-exact scans of six nullable columns, and
    # tz-naive vs tz-aware timestamps landing on one instant -- are properties
    # of the engine, so a fake connection cannot fail them. Weight 122s,
    # ceil() of a local run of all seven tests together (each starting its own
    # container).
    # CHAOS-4766 added internal/jobs/workgraph/edges (36 -> 37 discovered,
    # 36 -> 37 will run): the native issue<->issue edge derivation got its
    # first -tags integration file. writeorder_integration_test.go proves the
    # work_graph_edges ReplacingMergeTree collapse -- Python's confidence=1.0
    # and this port's variant-C 0.9 are the SAME row, because the sorting key
    # excludes confidence -- against the real migration chain in a real
    # container, asserting BOTH write orders so a pre-step regression cannot
    # pass. Manifest weight 20s (see ci/go_integration_shards.tsv header).
    # TWO packages arrived independently, each written as 37 -> 38 on its own
    # branch: CHAOS-4882 added internal/storage/postgres/authschema (the
    # auth-owned lineage's live-PostgreSQL posture suite, which connects AS
    # the runtime role to prove DDL and cross-schema access are refused), and
    # CHAOS-4769 added internal/jobs/workgraph/issueprlinks (a container-backed
    # provenance-collision acceptance test, the package's first -tags
    # integration file). Neither branch was wrong; the merged total was 39.
    # CHAOS-4441 then added internal/jobs/investment/chwrite: 39 -> 40.
    # CHAOS-4977 and CHAOS-4902 landed independently, each written as
    # 40 -> 41 on its own branch: CHAOS-4977's
    # cmd/query-api/internal/investmentexplain and CHAOS-4902's
    # internal/testsupport/chschema (the RMT sweep's own authoritative-count
    # integration test). Merged total: 42.
    # CHAOS-4989 and CHAOS-4897 landed independently, each written as
    # 42 -> 43 on its own branch: CHAOS-4989's internal/llmorgsettings (the
    # org-scoped BYO LLM settings read path's own TestResolveUsableProvider_
    # PrecedenceMatrix/TestCredentials_SourceBound, run against a real
    # Postgres container -- feature_flags/org_feature_overrides/
    # org_licenses/organizations/settings schema) and CHAOS-4897's
    # internal/teamownership (the owned-repo membership helper the
    # recommendations loader's team-scoping join reads). Merged total: 44.
    # CHAOS-5006 PR2 added internal/jobs/investment/categorize (44 -> 45
    # discovered, 44 -> 45 will run): TestResolveProviderKindForOrg_
    # UsesRealOrgSettings proves the org-BYO precedence end-to-end against
    # llmorgsettings.Store over a real Postgres container.
    # CHAOS-4924 added internal/jobs/workgraph/operationaledges (45 -> 46
    # discovered, 45 -> 46 will run): the native operational-incident/
    # flag-guards edge producer's five Testcontainers-backed tests (the
    # org-70d529e0 and synthetic golden replays, plus three targeted
    # regression tests) run against a real migration chain, same
    # container-per-test shape as internal/jobs/workgraph/edges.
    # This is why the literal is followed by SET MEMBERSHIP assertions below --
    # a count alone cannot tell you WHICH package a merge dropped.
    # CHAOS-4290 and CHAOS-4924 landed independently, each written as 45 -> 46
    # on its own branch: internal/jobs/metrics/daily/icfinalize
    # (TestARedriveSupersedesInsteadOfAccumulating proves the native
    # ic_finalize writer is idempotent under redrive against a real
    # ClickHouse -- the precondition #2241's no-fail-open ruling rests on)
    # and internal/jobs/workgraph/operationaledges. Merged total: 47.
    # CHAOS-5318 added internal/jobs/operational's first //go:build
    # integration file (github_app_events_integration_test.go, 5 Postgres-
    # backed tests): 47 -> 48. CHAOS-5319 added a second integration file to
    # the SAME already-discovered package (sync_dispatch_integration_test.go,
    # 11 more Postgres-backed tests) -- no further package-count change,
    # since the package was already counted once CHAOS-5318 landed.
    # CHAOS-5358 added THREE new -tags=integration packages in one PR:
    # internal/jobs/workgraph/issuecommitedges, issuepredges, and prcommit
    # (each gained its first integration-tagged test, the repo_id-as-string
    # bind fix's regression test). 48 -> 51.
    # CURRENT TOTAL: 51. Adding one -tags=integration package bumps every
    # literal below by +1 -- this is the one number to change; the
    # narrative above is for someone auditing history, not for the bump.
    assert "51 package(s) discovered, 0 denylisted, 51 will run" in result.stdout
    # Name the package explicitly (SET MEMBERSHIP), not just the count --
    # a bare count is exactly what let CHAOS-4643's own literal drift
    # 31 -> 32 -> 33 unnoticed.
    assert "  RUN  cmd/query-api/internal/analytics" in result.stdout
    assert "  SKIP cmd/query-api/internal/analytics: " not in result.stdout
