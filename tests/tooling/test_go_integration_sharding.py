"""Contracts for deterministic Go storage-integration CI sharding."""

from __future__ import annotations

import json
import os
import re
import shlex
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "go.yml"
CHECK_GO = ROOT / "ci" / "check_go.sh"
MANIFEST = ROOT / "ci" / "go_integration_shards.tsv"
PROVIDER_MANIFEST = ROOT / "ci" / "go_providersync_test_shards.tsv"
PROVIDER_PACKAGE = "internal/providersync"
CONTAINER_HARNESS = ROOT / "internal" / "testsupport" / "containers" / "harness.go"
TEST_GO_CACHE = Path(tempfile.gettempdir()) / "chaos3141-go-sharding-test-cache"
# Hosted job 93890967576 measured 49.596s for a cold planner invocation. This
# test-process guard is deliberately above twice that observed completion; it
# does not change the workflow job cap or the Go test timeout.
CHECK_GO_TIMEOUT_SECONDS = 120

EXPECTED_PACKAGES = {
    "internal/goapiproof",
    "internal/reconcilerservice",
    "internal/workerservice",
    "internal/workersctl",
    # CHAOS-5486: the routing verbs' first //go:build integration file --
    # `enable` driven end to end against a real Postgres and a real HTTP
    # server, because an adversarial round proved that disabling the
    # schema-agreement preflight and suppressing the enabled_unproven
    # WARNING, both at their real call sites in this package, survived the
    # entire suite while nothing ever ran a verb. CHAOS-6280 (spec S1)
    # moved the package from cmd/go-api-routing to internal/goapicli/routing
    # when go-api-routing folded into the dho operator binary.
    "internal/goapicli/routing",
    "internal/queryapi/server",
    "internal/queryapi/aianalytics",
    "internal/queryapi/analytics",
    "internal/queryapi/busfactor",
    "internal/queryapi/compoundingrisk",
    # internal/queryapi/explain's integration-tagged tests exercise
    # the team-scoped repo filter's pushed-down membership condition
    # against a real ClickHouse: the query text that once returned every
    # matching repo id as its own result set throws the read-only
    # client's row ceiling when run standalone, while the route itself
    # succeeds on the same data; a status-filtered metric's
    # empty-vs-populated response is exercised in both directions too.
    "internal/queryapi/explain",
    # CHAOS-5523: the featureFlagEvents port's own Testcontainers-backed
    # tests (events_integration_test.go) -- the org-scoping/flagKey-filter/
    # ORDER BY/count-not-limit-bound happy path and the real UNKNOWN_TABLE
    # degraded path, both against a real ClickHouse engine.
    "internal/queryapi/featureflags",
    # Every home reader runs once against a real ClickHouse container
    # with the canonical migration chain applied, each ReplacingMergeTree
    # table seeded with a duplicate physical version at its own natural
    # key: sum()/count() aggregates over integer columns promote to
    # UInt64 in ClickHouse, and a fake RowScanner cannot reproduce the
    # real clickhouse-go driver's refusal to scan that into a narrower
    # destination, nor can it prove FINAL/argMax actually collapses a
    # duplicate row the way canned fixture rows already assume it does
    # (see readers_seeded_integration_test.go's own header comment).
    "internal/queryapi/home",
    "internal/queryapi/hotspots",
    # The people summary naive-timestamp venue differential (skips without
    # the live Python env), in its own package for its own time budget.
    "internal/queryapi/people/summaryvenue",
    # CHAOS-4977 step 7: the recurrence guard for FetchWorkUnitInvestments'
    # real Map(String, Float64) theme/subcategory columns -- a fake
    # RowScanner double can hand back any Go type its author declares, so
    # it can never prove the real clickhouse-go driver's type-conversion
    # path actually works against the real column type (it didn't, see
    # workunitreader_seeded_integration_test.go's own header comment).
    "internal/queryapi/investmentexplain",
    # fetchInvestmentUnassignedCounts' countDistinctIf() columns: the
    # aggregate returns UInt64 in ClickHouse regardless of the counted
    # column's own type, and a fake RowScanner answers a Scan call by
    # reflecting on the Go destination type it is handed rather than
    # replaying the real clickhouse-go driver's own conversion rules, so
    # it cannot prove a narrower scan destination actually works against
    # the real driver (see unassignedcounts_seeded_integration_test.go's
    # own header comment).
    "internal/queryapi/investmentflow",
    # The argMax dedup NULL-skip fix (Nullable(Float64) fields on
    # work_item_metrics_daily/repo_metrics_daily/incident_metrics_daily/
    # ai_impact_metrics_daily) has its own real-ClickHouse proof, since a
    # fake RowScanner cannot reproduce argMax's server-side null-skip
    # behaviour.
    "internal/queryapi/operatingreview",
    "internal/queryapi/routeswitch",
    # sum()/countIf() aggregates over UInt32 columns (new_items_count,
    # new_bugs_count, items_touched, churn) promote to UInt64 in
    # ClickHouse, and a fake RowScanner cannot reproduce the real
    # clickhouse-go driver's refusal to scan that into a narrower/
    # mismatched Go destination (see
    # aggregatescan_seeded_integration_test.go's own header comment).
    "internal/queryapi/sankey",
    "internal/queryapi/scopelabel",
    "internal/queryapi/security",
    "internal/queryapi/testopsrisk",
    # Same argMax dedup NULL-skip proof as operatingreview above, for
    # wip_age_p50/p90_hours and pr_first_review_p50_hours.
    "internal/queryapi/throughputforecast",
    "internal/queryapi/workgraph",
    # The generic admin audit_logs writer: real INSERT round-trips (declared
    # columns, the changes/request_metadata "{}" coercion, the org_id FK
    # violation) against a real Postgres.
    "internal/api/audit",
    # The Stripe webhook dispatch oracle: executes the real Python route per
    # event type (skips without DEV_HEALTH_LIVE_PYTHON_ORACLES=1).
    "internal/api/billing",
    "internal/api/externalingest",
    "internal/api/externalurl",
    # The GitHub App install routes' venue differential oracle against the
    # real Python api (its own package: internal/apiservice is at its time
    # budget).
    "internal/api/githubapp",
    "internal/api/licensing",
    # The api's protected-route policy read path: provision, migrate, api
    # readiness and an authenticated request on a real Postgres.
    "internal/api/policy",
    # The sync admin reads' venue differential oracle: the real Python api
    # and dho api on two copies of one seeded Postgres (skips without the
    # live Python env).
    # The generic integration admin routes' venue differential (skips
    # without the live Python env), in its own package for its own budget.
    "internal/api/integrationsadmin/integrationsvenue",
    # The integration discover route's venue differential (skips without the live
    # Python env), in its own package for its own budget.
    "internal/api/integrationsadmin/discoveryvenue",
    "internal/api/syncadmin",
    # The session routes: the Go routes alone replayed against the recorded
    # Python answers, concurrent refreshes of one token, and the failed-
    # attempt record on a locked row, against real Postgres and ClickHouse.
    "internal/api/session",
    # The session routes' venue differential (skips without the live Python
    # env), in its own package for its own venue time budget.
    "internal/apiservice/sessionvenue",
    # The registration, verification, reset, invite and onboarding routes'
    # venue differential (skips without the live Python env), in its own
    # package for its own venue time budget.
    "internal/apiservice/authflowvenue",
    # The dho api's Prometheus counter parity venue differential (skips
    # without the live Python env), in its own package for its own venue
    # time budget.
    "internal/apiservice/metricsvenue",
    # The team + identity admin CRUD store/handlers: real ClickHouse CRUD
    # round trips and the identity POST route's 404/409/facet-reconciliation
    # logic, against a real server (testcontainers).
    "internal/api/teamsidentity",
    # The admin teams/identities naive-timestamp venue differential (skips
    # without the live Python env), in its own package for its own budget.
    "internal/api/teamsidentity/adminstampvenue",
    # CHAOS-6247: GitHub/GitLab/Jira webhook intake against a real Postgres
    # (durable delivery row + job outbox publish), PagerDuty intake against
    # a real Postgres and a real Valkey (binding lookup, replay claim/
    # accept, stream write).
    "internal/api/webhookintake",
    # The venue differential oracle: the real Python api and dho api on two
    # copies of one Alembic-built Postgres (runs with the live Python env).
    "internal/apiservice",
    "internal/apiservice/acr",
    # The admin org/user/invite/impersonation routes' venue-oracle
    # differentials against the real Python api; each test skips without
    # DEV_HEALTH_LIVE_PYTHON_ORACLES=1, so under the plain integration tag
    # this package still builds and its tests still register (SKIP counts
    # as run), matching internal/api/policy's own live-oracle pattern.
    # CHAOS-6463: `dho admin features seed` is checked against the Python verb
    # on databases the real Python upgrade built -- only a real engine can
    # build either side.
    "internal/admincli",
    # CHAOS-6466: the Atlassian Teams sync writes the team dimensions against a
    # real ClickHouse (manual members and project keys carried over, the
    # project-as-team rows untouched, a re-run).
    "internal/atlassianteams",
    # CHAOS-6466: the `dho sync teams` verb end to end against a real ClickHouse.
    "internal/synccli",
    "internal/apiservice/admin",
    # The admin setup-status venue differential (skips without the live
    # Python env), in its own package for its own venue time budget.
    "internal/apiservice/adminsetupvenue",
    # The admin LLM budget venue differential (skips without the live Python
    # env), in its own package for its own venue time budget.
    "internal/apiservice/adminllmvenue",
    # The billing venue oracles (plans, checkout, portal, ledger, the Stripe
    # webhook) in a package of their own, out of internal/apiservice's venue
    # time budget; each skips without DEV_HEALTH_LIVE_PYTHON_ORACLES=1.
    "internal/apiservice/billingvenue",
    # The external-ingest limiter venue differential (CHAOS-6480): a new venue
    # test lives in its own package because internal/apiservice is at its
    # 20-minute budget.
    "internal/apiservice/externalingestvenue",
    # CHAOS-6368: the shared rate-limit store against a real Valkey (limit held
    # across two clients, the fixed window, the per-caller path bound, TTLs).
    "internal/auth/ratelimitvalkey",
    # CHAOS-6467: `dho ai allowlist set|list` against a real ClickHouse at the head,
    # compared with the Python verb (venue oracle).
    "internal/aicli",
    "internal/cacheinvalidation",
    # CHAOS-6461: the ClickHouse head baseline is re-derived by executing the
    # real Python chain on a fresh ClickHouse, and dho's migrator is checked
    # against that database -- only a real engine can build either side.
    "internal/chmigrate",
    "internal/externalrecompute",
    # CHAOS-6465: `dho fixtures finalize-synthetic-sync` on a real PostgreSQL at the
    # head baseline, against the rows the Python producer wrote (frozen).
    "internal/fixturescli",
    "internal/joboperator",
    "internal/joboutbox",
    "internal/jobrescue",
    "internal/jobroute",
    "internal/jobruntime",
    # CHAOS-6467: `dho metrics validate-flags` on a real ClickHouse, seeded org by org
    # and compared with the frozen Python reports (and the live producer, by venue oracle).
    "internal/metricscli",
    "internal/migrationmatrix",
    # CHAOS-5006 PR2: the end-to-end proof that
    # ResolveProviderKindForOrg's org-BYO precedence (org BYO beats an
    # explicit platform LLM_PROVIDER, only the none/mock kill-switch
    # beats org BYO) holds against llmorgsettings.Store.ResolveUsableProvider
    # over a REAL Postgres container, not a fake resolver.
    # CHAOS-5359: the package root's first //go:build integration file,
    # hierarchycascade_integration_test.go -- proves the repo-hierarchy
    # cascade's Materializer.Run end-to-end wiring, and the sankeycoverage.go
    # repo-resolution expression it feeds, against a real ClickHouse.
    # The dimension-table fold's OPTIMIZE FINAL and its single-partition and
    # row-bound guard are properties of a real ClickHouse engine, run
    # against the migrated repos and teams tables.
    "internal/jobs/dimensionfold",
    "internal/jobs/investment",
    "internal/jobs/investment/categorize",
    "internal/jobs/investment/chquery",
    # CHAOS-4441: the ClickHouse writer for investment.materialize's three
    # ReplacingMergeTree tables. Its correctness claims (dedup-before-filter,
    # sub-millisecond version distinctness, org-scoping) are properties of the
    # real engine, so a fake connection cannot prove them.
    "internal/jobs/investment/chwrite",
    "internal/jobs/metrics/daily",
    # CHAOS-4806: the package's first //go:build integration file,
    # baselines_nullable_integration_test.go -- proves a nil *float64
    # aggregate field round-trips as a real ClickHouse NULL (migration
    # 090's newly-Nullable columns) and does not corrupt a present
    # sibling field on the same row, against a real server.
    "internal/jobs/metrics/daily/benchmarking",
    "internal/jobs/metrics/daily/icfinalize",
    "internal/jobs/metrics/remaining",
    # CHAOS-5318: the native GitHub App installation/marketplace_purchase
    # webhook handler's Postgres-backed proofs (the atomic ON CONFLICT DO
    # NOTHING upsert's 8-goroutine convergence, credential deactivation on
    # "deleted"). CHAOS-5319 added a second integration file to this SAME
    # package (the org/source resolution + scheduled_sync_occurrences/
    # sync_manual_triggers native dispatch path) -- no further count change.
    "internal/jobs/operational",
    "internal/jobs/pagerduty",
    "internal/jobs/repair",
    "internal/jobs/report",
    "internal/jobs/system",
    "internal/jobs/workgraph",
    "internal/jobs/workgraph/edges",
    # CHAOS-5358: the CHAOS-5341-adjacent bind-value-type fix (bind
    # window.RepoID.String(), not the raw uuid.UUID, to a {repo_id:UUID}
    # named ClickHouse query parameter) -- the regression test proves the
    # fix against a real server, since the defect is a server-side parse
    # error a fake connection can never reproduce.
    "internal/jobs/workgraph/issuecommitedges",
    "internal/jobs/workgraph/issuepredges",
    "internal/jobs/workgraph/issueprlinks",
    # CHAOS-4924: the native operational-incident/flag-guards edge producer.
    # Its correctness claims (the CHAOS-4269 NULL-valid_from guard, the
    # DateTime-vs-DateTime64 placeholder precision, the batch-clock stamp)
    # are properties of the real migration chain and a real ClickHouse
    # engine, so a fake connection cannot prove them.
    "internal/jobs/workgraph/operationaledges",
    # CHAOS-5358: same bind-value-type fix as issuecommitedges/issuepredges
    # above.
    "internal/jobs/workgraph/prcommit",
    # CHAOS-4989: the org-scoped BYO LLM settings read path's own
    # feature_flags/org_feature_overrides/org_licenses/organizations/
    # settings precedence matrix runs against a real Postgres container.
    "internal/llmorgsettings",
    # CHAOS-6462: the PostgreSQL head baseline is re-derived by executing the
    # real Python upgrade on a fresh PostgreSQL, and dho's migrator is checked
    # against that database -- only a real engine can build either side.
    # CHAOS-6467: `dho backfill run` on a real PostgreSQL at the head baseline, compared
    # with the rows the Python hand-off wrote (frozen, and live by venue oracle).
    "internal/backfillrun",
    # CHAOS-6669: `dho maintenance` on a real PostgreSQL at the head baseline, compared
    # with the rows the Python verbs left (frozen, and live by venue oracle).
    "internal/maintenancecli",
    # CHAOS-6467: `dho backfill operational` on a real ClickHouse at both operational
    # table shapes, compared with the rows the Python producer wrote (frozen, and live
    # by venue oracle).
    "internal/operationalbackfill",
    "internal/pgmigrate",
    "internal/platform/config",
    "internal/providerfoundation",
    "internal/providersync",
    # CHAOS-6464: `dho migrate upgrade` run end to end on real PostgreSQL and
    # ClickHouse.
    "internal/rivermigrate",
    "internal/scheduler/fixed",
    "internal/scheduler/sync",
    # The ClickHouse posture manifest (CHAOS-6310): SHOW GRANTS FOR
    # CURRENT_USER parsing proven against a real server, including the
    # missing-privilege/extra-grant/extra-privilege negative controls.
    "internal/storage/clickhouse",
    "internal/storage/postgres",
    # CHAOS-4882: the auth-owned schema's migration lineage. Its suite starts
    # a real PostgreSQL and connects AS the runtime role to prove DDL and
    # cross-schema access are refused, so it is integration-tagged.
    "internal/storage/postgres/authschema",
    "internal/storage/river",
    "internal/streamhandlers",
    "internal/streamrunner",
    # CHAOS-6243: the in-process budget estimator's loader runs its real
    # SQL against Postgres.
    "internal/syncbudget",
    "internal/syncdispatchruntime",
    "internal/syncreconciler",
    "internal/synccoverage",
    "internal/syncroute",
    # The argMax dedup NULL-skip fix for team_project_ownership.project_key,
    # team_repo_ownership.repo_id and team_memberships.raw_provider_user_id/
    # raw_email has its own real-ClickHouse proof, for the same reason as
    # operatingreview/throughputforecast above.
    "internal/teamattribution",
    # CHAOS-4897: the recommendations loader's owned-repo scoping join reads
    # team_repo_ownership's bitemporal window for real, so its correctness
    # (valid_from/valid_to boundaries, NULL-repo_id exclusion, org isolation)
    # is a property of the real engine a fake connection cannot prove.
    "internal/teamownership",
    # CHAOS-4902: the RMT sweep's own authoritative-count/dedup-key
    # integration test -- applies the real migration chain to a fresh
    # ClickHouse container and reads system.tables directly, so a fake
    # connection cannot prove the population it asserts.
    "internal/testsupport/chschema",
    "internal/testsupport/containers",
    # venueoracle's TableRows guard, checked against a real Postgres.
    "internal/testsupport/venueoracle",
}


def _run_check_go(
    *args: str,
    manifest: Path = MANIFEST,
    provider_manifest: Path = PROVIDER_MANIFEST,
    github_output: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["DEV_HEALTH_GO_INTEGRATION_SHARD_MANIFEST"] = str(manifest)
    env["DEV_HEALTH_GO_PROVIDER_TEST_SHARD_MANIFEST"] = str(provider_manifest)
    env["DEV_HEALTH_GO_CACHE"] = str(TEST_GO_CACHE)
    if github_output is not None:
        env["GITHUB_OUTPUT"] = str(github_output)
    return subprocess.run(
        ["bash", "ci/check_go.sh", *args],
        cwd=ROOT,
        env=env,
        check=False,
        capture_output=True,
        text=True,
        timeout=CHECK_GO_TIMEOUT_SECONDS,
    )


def _workflow() -> dict[str, Any]:
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))


def _pinned_clickhouse_image() -> str:
    match = re.search(
        r'(?m)^\s*ClickHouseImage\s*=\s*"(?P<image>[^"]+)"',
        CONTAINER_HARNESS.read_text(encoding="utf-8"),
    )
    assert match is not None, (
        "no ClickHouseImage declaration found in the container harness -- if it "
        "was renamed this helper is silently dead, so fix the pattern rather "
        "than the assertion"
    )
    image = match.group("image")
    # A TAG, not a digest. ClickHouse tracks the 26 MAJOR so minor and patch
    # upgrades apply -- ruled by chris (CHAOS-4854), the same policy CHAOS-4851
    # used for the CI service containers.
    #
    # The REPOSITORY is fixed even so. An earlier revision asserted only
    # `[^@\s]+:[^@\s]+|...`, i.e. "a reference of some kind", which blessed
    # `:latest`, `clickhouse/other-image:26.7` and
    # `quay.io/other/clickhouse-server:latest`. This helper feeds the pre-pull
    # expectations below, so whatever it accepts is what the mirror assertions
    # are then derived FROM -- a foreign registry accepted here would be
    # asserted as correct rather than caught.
    # `[0-9]`, never `\d`. This same pattern is written in three dialects --
    # bash ERE in ci/check_go.sh, Go RE2 in harness_test.go, and Python here --
    # and they must accept the same set. Python's `\d` is UNICODE-aware, so it
    # matches Arabic-Indic and other non-ASCII digits; bash `[0-9]` and Go RE2
    # (whose Perl classes are ASCII-only) both reject them. Measured: the tag
    # `26.\u0667` was ACCEPTED by Python's `\d` and rejected by the other two.
    # `[0-9]` is the one spelling that means the same thing in all three.
    assert re.fullmatch(
        r"clickhouse/clickhouse-server(:26(\.[0-9]+)*|@sha256:[0-9a-f]{64})", image
    ), (
        "ClickHouseImage must be clickhouse/clickhouse-server pinned to a 26.x "
        f"tag or a sha256 digest, got {image!r}"
    )
    return image


def test_provider_shard_discovery_does_not_compile_the_test_binary() -> None:
    source = CHECK_GO.read_text(encoding="utf-8")
    match = re.search(
        r"(?ms)^discover_providersync_tests\(\) \{\n(?P<body>.*?)^\}",
        source,
    )
    assert match is not None
    body = match.group("body")
    assert "go list -mod=readonly -tags=integration" in body
    assert "go test -mod=readonly" not in body
    assert 'done <<< "${files_output}"' not in body
    assert "done < <(printf '%s\\n' \"${files_output}\")" in body


def test_package_discovery_does_not_spawn_grep_for_every_tracked_test() -> None:
    source = CHECK_GO.read_text(encoding="utf-8")
    match = re.search(
        r"(?ms)^discover_integration_packages\(\) \{\n(?P<body>.*?)^\}",
        source,
    )
    assert match is not None
    body = match.group("body")
    assert 'git -C "${ROOT}/${module_dir}" grep -l -E' in body
    assert "ls-files --cached --others" not in body
    assert "ls-files --others --exclude-standard" in body


def test_integration_shard_arity_guard_uses_an_explicit_conditional() -> None:
    """Keep the public CLI guard compatible with the hosted ShellCheck gate."""

    source = CHECK_GO.read_text(encoding="utf-8")
    match = re.search(
        r"(?ms)^  integration-shard\)\n(?P<body>.*?^    ;;)$",
        source,
    )
    assert match is not None
    assert (
        'if [ "$#" -lt 3 ] || [ "$#" -gt 4 ]; then\n'
        '      die "integration-shard requires TARGET SHARD and accepts only '
        'optional --dry-run"\n'
        "    fi"
    ) in match.group("body")

    for args in (("packages",), ("packages", "2", "--dry-run", "extra")):
        result = _run_check_go("integration-shard", *args)
        assert result.returncode == 2
        assert (
            "integration-shard requires TARGET SHARD and accepts only optional "
            "--dry-run" in result.stderr
        )


def _providersync_top_level_tests() -> set[str]:
    env = os.environ.copy()
    env["GOTOOLCHAIN"] = "go1.27.0"
    env["GOWORK"] = "off"
    env["GOCACHE"] = str(TEST_GO_CACHE)
    result = subprocess.run(
        [
            "go",
            "list",
            "-mod=readonly",
            "-tags=integration",
            "-f={{range .TestGoFiles}}{{println .}}{{end}}{{range .XTestGoFiles}}{{println .}}{{end}}",
            f"./{PROVIDER_PACKAGE}",
        ],
        cwd=ROOT,
        env=env,
        check=False,
        capture_output=True,
        text=True,
        timeout=CHECK_GO_TIMEOUT_SECONDS,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    tests: list[str] = []
    for filename in result.stdout.splitlines():
        if not filename:
            continue
        assert Path(filename).name == filename
        source = ROOT.joinpath(PROVIDER_PACKAGE, filename).read_text(encoding="utf-8")
        tests.extend(re.findall(r"(?m)^func (Test[A-Za-z0-9_]+)\s*\(", source))
    assert tests
    assert len(tests) == len(set(tests))
    return set(tests)


def _providersync_integration_tagged_tests() -> set[str]:
    tests: set[str] = set()
    for path in ROOT.joinpath(PROVIDER_PACKAGE).glob("*_test.go"):
        source = path.read_text(encoding="utf-8")
        if not re.search(r"(?m)^//go:build.*\bintegration\b", source):
            continue
        tests.update(re.findall(r"(?m)^func (Test[A-Za-z0-9_]+)", source))
    return tests


def test_shard_plan_is_exhaustive_nonempty_and_machine_readable(
    tmp_path: Path,
) -> None:
    github_output = tmp_path / "github-output"
    result = _run_check_go("integration-shard-plan", github_output=github_output)

    assert result.returncode == 0, result.stdout + result.stderr
    package_total = len(EXPECTED_PACKAGES)
    assert (
        f"{package_total} package(s) discovered, 0 denylisted, {package_total} will run"
    ) in result.stdout
    # Raised 4 -> 6: at four shards each balanced "packages" shard's own
    # estimated test time (2191s) already exceeded the hosted job's
    # 25-minute (1500s) timeout-minutes cap before any setup/teardown
    # overhead. Six shards spreads the same non-isolated packages across
    # five balanced shards instead of three, each landing near 1315s --
    # comfortably under the cap -- see the weight-derived isolation check
    # below for how a future regression here is caught instead of silently
    # re-balanced.
    assert (
        f"integration shard plan: 6 shard(s), {package_total} package(s)"
    ) in result.stdout

    output = dict(
        line.split("=", maxsplit=1)
        for line in github_output.read_text(encoding="utf-8").splitlines()
    )
    matrix = json.loads(output["matrix"])["include"]
    assert {(entry["target"], entry["shard"]) for entry in matrix} == {
        ("providersync", 1),
        ("providersync", 2),
        ("providersync", 3),
        ("providersync", 4),
        ("packages", 2),
        ("packages", 3),
        ("packages", 4),
        ("packages", 5),
        ("packages", 6),
    }
    assert len(matrix) == 9

    assignments: dict[int, set[str]] = {}
    shard_weights: dict[str, int] = {}
    for line in result.stdout.splitlines():
        if not line.startswith("  SHARD "):
            continue
        _, shard, package, weight_field = line.split()
        assignments.setdefault(int(shard), set()).add(package)
        assert weight_field.startswith("weight=") and weight_field.endswith("s"), (
            weight_field
        )
        shard_weights[package] = int(
            weight_field.removeprefix("weight=").removesuffix("s")
        )

    assert set(assignments) == {1, 2, 3, 4, 5, 6}
    flattened = [package for packages in assignments.values() for package in packages]
    assert len(flattened) == len(set(flattened)) == len(EXPECTED_PACKAGES)
    assert set(flattened) == EXPECTED_PACKAGES

    # internal/providersync's isolation in the lowest-numbered shard is a
    # property of its WEIGHT relative to the other packages, not a fact this
    # test should hardcode as a bare membership literal -- a literal only
    # tells a human "this passed today", it does not explain why, and it
    # says nothing about how close the next re-time is to breaking it. Derive
    # the same inequality the LPT planner's own greedy placement depends on
    # directly from the planner's printed weights: providersync must cover
    # at least the other packages' balanced share once split evenly across
    # the remaining shards, or the planner starts packing packages into its
    # shard too. Checking the inequality AND the resulting membership means
    # a future package addition that erodes this margin fails loudly here,
    # with the actual numbers, instead of a human silently recounting a new
    # shard-1 membership as fine.
    provider_weight = shard_weights[PROVIDER_PACKAGE]
    other_total = sum(
        weight
        for package, weight in shard_weights.items()
        if package != PROVIDER_PACKAGE
    )
    non_isolated_shards = len(assignments) - 1
    balanced_share = other_total / non_isolated_shards
    assert provider_weight >= balanced_share, (
        f"{PROVIDER_PACKAGE}'s weight ({provider_weight}s) no longer covers "
        f"the other {len(shard_weights) - 1} packages' balanced per-shard "
        f"share ({balanced_share:.1f}s across {non_isolated_shards} shards) "
        "-- the LPT planner will start packing other packages into its "
        "shard. Re-time ci/go_integration_shards.tsv (or raise its shard "
        "count) before this reshuffles silently."
    )
    assert assignments[1] == {PROVIDER_PACKAGE}

    estimated = {
        int(match.group("shard")): int(match.group("seconds"))
        for line in result.stdout.splitlines()
        if (
            match := re.fullmatch(
                r"integration shard (?P<shard>\d+): estimated "
                r"(?P<seconds>\d+)s, \d+ package\(s\)",
                line,
            )
        )
    }
    assert set(estimated) == {1, 2, 3, 4, 5, 6}
    # The five non-isolated "packages" shards (2/3/4/5/6) are what the LPT
    # planner actually balances against each other -- shard 1 only ever
    # holds internal/providersync, checked above. Recounted directly from
    # this run's own planner output (not hand-adjusted) after CHAOS-6246
    # added internal/api/externalingest (weight 10, a placeholder estimate
    # -- no hosted CI run has timed it yet, so it is not the observed-wall-
    # time-from-a-green-run figure every other row's comment promises;
    # re-measure and replace it the same way CHAOS-6244's rows were): 1370s/
    # 1371s/1368s/1368s/1369s, a 3s spread. Re-tighten or loosen this to
    # match a future re-time's actual output rather than forcing new
    # weights to preserve today's gap.
    # The bound is proportional (1% of the largest shard, about 13s today)
    # so a package added at its measured weight is never tuned to fit a
    # fixed gap: LPT on real weights leaves a few seconds of spread.
    packages_totals = [estimated[shard] for shard in (2, 3, 4, 5, 6)]
    assert max(packages_totals) - min(packages_totals) <= max(packages_totals) // 100

    expected_provider_tests = _providersync_top_level_tests()
    expected_integration_tests = _providersync_integration_tagged_tests()
    assert expected_provider_tests
    assert expected_integration_tests
    assert expected_integration_tests < expected_provider_tests

    provider_assignments: dict[int, set[str]] = {}
    provider_class: dict[str, str] = {}
    for line in result.stdout.splitlines():
        if not line.startswith("  PROVIDER-SHARD "):
            continue
        _label, shard, test_name, _weight, classification = line.split()
        provider_assignments.setdefault(int(shard), set()).add(test_name)
        provider_class[test_name] = classification.removeprefix("class=")

    assert set(provider_assignments) == {1, 2, 3, 4}
    provider_flattened = [
        test_name for tests in provider_assignments.values() for test_name in tests
    ]
    assert len(provider_flattened) == len(set(provider_flattened))
    assert set(provider_flattened) == expected_provider_tests
    assert {
        name
        for name, classification in provider_class.items()
        if classification == "integration"
    } == expected_integration_tests
    assert set(provider_class.values()) == {"integration", "ordinary"}

    provider_totals: dict[int, int] = {}
    provider_integration_counts: dict[int, int] = {}
    for line in result.stdout.splitlines():
        match = re.fullmatch(
            r"providersync test shard (?P<shard>\d+): relative weight "
            r"(?P<weight>\d+), (?P<count>\d+) test\(s\), "
            r"(?P<integration>\d+) integration-tagged",
            line,
        )
        if match:
            provider_totals[int(match.group("shard"))] = int(match.group("weight"))
            provider_integration_counts[int(match.group("shard"))] = int(
                match.group("integration")
            )
    assert set(provider_totals) == {1, 2, 3, 4}
    assert max(provider_totals.values()) - min(provider_totals.values()) <= 1
    assert (
        max(provider_integration_counts.values())
        - min(provider_integration_counts.values())
        <= 1
    )


def test_each_shard_dry_run_executes_only_its_manifest_assignment() -> None:
    selected_packages: list[str] = []
    for shard in (2, 3, 4, 5, 6):
        result = _run_check_go("integration-shard", "packages", str(shard), "--dry-run")
        assert result.returncode == 0, result.stdout + result.stderr
        assert f"integration package shard {shard}: DRY RUN" in result.stdout
        selected_packages.extend(
            line.removeprefix("  SHARD-RUN ")
            for line in result.stdout.splitlines()
            if line.startswith("  SHARD-RUN ")
        )

    assert len(selected_packages) == len(set(selected_packages))
    assert set(selected_packages) == EXPECTED_PACKAGES - {PROVIDER_PACKAGE}

    selected_tests: list[str] = []
    for shard in (1, 2, 3, 4):
        result = _run_check_go(
            "integration-shard", "providersync", str(shard), "--dry-run"
        )
        assert result.returncode == 0, result.stdout + result.stderr
        assert f"providersync test shard {shard}: DRY RUN" in result.stdout
        selected_tests.extend(
            line.removeprefix("  PROVIDER-TEST-RUN ")
            for line in result.stdout.splitlines()
            if line.startswith("  PROVIDER-TEST-RUN ")
        )

    expected_tests = _providersync_top_level_tests()
    assert len(selected_tests) == len(set(selected_tests))
    assert set(selected_tests) == expected_tests


def test_manifest_drift_and_duplicate_packages_fail_loudly(tmp_path: Path) -> None:
    original = MANIFEST.read_text(encoding="utf-8")

    missing = tmp_path / "missing.tsv"
    missing.write_text(
        "\n".join(
            line
            for line in original.splitlines()
            if not line.startswith("internal/providersync\t")
        )
        + "\n",
        encoding="utf-8",
    )
    missing_result = _run_check_go("integration-shard-plan", manifest=missing)
    assert missing_result.returncode == 2
    assert "manifest is missing discovered package 'internal/providersync'" in (
        missing_result.stderr
    )

    duplicate = tmp_path / "duplicate.tsv"
    duplicate.write_text(original + "internal/providersync\t1166\n", encoding="utf-8")
    duplicate_result = _run_check_go("integration-shard-plan", manifest=duplicate)
    assert duplicate_result.returncode == 2
    assert "manifest lists 'internal/providersync' more than once" in (
        duplicate_result.stderr
    )

    invalid_provider = tmp_path / "invalid-provider.tsv"
    invalid_provider.write_text(
        "shards\t1\nintegration-test-weight\t100\nordinary-test-weight\t1\n",
        encoding="utf-8",
    )
    invalid_provider_result = _run_check_go(
        "integration-shard-plan", provider_manifest=invalid_provider
    )
    assert invalid_provider_result.returncode == 2
    assert "provider test shard manifest must declare at least two shards" in (
        invalid_provider_result.stderr
    )


def _declared_image(constant: str) -> str:
    match = re.search(
        rf'(?m)^\s*{constant}\s*=\s*"(?P<image>[^"]+)"',
        CONTAINER_HARNESS.read_text(encoding="utf-8"),
    )
    assert match is not None, f"{constant} is not declared in {CONTAINER_HARNESS}"
    return match.group("image")


def _mirrored_image(image: str, prefix: str) -> str:
    """Mirror ONE test-dependency image through TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX
    exactly as ci/check_go.sh's own mirrored_image() shell function does (see
    that function's comment for the "why" of each branch) -- this test's own
    prepull-consumer assertions must apply the SAME rule the harness applies,
    or a supported, non-empty prefix in the test's environment makes the
    literal source-declared image string wrong on purpose.

    `_declared_image`/`_pinned_clickhouse_image` never
    read this env var at all, so a host with a mirror configured in its
    environment (this repo's own documented, supported override) failed
    BOTH prepull tests -- the consumer (ci/check_go.sh) honoured the
    override, the test's hardcoded unprefixed expectation did not.
    Duplicated here (bash -> Python) rather than shared, deliberately: the
    rule is three lines and test-only, and spawning check_go.sh per
    expected-image lookup would be slower and no more trustworthy than a
    pinned, commented copy of the same three lines.
    """
    if not prefix:
        return image
    first = image.split("/", 1)[0]
    if first != image and first.lower() == "docker.io":
        raise AssertionError(
            f"test dependency image {image!r} names docker.io explicitly"
        )
    if first != image and ("." in first or ":" in first):
        return image
    return f"{prefix.rstrip('/')}/{image}"


def _prepull_stub_bin(tmp_path: Path) -> Path:
    """A docker+sleep pair that fails until the DOCKER_SUCCEED_ON'th call."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    docker = bin_dir / "docker"
    docker.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
attempt=0
if [ -f "${DOCKER_ATTEMPTS_FILE}" ]; then
  attempt="$(<"${DOCKER_ATTEMPTS_FILE}")"
fi
attempt=$((attempt + 1))
printf '%s\n' "${attempt}" > "${DOCKER_ATTEMPTS_FILE}"
printf '%s\n' "$*" >> "${DOCKER_ARGS_FILE}"
[ "${attempt}" -ge "${DOCKER_SUCCEED_ON}" ]
""",
        encoding="utf-8",
    )
    docker.chmod(0o755)
    sleep = bin_dir / "sleep"
    sleep.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "${SLEEP_ARGS_FILE}"
""",
        encoding="utf-8",
    )
    sleep.chmod(0o755)
    return bin_dir


def test_prepull_retries_the_exact_source_declared_image(tmp_path: Path) -> None:
    attempts = tmp_path / "attempts"
    docker_args = tmp_path / "docker-args"
    sleep_args = tmp_path / "sleep-args"
    bin_dir = _prepull_stub_bin(tmp_path)

    env = os.environ.copy()
    env.update(
        {
            "PATH": f"{bin_dir}:{env['PATH']}",
            "DOCKER_ATTEMPTS_FILE": str(attempts),
            "DOCKER_ARGS_FILE": str(docker_args),
            "DOCKER_SUCCEED_ON": "3",
            "SLEEP_ARGS_FILE": str(sleep_args),
        }
    )
    prefix = env.get("TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX", "")
    postgres = _mirrored_image(_declared_image("PostgresImage"), prefix)
    result = subprocess.run(
        ["bash", "ci/check_go.sh", "integration-prepull"],
        cwd=ROOT,
        env=env,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    # Postgres is first and takes three attempts; the stub's counter is past its
    # threshold by then, so the remaining three images succeed first time.
    assert docker_args.read_text(encoding="utf-8").splitlines() == [
        f"pull {postgres}",
        f"pull {postgres}",
        f"pull {postgres}",
        f"pull {_mirrored_image(_pinned_clickhouse_image(), prefix)}",
        f"pull {_mirrored_image(_declared_image('ValkeyImage'), prefix)}",
        f"pull {_mirrored_image(_declared_image('ReaperImage'), prefix)}",
    ]
    assert sleep_args.read_text(encoding="utf-8").splitlines() == ["5", "10"]
    assert f"pre-pulled postgres test dependency image {postgres} on attempt 3/3" in (
        result.stdout
    )

    attempts.unlink()
    docker_args.unlink()
    sleep_args.unlink()
    env["DOCKER_SUCCEED_ON"] = "4"
    failed = subprocess.run(
        ["bash", "ci/check_go.sh", "integration-prepull"],
        cwd=ROOT,
        env=env,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert failed.returncode == 1
    # Exhausting one image's budget stops the run; the later images are never
    # attempted, so a registry outage fails loudly instead of degrading into a
    # retry storm across every declared image.
    assert docker_args.read_text(encoding="utf-8").splitlines() == [
        f"pull {postgres}",
        f"pull {postgres}",
        f"pull {postgres}",
    ]
    assert sleep_args.read_text(encoding="utf-8").splitlines() == ["5", "10"]
    assert (
        f"failed to pre-pull postgres test dependency image {postgres} after 3 attempts"
    ) in failed.stderr


def test_prepull_warms_every_declared_image(tmp_path: Path) -> None:
    """The set warmed is the harness's, not a list maintained in the workflow.

    The reaper is in it because testcontainers-go starts one before the first
    container of any test binary, whether or not a test asks for it -- which is
    precisely why no human-maintained list would have included it.
    """
    docker_args = tmp_path / "docker-args"
    bin_dir = _prepull_stub_bin(tmp_path)

    env = os.environ.copy()
    env.update(
        {
            "PATH": f"{bin_dir}:{env['PATH']}",
            "DOCKER_ATTEMPTS_FILE": str(tmp_path / "attempts"),
            "DOCKER_ARGS_FILE": str(docker_args),
            "DOCKER_SUCCEED_ON": "1",
            "SLEEP_ARGS_FILE": str(tmp_path / "sleep-args"),
        }
    )
    prefix = env.get("TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX", "")
    result = subprocess.run(
        ["bash", "ci/check_go.sh", "integration-prepull"],
        cwd=ROOT,
        env=env,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert docker_args.read_text(encoding="utf-8").splitlines() == [
        f"pull {_mirrored_image(_declared_image('PostgresImage'), prefix)}",
        f"pull {_mirrored_image(_pinned_clickhouse_image(), prefix)}",
        f"pull {_mirrored_image(_declared_image('ValkeyImage'), prefix)}",
        f"pull {_mirrored_image(_declared_image('ReaperImage'), prefix)}",
    ]


def test_prepull_refuses_a_per_job_subset(tmp_path: Path) -> None:
    """Subsets are refused by construction, not validated.

    An earlier revision let a job name the images it needed. Nothing related that
    list to what the job actually started, so `integration-prepull clickhouse`
    followed by `ci` passed every guard while leaving PostgreSQL cold. The verb
    now takes no arguments, which removes the class rather than checking it.
    """
    bin_dir = _prepull_stub_bin(tmp_path)
    env = os.environ.copy()
    env.update(
        {
            "PATH": f"{bin_dir}:{env['PATH']}",
            "DOCKER_ATTEMPTS_FILE": str(tmp_path / "attempts"),
            "DOCKER_ARGS_FILE": str(tmp_path / "docker-args"),
            "DOCKER_SUCCEED_ON": "1",
            "SLEEP_ARGS_FILE": str(tmp_path / "sleep-args"),
        }
    )
    result = subprocess.run(
        ["bash", "ci/check_go.sh", "integration-prepull", "postgres"],
        cwd=ROOT,
        env=env,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 2
    assert "integration-prepull accepts no arguments" in result.stderr


# --- CHAOS-4778: every job that starts a container must warm its images -------
#
# The defect this guards is not "go-quality lacked a pre-pull step". It is "a CI
# job starts Testcontainers against images nobody warmed, so the first pull is
# cold and anonymous against Docker Hub". Asserting the fixed job would let the
# next such job reintroduce it.
#
# The guard therefore FAILS CLOSED. A check_go.sh verb is container-running
# unless it appears in _NON_CONTAINER_VERBS below; a new verb is dangerous by
# default and its job must log in and pre-pull. Deriving the dangerous set was
# the first design and a codex round broke it: a verb building
# `go test ... -tags=${tag}` through shell indirection is invisible to any static
# leaf scan, so it was omitted from the derived set and its job passed with no
# pre-pull at all. Under a safe-list the same verb is dangerous by default.
#
# The derivation survives as a CROSS-CHECK on the safe-list, and it earns its
# keep: it immediately caught `fast` being added to that list by mistake. `fast`
# ends in check_multi_replica_workers exactly as `ci` does -- the same two-hop
# chain whose invisibility caused the original defect.

_CONTAINER_LEAF_PATTERN = re.compile(r"go test\b[^\n]*-tags=integration")
_PREPULL_COMMAND = "ci/check_go.sh integration-prepull"
_DOCKER_LOGIN_ACTION = "docker/login-action@"

# Verbs asserted NOT to start a container. Adding a verb to check_go.sh without
# adding it here fails test_every_check_go_verb_is_classified, which is the
# point: the safe default is "this might start containers".
#
# `live-python-oracles` was here until CHAOS-6247: every live-oracle block in
# check_live_python_oracles used a plain `go test -mod=readonly -count=1`
# (pure-function comparisons, or a lightweight interpreter subprocess -- no
# Testcontainers). CHAOS-6247's webhookintake venue oracle block genuinely
# runs `go test -tags=integration ./internal/apiservice`, which starts real
# Postgres/Valkey containers, so the declaration became false -- removed
# rather than special-cased, since the verb only ever runs reached through
# `ci` (already classified "harness" regardless of this set) in every
# workflow today; test_every_workflow_job_starting_containers_authenticates_
# and_pre_pulls stays green unchanged.
_NON_CONTAINER_VERBS = frozenset(
    {
        "fmt",
        "vet",
        "test",
        "race",
        "build",
        "contract",
        "integration-vet",
        "integration-shard-plan",
        "integration-prepull",
        # `integration-coverage` only discovers and prints packages -- no go test,
        # no docker. `fast` is deliberately NOT here: like `ci`, it ends in
        # check_multi_replica_workers and starts Testcontainers. Writing this list
        # is exactly where the original defect came from, so the cross-check below
        # is not optional.
        "integration-coverage",
    }
)


def _check_go_functions() -> dict[str, str]:
    """Map every top-level check_go.sh function to its body."""
    source = CHECK_GO.read_text(encoding="utf-8")
    bodies: dict[str, str] = {}
    for match in re.finditer(r"(?m)^(?P<name>[a-z_][a-z0-9_]*)\(\) \{$", source):
        end = source.find("\n}\n", match.end())
        assert end != -1, f"unterminated function {match.group('name')}"
        bodies[match.group("name")] = source[match.end() : end]
    assert bodies, "parsed no functions out of check_go.sh"
    return bodies


def _check_go_dispatch() -> dict[str, str]:
    """Map every public verb to the dispatch arm that runs it."""
    source = CHECK_GO.read_text(encoding="utf-8")
    arms: dict[str, str] = {}
    for match in re.finditer(
        r"(?m)^  (?P<verbs>[a-z0-9|_-]+)\)\n(?P<body>.*?)^    ;;$",
        source,
        re.DOTALL,
    ):
        for verb in match.group("verbs").split("|"):
            arms[verb] = match.group("body")
    assert "ci" in arms, "did not parse check_go.sh's verb dispatch"
    return arms


def _verbs_reaching_a_container_leaf() -> set[str]:
    """Cross-check only. Static, so it under-reports; never used to grant safety."""
    functions = _check_go_functions()
    leaves = {
        name for name, body in functions.items() if _CONTAINER_LEAF_PATTERN.search(body)
    }
    assert leaves, "found no function running an integration-tagged go test"

    def reaches(body: str, seen: frozenset[str]) -> bool:
        for name in functions:
            if name in seen or not re.search(rf"\b{re.escape(name)}\b", body):
                continue
            if name in leaves or reaches(functions[name], seen | {name}):
                return True
        return False

    return {
        verb
        for verb, body in _check_go_dispatch().items()
        if reaches(body, frozenset())
    }


def test_no_verb_declared_safe_actually_starts_containers() -> None:
    """The declaration must not contradict the code.

    The static derivation under-reports, so it cannot grant safety -- but where it
    does fire it is authoritative, and it catches a verb wrongly declared safe.
    """
    overlap = _NON_CONTAINER_VERBS & _verbs_reaching_a_container_leaf()
    assert not overlap, (
        f"verbs declared non-container reach an integration go test: {sorted(overlap)}"
    )


def test_container_derivation_still_sees_the_known_verbs() -> None:
    """Keeps the cross-check above from rotting into a no-op."""
    derived = _verbs_reaching_a_container_leaf()

    assert {"ci", "all", "integration", "integration-shard"} <= derived
    assert derived.isdisjoint({"fmt", "vet", "build", "integration-shard-plan"})


# A `go test` carrying ANY -tags flag is treated as container-starting. Every one
# of the 169 real callers of containers.Start{Postgres,ClickHouse,Valkey} sits
# behind `//go:build integration`, so an untagged `go test` cannot compile them in
# — but the tag VALUE may be a shell variable, so the value is not inspected.
_GO_TEST_WITH_TAGS = re.compile(r"go test\b[^\n]*-tags[= ]")
_CHECK_GO_INVOCATION = re.compile(r"ci/check_go\.sh\s+(\S+)")
# A container runtime named anywhere in the step, and a build tag arriving
# through the environment rather than the command line.
_CONTAINER_TOOL = re.compile(r"\b(docker|podman|compose|nerdctl)\b")
_TAGS_VIA_ENV = re.compile(r"\b(GOFLAGS|GOEXPERIMENT)\b")


# `_run_starts_containers` lived here. It was defined and never called --
# orphaned when the guards were reduced for #2111, and still orphaned after I
# claimed to have rewired it: what I made load-bearing was its `_CONTAINER_TOOL`
# regex, not the function. Everything it checked (`go test -tags`, tags via
# GOFLAGS, an unsafe check_go verb) is checked by `_run_uses_go_test_harness`,
# which IS called. Dead code in a guard file reads as protection while providing
# none, so it is removed rather than left as a comfort.
def _run_uses_go_test_harness(command: str) -> bool:
    """Does this step start containers via internal/testsupport/containers?

    Only those are warmed by `integration-prepull`. A step that builds from a
    Python base image or brings up a compose stack needs the Docker Hub login but
    would gain nothing from warming the Go harness images, so the two obligations
    are tracked separately rather than demanding a pre-pull that cannot help.
    """
    if _GO_TEST_WITH_TAGS.search(command) or _TAGS_VIA_ENV.search(command):
        return True
    for match in _CHECK_GO_INVOCATION.finditer(command):
        verb = match.group(1)
        if re.fullmatch(r"[a-z0-9-]+", verb) is None:
            return True
        if verb not in _NON_CONTAINER_VERBS:
            return True
    return False


# Host match anchored at a boundary, NOT a substring. `"ghcr.io/" in text` is
# satisfied by `evil-ghcr.io/` and `ghcr.io.attacker.net/`, so a hostile or
# merely mistaken ref would be read as OUR mirror; CodeQL raised this as
# incomplete URL substring sanitization and was right. The lookbehind also
# excludes `/` so a PATH segment (`https://registry.example/ghcr.io/x`) does not
# match, which is why scheme separators are normalised first -- otherwise
# `docker://ghcr.io/x` and the path case are indistinguishable. An optional port
# keeps the valid `ghcr.io:443/...` form matching.
# IGNORECASE because registry HOSTS are case-insensitive: `GHCR.IO/full-chaos/x`
# is the same registry and was classified as not-the-mirror, so a job spelling
# it that way escaped the login requirement. The lookbehind uses `\w` (unicode)
# rather than an ASCII class so `éghcr.io/x` -- a different host entirely -- is
# not read as ours.
_MIRROR_HOST = re.compile(r"(?<![\w.:/-])ghcr\.io(?::\d+)?/", re.IGNORECASE)


def _workflow_files() -> list[Path]:
    """Every workflow file. GitHub honours BOTH extensions.

    Scanning only `*.yml` left `escape.yaml` invisible to the guards below.
    """
    directory = ROOT / ".github" / "workflows"
    return sorted(list(directory.glob("*.yml")) + list(directory.glob("*.yaml")))


def _normalise_scheme(text: str) -> str:
    """`scheme://host` -> `scheme host`, so a host and a path segment differ."""
    return text.replace("://", " ")


def _is_docker_login_action(step: dict[str, Any]) -> bool:
    """Case-insensitive, because GitHub resolves action refs case-insensitively.

    `uses: Docker/Login-Action@v4` is the SAME action as `docker/login-action@v4`
    (verified: `gh api repos/Docker/Login-Action` returns `docker/login-action`),
    so a case-sensitive compare let a login with no `registry` -- i.e. defaulting
    to Docker Hub -- pass the guard that exists to forbid exactly that.
    """
    return str(step.get("uses", "")).casefold().startswith(_DOCKER_LOGIN_ACTION)


def _step_kind(step: dict[str, Any]) -> str | None:
    """Classify a workflow step as login / prepull / container-running."""
    if _is_docker_login_action(step):
        registry = str((step.get("with") or {}).get("registry", ""))
        return "login" if registry == "ghcr.io" else None
    command = str(step.get("run", ""))
    if not command:
        return None
    # The step must BE the pre-pull, not merely contain the text. A substring
    # match accepted `: bash ci/check_go.sh integration-prepull` -- `:` is a
    # successful shell no-op, so nothing was warmed and the next step pulled cold.
    # It also rejects a leftover subset argument, which is refused at runtime now
    # but would otherwise fail in CI rather than in review.
    if re.fullmatch(rf"(?:bash\s+)?{re.escape(_PREPULL_COMMAND)}", command.strip()):
        return "prepull"
    if _run_uses_go_test_harness(command):
        return "harness"
    # The river compose pulls its images through the same mirror, so it needs the
    # ghcr login -- but it does NOT need `integration-prepull`, which only warms
    # the Go harness images.
    if "compose.compatibility.yml" in command or "tests/compatibility/river" in command:
        return "mirror"
    # Any direct reference to the mirror registry counts too. Recognising only the
    # two shapes that exist today was fail-open: a future job running
    # `docker pull ghcr.io/<owner>/...` with no login classified as None and was
    # skipped by BOTH assertions, leaving the suite green while the job failed.
    if _MIRROR_HOST.search(_normalise_scheme(command)):
        return "mirror"
    return None


def test_every_workflow_job_starting_containers_authenticates_and_pre_pulls() -> None:
    """The class, not the two jobs that happened to be caught failing."""
    offenders: list[str] = []

    for job_name, job in _workflow()["jobs"].items():
        kinds = [_step_kind(step) for step in job.get("steps", [])]
        mirror_users = [i for i, k in enumerate(kinds) if k in {"harness", "mirror"}]
        if not mirror_users:
            continue
        first_pull = mirror_users[0]
        harness = next((i for i, k in enumerate(kinds) if k == "harness"), None)
        login = next((i for i, k in enumerate(kinds) if k == "login"), None)
        prepull = next((i for i, k in enumerate(kinds) if k == "prepull"), None)

        # Every job that pulls an image needs the ghcr login: all test dependency
        # images come from the mirror now, and CI holds NO Docker Hub credentials
        # (chris, 2026-09-02) after a personal account's 200/hour quota took the
        # whole fleet down. A private package would fail at pull time without it.
        if login is None:
            offenders.append(f"{job_name}: pulls images with no ghcr.io login step")
        elif login > first_pull:
            offenders.append(f"{job_name}: logs in AFTER pulling images")

        # Only jobs using the Go test harness benefit from warming ITS images; a
        # step that builds from a python base or brings up compose does not.
        if harness is not None:
            if prepull is None:
                offenders.append(
                    f"{job_name}: starts Testcontainers with no "
                    f"`{_PREPULL_COMMAND}` step"
                )
            elif prepull > harness:
                offenders.append(f"{job_name}: pre-pulls AFTER starting Testcontainers")
            elif login is not None and login > prepull:
                offenders.append(f"{job_name}: logs in AFTER pre-pulling")

    assert not offenders, "\n".join(offenders)


def test_ci_holds_no_docker_hub_credentials() -> None:
    """CI must not authenticate to Docker Hub at all.

    A personal Docker Hub account metered at 200 pulls/hour, shared across every
    lane, hit zero on 2026-09-02 and took every Testcontainers job down on every
    branch. The fix was not a bigger quota: it was to stop depending on that
    registry from CI. Every test dependency now comes from the ghcr mirror, which
    GITHUB_TOKEN already authenticates.

    This asserts the absence, because a single reintroduced login would quietly
    put the fleet back on a shared personal quota.
    """
    offenders: list[str] = []
    # EVERY workflow, both extensions. A login reintroduced in test.yml or in a
    # new `escape.yaml` drains the same shared quota and takes the same fleet
    # down; scanning two named files was the same enumerate-the-known-cases
    # mistake the guards kept making.
    for path in _workflow_files():
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8")
        if "DOCKERHUB_" in text:
            offenders.append(f"{path.name}: references a DOCKERHUB_* secret")
        # Structural, not textual: `registry: docker.io` was the only spelling
        # checked, so a login-action with renamed secrets and the registry key
        # OMITTED passed -- and an omitted registry IS the Docker Hub default.
        document = yaml.safe_load(text) or {}
        for job_name, job in (document.get("jobs") or {}).items():
            if not isinstance(job, dict):
                continue
            for step in job.get("steps") or []:
                if not _is_docker_login_action(step):
                    continue
                registry = str((step.get("with") or {}).get("registry", "")).strip()
                if registry != "ghcr.io":
                    offenders.append(
                        f"{path.name}: job {job_name!r} runs docker/login-action "
                        f"with registry={registry or '<unset>'}; an unset registry "
                        "DEFAULTS to Docker Hub, which CI must never authenticate to"
                    )
    assert not offenders, "\n".join(offenders)


def test_ghcr_login_is_present_and_not_fork_guarded() -> None:
    """The mirror is only usable if every job that pulls it can authenticate.

    `GITHUB_TOKEN` works on fork pull requests where stored secrets do not, so
    these logins must NOT carry the Docker Hub fork guard -- and every job that
    pre-pulls or runs the river compose must have one, or a private package
    fails at pull time.
    """
    workflow = _workflow()
    guarded: list[str] = []
    for job_name, job in workflow["jobs"].items():
        for step in job.get("steps", []):
            if not _is_docker_login_action(step):
                continue
            if str((step.get("with") or {}).get("registry", "")) != "ghcr.io":
                continue
            if "head.repo.full_name" in str(step.get("if", "")):
                guarded.append(f"{job_name}: ghcr login is fork-guarded")
    assert not guarded, "\n".join(guarded)

    for job_name, job in workflow["jobs"].items():
        commands = [str(step.get("run", "")) for step in job.get("steps", [])]
        needs_mirror = any(
            _PREPULL_COMMAND in c
            or "compose.compatibility.yml" in c
            or "river" in c.lower()
            for c in commands
        )
        if not needs_mirror:
            continue
        registries = [
            str((step.get("with") or {}).get("registry", ""))
            for step in job.get("steps", [])
            if _is_docker_login_action(step)
        ]
        assert any(registry == "ghcr.io" for registry in registries), (
            f"{job_name}: pulls the mirror with no ghcr.io login"
        )


def test_mirrored_image_matches_testcontainers_prefix_semantics() -> None:
    """`check_go.sh` must redirect exactly as testcontainers-go does.

    The pre-pull warms a ref and testcontainers resolves the image again when a
    test starts a container. If the two disagree by even one rule, CI warms one
    image and pulls another -- strictly worse than not mirroring, because it adds
    a pull instead of removing one.

    The explicit `docker.io/...` form is REFUSED rather than matched:
    testcontainers-go cannot handle it coherently (`ExtractRegistry` normalises
    "docker.io" to its empty fallback, so its own docker.io exclusion never
    fires and it would build `<prefix>/docker.io/<image>`). Refusing is the only
    option that neither pre-warms a nonsense ref nor diverges silently.
    """
    script = ROOT / "ci" / "check_go.sh"
    source = script.read_text(encoding="utf-8")
    body = re.search(r"^mirrored_image\(\) \{.*?^\}", source, re.S | re.M)
    assert body, "mirrored_image not found in check_go.sh"

    def run(image: str, prefix: str) -> subprocess.CompletedProcess:
        program = (
            'die() { printf "ERROR: %s\\n" "$*" >&2; exit 2; }\n'
            + body.group(0)
            + f'\nmirrored_image "{image}"\n'
        )
        return subprocess.run(
            ["bash", "-c", program],
            capture_output=True,
            text=True,
            env={**os.environ, "TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX": prefix},
        )

    prefix = "ghcr.io/full-chaos"
    redirected = {
        "postgres:18-alpine@sha256:abc": f"{prefix}/postgres:18-alpine@sha256:abc",
        "clickhouse/clickhouse-server@sha256:d": f"{prefix}/clickhouse/clickhouse-server@sha256:d",
        "testcontainers/ryuk:0.14.0": f"{prefix}/testcontainers/ryuk:0.14.0",
    }
    for image, want in redirected.items():
        got = run(image, prefix)
        assert got.returncode == 0, got.stderr
        assert got.stdout.strip() == want, (image, got.stdout)

    # An image naming a real registry is left alone, exactly as prependHubRegistry
    # excludes it.
    for image in ("ghcr.io/x/y:1", "registry.hub.docker.com/library/postgres:1"):
        got = run(image, prefix)
        assert got.returncode == 0, got.stderr
        assert got.stdout.strip() == image, (image, got.stdout)

    # With no prefix configured, nothing is redirected -- local runs are unchanged.
    for image in redirected:
        got = run(image, "")
        assert got.returncode == 0, got.stderr
        assert got.stdout.strip() == image, (image, got.stdout)

    # The incoherent form is refused, loudly.
    refused = run("docker.io/postgres:1", prefix)
    assert refused.returncode != 0, refused.stdout
    assert "names docker.io explicitly" in refused.stderr, refused.stderr


def _mentions_mirror(value: Any) -> bool:
    """True if any nested string references the ghcr mirror.

    Uses the anchored `_MIRROR_HOST` matcher rather than a substring test. The
    earlier substring form was satisfied by `evil-ghcr.io/` and
    `ghcr.io.attacker.net/` -- CodeQL raised it as incomplete URL substring
    sanitization and was right.
    """
    if isinstance(value, str):
        return _MIRROR_HOST.search(_normalise_scheme(value)) is not None
    if isinstance(value, dict):
        return any(_mentions_mirror(v) for v in value.values())
    if isinstance(value, list):
        return any(_mentions_mirror(v) for v in value)
    return False


def _mirror_image_declarations(job: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    """Job-level `container:`/`services:` images that come from the mirror.

    These are NOT reachable by a login step: the runner pulls them BEFORE the
    first step executes, which is why GitHub requires declaration-level
    `credentials:` for a private image. A `docker/login-action` step cannot
    rescue them, so they need their own assertion rather than folding into the
    step-ordering one.
    """
    found: list[tuple[str, dict[str, Any]]] = []
    container = job.get("container")
    if isinstance(container, str):
        container = {"image": container}
    if isinstance(container, dict) and _mentions_mirror(container.get("image", "")):
        found.append(("container", container))
    services = job.get("services") or {}
    if isinstance(services, dict):
        for service_name, service in services.items():
            if isinstance(service, str):
                service = {"image": service}
            if isinstance(service, dict) and _mentions_mirror(service.get("image", "")):
                found.append((f"services.{service_name}", service))
    return found


_ALLOWED_IMAGE_REGISTRIES = ("ghcr.io/full-chaos/", "mirror.gcr.io/", "gcr.io/")

# EMPTY, and that is the point: CHAOS-4851 mirrored the last ten Docker Hub
# service images, so nothing is exempt. The mechanism stays because an empty
# exemption list is worth keeping honest -- each entry is (workflow, job, where,
# IMAGE), and the test asserts every entry is STILL PRESENT, so a debt slot
# cannot be quietly reused for a different image and a paid-off entry fails
# until it is deleted. Keying on the coordinate alone made it an exemption
# rather than a ratchet.
_UNMIRRORED_IMAGE_DEBT: set[tuple[str, str, str, str]] = set()

# Actions that pull an image themselves. `docker/setup-buildx-action` pulls
# `moby/buildkit:buildx-stable-1` from Docker Hub before any login runs -- the
# reason it was removed from the mirror workflow, where it deadlocked against
# the very quota that workflow exists to escape. Every entry below is
# ticketed debt (the test below asserts each one is STILL PRESENT, so the
# set stays a ratchet, not an allowlist) -- deliberately no hand-maintained
# count here (`len(_IMAGE_PULLING_ACTION_DEBT)` if one is ever needed): a
# hand-written count next to a growing set is exactly the merge hazard this
# session's own memory already flags -- it goes stale the moment an entry is
# added without anyone noticing the comment nearby.
_IMAGE_PULLING_ACTION_DEBT = {
    ("docker-images.yml", "build", "docker/setup-buildx-action"),
    ("docker-images.yml", "merge", "docker/setup-buildx-action"),
    ("docker-images.yml", "go-build", "docker/setup-buildx-action"),
    ("docker-images.yml", "go-merge", "docker/setup-buildx-action"),
    # CHAOS-5666: go-api-tools-build/go-api-tools-merge mirror
    # go-build/go-merge's exact shape (build-push-by-digest, then merge the
    # per-platform digests into one manifest list) for the one
    # dev-health-go-api-tools image -- same buildx-driver tradeoff as
    # those two, not a new one.
    ("docker-images.yml", "go-api-tools-build", "docker/setup-buildx-action"),
    ("docker-images.yml", "go-api-tools-merge", "docker/setup-buildx-action"),
    # CHAOS-4947: fan-in-latest applies moving tags via `docker buildx
    # imagetools create`, which -- like merge/go-merge above -- needs the
    # docker-container buildx driver, not the default one.
    ("docker-images.yml", "fan-in-latest", "docker/setup-buildx-action"),
    # CHAOS-4906: arc-runner-image.yml (PR #2154) needs a docker-container
    # buildx driver for cache-to: type=gha and for push+load together in one
    # build, neither of which the default docker-driven builder supports --
    # same tradeoff docker-images.yml already carries as debt above, not a
    # new one.
    ("arc-runner-image.yml", "build", "docker/setup-buildx-action"),
    # CHAOS-4906 (runner contract v1.6): the re-added (worker, linux/arm64)
    # self-hosted-pool pilot (ported from closed #2180 into v1.6 shape) --
    # same tradeoff as go-build/go-merge above, not a new one.
    #
    # CHAOS-5197: this job used to be TWO separate jobs -- an always-hosted
    # `go-build-worker-arm64` and an always-self-hosted
    # `go-build-worker-arm64-self-hosted` sibling, deny-list era. The
    # allow-list conversion consolidated them into ONE job (id kept as
    # `go-build-worker-arm64`, see docker-images.yml's own comment there)
    # whose `runs-on:` now routes via the allow-list ternary instead of two
    # separate job definitions -- so the `-self-hosted` entry this list used
    # to carry no longer names a real job and was deleted here, per this
    # test's own stale-entry error message ("delete them so the list cannot
    # rot into a silent allowlist").
    ("docker-images.yml", "go-build-dho-arm64", "docker/setup-buildx-action"),
}
_IMAGE_PULLING_ACTIONS = ("docker/setup-buildx-action",)


def _resolve_image_expressions(image: str) -> str:
    """Resolve the one GitHub expression we can prove, and only that one.

    `ghcr.io/${{ github.repository_owner }}/x` provably resolves to the allowed
    prefix in this repository. Every OTHER expression is left in place so the
    caller rejects it -- deliberately, because an unresolvable expression is an
    unknown shape and unknown shapes must fail.
    """
    return image.replace("${{ github.repository_owner }}", "full-chaos").replace(
        "${{github.repository_owner}}", "full-chaos"
    )


def _image_is_allowed(image: str) -> bool:
    """Allowlist, not blocklist. Anything not provably allowed is refused.

    THIS IS THE INVERSION. Eight fail-opens on this surface were all the same
    mistake: enumerate the dangerous spellings and let everything else through.
    An image can be named through run text, step env, job env, `container:`,
    `services:`, a matrix, a composite action, a reusable workflow or an
    expression like `format('{0}/{1}', 'ghcr.io', owner)` -- that set is
    unbounded and every round found another member of it.

    The approved registries are three and change roughly never, so the test
    enumerates THOSE and refuses everything else, including shapes nobody has
    thought of yet.
    """
    reference = _resolve_image_expressions(str(image)).strip()
    if "${{" in reference or "${" in reference:
        return False  # unresolvable => unknown => refused
    # Registry HOSTS are case-insensitive DNS names, so `GHCR.IO/full-chaos/x`
    # names our mirror and must not be refused. This compares prefixes rather
    # than calling `.startswith` on a casefolded string, because `casefold()`
    # can CHANGE LENGTH -- `'\u00df'.casefold() == 'ss'` -- and a match on the folded
    # form paired with a slice of the original mis-slices. Nothing is sliced
    # here, but the comparison is written the safe way so it stays correct if a
    # caller ever does.
    folded = reference.casefold()
    return any(
        folded[: len(allowed)] == allowed.casefold()
        for allowed in _ALLOWED_IMAGE_REGISTRIES
    )


def _all_image_declarations() -> list[tuple[str, str, str, str]]:
    """(workflow, job, where, image) for every image the RUNNER pulls.

    Job-level `container:` and `services:` only: those are pulled before the
    first step, which is what makes them un-rescuable by a login step. A step's
    `with: image:` (an SBOM scan of a locally built tag, say) is an action input,
    not a registry pull, and is out of scope by construction rather than by
    being overlooked.
    """
    found: list[tuple[str, str, str, str]] = []
    for path in _workflow_files():
        document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        for job_name, job in (document.get("jobs") or {}).items():
            if not isinstance(job, dict):
                continue
            container = job.get("container")
            if isinstance(container, str):
                container = {"image": container}
            if isinstance(container, dict) and container.get("image"):
                found.append(
                    (path.name, job_name, "container", str(container["image"]))
                )
            for service_name, service in (job.get("services") or {}).items():
                if isinstance(service, str):
                    service = {"image": service}
                if isinstance(service, dict) and service.get("image"):
                    found.append(
                        (
                            path.name,
                            job_name,
                            f"services.{service_name}",
                            str(service["image"]),
                        )
                    )
    return found


def test_every_pulled_image_resolves_to_an_approved_registry() -> None:
    """Allowlist every image the runner pulls; refuse unknown shapes.

    Replaces a guard that had been patched eight times, each time to recognise
    one more way of naming an image. The eighth was
    `${{ format('{0}/{1}/postgres:18-alpine', 'ghcr.io', github.repository_owner) }}`,
    which contains no literal `ghcr.io/` and so passed a literal check while
    pulling from the mirror without credentials.
    """
    offenders: list[str] = []
    seen_debt: set[tuple[str, str, str, str]] = set()

    for workflow, job_name, where, image in _all_image_declarations():
        # The image is PART of the key. Keying on the coordinate alone made this
        # an exemption rather than a ratchet: swapping any other Docker Hub
        # image into a debt slot kept the coordinate present and passed.
        key = (workflow, job_name, where, image)
        if key in _UNMIRRORED_IMAGE_DEBT:
            seen_debt.add(key)
            continue
        if not _image_is_allowed(image):
            offenders.append(
                f"{workflow}: {job_name}.{where} pulls {image!r}, which does not "
                f"resolve to an approved registry {_ALLOWED_IMAGE_REGISTRIES}. "
                "An unresolvable expression counts as unapproved."
            )

    stale = sorted(_UNMIRRORED_IMAGE_DEBT - seen_debt)
    assert not stale, (
        "ticketed image debt is stale -- these declarations no longer exist or "
        "were mirrored; delete them from _UNMIRRORED_IMAGE_DEBT so the list "
        f"cannot rot into a silent allowlist:\n  {stale}"
    )
    assert not offenders, "\n".join(offenders)


def test_mirror_pulled_container_images_declare_credentials() -> None:
    """A mirror image on `container:`/`services:` needs its own credentials.

    The runner pulls these BEFORE the first step, so no `docker/login-action`
    step can authenticate them; GitHub requires `credentials:` on the
    declaration itself.
    """
    offenders: list[str] = []
    for path in _workflow_files():
        document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        for job_name, job in (document.get("jobs") or {}).items():
            if not isinstance(job, dict):
                continue
            for where, declaration in _mirror_image_declarations(job):
                credentials = declaration.get("credentials") or {}
                if not credentials.get("username") or not credentials.get("password"):
                    offenders.append(
                        f"{path.name}: {job_name}.{where} pulls "
                        f"{declaration.get('image')!r} from the mirror without "
                        "`credentials:`; the runner pulls it before any login step"
                    )
    assert not offenders, "\n".join(offenders)


def test_no_unscanned_local_reusable_workflows_or_composite_actions() -> None:
    """Fail closed on indirection this file cannot see through.

    A `go.yml` job calling `./.github/workflows/hidden.yml`, whose callee runs
    `docker/login-action` with the registry omitted, is invisible to every guard
    here -- and an omitted registry IS Docker Hub. None exist today, so rather
    than write a scanner for a case with no instances, this REFUSES the
    indirection: adding one fails this test until the guards learn to follow it.
    """
    offenders: list[str] = []
    for path in _workflow_files():
        document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        for job_name, job in (document.get("jobs") or {}).items():
            if not isinstance(job, dict):
                continue
            if str(job.get("uses", "")).startswith("./"):
                offenders.append(
                    f"{path.name}: job {job_name!r} calls local reusable workflow "
                    f"{job.get('uses')!r}, which no guard in this file follows"
                )
            for step in job.get("steps") or []:
                if str(step.get("uses", "")).startswith("./"):
                    offenders.append(
                        f"{path.name}: {job_name} uses local action "
                        f"{step.get('uses')!r}, which no guard in this file follows"
                    )
    assert not offenders, (
        "\n".join(offenders)
        + "\n\nTeach the image/login guards to follow this indirection before adding it."
    )


def test_image_pulling_action_steps_are_allowlisted_or_ticketed() -> None:
    """Actions pull images too, and the allowlist could not see them.

    Two shapes reach a registry without any `container:`/`services:`
    declaration:

    * `uses: docker://<image>` -- a container action, pulled directly.
    * an action that pulls internally. `docker/setup-buildx-action` fetches
      `moby/buildkit:buildx-stable-1` from Docker Hub before any login step
      runs. That is not theoretical here: it deadlocked the mirror workflow
      against the very Docker Hub quota the mirror exists to escape, and was
      removed for exactly this reason.

    `docker://` refs must resolve to an approved registry. Internally-pulling
    actions are ticketed debt, asserted STILL PRESENT so the list cannot rot.
    """
    offenders: list[str] = []
    seen_debt: set[tuple[str, str, str]] = set()

    for path in _workflow_files():
        document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        for job_name, job in (document.get("jobs") or {}).items():
            if not isinstance(job, dict):
                continue
            for step in job.get("steps") or []:
                uses = str(step.get("uses", ""))
                if uses.startswith("docker://"):
                    image = uses[len("docker://") :]
                    if not _image_is_allowed(image):
                        offenders.append(
                            f"{path.name}: {job_name} runs container action "
                            f"{uses!r}, which does not resolve to an approved "
                            f"registry {_ALLOWED_IMAGE_REGISTRIES}"
                        )
                    continue
                action = uses.split("@", 1)[0]
                if action in _IMAGE_PULLING_ACTIONS:
                    key = (path.name, job_name, action)
                    if key in _IMAGE_PULLING_ACTION_DEBT:
                        seen_debt.add(key)
                    else:
                        offenders.append(
                            f"{path.name}: {job_name} uses {action!r}, which "
                            "pulls its own image from Docker Hub; mirror it or "
                            "add it to _IMAGE_PULLING_ACTION_DEBT with a ticket"
                        )

    stale = sorted(_IMAGE_PULLING_ACTION_DEBT - seen_debt)
    assert not stale, (
        "ticketed action debt is stale -- these steps no longer exist; delete "
        f"them so the list cannot rot into a silent allowlist:\n  {stale}"
    )
    assert not offenders, "\n".join(offenders)


# Docker subcommands that PULL an image, and those that provably do not. This is
# an allowlist too: an unrecognised subcommand is refused rather than assumed
# harmless, because the point of this file is that "assumed harmless" is how
# twelve fail-opens happened.
# `buildx` in this set is LOAD-BEARING, not an oversight. Two scanned workflows
# depend on it: mirror-test-images.yml runs `docker buildx imagetools create`
# against a Docker Hub SOURCE -- that is the mirror's entire job -- and
# docker-images.yml uses imagetools create/inspect. Removing `buildx` while
# tidying would fail the mirror and the image build on a required check.
_DOCKER_NON_PULLING_SUBCOMMANDS = frozenset(
    {
        "build",
        "buildx",
        "tag",
        "push",
        "save",
        "load",
        "login",
        "logout",
        "version",
        "info",
        "inspect",
        "rm",
        "rmi",
        "stop",
        "kill",
        "ps",
        "logs",
        "exec",
        "cp",
        "system",
        "ls",
        "list",
        "history",
        "prune",
        "df",
        "stats",
        "top",
        "port",
        "start",
        "restart",
        "pause",
        "unpause",
        "rename",
        "wait",
        "diff",
        "attach",
        "events",
        "commit",
        "export",
    }
)
# Which verb pulls an image depends on the NOUN it sits under. `create` pulls
# under `container` and bare (`docker create <image>`), but `docker volume
# create myvol` creates a named volume from no image at all -- and the flat verb
# set read `myvol` as an image, then reported it as unresolvable. Fail-closed,
# so nothing bad was ever admitted, but it would fail a legitimate workflow on a
# required check with a message naming the wrong cause.
#
# `run` has the same shape and is safe today only by accident: it exists solely
# under `container`. Encoding the dependence makes that deliberate.
_NOUN_PULLING_VERBS: dict[str, frozenset[str]] = {
    "": frozenset({"pull", "run", "create"}),  # bare `docker <verb>`
    "image": frozenset({"pull"}),
    "container": frozenset({"run", "create"}),
    "service": frozenset({"create"}),  # `docker service create <image>`
    "plugin": frozenset({"install"}),
}
# Nouns under which NO verb takes an image: they name a resource instead.
_NON_PULLING_NOUNS = frozenset(
    {
        "volume",
        "network",
        "config",
        "secret",
        "node",
        "context",
        "builder",
        "trust",
        "system",
        "manifest",
        "swarm",
    }
)
# Nouns whose verbs deploy from a FILE that names images. `docker stack deploy
# -c compose.yml` is the swarm analogue of `docker compose up`: it reads a
# compose file this scanner cannot see and pulls what the file names. Classing
# it as resource-naming was wrong in the same way, and in the same file, as
# `docker-compose` slipping past the refusal written for `docker compose`.
_COMPOSE_LIKE_VERBS: dict[str, frozenset[str]] = {"stack": frozenset({"deploy"})}
# The non-pulling verbs under a compose-like noun, ENUMERATED. The first cut let
# any verb that was not the deploying one through, which is the shape that put
# `image` in the exempt set: a category allowed wholesale because its known
# members looked harmless. A verb added under `stack` later would have passed
# unexamined. `config` is deliberately absent -- it reads a compose file, has no
# instances here, and refusing it is the fail-closed answer to "probably fine".
_COMPOSE_LIKE_SAFE_VERBS: dict[str, frozenset[str]] = {
    "stack": frozenset({"ls", "ps", "rm", "services"})
}
_COMPOSE_LIKE_NOUNS = frozenset(_COMPOSE_LIKE_VERBS)
_DOCKER_MANAGEMENT_NOUNS = (
    frozenset(_NOUN_PULLING_VERBS) - {""} | _NON_PULLING_NOUNS | _COMPOSE_LIKE_NOUNS
)
# Compose is refused rather than scanned; the legacy hyphenated binary is the
# same thing under another name and must not slip past that refusal.
_COMPOSE_BINARIES = frozenset({"docker-compose", "podman-compose", "nerdctl-compose"})
# The container tools this scans. A DECISION, not a completeness claim: `ctr`,
# `buildah` and `apptainer` are not scanned and have no instances here. The
# invariant is registry-scoped, so widening belongs with evidence of use.
_CONTAINER_BINARIES = frozenset({"docker", "podman", "nerdctl"})


def _run_pulled_images(command: str) -> tuple[list[str], list[str]]:
    """(images this run text pulls, reasons it could not be determined).

    `container:` and `services:` are not the only way a job reaches a registry:
    a step can simply `run: docker pull postgres:16`. That is a Docker Hub pull
    on a required check, and the declaration walk cannot see it.
    """
    images: list[str] = []
    unknown: list[str] = []
    # Join backslash continuations so a multi-line command is one command.
    joined = re.sub(r"\\\n\s*", " ", command)
    for raw in re.split(r"[\n;]|&&|\|\|", joined):
        line = raw.strip()
        if not _CONTAINER_TOOL.search(line):
            continue
        try:
            tokens = shlex.split(line, comments=True)
        except ValueError:
            unknown.append(f"unparseable shell: {line[:60]}")
            continue
        if any(Path(t).name in _COMPOSE_BINARIES for t in tokens):
            unknown.append(f"`docker-compose` invocation: {line[:60]}")
            continue
        tools = [i for i, t in enumerate(tokens) if Path(t).name in _CONTAINER_BINARIES]
        if not tools:
            continue
        for start in tools:
            rest = tokens[start + 1 :]
            words = [t for t in rest if not t.startswith("-")]
            sub = words[0] if words else None
            if sub is None:
                unknown.append(f"container tool with no subcommand: {line[:60]}")
                continue
            noun = ""
            if sub in _DOCKER_MANAGEMENT_NOUNS:
                # `docker image pull x` -> noun `image`, verb `pull`. A noun with
                # no verb after it is unknown, not harmless.
                if len(words) < 2:
                    unknown.append(f"`docker {sub}` with no verb: {line[:60]}")
                    continue
                noun, sub = sub, words[1]
                if sub in _COMPOSE_LIKE_VERBS.get(noun, frozenset()):
                    unknown.append(
                        f"`docker {noun} {sub}` deploys from a compose file whose "
                        f"images this cannot see: {line[:60]}"
                    )
                    continue
                if noun in _COMPOSE_LIKE_NOUNS:
                    # Allowlist, not fall-through. Only these enumerated verbs
                    # name a resource and pull nothing; anything else under this
                    # noun is unknown and refused.
                    if sub in _COMPOSE_LIKE_SAFE_VERBS.get(noun, frozenset()):
                        continue
                    unknown.append(
                        f"unrecognised verb `docker {noun} {sub}`: {line[:60]}"
                    )
                    continue
                if noun in _NON_PULLING_NOUNS:
                    # Nothing under this noun takes an image; the next token is a
                    # resource name, not a ref.
                    continue
            if sub == "compose":
                # Images live in the compose file, not here. Refused rather than
                # scanned: no workflow runs compose today, so a scanner would be
                # untested code, and adding one should fail loudly.
                unknown.append(f"`docker compose` invocation: {line[:60]}")
                continue
            if sub in _DOCKER_NON_PULLING_SUBCOMMANDS:
                continue
            if sub not in _NOUN_PULLING_VERBS.get(noun, frozenset()):
                where = f"`docker {noun} {sub}`" if noun else f"`docker {sub}`"
                unknown.append(f"unrecognised subcommand {where}: {line[:60]}")
                continue
            after = rest[rest.index(sub) + 1 :]
            image = None
            skip_next = False
            for token in after:
                if skip_next:
                    skip_next = False
                    continue
                if token.startswith("-"):
                    # Flags taking a value; "=" forms carry their own value.
                    if "=" not in token and token in {
                        "-v",
                        "--volume",
                        "-e",
                        "--env",
                        "-p",
                        "--publish",
                        "--name",
                        "--network",
                        "-w",
                        "--workdir",
                        "--platform",
                        "--entrypoint",
                        "-u",
                        "--user",
                        "--label",
                        "-l",
                    }:
                        skip_next = True
                    continue
                image = token
                break
            if image is None:
                unknown.append(f"`docker {sub}` with no image argument: {line[:60]}")
            else:
                images.append(image)
    return images, unknown


def test_run_steps_do_not_pull_from_unapproved_registries() -> None:
    """A `run:` step can reach a registry without any declaration.

    `run: docker pull postgres:16` is a Docker Hub pull on a required check, and
    the `container:`/`services:` walk is blind to it -- the same reach failure as
    the `*.yml`-only glob, one surface over. The stated invariant is "do not pull
    from Docker Hub", not "do not DECLARE a Docker Hub image".
    """
    offenders: list[str] = []
    for path in _workflow_files():
        document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        for job_name, job in (document.get("jobs") or {}).items():
            if not isinstance(job, dict):
                continue
            for index, step in enumerate(job.get("steps") or []):
                command = str(step.get("run", ""))
                if not command:
                    continue
                images, unknown = _run_pulled_images(command)
                for image in images:
                    if not _image_is_allowed(image):
                        offenders.append(
                            f"{path.name}: {job_name} step {index} pulls {image!r}, "
                            f"which does not resolve to {_ALLOWED_IMAGE_REGISTRIES}"
                        )
                for reason in unknown:
                    offenders.append(
                        f"{path.name}: {job_name} step {index}: {reason} -- an "
                        "image that cannot be determined is refused, not assumed safe"
                    )
    assert not offenders, "\n".join(offenders)


@pytest.mark.parametrize(
    "command,expected_refused",
    [
        # `image` and `container` are management NOUNS. Exempting the noun
        # exempted every verb beneath it, so the modern spelling of a pull was
        # silently allowed while the old spelling was caught.
        ("docker image pull postgres:16", True),
        ("podman image pull postgres:16", True),
        ("docker container run postgres:16", True),
        ("docker container create postgres:16", True),
        # The same forms against the mirror must still be ACCEPTED. A guard that
        # refuses valid syntax is as broken as one that admits a bad image, and
        # only the accepting half proves the noun is resolved rather than banned.
        ("docker image pull ghcr.io/full-chaos/postgres:18-alpine", False),
        ("docker container run ghcr.io/full-chaos/postgres:18-alpine", False),
        # Genuinely harmless verbs under a noun stay quiet.
        ("docker image rm stale", False),
        ("docker image ls", False),
        # A noun with no verb is unknown, and unknown is refused.
        ("docker image", True),
    ],
)
def test_management_noun_forms_resolve_to_their_verb(
    command: str, expected_refused: bool
) -> None:
    """`docker image pull` is `docker pull` spelled the modern way."""
    images, unknown = _run_pulled_images(command)
    refused = bool(unknown) or any(not _image_is_allowed(image) for image in images)
    assert refused is expected_refused, (
        f"{command!r}: refused={refused}, expected {expected_refused}. Management "
        "nouns must be resolved to the verb after them, not treated as verbs."
    )


@pytest.mark.parametrize(
    "command",
    [
        "docker compose up -d",
        "docker-compose up -d",
        "docker-compose -f tests/compose.yml up",
        "podman-compose up",
        "nerdctl-compose up",
    ],
)
def test_every_compose_spelling_is_refused(command: str) -> None:
    """Compose is refused rather than scanned -- in all of its spellings.

    Its images live in the compose file, so this scanner cannot see them. The
    hyphenated legacy binary is the same thing under another name: it matched
    the container-tool regex but not the basename check, so it passed silently
    while `docker compose` was correctly refused.
    """
    _, unknown = _run_pulled_images(command)
    assert unknown, (
        f"{command!r} was not refused. Compose images are invisible to this "
        "scanner, so every spelling of it must be refused rather than skipped."
    )


# Definitions in tests/tooling that are dead TODAY and not this ticket's to fix.
# Asserted STILL ORPHANED, so the list cannot rot into a silent exemption.
_KNOWN_ORPHANS: dict[str, frozenset[str]] = {
    # CHAOS-4843: _code_filter_patterns is no
    # longer orphaned -- test_paths_filter_covers_lefthook_yml now calls it.
    "test_aggregate_gate_results.py": frozenset({"_job", "_steps"}),
    # `pytestmark` is pytest's own module-level skip-marker hook (CHAOS-4976):
    # pytest's collector reads it by this exact reserved name, never by an
    # in-file reference, so it is structurally never "used" from this
    # detector's AST walk -- not dead code, a different class of orphan than
    # the row above. Every other tests/tooling module happens not to use this
    # (otherwise-standard, used elsewhere under tests/) pytest idiom yet, so
    # the detector has no reason to special-case the name globally today.
    "test_deploy_prod_go_workers_profile_coverage.py": frozenset({"pytestmark"}),
}


def _module_level_definitions_and_uses(tree: Any) -> tuple[set[str], set[str]]:
    """(defined, used) for one parsed module.

    `test_*` functions and pytest fixtures are excluded: pytest calls those, so
    they are never referenced by name. Dunders are excluded.
    """
    import ast

    defined: set[str] = set()
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.name.startswith("test_"):
                continue
            if any(
                (isinstance(d, ast.Attribute) and d.attr == "fixture")
                or (isinstance(d, ast.Name) and d.id == "fixture")
                or (
                    isinstance(d, ast.Call)
                    and isinstance(d.func, ast.Attribute)
                    and d.func.attr == "fixture"
                )
                for d in node.decorator_list
            ):
                continue
            defined.add(node.name)
        targets = (
            node.targets
            if isinstance(node, ast.Assign)
            else ([node.target] if isinstance(node, ast.AnnAssign) else [])
        )
        defined |= {
            t.id
            for t in targets
            if isinstance(t, ast.Name) and not t.id.startswith("__")
        }

    used = {
        n.id
        for n in ast.walk(tree)
        if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load)
    }
    used |= {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)}
    used |= {
        a.asname or a.name
        for n in ast.walk(tree)
        if isinstance(n, (ast.Import, ast.ImportFrom))
        for a in n.names
    }
    return defined, used


def test_no_orphaned_module_level_definitions_in_tooling_tests() -> None:
    """No definition in tests/tooling may be defined and never used.

    HOISTED from one file to the directory. The per-file version watched only
    the module it lived in, so a second guard file was entirely unwatched -- and
    `SHARED_JOB_ID` went orphaned in that unwatched file within hours of the
    detector shipping, when the test consuming it was removed. A detector that
    watches one file reports "no orphans" for the whole directory.

    The name filter widened with it: the first version required a leading
    underscore, so `SHARED_JOB_ID` would have been invisible even in the file
    already watched. Two blind spots, one symptom.
    """
    import ast

    directory = Path(__file__).resolve().parent
    offenders: list[str] = []
    stale: list[str] = []
    control_function = False
    control_constant = False

    for path in sorted(directory.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        defined, used = _module_level_definitions_and_uses(tree)

        # CONTROLS, one per definition kind, on this module where both are known
        # live. A control covering one kind certifies one kind while reading as
        # certifying the detector -- which is how the constant half shipped blind.
        if path.name == Path(__file__).name:
            control_function = "_image_is_allowed" in used
            control_constant = "_ALLOWED_IMAGE_REGISTRIES" in used

        known = _KNOWN_ORPHANS.get(path.name, frozenset())
        orphans = defined - used
        offenders += [f"{path.name}: {n}" for n in sorted(orphans - known)]
        stale += [f"{path.name}: {n}" for n in sorted(known - orphans)]

    assert control_function, (
        "detector control failed: a known-called function was not seen as used; "
        "the function half of the orphan list cannot be trusted"
    )
    assert control_constant, (
        "detector control failed: a known-used constant was not seen as used; "
        "the constant half cannot be trusted"
    )
    assert not stale, (
        "_KNOWN_ORPHANS is stale -- these are no longer orphaned; delete the "
        f"entries rather than letting the list rot into a silent exemption:\n  {stale}"
    )
    assert not offenders, (
        f"definition(s) defined and never used: {offenders}. Wire them in or "
        "delete them -- an uncalled helper in a guard file reads as protection "
        "while providing none."
    )


@pytest.mark.parametrize(
    "command,expected_refused",
    [
        # A resource name is not an image. `create` pulls under `container` and
        # bare, but `docker volume create myvol` names a volume -- the flat verb
        # set read `myvol` as an image and reported it unresolvable, failing a
        # legitimate workflow with a message naming the wrong cause.
        ("docker volume create myvol", False),
        ("docker network create ci-net", False),
        ("docker volume create --label keep=1 myvol", False),
        ("docker secret create tls ./cert.pem", False),
        ("docker config create app ./app.conf", False),
        # ...while `create` under container and bare STILL pulls.
        ("docker create postgres:16", True),
        ("docker container create postgres:16", True),
        ("docker create ghcr.io/full-chaos/postgres:18-alpine", False),
        ("docker container create ghcr.io/full-chaos/postgres:18-alpine", False),
        # An unrecognised verb under a known noun stays fail-closed.
        ("docker container frobnicate x", True),
    ],
)
def test_pulling_verbs_are_resolved_per_noun(
    command: str, expected_refused: bool
) -> None:
    """Which verb pulls depends on the noun above it.

    `run` exists only under `container`, so the flat set was safe for it by
    accident rather than by design; encoding the dependence makes that
    deliberate and fixes `create`, which genuinely differs by noun.
    """
    images, unknown = _run_pulled_images(command)
    refused = bool(unknown) or any(not _image_is_allowed(image) for image in images)
    assert refused is expected_refused, (
        f"{command!r}: refused={refused}, expected {expected_refused} "
        f"(images={images}, unknown={unknown}). A resource name must not be "
        "read as an image, and a real image must still be checked."
    )


@pytest.mark.parametrize(
    "command,expected_refused",
    [
        # `docker stack deploy -c file` is the swarm analogue of `docker compose
        # up`: it deploys from a compose file naming images this scanner cannot
        # see. Classing `stack` as resource-naming was the same error, in the
        # same file, as `docker-compose` slipping past the `docker compose`
        # refusal -- same shape, opposite treatment.
        ("docker stack deploy -c compose.yml mystack", True),
        ("docker stack deploy --compose-file x.yml svc", True),
        # ...but only the file-deploying verb. These name a resource and pull
        # nothing; refusing them would be a false failure on a valid workflow.
        # One accepting row per ENUMERATED safe verb: these are what the
        # allowlist admits, and an accepting row is the only thing that proves
        # the guard is resolving rather than banning.
        ("docker stack ls", False),
        ("docker stack ps mystack", False),
        ("docker stack rm mystack", False),
        ("docker stack services mystack", False),
        # ...and anything NOT enumerated is refused, including a verb that does
        # not exist. The previous cut allowed every non-deploying verb, so a
        # pulling verb added under `stack` later would have passed unexamined --
        # the same shape as `image` sitting in the exempt set.
        ("docker stack frobnicate mystack", True),
        ("docker stack config -c compose.yml", True),
    ],
)
def test_compose_like_nouns_refuse_only_their_deploying_verb(
    command: str, expected_refused: bool
) -> None:
    """A file-deploying verb is unknowable; its siblings are not."""
    images, unknown = _run_pulled_images(command)
    refused = bool(unknown) or any(not _image_is_allowed(image) for image in images)
    assert refused is expected_refused, (
        f"{command!r}: refused={refused}, expected {expected_refused} "
        f"(images={images}, unknown={unknown})."
    )


def test_exactly_one_workflow_declares_the_required_go_quality_job() -> None:
    """CHAOS-4834's acceptance criterion, and the whole point of the change.

    `go-quality` is a REQUIRED check. It used to be declared by TWO workflows --
    go.yml (path-filtered, does the work) and go-quality-noop.yml (paths-ignore,
    reports a vacuous success). Both write to the same required context, and
    GitHub's `paths` fires when ANY changed file matches while `paths-ignore`
    fires when ANY changed file does not, so **any mixed change set triggers
    both**. The no-op finishes in seconds and the real gate takes minutes, so the
    meaningless green always lands first.

    Observed live on 2026-09-02 at 15:21Z (lane-4441, PR #2103, SHA 5508d9b4e):

        33647995023  completed/success  Go (non-Go changes)   <- no-op, ALREADY GREEN
        33647996503  queued             Go                    <- real workflow, not started

    Widening the path list fixes which changes are *seen*; it does not remove the
    duplicate producer. Only one workflow declaring the job does that -- and with
    one producer there is no tie for the ruleset to break, which is why we never
    had to determine whether it resolves several same-named contexts by
    all-must-pass, latest-wins, or first-wins.
    """
    declaring = []
    for path in _workflow_files():  # both .yml and .yaml, unlike the
        # branch this was written on, which globbed one extension
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
        if "go-quality" in (document.get("jobs") or {}):
            declaring.append(path.name)

    assert declaring == ["go-quality.yml"], (
        f"exactly one workflow may declare the required `go-quality` job; found {declaring}. "
        "Two producers means the required context can be satisfied by whichever "
        "reports first, which is not necessarily the one that ran the gate."
    )


def test_the_go_quality_job_always_reports() -> None:
    """One producer is only safe if that producer cannot be filtered out.

    If the single declaring workflow carried a `paths` filter, a non-Go change
    would produce NO `go-quality` context at all and the required check would
    block forever -- which is the problem the no-op existed to solve. Relevance is
    decided inside the job instead, so the context always appears and says
    honestly whether the gate ran.
    """
    document = yaml.safe_load(
        (ROOT / ".github" / "workflows" / "go-quality.yml").read_text(encoding="utf-8")
    )
    on_block = document.get(True, document.get("on"))
    for event in ("pull_request", "push"):
        trigger = on_block.get(event) or {}
        assert "paths" not in trigger, (
            f"go-quality.yml must not filter {event} by paths: a filtered required "
            "check never reports on the changes it filters out."
        )
        assert "paths-ignore" not in trigger, (
            f"go-quality.yml must not filter {event} by paths-ignore either."
        )


@pytest.mark.parametrize(
    "changed,expected_relevant",
    [
        # `**/` matches ZERO OR MORE leading directories. Translating it as
        # `.*/` demanded at least one, so every ROOT-LEVEL Go file was judged
        # irrelevant and the gate skipped -- a false green produced by the
        # mechanism that replaced the vacuous no-op, on the change class most
        # likely to break the build.
        ("root.go", True),
        ("go.mod", True),
        ("go.sum", True),
        # ...and the nested forms it already handled must keep working.
        ("internal/x/y.go", True),
        ("cmd/dev-health-worker/main.go", True),
        ("testdata/a.json", True),
        ("internal/x/testdata/a.json", True),
        # The negative control: without it this only shows a decider that
        # returns true.
        ("docs/README.md", False),
    ],
)
def test_root_level_paths_match_the_double_star_prefix(
    changed: str, expected_relevant: bool
) -> None:
    """`**/*.go` must match `root.go`, not only `dir/root.go`."""
    import subprocess

    result = subprocess.run(
        ["python3", str(ROOT / "ci" / "go_relevance.py")],
        input=changed + "\n",
        capture_output=True,
        text=True,
        cwd=str(ROOT),
    )
    assert result.returncode == 0, f"go_relevance.py failed: {result.stderr[:300]}"
    relevant = "relevant=true" in result.stdout
    assert relevant is expected_relevant, (
        f"{changed!r}: relevant={relevant}, expected {expected_relevant}. "
        "A root-level Go change judged irrelevant skips the gate entirely."
    )
