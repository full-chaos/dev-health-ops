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
    "cmd/dev-health-reconciler",
    "cmd/dev-health-worker",
    "cmd/dev-health-workerctl",
    "cmd/query-api",
    "cmd/query-api/internal/analytics",
    "cmd/query-api/internal/hotspots",
    "cmd/query-api/internal/routeswitch",
    "cmd/query-api/internal/workgraph",
    "internal/cacheinvalidation",
    "internal/externalrecompute",
    "internal/joboperator",
    "internal/joboutbox",
    "internal/jobrescue",
    "internal/jobroute",
    "internal/jobruntime",
    "internal/jobs/investment/chquery",
    # CHAOS-4441: the ClickHouse writer for investment.materialize's three
    # ReplacingMergeTree tables. Its correctness claims (dedup-before-filter,
    # sub-millisecond version distinctness, org-scoping) are properties of the
    # real engine, so a fake connection cannot prove them.
    "internal/jobs/investment/chwrite",
    "internal/jobs/metrics/daily",
    "internal/jobs/metrics/remaining",
    "internal/jobs/pagerduty",
    "internal/jobs/report",
    "internal/jobs/system",
    "internal/jobs/workgraph",
    "internal/jobs/workgraph/edges",
    "internal/jobs/workgraph/issueprlinks",
    "internal/providerfoundation",
    "internal/providersync",
    "internal/scheduler/fixed",
    "internal/scheduler/sync",
    "internal/storage/postgres",
    # CHAOS-4882: the auth-owned schema's migration lineage. Its suite starts
    # a real PostgreSQL and connects AS the runtime role to prove DDL and
    # cross-schema access are refused, so it is integration-tagged.
    "internal/storage/postgres/authschema",
    "internal/storage/river",
    "internal/streamhandlers",
    "internal/streamrunner",
    "internal/syncdispatchruntime",
    "internal/syncreconciler",
    "internal/synccoverage",
    "internal/syncroute",
    "internal/testsupport/computeparity",
    "internal/testsupport/containers",
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
    # CHAOS-4352 (batch analytics port, commit 634414b0a) added
    # cmd/query-api/internal/analytics (32 -> 33): nan_class_live_test.go
    # is integration-tagged because it proves the ClickHouse NaN-class
    # breakdown live against a real container. This literal drifted stale
    # across the Wave 5 epic rebase/main-merge chain before anyone landed
    # the re-pin -- reproduced red on the base tip (e9ea257ff, pre-rebase)
    # and on this branch's own merge tip (da9aadadb) before re-pinning,
    # per root AGENTS.md's "never label a red check unrelated without
    # running it on the base SHA".
    # CHAOS-4643 denylisted cmd/query-api/internal/analytics out of the
    # shard manifest (still discovered, no longer runnable/shardable: 33
    # discovered, 32 will run) -- CI's integration-shard job never sets
    # CLICKHOUSE_URI, which nan_class_live_test.go requires directly (unlike
    # every sibling package, which gets its own ClickHouse via
    # testcontainers), so the enrolled test skipped on every CI run and the
    # skip reported as a pass. It remains a discretionary, slot-only proof
    # per orchestrator ruling 2026-08-29 (see the file's own STATUS header),
    # now run explicitly with CLICKHOUSE_URI + DEV_HEALTH_REQUIRE_LIVE=1, not
    # via this gate.
    #
    # CHAOS-4684 added cmd/query-api/internal/hotspots (33 -> 34 discovered,
    # 32 -> 33 will run): the argMax(<col>, (day, computed_at)) tie-break
    # regression guard runs against a REAL ClickHouse container (a fake
    # QueryClient cannot exercise how ClickHouse itself resolves an argMax
    # tie), so the package picked up its first -tags=integration file.
    # Weight 26s, measured locally across both top-level test functions
    # (the tie-break proof plus a second, codex-round-1-found regression
    # guard for a NULL-blame_concentration mixed-day row, each starting
    # its own container); LPT re-balanced shards 2/3 to 364s/363s (still
    # within 1s).
    #
    # CHAOS-4730 un-denylisted cmd/query-api/internal/analytics (34
    # discovered unchanged, 33 -> 34 will run, 1 -> 0 denylisted): the
    # CHAOS-4643 premise (the package's ONLY integration file could never
    # run in CI) no longer holds -- the SETTINGS max_execution_time =
    # {timeout:UInt64} native-parameter defect (parses fine on 26.7/prod,
    # fails to PARSE with Code: 62 on the pinned 26.6.1.1193 Testcontainers
    # image and on CI's 24.8) was fixed package-wide via one shared
    # literal-rendering helper, and the package now has two REAL
    # Testcontainers-backed regression tests
    # (breakdown_seeded_integration_test.go,
    # investmentquality_seeded_integration_test.go) that execute for real
    # in CI. nan_class_live_test.go and investmentquality_live_test.go stay
    # deliberately opt-in-live, each skipping with a message naming the env
    # var it needs -- that pattern (opt-in-live-with-a-named-skip inside an
    # otherwise real, executing package) is what CHAOS-4643 was never
    # objecting to; its complaint was a package whose ENTIRE integration
    # coverage was a permanent, silent skip. Weight 22s, ceil() of a local
    # run of both new tests together (21.575s, each starting its own
    # container); LPT re-balanced shards 2/3 to 374s/375s (still within
    # 1s).
    # CHAOS-4655 added cmd/query-api/internal/workgraph (34 -> 35
    # discovered, 34 -> 35 will run): the batch-membership pair-bound-match
    # fix (a cross-product-over-fetch correctness bug, not covered by any
    # fake-client unit test) needed a real-engine red/green proof plus an
    # adversarial round-trip test against a real ClickHouse container, so
    # the package picked up its first -tags=integration files. Weight 60s,
    # ceil() of a local run of both tests together (each starting its own
    # container).
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
    # Two packages landed independently, each written as 37 -> 38 on its own
    # branch: CHAOS-4882's internal/storage/postgres/authschema and
    # CHAOS-4769's internal/jobs/workgraph/issueprlinks. The merged total was
    # 39. CHAOS-4441 then added internal/jobs/investment/chwrite: 39 -> 40.
    assert "40 package(s) discovered, 0 denylisted, 40 will run" in result.stdout
    assert "integration shard plan: 3 shard(s), 40 package(s)" in result.stdout

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
    }
    assert len(matrix) == 6

    assignments: dict[int, set[str]] = {}
    for line in result.stdout.splitlines():
        if not line.startswith("  SHARD "):
            continue
        _, shard, package, _weight = line.split()
        assignments.setdefault(int(shard), set()).add(package)

    assert set(assignments) == {1, 2, 3}
    flattened = [package for packages in assignments.values() for package in packages]
    # CHAOS-4441: 36, not 35 -- internal/jobs/investment/chquery added. This
    # is the FLATTENED set across all shards, so unlike the selected-package
    # count above it INCLUDES the providersync shard-1 package.
    # CHAOS-4766: 37, not 36 -- internal/jobs/workgraph/edges added.
    # CHAOS-4882 and CHAOS-4769: 39, not 37 -- BOTH landed independently,
    # internal/storage/postgres/authschema and
    # internal/jobs/workgraph/issueprlinks, each written as 37 -> 38 on its
    # own branch. FLATTENED includes the providersync shard-1 package.
    # CHAOS-4441: 40, not 39 -- internal/jobs/investment/chwrite added.
    assert len(flattened) == len(set(flattened)) == 40
    assert set(flattened) == EXPECTED_PACKAGES
    assert assignments[1] == {"internal/providersync"}

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
    assert set(estimated) == {1, 2, 3}
    assert abs(estimated[2] - estimated[3]) <= 1

    expected_provider_tests = _providersync_top_level_tests()
    expected_integration_tests = _providersync_integration_tagged_tests()
    # CHAOS-4060 added 5 top-level tests to internal/providersync:
    # TestExecutedProofSatisfiedRequiresProofUnlessWaivedOrNeverAttempted,
    # TestExecutedProofEvidenceHasExecutedProofIsCaseInsensitiveOnMatrixKey,
    # TestExecutedProofSatisfiedDegradedRevokesOnlyTheNeverAttemptedPassThrough,
    # and TestExecutedProofEvidenceHasBeenAttempted (ordinary), plus
    # TestQueryExecutedProofEvidenceDistinguishesRealFromEmptySuccess
    # (integration-tagged).
    #
    # CHAOS-3092 P0a then removed 3 ordinary top-level tests (935 -> 932):
    # TestDiffRowsClauseCoverage, TestCheckExclusionIntegrityClauseCoverage and
    # TestTypedValuesEqualCanonicalizesFloatAndDatetimeText moved to
    # internal/testsupport/oraclecompare with the pure comparison functions
    # they cover. They still run, in that package, on the same `go test` tier
    # -- they are no longer PROVIDERSYNC tests, which is all this count
    # measures. The integration-tagged count is unchanged because all three
    # were ordinary.
    #
    # CHAOS-4130 then added 9 ordinary top-level tests (932 -> 941): six in
    # github_tests_page_budget_test.go and three in
    # gitlab_tests_page_budget_test.go, covering first-entry page counting, the
    # two independent budget-stop branches on both providers, and the drift
    # guard tying the dataset registry to the metric label allowlist. All nine
    # are ordinary, so the integration-tagged count is unchanged.
    #
    # CHAOS-4114 then added 3 INTEGRATION-tagged top-level tests (941 -> 944,
    # 110 -> 113) in executed_proof_ledger_integration_test.go:
    # TestPostgresCompleteStampsExecutedProofLedger,
    # TestExecutedProofLedgerAttemptedIsMonotoneAndNeverClobbersProof and
    # TestExecutedProofLedgerRefusesUnnormalizedIdentity. Both counts move by
    # the same 3 because all three are integration-tagged -- unlike 4130's
    # nine, which moved only the first.
    #
    # CHAOS-3978 then added 10 ordinary top-level tests (944 -> 954), all in
    # work_item_cross_provider_donor_test.go, covering the cross-provider
    # stored-donor-edge rescue, its red control, the stored-edge-before-donor-
    # targets ordering, the Python-parity pruning key, the retype backstop, the
    # D17 fail-closed and foreign-tenant rails, the rescue observation, the
    # deriver-to-route observation path, the bounded/retried ClickHouse read,
    # and the rejected-review legacy-semantics decision. None is
    # integration-tagged, so the integration count is unchanged -- the same
    # shape as CHAOS-4130's nine, not CHAOS-4114's three.
    #
    # CHAOS-4142 then added 17 ordinary top-level tests (954 -> 971): twelve in
    # github_tests_per_run_cap_test.go and five in
    # gitlab_tests_per_run_cap_test.go, covering both cap directions at each
    # per-run site (jobs, artifacts, report rows), the watermark
    # window-blocking classification over (component, cause) PAIRS, BOTH
    # directions of the bidirectional comparator invariant (nil iff
    # window-blocking), the closed per-run vocabulary on both providers, and
    # the codex round-1 regressions: a nested page-budget stop withholding the
    # watermark on each provider, and the report-row bound holding both when
    # one artifact is oversized and when several small ones would otherwise
    # creep past it. All seventeen are ordinary -- they drive the routes
    # through in-memory HTTP doers and touch no database -- so the
    # integration-tagged count stays 113, the way 4130's nine did.
    #
    # CHAOS-4142 codex round 2 then added 6 more ordinary top-level tests
    # (971 -> 977): three per provider, pinning the per-run PAGE BUDGET against
    # the item cap at its exact equality boundary, pinning the refutation of
    # codex's challenge-1 reading -- a combined item-cap + page-budget stop is
    # classified as the item cap and advances, because the committed prefix does
    # not depend on the page budget -- and the RED-FIRST pair asserting that a
    # starved per-run page budget never finalizes a unit with a withheld
    # watermark, which is the stall stated as an outcome rather than a
    # mechanism.
    #
    # (The seven new providerfoundation tests -- two paginator stop-reason,
    # three per-run truncation metric, and two from round 2 pinning stop-reason
    # mutual exclusion and prefix independence -- live in
    # internal/providerfoundation and so do not move this providersync count.)
    #
    # CHAOS-4177 then added 4 more ordinary top-level providersync tests
    # (977 -> 981): the healthy-artifacts control, the chunked and non-chunked
    # corrupt-artifact pair asserting that one unreadable archive is skipped
    # rather than sinking the unit, and the counter test asserting the skip
    # reaches dev_health_provider_artifact_skipped_total through the route.
    # All four drive the routes through in-memory HTTP doers and touch no
    # database, so the integration-tagged count stays 113.
    #
    # Codex review then added 1 more (981 -> 982): the chunked route's own
    # fail-closed assertion for a blocking archive-bounds issue, which the
    # retargeted oracle test covered only on the non-chunked twin.
    #
    # (The one new providerfoundation test bounding the artifact skip reason
    # label lives in internal/providerfoundation and so does not move this
    # providersync count.)
    #
    # CHAOS-4177 part 2 then added 5 more ordinary top-level providersync tests
    # (982 -> 987): the within-page control, the shrunk-page red test and its
    # artifacts-phase twin, and the counted/not-counted pair pinning that a
    # re-anchor is recorded and an unmoved page is not. All five drive the
    # routes through in-memory HTTP doers and touch no database, so the
    # integration-tagged count stays 113.
    #
    # Its review round then added 2 more (987 -> 989): a page that shrank
    # EXACTLY to the stored index, which addresses nothing and was previously
    # walked from the end silently, and the index-0-on-an-empty-page case that
    # must NOT be reported as a re-anchor.
    #
    # CHAOS-4190 then added 6 more ordinary top-level providersync tests
    # (989 -> 995): the cross-artifact same-name-suite/case natural-key
    # collision pair, the duplicate-natural-key cause-erasure test, the LCOV
    # and Cobertura coverage-snapshot cross-artifact collision pair, and the
    # GitLab cross-job coverage collision test. All six drive parsing/route
    # code directly or through in-memory HTTP doers and touch no database, so
    # the integration-tagged count stays 113.
    #
    # CHAOS-4191 then added 7 more ordinary top-level providersync tests
    # (995 -> 1002): a per-artifact skip-and-continue pair (chunked + oracle)
    # for a download redirect with no Location header, its telemetry-counted
    # twin, a read-failure and an oversized-download cause-attach pair each
    # driven through both the chunked route and the non-chunked oracle. All
    # seven drive the routes through in-memory HTTP doers and touch no
    # database, so the integration-tagged count stays 113.
    #
    # CHAOS-4194 then added 5 more ordinary top-level providersync tests
    # (1002 -> 1007): the GitHub Projects V2 pull-request board-membership
    # emission and its re-sync-stable event_id, plus three covering the
    # structured membership-skip log (its fields, its silence when nothing was
    # skipped, and the INFO/WARN level split). All five drive the fetcher
    # through in-memory HTTP doers and a captured slog handler and touch no
    # database.
    #
    # It also added ONE integration-tagged test (1007 -> 1008 total, 113 -> 114
    # integration): the reachability proof that a fetched pull-request board
    # item travels through the effects builder and the ClickHouse adapters into
    # the migrated tables. That one genuinely provisions a container, which is
    # why it moves the integration count and the other five do not.
    #
    # Then 2 more ordinary tests (1008 -> 1010): the superseded prepared
    # snapshot must be distinguishable from tampering, and only an untouched
    # document may be discarded. Both are pure predicate tests over in-memory
    # ledger state and touch no database, so the integration-tagged count stays
    # 114.
    #
    # CHAOS-4219 then added 5 more ordinary top-level providersync tests
    # (1010 -> 1015), all in pagerduty_incident_entitlement_test.go: the two
    # PagerDuty seam tests (refused before provider fetch; revoked grant
    # re-checked at the ClickHouse write boundary), the two sweeps asserting
    # every PagerDuty route handler and every PagerDuty sink carry the
    # re-check, and the completeness guard tying both sweeps to the native_go
    # pagerduty pairs in the capability matrix. All five drive the routes
    # through in-memory HTTP doers and an unreachable ClickHouse conn and
    # touch no database, so the integration-tagged count stays 114. (The
    # renamed entitlement files -- incident_entitlement_integration_test.go
    # and incident_entitlement_oracle_test.go -- keep their one test each.)
    #
    # Its codex round then added 2 more ordinary tests (1015 -> 1017) in
    # incident_entitlement_test.go: the unavailable-vs-disabled taxonomy pin
    # (a store fault must retry, never terminalize as feature_disabled) and
    # the one-structured-refusal-log-line pin. Both drive the entitlement
    # through a fake row scanner and a captured slog handler; the end-to-end
    # reachability proof lives in cmd/dev-health-worker and does not move
    # this count. Integration-tagged stays 114.
    #
    # CHAOS-4185 then added 11 more ordinary top-level providersync tests
    # (1017 -> 1028) in github_tests_all_artifacts_unreadable_test.go: the
    # totality-fires and below-the-floor pair, the partial-degradation
    # non-firing control, four decode-invariant tests for the new
    # ArchivesSeen/ArchivesUnreadable cursor counters, the legacy-cursor
    # (pre-deploy, unknown counters) non-firing test, the cross-continuation
    # accumulation test, the done-resume no-reevaluate/no-double-count test,
    # and the re-anchor-replay equality-preservation test. All eleven drive
    # the chunked route through in-memory HTTP doers and touch no database,
    # so the integration-tagged count stays 114. (The reachability proof
    # through the production handler lives in cmd/dev-health-worker and does
    # not move this count, matching CHAOS-4219's precedent above.)
    #
    # Its review round then added 1 more ordinary test (1028 -> 1029): the
    # structured-log-fields pin for the totality gate's slog.Error line
    # (org/dataset/sync_run_id/unit/repository/seen/unreadable), asserted on
    # the captured record's attributes rather than rendered text -- the same
    # observability standing order CHAOS-4194's membership-skip log tests
    # apply. Drives the chunked route through an in-memory HTTP doer and
    # touches no database, so the integration-tagged count stays 114.
    #
    # A first codex adversarial round (HIGH) then found a re-anchor whole-page
    # replay could re-download and re-count an artifact an earlier attempt
    # already reflected in ArchivesSeen/ArchivesUnreadable, crossing the
    # totality floor on a genuinely single real unreadable artifact. The fix
    # (a genuine artifacts-phase re-anchor poisons the totality counters to
    # UNKNOWN for the rest of the walk) replaced the prior
    # TestGitHubTestsAllArtifactsUnreadableSurvivesReanchorReplay test in
    # place (net 0) and added ONE new control test, +1 (1029 -> 1030):
    # TestGitHubTestsAllArtifactsUnreadableOrdinaryResumeKeepsCountersKnown,
    # proving an ordinary (non-shrinking) resume keeps its known counters and
    # still detects a genuine totality failure -- i.e. the fix poisons ONLY on
    # a genuine re-anchor, not on every resume. Both drive the chunked route
    # through in-memory HTTP doers and touch no database, so the
    # integration-tagged count stays 114. (The same round's two MEDIUM
    # findings -- moving the metric to only fire after providerunit's durable
    # Fail succeeds, and triaging the "malformed cursor" finding as the
    # ticket's own already-accepted bypass trade-off, not a new defect -- add
    # tests in internal/jobs/providerunit, a package this providersync-scoped
    # census does not track.)
    #
    # A second codex round (MEDIUM) then found a 2xx download with a TRULY
    # EMPTY body silently continued without incrementing ArchivesUnreadable
    # or recording any incomplete evidence -- ArchivesSeen grew while
    # ArchivesUnreadable stayed 0, so the totality gate never fired and a
    # broken proxy/edge answering every artifact with an empty body could
    # finalize the unit as healthy having ingested zero report rows. +1
    # ordinary test (1030 -> 1031):
    # TestGitHubTestsEmptyArtifactBodiesCountAsUnreadable. Drives the chunked
    # route through an in-memory HTTP doer and touches no database, so the
    # integration-tagged count stays 114.
    #
    # A third (final) codex round then found that fix regressed a ROUTINE
    # provider condition: downloadGitHubTestsArtifact returns the identical
    # (nil-archive, no-error) shape for BOTH a genuine 2xx-empty body and an
    # ordinary 404/410 (an artifact that expired or was deleted between
    # listing and download, which GitHub documents as a normal response) --
    # so two routine vanished artifacts would satisfy the totality floor and
    # terminalize a healthy unit. The fix gives the downloader a `notFound`
    # return so the chunked route can exclude a routine 404/410 from totality
    # accounting entirely (neither seen nor unreadable), while a genuinely
    # empty 2xx body still counts. +2 ordinary tests (1031 -> 1033):
    # TestGitHubTestsRoutineNotFoundArtifactsDoNotFireTotality (404 and 410,
    # subtests) and TestGitHubTestsMixedNotFoundAndUnreadableCountsOnlyTheObserved
    # (one not-found artifact alongside two genuinely unreadable ones must
    # still fire on exactly the two observed). Both drive the chunked route
    # through in-memory HTTP doers and touch no database, so the
    # integration-tagged count stays 114.
    #
    # CHAOS-4193 then added 17 more ordinary tests (1033 -> 1050) for the
    # Jira and Linear project-membership producers: pure decision tests over
    # jiraProjectMoveItems/resolveJiraProjectCatalog (changelog move
    # extraction, the current-project key fallback and its caching/ordering
    # correctness, the known-id-no-name case) and over
    # normalizeLinearProjectMemberships/linearProjectMembershipRowValid/
    # mergeLinearProjectCatalogNames (history extraction, removal rows,
    # epoch-anchored catalog versioning, cross-issue name merging). None
    # touch a real database -- the HTTP lookups run through in-memory doers,
    # and the sink-adapter/normalizer predicates are exercised directly.
    #
    # Then 1 more integration-tagged test (1050 -> 1051, 114 -> 115): the
    # team-lead-ruled replay-idempotency proof for the lease-retry gap
    # (CHAOS-4247 follow-up) -- commits the same Linear project-membership
    # content twice against a real ClickHouse container, once through a
    # crash-then-recovery pass over one shared ledger (exercising the actual
    # readback-mediated recovery path, per a task-route codex finding that
    # the first draft never touched it) and once through a second, wholly
    # unrelated from-scratch ledger (simulating an expired-lease reclaim with
    # no memory of the earlier attempt), and asserts the full persisted
    # project_membership_transitions/projects row content and the presence
    # view are byte-identical after each replay.
    #
    # CHAOS-4193(d) then added 1 more integration-tagged test (1051 -> 1052,
    # 115 -> 116): the github Projects v2 snapshot-diff producer's
    # reachability proof -- seeds two board snapshots against the same real
    # ClickHouse container (issue+PR present, then the issue dropped from the
    # board), drives both through the real effects/readback path, and asserts
    # the issue's project_membership_presence row appears after the first
    # sync and disappears after the second, while the untouched PR's does
    # not. Proves both halves the ticket owed: issue membership (no prior
    # mechanism existed at all) and removal of either subject kind (no prior
    # mechanism existed for either).
    #
    # A codex round-1 finding on that same producer then added 6 more ordinary
    # tests (1052 -> 1058, 116 unchanged): pure decision tests over
    # diffGitHubProjectV2Snapshot (additions restricted to work_item, removals
    # of either subject kind, unchanged-board emits nothing, an incomplete
    # snapshot suppresses removals only, subject keys distinguish repos) and
    # over the shared githubProjectV2ItemSubject identification helper. None
    # touch a real database. The finding itself: an unidentifiable board item
    # (an incomplete PullRequest payload, or an unrecognised content typename)
    # was silently omitted from the current snapshot, which read as "this
    # subject left the board" for anything previously active -- a destructive
    # false removal for a subject that never moved. Fixed by a Complete flag
    # on the snapshot (false whenever the sync could not name a real subject
    # it saw) that suppresses removal computation, but not additions, for that
    # project this sync.
    #
    # Codex round 2 verified the fix and sharpened one round-1 disposition
    # (the work_items-column fallback gap is not always a one-sync bootstrap
    # -- an issue removed from its board strictly BEFORE this producer's
    # first sync for that project never gets a first transition row at all,
    # so its stale presence edge persists; the doc comment on
    # GitHubProjectV2SnapshotDiffClickHouseReader was corrected to say so,
    # left open as a bounded historical gap rather than fixed, matching
    # #1896's own already-accepted interim caveat) and asked for one more
    # ordinary test (1058 -> 1059, 116 unchanged): a Fetch-level case for
    # every way a board's Complete flag can land (issue missing repository,
    # a board of only draft issues, an unrecognised content typename, a
    # fully identified mixed board) -- the original pagination test only
    # exercised the mixed case incidentally.
    #
    # CHAOS-4244 then added 12 more ordinary top-level providersync tests
    # (1059 -> 1071, developed in parallel with CHAOS-4193(d) above and
    # rebased onto it -- the two branches' counts are additive), all in
    # github_work_items_derivation_context_test.go except where noted:
    #   - three pinning the reporter/author membership candidate (resolves
    #     via the existing assignee_membership rank, stays unassigned with
    #     neither an assignee nor a reporter, and never outranks a
    #     repo_ownership fact);
    #   - two pinning the CHAOS-4110 ambiguity gate (a reporter whose
    #     membership resolves to two DIFFERENT teams contributes nothing;
    #     multiple candidate rows naming the SAME team still resolve);
    #   - two in work_item_cross_provider_donor_test.go pinning the
    #     written-by-source observation tally reaching the route's result
    #     map (and that a non-observer deriver still gets a present, empty
    #     tally rather than no key at all);
    #   - one in github_work_item_derived_effects_test.go pinning the
    #     written-source metric-label mapping the ClickHouse writer feeds to
    #     dev_health_work_item_team_attributions_written_total (author vs
    #     assignee split on the SAME stored assignee_membership source, via
    #     the evidence prefix);
    #   - four added by a codex adversarial-review round (2026-08-24) that
    #     found and fixed three HIGH-severity gaps in the Python mirror
    #     (Python's legacy TeamResolver reporter path bypassed the ambiguity
    #     gate entirely and was removed outright; a ReplacingMergeTree
    #     storage-key collision -- team_id/source only, evidence excluded,
    #     migration 051 -- when the reporter and assignee are the same
    #     person; the metric-label classifier could never actually match a
    #     real resolver-produced "author" row) plus a MEDIUM in the shared
    #     label logic: a bot/App author exclusion test, an
    #     ambiguity-tags-the-unassigned-row test, a same-person
    #     distinct-provenance test (Go needed NO collapse fix -- its
    #     write-time githubWorkItemDerivedSortingKeyDedupe in
    #     WriteGitHubWorkItemEffect already deduped deterministically by the
    #     real storage key; Python's sink had no such step, which is why
    #     _collapse_by_team_id lives there, scoped to assignee_membership
    #     only), and a real-row metric-source-split test.
    # All twelve drive the resolver/observation/label code directly over
    # in-memory fixtures and touch no database, so the integration-tagged
    # count stays 116 (unchanged by this ticket).
    #
    # A follow-up precedence ruling (chris, 2026-08-24) then added 1 more
    # ordinary test (1071 -> 1072) in github_work_items_derivation_context_test.go:
    # TestGitHubWorkItemDerivationAuthorNeverOutranksALinkedIssueDonor, proving
    # a PR with a team-mapped author AND a linked_issue donor for a DIFFERENT
    # team resolves to the linked issue's team -- author_membership moved to
    # its own rank 6 (below linked_issue at 5, above manual_fallback, now 7),
    # since a person-shaped author signal must never beat a real linked-issue
    # donor. Drives the resolver directly over in-memory fixtures and touches
    # no database, so the integration-tagged count stays 116.
    #
    # Then 1 more integration-tagged test (1072 -> 1073, 116 -> 117) in
    # github_work_item_derived_effects_integration_test.go:
    # TestGitHubWorkItemTeamAttributionSourceEnumCodesAreAppendedNotRenumbered,
    # pinning the EXACT numeric code the migration chain assigns to every
    # `source` value (not just that each name is present, which the sibling
    # enum-acceptance test above already covers) -- proving migration 078
    # appended `author_membership=9` rather than renumbering any of the
    # pre-existing 1-8 codes, which would otherwise silently reinterpret every
    # already-written row's stored source. Runs against a real migrated
    # ClickHouse container (chschema.Apply), so it is integration-tagged.
    #
    # A codex round-2 finding (2026-08-24, MEDIUM) then added 2 more ordinary
    # tests (1073 -> 1075) in github_work_items_derivation_context_test.go:
    # the earlier cross-team falsifier tests injected the linked_issue
    # candidate directly, proving only that an already-supplied linked_issue
    # candidate outranks author_membership -- never exercising donor
    # discovery/eligibility (allowedDonorSources in buildLinkedIssueIndex).
    # TestGitHubWorkItemDerivationCausalAuthorNeverOutranksARealLinkedIssueDonor
    # drives the REAL buildLinkedIssueIndex builder end to end (a donor
    # resolving via a first-class project_ownership fact beats an author on a
    # different team); TestGitHubWorkItemDerivationCausalAuthorOnlyDonorNeverBecomesALinkedIssueDonor
    # proves an item whose ONLY resolvable team is author_membership never
    # registers as a donor at all. Both drive the resolver/builder directly
    # over in-memory fixtures and touch no database, so the
    # integration-tagged count stays 117.
    #
    # A codex round-3 finding (2026-08-24, HIGH + MEDIUM) then added 2 more
    # ordinary tests (1075 -> 1077) in github_work_item_derived_surfaces_test.go
    # and github_work_items_derivation_context_test.go:
    # TestGitHubTeamAttributionDedupeNeverErasesTheOnlyPrimaryRow pins the
    # fix for the write-time sorting-key dedup, which used to be pure
    # last-wins and could silently discard the resolver's only is_primary=1
    # row when a reporter or assignee matched two membership facets naming
    # the same team; TestGitHubWorkItemDerivationAuthorMembershipNeverAppliesToANonPRIssue
    # pins author_membership's PR-only scope gate (a plain GitHub issue,
    # WorkItemID "gh:" not "ghpr:", must never gain a team via this signal).
    # Both drive the dedup/resolver directly over in-memory fixtures and
    # touch no database, so the integration-tagged count stays 117.
    #
    # A codex round-4 finding (2026-08-24, MEDIUM) then added 2 more ordinary
    # tests (1077 -> 1079): the PR-only gate above was GitHub-only ("ghpr:"
    # prefix), but this resolver is shared by GitHub, GitLab, and Jira --
    # silently diverging from Python's item.type in {"pr","merge_request"}
    # gate, leaving every GitLab MR author unassigned in Go.
    # TestGitHubWorkItemDerivationAuthorMembershipAppliesToAGitLabMergeRequest
    # proves a GitLab MR ("gitlab:...!...") still gains author_membership;
    # TestGitHubWorkItemDerivationAuthorMembershipNeverAppliesToAGitLabIssue
    # is its negative control ("gitlab:...#..."). Both drive the resolver
    # directly over in-memory fixtures and touch no database, so the
    # integration-tagged count stays 117.
    #
    # A codex round-5 finding (2026-08-25, BLOCK) then added 3 more ordinary
    # tests (1079 -> 1082): the R4 gate checked WorkItemID STRING SHAPE
    # ("gitlab:" prefix + contains "!") with no check that Provider actually
    # said "gitlab" -- a legacy/mismatched row (e.g. a Jira item whose
    # WorkItemID happened to look like a GitLab MR) could therefore wrongly
    # pass. The fix switched the gate to Provider+Type
    # (githubWorkItemDerivationIsPullOrMergeRequestType).
    # TestGitHubWorkItemDerivationAuthorMembershipGatesOnProviderNotIDShape
    # reproduces the exact defect (red-first against the pre-fix gate, see
    # the test's own doc comment); TestGitHubWorkItemDerivationAuthorMembershipNeverAppliesToAJiraIssue
    # and TestGitHubWorkItemDerivationAuthorMembershipNeverAppliesToALinearIssue
    # are the Jira/Linear negative regression proofs codex asked for ("Jira/Linear
    # are excluded only by convention, and existing tests do not prove exclusion
    # under mismatched or legacy rows"). All three drive the resolver directly
    # over in-memory fixtures and touch no database, so the integration-tagged
    # count stays 117.
    #
    # A codex round-6 finding (2026-08-25, BLOCK) then added 1 more ordinary
    # test (1082 -> 1083): the round-5 fix's tests all built
    # githubWorkItemDerivationSubject{} literals by hand, bypassing
    # githubWorkItemDerivationSubjectFromRow -- the actual production
    # row-to-subject conversion -- and no test anywhere in this package had
    # ever exercised loadDonors's Scan() with real returned data (every Rows
    # double's Next() returns false immediately), so a column-order
    # regression between the SELECT list and the Scan destinations could ship
    # undetected. The three round-5 tests above were rewritten to build a
    # githubWorkItemRow and convert it via githubWorkItemDerivationSubjectFromRow
    # (no new top-level test, just a rewrite -- doesn't change this count);
    # TestLoadGitHubWorkItemDerivationContextDonorScanPropagatesTypeInCorrectColumnOrder
    # is new and drives loadDonors's Scan against a fake driver.Rows that
    # actually returns one row. Still no database, so the integration-tagged
    # count stays 117.
    #
    # A codex round-7 finding (2026-08-25, BLOCK, 2 HIGH, both reproduced by
    # hand via mutate-and-rerun) then added 1 more ordinary test (1083 ->
    # 1084): (1) the round-6 negative tests all use Provider "jira", so the
    # provider+type gate is already closed on Provider alone -- they stayed
    # green even with "Type: row.Type" deleted from
    # githubWorkItemDerivationSubjectFromRow entirely, never actually proving
    # Type propagates; (2) fakeDonorRowsConn ignored the query text and
    # always returned hand-ordered values, so a SELECT-column reorder left
    # Scan() untouched still passed -- the donor-scan test only ever proved
    # Scan-destination order, not SELECT-list order.
    # TestGitHubWorkItemDerivationSubjectFromRowPropagatesTypeForAuthorMembership
    # is new: a real githubWorkItemRow (github/pr and gitlab/merge_request
    # subtests) converted through the actual production
    # githubWorkItemDerivationSubjectFromRow, asserting both the converted
    # Type AND that author_membership still resolves -- confirmed red when
    # Type propagation is removed. The donor-scan test above also gained a
    # pinned SELECT-projection string assertion (no new top-level test, just
    # an added assertion) -- confirmed red when the SELECT column order is
    # swapped. Still no database, so the integration-tagged count stays 117.
    #
    # CHAOS-4321's Round-1 commit (cbe8f65fe, "remove person-membership as a
    # team source") then deleted all 19 of the CHAOS-4244 author-path
    # top-level tests named above (1084 -> 1065), under that round's
    # original, wider scope ("author_membership and assignee_membership are
    # both removed as team sources"). Two later commits on the same branch
    # (501d34b16 restoring assignee_membership; 608308a99 adding the
    # two-layer admin/provider member resolution chris's 08:30 PT ruling
    # required) added 4 replacement top-level tests covering some of the same
    # ground as table-driven subtests rather than one-function-per-case
    # (1065 -> 1069) -- but 11 of the 19 deleted properties were never
    # restored, a genuine coverage gap confirmed against origin/main
    # (f26cf55e0, still at 1084) via an isolated worktree diff, not a stale
    # pin. This restoration pass added those 11 back as top-level tests
    # (1069 -> 1080): TestGitHubWorkItemDerivationSubjectFromRowPropagatesTypeForAuthorMembership,
    # TestGitHubWorkItemDerivationNoAssigneeNoReporterStaysUnassigned,
    # TestGitHubWorkItemDerivationUnambiguousReporterMembershipStillResolves,
    # TestGitHubWorkItemDerivationAuthorNeverOutranksALinkedIssueDonor,
    # TestGitHubWorkItemDerivationCausalAuthorNeverOutranksARealLinkedIssueDonor,
    # TestGitHubWorkItemDerivationAuthorMembershipNeverAppliesToAGitLabIssue,
    # TestGitHubWorkItemDerivationAuthorMembershipNeverAppliesToAJiraIssue,
    # TestGitHubWorkItemDerivationAuthorMembershipNeverAppliesToALinearIssue,
    # TestGitHubWorkItemDerivationAuthorMembershipGatesOnProviderNotIDShape,
    # TestGitHubWorkItemDerivationReporterAndAssigneeSamePersonSameTeamStayDistinctProvenance,
    # and TestGitHubWorkItemTeamAttributionMetricSourceSplitsRealReporterFromRealAssigneeRows.
    # The remaining 8 of the 19 stay covered only as subtests of the 4
    # already-existing consolidated functions (deliberately not split back
    # into 8 more top-level funcs -- table-driven subtests are equivalent
    # coverage, not a regression): author-with-no-mapping/one-team/
    # two-teams/bot/non-PR-issue/GitLab-MR cases live in
    # TestGitHubWorkItemDerivationNeverInfersTeamFromPersonMembershipUnlessAdminMapped;
    # the reporter-never-outranks-a-higher-source case lives in
    # TestGitHubWorkItemDerivationOwnershipWinsOverAssigneeAndAuthorMembership
    # (which also covers the assignee half); the causal-author-only-donor
    # case lives in TestGitHubWorkItemDerivationAuthorOnlyDonorNeverPropagatesATeam.
    # None of this touches a database, so the integration-tagged count stays
    # 117. Net: 1084 -> 1080 (four fewer top-level funcs than the original
    # 1084, all four accounted for above as consolidated subtests, not lost
    # coverage).
    #
    # CHAOS-4321 round 3 (team-lead ruling, 2026-08-26, codex adversarial
    # review HIGH finding) then added ONE ordinary top-level test (1080 ->
    # 1081): TestGitHubWorkItemLoadMembersScopesTeamsMembersFallbackByProvider,
    # pinning that a bare (non-email) `teams.members` roster facet only
    # joins the provider-scoped fallback pool when
    # `identities.provider_identities` confirms which provider it belongs
    # to -- closing a cross-provider leak class the earlier
    # `teams.members`-demotion fix (above) narrowed but did not structurally
    # prevent. Drives loadMembers through a fake ClickHouse conn and touches
    # no database, so the integration-tagged count stays 117.
    #
    # CHAOS-4078 added TWO integration-tagged top-level tests in
    # internal/providersync (repository_postgres_metrics_integration_test.go,
    # //go:build integration): TestPostgresRepositoryRecordsClaimAndFailMetrics
    # and TestPostgresRepositoryClaimAndFailToleratesNilMetrics, pinning the
    # new dev_health_provider_unit_claimed_total/dev_health_provider_unit_failed_total
    # telemetry through the real PostgresRepository.Claim/Fail SQL paths
    # against a real containers.StartPostgres instance. Both counts move:
    # 1081 -> 1083, 117 -> 119.
    #
    # A codex round-3 finding (CHAOS-4078: folded telemetry not attributing
    # to the enabled alias) added one more ordinary, non-integration-tagged
    # top-level test in internal/providersync
    # (work_item_alias_completion_test.go):
    # TestMetricDatasetKeysAttributesFoldedTelemetryToTheEnabledAliases.
    # Only the provider count moves: 1083 -> 1084; 119 stays.
    #
    # CHAOS-4394 added TWO ordinary top-level tests in
    # github_tests_download_failure_test.go pinning the new
    # dev_health_cicd_partial_success_total telemetry (1084 -> 1086):
    # TestGitHubTestsCicdPartialSuccessTelemetry (fires when a unit
    # advances its watermark despite a report_member skip) and
    # TestGitHubTestsCicdPartialSuccessDoesNotFireOnACleanUnit (must not fire
    # on a unit with nothing incomplete). Neither touches a database, so the
    # integration-tagged count stays 119.
    #
    # CHAOS-4392 (independent, same base) added another TWO ordinary,
    # non-integration-tagged top-level tests in internal/providersync,
    # pinning the within-suite duplicate test-case natural-key fix (a prod
    # run with two identically named <testcase> elements in one JUnit suite
    # hashed to the same case_id and WriteEffect's recordGitHubTestsKey
    # rejected the batch, burning all 5 River attempts on
    # ErrInvalidConfiguration):
    # TestGitHubTestsWithinSuiteDuplicateCaseNamesGetDistinctIDsAndWriteSucceeds
    # (github_tests_cross_artifact_key_collision_test.go) and
    # TestGitLabNativeTestReportWithinSuiteDuplicateCaseNamesGetDistinctIDs
    # (gitlab_tests_route_test.go, the GitLab-native twin). Neither touches a
    # database, so the integration-tagged count stays 119. Combined with
    # CHAOS-4394 above: 1084 -> 1088.
    #
    # CHAOS-4365 item 1b added 27 new top-level tests in internal/providersync
    # for the team_repo_ownership derivation: 19 ordinary tests in
    # team_repo_ownership_derivation_test.go, 1 ordinary test in
    # team_repo_ownership_donor_walk_oracle_test.go (the live-Python donor-walk
    # gating oracle), and 7 integration-tagged tests in
    # team_repo_ownership_derivation_integration_test.go (//go:build
    # integration). 1088 -> 1115 top-level; 119 -> 126 integration-tagged.
    #
    # CHAOS-4458 part (b) then added 6 more top-level tests in
    # internal/providersync for the Linear id-space fix (Linear's
    # team_project_ownership rows are keyed "{org_id}:linear:{team_key}"
    # while a Linear work item's own project_id is a disjoint raw Linear
    # Project UUID -- see team_repo_ownership_derivation.go's
    # TeamRepoOwnershipWorkItem doc comment): 5 ordinary tests in
    # team_repo_ownership_derivation_test.go
    # (TestLinearTeamKeyOwnResolutionMatchesTeamKeyShapedOwnership,
    # TestLinearTeamKeyDonorWalkMatchesTeamKeyShapedOwnership,
    # TestDirectProjectIDArmPreferredOverLinearTeamKeyArm,
    # TestLinearTeamKeyArmNeverAppliesToNonLinearProviders,
    # TestResolutionArmIsDeterministicWhenBothArmsAgreeOnTheSameRepoAndTeam --
    # the last one pins a codex adversarial-review fix: the recorded
    # resolution arm must not depend on ClickHouse scan order) and 1
    # integration-tagged test in
    # team_repo_ownership_derivation_integration_test.go
    # (TestTeamRepoOwnershipDerivationResolvesLinearTeamKeyShapedOwnership).
    # 1115 -> 1121 top-level; 126 -> 127 integration-tagged (this branch's delta).
    #
    # CHAOS-4431 (Linear team-catalog native route) added 15 new top-level
    # tests in internal/providersync across its codex review rounds: 5
    # ordinary tests in linear_reference_catalog_test.go
    # (TestLinearReferenceCatalogNonStrictMalformedProjectsKeepsOtherRows,
    # TestLinearReferenceCatalogNonStrictCycleFailureKeepsOtherRows,
    # TestLinearReferenceCatalogStrictCycleFailureAbortsTheWholeCall,
    # TestLinearReferenceTeamRosterFromMembershipsExcludesRejectedMemberships,
    # TestLinearReferenceTeamRosterFromMembershipsScopesByTeam -- round 2/3's
    # non-strict partial-prefix and roster-rebuild-after-guard fixes), 9
    # ordinary tests in team_membership_conflict_guard_test.go
    # (TestMembershipConflictsWithManualState* x8 + Test
    # ApplyTeamMembershipConflictGuardCountsAndFiltersSkippedRows -- the #6
    # guard's same-team-confirms/different-team-conflicts semantics, corrected
    # twice across rounds 2-3), and 1 integration-tagged test in
    # linear_reference_catalog_effects_integration_test.go
    # (TestLinearReferenceCatalogEffectsPreservesManualMembersAcrossWrites,
    # //go:build integration -- the sprints-fixture-extended ClickHouse
    # effects suite). 1115 -> 1130 top-level; 126 -> 127 integration-tagged.
    #
    # CHAOS-4434 (GitHub team-catalog native collector) added 34 new
    # top-level tests in internal/providersync, all new files: 10
    # integration-tagged in github_team_catalog_collector_integration_test.go
    # (roster preservation, sync_policy/membership-conflict guards, strict
    # fail-closed), 1 ordinary in github_team_catalog_collector_test.go, 4
    # integration-tagged in github_team_catalog_effects_integration_test.go
    # (ClickHouse effects incl. team_repo_ownership), 4 ordinary live-Python
    # oracle tests in github_team_catalog_generic_oracle_test.go, 8 ordinary
    # in github_team_catalog_guards_test.go (the GitHub-native twin of
    # CHAOS-4431's membership-conflict guard semantics), and 7 ordinary in
    # github_team_catalog_route_test.go. 1130 -> 1164 top-level; 127 -> 141
    # integration-tagged.
    #
    # CHAOS-4432 (GitLab team-catalog native collector) added 45 new
    # top-level tests in internal/providersync, all new files: 3
    # integration-tagged in gitlab_team_catalog_effects_integration_test.go
    # (ClickHouse effects), 2 ordinary in
    # gitlab_team_catalog_guards_fakeconn_test.go, 8 ordinary in
    # gitlab_team_catalog_guards_test.go (the GitLab-native twin of
    # CHAOS-4431/CHAOS-4434's membership-conflict guard semantics), 1
    # ordinary each in gitlab_team_catalog_ownership_oracle_test.go and
    # gitlab_team_catalog_project_oracle_test.go and
    # gitlab_team_catalog_team_oracle_test.go (live-Python oracles), 3
    # ordinary in gitlab_team_catalog_roster_preservation_scope_test.go, 23
    # ordinary in gitlab_team_catalog_test.go, and 3 ordinary in
    # gitlab_team_catalog_writeteams_fakeconn_test.go. A tenth new file,
    # gitlab_team_catalog_live_manual_test.go, is gated `//go:build
    # manuallive` (a manual, non-CI, live-provider-token check) rather than
    # `integration` -- go list -tags=integration never includes it, so its 1
    # test does NOT count toward either pin here. 1164 -> 1209 top-level;
    # 141 -> 144 integration-tagged.
    #
    # CHAOS-4444 (team-level + identity-drift staged-review engine,
    # replacing CHAOS-4431's plain-skip interim guards) added 12 new
    # top-level tests in internal/providersync, both new files, neither
    # `//go:build integration` (fake driver.Conn doubles, no real
    # ClickHouse): 7 ordinary in team_drift_json_test.go (Python-json.dumps-
    # parity pins for the canonical JSON encoder and change_id hash) and 5
    # ordinary in team_drift_review_fakeconn_test.go (the shared team-level
    # engine's auto-apply/stage/resolve/supersede paths). No new
    # integration-tagged tests. 1209 -> 1221 top-level; 144 unchanged.
    #
    # CHAOS-4444 follow-up (team-lead ruling: add live Python oracle pairs
    # for the drift-row outputs before the codex round) added 4 more
    # ordinary top-level tests in a new file, team_drift_generic_oracle_test.go
    # (also not `//go:build integration` -- these shell out to the live
    # Python interpreter via the SAME python_generic_row_oracle.py harness
    # every other oracle pair in this package uses, gated by
    # DEV_HEALTH_LIVE_PYTHON_ORACLES, not by the `integration` build tag):
    # TestTeamCatalogObservedRowMatchesLivePythonProducer,
    # TestTeamCatalogChangeIDMatchesLivePythonProducer,
    # TestIdentityDriftChangeIDMatchesLivePythonProducer,
    # TestIdentityDriftConflictDecisionMatchesLivePythonProducer -- pinning
    # clickhouse_team_drift_projector.py's _observed_row/
    # change_id_for_team_field and clickhouse_identity_drift.py's
    # change_id_for_identity_membership/_conflict_for against the Go engine,
    # live. 1221 -> 1225 top-level; 144 unchanged.
    #
    # CHAOS-4508 (sibling suite-object natural-key discriminator) added 3
    # ordinary top-level tests, none `//go:build integration`:
    # TestGitHubTestsSameArtifactSiblingSuitesSameNameCollide (the red
    # repro, cherry-picked from the CHAOS-4487 diagnosis lane and now
    # green), TestGitHubTestsSingleSuiteArtifactSuiteIDUnchanged (pins that
    # a non-colliding single suite's SuiteID hash is unchanged), and
    # TestGitLabNativeTestReportSameReportSiblingSuitesSameNameCollide (the
    # GitLab-native twin). 1225 -> 1228 top-level; 144 unchanged
    # (origin/main's delta from the shared 1225/144 base).
    #
    # CHAOS-4458 part (b) (this branch's delta from the SAME shared 1225/144
    # base): 6 tests for the Linear id-space fix (5 ordinary +
    # 1 integration-tagged) plus 2 more ordinary pinning tests from
    # lane-4458b-live's compose live-proof
    # (TestLinearTeamKeyOwnResolutionWithEmptyProjectID,
    # TestLinearTeamKeyResolvesViaPRInheritanceIssueLink -- closing two
    # fixture gaps the live proof found: no prior test used an actually-empty
    # `project_id`, and none exercised the `issuePRLinks`/PR-inheritance path
    # with a Linear donor). 1225 -> 1233 top-level; 144 -> 145
    # integration-tagged (this branch's own delta, +8/+1).
    #
    # Merged total (this branch's +8/+1 delta applied on top of origin/main's
    # independent +3/+0 delta from the same 1225/144 base): 1225 -> 1236
    # top-level; 144 -> 145 integration-tagged.
    #
    # CHAOS-4530 ("a team key is not a project key") added 3 ordinary
    # top-level tests in linear_reference_catalog_test.go (not `//go:build
    # integration` -- in-memory GraphQL doer fixtures, no real ClickHouse):
    # TestLinearReferenceCatalogNeverWritesTeamKeyShapedPseudoProject (red-
    # first proof that the synthetic {org}:linear:{teamKey} pseudo-project is
    # never written ACTIVE to `projects`, only as a retiring tombstone),
    # TestLinearReferenceCatalogRealProjectOwnershipNeverCarriesTheTeamKey
    # (a real project's team_project_ownership row never carries the owning
    # team's key as project_key), and
    # TestLinearReferenceCatalogTeamKeyOwnershipRowMatchesItsOneReader (the
    # retained team-key-shaped ownership row stays byte-identical to what
    # team_repo_ownership_derivation.go's linearTeamKeyProjectID reconstructs
    # to look it up -- CHAOS-4458 part (b)'s reader). None is
    # integration-tagged. 1236 -> 1239 top-level; 145 unchanged.
    #
    # CHAOS-4530 codex review round 2 (confirmed real: the per-response
    # tombstone loop only revisits a team key present in the CURRENT Linear
    # response, so a team deleted or re-keyed between syncs never gets its
    # OLD pseudo-project identity retired) added 4 more ordinary top-level
    # tests in a new file, linear_pseudo_project_retirement_test.go (not
    # `//go:build integration` -- a fake driver.Conn double, no real
    # ClickHouse, same convention as gitlab_team_catalog_guards_fakeconn_test.go):
    # TestRetireOrphanedLinearPseudoProjectsRetiresOnlyTheDroppedTeam,
    # TestRetireOrphanedLinearPseudoProjectsRetiresNothingWhenEveryTeamStillSeen,
    # TestRetireOrphanedLinearPseudoProjectsPropagatesQueryFailure, and
    # TestRetireOrphanedLinearPseudoProjectsRejectsInvalidInput -- pinning
    # the new RetireOrphanedLinearPseudoProjects reconciliation helper's
    # orphan-detection, non-regression (nothing retired when every team is
    # still observed), fail-closed, and input-validation behavior. None is
    # integration-tagged. 1239 -> 1243 top-level; 145 unchanged.
    #
    # CHAOS-4530 follow-up (CF/acr finding: an is_active=0 tombstone is not
    # a signal acr's identity resolution recognizes -- see linear_reference_
    # catalog_route.go's updated doc comment) REMOVED
    # linear_pseudo_project_retirement_test.go's 4 tests (the now-deleted
    # per-sync orphan-reconciliation helper) and ADDED
    # linear_pseudo_project_cleanup_test.go's 7 tests for the one-time
    # operator cleanup that replaced it (RetireLinearPseudoProjectRows):
    # TestRetireLinearPseudoProjectRowsRejectsNilConn,
    # TestRetireLinearPseudoProjectRowsDryRunFindsButNeverDeletes,
    # TestRetireLinearPseudoProjectRowsRealRunDeletesAndReportsCounts,
    # TestRetireLinearPseudoProjectRowsRealRunFindsNothingSkipsMutation,
    # TestRetireLinearPseudoProjectRowsScopedByOrgAddsThePredicate,
    # TestRetireLinearPseudoProjectRowsPropagatesQueryFailure,
    # TestRetireLinearPseudoProjectRowsPropagatesDeleteFailureButStillReportsFoundRows.
    # None is integration-tagged. Net +3 (-4/+7): 1243 -> 1246 top-level; 145
    # unchanged.
    #
    # Codex review round 1 on the cleanup verb (P2, confirmed real) added 1
    # more ordinary top-level test in linear_pseudo_project_cleanup_test.go:
    # TestLinearPseudoProjectIdentityPredicateMatchesOwnOrgIDOnly, pinning
    # the fix that ties the identity predicate to each row's OWN org_id
    # (startsWith(id, concat(org_id, ':linear:'))) instead of a bare
    # substring test anywhere in id. 1246 -> 1247 top-level; 145 unchanged.
    #
    # CHAOS-4537 added 1 new `//go:build integration` top-level test in
    # team_repo_ownership_derivation_integration_test.go:
    # TestTeamRepoOwnershipDerivationResolvesLinearTeamKeyWithNoProjectOwnershipAtAll,
    # the ClickHouse-loading Derive() method's own red-first proof for the
    # ticket's redirect: a Linear work item with native_team_key resolves
    # even when team_project_ownership has zero rows for the org (Derive's
    # early return used to gate on team_project_ownership alone before
    # loading work_items at all -- fixed in the same change). 1247 -> 1248
    # top-level; 145 -> 146 integration-tagged.
    #
    # Codex review round 1 on this PR (P1, confirmed real) found the same
    # change had removed a SEPARATE, still-necessary guard along with the
    # one above: if work_items/dependencyEdges/issuePRLinks are all empty
    # regardless of team_project_ownership's state (a transient partial-sync
    # snapshot, the OPPOSITE ordering from the test above), proceeding
    # anyway would retract every previously-derived row for the org. Fixed
    # by restoring that guard (unchanged from before this ticket) while
    # still removing only the projectLinks-only one. Added 1 more
    # `//go:build integration` top-level test, deliberately GitHub-shaped
    # per AGENTS.md's provider-matrix rule:
    # TestTeamRepoOwnershipDerivationPreservesReadinessGateForNonLinearOrgsTransientLinkageGap.
    # 1248 -> 1249 top-level; 146 -> 147 integration-tagged.
    #
    # Codex review round 2 on this PR (P1, confirmed real) found
    # resolveWorkItemTeamID's linear_team_key arm unconditionally trusted a
    # Linear work item's native_team_key with no validation against the org's
    # CURRENT team catalog -- diverging from the established "native_team"
    # resolution contract every other native-team lookup in this codebase
    # follows (compute_work_items.py's _native_team_candidate;
    # github_work_items_derivation_context.go's nativeTeamCandidate). Fixed
    # by adding a knownTeams input (loaded from `teams`) and validating
    # NativeTeamKey against it before trusting the value. Added 1 new
    # ordinary top-level test in team_repo_ownership_derivation_test.go:
    # TestLinearTeamKeyArmRejectsUnknownNativeTeamKey, and 1 new
    # `//go:build integration` top-level test in
    # team_repo_ownership_derivation_integration_test.go:
    # TestTeamRepoOwnershipDerivationRejectsUnknownNativeTeamKey.
    # 1249 -> 1251 top-level; 147 -> 148 integration-tagged.
    #
    # Codex review round 3 (final) on this PR found 2 more findings against
    # round 2's own fix. (P1, confirmed real) the projectLinks-empty guard
    # round 1 removed unconditionally reopened the identical retraction
    # hazard round 1 itself fixed, mirrored onto the opposite input
    # combination (team_project_ownership transiently empty for a
    # NON-Linear org, or a Linear org with no native_team_key signal) --
    # fixed with a new hasResolvableLinearNativeTeamKey helper and a
    # conditional guard. (P2 raised, verified NOT applicable to this
    # codebase: both the live Go writer and the retired Python writer stamp
    # `teams.id` and `teams.native_team_key` from the exact same value,
    # always -- executed-read proof, not asserted) native keys are not
    # separately resolved to a canonical team id; documented instead of
    # adding unreachable-case handling, plus a pinning test guarding the
    # verified invariant against future drift. Added 3 new tests:
    # TestHasResolvableLinearNativeTeamKey and
    # TestLinearReferenceCatalogTeamRowIDMatchesNativeTeamKey (both ordinary,
    # team_repo_ownership_derivation_test.go), and
    # TestTeamRepoOwnershipDerivationPreservesReadinessGateWhenProjectOwnershipIsTransientlyEmptyForANonLinearOrg
    # (`//go:build integration`, team_repo_ownership_derivation_integration_test.go).
    # 1251 -> 1254 top-level; 148 -> 149 integration-tagged.
    #
    # A delta-only re-review of round 3's own fix (the final allowed codex
    # pass per the round cap -- its finding gets a minimal fix, no further
    # round) found a second real P1: diffTeamRepoOwnershipRetractions is a
    # single GLOBAL diff over the whole org, not scoped per resolution arm.
    # Round 3's hasResolvableLinearNativeTeamKey guard let a cycle proceed
    # with projectLinks empty on ONE Linear item's valid native key, but
    # `derived` can never reproduce a project_id-arm-derived pair in that
    # state -- retracting would wrongly wipe every previously-good
    # project_id-arm row for a MIXED org. Fixed: skip retraction entirely
    # whenever projectLinks is empty (still derive/write new linear_team_key
    # rows). Added 1 new `//go:build integration` top-level test in
    # team_repo_ownership_derivation_integration_test.go:
    # TestTeamRepoOwnershipDerivationSkipsRetractionWhenProjectOwnershipIsTransientlyEmptyForAMixedOrg.
    # 1254 -> 1255 top-level; 149 -> 150 integration-tagged.
    #
    # CHAOS-4557 (duplicate_natural_key detail never survived a worker
    # restart -- only the bare category reached sync_run_units). Added 2 new
    # ordinary top-level tests in
    # github_tests_cross_artifact_key_collision_test.go:
    # TestDuplicateNaturalKeyDetailFromExtractsTableAndFields and
    # TestDuplicateNaturalKeyDetailFromIsBounded, and 1 new
    # `//go:build integration` top-level test in
    # repository_postgres_metrics_integration_test.go:
    # TestPostgresRepositoryFailWithDuplicateKeyDetailPersistsStructuredKey.
    # 1255 -> 1258 top-level; 150 -> 151 integration-tagged.
    #
    # CHAOS-4559 (sync_runs.completed_units/failed_units read 0/0 while units
    # actually succeeded -- the per-unit terminal commit never touched the
    # parent row). A codex adversarial review round 1 found a real
    # concurrency race in the fix's own recompute (uncorrelated COUNT(*)
    # subqueries plan as InitPlans, evaluated once at statement start, so a
    # second concurrent completion blocked on the row lock could resume
    # using its stale pre-wait count). Added 1 new
    # `//go:build integration` top-level test in
    # repository_postgres_integration_test.go:
    # TestPostgresRollupCountsBothUnitsUnderConcurrentCompletion (mutation-
    # tested: reverting the lock-before-recompute fix made it fail 19/20
    # runs).
    # 1258 -> 1259 top-level; 151 -> 152 integration-tagged.
    #
    # CHAOS-4548 (stale project_key='CHAOS' team_project_ownership rows for
    # real Linear projects, superseded by NULL-keyed rows but never
    # collapsed by ReplacingMergeTree). Added 9 new ordinary top-level tests
    # in linear_stale_project_ownership_cleanup_test.go: TestRetireStale
    # LinearProjectOwnershipRowsRejectsNilConn, ...DryRunFindsButNever
    # Deletes, ...RealRunDeletesAndReportsCounts, ...RealRunFindsNothing
    # SkipsMutation, ...ScopedByOrgAddsThePredicate, ...PropagatesQuery
    # Failure, ...PropagatesDeleteFailureButStillReportsFoundRows,
    # TestLinearStaleProjectOwnershipPredicateExcludesThePseudoIdentityRow,
    # and TestLinearStaleProjectOwnershipPredicateRequiresAReplacementRow
    # (codex review P1: never delete a project's only ownership signal).
    # None are integration-tagged (they use a fake driver.Conn, no real
    # ClickHouse needed).
    # 1259 -> 1268 top-level; 152 -> 152 integration-tagged (unchanged).
    #
    # CHAOS-4588 (every report_member artifact of full-chaos/dev-health-ops CI
    # runs skipped as unreadable_archive -- GitHub's auto-generated
    # ".dockerbuild" Docker Build Summary artifacts are raw gzip, not a
    # zip-wrapped container like an ordinary actions/upload-artifact
    # artifact, and the walk never filtered candidate artifacts by name at
    # all; also found this crowds the per-run artifact cap ahead of real
    # report artifacts, which is window-blocking and pinned the repo's real
    # `tests` watermark since 2026-08-08). Added 4 new ordinary top-level
    # tests: TestGitHubTestsDockerBuildArtifactsExcludedBeforeDownload and
    # TestGitHubTestsDockerBuildArtifactsDoNotConsumePerRunArtifactCap in
    # github_tests_non_report_artifact_test.go, and
    # TestGitHubTestsArtifactSkipsLogOncePerUnitWithCountsByCause and
    # TestGitHubTestsHealthyUnitLogsNoSkipSummary in
    # github_tests_artifact_skip_log_test.go (the log-storm collapse half of
    # the fix). None are integration-tagged.
    # 1268 -> 1272 top-level; 152 -> 152 integration-tagged (unchanged).
    #
    # CHAOS-4588 follow-up (CHAOS-4591 prep, per-sync-config selectable
    # artifact ingestion): renamed the artifact-name filter into a named seam
    # (githubTestsArtifactSelectionSeam). Added 1 new ordinary top-level test
    # in github_tests_non_report_artifact_test.go:
    # TestGitHubTestsDigestArtifactsExcludedBeforeDownloadWithBookkeeping
    # (later replaced, see below).
    # 1272 -> 1273 top-level; 152 -> 152 integration-tagged (unchanged).
    #
    # CHAOS-4588 codex review round 1 (P1/P2 fixes): bounded provider-supplied
    # artifact names before they reach the cursor (githubTestsTruncateArtifactName,
    # unbounded growth could exceed maxChunkCursorBytes); fixed a missing
    # Name on the empty-archive skip marker; reset the new exclusion counters
    # on a genuine page re-anchor (mirrors the existing ArchivesSeen/Unreadable
    # reset); split the summary's total into artifact_skip_total vs
    # incomplete_total so it no longer conflates report_member skips with
    # run-level truncations; and -- the collision-risk finding -- reverted
    # "digests-*" out of the default exclusion list (githubTestsNonReportArtifactPrefixes
    # is now empty by default; unlike ".dockerbuild" it is a plausible real
    # artifact prefix, so silently excluding it belongs to CHAOS-4591's
    # config-driven predicate, not a global default). Replaced
    # TestGitHubTestsDigestArtifactsExcludedBeforeDownloadWithBookkeeping with
    # TestGitHubTestsDockerBuildArtifactExclusionBookkeeping (suffix-only) and
    # TestGitHubTestsDigestArtifactsAreNotExcludedByDefault (pins the revert
    # as an executable spec); added TestGitHubTestsTruncateArtifactNameStaysWithinBound
    # and TestGitHubTestsExcludedArtifactSampleNameIsBounded for the name
    # truncation fix. Net: -1 removed, +4 added.
    # 1273 -> 1276 top-level; 152 -> 152 integration-tagged (unchanged).
    #
    # CHAOS-4588 codex review round 2 (P2 fixes): artifact_skip_total counted
    # member-level malformed/unreadable causes as if the whole artifact were
    # skipped; narrowed to the three whole-artifact-skip causes only
    # (artifact_oversized/artifact_unavailable/unreadable_archive). Reverted
    # round 1's exclusion-counter reset on page re-anchor -- it discarded
    # EARLIER pages' legitimate totals, not just the replayed page's; the
    # counters are a cursor-wide running total, not a per-walk gate input
    # like ArchivesSeen/Unreadable, so leaving them alone (accepting a bounded,
    # purely cosmetic double-count on the rare re-anchor) is safer than
    # silently undercounting. Added 1 new ordinary top-level test in
    # github_tests_artifact_skip_log_test.go:
    # TestGitHubTestsMemberLevelSkipDoesNotCountAsAnArtifactSkip.
    # 1276 -> 1277 top-level; 152 -> 152 integration-tagged (unchanged).
    #
    # CHAOS-4585 (native Go jira work-items route called the retired
    # GET /rest/api/3/search -- 410 Gone -- instead of the registered
    # JiraAtlassianRouteHandler's replacement). Added 2 new ordinary
    # top-level tests: TestJiraAtlassianRouteMigratedFromRetiredSearchEndpoint
    # (jira_atlassian_search_jql_migration_test.go, red-on-baseline proof
    # reproducing the live 410 body) and
    # TestNoGoJiraCallerTargetsTheRetiredSearchEndpoint
    # (jira_search_endpoint_guard_test.go, registry-level regression guard
    # scanning every production jira_*.go file for the retired path).
    # Neither is integration-tagged.
    # 1277 -> 1279 top-level; 152 -> 152 integration-tagged (unchanged).
    #
    # CHAOS-4592 (github tests/cicd watermark pinned at last_synced_at=
    # 2026-08-08 for weeks: report_member's report-parse-time causes,
    # malformed/unreadable, were never added to githubTestsWatermarkAdvancingPairs
    # when CHAOS-4394 fixed the three whole-artifact causes for the identical
    # reason -- an immutable historical CI artifact's bytes parse the same
    # way on every re-attempt). Codex review round 1 (P1) found the aggregate
    # SkippedArtifactsOverflow shortcut in
    # githubTestsReportMemberSkippedWithoutDurableMarker could let an
    # intermediate binary's cursor (post-CHAOS-4394, pre-CHAOS-4592) advance
    # over an unmarked malformed/unreadable skip during a rolling upgrade;
    # narrowed the shortcut to the three original causes only. Added 1 new
    # ordinary top-level test in complete_route_comparator_decoded_test.go:
    # TestGitHubTestsChunkedFinalMetadataOverflowShortcutExcludesReportParseCauses.
    # None are integration-tagged.
    # 1279 -> 1280 top-level; 152 -> 152 integration-tagged (unchanged).
    #
    # CHAOS-4592 codex review round 2 (P1): round 1's overflow-shortcut
    # narrowing still shared ONE aggregate SkippedArtifactsOverflow int
    # across every report_member cause, so it could not prove which cause
    # actually overflowed -- one cause's overflow could wrongly excuse an
    # unrelated unmarked cause, or a heavy run of the three original causes
    # could permanently starve a later malformed/unreadable skip out of ever
    # counting as covered. Added SkippedArtifactCauseOverflow (per-cause).
    # No new top-level tests (extended the existing overflow test in place).
    # 1280 -> 1280 top-level; 152 -> 152 integration-tagged (unchanged).
    #
    # CHAOS-4592 codex review round 3 (P2): round 2's per-cause fix gated its
    # legacy-cursor fallback on "causeOverflow has zero entries", which a
    # walk straddling this exact deploy would break the instant its own
    # post-upgrade marker-writing touched even one unrelated cause. Added
    # githubTestsLegacyReportOverflowSentinel, stamped once at decode from
    # the cursor's raw shape, to distinguish legacy-shaped provenance from
    # "merely non-empty". Added 1 new ordinary top-level test in
    # complete_route_comparator_decoded_test.go:
    # TestGitHubTestsChunkedFinalMetadataPreservesLegacyOverflowAcrossResume.
    # None are integration-tagged.
    # 1280 -> 1281 top-level; 152 -> 152 integration-tagged (unchanged).
    #
    # CHAOS-4592 (child of CHAOS-4588: codex reviews on lanes 4586/4587 found
    # two log-contract defects in CHAOS-4588's merged code, folded in here
    # since this lane owns github_tests_chunked_route.go today). (1)
    # githubTestsLogArtifactSkipSummary's gate was `len(incomplete) == 0`,
    # firing "provider artifacts skipped this unit" for a unit whose only
    # incompleteness was a run-level page-budget truncation -- zero artifacts
    # ever skipped, misleading artifact_skip_total=0 right next to the claim.
    # (2) the oversized-artifact branch kept its own pre-CHAOS-4588 direct
    # slog.Warn, so a unit with an oversized artifact logged that line PLUS
    # the summary line -- two records, violating the at-most-one-per-unit
    # contract CHAOS-4588 established for every other cause. Added 2 new
    # ordinary top-level tests in github_tests_artifact_skip_log_test.go:
    # TestGitHubTestsRunLevelTruncationDoesNotLogArtifactSkipSummary and
    # TestGitHubTestsOversizedArtifactLogsExactlyOneLine. Neither is
    # integration-tagged.
    # 1281 -> 1283 top-level; 152 -> 152 integration-tagged (unchanged).
    #
    # CHAOS-4592 third CHAOS-4588 fold-in (codex P1 via lane-4587): a
    # GitHubTestsSkippedArtifact record serializes far larger once its Name
    # field (CHAOS-4588/4591) exists than githubTestsMaxSkippedArtifactRecords
    # was originally sized against -- 20 records at the old 48-byte name cap
    # alone encoded to ~4.1KB, already exceeding the WHOLE cursor's 4KiB
    # maxChunkCursorBytes budget before every other field is added, so a
    # heavy-skip-volume unit's checkpoint write could fail
    # ErrChunkCheckpointConflict outright instead of degrading into
    # SkippedArtifactsOverflow. Shrank githubTestsMaxSkippedArtifactRecords
    # (20 -> 8) and githubTestsMaxArtifactNameBytes (48 -> 24) to a
    # combined budget verified, not assumed. Added 1 new ordinary top-level
    # test in github_tests_chunk_cursor_budget_test.go:
    # TestGitHubTestsChunkCursorWorstCaseStaysWithinBudget. Not
    # integration-tagged.
    # 1283 -> 1284 top-level; 152 -> 152 integration-tagged (unchanged).
    #
    # CHAOS-4592 fourth CHAOS-4588 fold-in (codex P1, round 5): shrinking the
    # skipped-artifact caps only bounds a NEWLY appended record -- a cursor a
    # PRIOR binary version already wrote under the OLDER, larger caps
    # decodes with its sample exactly as written (up to 20 records with
    # 48-byte names), and without normalizing it down to the current bounded
    # shape, an otherwise-ordinary in-flight cursor can already exceed
    # maxChunkCursorBytes on its own once that legacy sample is added back
    # in -- the very next re-encode during a rolling deploy fails
    # ErrChunkCheckpointConflict outright, losing committed progress.
    # decodeGitHubTestsChunkCursor now normalizes an inherited
    # SkippedArtifacts sample to the current caps, trimming into
    # SkippedArtifactsOverflow exactly as appendGitHubTestsSkippedArtifact
    # already does for a new record. Added 1 new ordinary top-level test in
    # github_tests_chunk_cursor_budget_test.go:
    # TestGitHubTestsChunkCursorNormalizesLegacySkippedArtifactsOnDecode.
    # Not integration-tagged.
    # 1284 -> 1285 top-level; 152 -> 152 integration-tagged (unchanged).
    #
    # CHAOS-4592 sixth CHAOS-4588 fold-in (codex P1 + P2 on lane-4587's round
    # 5/6): (P1) the legacy-sample migration bumped SkippedArtifactsOverflow
    # in aggregate, so the generic legacy sentinel could wrongly cover an
    # UNRELATED unmarked cause the trimmed records never touched -- a pre-
    # CHAOS-4394 cursor with exactly 20 artifact_oversized markers
    # (overflow==0) plus an unmarked artifact_unavailable observation would
    # advance the watermark for artifact_unavailable with zero durable
    # evidence for it. normalizeLegacyGitHubTestsSkippedArtifacts now
    # attributes migration-induced overflow to each dropped record's OWN
    # cause, and the legacy sentinel is decided from the RAW pre-
    # normalization signal instead of the post-normalization one, so the two
    # provenances never conflate. (P2) the folded per-unit summary line
    # dropped the run/artifact ids and cap the old per-artifact oversized
    # WARN carried, and the totality-gate failure path (which returns before
    # the summary ever runs) logged none at all -- both durable-marker
    # fields, already collected, now render into githubTestsSkippedArtifactLogSample
    # and the totality-gate ERROR line. Added 1 new ordinary top-level test
    # in github_tests_chunk_cursor_budget_test.go:
    # TestGitHubTestsChunkCursorLegacyTrimDoesNotExcuseAnUnrelatedUnmarkedCause.
    # Not integration-tagged.
    # 1285 -> 1286 top-level; 152 -> 152 integration-tagged (unchanged).
    # CHAOS-4592/4601 codex review gate round (terra/xhigh, full-base):
    # single-layer tests for the malformed/unreadable causes never proved the
    # parser's marker actually reaches the cursor the chunked route builds --
    # a regression dropping that forwarding loop would leave every existing
    # test green while recreating CHAOS-4592. Added 1 new ordinary top-level
    # test in complete_route_comparator_decoded_test.go:
    # TestGitHubTestsMalformedAndUnreadableReportsAdvanceWatermarkEndToEnd.
    # Not integration-tagged.
    # 1286 -> 1287 top-level; 152 -> 152 integration-tagged (unchanged).
    # CHAOS-4592/4601 codex review gate round 2 (terra/xhigh, full-base +
    # .codex-review-context.md): 1 P1 + 2 P2 findings, each fixed with a
    # regression test. P1 -- the durable-marker guard checked cause PRESENCE
    # not COUNT, so one marker could excuse an unrelated remainder of that
    # same cause's Incomplete count with zero evidence (a cursor straddling
    # this deploy could carry N unmarked pre-deploy skips + 1 post-deploy
    # marked one, advancing over all N+1). Fixed with an exact per-cause
    # SkippedArtifactCauseCount field, tested by
    # TestGitHubTestsChunkedFinalMetadataRequiresFullCountNotMarkerPresence
    # (complete_route_comparator_decoded_test.go). P2 #1 -- per-cause marker
    # overflow was durable but not observable in the summary log line,
    # tested by TestGitHubTestsSkipSummaryLogsOverflowedCauses
    # (github_tests_artifact_skip_log_test.go). P2 #2 -- the prior round's
    # end-to-end test reimplemented the route's forwarding loop instead of
    # exercising it, tested by
    # TestGitHubTestsMemberLevelSkipAdvancesWatermarkThroughRealRoute
    # (github_tests_artifact_skip_log_test.go), which drives the real
    # chunked route through an HTTP-mocked walk. 3 new ordinary top-level
    # tests, none integration-tagged.
    # 1287 -> 1290 top-level; 152 -> 152 integration-tagged (unchanged).
    # CHAOS-4592/4601 codex review gate round 3 (terra/xhigh, full-base +
    # .codex-review-context.md): 2 P2 correctness bugs, both regressions in
    # round 2's own P1 fix or its supporting normalize path, plus 2 P3 proof
    # gaps in unrelated-package/test-double wiring. P2 #1 -- causeCount was
    # treated as authoritative the moment it was tracked at all, abandoning
    # the sampleCount/causeOverflow fallback -- a cursor with fully retained
    # markers from BEFORE causeCount existed, resumed under this binary with
    # ONE new skip, wrongly withheld a fully-marked cause. Fixed by checking
    # every signal unconditionally (first success wins) instead of gating on
    # causeCount's presence; tested by
    # TestGitHubTestsChunkedFinalMetadataCombinesCauseCountWithRetainedMarkers
    # (complete_route_comparator_decoded_test.go). P2 #2 -- legacy-marker
    # trim attributed dropped records' overflow to the RAW (often empty)
    # Cause field instead of resolving it through
    # githubTestsSkippedArtifactCause's SizeBytes fallback, mis-keying
    # migration overflow under "" instead of artifact_oversized; the round-6
    # test that was supposed to cover this manufactured modern-shaped markers
    # (Cause already set) and never exercised the bug at all -- fixed both the
    # code and that test's marker construction (no new test func). The 2 P3s
    # (chunk-continuation metric wiring in internal/jobs/providerunit; the
    # causeOverflow-to-log-line forwarding through the real route) each get a
    # new test that exercises the real call path instead of a direct/synthetic
    # one: TestGitHubTestsSkipSummaryLogsOverflowThroughRealRoute
    # (github_tests_artifact_skip_log_test.go, providersync package) and
    # TestChunkContinuationDeferRecordsTheMetric
    # (chunk_continuation_test.go, internal/jobs/providerunit package --
    # does NOT count toward this providersync-only pin). 2 new ordinary
    # top-level providersync tests, none integration-tagged.
    # 1290 -> 1292 top-level; 152 -> 152 integration-tagged (unchanged).
    # CHAOS-4592/4601 codex review gate round 5 (terra/xhigh, full-base +
    # ledger, chris's ruling: apply the CLASS fix, not the layer patch).
    # Round 4 found causeOverflow[cause] was STILL a boolean with no
    # magnitude check -- the identical defect as round 2's original bug
    # (marker presence) and round 3's regression (causeCount trusted the
    # moment it was "tracked"), recurring a 3rd time at a new layer. Deleted
    # the generic causeOverflow[cause] fallback entirely (now provably
    # redundant: causeCount is unconditional/exact, so it already covers
    # every legitimate same-binary overflow via the first check; the
    # boolean could only ever fire in the unsafe mixed-era gap it was
    # supposed to guard). Fixed one now-stale sub-case in
    # TestGitHubTestsChunkedFinalMetadataOverflowShortcutExcludesReportParseCauses
    # that manufactured a causeOverflow-without-causeCount cursor no real
    # binary could produce. Also fixed the P2 sibling: the totality-gate
    # ERROR log had its own separate copy of the skipped-sample attrs that
    # silently missed skipped_sample_cause_overflow when round 2 added it to
    # the OTHER copy -- extracted githubTestsSkippedArtifactMarkerAttrs so
    # both (and any future third caller) share one builder. Added 2 new
    # ordinary top-level tests, none integration-tagged:
    # TestGitHubTestsAllArtifactsUnreadableLogsCauseOverflow
    # (github_tests_all_artifacts_unreadable_test.go) and
    # TestGitHubTestsReportMemberMagnitudeInvariant
    # (complete_route_comparator_decoded_test.go) -- the latter pins THE
    # class invariant ("watermark advances only when some signal proves the
    # FULL Count; no boolean may excuse a magnitude") with the round
    # 2/3/5 repros as its three sub-cases, so a 4th recurrence of this
    # pattern fails loudly here instead of needing a round 6 to find it.
    # 1292 -> 1294 top-level; 152 -> 152 integration-tagged (unchanged).
    # CHAOS-4592/4601 codex review gate round 5's OWN merge-gate re-run
    # (terra/xhigh, full-base + ledger, THE INVARIANT confirmed closed: "No
    # additional watermark-advance defect found... the sole boolean legacy
    # fallback remains limited to artifact_oversized"). Found 1 P2 + 1 P3, a
    # DIFFERENT class from the invariant (not a watermark-advance defect --
    # both fixed the same commit). P2: RunID/ArtifactID (json.Number-decoded,
    # syntactically unbounded) were never length-bounded the way Name was
    # (round 1's own fix) -- an oversized provider-supplied ID could blow
    # maxChunkCursorBytes and fail the checkpoint outright, losing progress
    # instead of degrading into overflow. Fixed with
    # githubTestsMaxArtifactIDBytes (24) truncation at the same single append
    # site Name already used; the worst-case budget test's RunID/ArtifactID
    # values were also fixed to genuinely exercise the new hard bound rather
    # than realistic-looking-but-short values. P3: per-cause sample overflow
    # had no metric of its own, only RecordCicdPartialSuccess's single
    # dominant-reason label (indistinguishable from a single skip) and the
    # round-2 log line. Added
    # dev_health_provider_skipped_artifact_cause_overflow_total, wired
    # through observeCicdPartialSuccess. 1 new ordinary top-level test:
    # TestGitHubTestsSkippedArtifactAppendTruncatesOversizedID
    # (github_tests_chunk_cursor_budget_test.go). Not integration-tagged.
    # TestObserveCicdPartialSuccessRecordsPerCauseOverflow (the P3 fix's
    # test) is in internal/jobs/providerunit, does not count toward this pin.
    # 1294 -> 1295 top-level; 152 -> 152 integration-tagged (unchanged).
    # CHAOS-4592/4601 codex review gate round 6 (terra/xhigh, full-base +
    # ledger; THE INVARIANT still confirmed closed -- these 2 findings are
    # in items 24/25's OWN robustness/telemetry class, not the invariant).
    # P2: item 24's ID truncation bounded only NEWLY appended markers --
    # normalizeLegacyGitHubTestsSkippedArtifacts re-truncates an inherited
    # Name for exactly this reason (a prior binary's shape can exceed this
    # binary's own caps) but left RunID/ArtifactID untouched, so a resumed
    # cursor with a legacy oversized ID could still fail its next checkpoint
    # after any new skip. Fixed by re-truncating RunID/ArtifactID in the
    # same legacy-normalize loop that already re-truncates Name. 1 new
    # ordinary top-level test:
    # TestGitHubTestsChunkCursorNormalizeRetruncatesInheritedIDs
    # (github_tests_chunk_cursor_budget_test.go). Not integration-tagged.
    # 1295 -> 1296 top-level; 152 -> 152 integration-tagged (unchanged).
    # CHAOS-4757: 2 new ordinary top-level tests --
    # TestExtractGitHubClosingIssueReferencesEmitsDedupedCrossRepoEdges,
    # TestExtractGitHubClosingIssueReferencesRejectsMalformedNode
    # (github_work_items_rows_test.go). Not integration-tagged.
    # 1296 -> 1298 top-level; 152 -> 152 integration-tagged (unchanged).
    # CHAOS-4757 (codex round 2b fix): 2 new ordinary top-level tests --
    # TestGitHubWorkItemPRSocialFetcherSignalsClosingReferenceTruncation,
    # TestGitHubWorkItemPRSocialFetcherClosingReferenceCompletePageIsNotTruncated
    # (github_work_items_social_fetch_test.go). Not integration-tagged.
    # 1298 -> 1300 top-level; 152 -> 152 integration-tagged (unchanged).
    # CHAOS-4757 (Jira dev-status slice): 7 new ordinary top-level tests --
    # TestJiraDevStatusPullRequestSourceIDParsesTrustedGitHubURLOnly,
    # TestExtractJiraDevStatusDependenciesEmitsDedupedPrimaryEdges,
    # TestFetchJiraDevStatusPullRequestsParsesOKResponse,
    # TestFetchJiraDevStatusPullRequestsTreats400And404AsCleanNoOp,
    # TestFetchJiraDevStatusPullRequestsFailsOnUnexpectedStatus
    # (jira_dev_status_test.go); TestJiraWorkItemsRouteDevStatusSyncsPrimaryDependencyRow,
    # TestJiraWorkItemsRouteDevStatusUnavailableIsCleanNoOp (jira_work_items_route_test.go).
    # Not integration-tagged. 1300 -> 1307 top-level; 152 -> 152 integration-tagged (unchanged).
    # codex round 1 (P1) moved the dev-status wiring from JiraWorkItemsRouteHandler
    # (never constructed by the worker) to JiraAtlassianRouteHandler (the real
    # route): -2 (jira_work_items_route_test.go) +3
    # (TestJiraAtlassianRouteDevStatusSyncsPrimaryDependencyRow,
    # TestJiraAtlassianRouteDevStatusUnavailableIsCleanNoOp,
    # TestJiraAtlassianRouteDevStatusCapCountsRealWireAttempts). codex round 1 (P2)
    # added a real-wire-attempt counting fix: +2
    # (TestFetchJiraDevStatusPullRequestsCountingAttemptsCountsRetries,
    # TestFetchJiraDevStatusPullRequestsCountingAttemptsCountsExactlyOneOnSuccess,
    # jira_dev_status_test.go). Net +3. 1307 -> 1310 top-level; 152 -> 152
    # integration-tagged (unchanged).
    # codex round 2 (P2): the cap must limit the retry policy itself, not just
    # count after the fact -- TestJiraAtlassianRouteDevStatusCapCountsRealWireAttempts
    # renamed to TestJiraAtlassianRouteDevStatusCapLimitsRealWireAttempts (net 0) plus
    # 1 new test, TestFetchJiraDevStatusPullRequestsCountingAttemptsHonorsRemainingBudget
    # (jira_dev_status_test.go). Net +1. 1310 -> 1311 top-level; 152 -> 152
    # integration-tagged (unchanged).
    # codex round 3 (CLEAN, coverage note): added
    # TestJiraAtlassianRouteDevStatusBudgetIsSharedAcrossIssues, EXECUTED
    # multi-issue coverage for the cross-issue budget invariant round 3 verified
    # only statically. 1311 -> 1312 top-level; 152 -> 152 integration-tagged
    # (unchanged).
    # codex round 4 (scoped, P3): the multi-issue test above covered only
    # all-503 exhaustion, not proving a clean 400/404 no-op also debits the
    # shared budget. Added TestJiraAtlassianRouteDevStatusCleanNoOpStillDebitsSharedBudget
    # (production confirmed correct by codex's own mutation probes; this closes
    # the missing regression oracle). 1312 -> 1313 top-level; 152 -> 152
    # integration-tagged (unchanged).
    # CHAOS-4848 (comment-only PR + its drift guard): seven doc-comments in
    # internal/providersync asserted their symbol was unregistered/inactive while
    # cmd/dev-health-worker/provider_sync.go constructs it. Corrected the comments
    # and added a guard so the class stops being hand-maintained -- the previous
    # "fix" for this class was enumerating one more site, which then silently
    # missed four. +3 ordinary tests (1313 -> 1316):
    # TestNoLiveSymbolIsDocumentedAsUnregistered (the guard; red on the four
    # unfixed sites before the comment fixes), TestDriftGuardCatchesAPlantedStaleClaim
    # (drives the real detector against a planted violation AND against quoted
    # retractions, so a matcher that stopped matching cannot read as clean), and
    # TestDriftGuardSeesTheSymbolsItMustCover (validates the discovery mechanism
    # against a known superset, plus a negative control that
    # JiraWorkItemsRouteHandler is NOT reported wired -- its "intentionally
    # unregistered" comment is true and must stay). All three parse source with
    # go/ast and touch no database, so the integration-tagged count stays 152.
    # codex round 2 (NOT CLEAN, 3x P2 EXECUTED) then showed the guard accepting
    # a stale claim on a CONSTRUCTOR doc and on a type embedded two hops below a
    # wired handler. Discovery now walks func docs and closes over struct fields
    # to fixpoint, pinned by +1 ordinary test (1316 -> 1317):
    # TestDriftGuardCoversConstructorDocsAndDeepFields. Parses source only, so
    # the integration-tagged count stays 152.
    # codex round 3 (NOT CLEAN, 2xP2+P3) then killed the prose-marker heuristic
    # outright in favour of a lexical SUPERSEDED: tag, and a reach probe found
    # discovery filtered `Recv == nil` and accepted only token.TYPE -- so method,
    # var and const docs were silently unread. Discovery now reads the closed set
    # of Go decl kinds. +1 ordinary test (1317 -> 1318):
    # TestDriftGuardReadsEveryDeclKindItClaims, red on the old filter (verified by
    # mutation: WiredThing/SomeVar/SomeConst all report "is NOT read"). Parses
    # source only, so the integration-tagged count stays 152.
    assert len(expected_provider_tests) == 1318
    assert len(expected_integration_tests) == 152
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
    assert len(provider_flattened) == len(set(provider_flattened)) == 1318
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
    for shard in (2, 3):
        result = _run_check_go("integration-shard", "packages", str(shard), "--dry-run")
        assert result.returncode == 0, result.stdout + result.stderr
        assert f"integration package shard {shard}: DRY RUN" in result.stdout
        selected_packages.extend(
            line.removeprefix("  SHARD-RUN ")
            for line in result.stdout.splitlines()
            if line.startswith("  SHARD-RUN ")
        )

    # CHAOS-4655: 34, not 33 -- cmd/query-api/internal/workgraph added
    # (35 discovered - 1 for the providersync shard-1 package = 34 across
    # shards 2/3).
    # CHAOS-4441: 35, not 34 -- internal/jobs/investment/chquery added
    # (36 discovered - 1 for the providersync shard-1 package = 35 across
    # shards 2/3).
    # CHAOS-4766: 36, not 35 -- internal/jobs/workgraph/edges added
    # (37 discovered - 1 for the providersync shard-1 package = 36 across
    # shards 2/3).
    # CHAOS-4882 and CHAOS-4769: 38, not 36 -- both packages added
    # (39 discovered - 1 for the providersync shard-1 package = 38 across
    # the packages shards).
    # CHAOS-4441: 39, not 38 -- internal/jobs/investment/chwrite added
    # (40 discovered - 1 for the providersync shard-1 package = 39).
    assert len(selected_packages) == len(set(selected_packages)) == 39
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
    assert len(selected_tests) == len(set(selected_tests)) == 1318
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

    postgres = _declared_image("PostgresImage")
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
        f"pull {_pinned_clickhouse_image()}",
        f"pull {_declared_image('ValkeyImage')}",
        f"pull {_declared_image('ReaperImage')}",
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
        f"pull {_declared_image('PostgresImage')}",
        f"pull {_pinned_clickhouse_image()}",
        f"pull {_declared_image('ValkeyImage')}",
        f"pull {_declared_image('ReaperImage')}",
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
_NON_CONTAINER_VERBS = frozenset(
    {
        "fmt",
        "vet",
        "test",
        "race",
        "live-python-oracles",
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
# the very quota that workflow exists to escape. These seven are ticketed debt.
_IMAGE_PULLING_ACTION_DEBT = {
    ("docker-images.yml", "build", "docker/setup-buildx-action"),
    ("docker-images.yml", "merge", "docker/setup-buildx-action"),
    ("docker-images.yml", "go-build", "docker/setup-buildx-action"),
    ("docker-images.yml", "go-merge", "docker/setup-buildx-action"),
    # CHAOS-4906: arc-runner-image.yml (PR #2154) needs a docker-container
    # buildx driver for cache-to: type=gha and for push+load together in one
    # build, neither of which the default docker-driven builder supports --
    # same tradeoff docker-images.yml already carries as debt above, not a
    # new one.
    ("arc-runner-image.yml", "build", "docker/setup-buildx-action"),
    # CHAOS-4906: the (worker, linux/arm64) self-hosted-pool pilot pair --
    # same tradeoff as go-build/go-merge above (mirrors the exact same
    # build-and-push steps for one matrix combination), not a new one.
    (
        "docker-images.yml",
        "go-build-worker-arm64-self-hosted",
        "docker/setup-buildx-action",
    ),
    (
        "docker-images.yml",
        "go-build-worker-arm64-fallback",
        "docker/setup-buildx-action",
    ),
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
    "test_aggregate_gate_results.py": frozenset(
        {"_code_filter_patterns", "_job", "_steps"}
    ),
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
