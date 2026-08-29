"""Contracts for deterministic Go storage-integration CI sharding."""

from __future__ import annotations

import json
import os
import re
import subprocess
import tempfile
from pathlib import Path
from typing import Any

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
    "cmd/query-api/internal/routeswitch",
    "internal/cacheinvalidation",
    "internal/externalrecompute",
    "internal/joboperator",
    "internal/joboutbox",
    "internal/jobrescue",
    "internal/jobroute",
    "internal/jobruntime",
    "internal/jobs/metrics/daily",
    "internal/jobs/metrics/remaining",
    "internal/jobs/pagerduty",
    "internal/jobs/report",
    "internal/jobs/system",
    "internal/jobs/workgraph",
    "internal/providerfoundation",
    "internal/providersync",
    "internal/scheduler/fixed",
    "internal/scheduler/sync",
    "internal/storage/postgres",
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
    assert match is not None
    image = match.group("image")
    assert re.fullmatch(r"[^@]+@sha256:[0-9a-f]{64}", image)
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
    env["GOTOOLCHAIN"] = "go1.25.9"
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
    assert "31 package(s) discovered, 0 denylisted, 31 will run" in result.stdout
    assert "integration shard plan: 3 shard(s), 31 package(s)" in result.stdout

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
    assert len(flattened) == len(set(flattened)) == 31
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
    # 1115 -> 1121 top-level; 126 -> 127 integration-tagged.
    assert len(expected_provider_tests) == 1121
    assert len(expected_integration_tests) == 127
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
    assert len(provider_flattened) == len(set(provider_flattened)) == 1121
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

    assert len(selected_packages) == len(set(selected_packages)) == 30
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
    assert len(selected_tests) == len(set(selected_tests)) == 1121
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


def test_clickhouse_prepull_retries_the_exact_source_pinned_image(
    tmp_path: Path,
) -> None:
    attempts = tmp_path / "attempts"
    docker_args = tmp_path / "docker-args"
    sleep_args = tmp_path / "sleep-args"
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

    image = _pinned_clickhouse_image()
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
    assert attempts.read_text(encoding="utf-8") == "3\n"
    assert docker_args.read_text(encoding="utf-8").splitlines() == [
        f"pull {image}",
        f"pull {image}",
        f"pull {image}",
    ]
    assert sleep_args.read_text(encoding="utf-8").splitlines() == ["5", "10"]
    assert f"pre-pulled pinned ClickHouse image {image} on attempt 3/3" in (
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
    assert attempts.read_text(encoding="utf-8") == "3\n"
    assert docker_args.read_text(encoding="utf-8").splitlines() == [
        f"pull {image}",
        f"pull {image}",
        f"pull {image}",
    ]
    assert sleep_args.read_text(encoding="utf-8").splitlines() == ["5", "10"]
    assert f"failed to pre-pull pinned ClickHouse image {image} after 3 attempts" in (
        failed.stderr
    )


def test_workflow_runs_all_shards_and_preserves_required_check_name() -> None:
    workflow = _workflow()
    jobs = workflow["jobs"]

    planner = jobs["go-storage-integration-plan"]
    assert planner["outputs"]["matrix"] == "${{ steps.plan.outputs.matrix }}"
    assert any(
        step.get("id") == "plan"
        and step.get("run") == "bash ci/check_go.sh integration-shard-plan"
        for step in planner["steps"]
    )

    shards = jobs["go-storage-integration-shard"]
    assert shards["needs"] == "go-storage-integration-plan"
    assert shards["timeout-minutes"] == 25
    assert shards["strategy"]["fail-fast"] is False
    assert shards["strategy"]["matrix"] == (
        "${{ fromJSON(needs.go-storage-integration-plan.outputs.matrix) }}"
    )
    shard_commands = [str(step.get("run", "")) for step in shards["steps"]]
    assert "bash ci/check_go.sh integration-prepull" in shard_commands
    assert (
        'bash ci/check_go.sh integration-shard "${{ matrix.target }}" '
        '"${{ matrix.shard }}"'
    ) in shard_commands
    assert shard_commands.index("bash ci/check_go.sh integration-prepull") < (
        shard_commands.index(
            'bash ci/check_go.sh integration-shard "${{ matrix.target }}" '
            '"${{ matrix.shard }}"'
        )
    )
    assert all("--dry-run" not in command for command in shard_commands)
    assert shards["name"] == (
        "go-storage-integration-shard-${{ matrix.target }}-${{ matrix.shard }}"
    )

    for job in (planner, shards):
        go_setup = next(
            step
            for step in job["steps"]
            if str(step.get("uses", "")).startswith("actions/setup-go@")
        )
        assert go_setup["uses"] == (
            "actions/setup-go@b7ad1dad31e06c5925ef5d2fc7ad053ef454303e"
        )
        assert go_setup["with"] == {
            "go-version-file": "go.mod",
            "cache-dependency-path": "**/go.sum",
        }
    assert re.search(
        r"(?m)^go 1\.25\.9$", (ROOT / "go.mod").read_text(encoding="utf-8")
    )

    aggregate = jobs["go-storage-integration"]
    assert aggregate["name"] == "go-storage-integration"
    assert aggregate["if"] == "${{ always() }}"
    assert set(aggregate["needs"]) == {
        "go-storage-integration-plan",
        "go-storage-integration-shard",
    }
    assert aggregate["env"] == {
        "PLAN_RESULT": "${{ needs.go-storage-integration-plan.result }}",
        "SHARD_RESULT": "${{ needs.go-storage-integration-shard.result }}",
    }
    aggregate_command = "\n".join(
        str(step.get("run", "")) for step in aggregate["steps"]
    )
    assert (
        'if [ "${PLAN_RESULT}" != "success" ] '
        '|| [ "${SHARD_RESULT}" != "success" ]; then'
    ) in aggregate_command
    assert "exit 1" in aggregate_command

    workflow_source = WORKFLOW.read_text(encoding="utf-8")
    assert workflow_source.count("- 'ci/go_integration_shards.tsv'") == 2
    assert workflow_source.count("- 'ci/go_providersync_test_shards.tsv'") == 2

    check_go_source = CHECK_GO.read_text(encoding="utf-8")
    assert 'GO_TOOLCHAIN="go1.25.9"' in check_go_source
    assert 'export GOTOOLCHAIN="${GO_TOOLCHAIN}"' in check_go_source
    assert 'export GOCACHE="${DEV_HEALTH_GO_CACHE}"' in check_go_source
    assert (
        "GOWORK=off go test -mod=readonly -tags=integration -count=1 "
        '-timeout=30m "${run_pkgs[@]}"'
    ) in check_go_source
    assert (
        "GOWORK=off go test -mod=readonly -tags=integration -count=1 "
        '-timeout=30m -run "${test_regex}" ./internal/providersync'
    ) in check_go_source
