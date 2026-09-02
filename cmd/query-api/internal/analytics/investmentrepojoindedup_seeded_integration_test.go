//go:build integration

// CHAOS-4773 red-first proof, against a REAL ClickHouse engine
// (containers.StartClickHouse, the same digest-pinned Testcontainers image
// every other seeded test in this package uses -- never the routingFakeClient
// double).
//
// SCENARIO: one work unit, one repo, allocation_weight exactly 1.0 (single
// subcategory summing to 1.0, single repo-effort row equal to the unit's own
// effort_value), attributed to team "Alpha". The `repos` row for that one
// repo is seeded as TWO physical versions in SEPARATE INSERT statements
// (never one multi-row INSERT -- optimize_on_insert, ON by default, would
// pre-merge same-sort-key ReplacingMergeTree rows within one INSERT block
// and silently hide the exact defect this test exists to catch; see
// lane-common-brief's ClickHouse seeded-test traps). No OPTIMIZE/merge is
// ever run, matching production's live window between a repo sync write and
// the next background merge (investment.go's investmentContextFor doc
// comment cites the executed system.part_log evidence for that window).
//
// RED on origin/main (and on any tree where investment.go:504's repos join
// has no FINAL): the TEAM node for "Alpha" reads 2.0 -- the unmerged
// duplicate physical repos row fans out every downstream row before the
// SUM(subcategory_kv.2 * allocation_weight) aggregate. GREEN once the join
// carries FINAL: exactly 1.0, matching the unit's true allocation_weight.
package analytics

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

func TestSankeyTeamNode_SeededRealClickHouse_RepoJoinDoesNotFanOutOnUnmergedRepoVersion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = inst.Close(context.Background()) }()

	opts, err := stdclickhouse.ParseDSN(inst.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatalf("open raw ClickHouse connection: %v", err)
	}
	defer func() { _ = conn.Close() }()

	for _, stmt := range splitSQLStatements(seededQualitySchemaDDL + seededCoverageExtraDDL) {
		if err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("exec DDL %q: %v", stmt, err)
		}
	}

	const orgID = "seeded-repojoin-dedup"
	const repoID = "33333333-3333-3333-3333-333333333333"

	// One unit, one repo, allocation_weight exactly 1.0 (effort_value ==
	// repo_effort_value), one subcategory summing to 1.0, attributed to
	// team Alpha -- the same worked-example shape as CHAOS-4773's ticket
	// (unit d86aca6013467340..., single repo, single subcategory), scaled
	// to a round number so the assertion is exact rather than a float
	// artifact of a fractional weight.
	seedCoverageUnits(t, ctx, conn, orgID, []seededCoverageUnit{
		{workUnitID: "wu-dup-repo", effortValue: 100, repoID: repoID, issueRef: "linear:ALPHA-1"},
	})

	if err := conn.Exec(ctx, fmt.Sprintf(`INSERT INTO work_unit_repo_effort
        (work_unit_id, repo_id, effort_metric, effort_value, allocation_weight, allocation_source, categorization_run_id, computed_at, org_id) VALUES
        ('wu-dup-repo', toUUID('%[1]s'), 'churn_loc', 100, 1.0, 'evidence', 'run-1', toDateTime64('%[2]s', 3, 'UTC'), '%[3]s')`,
		repoID, seededCoverageTS, orgID)); err != nil {
		t.Fatalf("seed work_unit_repo_effort: %v", err)
	}

	if err := conn.Exec(ctx, fmt.Sprintf(`INSERT INTO work_item_team_attributions
        (org_id, repo_id, work_item_id, provider, team_id, team_name, source, is_primary, confidence, evidence, computed_at) VALUES
        ('%[3]s', toUUID('%[1]s'), 'linear:ALPHA-1', 'linear', 'ALPHA', 'Alpha', 'native_team', 1, 'high', '', toDateTime64('%[2]s', 3, 'UTC'))`,
		repoID, seededCoverageTS, orgID)); err != nil {
		t.Fatalf("seed work_item_team_attributions: %v", err)
	}

	// THE DEFECT'S PRECONDITION: two physical versions of the SAME
	// (org_id, id) repos row, in separate INSERTs, never merged. This is
	// the live-production window this ticket's system.part_log evidence
	// found repeats every sync cycle (~10-40 min between NewPart and the
	// next MergeParts) -- not a contrived schema violation.
	if err := conn.Exec(ctx, fmt.Sprintf(`INSERT INTO repos
        (id, repo, ref, created_at, settings, tags, last_synced, org_id, provider, source_id) VALUES
        (toUUID('%[1]s'), 'acme/dup', NULL, toDateTime64('%[2]s', 3, 'UTC'), NULL, NULL, toDateTime64('2026-01-02 10:00:00.000', 3, 'UTC'), '%[3]s', 'github', NULL)`,
		repoID, seededCoverageTS, orgID)); err != nil {
		t.Fatalf("seed repos version 1: %v", err)
	}
	if err := conn.Exec(ctx, fmt.Sprintf(`INSERT INTO repos
        (id, repo, ref, created_at, settings, tags, last_synced, org_id, provider, source_id) VALUES
        (toUUID('%[1]s'), 'acme/dup', NULL, toDateTime64('%[2]s', 3, 'UTC'), NULL, NULL, toDateTime64('2026-01-02 11:00:00.000', 3, 'UTC'), '%[3]s', 'github', NULL)`,
		repoID, seededCoverageTS, orgID)); err != nil {
		t.Fatalf("seed repos version 2 (deliberately unmerged): %v", err)
	}

	// Confirm the precondition actually landed as two live physical rows
	// (guards against a false green if ClickHouse ever pre-merges within
	// a single test run): a raw scan without FINAL must see 2, and this
	// is exactly the count investmentrepojointelemetry.go's check query
	// computes.
	rows, err := conn.Query(ctx, "SELECT count() FROM repos WHERE toString(id) = ?", repoID)
	if err != nil {
		t.Fatalf("verify unmerged repos precondition: %v", err)
	}
	var rawVersions uint64
	if !rows.Next() {
		t.Fatal("verify unmerged repos precondition: no rows")
	}
	if scanErr := rows.Scan(&rawVersions); scanErr != nil {
		t.Fatalf("verify unmerged repos precondition scan: %v", scanErr)
	}
	_ = rows.Close()
	if rawVersions != 2 {
		t.Fatalf("precondition failed: repos has %d live physical rows for id %s, want 2 (test is not exercising the defect)", rawVersions, repoID)
	}

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	req, err := SankeyRequestFromInput(model.SankeyRequestInput{
		Path:    []model.DimensionInput{model.DimensionInputTeam, model.DimensionInputTheme, model.DimensionInputRepo},
		Measure: model.MeasureInputCount,
		DateRange: &model.DateRangeInput{
			StartDate: mustGraphQLDate("2026-01-01"),
			EndDate:   mustGraphQLDate("2026-01-08"),
		},
		MaxNodes: 16,
		MaxEdges: 100,
	})
	if err != nil {
		t.Fatalf("SankeyRequestFromInput: %v", err)
	}

	nodesQuery, edgesQueries, err := CompileSankey(req, orgID, queryTimeoutSecs, true, nil)
	if err != nil {
		t.Fatalf("CompileSankey: %v", err)
	}

	nodes, _, err := ExecuteSankeyQueries(ctx, client, []compiledQuery{nodesQuery}, edgesQueries)
	if err != nil {
		t.Fatalf("ExecuteSankeyQueries: %v", err)
	}

	var alphaValue *float64
	found := false
	for _, n := range nodes {
		if n.Dimension == "TEAM" && n.Label == "Alpha" {
			found = true
			alphaValue = n.Value
		}
	}
	if !found {
		t.Fatalf("no TEAM/Alpha node in result: %+v", nodes)
	}
	if alphaValue == nil {
		t.Fatal("TEAM/Alpha node value is nil, want 1.0")
	}

	const (
		want = 1.0
		tol  = 1e-9
	)
	if math.Abs(*alphaValue-want) > tol {
		if math.Abs(*alphaValue-2*want) <= tol {
			t.Fatalf("TEAM/Alpha value = %v, want %v -- this is exactly 2x, the CHAOS-4773 repos-join fan-out on the unmerged duplicate physical row (investment.go:504's join needs FINAL)", *alphaValue, want)
		}
		t.Fatalf("TEAM/Alpha value = %v, want %v", *alphaValue, want)
	}

	// Same assertion the ticket's per-unit decomposition made: a unit's
	// value can never exceed its own allocation_weight (here, 1.0). Kept
	// as an explicit, human-readable invariant check independent of the
	// exact-equality assertion above, so a future regression that changes
	// the expected constant still trips this guard.
	if *alphaValue > 1.0+tol {
		t.Fatalf("TEAM/Alpha value %v exceeds the unit's own allocation_weight 1.0 -- distribution weights must sum to <= allocation_weight per unit", *alphaValue)
	}

	// CHAOS-4773 codex round 2 (P2, CONFIRMED): investmentRepoDedupCollisionCheckSQL's
	// count() is UInt64; the REAL clickhouse-go driver rejects a UInt64 ->
	// *int64 Scan destination outright, unlike the fake row scanner every
	// OTHER telemetry test in this package uses (which does not enforce
	// wire-type exactness -- exactly why round 1's own green unit test did
	// not catch this). This is the one place in this PR's test suite that
	// exercises the check against the REAL driver: this seeded org's
	// `repos` table has exactly the deliberate 2-version duplicate seeded
	// above, so a correct check must report excess=1 here.
	resetRepoJoinDedupCollisionCooldown(t)
	var recordedOrgID string
	var recordedExcess int64
	recordedCalls := 0
	orig := recordInvestmentRepoJoinDedupCollisions
	recordInvestmentRepoJoinDedupCollisions = func(_ context.Context, gotOrgID string, gotExcess int64) {
		recordedCalls++
		recordedOrgID = gotOrgID
		recordedExcess = gotExcess
	}
	t.Cleanup(func() { recordInvestmentRepoJoinDedupCollisions = orig })

	RecordInvestmentRepoJoinDedupCollisions(ctx, client, orgID)

	if recordedCalls != 1 {
		t.Fatalf("dedup-collision check reported %d times, want exactly 1 -- if 0, the UInt64->*int64 Scan is failing against the real driver again (the exact regression this test guards)", recordedCalls)
	}
	if recordedOrgID != orgID || recordedExcess != 1 {
		t.Fatalf("dedup-collision check reported (org=%q, excess=%d), want (org=%q, excess=1)", recordedOrgID, recordedExcess, orgID)
	}
}
