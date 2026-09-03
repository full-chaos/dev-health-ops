//go:build integration

package remaining

import (
	"context"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestWorkItemAttributionClosurePromotesToOrgWide is the real-engine proof
// for team-lead's PR-B ruling: a scoped run whose linked_issue closure
// (donors of affected items, and items whose donor is affected, one hop
// each way) exceeds workItemAttributionClosurePromotionBound of the org's
// total item count is promoted to fully org-wide, with the reason recorded
// on the org-wide marker rather than a scoped one -- something no unit
// test against fakes can prove, since evaluateClosurePromotion's reverse
// hop is a live SQL query this file has no narrow interface to fake.
//
// Fixture: org has 4 items. A is owned by repoX (the only ownership
// change, so the FIRST run's scope is exactly {A}). A --relates_to--> C
// (forward: C is A's donor target). B --relates_to--> A (reverse: B's
// donor is the affected item A). D has no dependency edges at all --
// present only so the org total (4) makes the closure {A, B, C} = 3 cross
// the 25% bound (75%) without being the WHOLE org already, which would
// prove nothing about promotion specifically.
func TestWorkItemAttributionClosurePromotesToOrgWide(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionMigratedClickHouse(t, ctx)

	writer, err := NewWorkItemAttributionClickHouseWriter(conn)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	executor, err := NewWorkItemAttributionExecutor(ctx, conn, writer)
	if err != nil {
		t.Fatalf("new executor (real migrated schema must be accepted): %v", err)
	}

	orgID := "org-closure-" + uuid.NewString()
	repoX := uuid.New()
	now := time.Now().UTC()

	seedWorkItemAttributionItem(t, ctx, conn, orgID, "A", repoX, now)
	seedWorkItemAttributionItem(t, ctx, conn, orgID, "B", uuid.Nil, now)
	seedWorkItemAttributionItem(t, ctx, conn, orgID, "C", uuid.Nil, now)
	seedWorkItemAttributionItem(t, ctx, conn, orgID, "D", uuid.Nil, now)
	seedWorkItemAttributionRepoOwnership(t, ctx, conn, orgID, repoX, "team-infra", now)
	seedWorkItemAttributionDependency(t, ctx, conn, orgID, "A", "C", "relates_to", now)
	seedWorkItemAttributionDependency(t, ctx, conn, orgID, "B", "A", "relates_to", now)

	// A raw `SELECT count() ... FINAL` sees the freshly-inserted
	// team_repo_ownership row immediately, every time. teamattribution's
	// own LoadRepos -- ClickHouseFactSource's SELECT, the one ComputeOrg's
	// loadFacts actually calls -- does NOT: it aggregates via
	// argMax(...)/GROUP BY with no FINAL (a deliberate choice upstream, not
	// something PR-B can change), and was observed, reproducibly, to return
	// ZERO rows on the FIRST query issued right after the insert, then the
	// correct row on a second query a beat later, with no error either
	// time. Confirmed by hand: the SAME LoadRepos call, called twice in a
	// row, differs only in elapsed wall time. That gap is a real property
	// of the query this file depends on, not a bug this test should paper
	// over with FINAL of its own -- so it polls the EXACT call ComputeOrg
	// will make, not a proxy for it.
	waitForWorkItemAttributionRepoFactVisible(t, ctx, conn, orgID)

	outcome, err := executor.ComputeOrg(ctx, orgID)
	if err != nil {
		t.Fatalf("ComputeOrg run 1: %v", err)
	}
	if !outcome.OrgWide {
		t.Fatalf("outcome = %+v, want a PROMOTED (org-wide) run: closure {A,B,C} is 3/4 "+
			"of the org's items, over the 25%% bound", outcome)
	}
	if outcome.ItemsSeen != 4 {
		t.Fatalf("outcome.ItemsSeen = %d, want 4 -- a promoted run must cover the WHOLE "+
			"org (A,B,C,D), not just the 3-item closure that triggered the promotion", outcome.ItemsSeen)
	}
	if outcome.RowsWritten < 4 {
		t.Fatalf("outcome.RowsWritten = %d, want at least 4 (one row per item, even an "+
			"unassigned one for B/C/D)", outcome.RowsWritten)
	}

	runs := queryWorkItemAttributionRuns(t, ctx, conn, orgID)
	if len(runs) != 1 {
		t.Fatalf("work_item_attribution_backstop_runs has %d rows, want exactly 1: %v", len(runs), runs)
	}
	if !strings.Contains(runs[0].promotedReason, "linked_issue_closure_exceeded") {
		t.Fatalf("run marker promoted_reason = %q, want it to name the closure-promotion rule", runs[0].promotedReason)
	}

	scopedRuns := queryWorkItemAttributionScopedRuns(t, ctx, conn, orgID)
	if len(scopedRuns) != 0 {
		t.Fatalf("work_item_attribution_backstop_scoped_runs has %d rows, want 0 -- a "+
			"PROMOTED run must never also leave a scoped marker behind", len(scopedRuns))
	}

	attributionRows := queryWorkItemAttributionRows(t, ctx, conn, orgID)
	var sawA bool
	for _, row := range attributionRows {
		if row.workItemID == "A" {
			sawA = true
			if row.teamID == nil || *row.teamID != "team-infra" {
				t.Errorf("A's attribution row team_id = %v, want \"team-infra\" (repo_ownership)", row.teamID)
			}
		}
	}
	if !sawA {
		t.Fatal("no work_item_team_attributions row for A -- the promoted run must still cover the originally affected item")
	}

	// Run 2, immediately, with nothing changed: the org-wide watermark run 1
	// just published now covers everything (including the ownership row
	// that triggered run 1), so this must be a genuine no-op -- not a
	// second promoted run, and not a second write.
	outcome2, err := executor.ComputeOrg(ctx, orgID)
	if err != nil {
		t.Fatalf("ComputeOrg run 2: %v", err)
	}
	if !outcome2.SkippedNoop {
		t.Fatalf("run 2 outcome = %+v, want SkippedNoop -- nothing changed since run 1's watermark", outcome2)
	}

	runsAfter := queryWorkItemAttributionRuns(t, ctx, conn, orgID)
	if len(runsAfter) != 1 {
		t.Fatalf("work_item_attribution_backstop_runs has %d rows after the no-op run, want still 1", len(runsAfter))
	}
}

// waitForWorkItemAttributionRowVisible polls query (expected to return one
// count() column) until it reports at least one row, or fails the test once
// the budget below is exhausted. See its call site's comment for why this
// exists.
func waitForWorkItemAttributionRepoFactVisible(
	t *testing.T, ctx context.Context, conn driver.Conn, orgID string,
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for attempt := 1; ; attempt++ {
		repos, err := teamattribution.ClickHouseFactSource{Conn: conn}.LoadRepos(ctx, orgID, time.Now().UTC())
		if err != nil {
			t.Fatalf("poll LoadRepos: %v", err)
		}
		if len(repos) > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("LoadRepos never returned a row within the poll budget (%d attempts)", attempt)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func seedWorkItemAttributionItem(
	t *testing.T, ctx context.Context, conn driver.Conn,
	orgID, workItemID string, repoID uuid.UUID, now time.Time,
) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx,
		`INSERT INTO work_items (repo_id, work_item_id, provider, project_id, org_id, last_synced)`)
	if err != nil {
		t.Fatalf("prepare work_items batch: %v", err)
	}
	if err := batch.Append(repoID, workItemID, "github", "", orgID, now); err != nil {
		t.Fatalf("append work_items row: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send work_items batch: %v", err)
	}
}

func seedWorkItemAttributionRepoOwnership(
	t *testing.T, ctx context.Context, conn driver.Conn,
	orgID string, repoID uuid.UUID, teamID string, now time.Time,
) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO team_repo_ownership (
		org_id, provider, team_id, repo_id, repo_full_name, match_type, source,
		is_primary, specificity, priority, valid_from, valid_to, updated_at
	)`)
	if err != nil {
		t.Fatalf("prepare team_repo_ownership batch: %v", err)
	}
	if err := batch.Append(
		orgID, "github", teamID, repoID, "acme/"+teamID, "exact", "native",
		uint8(1), uint16(100), int32(0), now, nil, now,
	); err != nil {
		t.Fatalf("append team_repo_ownership row: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send team_repo_ownership batch: %v", err)
	}
}

func seedWorkItemAttributionDependency(
	t *testing.T, ctx context.Context, conn driver.Conn,
	orgID, sourceID, targetID, relationshipType string, now time.Time,
) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO work_item_dependencies (
		source_work_item_id, target_work_item_id, relationship_type, relationship_type_raw,
		last_synced, org_id
	)`)
	if err != nil {
		t.Fatalf("prepare work_item_dependencies batch: %v", err)
	}
	if err := batch.Append(sourceID, targetID, relationshipType, relationshipType, now, orgID); err != nil {
		t.Fatalf("append work_item_dependencies row: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send work_item_dependencies batch: %v", err)
	}
}

type workItemAttributionRunRow struct {
	runID          string
	promotedReason string
}

func queryWorkItemAttributionRuns(
	t *testing.T, ctx context.Context, conn driver.Conn, orgID string,
) []workItemAttributionRunRow {
	t.Helper()
	rows, err := conn.Query(ctx,
		`SELECT run_id, promoted_reason FROM work_item_attribution_backstop_runs FINAL WHERE org_id = ?`, orgID)
	if err != nil {
		t.Fatalf("query work_item_attribution_backstop_runs: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var result []workItemAttributionRunRow
	for rows.Next() {
		var row workItemAttributionRunRow
		if err := rows.Scan(&row.runID, &row.promotedReason); err != nil {
			t.Fatalf("scan work_item_attribution_backstop_runs row: %v", err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate work_item_attribution_backstop_runs: %v", err)
	}
	return result
}

func queryWorkItemAttributionScopedRuns(
	t *testing.T, ctx context.Context, conn driver.Conn, orgID string,
) []string {
	t.Helper()
	rows, err := conn.Query(ctx,
		`SELECT run_id FROM work_item_attribution_backstop_scoped_runs FINAL WHERE org_id = ?`, orgID)
	if err != nil {
		t.Fatalf("query work_item_attribution_backstop_scoped_runs: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var result []string
	for rows.Next() {
		var runID string
		if err := rows.Scan(&runID); err != nil {
			t.Fatalf("scan work_item_attribution_backstop_scoped_runs row: %v", err)
		}
		result = append(result, runID)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate work_item_attribution_backstop_scoped_runs: %v", err)
	}
	return result
}

type workItemAttributionRowResult struct {
	workItemID string
	teamID     *string
}

func queryWorkItemAttributionRows(
	t *testing.T, ctx context.Context, conn driver.Conn, orgID string,
) []workItemAttributionRowResult {
	t.Helper()
	rows, err := conn.Query(ctx,
		`SELECT work_item_id, team_id FROM work_item_team_attributions FINAL WHERE org_id = ?`, orgID)
	if err != nil {
		t.Fatalf("query work_item_team_attributions: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var result []workItemAttributionRowResult
	for rows.Next() {
		var row workItemAttributionRowResult
		if err := rows.Scan(&row.workItemID, &row.teamID); err != nil {
			t.Fatalf("scan work_item_team_attributions row: %v", err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate work_item_team_attributions: %v", err)
	}
	return result
}

// workItemAttributionMigratedClickHouse mirrors membershipMigratedClickHouse
// (membership_native_integration_test.go, CHAOS-4282): a fresh container,
// the real migration chain, and a real clickhouse-go connection -- no
// shared-instance caching, since this file's only test does not need it.
func workItemAttributionMigratedClickHouse(t *testing.T, ctx context.Context) driver.Conn {
	t.Helper()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := instance.Close(ctx); closeErr != nil {
			t.Logf("close clickhouse container: %v", closeErr)
		}
	})
	chschema.Apply(ctx, t, instance)

	dsn, err := containers.ClickHouseHTTPDSN(ctx, instance)
	if err != nil {
		t.Fatalf("clickhouse dsn: %v", err)
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	password, _ := parsed.User.Password()
	conn, err := clickhouse.Open(&clickhouse.Options{
		Protocol: clickhouse.HTTP,
		Addr:     []string{parsed.Host},
		Auth: clickhouse.Auth{
			Database: strings.TrimPrefix(parsed.Path, "/"),
			Username: parsed.User.Username(),
			Password: password,
		},
	})
	if err != nil {
		t.Fatalf("open clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}
