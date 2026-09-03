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

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestMembershipEndToEndAgainstRealClickHouse is the real-engine proof for
// CHAOS-4282: everything membership_native_test.go/membership_write_test.go
// prove against fakes, proved again against the REAL migrated schema and a
// REAL clickhouse-go connection.
//
// It exists specifically to close the one assumption no unit test can check:
// theme_distribution_json/subcategory_distribution_json are
// Map(String, Float64) columns, and fetchLatestDistributions reads them via
// mapKeys()/mapValues() rather than scanning the Map column directly, because
// clickhouse-go decodes Map columns into a genuine Go map (lib/column/map.go,
// reflect.MakeMap) whose iteration order is not the physical insertion order
// units.Distribution needs to preserve (see fetchLatestDistributions' doc
// comment). That claim is about the DRIVER's real behaviour against a REAL
// server -- a fake can only assert what this package's own code does with
// whatever it is handed, never whether the query this file wrote actually
// round-trips a Map value through a live ClickHouse the way the comment
// claims.
func TestMembershipEndToEndAgainstRealClickHouse(t *testing.T) {
	ctx := context.Background()
	conn := membershipMigratedClickHouse(t, ctx)

	writer, err := NewMembershipClickHouseWriter(conn)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	executor, err := NewMembershipExecutor(ctx, conn, writer)
	if err != nil {
		t.Fatalf("new executor (real migrated schema must be accepted): %v", err)
	}

	orgID := "org-" + uuid.NewString()
	seedMembershipEdges(t, ctx, conn, orgID)
	matchedID, skippedID := membershipSeedUnitIDs(t)
	seedMembershipInvestment(t, ctx, conn, orgID, matchedID)

	// Run 1: org-wide. One matched unit (multi-membership: 2 theme rows + 1
	// subcategory row, per-node -> 2 nodes * 3 rows = 6), one skipped (no
	// investment row seeded for it).
	outcome, err := executor.ComputeOrg(ctx, orgID, nil, time.Now())
	if err != nil {
		t.Fatalf("ComputeOrg run 1: %v", err)
	}
	if outcome.Components != 2 || outcome.Matched != 1 || outcome.Skipped != 1 || outcome.MembershipRows != 6 {
		t.Fatalf("run 1 outcome = %+v, want Components=2 Matched=1 Skipped=1 MembershipRows=6", outcome)
	}

	rows := queryMembershipRows(t, ctx, conn, orgID)
	if len(rows) != 6 {
		t.Fatalf("work_unit_membership has %d rows for org %s, want 6", len(rows), orgID)
	}
	for _, row := range rows {
		if row.workUnitID != matchedID {
			t.Errorf("row for unit %q, want only the matched unit %q (skipped unit %q must contribute nothing)",
				row.workUnitID, matchedID, skippedID)
		}
	}
	// This is the load-bearing assertion for the mapKeys/mapValues doc
	// comment: if the driver's real Map decode order did not survive the
	// round trip, one of the two categories below would come back with the
	// OTHER's weight, or is_dominant would land on the wrong category.
	assertMembershipCategory(t, rows, "theme", "feature_delivery", 0.65, 1)
	assertMembershipCategory(t, rows, "theme", "maintenance", 0.35, 0)
	assertMembershipCategory(t, rows, "subcategory", "backend", 1.0, 1)

	runIDs := queryMembershipRunIDs(t, ctx, conn, orgID)
	if len(runIDs) != 1 {
		t.Fatalf("work_unit_membership_runs has %d rows after run 1, want 1: %v", len(runIDs), runIDs)
	}
	firstRunID := runIDs[0]

	// Run 2 and run 3: same org, same seed. Each publishes its own run_id, so
	// after run 3 there are 3 markers total -- one more than keep=2 retains.
	if _, err := executor.ComputeOrg(ctx, orgID, nil, time.Now()); err != nil {
		t.Fatalf("ComputeOrg run 2: %v", err)
	}
	if _, err := executor.ComputeOrg(ctx, orgID, nil, time.Now()); err != nil {
		t.Fatalf("ComputeOrg run 3: %v", err)
	}

	// PruneMembershipRuns' ALTER TABLE DELETE is an async ClickHouse
	// mutation (no mutations_sync setting, matching the Python sink's own
	// prune_membership_runs -- see membership_write.go's doc comment for why
	// that is deliberate parity, not an oversight), so the retention effect
	// is polled rather than asserted immediately.
	waitForCondition(t, 10*time.Second, func() bool {
		return len(queryMembershipRunIDs(t, ctx, conn, orgID)) == membershipRetentionKeep
	}, "expected exactly keep=2 membership_backfill run markers to survive retention")

	finalRunIDs := queryMembershipRunIDs(t, ctx, conn, orgID)
	for _, id := range finalRunIDs {
		if id == firstRunID {
			t.Errorf("the FIRST run's marker (%s) survived retention; only the latest %d generations should remain: %v",
				firstRunID, membershipRetentionKeep, finalRunIDs)
		}
	}
	waitForCondition(t, 10*time.Second, func() bool {
		return len(queryMembershipRows(t, ctx, conn, orgID)) == 6
	}, "expected exactly 6 membership rows to survive retention (the pruned generation's rows deleted, the kept generations' rows are byte-identical re-writes of the same seed)")
}

func waitForCondition(t *testing.T, timeout time.Duration, check func() bool, message string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if check() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for: %s", timeout, message)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// membershipSeedUnitIDs computes the two work_unit_ids seedMembershipEdges'
// two disjoint components hash to, using the SAME production function the
// executor itself calls (units.WorkUnitID) -- not a hand-copied hex string,
// which would silently stop testing the real hash the moment the algorithm
// or the seed data changed.
func membershipSeedUnitIDs(t *testing.T) (matched, skipped string) {
	t.Helper()
	matched = units.WorkUnitID([]units.NodeKey{
		{Type: "pull_request", ID: "1"}, {Type: "issue", ID: "1"},
	})
	skipped = units.WorkUnitID([]units.NodeKey{
		{Type: "pull_request", ID: "2"}, {Type: "issue", ID: "2"},
	})
	return matched, skipped
}

func seedMembershipEdges(t *testing.T, ctx context.Context, conn driver.Conn, orgID string) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO work_graph_edges (
		edge_id, org_id, source_type, source_id, target_type, target_id,
		edge_type, repo_id, provider, provenance, confidence, evidence,
		last_synced, event_ts, day
	)`)
	if err != nil {
		t.Fatalf("prepare work_graph_edges batch: %v", err)
	}
	now := time.Now().UTC()
	repoID := uuid.New()
	rows := [][2]string{
		{"1", "1"}, // pull_request:1 -> issue:1
		{"2", "2"}, // pull_request:2 -> issue:2
	}
	for _, pair := range rows {
		if err := batch.Append(
			"edge-"+pair[0], orgID, "pull_request", pair[0], "issue", pair[1],
			"relates_to", repoID, "github", "native", float32(0.9), "",
			now, now, now,
		); err != nil {
			t.Fatalf("append work_graph_edges row: %v", err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send work_graph_edges batch: %v", err)
	}
}

func seedMembershipInvestment(
	t *testing.T, ctx context.Context, conn driver.Conn, orgID, workUnitID string,
) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO work_unit_investments (
		work_unit_id, work_unit_type, work_unit_name, from_ts, to_ts, repo_id,
		provider, effort_metric, effort_value, theme_distribution_json,
		subcategory_distribution_json, structural_evidence_json, evidence_quality,
		evidence_quality_band, categorization_status, categorization_errors_json,
		categorization_model_version, categorization_input_hash,
		categorization_run_id, computed_at, org_id
	)`)
	if err != nil {
		t.Fatalf("prepare work_unit_investments batch: %v", err)
	}
	now := time.Now().UTC()
	// Insertion order matters: feature_delivery before maintenance. If
	// clickhouse-go's Map decode did not preserve it, this is the row that
	// would catch it -- see the test's own doc comment.
	themeDistribution := map[string]float64{"feature_delivery": 0.65, "maintenance": 0.35}
	subcategoryDistribution := map[string]float64{"backend": 1.0}
	if err := batch.Append(
		workUnitID, nil, nil, now, now, nil,
		nil, "commits", 1.0, themeDistribution,
		subcategoryDistribution, "", 1.0,
		"high", "completed", "",
		"", "", "seed", now, orgID,
	); err != nil {
		t.Fatalf("append work_unit_investments row: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send work_unit_investments batch: %v", err)
	}
}

type membershipRowResult struct {
	workUnitID   string
	categoryKind string
	category     string
	weight       float64
	isDominant   uint8
}

func queryMembershipRows(
	t *testing.T, ctx context.Context, conn driver.Conn, orgID string,
) []membershipRowResult {
	t.Helper()
	rows, err := conn.Query(ctx, `
		SELECT work_unit_id, category_kind, category, weight, is_dominant
		FROM work_unit_membership
		WHERE org_id = {org_id:String}
		ORDER BY work_unit_id, node_id, category_kind, category
	`, clickhouse.Named("org_id", orgID))
	if err != nil {
		t.Fatalf("query work_unit_membership: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var result []membershipRowResult
	for rows.Next() {
		var row membershipRowResult
		if err := rows.Scan(&row.workUnitID, &row.categoryKind, &row.category, &row.weight, &row.isDominant); err != nil {
			t.Fatalf("scan work_unit_membership row: %v", err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate work_unit_membership rows: %v", err)
	}
	return result
}

func queryMembershipRunIDs(
	t *testing.T, ctx context.Context, conn driver.Conn, orgID string,
) []string {
	t.Helper()
	rows, err := conn.Query(ctx, `
		SELECT run_id FROM work_unit_membership_runs WHERE org_id = {org_id:String}
	`, clickhouse.Named("org_id", orgID))
	if err != nil {
		t.Fatalf("query work_unit_membership_runs: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var runIDs []string
	for rows.Next() {
		var runID string
		if err := rows.Scan(&runID); err != nil {
			t.Fatalf("scan work_unit_membership_runs row: %v", err)
		}
		runIDs = append(runIDs, runID)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate work_unit_membership_runs rows: %v", err)
	}
	return runIDs
}

func assertMembershipCategory(
	t *testing.T, rows []membershipRowResult, kind, category string, weight float64, isDominant uint8,
) {
	t.Helper()
	// Multi-membership: the same (kind, category) is expected on BOTH nodes
	// of the matched component (pull_request AND issue), so at least one
	// match is the bar, not exactly one.
	for _, row := range rows {
		if row.categoryKind != kind || row.category != category {
			continue
		}
		if row.weight != weight || row.isDominant != isDominant {
			t.Errorf("%s/%s: weight=%v is_dominant=%v, want weight=%v is_dominant=%v",
				kind, category, row.weight, row.isDominant, weight, isDominant)
		}
		return
	}
	t.Errorf("no row found for %s/%s among %d rows", kind, category, len(rows))
}

func membershipMigratedClickHouse(t *testing.T, ctx context.Context) driver.Conn {
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
