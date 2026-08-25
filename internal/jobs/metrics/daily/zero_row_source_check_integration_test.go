//go:build integration

package daily

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// stubSourceDataConn answers ClickHouse Query calls by matching the query
// text against a table name -> "has rows" map. Every one of the checker's
// source and output queries names its table exactly once and each table name
// is unique across the four families, so a single substring match
// unambiguously distinguishes them without needing to parse the query.
type stubSourceDataConn struct {
	hasRows map[string]bool
	queries []string
}

func (conn *stubSourceDataConn) Query(_ context.Context, query string, _ ...any) (driver.Rows, error) {
	conn.queries = append(conn.queries, query)
	for table, present := range conn.hasRows {
		if strings.Contains(query, table) {
			return &boolRows{present: present}, nil
		}
	}
	return &boolRows{present: false}, nil
}

// boolRows is the minimal driver.Rows the checker's exists/existsOrgScoped
// helpers need: they only ever call Next and Err, never Scan.
type boolRows struct {
	present bool
	yielded bool
}

func (rows *boolRows) Next() bool {
	if rows.yielded || !rows.present {
		return false
	}
	rows.yielded = true
	return true
}
func (*boolRows) Scan(...any) error                { return nil }
func (*boolRows) ScanStruct(any) error             { return nil }
func (*boolRows) ColumnTypes() []driver.ColumnType { return nil }
func (*boolRows) Totals(...any) error              { return nil }
func (*boolRows) Columns() []string                { return nil }
func (*boolRows) Close() error                     { return nil }
func (*boolRows) Err() error                       { return nil }
func (*boolRows) HasData() bool                    { return true }

func insertPartitionScope(
	t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	runID, partitionID, orgID, day string, repoIDs []RepositoryID,
) {
	t.Helper()
	now := time.Now().UTC()
	if _, err := pool.Exec(ctx, `
INSERT INTO daily_metrics_runs (id, org_id, target_day, generation, status, finalization_status, created_at, updated_at)
VALUES ($1::uuid, $2::uuid, $3::date, 'zero-row-check-test', 'running', 'pending', $4, $4)`,
		runID, orgID, day, now); err != nil {
		t.Fatal(err)
	}
	if repoIDs == nil {
		repoIDs = []RepositoryID{}
	}
	raw, err := json.Marshal(repoIDs)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO daily_metrics_partitions (id, run_id, ordinal, repo_ids, status, attempt_count, created_at, updated_at)
VALUES ($1::uuid, $2::uuid, 0, $3::jsonb, 'pending', 0, $4, $4)`,
		partitionID, runID, raw, now); err != nil {
		t.Fatal(err)
	}
}

// TestClickHouseSourceDataCheckerFlagsOnlyFamiliesWithSourceButNoOutput pins
// the CHAOS-4263 ruling's core invariant: a family whose source data exists
// but whose output is empty is flagged; a family with no source data at all
// (a genuinely empty day) is not, even though its output is equally empty.
func TestClickHouseSourceDataCheckerFlagsOnlyFamiliesWithSourceButNoOutput(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createDailyTables(t, ctx, pool)

	const (
		runID       = "00000000-0000-4000-8000-000000000101"
		partitionID = "00000000-0000-4000-8000-000000000102"
		orgID       = "00000000-0000-4000-8000-000000000009"
		day         = "2026-08-25"
	)
	repoIDs := []RepositoryID{"00000000-0000-4000-8000-000000000002"}
	insertPartitionScope(t, ctx, pool, runID, partitionID, orgID, day, repoIDs)

	// cicd: source present, output missing -> flagged.
	// deploy: neither present -> genuinely empty day, not flagged.
	// testops_risk: source present, output present (both of its output
	// tables agree, to avoid the UNION ALL query's two table names racing
	// each other under a substring-keyed stub) -> not flagged.
	// incident: source present, output missing -> flagged.
	conn := &stubSourceDataConn{hasRows: map[string]bool{
		"ci_pipeline_runs":           true,
		"cicd_metrics_daily":         false,
		"deployments":                false,
		"deploy_metrics_daily":       false,
		"test_suite_results":         true,
		"testops_release_confidence": true,
		"testops_pipeline_stability": true,
		"operational_incidents":      true,
		"incident_metrics_daily":     false,
	}}
	checker, err := NewClickHouseSourceDataChecker(pool, conn)
	if err != nil {
		t.Fatal(err)
	}
	families, err := checker.ZeroRowFamiliesWithSourceData(ctx, partitionID)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, family := range families {
		got[family] = true
	}
	if !got["cicd"] || got["deploy"] || got["testops_risk"] || !got["incident"] {
		t.Fatalf("flagged families=%v, want exactly cicd and incident", families)
	}
}

// TestClickHouseSourceDataCheckerSkipsPartitionsWithNoRepositories pins that a
// no_repositories partition (empty repo_ids) is never checked: that terminal
// state is MaterializeScheduledFanout's, not this checker's, to report.
func TestClickHouseSourceDataCheckerSkipsPartitionsWithNoRepositories(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createDailyTables(t, ctx, pool)

	const (
		runID       = "00000000-0000-4000-8000-000000000103"
		partitionID = "00000000-0000-4000-8000-000000000104"
		orgID       = "00000000-0000-4000-8000-000000000009"
		day         = "2026-08-25"
	)
	insertPartitionScope(t, ctx, pool, runID, partitionID, orgID, day, nil)

	conn := &stubSourceDataConn{hasRows: map[string]bool{
		"ci_pipeline_runs": true, "cicd_metrics_daily": false,
	}}
	checker, err := NewClickHouseSourceDataChecker(pool, conn)
	if err != nil {
		t.Fatal(err)
	}
	families, err := checker.ZeroRowFamiliesWithSourceData(ctx, partitionID)
	if err != nil {
		t.Fatal(err)
	}
	if len(families) != 0 {
		t.Fatalf("no_repositories partition was checked, flagged=%v", families)
	}
	if len(conn.queries) != 0 {
		t.Fatalf("no_repositories partition issued %d ClickHouse queries, want 0", len(conn.queries))
	}
}
