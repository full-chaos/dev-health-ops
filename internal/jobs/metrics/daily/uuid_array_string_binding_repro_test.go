//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestPreFixArrayStringBindingAgainstRealUUIDColumn reproduces, against a
// real ClickHouse UUID column, the exact binding shapes the codex review on
// #1911 named as a BLOCK finding: []string values bound where the production
// column is UUID, in both directions -- (1) the query text itself declaring
// {repo_ids:Array(String)}, and (2) the query text declaring the CORRECT
// {repo_ids:Array(UUID)} but the Go-side value still being []string.
//
// Result (chris's ruling 2026-08-25: a "prod-facing bug" claim needs a red
// artifact, not code logic -- this is that artifact): BOTH shapes ran with NO
// error and MATCHED the real row. clickhouse-go v2 against this pinned
// ClickHouse version coerces String<->UUID for an IN comparison in both
// directions. The original BLOCK finding does NOT reproduce as an observed
// runtime defect in this environment -- downgrading it from "proven bug" to
// "provisional (code-argued)": the shipped []uuid.UUID binding
// (zero_row_source_check.go) is still the correct engineering choice (Go-side
// type safety, no reliance on an implicit driver/server coercion that a
// future ClickHouse or driver version is not contractually obligated to keep
// doing), but it is not fixing an observed false negative.
func TestPreFixArrayStringBindingAgainstRealUUIDColumn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.Exec(ctx, `
CREATE TABLE ci_pipeline_runs (
    org_id String,
    repo_id UUID,
    finished_at DateTime64(3, 'UTC')
) ENGINE = MergeTree ORDER BY (org_id, repo_id, finished_at)`); err != nil {
		t.Fatal(err)
	}
	const (
		orgID  = "00000000-0000-4000-8000-000000000009"
		repoID = "00000000-0000-4000-8000-000000000002"
	)
	if err := conn.Exec(ctx, `
INSERT INTO ci_pipeline_runs (org_id, repo_id, finished_at)
VALUES ('00000000-0000-4000-8000-000000000009', toUUID('00000000-0000-4000-8000-000000000002'), toDateTime64('2026-08-25 12:00:00', 3, 'UTC'))`); err != nil {
		t.Fatal(err)
	}

	for _, testCase := range []struct {
		name        string
		paramClause string
	}{
		{name: "query_declares_Array(String)", paramClause: "{repo_ids:Array(String)}"},
		{name: "query_declares_Array(UUID)_go_value_is_[]string", paramClause: "{repo_ids:Array(UUID)}"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			rows, err := conn.Query(ctx, `
SELECT 1 FROM ci_pipeline_runs
WHERE org_id = {org_id:String} AND repo_id IN `+testCase.paramClause+`
  AND finished_at IS NOT NULL AND toDate(finished_at) = {day:Date}
LIMIT 1`,
				clickhouse.Named("org_id", orgID),
				clickhouse.Named("day", "2026-08-25"),
				clickhouse.Named("repo_ids", []string{repoID}),
			)
			if err != nil {
				t.Fatalf("query rejected at bind/execute time (this WOULD be the proven bug): %v", err)
			}
			defer rows.Close()
			found := rows.Next()
			if rowErr := rows.Err(); rowErr != nil {
				t.Fatalf("row iteration error (this WOULD be the proven bug): %v", rowErr)
			}
			if !found {
				t.Fatal("query executed with no error but matched zero rows against a real, matching UUID -- THIS is the proven bug shape (a live repo_id would never be found)")
			}
			t.Log("no error, row matched: this []string-against-UUID binding shape is NOT observably broken in this clickhouse-go v2 + ClickHouse version")
		})
	}
}
