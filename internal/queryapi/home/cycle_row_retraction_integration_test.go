//go:build integration

package home

import (
	"context"
	"fmt"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chquery"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// A reopened work item's newest cycle row is a retraction (no completed_at, no hours). It is not a completion:
// the coverage denominator counts completed items only.
func TestFetchCoverageLeavesARetractedCycleRowOut(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	defer func() { _ = inst.Close(context.Background()) }()
	chschema.Apply(ctx, t, inst)
	opts, err := stdclickhouse.ParseDSN(inst.URI)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	client, err := chquery.NewProductionClient(inst.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	const org = "home-cycle-retraction-it"
	insert := func(id, status, completedAt, cycle, computedAt string) {
		seededExec(ctx, t, conn, fmt.Sprintf(
			`INSERT INTO work_item_cycle_times (work_item_id, provider, day, work_scope_id, type, status, created_at, completed_at, cycle_time_hours, computed_at, org_id) VALUES
			 ('%s', 'jira', '2026-01-02', 'scope-1', 'bug', '%s', toDateTime('2026-01-01 00:00:00'), %s, %s, toDateTime('%s'), '%s')`,
			id, status, completedAt, cycle, computedAt, org))
	}
	insert("done-1", "done", "toDateTime('2026-01-02 09:00:00')", "5.0", "2026-01-02 10:00:00")
	insert("reopened-1", "done", "toDateTime('2026-01-02 09:00:00')", "5.0", "2026-01-02 10:00:00")
	insert("reopened-1", "in_progress", "NULL", "NULL", "2026-01-03 10:00:00")

	got, err := fetchCoverage(ctx, client, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC), org)
	if err != nil {
		t.Fatalf("fetchCoverage: %v", err)
	}
	if got.IssuesWithCycleStatesPct == nil || *got.IssuesWithCycleStatesPct != 100.0 {
		t.Fatalf("issues_with_cycle_states_pct = %v, want 100 (1 completed item with a cycle time of 1 completed; the reopened item is not a completion)", got.IssuesWithCycleStatesPct)
	}
}
