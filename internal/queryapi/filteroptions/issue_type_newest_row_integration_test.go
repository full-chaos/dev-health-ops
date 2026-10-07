//go:build integration

package filteroptions

import (
	"context"
	"reflect"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestIssueTypeFilterListHoldsEachTypeOnce is the reader half of the
// append-only contract of issue_type_metrics_daily (CHAOS-8810), for its one
// reader: the issue type list of the filter options.
//
// The table is plain MergeTree and every writer appends: for one key and one
// day the store holds the row of the full sync, the row of an hourly sync unit
// and the row of the daily job. The reader reads one column, which is part of
// the row key, so for it "the newest computed_at per key" and "each value
// once" are the same answer, and DISTINCT is what gives it. Without it the
// list would hold "bug" three times.
func TestIssueTypeFilterListHoldsEachTypeOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = inst.Close(context.Background()) }()
	chschema.Apply(ctx, t, inst)

	opts, err := stdclickhouse.ParseDSN(inst.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatalf("open raw ClickHouse connection: %v", err)
	}
	defer func() { _ = conn.Close() }()
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	const org = "org-8810-issue-types"
	const repo = "11111111-1111-4111-8111-111111111111"
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	at := func(hour, minute int) time.Time {
		return day.Add(time.Duration(hour)*time.Hour + time.Duration(minute)*time.Minute)
	}
	insert := `INSERT INTO issue_type_metrics_daily
		(repo_id, day, provider, team_id, issue_type_norm, created_count, completed_count,
		 active_count, cycle_p50_hours, cycle_p90_hours, lead_p50_hours, computed_at, org_id)
		VALUES (?, ?, 'github', 'unassigned', ?, 0, ?, ?, 0, 0, 0, ?, ?)`
	for _, row := range []struct {
		issueType  string
		completed  uint32
		computedAt time.Time
	}{
		{"bug", 3, at(10, 0)},  // the full sync
		{"bug", 1, at(11, 0)},  // the hourly unit
		{"bug", 4, at(11, 5)},  // the daily job
		{"task", 1, at(10, 0)}, // the full sync
		{"task", 0, at(11, 5)}, // the daily job: a row of zeros, the type is still a type
	} {
		if err := conn.Exec(ctx, insert, repo, day, row.issueType, row.completed, row.completed, row.computedAt, org); err != nil {
			t.Fatalf("insert %+v: %v", row, err)
		}
	}
	// Another tenant's type must not be in the list.
	if err := conn.Exec(ctx, insert, repo, day, "epic", uint32(1), uint32(1), at(10, 0), "org-8810-other"); err != nil {
		t.Fatal(err)
	}

	got, err := distinctValues(ctx, client, issueTypeQuery, "filteroptions: issue_type",
		[]dhclickhouse.Binding{{Name: "org_id", Value: org}})
	if err != nil {
		t.Fatalf("issue type list: %v", err)
	}
	if want := []string{"bug", "task"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("issue type list = %v, want %v: each type of the tenant once, whatever number of versions its key holds", got, want)
	}
}
