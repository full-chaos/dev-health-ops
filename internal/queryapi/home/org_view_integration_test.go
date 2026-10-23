//go:build integration

package home

import (
	"context"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chquery"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-8863: the argMax shape for work_item_state_durations_daily. One key holds a legacy row that a later run
// retracted with the NEWEST row of the key (a row of zeros), an org row and a scope row. A read that groups by the key
// without row_view would merge the retraction with the org row, argMax would take the zeros, and the key would read as
// dead. The blocked-hours read must take the org row; a legacy-only day reads as before.
func TestBlockedHoursTakeTheOrgViewWithoutADeadKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
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
	client, err := chquery.NewProductionClient(inst.URI)
	if err != nil {
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	const org = "org-8863-state"
	legacyOnlyDay := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	recomputedDay := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	t1 := time.Date(2026, 1, 2, 9, 0, 0, 0, time.UTC)
	t2, t3 := t1.Add(time.Hour), t1.Add(2*time.Hour)

	insert := `INSERT INTO work_item_state_durations_daily
		(day, provider, work_scope_id, team_id, team_name, status, duration_hours, items_touched, computed_at, org_id, row_view)
		VALUES (?, 'jira', 'S', 't1', 'T', 'blocked', ?, ?, ?, ?, ?)`
	for _, row := range []struct {
		day   time.Time
		hours float64
		items uint32
		at    time.Time
		view  string
		what  string
	}{
		{legacyOnlyDay, 3, 1, t1, "", "legacy row of a day that was never recomputed"},
		{recomputedDay, 10, 2, t1, "", "legacy row of the recomputed day"},
		{recomputedDay, 4, 1, t2, "org", "org row of the recomputed day"},
		{recomputedDay, 6, 2, t2, "scope", "scope row of the recomputed day"},
		{recomputedDay, 0, 0, t3, "", "the retraction of the legacy row, the newest row of the day"},
	} {
		if err := conn.Exec(ctx, insert, row.day, row.hours, row.items, row.at, org, row.view); err != nil {
			t.Fatalf("insert %s: %v", row.what, err)
		}
	}

	total, rows, hasData, err := fetchBlockedHours(ctx, client, legacyOnlyDay, recomputedDay.AddDate(0, 0, 1), "", nil, org)
	if err != nil {
		t.Fatalf("fetchBlockedHours: %v", err)
	}
	if !hasData || len(rows) != 2 {
		t.Fatalf("hasData=%v rows=%d, want 2 days with data (a dead key would drop the recomputed day)", hasData, len(rows))
	}
	if total != 7 || rows[0].Value != 3 || rows[1].Value != 4 {
		t.Fatalf("blocked hours total=%v days=%v, want 7 = 3 (legacy-only day) + 4 (the org row)", total, rows)
	}
}
