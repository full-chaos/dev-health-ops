//go:build integration

package home

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/investmentexplain"
)

// Two sizes of one work unit written in two inserts at ONE computed_at millisecond: the served ISSUE_SIZE share
// weighs the later insert, the row a ReplacingMergeTree merge keeps, before and after the merge. Both insert orders
// run, so a pick that takes the first row seen fails one of them.
func TestInvestmentByThemeIssueSizeTieTakesTheLaterInsert(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	admin, client := newHomeTestClickHouse(ctx, t)
	const org = "org-issue-size-tie"
	const repo = "96240022-9624-4624-8624-962496249624"
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	synced := time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC)
	if err := admin.Exec(ctx, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, 'acme/a', 'github', ?, ?, ?)`, repo, org, synced, synced); err != nil {
		t.Fatal(err)
	}
	end := day.AddDate(0, 0, 3).Add(12 * time.Hour)
	at := day.Add(10 * time.Hour)
	tie := at.Add(time.Hour)
	type sizeRow struct {
		unit     string
		size     float64
		computed time.Time
	}
	insert := func(rows []sizeRow) {
		t.Helper()
		values := make([]string, len(rows))
		for i, r := range rows {
			values[i] = fmt.Sprintf("('%s', '%s', %g, 1, 1, 'run-1', '%s')", org, r.unit, r.size, r.computed.Format("2006-01-02 15:04:05.000"))
		}
		stmt := "INSERT INTO work_unit_issue_size (org_id, work_unit_id, issue_size, sized_issue_count, issue_count, categorization_run_id, computed_at) SETTINGS async_insert=0 VALUES " + strings.Join(values, ", ")
		if err := admin.Exec(ctx, stmt); err != nil {
			t.Fatalf("seed sizes: %v", err)
		}
	}
	if err := admin.Exec(ctx, `SYSTEM STOP MERGES work_unit_issue_size`); err != nil {
		t.Fatal(err)
	}
	var first, second []sizeRow
	for i := 0; i < 16; i++ {
		first = append(first, sizeRow{fmt.Sprintf("a-fill-%02d", i), 1, tie})
	}
	for rep := 0; rep < 4; rep++ {
		xy, yx := fmt.Sprintf("wu-xy-%d", rep), fmt.Sprintf("wu-yx-%d", rep)
		seedThemeUnit(t, ctx, admin, org, xy, repo, at, 10, map[string]float64{"quality.bugfix": 1}, "ok", 0)
		seedThemeUnit(t, ctx, admin, org, yx, repo, at, 10, map[string]float64{"maintenance.debt": 1}, "ok", 0)
		first = append(first, sizeRow{xy, 3, tie}, sizeRow{yx, 8, tie})
		second = append(second, sizeRow{xy, 8, tie}, sizeRow{yx, 3, tie}) // later insert wins: xy 8, yx 3
	}
	seedThemeUnit(t, ctx, admin, org, "wu-ver", repo, at, 10, map[string]float64{"feature_delivery.roadmap": 1}, "ok", 0)
	first = append(first, sizeRow{"wu-ver", 5, tie.Add(time.Hour)})
	second = append(second, sizeRow{"wu-ver", 13, tie.Add(-48 * time.Hour)}) // later insert, older version: 5 stays
	insert(first)
	insert(second)
	var parts uint64
	if err := admin.QueryRow(ctx, `SELECT uniqExact(_part) FROM work_unit_issue_size WHERE org_id = ?`, org).Scan(&parts); err != nil || parts != 2 {
		t.Fatalf("fixture: %d parts (err %v), want 2", parts, err)
	}

	f := Filters{Time: TimeFilter{RangeDays: 7, CompareDays: 7, EndDate: ptrTime(day.AddDate(0, 0, 3))}}
	startDay, endDay := day.AddDate(0, 0, -4), day.AddDate(0, 0, 4)
	want := map[string]float64{"quality": 32, "maintenance": 12, "feature_delivery": 5}
	check := func(label string) {
		t.Helper()
		rows, err := fetchThemeAllocationIn(ctx, client, f, org, startDay, endDay, end, investmentexplain.MeasureIssueSize)
		if err != nil {
			t.Fatalf("%s: fetchThemeAllocationIn: %v", label, err)
		}
		if len(rows) != len(want) {
			t.Fatalf("%s: themes = %+v, want %v", label, rows, want)
		}
		for _, row := range rows {
			if w, ok := want[row.Theme]; !ok || math.Abs(row.Allocation-w) > 1e-3 {
				t.Errorf("%s: %s allocation = %v, want %v", label, row.Theme, row.Allocation, want[row.Theme])
			}
			if math.Abs(row.Denominator-49) > 1e-3 {
				t.Errorf("%s: %s denominator = %v, want 49", label, row.Theme, row.Denominator)
			}
		}
	}
	check("before merge")
	if err := admin.Exec(ctx, `SYSTEM START MERGES work_unit_issue_size`); err != nil {
		t.Fatal(err)
	}
	if err := admin.Exec(ctx, `OPTIMIZE TABLE work_unit_issue_size FINAL`); err != nil {
		t.Fatal(err)
	}
	check("after merge")
}
