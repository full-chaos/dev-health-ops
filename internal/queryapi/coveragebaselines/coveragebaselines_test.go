package coveragebaselines

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

type fakeRows struct {
	rows   [][]any
	cursor int
}

func (f *fakeRows) Next() bool   { return f.cursor < len(f.rows) }
func (f *fakeRows) Err() error   { return nil }
func (f *fakeRows) Close() error { return nil }
func (f *fakeRows) Scan(dest ...any) error {
	row := f.rows[f.cursor]
	f.cursor++
	if len(dest) != len(row) {
		return errors.New("scan arity mismatch")
	}
	for i, d := range dest {
		switch ptr := d.(type) {
		case *string:
			*ptr = row[i].(string)
		case *uint64:
			*ptr = row[i].(uint64)
		case **string:
			*ptr = nil
			if row[i] != nil {
				v := row[i].(string)
				*ptr = &v
			}
		case **float64:
			*ptr = nil
			if row[i] != nil {
				v := row[i].(float64)
				*ptr = &v
			}
		default:
			return errors.New("unsupported scan destination")
		}
	}
	return nil
}

type fakeClient struct {
	rows      [][]any // repo_id, repo_name, line_mean, line_days, branch_mean, branch_days
	err       error
	statement string
	bindings  []clickhouse.Binding
	calls     int
}

func (f *fakeClient) Query(_ context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error) {
	f.calls++
	f.statement, f.bindings = statement, bindings
	if f.err != nil {
		return nil, f.err
	}
	return &fakeRows{rows: f.rows}, nil
}

func bound(bindings []clickhouse.Binding, name string) any {
	for _, b := range bindings {
		if b.Name == name {
			return b.Value
		}
	}
	return nil
}

func endDay() graphqldate.Date {
	return graphqldate.New(time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC))
}

func show(p *float64) any {
	if p == nil {
		return "<nil>"
	}
	return *p
}

// The rule the ticket exists for: a mean of fewer than seven days is not a
// baseline, and "no baseline" is null, never 0 and never the mean itself.
func TestResolve_ABaselineNeedsSevenDaysWithAValue(t *testing.T) {
	client := &fakeClient{rows: [][]any{
		{"repo-30", "acme/thirty", 81.5, uint64(30), 70.25, uint64(30)},
		{"repo-7", "acme/seven", 60.0, uint64(7), 0.0, uint64(7)}, // a stored mean of 0 is a baseline
		{"repo-6", "acme/six", 55.0, uint64(6), 44.0, uint64(6)},
		{"repo-line-only", "acme/line", 90.0, uint64(20), nil, uint64(0)},
		{"repo-mixed", nil, 75.0, uint64(8), 50.0, uint64(3)},
		{"repo-empty-name", "", 70.0, uint64(10), nil, uint64(0)}, // an empty name is no name
	}}
	got, err := Resolve(context.Background(), client, "org-1", endDay(), Scope{})
	if err != nil {
		t.Fatal(err)
	}
	want := []struct {
		id, name         string
		line, branch     any
		lineDays, brDays int
	}{
		{"repo-30", "acme/thirty", 81.5, 70.25, 30, 30},
		{"repo-7", "acme/seven", 60.0, 0.0, 7, 7},
		{"repo-6", "acme/six", "<nil>", "<nil>", 6, 6},
		{"repo-line-only", "acme/line", 90.0, "<nil>", 20, 0},
		{"repo-mixed", "<nil>", 75.0, "<nil>", 8, 3},
		{"repo-empty-name", "<nil>", 70.0, "<nil>", 10, 0},
	}
	if len(got) != len(want) {
		t.Fatalf("%d rows, want %d", len(got), len(want))
	}
	for i, w := range want {
		g := got[i]
		name := "<nil>"
		if g.RepoName != nil {
			name = *g.RepoName
		}
		if g.RepoID != w.id || name != w.name || show(g.LineBaselinePct) != w.line || show(g.BranchBaselinePct) != w.branch ||
			g.LineDays != w.lineDays || g.BranchDays != w.brDays {
			t.Errorf("row %d = %s %s line %v (%d days) branch %v (%d days), want %+v", i, g.RepoID, name,
				show(g.LineBaselinePct), g.LineDays, show(g.BranchBaselinePct), g.BranchDays, w)
		}
	}
}

func TestBaseline_ClauseByClause(t *testing.T) {
	value := 42.0
	for name, tc := range map[string]struct {
		mean *float64
		days int
		want any
	}{
		"seven days":            {&value, MinDays, 42.0},
		"six days":              {&value, MinDays - 1, "<nil>"},
		"no mean, days claimed": {nil, 30, "<nil>"},
		"no mean, no days":      {nil, 0, "<nil>"},
		"thirty days":           {&value, 30, 42.0},
		"a mean with zero days": {&value, 0, "<nil>"},
	} {
		if got := show(baseline(tc.mean, tc.days)); got != tc.want {
			t.Errorf("%s: %v, want %v", name, got, tc.want)
		}
	}
	if MinDays != 7 || WindowDays != 30 {
		t.Fatalf("MinDays %d, WindowDays %d: the served rule is 7 of 30", MinDays, WindowDays)
	}
}

// The window is the 30 days BEFORE the end day: the end day is not in it.
func TestResolve_BindsTheOrgAndTheThirtyDaysBeforeTheEndDay(t *testing.T) {
	client := &fakeClient{}
	got, err := Resolve(context.Background(), client, "org-1", endDay(), Scope{})
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || len(got) != 0 {
		t.Fatalf("no stored row: got %v, want an empty non-nil list", got)
	}
	for name, want := range map[string]any{"org_id": "org-1", "start_day": "2026-08-02", "end_day": "2026-09-01"} {
		if v := bound(client.bindings, name); v != want {
			t.Errorf("%s = %v, want %v", name, v, want)
		}
	}
	for _, text := range []string{
		"AND day >= {start_day:Date}", "AND day < {end_day:Date}",
		"(argMax(tuple(line_coverage_pct), computed_at)).1 AS line_pct",
		"(argMax(tuple(branch_coverage_pct), computed_at)).1 AS branch_pct",
		"GROUP BY repo_id, day", "avg(b.line_pct) AS line_mean", "countIf(b.line_pct IS NOT NULL) AS line_days",
		"avg(b.branch_pct) AS branch_mean", "countIf(b.branch_pct IS NOT NULL) AS branch_days", "ORDER BY repo_id",
	} {
		if !strings.Contains(client.statement, text) {
			t.Errorf("the statement lacks %q", text)
		}
	}
	if got := strings.Count(client.statement, "org_id = {org_id:String}"); got != 2 {
		t.Errorf("%d org predicates, want one per table (2)", got)
	}
	if strings.Contains(client.statement, ";") || client.calls != 1 {
		t.Errorf("want one single SELECT, got %d call(s)", client.calls)
	}
}

func TestResolve_AFailedReadIsAnError(t *testing.T) {
	if _, err := Resolve(context.Background(), nil, "org-1", endDay(), Scope{}); err == nil {
		t.Error("nil client: want an error")
	}
	if _, err := Resolve(context.Background(), &fakeClient{err: errors.New("boom")}, "org-1", endDay(), Scope{}); err == nil {
		t.Error("failed read: want an error, not an empty list")
	}
}

func TestResolve_ScopeFilters(t *testing.T) {
	asOf := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	teamCondition, teamBindings := teamscope.RepoCondition("org-1", "toString(repo_id)", []string{"team-a"}, asOf)
	if teamCondition == "" || len(teamBindings) == 0 {
		t.Fatal("teamscope gave no condition for a team: nothing to measure")
	}
	for name, tc := range map[string]struct {
		scope      Scope
		repo, team bool
	}{
		"no scope":       {Scope{AsOf: asOf}, false, false},
		"repositories":   {Scope{RepoIDs: []string{"acme/alpha"}, AsOf: asOf}, true, false},
		"teams":          {Scope{TeamIDs: []string{"team-a"}, AsOf: asOf}, false, true},
		"both":           {Scope{RepoIDs: []string{"acme/alpha"}, TeamIDs: []string{"team-a"}, AsOf: asOf}, true, true},
		"blank team ids": {Scope{TeamIDs: []string{""}, AsOf: asOf}, false, false},
	} {
		client := &fakeClient{}
		if _, err := Resolve(context.Background(), client, "org-1", endDay(), tc.scope); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got := strings.Contains(client.statement, "SELECT id FROM repos"); got != tc.repo {
			t.Errorf("%s: repository filter present = %v, want %v", name, got, tc.repo)
		}
		if got := strings.Contains(client.statement, teamCondition); got != tc.team {
			t.Errorf("%s: team ownership condition present = %v, want %v", name, got, tc.team)
		}
		if got := bound(client.bindings, "repo_ids"); tc.repo != (got != nil) {
			t.Errorf("%s: repo_ids binding = %v", name, got)
		}
		for _, b := range teamBindings {
			if got := bound(client.bindings, b.Name); tc.team && !reflect.DeepEqual(got, b.Value) {
				t.Errorf("%s: team binding %s = %v, want %v", name, b.Name, got, b.Value)
			}
		}
		// The scope narrows the coverage rows inside the inner query, before the mean.
		inner := client.statement[strings.Index(client.statement, "FROM testops_coverage_metrics_daily"):strings.Index(client.statement, "GROUP BY repo_id, day")]
		if tc.repo && !strings.Contains(inner, "SELECT id FROM repos") {
			t.Errorf("%s: the repository filter is not inside the coverage read", name)
		}
		if tc.team && !strings.Contains(inner, teamCondition) {
			t.Errorf("%s: the team condition is not inside the coverage read", name)
		}
	}
}
