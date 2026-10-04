package coveragebaselines

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

// The scope rows of the fake client: line_mean, line_days, branch_mean, branch_days.

// The rule of the per-repository baseline holds for the scope: fewer than seven
// days with a value is no baseline, and "no baseline" is null, never 0 and never
// the mean itself. Each measure is decided on its own days.
func TestResolveScope_ABaselineNeedsSevenDaysWithAValue(t *testing.T) {
	for name, tc := range map[string]struct {
		row                  []any
		line, branch         any
		lineDays, branchDays int
	}{
		"seven days: a baseline": {
			[]any{80.0, uint64(7), 40.0, uint64(7)}, 80.0, 40.0, 7, 7},
		"six line days, thirty branch days: each measure on its own days": {
			[]any{80.0, uint64(6), 40.0, uint64(30)}, "<nil>", 40.0, 6, 30},
		"thirty line days, six branch days": {
			[]any{80.0, uint64(30), 40.0, uint64(6)}, 80.0, "<nil>", 30, 6},
		"no day with a value": {
			[]any{nil, uint64(0), nil, uint64(0)}, "<nil>", "<nil>", 0, 0},
		"a mean of 0 over thirty days is a baseline of 0, not none": {
			[]any{0.0, uint64(30), 0.0, uint64(30)}, 0.0, 0.0, 30, 30},
	} {
		got, err := ResolveScope(context.Background(), &fakeClient{rows: [][]any{tc.row}}, "org-1", endDay(), Scope{})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got == nil {
			t.Fatalf("%s: nil answer", name)
		}
		if show(got.LineBaselinePct) != tc.line || got.LineDays != tc.lineDays {
			t.Errorf("%s: line = %v over %d days, want %v over %d", name, show(got.LineBaselinePct), got.LineDays, tc.line, tc.lineDays)
		}
		if show(got.BranchBaselinePct) != tc.branch || got.BranchDays != tc.branchDays {
			t.Errorf("%s: branch = %v over %d days, want %v over %d", name, show(got.BranchBaselinePct), got.BranchDays, tc.branch, tc.branchDays)
		}
	}
}

// Nothing measured is an answer (no baseline, 0 days), not an error and not nil.
func TestResolveScope_NoRowIsNoBaselineOverZeroDays(t *testing.T) {
	got, err := ResolveScope(context.Background(), &fakeClient{}, "org-1", endDay(), Scope{})
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.LineBaselinePct != nil || got.BranchBaselinePct != nil || got.LineDays != 0 || got.BranchDays != 0 {
		t.Fatalf("no row: %+v, want no baseline over 0 days", got)
	}
}

func TestResolveScope_AFailedReadIsAnError(t *testing.T) {
	if _, err := ResolveScope(context.Background(), nil, "org-1", endDay(), Scope{}); err == nil {
		t.Error("nil client: want an error")
	}
	if got, err := ResolveScope(context.Background(), &fakeClient{err: errors.New("boom")}, "org-1", endDay(), Scope{}); err == nil {
		t.Errorf("failed read: got %+v, want an error, not an answer with no baseline", got)
	}
	// One scope has one baseline: a second row means the statement is not the
	// one this code was written for.
	two := [][]any{{80.0, uint64(30), 40.0, uint64(30)}, {10.0, uint64(30), 10.0, uint64(30)}}
	if got, err := ResolveScope(context.Background(), &fakeClient{rows: two}, "org-1", endDay(), Scope{}); err == nil {
		t.Errorf("two rows: got %+v, want an error", got)
	}
}

// The scope baseline and the per-repository baseline are one rule: the same
// window, the same scope, and the same coverage read (the newest version of each
// repository and day).
func TestResolveScope_ReadsTheSameRowsAsThePerRepositoryBaseline(t *testing.T) {
	asOf := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	for name, scope := range map[string]Scope{
		"no scope":     {AsOf: asOf},
		"repositories": {RepoIDs: []string{"acme/alpha"}, AsOf: asOf},
		"teams":        {TeamIDs: []string{"team-a"}, AsOf: asOf},
		"both":         {RepoIDs: []string{"acme/alpha"}, TeamIDs: []string{"team-a"}, AsOf: asOf},
	} {
		perRepo, whole := &fakeClient{}, &fakeClient{}
		if _, err := Resolve(context.Background(), perRepo, "org-1", endDay(), scope); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if _, err := ResolveScope(context.Background(), whole, "org-1", endDay(), scope); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !reflect.DeepEqual(perRepo.bindings, whole.bindings) {
			t.Errorf("%s: bindings differ:\nper repository %v\nscope          %v", name, perRepo.bindings, whole.bindings)
		}
		scopeFilter, _ := windowAndScope("org-1", endDay(), scope)
		read := newestPerRepoDay(scopeFilter)
		for label, statement := range map[string]string{"per repository": perRepo.statement, "scope": whole.statement} {
			if strings.Count(statement, read) != 1 {
				t.Errorf("%s: the %s statement does not hold the shared coverage read exactly once", name, label)
			}
		}
	}
	whole := &fakeClient{}
	if _, err := ResolveScope(context.Background(), whole, "org-1", endDay(), Scope{}); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]any{"org_id": "org-1", "start_day": "2026-08-02", "end_day": "2026-09-01"} {
		if v := bound(whole.bindings, name); v != want {
			t.Errorf("%s = %v, want %v", name, v, want)
		}
	}
}

// The value of a day is the mean over the repositories; the baseline is the mean
// over the days. The text pins the three levels and their order; the values are
// pinned on a real ClickHouse (scope_integration_test.go).
func TestScopeStatement_IsTheMeanOfTheDayValuesNotOfTheRepositories(t *testing.T) {
	client := &fakeClient{}
	if _, err := ResolveScope(context.Background(), client, "org-1", endDay(), Scope{}); err != nil {
		t.Fatal(err)
	}
	statement := client.statement
	order := []string{
		"avg(d.day_line_pct) AS line_mean",
		"countIf(d.day_line_pct IS NOT NULL) AS line_days",
		"avg(d.day_branch_pct) AS branch_mean",
		"countIf(d.day_branch_pct IS NOT NULL) AS branch_days",
		"avg(b.line_pct) AS day_line_pct",
		"avg(b.branch_pct) AS day_branch_pct",
		"(argMax(tuple(line_coverage_pct), computed_at)).1 AS line_pct",
		"(argMax(tuple(branch_coverage_pct), computed_at)).1 AS branch_pct",
		"GROUP BY repo_id, day",
		"GROUP BY b.day",
	}
	at := -1
	for _, text := range order {
		next := strings.Index(statement, text)
		if next < 0 {
			t.Fatalf("the statement lacks %q", text)
		}
		if next <= at {
			t.Errorf("%q is not after the text before it: the levels are out of order", text)
		}
		at = next
	}
	// Level 2 groups by the day and by nothing else, and level 3 has no GROUP BY:
	// one row for the scope.
	if !strings.Contains(statement, "GROUP BY b.day\n        ) AS d") || !strings.HasSuffix(statement, ") AS d") {
		t.Error("the day level does not group by the day alone, or the statement does not end at the day level")
	}
	if strings.Count(statement, "GROUP BY") != 2 || strings.Contains(statement, "FROM repos") {
		t.Error("the scope statement has another grouping or reads the catalogue: it must have one row, by day then over the days")
	}
	if got := strings.Count(statement, "org_id = {org_id:String}"); got != 1 {
		t.Errorf("%d org predicates, want 1 (one table)", got)
	}
	if strings.Contains(statement, ";") || client.calls != 1 {
		t.Errorf("want one single SELECT, got %d call(s)", client.calls)
	}
}

func TestResolveScope_ScopeFilters(t *testing.T) {
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
		if _, err := ResolveScope(context.Background(), client, "org-1", endDay(), tc.scope); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		// The scope narrows the coverage rows inside the innermost read, before any mean.
		inner := client.statement[strings.Index(client.statement, "FROM testops_coverage_metrics_daily"):strings.Index(client.statement, "GROUP BY repo_id, day")]
		if got := strings.Contains(inner, "SELECT id FROM repos"); got != tc.repo {
			t.Errorf("%s: repository filter inside the coverage read = %v, want %v", name, got, tc.repo)
		}
		if got := strings.Contains(inner, teamCondition); got != tc.team {
			t.Errorf("%s: team ownership condition inside the coverage read = %v, want %v", name, got, tc.team)
		}
		if got := bound(client.bindings, "repo_ids"); tc.repo != (got != nil) {
			t.Errorf("%s: repo_ids binding = %v", name, got)
		}
		for _, b := range teamBindings {
			if got := bound(client.bindings, b.Name); tc.team && !reflect.DeepEqual(got, b.Value) {
				t.Errorf("%s: team binding %s = %v, want %v", name, b.Name, got, b.Value)
			}
		}
	}
}
