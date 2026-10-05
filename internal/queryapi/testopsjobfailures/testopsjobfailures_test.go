package testopsjobfailures

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
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
			if row[i] == nil {
				*ptr = nil
			} else {
				v := row[i].(string)
				*ptr = &v
			}
		default:
			return errors.New("unsupported scan destination")
		}
	}
	return nil
}

// fakeClient answers the list statement and the count statement apart.
type fakeClient struct {
	groups             [][]any // workflow_name, job_name, provider, runs, failed_runs
	total              uint64
	listErr, countErr  error
	listStatement      string
	countStatementText string
	listBindings       []clickhouse.Binding
	countBindings      []clickhouse.Binding
	calls              int
}

func (f *fakeClient) Query(_ context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error) {
	f.calls++
	if strings.HasPrefix(statement, "SELECT count() FROM") {
		f.countStatementText, f.countBindings = statement, bindings
		if f.countErr != nil {
			return nil, f.countErr
		}
		return &fakeRows{rows: [][]any{{f.total}}}, nil
	}
	f.listStatement, f.listBindings = statement, bindings
	if f.listErr != nil {
		return nil, f.listErr
	}
	return &fakeRows{rows: f.groups}, nil
}

func bound(bindings []clickhouse.Binding, name string) any {
	for _, b := range bindings {
		if b.Name == name {
			return b.Value
		}
	}
	return nil
}

func date(t *testing.T, s string) graphqldate.Date {
	t.Helper()
	parsed, err := time.Parse("2006-01-02", s)
	if err != nil {
		t.Fatal(err)
	}
	return graphqldate.New(parsed)
}

func twoGroups() [][]any {
	return [][]any{
		{"CI", "build", "github", uint64(10), uint64(4)},
		{nil, "e2e", nil, uint64(3), uint64(3)},
	}
}

func TestResolve_ServesGroupsWithTheirRateTotalAndTruncation(t *testing.T) {
	for name, tc := range map[string]struct {
		total     uint64
		wantTotal int
		truncated bool
	}{
		"more groups than served":            {5, 5, true},
		"all groups served":                  {2, 2, false},
		"a count below the list is the list": {1, 2, false},
	} {
		client := &fakeClient{groups: twoGroups(), total: tc.total}
		got, err := Resolve(context.Background(), client, "org-1", date(t, "2026-08-01"), date(t, "2026-08-30"), Scope{}, 20)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got.TotalCount != tc.wantTotal || got.Truncated != tc.truncated {
			t.Errorf("%s: totalCount %d truncated %v, want %d %v", name, got.TotalCount, got.Truncated, tc.wantTotal, tc.truncated)
		}
		if len(got.Groups) != 2 {
			t.Fatalf("%s: %d groups, want 2", name, len(got.Groups))
		}
		first, second := got.Groups[0], got.Groups[1]
		if first.JobName != "build" || first.WorkflowName == nil || *first.WorkflowName != "CI" || first.Provider == nil || *first.Provider != "github" ||
			first.Runs != 10 || first.FailedRuns != 4 || first.FailureRate == nil || *first.FailureRate != 0.4 {
			t.Errorf("%s: first group %+v", name, first)
		}
		if second.JobName != "e2e" || second.WorkflowName != nil || second.Provider != nil || second.Runs != 3 || second.FailedRuns != 3 ||
			second.FailureRate == nil || *second.FailureRate != 1 {
			t.Errorf("%s: second group %+v (a job run with no pipeline row has no workflow and no provider)", name, second)
		}
	}
}

func TestFailureRate_IsAShareOfTheRuns(t *testing.T) {
	if got := failureRate(3, 4); got == nil || *got != 0.75 {
		t.Errorf("3 of 4 = %v, want 0.75 (a share, not 75)", got)
	}
	if got := failureRate(0, 5); got == nil || *got != 0 {
		t.Errorf("0 of 5 = %v, want 0", got)
	}
	if got := failureRate(1, 0); got != nil {
		t.Errorf("no run = %v, want nil", *got)
	}
}

func TestResolve_AnEmptyWindowIsAnEmptyListNotAnError(t *testing.T) {
	client := &fakeClient{}
	got, err := Resolve(context.Background(), client, "org-1", date(t, "2026-08-01"), date(t, "2026-08-02"), Scope{}, 20)
	if err != nil {
		t.Fatal(err)
	}
	if got.Groups == nil || len(got.Groups) != 0 || got.TotalCount != 0 || got.Truncated {
		t.Fatalf("got %+v, want an empty non-nil list, totalCount 0, not truncated", got)
	}
}

// The count of groups is built from the same text as the list (one source), and
// both are bound to the org, the window and the status lists.
func TestResolve_TheListAndTheCountShareOneFilterText(t *testing.T) {
	client := &fakeClient{groups: twoGroups(), total: 2}
	if _, err := Resolve(context.Background(), client, "org-1", date(t, "2026-08-01"), date(t, "2026-08-30"), Scope{}, 20); err != nil {
		t.Fatal(err)
	}
	groups := groupsStatement("")
	if client.listStatement != listStatement(groups) || client.countStatementText != countStatement(groups) {
		t.Fatalf("the two statements are not built from one groups text\nlist:\n%s\ncount:\n%s", client.listStatement, client.countStatementText)
	}
	if strings.Count(client.listStatement, groups) != 1 || strings.Count(client.countStatementText, groups) != 1 {
		t.Fatal("the groups text is not inside both statements")
	}
	for name, bindings := range map[string][]clickhouse.Binding{"list": client.listBindings, "count": client.countBindings} {
		want := map[string]any{
			"org_id": "org-1", "since_date": "2026-08-01", "until_date": "2026-08-30",
			"failure_statuses":  FailureStatuses,
			"terminal_statuses": []string{"success", "succeeded", "passed", "failure", "failed", "error", "errors", "timeout", "timed_out", "cancelled", "canceled", "cancel"},
		}
		for key, value := range want {
			if got := bound(bindings, key); !reflect.DeepEqual(got, value) {
				t.Errorf("%s statement binds %s=%v, want %v", name, key, got, value)
			}
		}
	}
	if bound(client.countBindings, "limit") != nil {
		t.Error("the count statement is bound to a limit")
	}
	for _, text := range []string{
		"FROM ci_job_runs FINAL", "FROM ci_pipeline_runs FINAL", "HAVING failed_runs > 0",
		"WHERE j.status IN {terminal_statuses:Array(String)}", "countIf(j.status IN {failure_statuses:Array(String)})",
		"GROUP BY workflow_name, job_name, provider",
	} {
		if !strings.Contains(groups, text) {
			t.Errorf("the groups text lacks %q", text)
		}
	}
	if got := strings.Count(groups, "org_id = {org_id:String}"); got != 2 {
		t.Errorf("%d org predicates, want one per table (2)", got)
	}
	if !strings.Contains(client.listStatement, "ORDER BY failed_runs DESC, job_name, ifNull(workflow_name, ''), ifNull(provider, '')") {
		t.Errorf("the list is not in the stated order:\n%s", client.listStatement)
	}
	if strings.Contains(client.listStatement, ";") || strings.Contains(client.countStatementText, ";") {
		t.Error("a statement is not a single SELECT")
	}
}

func TestResolve_LimitIsClamped(t *testing.T) {
	for limit, want := range map[int]int{-5: 1, 0: 1, 1: 1, 20: 20, 100: 100, 101: 100, 5000: 100} {
		client := &fakeClient{}
		if _, err := Resolve(context.Background(), client, "org-1", date(t, "2026-08-01"), date(t, "2026-08-02"), Scope{}, limit); err != nil {
			t.Fatal(err)
		}
		if got := bound(client.listBindings, "limit"); got != want {
			t.Errorf("limit %d is bound as %v, want %d", limit, got, want)
		}
	}
}

// A window is refused, never cut: no statement is issued for a refused window.
func TestResolve_WindowRules(t *testing.T) {
	for name, tc := range map[string]struct {
		since, until string
		ok           bool
	}{
		"one day": {"2026-08-01", "2026-08-01", true},
		// The web's "90d" window: today minus 90 days to today, 91 calendar days.
		"ninety days after":     {"2026-05-03", "2026-08-01", true},
		"ninety-one days after": {"2026-05-02", "2026-08-01", false},
		"ends before starts":    {"2026-08-02", "2026-08-01", false},
	} {
		client := &fakeClient{}
		_, err := Resolve(context.Background(), client, "org-1", date(t, tc.since), date(t, tc.until), Scope{}, 20)
		if (err == nil) != tc.ok {
			t.Errorf("%s: err = %v, want ok=%v", name, err, tc.ok)
		}
		if !tc.ok && client.calls != 0 {
			t.Errorf("%s: %d statement(s) issued for a refused window", name, client.calls)
		}
	}
}

func TestResolve_AFailedReadIsAnError(t *testing.T) {
	since, until := date(t, "2026-08-01"), date(t, "2026-08-02")
	if _, err := Resolve(context.Background(), nil, "org-1", since, until, Scope{}, 20); err == nil {
		t.Error("nil client: want an error")
	}
	if _, err := Resolve(context.Background(), &fakeClient{listErr: errors.New("boom")}, "org-1", since, until, Scope{}, 20); err == nil {
		t.Error("failed list read: want an error, not an empty list")
	}
	if _, err := Resolve(context.Background(), &fakeClient{groups: twoGroups(), countErr: errors.New("boom")}, "org-1", since, until, Scope{}, 20); err == nil {
		t.Error("failed count read: want an error, not a guessed total")
	}
}

// Both scope filters are applied to the job rows AND to the pipeline rows, and
// the team scope is the ownership condition of teamscope.
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
		"blank team ids": {Scope{TeamIDs: []string{"", ""}, AsOf: asOf}, false, false},
	} {
		client := &fakeClient{}
		if _, err := Resolve(context.Background(), client, "org-1", date(t, "2026-08-01"), date(t, "2026-08-02"), tc.scope, 20); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		for which, statement := range map[string]string{"list": client.listStatement, "count": client.countStatementText} {
			wantRepo, wantTeam := 0, 0
			if tc.repo {
				wantRepo = 2
			}
			if tc.team {
				wantTeam = 2
			}
			if got := strings.Count(statement, "SELECT id FROM repos"); got != wantRepo {
				t.Errorf("%s, %s statement: %d repository filter(s), want %d (job rows and pipeline rows)", name, which, got, wantRepo)
			}
			if got := strings.Count(statement, teamCondition); got != wantTeam {
				t.Errorf("%s, %s statement: %d team ownership condition(s), want %d", name, which, got, wantTeam)
			}
		}
		if got := bound(client.listBindings, "repo_ids"); tc.repo != (got != nil) {
			t.Errorf("%s: repo_ids binding = %v", name, got)
		}
		for _, b := range teamBindings {
			if got := bound(client.listBindings, b.Name); tc.team && !reflect.DeepEqual(got, b.Value) {
				t.Errorf("%s: team binding %s = %v, want %v", name, b.Name, got, b.Value)
			}
		}
	}
}

// The three SQL status lists are the classes the pipeline rollup gives the same
// spellings: every known provider spelling is in the list of its class and in
// no other, and a spelling of no class is in no list.
func TestStatusListsMatchTheWorkerVocabulary(t *testing.T) {
	lists := map[string][]string{"success": SuccessStatuses, "failure": FailureStatuses, "cancelled": CancelledStatuses}
	classOf := func(spelling string) string {
		found := ""
		for class, list := range lists {
			for _, s := range list {
				if s == spelling {
					if found != "" {
						t.Fatalf("%q is in two lists", spelling)
					}
					found = class
				}
			}
		}
		return found
	}
	universe := []string{
		"success", "succeeded", "passed", "failure", "failed", "error", "errors", "timeout", "timed_out",
		"cancelled", "canceled", "cancel", "skipped", "skip", "queued", "running", "in_progress", "pending",
		"manual", "neutral", "action_required", "stale", "created", "waiting_for_resource", "preparing", "scheduled", "",
	}
	for _, list := range lists {
		universe = append(universe, list...)
	}
	classes := map[string]bool{"success": true, "failure": true, "cancelled": true}
	for _, spelling := range universe {
		worker := testops.NormalizeStatus(spelling)
		if !classes[worker] {
			worker = ""
		}
		if got := classOf(spelling); got != worker {
			t.Errorf("%q: the SQL lists class it as %q, the worker as %q", spelling, got, worker)
		}
	}
	for class, list := range lists {
		if len(list) == 0 {
			t.Errorf("the %s list is empty", class)
		}
	}
}
