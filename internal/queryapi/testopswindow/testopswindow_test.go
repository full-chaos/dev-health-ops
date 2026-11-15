package testopswindow

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
)

type fakeRows struct {
	row    []any
	served bool
}

func (f *fakeRows) Next() bool {
	if f.served {
		return false
	}
	f.served = true
	return true
}
func (f *fakeRows) Err() error   { return nil }
func (f *fakeRows) Close() error { return nil }
func (f *fakeRows) Scan(dest ...any) error {
	if len(dest) != len(f.row) {
		return errors.New("scan arity mismatch")
	}
	for i, d := range dest {
		switch ptr := d.(type) {
		case *uint64:
			*ptr = f.row[i].(uint64)
		case **uint64:
			if f.row[i] == nil {
				*ptr = nil
			} else {
				v := f.row[i].(uint64)
				*ptr = &v
			}
		default:
			return errors.New("unsupported scan destination")
		}
	}
	return nil
}

// fakeClient answers the coverage statement and the cases statement apart.
type fakeClient struct {
	coverage, cases               []any
	coverageErr, casesErr         error
	coverageText, casesText       string
	coverageBinding, casesBinding []clickhouse.Binding
}

func (f *fakeClient) Query(_ context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error) {
	if strings.Contains(statement, "FROM ci_pipeline_runs") {
		f.coverageText, f.coverageBinding = statement, bindings
		if f.coverageErr != nil {
			return nil, f.coverageErr
		}
		return &fakeRows{row: f.coverage}, nil
	}
	f.casesText, f.casesBinding = statement, bindings
	if f.casesErr != nil {
		return nil, f.casesErr
	}
	return &fakeRows{row: f.cases}, nil
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

// cases row: passed, failed, skipped, quarantined, total, flake_cases, distinct_cases, rows_without_counts.
func casesRow(passed, failed, skipped, quarantined, total uint64, flake, distinct any, without uint64) []any {
	return []any{passed, failed, skipped, quarantined, total, flake, distinct, without}
}

func resolve(t *testing.T, client *fakeClient, scope Scope) (*float64, *float64, *float64, *float64, int, int) {
	t.Helper()
	got, err := Resolve(context.Background(), client, "org-1", date(t, "2026-10-31"), date(t, "2026-11-13"), scope)
	if err != nil {
		t.Fatal(err)
	}
	return got.Coverage.SharePct, got.PassRatePct, got.FailureRatePct, got.FlakeRatePct, got.Coverage.CompletedRuns, got.Coverage.RunsWithTests
}

func TestResolve_ServesTheRecordedWindowNumbers(t *testing.T) {
	// Prod record 2026-10-31..11-13: 4,563,278 cases, 4,555,926 passed, 5 failed, 7,347 skipped.
	client := &fakeClient{
		coverage: []any{uint64(71657), uint64(878)},
		cases:    casesRow(4555926, 5, 7347, 0, 4563278, uint64(10), uint64(1000), 0),
	}
	got, err := Resolve(context.Background(), client, "org-1", date(t, "2026-10-31"), date(t, "2026-11-13"), Scope{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Coverage.CompletedRuns != 71657 || got.Coverage.RunsWithTests != 878 {
		t.Errorf("coverage counts %+v", got.Coverage)
	}
	if want := 878.0 / 71657.0 * 100; got.Coverage.SharePct == nil || *got.Coverage.SharePct != want {
		t.Errorf("sharePct %v, want %v", got.Coverage.SharePct, want)
	}
	if want := 4555926.0 / 4555931.0 * 100; got.PassRatePct == nil || *got.PassRatePct != want {
		t.Errorf("passRatePct %v, want %v (skipped is not in the denominator)", got.PassRatePct, want)
	}
	if want := 5.0 / 4555931.0 * 100; got.FailureRatePct == nil || *got.FailureRatePct != want {
		t.Errorf("failureRatePct %v, want %v", got.FailureRatePct, want)
	}
	if got.FlakeRatePct == nil || *got.FlakeRatePct != 1.0 {
		t.Errorf("flakeRatePct %v, want 1", got.FlakeRatePct)
	}
	c := got.Cases
	if c.Passed != 4555926 || c.Failed != 5 || c.Skipped != 7347 || c.Quarantined != 0 || c.Total != 4563278 {
		t.Errorf("cases %+v", c)
	}
	if got.SinceDate.String() != "2026-10-31" || got.UntilDate.String() != "2026-11-13" {
		t.Errorf("window echo %s..%s", got.SinceDate, got.UntilDate)
	}
}

func TestResolve_AllPassedAndSkippedIsExactly100(t *testing.T) {
	client := &fakeClient{coverage: []any{uint64(10), uint64(10)}, cases: casesRow(10, 0, 5, 0, 15, uint64(0), uint64(10), 0)}
	_, pass, fail, _, _, _ := resolve(t, client, Scope{})
	if pass == nil || *pass != 100 {
		t.Errorf("pass %v, want exactly 100 (skipped cases are not in the denominator)", pass)
	}
	if fail == nil || *fail != 0 {
		t.Errorf("failure %v, want a served 0", fail)
	}
}

func TestResolve_MissingIsNullNeverZero(t *testing.T) {
	for name, tc := range map[string]struct {
		coverage, cases []any
		wantShare       bool
		wantRates       bool
		wantFlake       bool
	}{
		"no completed run and no case row": {[]any{uint64(0), uint64(0)}, casesRow(0, 0, 0, 0, 0, nil, nil, 0), false, false, false},
		"only skipped cases":               {[]any{uint64(4), uint64(1)}, casesRow(0, 0, 9, 0, 9, uint64(0), uint64(0), 0), true, false, false},
		"a day without the flake counts":   {[]any{uint64(4), uint64(1)}, casesRow(8, 2, 0, 0, 10, uint64(1), uint64(5), 1), true, true, false},
		"no distinct case":                 {[]any{uint64(4), uint64(1)}, casesRow(8, 2, 0, 0, 10, uint64(0), uint64(0), 0), true, true, false},
		"counts present":                   {[]any{uint64(4), uint64(1)}, casesRow(8, 2, 0, 0, 10, uint64(1), uint64(5), 0), true, true, true},
	} {
		share, pass, fail, flake, _, _ := resolve(t, &fakeClient{coverage: tc.coverage, cases: tc.cases}, Scope{})
		if (share != nil) != tc.wantShare || (pass != nil) != tc.wantRates || (fail != nil) != tc.wantRates || (flake != nil) != tc.wantFlake {
			t.Errorf("%s: share %v pass %v fail %v flake %v", name, share, pass, fail, flake)
		}
	}
}

func TestResolve_RateIsWeightedNotAMeanOfRates(t *testing.T) {
	// Two repositories: 1,000,000 cases at 99.9 % and 100 cases at 90 %. The weighted rate is 99.90 %, a mean of the two rates is 94.95 %.
	client := &fakeClient{coverage: []any{uint64(2), uint64(2)}, cases: casesRow(999000+90, 1000+10, 0, 0, 1000100, uint64(0), uint64(2), 0)}
	_, pass, _, _, _, _ := resolve(t, client, Scope{})
	if pass == nil || *pass < 99.89 || *pass > 99.91 {
		t.Errorf("pass %v, want about 99.90 (weighted), not 94.95", pass)
	}
}

func TestStatements_PinWhatEachGuardMeans(t *testing.T) {
	client := &fakeClient{coverage: []any{uint64(1), uint64(1)}, cases: casesRow(1, 0, 0, 0, 1, uint64(0), uint64(1), 0)}
	if _, _, _, _, _, _ = resolve(t, client, Scope{}); t.Failed() {
		return
	}
	for _, want := range []string{
		"FROM ci_pipeline_runs FINAL",
		"org_id = {org_id:String}",
		"toDate(started_at) >= {since_date:Date}",
		"toDate(started_at) <= {until_date:Date}",
		"lowerUTF8(trimBoth(ifNull(status, ''))) IN {completed_statuses:Array(String)}",
		"FROM test_suite_results FINAL",
		"total_count > 0",
	} {
		if !strings.Contains(client.coverageText, want) {
			t.Errorf("coverage statement lacks %q", want)
		}
	}
	for _, want := range []string{
		"FROM testops_test_metrics_daily",
		"org_id = {org_id:String}",
		"day >= {since_date:Date}",
		"day <= {until_date:Date}",
		"ORDER BY computed_at DESC",
		"LIMIT 1 BY org_id, repo_id, day",
		"sum(passed_count)", "sum(failed_count)", "sum(skipped_count)", "sum(quarantined_count)", "sum(total_cases)",
		"countIf(flake_cases IS NULL OR distinct_cases IS NULL)",
	} {
		if !strings.Contains(client.casesText, want) {
			t.Errorf("cases statement lacks %q", want)
		}
	}
	if strings.Contains(client.casesText, "pass_rate") || strings.Contains(client.casesText, "AVG(") || strings.Contains(client.casesText, "avg(") {
		t.Error("the cases statement reads a stored rate or a mean: the window rate is summed counts")
	}
	if got := bound(client.coverageBinding, "completed_statuses"); !reflect.DeepEqual(got, CompletedStatuses()) {
		t.Errorf("completed_statuses = %v", got)
	}
	if bound(client.coverageBinding, "since_date") != "2026-10-31" || bound(client.coverageBinding, "until_date") != "2026-11-13" {
		t.Errorf("window bindings %v", client.coverageBinding)
	}
}

// A completed run is a success or a failure class of the pipeline rollup (testops.NormalizeStatus); cancelled, skipped, queued and running are not.
func TestCompletedStatuses_AreTheRollupsSuccessAndFailureClasses(t *testing.T) {
	for _, spelling := range CompletedStatuses() {
		if class := testops.NormalizeStatus(spelling); class != "success" && class != "failure" {
			t.Errorf("%q is completed here but the rollup calls it %q", spelling, class)
		}
	}
	for _, spelling := range []string{"success", "succeeded", "passed", "failure", "failed", "error", "errors", "timeout", "timed_out"} {
		found := false
		for _, s := range CompletedStatuses() {
			found = found || s == spelling
		}
		if !found {
			t.Errorf("%q is a result class spelling and is not completed here", spelling)
		}
	}
	got := append([]string{}, CompletedStatuses()...)
	sort.Strings(got)
	for _, spelling := range []string{"cancelled", "canceled", "cancel", "skipped", "queued", "running", "completed", "in_progress", "pending", ""} {
		if i := sort.SearchStrings(got, spelling); i < len(got) && got[i] == spelling {
			t.Errorf("%q is not a completed run", spelling)
		}
	}
}

func TestResolve_RefusesAWindowItCannotServe(t *testing.T) {
	client := &fakeClient{}
	for name, tc := range map[string]struct{ since, until string }{
		"ends before it starts": {"2026-11-13", "2026-11-12"},
		"more than 90 days":     {"2026-01-01", "2026-04-02"},
	} {
		if _, err := Resolve(context.Background(), client, "org-1", date(t, tc.since), date(t, tc.until), Scope{}); err == nil {
			t.Errorf("%s: served", name)
		}
	}
	if client.coverageText != "" || client.casesText != "" {
		t.Error("a refused window read the database")
	}
	if _, err := Resolve(context.Background(), nil, "org-1", date(t, "2026-11-01"), date(t, "2026-11-02"), Scope{}); err == nil {
		t.Error("a nil client was accepted")
	}
}

func TestResolve_AFailedReadFailsTheField(t *testing.T) {
	boom := errors.New("boom")
	if _, err := Resolve(context.Background(), &fakeClient{coverageErr: boom}, "org-1", date(t, "2026-11-01"), date(t, "2026-11-02"), Scope{}); !errors.Is(err, boom) {
		t.Errorf("coverage read error %v", err)
	}
	if _, err := Resolve(context.Background(), &fakeClient{coverage: []any{uint64(1), uint64(1)}, casesErr: boom}, "org-1", date(t, "2026-11-01"), date(t, "2026-11-02"), Scope{}); !errors.Is(err, boom) {
		t.Errorf("cases read error %v", err)
	}
}

func TestResolve_ScopeReachesBothReads(t *testing.T) {
	client := &fakeClient{coverage: []any{uint64(1), uint64(1)}, cases: casesRow(1, 0, 0, 0, 1, uint64(0), uint64(1), 0)}
	scope := Scope{RepoIDs: []string{"acme/hub"}, TeamIDs: []string{"team-a"}, AsOf: time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC)}
	if _, _, _, _, _, _ = resolve(t, client, scope); t.Failed() {
		return
	}
	for name, text := range map[string]string{"coverage": client.coverageText, "cases": client.casesText} {
		if !strings.Contains(text, "toString(id) IN {repo_ids:Array(String)}") {
			t.Errorf("%s: no repository scope", name)
		}
		if !strings.Contains(text, "team_repo_ownership") {
			t.Errorf("%s: no team ownership scope (membership is never read)", name)
		}
	}
	for name, bindings := range map[string][]clickhouse.Binding{"coverage": client.coverageBinding, "cases": client.casesBinding} {
		if !reflect.DeepEqual(bound(bindings, "repo_ids"), []string{"acme/hub"}) {
			t.Errorf("%s: repo_ids %v", name, bound(bindings, "repo_ids"))
		}
	}
	// An unscoped request carries no scope text.
	plain := &fakeClient{coverage: []any{uint64(1), uint64(1)}, cases: casesRow(1, 0, 0, 0, 1, uint64(0), uint64(1), 0)}
	resolve(t, plain, Scope{})
	if strings.Contains(plain.coverageText, "team_repo_ownership") || strings.Contains(plain.casesText, "repo_ids") {
		t.Error("an unscoped request was narrowed")
	}
}
