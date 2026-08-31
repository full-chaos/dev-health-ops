package complexitytimeseries

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
)

// fakeRowScanner is one scripted response, whose values are copied into a
// Scan call's destination pointers in call order.
type fakeRowScanner struct {
	rows   [][]any
	cursor int
	err    error
}

func (f *fakeRowScanner) Next() bool {
	if f.err != nil {
		return false
	}
	return f.cursor < len(f.rows)
}

func (f *fakeRowScanner) Scan(dest ...any) error {
	row := f.rows[f.cursor]
	f.cursor++
	if len(dest) != len(row) {
		return errors.New("complexitytimeseries test: scan arity mismatch")
	}
	for i, d := range dest {
		switch ptr := d.(type) {
		case *string:
			*ptr = row[i].(string)
		case *time.Time:
			*ptr = row[i].(time.Time)
		case *uint64:
			*ptr = row[i].(uint64)
		case *uint32:
			*ptr = row[i].(uint32)
		case *float64:
			*ptr = row[i].(float64)
		case **uint64:
			if row[i] == nil {
				*ptr = nil
			} else {
				v := row[i].(uint64)
				*ptr = &v
			}
		case **uint32:
			if row[i] == nil {
				*ptr = nil
			} else {
				v := row[i].(uint32)
				*ptr = &v
			}
		case **float64:
			if row[i] == nil {
				*ptr = nil
			} else {
				v := row[i].(float64)
				*ptr = &v
			}
		default:
			return errors.New("complexitytimeseries test: unsupported scan destination")
		}
	}
	return nil
}

func (f *fakeRowScanner) Err() error   { return f.err }
func (f *fakeRowScanner) Close() error { return nil }

// fakeClient scripts one response per Query call, in call order.
type fakeClient struct {
	responses  []*fakeRowScanner
	errs       []error
	calls      int
	statements []string
	bindings   [][]clickhouse.Binding
}

func (f *fakeClient) Query(_ context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error) {
	i := f.calls
	f.calls++
	f.statements = append(f.statements, statement)
	f.bindings = append(f.bindings, bindings)
	if i < len(f.errs) && f.errs[i] != nil {
		return nil, f.errs[i]
	}
	return f.responses[i], nil
}

func mustDay(t *testing.T, s string) time.Time {
	t.Helper()
	d, err := time.Parse("2006-01-02T15:04:05Z", s)
	if err != nil {
		t.Fatalf("time.Parse(%q): %v", s, err)
	}
	return d
}

func intPtr(v int) *int { return &v }

// --- bucketCount / pyWeekday --------------------------------------------

func TestBucketCount_DayGranularity(t *testing.T) {
	since := dateOnly(mustDay(t, "2026-08-01T00:00:00Z"))
	until := dateOnly(mustDay(t, "2026-08-10T00:00:00Z"))
	if got := bucketCount(since, until, model.TimeGranularityDay); got != 10 {
		t.Fatalf("bucketCount(day) = %d, want 10", got)
	}
}

func TestBucketCount_UntilBeforeSince(t *testing.T) {
	since := dateOnly(mustDay(t, "2026-08-10T00:00:00Z"))
	until := dateOnly(mustDay(t, "2026-08-01T00:00:00Z"))
	if got := bucketCount(since, until, model.TimeGranularityDay); got != 1 {
		t.Fatalf("bucketCount(until<since) = %d, want 1", got)
	}
}

// TestBucketCount_WeekGranularity pins the Python `date.weekday()`
// (Monday=0) week-bucket boundary math: 2026-08-01 is a Saturday and
// 2026-08-14 is a Friday -- spanning parts of 3 Monday-aligned weeks
// (week of 07-27, week of 08-03, week of 08-10).
func TestBucketCount_WeekGranularity(t *testing.T) {
	since := dateOnly(mustDay(t, "2026-08-01T00:00:00Z")) // Saturday
	until := dateOnly(mustDay(t, "2026-08-14T00:00:00Z")) // Friday
	if got := bucketCount(since, until, model.TimeGranularityWeek); got != 3 {
		t.Fatalf("bucketCount(week) = %d, want 3", got)
	}
}

func TestBucketCount_WeekGranularity_SameWeek(t *testing.T) {
	since := dateOnly(mustDay(t, "2026-08-03T00:00:00Z")) // Monday
	until := dateOnly(mustDay(t, "2026-08-09T00:00:00Z")) // Sunday, same ISO week
	if got := bucketCount(since, until, model.TimeGranularityWeek); got != 1 {
		t.Fatalf("bucketCount(week, same week) = %d, want 1", got)
	}
}

func TestPyWeekday_MatchesPythonConvention(t *testing.T) {
	cases := []struct {
		date string
		want int
	}{
		{"2026-08-03T00:00:00Z", 0}, // Monday
		{"2026-08-04T00:00:00Z", 1}, // Tuesday
		{"2026-08-08T00:00:00Z", 5}, // Saturday
		{"2026-08-09T00:00:00Z", 6}, // Sunday
	}
	for _, c := range cases {
		got := pyWeekday(mustDay(t, c.date))
		if got != c.want {
			t.Errorf("pyWeekday(%s) = %d, want %d", c.date, got, c.want)
		}
	}
}

// --- effectiveLimit -------------------------------------------------------

func TestEffectiveLimit_DefaultsAndClamps(t *testing.T) {
	if got := effectiveLimit(nil, 1); got != DefaultLimit {
		t.Fatalf("effectiveLimit(nil, 1) = %d, want %d", got, DefaultLimit)
	}
	if got := effectiveLimit(intPtr(-5), 1); got != 1 {
		t.Fatalf("effectiveLimit(-5, 1) = %d, want 1", got)
	}
	if got := effectiveLimit(intPtr(5000), 1); got != MaxRows {
		t.Fatalf("effectiveLimit(5000, 1) = %d, want %d", got, MaxRows)
	}
}

func TestEffectiveLimit_ShrinksWithBucketCount(t *testing.T) {
	// MaxTimeseriesPoints=1000 / bucketCount=200 -> perBucket=5, which is
	// less than the requested 500.
	if got := effectiveLimit(intPtr(500), 200); got != 5 {
		t.Fatalf("effectiveLimit(500, 200 buckets) = %d, want 5", got)
	}
}

// --- Resolve: REPO scope ---------------------------------------------------

func TestResolve_RepoScope_HappyPath(t *testing.T) {
	client := &fakeClient{
		responses: []*fakeRowScanner{
			{rows: [][]any{
				{mustDay(t, "2026-08-01T00:00:00Z"), "repo-a", uint64(1000), uint64(50), 12.5, uint64(2), uint64(1)},
			}},
			{rows: [][]any{
				{"repo-a", "org/repo-a"},
			}},
		},
	}

	result, err := Resolve(context.Background(), client, "org-1",
		mustDay(t, "2026-08-01T00:00:00Z"), mustDay(t, "2026-08-31T23:59:59Z"),
		model.TimeGranularityDay, model.ComplexityScopeRepo, nil, nil)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(result.Points) != 1 {
		t.Fatalf("len(Points) = %d, want 1", len(result.Points))
	}
	p := result.Points[0]
	if p.ScopeID != "repo-a" || p.ScopeName != "org/repo-a" {
		t.Fatalf("point = %+v, want scopeId=repo-a scopeName=org/repo-a", p)
	}
	if p.LocTotal == nil || *p.LocTotal != 1000 {
		t.Fatalf("LocTotal = %v, want 1000", p.LocTotal)
	}
	if p.CyclomaticPerKloc == nil || *p.CyclomaticPerKloc != 12.5 {
		t.Fatalf("CyclomaticPerKloc = %v, want 12.5", p.CyclomaticPerKloc)
	}
	if p.CyclomaticAvg != nil {
		t.Fatalf("CyclomaticAvg = %v, want nil (not stored per-repo)", p.CyclomaticAvg)
	}
	if result.TotalScope != 1 {
		t.Fatalf("TotalScope = %d, want 1", result.TotalScope)
	}
	if client.calls != 2 {
		t.Fatalf("calls = %d, want 2 (repo fetch + label lookup)", client.calls)
	}
}

// TestResolve_RepoScope_LabelFallback pins _load_repo_labels's fallback:
// a repo_id with no catalog row (or an empty `repo` column) uses the
// repo_id string itself as scopeName.
func TestResolve_RepoScope_LabelFallback(t *testing.T) {
	client := &fakeClient{
		responses: []*fakeRowScanner{
			{rows: [][]any{
				{mustDay(t, "2026-08-01T00:00:00Z"), "repo-missing", uint64(0), uint64(0), 0.0, uint64(0), uint64(0)},
			}},
			{rows: [][]any{}}, // no catalog row for repo-missing
		},
	}
	result, err := Resolve(context.Background(), client, "org-1",
		mustDay(t, "2026-08-01T00:00:00Z"), mustDay(t, "2026-08-01T23:59:59Z"),
		model.TimeGranularityDay, model.ComplexityScopeRepo, nil, nil)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if result.Points[0].ScopeName != "repo-missing" {
		t.Fatalf("ScopeName = %q, want fallback to repo_id", result.Points[0].ScopeName)
	}
}

func TestResolve_RepoScope_EmptyResultSkipsLabelLookup(t *testing.T) {
	client := &fakeClient{
		responses: []*fakeRowScanner{
			{rows: [][]any{}},
		},
	}
	result, err := Resolve(context.Background(), client, "org-1",
		mustDay(t, "2026-08-01T00:00:00Z"), mustDay(t, "2026-08-01T23:59:59Z"),
		model.TimeGranularityDay, model.ComplexityScopeRepo, nil, nil)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(result.Points) != 0 || result.TotalScope != 0 {
		t.Fatalf("result = %+v, want empty", result)
	}
	// loadRepoLabels short-circuits on an empty repoIDs input without
	// issuing a second query.
	if client.calls != 1 {
		t.Fatalf("calls = %d, want 1 (no label lookup for zero rows)", client.calls)
	}
}

func TestResolve_RepoScope_RepoIDsFilterBindsAndBoundsToLimit(t *testing.T) {
	client := &fakeClient{
		responses: []*fakeRowScanner{
			{rows: [][]any{}},
		},
	}
	limit := 2
	repoIDs := []string{"r1", "r2", "r3"}
	_, err := Resolve(context.Background(), client, "org-1",
		mustDay(t, "2026-08-01T00:00:00Z"), mustDay(t, "2026-08-01T23:59:59Z"),
		model.TimeGranularityDay, model.ComplexityScopeRepo, repoIDs, &limit)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	var found bool
	for _, b := range client.bindings[0] {
		if b.Name == "repo_ids" {
			found = true
			got := b.Value.([]string)
			if len(got) != 2 || got[0] != "r1" || got[1] != "r2" {
				t.Fatalf("repo_ids binding = %v, want [r1 r2] (bounded to limit)", got)
			}
		}
	}
	if !found {
		t.Fatal("expected a repo_ids binding when repoIDs is non-empty")
	}
}

func TestResolve_RepoScope_NoRepoIDsUsesDefaultSubqueryLimit(t *testing.T) {
	client := &fakeClient{
		responses: []*fakeRowScanner{
			{rows: [][]any{}},
		},
	}
	_, err := Resolve(context.Background(), client, "org-1",
		mustDay(t, "2026-08-01T00:00:00Z"), mustDay(t, "2026-08-01T23:59:59Z"),
		model.TimeGranularityDay, model.ComplexityScopeRepo, nil, nil)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	var foundLimit bool
	for _, b := range client.bindings[0] {
		if b.Name == "limit" {
			foundLimit = true
			if b.Value.(uint32) != uint32(DefaultLimit) {
				t.Fatalf("limit binding = %v, want %d", b.Value, DefaultLimit)
			}
		}
		if b.Name == "repo_ids" {
			t.Fatal("did not expect a repo_ids binding when repoIDs is empty")
		}
	}
	if !foundLimit {
		t.Fatal("expected a limit binding for the default top-N subquery")
	}
}

// TestResolve_RepoScope_NullMetricPropagatesAsNil is the regression test
// for codex round-1 finding [P2] on PR #1992: the porting contract is
// parity with Python's _nint/_nfloat None-check (a null ClickHouse
// aggregate must produce a null GraphQL field), not parity with today's
// schema -- even though every column here is non-nullable as of
// migration 007_complexity_investment_issues.sql (see repoTimeseriesRow's
// doc comment), the scan path must still propagate a nil correctly if a
// column value is ever null.
func TestResolve_RepoScope_NullMetricPropagatesAsNil(t *testing.T) {
	client := &fakeClient{
		responses: []*fakeRowScanner{
			{rows: [][]any{
				{mustDay(t, "2026-08-01T00:00:00Z"), "repo-a", nil, nil, nil, nil, nil},
			}},
			{rows: [][]any{}},
		},
	}
	result, err := Resolve(context.Background(), client, "org-1",
		mustDay(t, "2026-08-01T00:00:00Z"), mustDay(t, "2026-08-01T23:59:59Z"),
		model.TimeGranularityDay, model.ComplexityScopeRepo, nil, nil)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	p := result.Points[0]
	if p.LocTotal != nil || p.CyclomaticPerKloc != nil || p.CyclomaticTotal != nil ||
		p.HighComplexityFunctions != nil || p.VeryHighComplexityFunctions != nil {
		t.Fatalf("expected every metric field nil for a null-scanned row, got %+v", p)
	}
}

// --- Resolve: FILE scope ---------------------------------------------------

func TestResolve_FileScope_HappyPath(t *testing.T) {
	client := &fakeClient{
		responses: []*fakeRowScanner{
			{rows: [][]any{
				{mustDay(t, "2026-08-01T00:00:00Z"), "repo-a", "src/main.go", uint32(20), 5.5, uint32(1), uint32(0)},
			}},
		},
	}
	result, err := Resolve(context.Background(), client, "org-1",
		mustDay(t, "2026-08-01T00:00:00Z"), mustDay(t, "2026-08-31T23:59:59Z"),
		model.TimeGranularityDay, model.ComplexityScopeFile, nil, nil)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(result.Points) != 1 {
		t.Fatalf("len(Points) = %d, want 1", len(result.Points))
	}
	p := result.Points[0]
	if p.ScopeID != "repo-a/src/main.go" || p.ScopeName != "src/main.go" {
		t.Fatalf("point = %+v, want scopeId=repo-a/src/main.go scopeName=src/main.go", p)
	}
	if p.LocTotal != nil {
		t.Fatalf("LocTotal = %v, want nil (not stored per-file)", p.LocTotal)
	}
	if p.CyclomaticPerKloc != nil {
		t.Fatalf("CyclomaticPerKloc = %v, want nil (not stored per-file)", p.CyclomaticPerKloc)
	}
	if p.CyclomaticAvg == nil || *p.CyclomaticAvg != 5.5 {
		t.Fatalf("CyclomaticAvg = %v, want 5.5", p.CyclomaticAvg)
	}
	// FILE scope never issues a label-lookup query.
	if client.calls != 1 {
		t.Fatalf("calls = %d, want 1", client.calls)
	}
}

func TestResolve_FileScope_LimitIsInterpolatedIntoQueryText(t *testing.T) {
	client := &fakeClient{
		responses: []*fakeRowScanner{
			{rows: [][]any{}},
		},
	}
	limit := 7
	_, err := Resolve(context.Background(), client, "org-1",
		mustDay(t, "2026-08-01T00:00:00Z"), mustDay(t, "2026-08-01T23:59:59Z"),
		model.TimeGranularityDay, model.ComplexityScopeFile, nil, &limit)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got := client.statements[0]; !contains(got, "LIMIT 7") {
		t.Fatalf("query = %q, want it to contain a literal LIMIT 7", got)
	}
}

// TestResolve_FileScope_NullMetricPropagatesAsNil is FILE scope's
// counterpart to TestResolve_RepoScope_NullMetricPropagatesAsNil.
func TestResolve_FileScope_NullMetricPropagatesAsNil(t *testing.T) {
	client := &fakeClient{
		responses: []*fakeRowScanner{
			{rows: [][]any{
				{mustDay(t, "2026-08-01T00:00:00Z"), "repo-a", "src/main.go", nil, nil, nil, nil},
			}},
		},
	}
	result, err := Resolve(context.Background(), client, "org-1",
		mustDay(t, "2026-08-01T00:00:00Z"), mustDay(t, "2026-08-01T23:59:59Z"),
		model.TimeGranularityDay, model.ComplexityScopeFile, nil, nil)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	p := result.Points[0]
	if p.CyclomaticTotal != nil || p.CyclomaticAvg != nil ||
		p.HighComplexityFunctions != nil || p.VeryHighComplexityFunctions != nil {
		t.Fatalf("expected every metric field nil for a null-scanned row, got %+v", p)
	}
}

// --- Errors -----------------------------------------------------------------

func TestResolve_NilClientErrors(t *testing.T) {
	_, err := Resolve(context.Background(), nil, "org-1",
		mustDay(t, "2026-08-01T00:00:00Z"), mustDay(t, "2026-08-01T23:59:59Z"),
		model.TimeGranularityDay, model.ComplexityScopeRepo, nil, nil)
	if err == nil {
		t.Fatal("expected an error for a nil client")
	}
}

func TestResolve_ErrorPropagatesNoDegradedPath(t *testing.T) {
	client := &fakeClient{
		errs: []error{errors.New("boom")},
	}
	_, err := Resolve(context.Background(), client, "org-1",
		mustDay(t, "2026-08-01T00:00:00Z"), mustDay(t, "2026-08-01T23:59:59Z"),
		model.TimeGranularityDay, model.ComplexityScopeRepo, nil, nil)
	if err == nil {
		t.Fatal("expected the ClickHouse error to propagate (no degraded path for this operation)")
	}
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
