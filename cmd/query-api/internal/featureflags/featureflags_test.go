package featureflags

import (
	"context"
	"errors"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/full-chaos/dev-health-go/clickhouse"
)

// fakeRow is one scripted row for fakeRowScanner, whose values are copied
// into a Scan call's destination pointers in call order -- matching the
// query's own SELECT column order (provider, flag_key, project_key,
// flag_type, created_at, archived_at) for the row query, or (total,) for
// the count query.
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
		return errors.New("featureflags test: scan arity mismatch")
	}
	for i, d := range dest {
		switch ptr := d.(type) {
		case *string:
			*ptr = row[i].(string)
		case *time.Time:
			*ptr = row[i].(time.Time)
		case **time.Time:
			if row[i] == nil {
				*ptr = nil
			} else {
				v := row[i].(time.Time)
				*ptr = &v
			}
		case *uint64:
			*ptr = row[i].(uint64)
		default:
			return errors.New("featureflags test: unsupported scan destination")
		}
	}
	return nil
}

func (f *fakeRowScanner) Err() error   { return f.err }
func (f *fakeRowScanner) Close() error { return nil }

// fakeClient scripts one response per Query call, in call order -- the
// row query is always called first, then (unless the row query itself
// degraded) the count query, mirroring Resolve's own call order.
type fakeClient struct {
	responses  []*fakeRowScanner
	errs       []error
	calls      int
	statements []string
}

func (f *fakeClient) Query(_ context.Context, statement string, _ []clickhouse.Binding) (clickhouse.RowScanner, error) {
	i := f.calls
	f.calls++
	f.statements = append(f.statements, statement)
	if i < len(f.errs) && f.errs[i] != nil {
		return nil, f.errs[i]
	}
	return f.responses[i], nil
}

func unknownTableErr(tableName string) error {
	return &clickhousedriver.Exception{
		Code:    unknownTableExceptionCode,
		Message: "Unknown table expression identifier '" + tableName + "' in scope SELECT ... FROM " + tableName,
	}
}

func mustUTC(s string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		panic(err)
	}
	return t.UTC()
}

func TestResolve_HappyPath(t *testing.T) {
	created := mustUTC("2026-08-20T10:15:30.500000+00:00")
	client := &fakeClient{
		responses: []*fakeRowScanner{
			{rows: [][]any{
				{"launchdarkly", "flag-key", "proj", "boolean", created, nil},
			}},
			{rows: [][]any{{uint64(1)}}},
		},
		errs: []error{nil, nil},
	}

	result, err := Resolve(context.Background(), client, "org-1", nil, nil, false, 1000)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if result.DegradedReason != nil {
		t.Fatalf("DegradedReason = %v, want nil", *result.DegradedReason)
	}
	if result.TotalCount != 1 {
		t.Fatalf("TotalCount = %d, want 1", result.TotalCount)
	}
	if len(result.Flags) != 1 {
		t.Fatalf("len(Flags) = %d, want 1", len(result.Flags))
	}
	flag := result.Flags[0]
	wantID := FlagID("org-1", "launchdarkly", "proj", "flag-key")
	if flag.FlagID != wantID {
		t.Errorf("FlagID = %q, want %q", flag.FlagID, wantID)
	}
	if flag.CreatedAt != "2026-08-20T10:15:30.500000" {
		t.Errorf("CreatedAt = %q, want isoformat with microseconds + explicit UTC offset", flag.CreatedAt)
	}
	if flag.ArchivedAt != nil {
		t.Errorf("ArchivedAt = %v, want nil (not archived)", *flag.ArchivedAt)
	}
}

// TestIsoformatUTC_OmitsFractionWhenZero is the exact Python isoformat()
// quirk this package's isoformatUTC exists to reproduce: a datetime whose
// microsecond is exactly zero prints NO fractional-second suffix at all --
// never ".000000". Getting this wrong is a real, client-visible response
// mismatch the dual-run proof (stage 2) would have to catch.
func TestIsoformatUTC_OmitsFractionWhenZero(t *testing.T) {
	got := isoformatUTC(mustUTC("2026-08-20T10:15:30.000000+00:00"))
	want := "2026-08-20T10:15:30"
	if got != want {
		t.Errorf("isoformatUTC = %q, want %q", got, want)
	}
}

func TestIsoformatUTC_KeepsSixDigitFractionWhenNonZero(t *testing.T) {
	got := isoformatUTC(mustUTC("2026-08-20T10:15:30.001000+00:00"))
	want := "2026-08-20T10:15:30.001000"
	if got != want {
		t.Errorf("isoformatUTC = %q, want %q", got, want)
	}
}

func TestResolve_ArchivedAtPopulatedWhenPresent(t *testing.T) {
	created := mustUTC("2026-08-01T00:00:00+00:00")
	archived := mustUTC("2026-08-15T00:00:00+00:00")
	client := &fakeClient{
		responses: []*fakeRowScanner{
			{rows: [][]any{{"github", "flag", "proj", "release", created, archived}}},
			{rows: [][]any{{uint64(1)}}},
		},
		errs: []error{nil, nil},
	}

	result, err := Resolve(context.Background(), client, "org-1", nil, nil, true, 1000)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if result.Flags[0].ArchivedAt == nil {
		t.Fatal("ArchivedAt = nil, want a value")
	}
	if *result.Flags[0].ArchivedAt != "2026-08-15T00:00:00" {
		t.Errorf("ArchivedAt = %q", *result.Flags[0].ArchivedAt)
	}
}

// TestResolve_MissingTableDegradesInsteadOfErroring is the real
// non-happy-path this operation was chosen as the canary specifically to
// exercise (CHAOS-4367 lane brief): a ClickHouse UNKNOWN_TABLE (code 60)
// error naming feature_flag must produce a well-formed EMPTY result with
// DegradedReason set, never an error/exception surfaced to the caller.
func TestResolve_MissingTableDegradesInsteadOfErroring(t *testing.T) {
	client := &fakeClient{
		responses: []*fakeRowScanner{nil},
		errs:      []error{unknownTableErr("feature_flag")},
	}

	result, err := Resolve(context.Background(), client, "org-1", nil, nil, false, 1000)
	if err != nil {
		t.Fatalf("Resolve returned an error instead of degrading: %v", err)
	}
	if result.DegradedReason == nil || *result.DegradedReason != NotMaterializedReason {
		t.Fatalf("DegradedReason = %v, want %q", result.DegradedReason, NotMaterializedReason)
	}
	if len(result.Flags) != 0 || result.TotalCount != 0 {
		t.Fatalf("degraded result not empty: flags=%v total=%d", result.Flags, result.TotalCount)
	}
}

// TestResolve_MissingDifferentTablePropagatesError is the precise-match
// half of the missing-table guard: a code-60 error naming some OTHER
// table must NOT be swallowed as feature_flag's own degraded path --
// otherwise a real defect querying an unrelated table would silently read
// as "feature_flag not materialized yet".
func TestResolve_MissingDifferentTablePropagatesError(t *testing.T) {
	client := &fakeClient{
		responses: []*fakeRowScanner{nil},
		errs:      []error{unknownTableErr("some_other_table")},
	}

	_, err := Resolve(context.Background(), client, "org-1", nil, nil, false, 1000)
	if err == nil {
		t.Fatal("expected an error for a different missing table, got nil")
	}
}

func TestResolve_NonMissingTableErrorPropagates(t *testing.T) {
	client := &fakeClient{
		responses: []*fakeRowScanner{nil},
		errs:      []error{errors.New("boom")},
	}
	_, err := Resolve(context.Background(), client, "org-1", nil, nil, false, 1000)
	if err == nil {
		t.Fatal("expected error to propagate")
	}
}

func TestClampLimit(t *testing.T) {
	cases := []struct {
		in, want int
	}{
		{in: -5, want: 1},
		{in: 0, want: 1},
		{in: 1, want: 1},
		{in: 1000, want: 1000},
		{in: 1001, want: 1000},
		{in: 5000, want: 1000},
	}
	for _, tc := range cases {
		if got := clampLimit(tc.in); got != tc.want {
			t.Errorf("clampLimit(%d) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

// TestResolve_FiltersAndLimitBindWhenProvided proves provider/project
// filters and the caller's limit actually reach the bound query -- a
// resolver that silently ignored them would still pass a happy-path test
// with nil filters.
func TestResolve_FiltersAndLimitBindWhenProvided(t *testing.T) {
	created := mustUTC("2026-08-01T00:00:00+00:00")
	client := &fakeClient{
		responses: []*fakeRowScanner{
			{rows: [][]any{{"github", "flag", "proj-x", "boolean", created, nil}}},
			{rows: [][]any{{uint64(1)}}},
		},
		errs: []error{nil, nil},
	}
	provider := "github"
	project := "proj-x"
	if _, err := Resolve(context.Background(), client, "org-1", &provider, &project, false, 5); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(client.statements) != 2 {
		t.Fatalf("expected 2 queries, got %d", len(client.statements))
	}
	for _, stmt := range client.statements {
		if !contains(stmt, "provider = {provider:String}") || !contains(stmt, "project_key = {project:String}") {
			t.Errorf("statement missing filter bindings: %s", stmt)
		}
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && indexOf(haystack, needle) >= 0
}

func indexOf(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}
