package hotspots

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"
)

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
		return errors.New("hotspots test: scan arity mismatch")
	}
	for i, d := range dest {
		switch ptr := d.(type) {
		case *string:
			*ptr = row[i].(string)
		case *uint64:
			*ptr = row[i].(uint64)
		case *uint32:
			*ptr = row[i].(uint32)
		case *float64:
			*ptr = row[i].(float64)
		case **float64:
			if row[i] == nil {
				*ptr = nil
			} else {
				v := row[i].(float64)
				*ptr = &v
			}
		default:
			return errors.New("hotspots test: unsupported scan destination")
		}
	}
	return nil
}

func (f *fakeRowScanner) Err() error   { return f.err }
func (f *fakeRowScanner) Close() error { return nil }

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

func mustTime(t *testing.T, s string) time.Time {
	t.Helper()
	ts, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t.Fatalf("time.Parse(%q): %v", s, err)
	}
	return ts
}

func intPtr(v int) *int { return &v }

// --- clampLimit / effectiveLimit -------------------------------------------

func TestEffectiveLimit_DefaultsAndClamps(t *testing.T) {
	if got := effectiveLimit(nil); got != DefaultLimit {
		t.Fatalf("effectiveLimit(nil) = %d, want %d", got, DefaultLimit)
	}
	if got := effectiveLimit(intPtr(-5)); got != 1 {
		t.Fatalf("effectiveLimit(-5) = %d, want 1", got)
	}
	if got := effectiveLimit(intPtr(5000)); got != MaxHotspotsRows {
		t.Fatalf("effectiveLimit(5000) = %d, want %d", got, MaxHotspotsRows)
	}
	if got := effectiveLimit(intPtr(10)); got != 10 {
		t.Fatalf("effectiveLimit(10) = %d, want 10", got)
	}
}

// --- dateFromInput: NO UTC normalization, unlike complexitytimeseries -----

func TestDateFromInput_UsesInputOffsetDirectlyNoUTCNormalization(t *testing.T) {
	// 23:30 in UTC+05:00 is 2026-08-10 local, but 2026-08-09T18:30Z in
	// UTC -- dateFromInput must return the LOCAL (as-parsed) date,
	// matching Python's un-normalized `input.since_utc.date()`.
	loc := time.FixedZone("+0500", 5*60*60)
	ts := time.Date(2026, 8, 10, 23, 30, 0, 0, loc)
	if got := dateFromInput(ts); got != "2026-08-10" {
		t.Fatalf("dateFromInput = %q, want 2026-08-10 (no UTC normalization)", got)
	}
}

func TestDateFromInput_UTCInput(t *testing.T) {
	ts := mustTime(t, "2026-08-10T00:00:00Z")
	if got := dateFromInput(ts); got != "2026-08-10" {
		t.Fatalf("dateFromInput = %q, want 2026-08-10", got)
	}
}

// --- evidenceURL / quotePython ----------------------------------------------

func TestQuotePython_MatchesPythonQuoteDefaultSafeSet(t *testing.T) {
	cases := map[string]string{
		"src/main.go":          "src/main.go",
		"src/my file (v2).go":  "src/my%20file%20%28v2%29.go",
		"a+b":                  "a%2Bb",
		"a b":                  "a%20b",
		"café/x.go":            "caf%C3%A9/x.go",
		"":                     "",
		"already_safe-name.go": "already_safe-name.go",
	}
	for input, want := range cases {
		if got := quotePython(input); got != want {
			t.Errorf("quotePython(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestEvidenceURL_BuildsDeeplink(t *testing.T) {
	if got := evidenceURL("src/main.go"); got != "/code?file=src/main.go" {
		t.Fatalf("evidenceURL = %q", got)
	}
}

// --- Resolve: happy path -----------------------------------------------------

func TestResolve_HappyPath(t *testing.T) {
	client := &fakeClient{
		responses: []*fakeRowScanner{
			{rows: [][]any{
				{"repo-a", "src/main.go", uint64(500), uint32(20), uint32(30), 4.5, 0.75, 92.3},
			}},
			{rows: [][]any{
				{"repo-a", "acme/backend"},
			}},
		},
	}
	result, err := Resolve(context.Background(), client, "org-1",
		mustTime(t, "2026-08-01T00:00:00Z"), mustTime(t, "2026-08-31T23:59:59Z"), nil, nil)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("len(Rows) = %d, want 1", len(result.Rows))
	}
	row := result.Rows[0]
	if row.FilePath != "src/main.go" || row.RepoID != "repo-a" || row.RepoName != "acme/backend" {
		t.Fatalf("row = %+v", row)
	}
	if row.ChurnLoc30d != 500 || row.ChurnCommits30d != 20 || row.CyclomaticTotal != 30 {
		t.Fatalf("row = %+v", row)
	}
	if row.CyclomaticAvg != 4.5 || row.RiskScore != 92.3 {
		t.Fatalf("row = %+v", row)
	}
	if row.BlameConcentration == nil || *row.BlameConcentration != 0.75 {
		t.Fatalf("BlameConcentration = %v, want 0.75", row.BlameConcentration)
	}
	if row.EvidenceURL == nil || *row.EvidenceURL != "/code?file=src/main.go" {
		t.Fatalf("EvidenceURL = %v", row.EvidenceURL)
	}
	if client.calls != 2 {
		t.Fatalf("calls = %d, want 2 (hotspot fetch + label lookup)", client.calls)
	}
}

func TestResolve_NullBlameConcentrationPropagatesAsNil(t *testing.T) {
	client := &fakeClient{
		responses: []*fakeRowScanner{
			{rows: [][]any{
				{"repo-a", "src/main.go", uint64(0), uint32(0), uint32(0), 0.0, nil, 0.0},
			}},
			{rows: [][]any{}},
		},
	}
	result, err := Resolve(context.Background(), client, "org-1",
		mustTime(t, "2026-08-01T00:00:00Z"), mustTime(t, "2026-08-01T23:59:59Z"), nil, nil)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if result.Rows[0].BlameConcentration != nil {
		t.Fatalf("BlameConcentration = %v, want nil", result.Rows[0].BlameConcentration)
	}
	// Label fallback: no catalog row for repo-a -> RepoName falls back to repo_id.
	if result.Rows[0].RepoName != "repo-a" {
		t.Fatalf("RepoName = %q, want fallback to repo_id", result.Rows[0].RepoName)
	}
}

func TestResolve_EmptyPathSkipsEvidenceURL(t *testing.T) {
	client := &fakeClient{
		responses: []*fakeRowScanner{
			{rows: [][]any{
				{"repo-a", "", uint64(0), uint32(0), uint32(0), 0.0, nil, 0.0},
			}},
			{rows: [][]any{}},
		},
	}
	result, err := Resolve(context.Background(), client, "org-1",
		mustTime(t, "2026-08-01T00:00:00Z"), mustTime(t, "2026-08-01T23:59:59Z"), nil, nil)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if result.Rows[0].EvidenceURL != nil {
		t.Fatalf("EvidenceURL = %v, want nil for empty file_path", result.Rows[0].EvidenceURL)
	}
}

func TestResolve_EmptyResultSkipsLabelLookup(t *testing.T) {
	client := &fakeClient{
		responses: []*fakeRowScanner{
			{rows: [][]any{}},
		},
	}
	result, err := Resolve(context.Background(), client, "org-1",
		mustTime(t, "2026-08-01T00:00:00Z"), mustTime(t, "2026-08-01T23:59:59Z"), nil, nil)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(result.Rows) != 0 {
		t.Fatalf("Rows = %+v, want empty", result.Rows)
	}
	if client.calls != 1 {
		t.Fatalf("calls = %d, want 1 (no label lookup for zero rows)", client.calls)
	}
}

func TestResolve_RepoIDsFilterBindsUnboundedByRowLimit(t *testing.T) {
	// repoIDs truncation uses MaxRepoIDsBound (1000), NOT the row limit
	// (5 here) -- pinning the divergence documented in the package doc
	// comment.
	client := &fakeClient{
		responses: []*fakeRowScanner{
			{rows: [][]any{}},
		},
	}
	limit := 5
	repoIDs := []string{"r1", "r2", "r3", "r4", "r5", "r6"}
	_, err := Resolve(context.Background(), client, "org-1",
		mustTime(t, "2026-08-01T00:00:00Z"), mustTime(t, "2026-08-01T23:59:59Z"), repoIDs, &limit)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	var found bool
	for _, b := range client.bindings[0] {
		if b.Name == "repo_ids" {
			found = true
			got := b.Value.([]string)
			if len(got) != 6 {
				t.Fatalf("repo_ids binding truncated to %d, want all 6 (bound is MaxRepoIDsBound=1000, not the row limit)", len(got))
			}
		}
	}
	if !found {
		t.Fatal("expected a repo_ids binding when repoIDs is non-empty")
	}
	if got := client.statements[0]; !contains(got, "LIMIT 5") {
		t.Fatalf("query = %q, want it to contain the literal row LIMIT 5", got)
	}
}

func TestResolve_NilClientErrors(t *testing.T) {
	_, err := Resolve(context.Background(), nil, "org-1",
		mustTime(t, "2026-08-01T00:00:00Z"), mustTime(t, "2026-08-01T23:59:59Z"), nil, nil)
	if err == nil {
		t.Fatal("expected an error for a nil client")
	}
}

func TestResolve_ErrorPropagatesNoDegradedPath(t *testing.T) {
	client := &fakeClient{
		errs: []error{errors.New("boom")},
	}
	_, err := Resolve(context.Background(), client, "org-1",
		mustTime(t, "2026-08-01T00:00:00Z"), mustTime(t, "2026-08-01T23:59:59Z"), nil, nil)
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
