package cognitiveload

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graphqldate"
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
		return errors.New("cognitiveload test: scan arity mismatch")
	}
	for i, d := range dest {
		switch ptr := d.(type) {
		case *string:
			*ptr = row[i].(string)
		case *time.Time:
			*ptr = row[i].(time.Time)
		case *uint64:
			*ptr = row[i].(uint64)
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
			return errors.New("cognitiveload test: unsupported scan destination")
		}
	}
	return nil
}

func (f *fakeRowScanner) Err() error   { return f.err }
func (f *fakeRowScanner) Close() error { return nil }

// fakeClient scripts one response per Query call, in call order --
// Resolve issues 1 or 2 sequential queries depending on which path fires.
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

func mustDate(t *testing.T, s string) graphqldate.Date {
	t.Helper()
	d, err := graphqldate.Parse(s)
	if err != nil {
		t.Fatalf("graphqldate.Parse(%q): %v", s, err)
	}
	return d
}

func day(s string) time.Time {
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		panic(err)
	}
	return t
}

func strPtr(s string) *string { return &s }

// ---------------------------------------------------------------------------
// Path 1: single-team (teamId set, repoId unset) -- team_cognitive_load_daily
// ---------------------------------------------------------------------------

func TestResolve_SingleTeamPath_ReadsTeamCognitiveLoadDailyDirectly(t *testing.T) {
	client := &fakeClient{
		responses: []*fakeRowScanner{
			{rows: [][]any{
				{day("2026-08-20"), 4.0, 2.0, 1.0, 0.25, nil},
			}},
		},
		errs: []error{nil},
	}

	result, err := Resolve(context.Background(), client, "org-1", mustDate(t, "2026-08-01"), mustDate(t, "2026-08-31"), strPtr("team-a"), nil)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if client.calls != 1 {
		t.Fatalf("calls = %d, want 1 (single dedup read, no merge)", client.calls)
	}
	if !strings.Contains(client.statements[0], "team_cognitive_load_daily") {
		t.Errorf("expected query against team_cognitive_load_daily, got: %s", client.statements[0])
	}
	if result.TotalDays != 1 || len(result.Signals) != 1 {
		t.Fatalf("expected 1 signal, got %+v", result)
	}
	sig := result.Signals[0]
	if sig.PrInterruptionLoad != 4.0 || sig.ContextSpreadCount != 2.0 || sig.ReviewRequestLoad != 1.0 {
		t.Errorf("load fields = %+v", sig)
	}
	if sig.AfterHoursCommitRatio == nil || *sig.AfterHoursCommitRatio != 0.25 {
		t.Errorf("AfterHoursCommitRatio = %v, want 0.25", sig.AfterHoursCommitRatio)
	}
	if sig.WeekendCommitRatio != nil {
		t.Errorf("WeekendCommitRatio = %v, want nil (genuinely unmeasured)", *sig.WeekendCommitRatio)
	}
	if result.TeamID == nil || *result.TeamID != "team-a" {
		t.Errorf("TeamID = %v, want team-a", result.TeamID)
	}
}

// TestResolve_SingleTeamPath_NullRatiosStayNull proves a row whose
// ratio(s) are genuinely NULL (unmeasured) surface as nil, never a
// silently-defaulted 0.0 -- the exact bug the tuple-argMax bundling in
// fetchTeamCognitiveLoad's doc comment exists to prevent.
func TestResolve_SingleTeamPath_NullRatiosStayNull(t *testing.T) {
	client := &fakeClient{
		responses: []*fakeRowScanner{
			{rows: [][]any{
				{day("2026-08-20"), 0.0, 0.0, 0.0, nil, nil},
			}},
		},
		errs: []error{nil},
	}
	result, err := Resolve(context.Background(), client, "org-1", mustDate(t, "2026-08-01"), mustDate(t, "2026-08-31"), strPtr("team-a"), nil)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	sig := result.Signals[0]
	if sig.AfterHoursCommitRatio != nil || sig.WeekendCommitRatio != nil {
		t.Errorf("expected both ratios nil, got %+v", sig)
	}
}

func TestResolve_SingleTeamPath_ErrorPropagates(t *testing.T) {
	client := &fakeClient{
		responses: []*fakeRowScanner{nil},
		errs:      []error{errors.New("UNKNOWN_TABLE: team_cognitive_load_daily")},
	}
	_, err := Resolve(context.Background(), client, "org-1", mustDate(t, "2026-08-01"), mustDate(t, "2026-08-31"), strPtr("team-a"), nil)
	if err == nil {
		t.Fatal("expected error to propagate, got nil")
	}
}

// ---------------------------------------------------------------------------
// Path 2: org-wide (teamId unset) OR team+repo combined (both set)
// ---------------------------------------------------------------------------

func TestResolve_OrgWide_MergesOnUnionOfDays(t *testing.T) {
	client := &fakeClient{
		responses: []*fakeRowScanner{
			// user metrics: day 1 only
			{rows: [][]any{{day("2026-08-20"), uint64(4), uint64(2), uint64(1)}}},
			// team metrics: day 2 only (a weekend with commit-timing data but no per-developer load)
			{rows: [][]any{{day("2026-08-21"), 0.5, 0.0}}},
		},
		errs: []error{nil, nil},
	}
	result, err := Resolve(context.Background(), client, "org-1", mustDate(t, "2026-08-01"), mustDate(t, "2026-08-31"), nil, nil)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if client.calls != 2 {
		t.Fatalf("calls = %d, want 2", client.calls)
	}
	if len(result.Signals) != 2 {
		t.Fatalf("expected 2 signals (union of days), got %d: %+v", len(result.Signals), result.Signals)
	}
	first, second := result.Signals[0], result.Signals[1]
	if first.Day.String() != "2026-08-20" || second.Day.String() != "2026-08-21" {
		t.Fatalf("expected sorted days 08-20 then 08-21, got %s then %s", first.Day.String(), second.Day.String())
	}
	if first.PrInterruptionLoad != 4.0 {
		t.Errorf("day1 PrInterruptionLoad = %v, want 4.0", first.PrInterruptionLoad)
	}
	if first.AfterHoursCommitRatio != nil {
		t.Errorf("day1 AfterHoursCommitRatio = %v, want nil (no team row that day)", *first.AfterHoursCommitRatio)
	}
	if second.PrInterruptionLoad != 0.0 {
		t.Errorf("day2 PrInterruptionLoad = %v, want 0.0 (no user row that day)", second.PrInterruptionLoad)
	}
	if second.AfterHoursCommitRatio == nil || *second.AfterHoursCommitRatio != 0.5 {
		t.Errorf("day2 AfterHoursCommitRatio = %v, want 0.5", second.AfterHoursCommitRatio)
	}
}

func TestResolve_OrgWide_RepoIdOnlyFiltersUserMetrics(t *testing.T) {
	client := &fakeClient{
		responses: []*fakeRowScanner{
			{rows: nil},
			{rows: nil},
		},
		errs: []error{nil, nil},
	}
	_, err := Resolve(context.Background(), client, "org-1", mustDate(t, "2026-08-01"), mustDate(t, "2026-08-31"), nil, strPtr("org/repo-a"))
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if !strings.Contains(client.statements[0], "repo_id IN (") {
		t.Errorf("expected user metrics query to filter by repo_id, got: %s", client.statements[0])
	}
	// The team-metrics query references its own repo_id COLUMN internally
	// (the CHAOS-4329 legacy-bucket exclusion, unconditional) but must
	// never bind the caller-supplied repoId as a FILTER parameter --
	// fetchTeamMetrics takes no repo_id argument at all, matching Python
	// exactly (its own docstring: "_fetch_team_metrics does not (yet)
	// accept a repo_id filter").
	if strings.Contains(client.statements[1], "{repo_id:String}") {
		t.Errorf("team metrics query must not filter by the caller-supplied repo_id, got: %s", client.statements[1])
	}
	for _, b := range client.bindings[1] {
		if b.Name == "repo_id" {
			t.Errorf("team metrics query unexpectedly bound repo_id: %v", b.Value)
		}
	}
}

// TestResolve_TeamAndRepoCombined_UsesTheSameMergePath proves the
// team+repo-combined case (both set) is NOT a third branch -- it falls
// through to the identical org-wide two-query merge, with BOTH team_id
// AND repo_id bound on the user-metrics query and ONLY team_id bound on
// the team-metrics query. This is the feature-branch tip's ACTUAL
// behavior (verified via `git merge-base --is-ancestor 8519cd2a8
// origin/feature/chaos-4352-go-api`, which returns false): CHAOS-4406's
// ownership-gated resolution exists only on origin/main as of this port,
// not on the feature branch this port targets -- see this package's own
// doc comment for the full finding.
func TestResolve_TeamAndRepoCombined_UsesTheSameMergePath(t *testing.T) {
	client := &fakeClient{
		responses: []*fakeRowScanner{
			{rows: nil},
			{rows: nil},
		},
		errs: []error{nil, nil},
	}
	_, err := Resolve(context.Background(), client, "org-1", mustDate(t, "2026-08-01"), mustDate(t, "2026-08-31"), strPtr("team-a"), strPtr("org/repo-a"))
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if client.calls != 2 {
		t.Fatalf("calls = %d, want 2 (the two-query merge, not a single team_cognitive_load_daily read)", client.calls)
	}
	if strings.Contains(client.statements[0], "team_cognitive_load_daily") {
		t.Error("team+repo combined must not read team_cognitive_load_daily -- it has no repo_id dimension")
	}
	userNames := map[string]bool{}
	for _, b := range client.bindings[0] {
		userNames[b.Name] = true
	}
	if !userNames["team_id"] || !userNames["repo_id"] {
		t.Errorf("expected user-metrics query bound with BOTH team_id and repo_id, got %v", client.bindings[0])
	}
	teamNames := map[string]bool{}
	for _, b := range client.bindings[1] {
		teamNames[b.Name] = true
	}
	if !teamNames["team_id"] {
		t.Errorf("expected team-metrics query bound with team_id, got %v", client.bindings[1])
	}
	if teamNames["repo_id"] {
		t.Errorf("team-metrics query must never bind repo_id, got %v", client.bindings[1])
	}
}

func TestResolve_OrgWide_ErrorPropagatesNoDegradedPath(t *testing.T) {
	client := &fakeClient{
		responses: []*fakeRowScanner{nil},
		errs:      []error{errors.New("UNKNOWN_TABLE: user_metrics_daily")},
	}
	_, err := Resolve(context.Background(), client, "org-1", mustDate(t, "2026-08-01"), mustDate(t, "2026-08-31"), nil, nil)
	if err == nil {
		t.Fatal("expected error to propagate, got nil")
	}
}

func TestResolve_NilClientErrors(t *testing.T) {
	_, err := Resolve(context.Background(), nil, "org-1", mustDate(t, "2026-08-01"), mustDate(t, "2026-08-31"), nil, nil)
	if err == nil {
		t.Fatal("expected an error for a nil client")
	}
}

// ---------------------------------------------------------------------------
// mergeUserAndTeamRows
// ---------------------------------------------------------------------------

func TestMergeUserAndTeamRows_EmptyBothSidesIsNonNilEmptySlice(t *testing.T) {
	signals := mergeUserAndTeamRows(nil, nil)
	if signals == nil {
		t.Fatal("expected a non-nil empty slice (schema declares signals: [CognitiveLoadSignal!]!)")
	}
	if len(signals) != 0 {
		t.Fatalf("expected 0 signals, got %d", len(signals))
	}
}

// ---------------------------------------------------------------------------
// fetchUserMetrics filter-clause construction
// ---------------------------------------------------------------------------

func TestFetchUserMetrics_TeamAndRepoFiltersOmittedWhenUnset(t *testing.T) {
	client := &fakeClient{responses: []*fakeRowScanner{{rows: nil}}, errs: []error{nil}}
	if _, err := fetchUserMetrics(context.Background(), client, "org-1", "2026-08-01", "2026-08-31", nil, nil); err != nil {
		t.Fatalf("fetchUserMetrics: %v", err)
	}
	if strings.Contains(client.statements[0], "team_id = {team_id:String}") {
		t.Error("unexpected team_id filter clause")
	}
	if strings.Contains(client.statements[0], "repo_id IN (") {
		t.Error("unexpected repo_id filter clause")
	}
}

func TestFetchUserMetrics_TeamAndRepoFiltersBoundWhenSet(t *testing.T) {
	client := &fakeClient{responses: []*fakeRowScanner{{rows: nil}}, errs: []error{nil}}
	if _, err := fetchUserMetrics(context.Background(), client, "org-1", "2026-08-01", "2026-08-31", strPtr("team-a"), strPtr("repo-a")); err != nil {
		t.Fatalf("fetchUserMetrics: %v", err)
	}
	if !strings.Contains(client.statements[0], "team_id = {team_id:String}") {
		t.Error("expected team_id filter clause")
	}
	if !strings.Contains(client.statements[0], "repo_id IN (") {
		t.Error("expected repo_id filter clause")
	}
	names := map[string]bool{}
	for _, b := range client.bindings[0] {
		names[b.Name] = true
	}
	if !names["team_id"] || !names["repo_id"] {
		t.Errorf("expected team_id and repo_id bindings, got %v", client.bindings[0])
	}
}

func TestFetchUserMetrics_ScanErrorPropagates(t *testing.T) {
	client := &fakeClient{
		responses: []*fakeRowScanner{{rows: [][]any{{"not-a-time"}}}},
		errs:      []error{nil},
	}
	_, err := fetchUserMetrics(context.Background(), client, "org-1", "2026-08-01", "2026-08-31", nil, nil)
	if err == nil {
		t.Fatal("expected scan error to propagate")
	}
}
