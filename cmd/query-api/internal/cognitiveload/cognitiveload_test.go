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
		case *[]string:
			if row[i] == nil {
				*ptr = nil
			} else {
				*ptr = row[i].([]string)
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
// Path 3: org-wide (teamId unset). repoId, when set without teamId,
// narrows only user_metrics_daily -- see below.
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

// ---------------------------------------------------------------------------
// Path 2 (CHAOS-4406/CHAOS-4462): team+repo combined -- ownership-gated
// ---------------------------------------------------------------------------

// TestResolve_TeamAndRepoCombined_OwnershipResolves proves the current
// (post-CHAOS-4462) behavior: both set resolves ownership FIRST via
// team_repo_ownership, then filters both fetches by repo_id ALONE --
// never team_id -- once ownership is confirmed. 4 queries: repo
// candidates, ownership, user metrics, repo-scoped team metrics.
func TestResolve_TeamAndRepoCombined_OwnershipResolves(t *testing.T) {
	client := &fakeClient{
		responses: []*fakeRowScanner{
			{rows: [][]any{{"repo-uuid-1", "org/repo-a"}}},                        // repo candidates
			{rows: [][]any{{"repo-uuid-1", "team-a"}}},                            // ownership: native, owned by team-a
			{rows: [][]any{{day("2026-08-20"), uint64(4), uint64(2), uint64(1)}}}, // user metrics
			{rows: [][]any{{day("2026-08-20"), 0.25, 0.0}}},                       // repo-scoped team metrics
		},
		errs: []error{nil, nil, nil, nil},
	}
	result, err := Resolve(context.Background(), client, "org-1", mustDate(t, "2026-08-01"), mustDate(t, "2026-08-31"), strPtr("team-a"), strPtr("org/repo-a"))
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if client.calls != 4 {
		t.Fatalf("calls = %d, want 4 (candidates, ownership, user metrics, repo-scoped team metrics)", client.calls)
	}
	if !strings.Contains(client.statements[0], "FROM repos FINAL") {
		t.Errorf("expected first query to resolve repo candidates, got: %s", client.statements[0])
	}
	if !strings.Contains(client.statements[1], "team_repo_ownership") {
		t.Errorf("expected second query to check ownership, got: %s", client.statements[1])
	}
	// The user-metrics query must be bound with the RESOLVED repo_id and
	// must NOT bind team_id at all -- ownership already scopes every
	// signal for this repo to the requesting team.
	userNames := map[string]bool{}
	for _, b := range client.bindings[2] {
		userNames[b.Name] = true
		if b.Name == "repo_id" && b.Value != "repo-uuid-1" {
			t.Errorf("expected user-metrics repo_id = repo-uuid-1 (the resolved id, not the input slug), got %v", b.Value)
		}
	}
	if userNames["team_id"] {
		t.Error("user-metrics query must never bind team_id in the ownership-gated path")
	}
	if !userNames["repo_id"] {
		t.Error("expected user-metrics query bound with repo_id")
	}
	if strings.Contains(client.statements[3], "{team_id:String}") {
		t.Errorf("repo-scoped team-metrics query must never filter by team_id, got: %s", client.statements[3])
	}
	if result.TotalDays != 1 || len(result.Signals) != 1 {
		t.Fatalf("expected 1 merged signal, got %+v", result)
	}
	sig := result.Signals[0]
	if sig.PrInterruptionLoad != 4.0 {
		t.Errorf("PrInterruptionLoad = %v, want 4.0", sig.PrInterruptionLoad)
	}
	if sig.AfterHoursCommitRatio == nil || *sig.AfterHoursCommitRatio != 0.25 {
		t.Errorf("AfterHoursCommitRatio = %v, want 0.25", sig.AfterHoursCommitRatio)
	}
}

// TestResolve_TeamAndRepoCombined_RepoDoesNotExist_ReturnsEmptyOneQuery
// proves an unresolvable repoId short-circuits after the FIRST query --
// an explicit empty result, never an error, never a second query.
func TestResolve_TeamAndRepoCombined_RepoDoesNotExist_ReturnsEmptyOneQuery(t *testing.T) {
	client := &fakeClient{
		responses: []*fakeRowScanner{{rows: nil}},
		errs:      []error{nil},
	}
	result, err := Resolve(context.Background(), client, "org-1", mustDate(t, "2026-08-01"), mustDate(t, "2026-08-31"), strPtr("team-a"), strPtr("no-such-repo"))
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if client.calls != 1 {
		t.Fatalf("calls = %d, want 1 (short-circuit on no candidates)", client.calls)
	}
	if result.Signals == nil || len(result.Signals) != 0 || result.TotalDays != 0 {
		t.Fatalf("expected an explicit non-nil empty result, got %+v", result)
	}
}

// TestResolve_TeamAndRepoCombined_OwnedByADifferentTeam_NeverPatternFallback_ReturnsEmpty
// proves a repo that resolves NATIVELY to a DIFFERENT team is claimed and
// NEVER re-checked against repo_patterns (a candidate resolved by native
// ownership is removed from the "unresolved" set regardless of which team
// it resolved to) -- only 2 queries, no pattern-fallback teams read, and
// an explicit empty result rather than the wrong team's data.
func TestResolve_TeamAndRepoCombined_OwnedByADifferentTeam_NeverPatternFallback_ReturnsEmpty(t *testing.T) {
	client := &fakeClient{
		responses: []*fakeRowScanner{
			{rows: [][]any{{"repo-uuid-1", "org/repo-a"}}},
			{rows: [][]any{{"repo-uuid-1", "team-b"}}}, // owned by team-b, not team-a
		},
		errs: []error{nil, nil},
	}
	result, err := Resolve(context.Background(), client, "org-1", mustDate(t, "2026-08-01"), mustDate(t, "2026-08-31"), strPtr("team-a"), strPtr("org/repo-a"))
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if client.calls != 2 {
		t.Fatalf("calls = %d, want 2 (candidates, ownership -- a natively-resolved candidate is claimed, never pattern-fallback-checked)", client.calls)
	}
	if result.Signals == nil || len(result.Signals) != 0 || result.TotalDays != 0 {
		t.Fatalf("expected an explicit non-nil empty result (owned by team-b, not team-a), got %+v", result)
	}
}

// TestResolve_TeamAndRepoCombined_PatternFallbackResolves proves a repo
// with NO native team_repo_ownership row at all still resolves via
// teams.repo_patterns (the path GitLab/Jira/Linear auto-imports rely on
// entirely, since none of them write team_repo_ownership).
func TestResolve_TeamAndRepoCombined_PatternFallbackResolves(t *testing.T) {
	client := &fakeClient{
		responses: []*fakeRowScanner{
			{rows: [][]any{{"repo-uuid-1", "org/repo-a"}}},
			{rows: nil}, // no native ownership row for this repo at all
			{rows: [][]any{{"team-a", []string{"org/repo-a"}}}},
			{rows: nil}, // user metrics
			{rows: nil}, // repo-scoped team metrics
		},
		errs: []error{nil, nil, nil, nil, nil},
	}
	result, err := Resolve(context.Background(), client, "org-1", mustDate(t, "2026-08-01"), mustDate(t, "2026-08-31"), strPtr("team-a"), strPtr("org/repo-a"))
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if client.calls != 5 {
		t.Fatalf("calls = %d, want 5 (candidates, ownership, teams, user metrics, repo-scoped team metrics)", client.calls)
	}
	if result.Signals == nil {
		t.Fatal("expected a non-nil (possibly empty) signals slice")
	}
}

// TestResolveOwnedRepoID_DedupesToFirstRowPerResolvedRepoAndRespectsOrder
// proves the Go port trusts ClickHouse's own ORDER BY entirely: it never
// re-sorts, it only dedups to the FIRST row per resolved_repo_id (the
// scripted response below simulates two rows already tied and ordered for
// the same resolved_repo_id, as the real ORDER BY (is_primary DESC,
// specificity DESC, updated_at DESC, team_id ASC) would produce) and then
// returns the first resolved_repo_id (in ClickHouse's own ascending
// resolved_repo_id order) whose canonical owner matches teamID.
func TestResolveOwnedRepoID_DedupesToFirstRowPerResolvedRepoAndRespectsOrder(t *testing.T) {
	client := &fakeClient{
		responses: []*fakeRowScanner{
			{rows: [][]any{{"r1", "org/repo-a"}, {"r2", "org/repo-a"}}},
			{rows: [][]any{
				{"r1", "team-x"}, // r1's canonical (first-seen) owner: team-x
				{"r1", "team-y"}, // a losing tie-break row for r1 -- must be ignored
				{"r2", "team-a"}, // r2's canonical owner: team-a, the match
			}},
		},
		errs: []error{nil, nil},
	}
	id, found, err := resolveOwnedRepoID(context.Background(), client, "org-1", "team-a", "org/repo-a")
	if err != nil {
		t.Fatalf("resolveOwnedRepoID: %v", err)
	}
	if !found || id != "r2" {
		t.Fatalf("resolveOwnedRepoID = (%q, %v), want (\"r2\", true)", id, found)
	}
}

// ---------------------------------------------------------------------------
// repoPatternResolver
// ---------------------------------------------------------------------------

func TestRepoPatternResolver_ExactMatchWinsOverPrefix(t *testing.T) {
	resolver := newRepoPatternResolver([]patternTeam{
		{id: "team-broad", repoPatterns: []string{"org/*"}},
		{id: "team-exact", repoPatterns: []string{"org/repo-a"}},
	})
	teamID, ok := resolver.resolve("org/repo-a")
	if !ok || teamID != "team-exact" {
		t.Errorf("resolve(org/repo-a) = (%q, %v), want (team-exact, true)", teamID, ok)
	}
}

func TestRepoPatternResolver_LongestPrefixWins(t *testing.T) {
	resolver := newRepoPatternResolver([]patternTeam{
		{id: "team-broad", repoPatterns: []string{"org/*"}},
		{id: "team-narrow", repoPatterns: []string{"org/repo-*"}},
	})
	teamID, ok := resolver.resolve("org/repo-a")
	if !ok || teamID != "team-narrow" {
		t.Errorf("resolve(org/repo-a) = (%q, %v), want (team-narrow, true) -- longest prefix must win", teamID, ok)
	}
}

func TestRepoPatternResolver_NoMatchReturnsFalse(t *testing.T) {
	resolver := newRepoPatternResolver([]patternTeam{
		{id: "team-a", repoPatterns: []string{"org/repo-a"}},
	})
	if _, ok := resolver.resolve("org/repo-z"); ok {
		t.Error("expected no match for an unrelated repo")
	}
	if _, ok := resolver.resolve(""); ok {
		t.Error("expected no match for an empty repo name")
	}
}

func TestRepoPatternResolver_CaseInsensitive(t *testing.T) {
	resolver := newRepoPatternResolver([]patternTeam{
		{id: "team-a", repoPatterns: []string{"Org/Repo-A"}},
	})
	teamID, ok := resolver.resolve("org/repo-a")
	if !ok || teamID != "team-a" {
		t.Errorf("resolve should be case-insensitive, got (%q, %v)", teamID, ok)
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
