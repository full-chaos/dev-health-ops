package daily

import (
	"context"
	"errors"
	"testing"
	"time"

	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// Test fixtures below port test_team_cognitive_load.py's own fixtures
// directly, proving buildTeamCognitiveLoadRows produces the SAME shape of
// output as build_team_cognitive_load_rows_for_day for the same inputs.

var (
	cognitiveLoadTestDay        = time.Date(2026, 8, 28, 0, 0, 0, 0, time.UTC)
	cognitiveLoadTestComputedAt = time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
)

// TestBuildTeamCognitiveLoadRowsAggregatesByOwnershipNeverReadingRowTeamID
// ports test_aggregates_by_ownership_never_reading_the_rows_own_team_id: a
// single user_metrics row resolves to its team ONLY via repoToTeam, and
// context_spread_count is the distinct (author, repo) pair count -- NOT the
// row's own context_spread_count field (which this executor's query does
// not even select, by construction: see team_cognitive_load_clickhouse.go's
// contextSpreadPair doc comment).
func TestBuildTeamCognitiveLoadRowsAggregatesByOwnershipNeverReadingRowTeamID(t *testing.T) {
	repoID := uuid.New()
	userRows := []userMetricsCognitiveLoadInput{
		{RepoID: repoID, AuthorEmail: "a@example.com", PRInterruptionLoad: 3, ReviewRequestLoad: 1},
	}
	repoToTeam := map[string]string{repoID.String(): "gh:platform"}

	records := buildTeamCognitiveLoadRows("acme", cognitiveLoadTestDay, userRows, nil, repoToTeam, cognitiveLoadTestComputedAt)

	if len(records) != 1 {
		t.Fatalf("records=%d, want 1: %#v", len(records), records)
	}
	row := records[0]
	if row.TeamID != "gh:platform" {
		t.Fatalf("team_id=%q, want gh:platform", row.TeamID)
	}
	if row.OrganizationID != "acme" {
		t.Fatalf("org_id=%q, want acme", row.OrganizationID)
	}
	if !row.Day.Equal(cognitiveLoadTestDay) {
		t.Fatalf("day=%v, want %v", row.Day, cognitiveLoadTestDay)
	}
	if row.PRInterruptionLoad != 3.0 {
		t.Fatalf("pr_interruption_load=%v, want 3.0", row.PRInterruptionLoad)
	}
	if row.ContextSpreadCount != 1.0 {
		t.Fatalf("context_spread_count=%v, want 1.0 (distinct pairs, not the raw field)", row.ContextSpreadCount)
	}
	if row.ReviewRequestLoad != 1.0 {
		t.Fatalf("review_request_load=%v, want 1.0", row.ReviewRequestLoad)
	}
	if row.SampleAuthorCount != 1 {
		t.Fatalf("sample_author_count=%d, want 1", row.SampleAuthorCount)
	}
	if row.ContributingRepoCount != 1 {
		t.Fatalf("contributing_repo_count=%d, want 1", row.ContributingRepoCount)
	}
	if row.AfterHoursCommitRatio != nil || row.WeekendCommitRatio != nil {
		t.Fatalf("ratios=%v/%v, want nil/nil (no team_metrics_daily row at all -- unmeasured, not a measured zero)",
			row.AfterHoursCommitRatio, row.WeekendCommitRatio)
	}
}

// TestBuildTeamCognitiveLoadRowsSumsAcrossEveryOwnedRepoAndRecomputesRatio
// ports test_sums_across_every_repo_the_team_owns_and_recomputes_the_ratio: a
// team owning 2 repos sums load counters across both, and the after-hours/
// weekend ratios are recomputed from the SUMMED counts, never averaged
// per-repo. Also proves an author active in both owned repos is counted ONCE
// in sample_author_count.
func TestBuildTeamCognitiveLoadRowsSumsAcrossEveryOwnedRepoAndRecomputesRatio(t *testing.T) {
	repoA, repoB := uuid.New(), uuid.New()
	userRows := []userMetricsCognitiveLoadInput{
		{RepoID: repoA, AuthorEmail: "a@example.com", PRInterruptionLoad: 2},
		{RepoID: repoB, AuthorEmail: "b@example.com", PRInterruptionLoad: 5},
		// Same author active in both owned repos -- distinct author count
		// must still be 1, not 2.
		{RepoID: repoB, AuthorEmail: "a@example.com", PRInterruptionLoad: 1},
	}
	teamRows := []teamMetricsCognitiveLoadInput{
		// repo_a: 10 commits, 2 after-hours, 1 weekend.
		{RepoID: repoA, CommitsCount: 10, AfterHoursCommitsCount: 2, WeekendCommitsCount: 1},
		// repo_b: 20 commits, 4 after-hours, 3 weekend.
		{RepoID: repoB, CommitsCount: 20, AfterHoursCommitsCount: 4, WeekendCommitsCount: 3},
	}
	repoToTeam := map[string]string{repoA.String(): "gh:platform", repoB.String(): "gh:platform"}

	records := buildTeamCognitiveLoadRows("acme", cognitiveLoadTestDay, userRows, teamRows, repoToTeam, cognitiveLoadTestComputedAt)

	if len(records) != 1 {
		t.Fatalf("records=%d, want 1: %#v", len(records), records)
	}
	row := records[0]
	if row.PRInterruptionLoad != 8.0 { // 2 + 5 + 1
		t.Fatalf("pr_interruption_load=%v, want 8.0 (summed across repos)", row.PRInterruptionLoad)
	}
	if row.SampleAuthorCount != 2 {
		t.Fatalf("sample_author_count=%d, want 2 (a, b -- a counted once despite 2 repos)", row.SampleAuthorCount)
	}
	if row.ContributingRepoCount != 2 {
		t.Fatalf("contributing_repo_count=%d, want 2", row.ContributingRepoCount)
	}
	// commits: 10 + 20 = 30; after-hours: 2 + 4 = 6; weekend: 1 + 3 = 4.
	wantAfterHours, wantWeekend := 6.0/30.0, 4.0/30.0
	if row.AfterHoursCommitRatio == nil || *row.AfterHoursCommitRatio != wantAfterHours {
		t.Fatalf("after_hours_commit_ratio=%v, want %v (recomputed from summed counts, never averaged per-repo)",
			row.AfterHoursCommitRatio, wantAfterHours)
	}
	if row.WeekendCommitRatio == nil || *row.WeekendCommitRatio != wantWeekend {
		t.Fatalf("weekend_commit_ratio=%v, want %v", row.WeekendCommitRatio, wantWeekend)
	}
}

// TestBuildTeamCognitiveLoadRowsUnownedRepoContributesToNoTeam proves a repo
// with no repoToTeam entry contributes to NO team row at all -- never
// guessed, matching build_team_cognitive_load_rows_for_day's own
// `if not team_id: continue` guard.
func TestBuildTeamCognitiveLoadRowsUnownedRepoContributesToNoTeam(t *testing.T) {
	repoID := uuid.New()
	userRows := []userMetricsCognitiveLoadInput{
		{RepoID: repoID, AuthorEmail: "a@example.com", PRInterruptionLoad: 3},
	}

	records := buildTeamCognitiveLoadRows("acme", cognitiveLoadTestDay, userRows, nil, map[string]string{}, cognitiveLoadTestComputedAt)

	if len(records) != 0 {
		t.Fatalf("records=%d, want 0 (unowned repo must contribute to no team): %#v", len(records), records)
	}
}

// TestBuildTeamCognitiveLoadRowsMeasuredZeroVsUnmeasuredNil proves a team
// with a team_metrics_daily row that SUMS to zero commits reports a
// measured 0.0 ratio, distinct from no team_metrics_daily row at all
// (which reports nil/unmeasured, covered by the first test above).
func TestBuildTeamCognitiveLoadRowsMeasuredZeroVsUnmeasuredNil(t *testing.T) {
	repoID := uuid.New()
	teamRows := []teamMetricsCognitiveLoadInput{
		{RepoID: repoID, CommitsCount: 0, AfterHoursCommitsCount: 0, WeekendCommitsCount: 0},
	}
	repoToTeam := map[string]string{repoID.String(): "gh:platform"}

	records := buildTeamCognitiveLoadRows("acme", cognitiveLoadTestDay, nil, teamRows, repoToTeam, cognitiveLoadTestComputedAt)

	if len(records) != 1 {
		t.Fatalf("records=%d, want 1: %#v", len(records), records)
	}
	row := records[0]
	if row.AfterHoursCommitRatio == nil || *row.AfterHoursCommitRatio != 0.0 {
		t.Fatalf("after_hours_commit_ratio=%v, want a measured 0.0, not nil", row.AfterHoursCommitRatio)
	}
	if row.WeekendCommitRatio == nil || *row.WeekendCommitRatio != 0.0 {
		t.Fatalf("weekend_commit_ratio=%v, want a measured 0.0, not nil", row.WeekendCommitRatio)
	}
}

// TestBuildTeamCognitiveLoadRowsEmptyInputsProduceNoRows mirrors Python's
// `if not user_metrics_rows and not team_wellbeing_rows: return 0` early
// exit at the caller level -- the aggregator itself must also produce zero
// records for zero input, independent of that caller-level shortcut.
func TestBuildTeamCognitiveLoadRowsEmptyInputsProduceNoRows(t *testing.T) {
	records := buildTeamCognitiveLoadRows("acme", cognitiveLoadTestDay, nil, nil, map[string]string{}, cognitiveLoadTestComputedAt)
	if len(records) != 0 {
		t.Fatalf("records=%d, want 0", len(records))
	}
}

// TestApplyOwnershipToRepoToTeamRequiresRepoNamesByIDMembership ports the
// CHAOS-4365 codex r2 P1 guard from Python's
// _repo_to_team_map_for_compounding_risk: "a repo is trusted from EITHER
// source only when it also appears in repo_names_by_id" -- ownership
// included, not just the pattern-resolver fallback. A stale
// team_repo_ownership row (that table is INSERT-only; a repo removed or
// renamed after auto-import last ran still carries one) must resolve to
// UNRESOLVED here, exactly as it does on the Python side, never attribute a
// repo the org's current repos catalog no longer carries.
func TestApplyOwnershipToRepoToTeamRequiresRepoNamesByIDMembership(t *testing.T) {
	knownRepo := uuid.New()
	staleRepo := uuid.New() // owned, but absent from repoNamesByID
	repoNamesByID := map[string]string{knownRepo.String(): "acme/known-repo"}

	owners := map[string]string{
		knownRepo.String(): "team-platform",
		staleRepo.String(): "team-platform",
	}
	repoToTeam := authoritativeOwnersKnownToRepoCatalog(owners, repoNamesByID)

	if got := repoToTeam[knownRepo.String()]; got != "team-platform" {
		t.Fatalf("known repo team=%q, want team-platform", got)
	}
	if teamID, resolved := repoToTeam[staleRepo.String()]; resolved {
		t.Fatalf("stale repo (owned but absent from repo_names_by_id) resolved to %q, want "+
			"unresolved -- an owned repo_id the current repos catalog does not carry must never "+
			"be trusted, matching Python's guard", teamID)
	}
}

// erroringOwnershipConn is a driver.Conn whose ONLY reachable method is
// Query, always failing -- everything else panics if reached (mirrors
// stubDriverConn's "unimplemented" discipline, wellbeing_native_executor_test.go).
// resolveDailyFinalizeRepoToTeam's only conn use is the single
// AuthoritativeOwnerByRepo call, so this is a faithful, minimal fake for
// testing that path's error propagation.
type erroringOwnershipConn struct{ stubDriverConn }

var errOwnershipQueryFailed = errors.New("clickhouse: connection reset by peer")

func (erroringOwnershipConn) Query(context.Context, string, ...any) (chdriver.Rows, error) {
	return nil, errOwnershipQueryFailed
}

// TestResolveDailyFinalizeRepoToTeamPropagatesOwnershipQueryError is the
// failing-first proof for CHAOS-5141, #2255 r1 finding 3: a transient
// ClickHouse error while loading ownership must FAIL this resolution, never
// silently degrade to an empty map. A prior revision swallowed this error
// (`continue` inside a per-team loop), which let ComputeFinalizeFamily
// return (0, nil) -- a SUCCESS with zero rows -- for what was actually an
// infrastructure failure, and the finalize handler then marked
// team_cognitive_load Computed and skipped the Python bridge for that run,
// silently losing the family for the whole run.
func TestResolveDailyFinalizeRepoToTeamPropagatesOwnershipQueryError(t *testing.T) {
	repoID := uuid.New()
	repoNamesByID := map[string]string{repoID.String(): "acme/repo"}
	patternResolver := NewRepoPatternResolver(nil)

	_, err := resolveDailyFinalizeRepoToTeam(
		context.Background(), erroringOwnershipConn{}, "acme", cognitiveLoadTestDay,
		[]uuid.UUID{repoID}, repoNamesByID, patternResolver,
	)
	if err == nil {
		t.Fatal("err=nil, want the ownership query failure to propagate -- " +
			"a resolution failure must never silently become an empty map")
	}
	if !errors.Is(err, errOwnershipQueryFailed) {
		t.Fatalf("err=%v, want it to wrap errOwnershipQueryFailed", err)
	}
}
