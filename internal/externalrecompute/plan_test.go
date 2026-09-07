package externalrecompute

// plan_test.go is the 1:1 port of
// tests/test_external_ingest_recompute_planner.py (CHAOS-2699, brief D5-D9).
// Each test below keeps its Python name so the two can be diffed by eye: these
// ARE the D4/D5/D7/D8 invariant tests, and a renamed or merged case is a case
// that stopped pinning the incident it was written for.
//
// The Python file is NOT deleted alongside this port. Its subject
// (external_ingest/recompute.py's plan_recompute) still has a live caller --
// flush_external_ingest_recompute, reached from external_ingest/processor.py's
// schedule_or_coalesce -- so the two planners genuinely coexist until that path
// is ported too (CHAOS-4427). These tests exist so the Go planner can be shown
// to agree with the Python one case for case while both are live.

import (
	"fmt"
	"slices"
	"testing"
	"time"
)

const (
	planTestOrg      = "org-1"
	planTestSystem   = "github"
	planTestInstance = "acme/api"
)

func planTestNow() time.Time { return time.Date(2026, 6, 26, 12, 0, 0, 0, time.UTC) }

func timePtr(value time.Time) *time.Time { return &value }

// planTestScope mirrors the Python helper's defaults exactly, including the
// 2026-06-25 -> 2026-06-26 window that makes the default BackfillDays 2.
func planTestScope(mutate func(*PlanScope)) PlanScope {
	scope := PlanScope{
		OrgID:       planTestOrg,
		WindowStart: timePtr(time.Date(2026, 6, 25, 0, 0, 0, 0, time.UTC)),
		WindowEnd:   timePtr(time.Date(2026, 6, 26, 0, 0, 0, 0, time.UTC)),
	}
	if mutate != nil {
		mutate(&scope)
	}
	return scope
}

func TestGitKindsSingleRepoPerRepoChainNoFallback(t *testing.T) {
	plan := PlanRecompute(planTestScope(func(scope *PlanScope) {
		scope.RecordKinds = []string{"pull_request.v1"}
		scope.RepoIDs = []string{"repo-a"}
	}), planTestNow())

	if !plan.Trigger || !plan.DispatchDaily {
		t.Fatalf("expected trigger and daily dispatch, got %+v", plan)
	}
	if !slices.Equal(plan.RepoIDs, []string{"repo-a"}) {
		t.Fatalf("repo ids = %v", plan.RepoIDs)
	}
	if plan.FallbackOrgWideDaily || plan.SkipInvestmentNoScope ||
		plan.CappedDays || plan.CappedRepos {
		t.Fatalf("expected no fallback/skip/capping, got %+v", plan)
	}
	if got := plan.Day.Format(time.DateOnly); got != "2026-06-26" {
		t.Fatalf("day = %s", got)
	}
	if plan.BackfillDays != 2 {
		t.Fatalf("backfill days = %d", plan.BackfillDays)
	}
}

func TestWorkItemKindsEmptyRepoIDsFallsBackOrgWide(t *testing.T) {
	plan := PlanRecompute(planTestScope(func(scope *PlanScope) {
		scope.RecordKinds = []string{"work_item.v1"}
	}), planTestNow())

	if !plan.Trigger || !plan.DispatchDaily {
		t.Fatalf("expected trigger and daily dispatch, got %+v", plan)
	}
	if len(plan.RepoIDs) != 0 {
		t.Fatalf("repo ids = %v", plan.RepoIDs)
	}
	if !plan.FallbackOrgWideDaily {
		t.Fatal("expected the D8 org-wide fallback")
	}
	// D4: no team ids either -> investment has nothing to scope onto.
	if !plan.SkipInvestmentNoScope {
		t.Fatal("expected skip_investment_no_scope")
	}
}

func TestTeamKindsOnlyNoDailyInvestmentTeamIDsOnly(t *testing.T) {
	plan := PlanRecompute(planTestScope(func(scope *PlanScope) {
		scope.RecordKinds = []string{"identity.v1"}
		scope.TeamIDs = []string{"team-a"}
	}), planTestNow())

	if !plan.Trigger {
		t.Fatal("expected trigger")
	}
	if plan.DispatchDaily || plan.FallbackOrgWideDaily {
		t.Fatalf("team-only kinds must not dispatch daily, got %+v", plan)
	}
	if !slices.Equal(plan.TeamIDs, []string{"team-a"}) {
		t.Fatalf("team ids = %v", plan.TeamIDs)
	}
	if plan.SkipInvestmentNoScope {
		t.Fatal("team scope is a scope: investment must not be skipped")
	}
}

func TestRepoOnlyKindNotApplicable(t *testing.T) {
	plan := PlanRecompute(planTestScope(func(scope *PlanScope) {
		scope.RecordKinds = []string{"repository.v1"}
	}), planTestNow())

	if plan.Trigger || plan.DispatchDaily ||
		len(plan.RepoIDs) != 0 || len(plan.TeamIDs) != 0 {
		t.Fatalf("repository.v1-only must be not-applicable, got %+v", plan)
	}
}

func TestEmptyRecordKindsNotApplicable(t *testing.T) {
	if plan := PlanRecompute(planTestScope(nil), planTestNow()); plan.Trigger {
		t.Fatalf("empty record kinds must be not-applicable, got %+v", plan)
	}
}

func TestMixedKindsRepoAndTeamScopeSingleInvestmentCall(t *testing.T) {
	plan := PlanRecompute(planTestScope(func(scope *PlanScope) {
		scope.RecordKinds = []string{"pull_request.v1", "identity.v1"}
		scope.RepoIDs = []string{"repo-b", "repo-a"}
		scope.TeamIDs = []string{"team-a"}
	}), planTestNow())

	if !plan.DispatchDaily {
		t.Fatal("expected daily dispatch")
	}
	if !slices.Equal(plan.RepoIDs, []string{"repo-a", "repo-b"}) {
		t.Fatalf("repo ids = %v (must be sorted)", plan.RepoIDs)
	}
	if !slices.Equal(plan.TeamIDs, []string{"team-a"}) {
		t.Fatalf("team ids = %v", plan.TeamIDs)
	}
	if plan.FallbackOrgWideDaily || plan.SkipInvestmentNoScope {
		t.Fatalf("expected no fallback and no investment skip, got %+v", plan)
	}
}

func TestWindowSpanning40DaysCappedTo14(t *testing.T) {
	plan := PlanRecompute(planTestScope(func(scope *PlanScope) {
		scope.RecordKinds = []string{"pull_request.v1"}
		scope.RepoIDs = []string{"repo-a"}
		scope.WindowStart = timePtr(time.Date(2026, 5, 18, 0, 0, 0, 0, time.UTC))
		scope.WindowEnd = timePtr(time.Date(2026, 6, 26, 0, 0, 0, 0, time.UTC))
	}), planTestNow())

	if !plan.CappedDays {
		t.Fatal("expected capped_days")
	}
	if plan.BackfillDays != 14 {
		t.Fatalf("backfill days = %d", plan.BackfillDays)
	}
	if got := plan.Day.Format(time.DateOnly); got != "2026-06-26" {
		t.Fatalf("day = %s", got)
	}
	expectedFrom := time.Date(2026, 6, 13, 0, 0, 0, 0, time.UTC)
	if !plan.FromDate.Equal(expectedFrom) {
		t.Fatalf("from date = %s, want %s", plan.FromDate, expectedFrom)
	}
	// The day list the native per-day daily contract consumes must match the
	// clamped window exactly at both ends -- an off-by-one here silently drops
	// or double-counts a day of metrics.
	days := plan.DailyTargetDays()
	if len(days) != 14 {
		t.Fatalf("daily target days = %d", len(days))
	}
	if !days[0].Equal(plan.Day) || !days[13].Equal(expectedFrom) {
		t.Fatalf("daily target day range = %s .. %s", days[0], days[13])
	}
}

func Test60RepoIDsCappedTo25StableSorted(t *testing.T) {
	repoIDs := make([]string, 0, 60)
	for index := range 60 {
		repoIDs = append(repoIDs, fmt.Sprintf("repo-%03d", index))
	}
	plan := PlanRecompute(planTestScope(func(scope *PlanScope) {
		scope.RecordKinds = []string{"pull_request.v1"}
		scope.RepoIDs = slices.Clone(repoIDs)
	}), planTestNow())

	if !plan.CappedRepos {
		t.Fatal("expected capped_repos")
	}
	sorted := slices.Clone(repoIDs)
	slices.Sort(sorted)
	if !slices.Equal(plan.RepoIDs, sorted[:25]) {
		t.Fatalf("repo ids = %v", plan.RepoIDs)
	}
}

func TestGitOnlyEmptyRepoIDsDefensiveSkipInvestmentNoScope(t *testing.T) {
	// Structurally impossible in practice (PR/review/commit always carry a
	// resolved repo id) but the defensive skip path is asserted anyway per the
	// brief's D4 hard-invariant test plan bullet.
	plan := PlanRecompute(planTestScope(func(scope *PlanScope) {
		scope.RecordKinds = []string{"pull_request.v1"}
	}), planTestNow())

	if !plan.DispatchDaily {
		t.Fatal("expected daily dispatch")
	}
	if len(plan.RepoIDs) != 0 {
		t.Fatalf("repo ids = %v", plan.RepoIDs)
	}
	// D8 fallback is work-item-only; git kinds never get it.
	if plan.FallbackOrgWideDaily {
		t.Fatal("git kinds must never take the org-wide fallback")
	}
	if !plan.SkipInvestmentNoScope {
		t.Fatal("expected skip_investment_no_scope")
	}
}

func TestSingleDayWindowBackfillDaysIsOne(t *testing.T) {
	sameDay := time.Date(2026, 6, 26, 10, 0, 0, 0, time.UTC)
	plan := PlanRecompute(planTestScope(func(scope *PlanScope) {
		scope.RecordKinds = []string{"pull_request.v1"}
		scope.RepoIDs = []string{"repo-a"}
		scope.WindowStart = timePtr(sameDay)
		scope.WindowEnd = timePtr(sameDay.Add(2 * time.Hour))
	}), planTestNow())

	if plan.BackfillDays != 1 || plan.CappedDays {
		t.Fatalf("expected a single uncapped day, got %+v", plan)
	}
	if days := plan.DailyTargetDays(); len(days) != 1 {
		t.Fatalf("daily target days = %d", len(days))
	}
}

func TestMissingWindowDefaultsToNow(t *testing.T) {
	plan := PlanRecompute(planTestScope(func(scope *PlanScope) {
		scope.RecordKinds = []string{"pull_request.v1"}
		scope.RepoIDs = []string{"repo-a"}
		scope.WindowStart = nil
		scope.WindowEnd = nil
	}), planTestNow())

	if !plan.Trigger {
		t.Fatal("expected trigger")
	}
	if plan.Day.IsZero() {
		t.Fatal("expected a resolved day")
	}
	if !plan.Day.Equal(time.Date(2026, 6, 26, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("day = %s (must come from the injected now)", plan.Day)
	}
	if plan.BackfillDays != 1 {
		t.Fatalf("backfill days = %d", plan.BackfillDays)
	}
}

// TestWindowStartOnlyUsesStartAsEnd covers the Python expression
// `window_end = scope.window_end or scope.window_start or now`, whose middle
// branch has no Python test: a scope carrying only a start must not silently
// fall through to now and widen the window to today.
func TestWindowStartOnlyUsesStartAsEnd(t *testing.T) {
	start := time.Date(2026, 6, 20, 8, 0, 0, 0, time.UTC)
	plan := PlanRecompute(planTestScope(func(scope *PlanScope) {
		scope.RecordKinds = []string{"pull_request.v1"}
		scope.RepoIDs = []string{"repo-a"}
		scope.WindowStart = timePtr(start)
		scope.WindowEnd = nil
	}), planTestNow())

	if !plan.Day.Equal(time.Date(2026, 6, 20, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("day = %s", plan.Day)
	}
	if plan.BackfillDays != 1 {
		t.Fatalf("backfill days = %d", plan.BackfillDays)
	}
}

// TestOperationalKindsTriggerWithoutDaily pins the D7 operational row: those
// kinds make the plan applicable (so investment still materializes for a scoped
// batch) but never dispatch daily, which no Python test covered directly.
func TestOperationalKindsTriggerWithoutDaily(t *testing.T) {
	plan := PlanRecompute(planTestScope(func(scope *PlanScope) {
		scope.RecordKinds = []string{"operational_incident.v1"}
		scope.RepoIDs = []string{"repo-a"}
	}), planTestNow())

	if !plan.Trigger {
		t.Fatal("expected trigger")
	}
	if plan.DispatchDaily {
		t.Fatal("operational kinds must not dispatch daily")
	}
	if plan.SkipInvestmentNoScope {
		t.Fatal("a repo-scoped operational batch still materializes investment")
	}
	if days := plan.DailyTargetDays(); days != nil {
		t.Fatalf("daily target days = %v", days)
	}
}

func TestEnvOverridesNarrowTheCaps(t *testing.T) {
	t.Setenv(envMaxBackfillDays, "3")
	t.Setenv(envMaxFanoutRepos, "2")
	plan := PlanRecompute(planTestScope(func(scope *PlanScope) {
		scope.RecordKinds = []string{"pull_request.v1"}
		scope.RepoIDs = []string{"repo-a", "repo-b", "repo-c"}
		scope.WindowStart = timePtr(time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC))
		scope.WindowEnd = timePtr(time.Date(2026, 6, 26, 0, 0, 0, 0, time.UTC))
	}), planTestNow())

	if plan.BackfillDays != 3 || !plan.CappedDays {
		t.Fatalf("backfill days = %d capped = %v", plan.BackfillDays, plan.CappedDays)
	}
	if !slices.Equal(plan.RepoIDs, []string{"repo-a", "repo-b"}) || !plan.CappedRepos {
		t.Fatalf("repo ids = %v capped = %v", plan.RepoIDs, plan.CappedRepos)
	}
}

// TestEnvOverrideRejectsZeroAndGarbage mirrors Python's max(1, _env_int(...)):
// a zero cap would mean "recompute nothing", and an unparseable value must fall
// back to the default rather than to zero.
func TestEnvOverrideRejectsZeroAndGarbage(t *testing.T) {
	t.Setenv(envMaxBackfillDays, "0")
	t.Setenv(envMaxFanoutRepos, "not-a-number")
	plan := PlanRecompute(planTestScope(func(scope *PlanScope) {
		scope.RecordKinds = []string{"pull_request.v1"}
		scope.RepoIDs = []string{"repo-a"}
	}), planTestNow())

	if plan.BackfillDays != 1 {
		t.Fatalf("backfill days = %d, want the floor of 1", plan.BackfillDays)
	}
	if !slices.Equal(plan.RepoIDs, []string{"repo-a"}) {
		t.Fatalf("repo ids = %v", plan.RepoIDs)
	}
}
