package externalrecompute

// replay_test.go covers the collapse rules, which are the whole correctness
// argument for pointing the replay command at production: an operator draining
// ~2.5 weeks of backlog needs to know that "collapse" widens coverage and never
// narrows it.

import (
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
)

func backlogRow(
	org, system, instance string,
	dispatchedAt time.Time,
	scope *PlanScope,
	ingestionIDs ...string,
) BacklogRow {
	return BacklogRow{
		JobID:          uuid.New(),
		BridgeID:       uuid.NewString(),
		OrgID:          org,
		SourceSystem:   system,
		SourceInstance: instance,
		DispatchedAt:   dispatchedAt,
		Scope:          scope,
		IngestionIDs:   ingestionIDs,
	}
}

func TestCollapseBacklogUnionsScopeAndWidensWindow(t *testing.T) {
	early := time.Date(2026, 8, 19, 3, 0, 0, 0, time.UTC)
	late := time.Date(2026, 9, 6, 21, 0, 0, 0, time.UTC)
	rows := []BacklogRow{
		backlogRow("org-1", "github", "acme/api", early, &PlanScope{
			OrgID:       "org-1",
			RepoIDs:     []string{"repo-b", "repo-a"},
			TeamIDs:     []string{"team-a"},
			RecordKinds: []string{"commit.v1"},
			WindowStart: timePtr(early.Add(-2 * time.Hour)),
			WindowEnd:   timePtr(early),
		}, "ing-1"),
		backlogRow("org-1", "github", "acme/api", late, &PlanScope{
			OrgID:       "org-1",
			RepoIDs:     []string{"repo-c", "repo-a"},
			RecordKinds: []string{"work_item.v1"},
			WindowStart: timePtr(late.Add(-time.Hour)),
			WindowEnd:   timePtr(late),
		}, "ing-2"),
	}

	groups := CollapseBacklog(rows)
	if len(groups) != 1 {
		t.Fatalf("groups = %d, want one per (org, system, instance)", len(groups))
	}
	group := groups[0]
	if group.Rows != 2 || len(group.JobIDs) != 2 {
		t.Fatalf("group rows = %d job ids = %d", group.Rows, len(group.JobIDs))
	}
	if !slices.Equal(group.Scope.RepoIDs, []string{"repo-a", "repo-b", "repo-c"}) {
		t.Fatalf("repo ids = %v (must be the deduped union)", group.Scope.RepoIDs)
	}
	if !slices.Equal(group.Scope.TeamIDs, []string{"team-a"}) {
		t.Fatalf("team ids = %v", group.Scope.TeamIDs)
	}
	if !slices.Equal(group.Scope.RecordKinds, []string{"commit.v1", "work_item.v1"}) {
		t.Fatalf("record kinds = %v (must be the union, so kind gating sees both)", group.Scope.RecordKinds)
	}
	if !slices.Equal(group.IngestionIDs, []string{"ing-1", "ing-2"}) {
		t.Fatalf("ingestion ids = %v", group.IngestionIDs)
	}
	// The widest window, not the newest row's: collapsing must cover every day
	// any row in the group asked for, or the collapse silently loses coverage
	// that a per-row replay would have had.
	if !group.Scope.WindowStart.Equal(early.Add(-2 * time.Hour)) {
		t.Fatalf("window start = %s", group.Scope.WindowStart)
	}
	if !group.Scope.WindowEnd.Equal(late) {
		t.Fatalf("window end = %s", group.Scope.WindowEnd)
	}
	if !group.Oldest.Equal(early) || !group.Newest.Equal(late) {
		t.Fatalf("age range = %s .. %s", group.Oldest, group.Newest)
	}
}

func TestCollapseBacklogSeparatesSourceInstances(t *testing.T) {
	at := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	scope := func(repo string) *PlanScope {
		return &PlanScope{OrgID: "org-1", RepoIDs: []string{repo},
			RecordKinds: []string{"commit.v1"}, WindowEnd: timePtr(at)}
	}
	groups := CollapseBacklog([]BacklogRow{
		backlogRow("org-1", "github", "acme/api", at, scope("repo-a")),
		backlogRow("org-1", "github", "acme/web", at, scope("repo-b")),
		backlogRow("org-2", "github", "acme/api", at, scope("repo-c")),
	})
	if len(groups) != 3 {
		t.Fatalf("groups = %d, want one per debounce grain", len(groups))
	}
	// D10: different source instances have disjoint repo/team scopes, so
	// merging them would recompute each org's repositories under the other's
	// window.
	for _, group := range groups {
		if len(group.Scope.RepoIDs) != 1 {
			t.Fatalf("group %+v merged across grains", group)
		}
	}
}

func TestCollapseBacklogRetiresScopelessRowsWithoutWideningScope(t *testing.T) {
	at := time.Date(2026, 8, 21, 0, 0, 0, 0, time.UTC)
	rows := []BacklogRow{
		backlogRow("org-1", "github", "acme/api", at, &PlanScope{
			OrgID: "org-1", RepoIDs: []string{"repo-a"},
			RecordKinds: []string{"commit.v1"}, WindowEnd: timePtr(at),
		}, "ing-1"),
		// Already-terminal or corrupt: no pending batch row still carries its
		// bridge id. It must still be retired -- leaving it behind would strand
		// a row nothing can ever consume -- but it must not contribute scope.
		backlogRow("org-1", "github", "acme/api", at.Add(time.Hour), nil),
	}
	groups := CollapseBacklog(rows)
	if len(groups) != 1 {
		t.Fatalf("groups = %d", len(groups))
	}
	group := groups[0]
	if len(group.JobIDs) != 2 {
		t.Fatalf("job ids = %d, want both rows retired", len(group.JobIDs))
	}
	if !slices.Equal(group.Scope.RepoIDs, []string{"repo-a"}) {
		t.Fatalf("repo ids = %v", group.Scope.RepoIDs)
	}
	if !slices.Equal(group.IngestionIDs, []string{"ing-1"}) {
		t.Fatalf("ingestion ids = %v", group.IngestionIDs)
	}
}

// TestCollapsedPlanStaysBoundedAcrossAWideBacklog is the property an operator
// actually needs before running this against production: collapsing weeks of
// rows into one plan must not produce an unbounded recompute. The planner's own
// caps apply to the collapsed scope exactly as they do to a single row's.
func TestCollapsedPlanStaysBoundedAcrossAWideBacklog(t *testing.T) {
	start := time.Date(2026, 8, 19, 0, 0, 0, 0, time.UTC)
	rows := make([]BacklogRow, 0, 40)
	for day := range 40 {
		at := start.AddDate(0, 0, day)
		repos := make([]string, 0, 3)
		for index := range 3 {
			repos = append(repos, uuid.NewSHA1(uuid.NameSpaceOID,
				[]byte{byte(day), byte(index)}).String())
		}
		rows = append(rows, backlogRow("org-1", "github", "acme/api", at, &PlanScope{
			OrgID: "org-1", RepoIDs: repos,
			RecordKinds: []string{"commit.v1"},
			WindowStart: timePtr(at), WindowEnd: timePtr(at),
		}))
	}
	groups := CollapseBacklog(rows)
	if len(groups) != 1 {
		t.Fatalf("groups = %d", len(groups))
	}
	plan := PlanRecompute(groups[0].Scope, start.AddDate(0, 0, 45))
	if plan.BackfillDays != defaultMaxBackfillDays || !plan.CappedDays {
		t.Fatalf("backfill days = %d capped = %v, want the cap to bind",
			plan.BackfillDays, plan.CappedDays)
	}
	if len(plan.RepoIDs) != defaultMaxFanoutRepos || !plan.CappedRepos {
		t.Fatalf("repo ids = %d capped = %v, want the fan-out cap to bind",
			len(plan.RepoIDs), plan.CappedRepos)
	}
	if days := plan.DailyTargetDays(); len(days) != defaultMaxBackfillDays {
		t.Fatalf("daily target days = %d, want the cap", len(days))
	}
}
