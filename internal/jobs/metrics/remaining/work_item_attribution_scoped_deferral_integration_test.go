//go:build integration

package remaining

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// TestWorkItemAttributionScopedRunRetriesTheItemsItDeferredOnTheNextDay: run 1 (org-wide) and run 2 (scoped to
// project PA / proj-a) fall on one UTC day D, so run 2 defers A and its reverse-hop dependent B (the same-day
// rule): their rows from run 1 are older than the change run 2 saw. A scoped run on D+1 with nothing else changed
// must still rederive them, and B takes A's team through linked_issue. A later run of D+1 finds nothing.
func TestWorkItemAttributionScopedRunRetriesTheItemsItDeferredOnTheNextDay(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionMigratedClickHouse(t, ctx)
	writer, err := NewWorkItemAttributionClickHouseWriter(conn)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	executor, err := NewWorkItemAttributionExecutor(ctx, conn, writer)
	if err != nil {
		t.Fatalf("new executor: %v", err)
	}
	orgID := "org-scopeddeferral-" + uuid.NewString()
	now := startAwayFromMidnight()

	outcome2, bRows := issueDonorReverseHopRuns(t, ctx, conn, orgID, now, executor, executor)
	t.Logf("run 2 (day D) outcome=%+v B=%s", outcome2, primaryTeamSources(bRows))
	if outcome2.ItemsSeen != 2 || outcome2.RowsWritten != 0 {
		t.Fatalf("run 2 outcome = %+v, want A and B seen and both deferred (2, 0)", outcome2)
	}

	nextDay := time.Now().UTC().Add(24 * time.Hour)
	runNextDay := &WorkItemAttributionExecutor{conn: conn, writer: writer, nowUTC: func() time.Time { return nextDay }}
	outcome3, err := runNextDay.ComputeOrg(ctx, orgID)
	if err != nil {
		t.Fatalf("ComputeOrg run 3: %v", err)
	}
	bAfter := latestAttributions(t, ctx, conn, orgID, "B")
	t.Logf("run 3 (day D+1) outcome=%+v B=%s", outcome3, primaryTeamSources(bAfter))
	if outcome3.SkippedNoop || outcome3.OrgWide || !containsString(outcome3.ProjectKeys, "PA") {
		t.Fatalf("run 3 outcome = %+v, want a run scoped to PA that rederives the items run 2 deferred", outcome3)
	}
	if got := primaryTeamSources(bAfter); got != "[team-infra/linked_issue]" {
		t.Fatalf("B primary row after run 3 = %s, want [team-infra/linked_issue]", got)
	}
	const wantEvidence = "linked_issue=A;dependency_type=relates_to;donor_provider=github;donor_source=project_ownership"
	for _, row := range bAfter {
		if row.isPrimary == 1 && row.evidence != wantEvidence {
			t.Errorf("B linked_issue evidence = %q, want %q", row.evidence, wantEvidence)
		}
	}

	laterNextDay := nextDay.Add(time.Minute)
	runLater := &WorkItemAttributionExecutor{conn: conn, writer: writer, nowUTC: func() time.Time { return laterNextDay }}
	outcome4, err := runLater.ComputeOrg(ctx, orgID)
	if err != nil {
		t.Fatalf("ComputeOrg run 4: %v", err)
	}
	if !outcome4.SkippedNoop {
		t.Fatalf("run 4 (later on D+1) outcome = %+v, want a no-op: run 3 covered the change", outcome4)
	}
}

// TestWorkItemAttributionOrgWideRunRetriesTheItemsItDeferredOnTheNextDay is the org-wide form: run 2 is org-wide
// because a team row changed after run 1 on the same day; it defers every item, and the org-wide run of D+1
// rederives them.
func TestWorkItemAttributionOrgWideRunRetriesTheItemsItDeferredOnTheNextDay(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionMigratedClickHouse(t, ctx)
	writer, err := NewWorkItemAttributionClickHouseWriter(conn)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	executor, err := NewWorkItemAttributionExecutor(ctx, conn, writer)
	if err != nil {
		t.Fatalf("new executor: %v", err)
	}
	orgID := "org-orgwidedeferral-" + uuid.NewString()
	now := startAwayFromMidnight()
	old := now.Add(-72 * time.Hour)
	if err := conn.Exec(ctx, `INSERT INTO work_items (repo_id, work_item_id, provider, type, project_id, project_key, org_id, last_synced)
		VALUES (?, 'A', 'github', 'issue', 'proj-a', 'PA', ?, ?)`, uuid.Nil, orgID, now); err != nil {
		t.Fatalf("seed issue A: %v", err)
	}
	seedWorkItemAttributionPullRequest(t, ctx, conn, orgID, "B", uuid.Nil, now)
	for i := 0; i < 18; i++ {
		seedWorkItemAttributionItem(t, ctx, conn, orgID, fmt.Sprintf("filler-%d", i), uuid.Nil, now)
	}
	insertTeamInfra := func(updatedAt time.Time) {
		t.Helper()
		if err := conn.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, manual_members, project_keys, repo_patterns, is_active, updated_at, last_synced, org_id, provider, native_team_key) VALUES ('team-infra', ?, 'Team infra', [], [], [], 1, ?, ?, ?, 'github', '')`,
			uuid.New(), updatedAt, updatedAt, orgID); err != nil {
			t.Fatalf("insert team-infra: %v", err)
		}
	}
	insertTeamInfra(old)
	projectKey := "PA"
	seedWorkItemAttributionProjectOwnershipAt(t, ctx, conn, orgID, "github", "proj-a", &projectKey, "team-infra", old, old)
	waitForWorkItemAttributionProjectFactVisible(t, ctx, conn, orgID)

	outcome1, err := executor.ComputeOrg(ctx, orgID)
	if err != nil || !outcome1.OrgWide {
		t.Fatalf("ComputeOrg run 1 = %+v, %v; want an org-wide run", outcome1, err)
	}

	time.Sleep(1100 * time.Millisecond)
	seedWorkItemAttributionDependency(t, ctx, conn, orgID, "B", "A", "relates_to", time.Now().UTC())
	insertTeamInfra(time.Now().UTC())

	outcome2, err := executor.ComputeOrg(ctx, orgID)
	if err != nil {
		t.Fatalf("ComputeOrg run 2: %v", err)
	}
	if !outcome2.OrgWide || outcome2.RowsWritten != 0 {
		t.Fatalf("run 2 outcome = %+v, want an org-wide run that defers every item written today (0 rows)", outcome2)
	}

	nextDay := time.Now().UTC().Add(24 * time.Hour)
	runNextDay := &WorkItemAttributionExecutor{conn: conn, writer: writer, nowUTC: func() time.Time { return nextDay }}
	outcome3, err := runNextDay.ComputeOrg(ctx, orgID)
	if err != nil {
		t.Fatalf("ComputeOrg run 3: %v", err)
	}
	if !outcome3.OrgWide || outcome3.RowsWritten == 0 {
		t.Fatalf("run 3 (day D+1) outcome = %+v, want an org-wide run that rederives the items run 2 deferred", outcome3)
	}
	if got := primaryTeamSources(latestAttributions(t, ctx, conn, orgID, "B")); got != "[team-infra/linked_issue]" {
		t.Fatalf("B primary row after run 3 = %s, want [team-infra/linked_issue]", got)
	}

	laterNextDay := nextDay.Add(time.Minute)
	runLater := &WorkItemAttributionExecutor{conn: conn, writer: writer, nowUTC: func() time.Time { return laterNextDay }}
	outcome4, err := runLater.ComputeOrg(ctx, orgID)
	if err != nil {
		t.Fatalf("ComputeOrg run 4: %v", err)
	}
	if !outcome4.SkippedNoop {
		t.Fatalf("run 4 (later on D+1) outcome = %+v, want a no-op", outcome4)
	}
}

// TestWorkItemAttributionScopedRunDefersAnItemWrittenAfterTheChangeOnce: the daily family wrote W today AFTER the
// ownership change, so its row already holds the change. The backstop defers W and the next day's run finds
// nothing: a deferral of a row newer than the change does not make the scope run again.
func TestWorkItemAttributionScopedRunDefersAnItemWrittenAfterTheChangeOnce(t *testing.T) {
	ctx := context.Background()
	conn, writer, orgID, repoX, base := deferralRepoFixture(t, "W1")
	change := base.Add(-time.Second)
	seedWorkItemAttributionRepoOwnership(t, ctx, conn, orgID, repoX, "team-infra", change)
	seedDailyAttributionRow(t, ctx, writer, orgID, "W1", repoX, base)
	waitForWorkItemAttributionRepoFactVisible(t, ctx, conn, orgID)

	outcomeD, outcomeNext := runTodayThenNextDay(t, ctx, conn, writer, orgID)
	if outcomeD.RowsWritten != 0 || outcomeD.ItemsSeen != 1 {
		t.Fatalf("run on D outcome = %+v, want W1 seen and deferred (1, 0)", outcomeD)
	}
	if !outcomeNext.SkippedNoop {
		t.Fatalf("run on D+1 outcome = %+v, want a no-op: W1's row of D is newer than the change", outcomeNext)
	}
}

// TestWorkItemAttributionScopedRunHoldsItsMarkerAtTheEarliestDeferredRow: W1's row of today is older than the
// ownership change, W2's is newer. The run defers both; the next day's run must rederive the scope, because W1's
// row does not hold the change.
func TestWorkItemAttributionScopedRunHoldsItsMarkerAtTheEarliestDeferredRow(t *testing.T) {
	ctx := context.Background()
	conn, writer, orgID, repoX, base := deferralRepoFixture(t, "W1", "W2")
	change := base.Add(-time.Second)
	seedDailyAttributionRow(t, ctx, writer, orgID, "W1", repoX, base.Add(-2*time.Second))
	seedWorkItemAttributionRepoOwnership(t, ctx, conn, orgID, repoX, "team-infra", change)
	seedDailyAttributionRow(t, ctx, writer, orgID, "W2", repoX, base)
	waitForWorkItemAttributionRepoFactVisible(t, ctx, conn, orgID)

	outcomeD, outcomeNext := runTodayThenNextDay(t, ctx, conn, writer, orgID)
	if outcomeD.RowsWritten != 0 || outcomeD.ItemsSeen != 2 {
		t.Fatalf("run on D outcome = %+v, want W1 and W2 seen and deferred (2, 0)", outcomeD)
	}
	if outcomeNext.SkippedNoop || outcomeNext.RowsWritten == 0 {
		t.Fatalf("run on D+1 outcome = %+v, want the scope rederived: W1's row of D predates the change", outcomeNext)
	}
	if got := primaryTeamSources(latestAttributions(t, ctx, conn, orgID, "W1")); got != "[team-infra/repo_ownership]" {
		t.Fatalf("W1 primary row after D+1 = %s, want [team-infra/repo_ownership]", got)
	}
}

// TestWorkItemAttributionMarkerHoldBackCoversTheWriterRankFold: the daily row of W1 is stored 2 ms after the
// instant it was computed (the writer rank fold), and the ownership change lands 1 ms after that instant. The
// row does not hold the change, so the next day's run must rederive the scope.
func TestWorkItemAttributionMarkerHoldBackCoversTheWriterRankFold(t *testing.T) {
	ctx := context.Background()
	conn, writer, orgID, repoX, base := deferralRepoFixture(t, "W1")
	computed := base.Add(-time.Second)
	seedDailyAttributionRow(t, ctx, writer, orgID, "W1", repoX, computed)
	seedWorkItemAttributionRepoOwnership(t, ctx, conn, orgID, repoX, "team-infra", computed.Add(time.Millisecond))
	waitForWorkItemAttributionRepoFactVisible(t, ctx, conn, orgID)

	outcomeD, outcomeNext := runTodayThenNextDay(t, ctx, conn, writer, orgID)
	if outcomeD.RowsWritten != 0 {
		t.Fatalf("run on D outcome = %+v, want W1 deferred (0 rows)", outcomeD)
	}
	if outcomeNext.SkippedNoop || outcomeNext.RowsWritten == 0 {
		t.Fatalf("run on D+1 outcome = %+v, want the scope rederived: the change is 1 ms after W1 was computed", outcomeNext)
	}
}

// startAwayFromMidnight returns now (UTC), after waiting out the last two minutes of a UTC day: the tests put
// several writes and runs on one day.
func startAwayFromMidnight() time.Time {
	now := time.Now().UTC()
	if untilMidnight := now.Truncate(24 * time.Hour).Add(24 * time.Hour).Sub(now); untilMidnight < 2*time.Minute {
		time.Sleep(untilMidnight + time.Second)
		now = time.Now().UTC()
	}
	return now
}

// deferralRepoFixture: an org with the items of repository X and 18 fillers with no repository. It returns a base
// instant (millisecond precision) at least 3 s after the start of today.
func deferralRepoFixture(t *testing.T, items ...string) (driver.Conn, *WorkItemAttributionClickHouseWriter, string, uuid.UUID, time.Time) {
	t.Helper()
	ctx := context.Background()
	conn := workItemAttributionMigratedClickHouse(t, ctx)
	writer, err := NewWorkItemAttributionClickHouseWriter(conn)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	base := startAwayFromMidnight().Truncate(time.Millisecond)
	if base.Sub(base.Truncate(24*time.Hour)) < 3*time.Second {
		time.Sleep(3 * time.Second)
		base = time.Now().UTC().Truncate(time.Millisecond)
	}
	orgID := "org-deferralmarker-" + uuid.NewString()
	repoX := uuid.New()
	for _, item := range items {
		seedWorkItemAttributionItem(t, ctx, conn, orgID, item, repoX, base)
	}
	for i := 0; i < 18; i++ {
		seedWorkItemAttributionItem(t, ctx, conn, orgID, fmt.Sprintf("filler-%d", i), uuid.Nil, base)
	}
	return conn, writer, orgID, repoX, base
}

// seedDailyAttributionRow writes W's row as the daily family does (writer daily, so the rank fold applies).
func seedDailyAttributionRow(
	t *testing.T, ctx context.Context, writer *WorkItemAttributionClickHouseWriter,
	orgID, workItemID string, repoID uuid.UUID, computedAt time.Time,
) {
	t.Helper()
	team := "team-old"
	if _, err := writer.WriteAttributions(ctx, WorkItemAttributionProducer{Writer: WorkItemAttributionWriterDaily, RunID: "seed-daily"},
		[]WorkItemAttributionRow{{
			WorkItemID: workItemID, Provider: "github", Source: "repo_ownership", IsPrimary: 1,
			Confidence: "high", Evidence: "seeded-daily", ComputedAt: computedAt, RepoID: &repoID,
			TeamID: &team, TeamName: &team, OrgID: orgID,
		}}); err != nil {
		t.Fatalf("seed daily row %s: %v", workItemID, err)
	}
}

// runTodayThenNextDay runs the backstop now (day D, which must be a scoped run of repository X), then with the
// clock of the next day.
func runTodayThenNextDay(
	t *testing.T, ctx context.Context, conn driver.Conn, writer *WorkItemAttributionClickHouseWriter, orgID string,
) (WorkItemAttributionOutcome, WorkItemAttributionOutcome) {
	t.Helper()
	executor, err := NewWorkItemAttributionExecutor(ctx, conn, writer)
	if err != nil {
		t.Fatalf("new executor: %v", err)
	}
	outcomeD, err := executor.ComputeOrg(ctx, orgID)
	if err != nil {
		t.Fatalf("ComputeOrg on D: %v", err)
	}
	if outcomeD.OrgWide || len(outcomeD.RepoIDs) != 1 {
		t.Fatalf("run on D outcome = %+v, want a run scoped to repository X", outcomeD)
	}
	nextDay := time.Now().UTC().Add(24 * time.Hour)
	runNextDay := &WorkItemAttributionExecutor{conn: conn, writer: writer, nowUTC: func() time.Time { return nextDay }}
	outcomeNext, err := runNextDay.ComputeOrg(ctx, orgID)
	if err != nil {
		t.Fatalf("ComputeOrg on D+1: %v", err)
	}
	t.Logf("run on D outcome=%+v; run on D+1 outcome=%+v", outcomeD, outcomeNext)
	return outcomeD, outcomeNext
}
