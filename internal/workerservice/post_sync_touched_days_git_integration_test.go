//go:build integration

package workerservice

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

// seedProviderSync is seedSync for a unit of the given provider and dataset.
func (rig *touchedRig) seedProviderSync(t *testing.T, ctx context.Context, orgID, provider, dataset string, day time.Time) syncdispatchruntime.PostSyncArgs {
	t.Helper()
	since, before := day, day.Add(12*time.Hour)
	runID, outboxID, integrationID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	pgseed.EnsureSyncRun(ctx, t, rig.pool, pgseed.SyncRun{ID: runID, OrgID: orgID, IntegrationID: integrationID})
	pgseed.SyncDispatchOutbox(ctx, t, rig.pool, outboxID, runID, orgID, "post_sync", "dispatched", "river", touchedRigRouteGeneration)
	pgseed.InsertSyncRunUnit(ctx, t, rig.pool, pgseed.SyncRunUnit{
		ID: uuid.NewString(), RunID: runID, OrgID: orgID, IntegrationID: integrationID, SourceID: uuid.NewString(),
		Provider: provider, DatasetKey: dataset, Status: "success", SinceAt: &since, BeforeAt: &before,
	})
	return syncdispatchruntime.PostSyncArgs{TransportArgs: syncdispatchruntime.TransportArgs{
		Version: syncdispatchruntime.ContractVersionV1, OrgID: orgID, RunID: runID,
		DispatchOutbox: outboxID, RouteGeneration: touchedRigRouteGeneration,
	}}
}

// CHAOS-9169: a commit or a pull request that a sync wrote for a day far
// outside its own window gets a daily run for that day and that repository, for
// a github commits unit and for a gitlab pull-requests unit alike; a sync of a
// provider that writes neither (a jira work-items unit) starts only its window
// run. The record is read from the tables the run wrote, never from the window.
func TestPostSyncFanoutRecomputesTheDayOfALateCommitAndPullRequest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	rig := newTouchedRig(t, ctx)
	orgID := uuid.NewString()
	target := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	lateCommitDay, latePullRequestDay := target.AddDate(0, 0, -40), target.AddDate(0, 0, -55)
	repoA, repoB := uuid.New(), uuid.New()
	insertTouchedRepo(t, ctx, rig.conn, orgID, repoA, "acme/api", "github")
	insertTouchedRepo(t, ctx, rig.conn, orgID, repoB, "acme/web", "gitlab")
	service := rig.service(t, rig.touched, nil)
	targetKey := target.Format("2006-01-02")

	// A github commits unit; its commit was committed 40 days before its window.
	now := time.Now().UTC()
	if err := rig.conn.Exec(ctx, `INSERT INTO git_commits (repo_id, hash, author_when, committer_when, parents, org_id, last_synced) VALUES (?, ?, ?, ?, 1, ?, ?)`,
		repoA, "late-commit", lateCommitDay.Add(9*time.Hour), lateCommitDay.Add(9*time.Hour), orgID, now); err != nil {
		t.Fatal(err)
	}
	commits := rig.seedProviderSync(t, ctx, orgID, "github", "commits", target)
	if err := service.Fanout(ctx, commits); err != nil {
		t.Fatal(err)
	}
	runs := rig.runsOf(t, ctx, orgID, commits)
	commitKey := lateCommitDay.Format("2006-01-02")
	if got := touchedRunDays(runs); !reflect.DeepEqual(got, []string{commitKey, targetKey}) {
		t.Fatalf("days with a run after a github commits sync = %v, want the late commit's day and the target day", got)
	}
	if runs[commitKey].fullOrg || runs[commitKey].repos != repoA.String() {
		t.Fatalf("run of the late commit's day = %+v, want a run of repository %s only", runs[commitKey], repoA)
	}

	// A gitlab pull-requests unit of a later run; its pull request was created 55 days before its window.
	later := now.Add(time.Hour)
	if err := rig.conn.Exec(ctx, `INSERT INTO git_pull_requests (repo_id, number, created_at, org_id, last_synced) VALUES (?, 7, ?, ?, ?)`,
		repoB, latePullRequestDay.Add(9*time.Hour), orgID, later); err != nil {
		t.Fatal(err)
	}
	pullRequests := rig.seedProviderSync(t, ctx, orgID, "gitlab", "prs", target)
	rig.setRunStart(t, ctx, pullRequests, later.Add(-time.Minute))
	if err := service.Fanout(ctx, pullRequests); err != nil {
		t.Fatal(err)
	}
	runs = rig.runsOf(t, ctx, orgID, pullRequests)
	pullRequestKey := latePullRequestDay.Format("2006-01-02")
	if got := touchedRunDays(runs); !reflect.DeepEqual(got, []string{pullRequestKey, targetKey}) {
		t.Fatalf("days with a run after a gitlab prs sync = %v, want the pull request's day and the target day", got)
	}
	if runs[pullRequestKey].fullOrg || runs[pullRequestKey].repos != repoB.String() {
		t.Fatalf("run of the late pull request's day = %+v, want a run of repository %s only", runs[pullRequestKey], repoB)
	}

	// A jira work-items unit writes neither commits nor pull requests: its fan-out records none
	// (the rows above are older than its run start) and starts only the window run.
	items := rig.seedProviderSync(t, ctx, orgID, "jira", "work-items", target)
	rig.setRunStart(t, ctx, items, later.Add(time.Hour))
	if err := service.Fanout(ctx, items); err != nil {
		t.Fatal(err)
	}
	if got := touchedRunDays(rig.runsOf(t, ctx, orgID, items)); !reflect.DeepEqual(got, []string{targetKey}) {
		t.Fatalf("days with a run after a jira work-items sync = %v, want only the target day", got)
	}
}
