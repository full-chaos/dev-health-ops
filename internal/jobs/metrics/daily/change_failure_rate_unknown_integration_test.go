//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/opfixture"
)

// The defect of CHAOS-8981, through the production entry point: a repository
// that commits and deploys but has no incident evidence must not store a 0%
// change failure rate. Before the fix the stored change failure rate was
// repo_metrics_daily.change_failure_rate: reverted / merged pull requests with
// the denominator forced to 1, so this repository read 0.0. The change failure
// rate is now change_failure_rate_incident: NULL (unknown) here, and a
// measured value only when an incident ties to the repository.
//
// The test reads the column that holds the change failure rate in the schema
// it runs against, so the same file is red on the commit before the fix
// (which has only the old column) and green after it.
func TestRepoUserCommitStoresNoZeroChangeFailureRateWithoutIncidentEvidence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	_, conn := opfixture.Start(ctx, t)

	const org = "5d0c2c1e-0000-4000-8000-000000000001"
	silent := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	withIncident := uuid.MustParse("22222222-2222-4222-8222-222222222222")
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	stamp := day.Add(10 * time.Hour).Format("2006-01-02 15:04:05")

	for _, repo := range []uuid.UUID{silent, withIncident} {
		for _, statement := range []string{
			"INSERT INTO repos (id, repo, created_at, last_synced, org_id) VALUES ('" + repo.String() + "', 'acme/" + repo.String()[:4] + "', now64(3), now64(3), '" + org + "')",
			"INSERT INTO git_commits (repo_id, hash, author_name, author_email, author_when, committer_when, parents, last_synced, org_id) VALUES ('" + repo.String() + "', 'c1', 'Ada', 'ada@example.com', '" + stamp + "', '" + stamp + "', 1, now64(3), '" + org + "')",
			"INSERT INTO deployments (repo_id, deployment_id, status, deployed_at, last_synced, org_id) VALUES ('" + repo.String() + "', 'd1', 'success', '" + stamp + "', now64(3), '" + org + "')",
			"INSERT INTO deployments (repo_id, deployment_id, status, deployed_at, last_synced, org_id) VALUES ('" + repo.String() + "', 'd2', 'success', '" + stamp + "', now64(3), '" + org + "')",
		} {
			if err := conn.Exec(ctx, statement); err != nil {
				t.Fatal(err)
			}
		}
	}
	opfixture.InsertMapping(ctx, t, conn, opfixture.Mapping{Org: org, ID: "m1", ServiceID: "svc", RepoID: withIncident.String(), Revision: 1, Active: true, At: day})
	opfixture.InsertIncident(ctx, t, conn, opfixture.Incident{Org: org, ID: "I1", ServiceID: "svc", Status: "open", Title: "I1", Revision: 1, StartedAt: day.Add(11 * time.Hour)})

	executor, err := NewRepoUserCommitExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executor.ComputeFamily(ctx,
		Run{ID: "run", OrganizationID: org, TargetDay: day},
		Partition{ID: "partition", RepoIDs: []RepositoryID{RepositoryID(silent.String()), RepositoryID(withIncident.String())}},
	); err != nil {
		t.Fatal(err)
	}

	var incidentColumn uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM system.columns
WHERE database = currentDatabase() AND table = 'repo_metrics_daily' AND name = 'change_failure_rate_incident'`).Scan(&incidentColumn); err != nil {
		t.Fatal(err)
	}
	column := "change_failure_rate"
	if incidentColumn == 1 {
		column = "change_failure_rate_incident"
	}
	read := func(repo uuid.UUID) *float64 {
		t.Helper()
		rows, err := conn.Query(ctx, `SELECT toNullable(`+column+`) FROM repo_metrics_daily
WHERE org_id = ? AND repo_id = ? AND day = ? ORDER BY computed_at DESC LIMIT 1`, org, repo, day)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		if !rows.Next() {
			t.Fatalf("repository %s has no repo_metrics_daily row: the executor measured nothing", repo)
		}
		var value *float64
		if err := rows.Scan(&value); err != nil {
			t.Fatal(err)
		}
		return value
	}
	if got := read(silent); got != nil {
		t.Errorf("repository with deployments and no incident evidence stores %s %v, want NULL (unknown)", column, *got)
	}
	// Control: the same day, the same deployments, plus one incident tied to
	// the repository. The heuristic link producer links it to both deployments.
	if got := read(withIncident); got == nil {
		t.Errorf("repository with an incident tied to it stores %s NULL, want a measured 1", column)
	} else if *got != 1 {
		t.Errorf("repository with an incident tied to it stores %s %v, want a measured 1", column, *got)
	}
}

// Revert rate is not measured: nothing detects a reverted pull request (the
// loader does not read the title the ported rule needs). So a day with merged
// pull requests stores no revert rate, through the production entry point,
// also when one of them is titled as a revert. An unmeasured rate is unknown,
// never 0%. The deprecated change_failure_rate column keeps the 0 the ported
// rule always produced.
func TestRepoUserCommitStoresNoRevertRateForADayWithMergedPullRequests(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	_, conn := opfixture.Start(ctx, t)

	const org = "5d0c2c1e-0000-4000-8000-000000000002"
	repo := uuid.MustParse("33333333-3333-4333-8333-333333333333")
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	stamp := day.Add(10 * time.Hour).Format("2006-01-02 15:04:05")
	for _, statement := range []string{
		"INSERT INTO repos (id, repo, created_at, last_synced, org_id) VALUES ('" + repo.String() + "', 'acme/reverts', now64(3), now64(3), '" + org + "')",
		"INSERT INTO git_commits (repo_id, hash, author_name, author_email, author_when, committer_when, parents, last_synced, org_id) VALUES ('" + repo.String() + "', 'c1', 'Ada', 'ada@example.com', '" + stamp + "', '" + stamp + "', 1, now64(3), '" + org + "')",
		"INSERT INTO git_pull_requests (repo_id, number, title, state, author_name, author_email, created_at, merged_at, last_synced, org_id) VALUES ('" + repo.String() + "', 1, 'Revert \"Add checkout retry\"', 'merged', 'Ada', 'ada@example.com', '" + stamp + "', '" + stamp + "', now64(3), '" + org + "')",
		"INSERT INTO git_pull_requests (repo_id, number, title, state, author_name, author_email, created_at, merged_at, last_synced, org_id) VALUES ('" + repo.String() + "', 2, 'Add checkout retry', 'merged', 'Ada', 'ada@example.com', '" + stamp + "', '" + stamp + "', now64(3), '" + org + "')",
	} {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	executor, err := NewRepoUserCommitExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executor.ComputeFamily(ctx,
		Run{ID: "run", OrganizationID: org, TargetDay: day},
		Partition{ID: "partition", RepoIDs: []RepositoryID{RepositoryID(repo.String())}},
	); err != nil {
		t.Fatal(err)
	}
	var merged uint32
	var revert *float64
	var deprecated float64
	if err := conn.QueryRow(ctx, `SELECT prs_merged, revert_rate, change_failure_rate FROM repo_metrics_daily
WHERE org_id = ? AND repo_id = ? AND day = ? ORDER BY computed_at DESC LIMIT 1`, org, repo, day).Scan(&merged, &revert, &deprecated); err != nil {
		t.Fatal(err)
	}
	if merged != 2 {
		t.Fatalf("prs_merged = %d, want 2: the day must have merges for the test to measure anything", merged)
	}
	if revert != nil {
		t.Errorf("revert_rate = %v for a day with 2 merged pull requests, want NULL (unknown, not measured)", *revert)
	}
	if deprecated != 0 {
		t.Errorf("deprecated change_failure_rate = %v, want the 0 older readers always read", deprecated)
	}
}
