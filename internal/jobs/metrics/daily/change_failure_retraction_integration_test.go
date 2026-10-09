//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/changefailure"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/opfixture"
)

// newestChangeFailure reads the newest stored counts of one repository and day
// and the number of stored versions of that key.
func newestChangeFailure(ctx context.Context, t *testing.T, conn driver.Conn, org string, repo uuid.UUID, day time.Time) (changefailure.Counts, uint64) {
	t.Helper()
	var versions uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM repo_change_failure_daily WHERE org_id = ? AND repo_id = ? AND day = ?", org, repo, day).Scan(&versions); err != nil {
		t.Fatal(err)
	}
	rows, err := conn.Query(ctx, `SELECT toUInt64(deployments_count), toUInt64(failed_deployments_native), toUInt64(failed_deployments_heuristic),
		toUInt64(incidents_direct), toUInt64(incidents_via_deployment)
FROM repo_change_failure_daily WHERE org_id = ? AND repo_id = ? AND day = ? ORDER BY computed_at DESC LIMIT 1`, org, repo, day)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var counts changefailure.Counts
	if rows.Next() {
		if err := rows.Scan(&counts.Deployments, &counts.FailedNative, &counts.FailedHeuristic, &counts.IncidentsDirect, &counts.IncidentsViaDeployment); err != nil {
			t.Fatal(err)
		}
	}
	return counts, versions
}

func computeChangeFailureDay(ctx context.Context, t *testing.T, conn driver.Conn, org string, day, at time.Time, repos ...uuid.UUID) {
	t.Helper()
	executor, err := NewRepoUserCommitExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	executor.nowUTC = func() time.Time { return at }
	ids := make([]RepositoryID, 0, len(repos))
	for _, repo := range repos {
		ids = append(ids, RepositoryID(repo.String()))
	}
	if _, err := executor.ComputeFamily(ctx, Run{ID: "run", OrganizationID: org, TargetDay: day}, Partition{ID: "partition", RepoIDs: ids}); err != nil {
		t.Fatal(err)
	}
}

// A day computed again after its evidence is gone must not keep the old
// counts as its newest row. repo_change_failure_daily keeps the newest row per
// key and cannot drop one, so the run writes a row of zeros for a repository
// that has a stored row and nothing to count any more.
//
//	gone       1 incident, no deployment; the incident is then deleted
//	deploys    2 deployments and a commit, 1 incident; the incident is then deleted
//	moved-from 1 incident through a service mapping; the mapping then moves the
//	           service to moved-to
//	never      nothing to count in either run
func TestRecomputeReplacesChangeFailureCountsWhoseEvidenceIsGone(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	_, conn := opfixture.Start(ctx, t)

	const org = "5d0c2c1e-0000-4000-8000-0000000000a1"
	gone := uuid.MustParse("aaaaaaaa-1111-4111-8111-111111111111")
	deploys := uuid.MustParse("aaaaaaaa-2222-4222-8222-222222222222")
	movedFrom := uuid.MustParse("aaaaaaaa-3333-4333-8333-333333333333")
	movedTo := uuid.MustParse("aaaaaaaa-4444-4444-8444-444444444444")
	never := uuid.MustParse("aaaaaaaa-5555-4555-8555-555555555555")
	all := []uuid.UUID{gone, deploys, movedFrom, movedTo, never}
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	stamp := day.Add(10 * time.Hour).Format("2006-01-02 15:04:05")

	for _, repo := range all {
		if err := conn.Exec(ctx, "INSERT INTO repos (id, repo, created_at, last_synced, org_id) VALUES ('"+repo.String()+"', 'acme/"+repo.String()[9:13]+"', now64(3), now64(3), '"+org+"')"); err != nil {
			t.Fatal(err)
		}
	}
	for _, statement := range []string{
		"INSERT INTO git_commits (repo_id, hash, author_name, author_email, author_when, committer_when, parents, last_synced, org_id) VALUES ('" + deploys.String() + "', 'c1', 'Ada', 'ada@example.com', '" + stamp + "', '" + stamp + "', 1, now64(3), '" + org + "')",
		"INSERT INTO deployments (repo_id, deployment_id, status, deployed_at, last_synced, org_id) VALUES ('" + deploys.String() + "', 'd1', 'success', '" + stamp + "', now64(3), '" + org + "')",
		"INSERT INTO deployments (repo_id, deployment_id, status, deployed_at, last_synced, org_id) VALUES ('" + deploys.String() + "', 'd2', 'success', '" + stamp + "', now64(3), '" + org + "')",
	} {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	started := day.Add(11 * time.Hour)
	opfixture.InsertMapping(ctx, t, conn, opfixture.Mapping{Org: org, ID: "m-gone", ServiceID: "svc-gone", RepoID: gone.String(), Revision: 1, Active: true, At: day})
	opfixture.InsertMapping(ctx, t, conn, opfixture.Mapping{Org: org, ID: "m-deploys", ServiceID: "svc-deploys", RepoID: deploys.String(), Revision: 1, Active: true, At: day})
	opfixture.InsertMapping(ctx, t, conn, opfixture.Mapping{Org: org, ID: "m-moved", ServiceID: "svc-moved", RepoID: movedFrom.String(), Revision: 1, Active: true, At: day})
	opfixture.InsertIncident(ctx, t, conn, opfixture.Incident{Org: org, ID: "I-gone", ServiceID: "svc-gone", Status: "open", Title: "gone", Revision: 1, StartedAt: started})
	opfixture.InsertIncident(ctx, t, conn, opfixture.Incident{Org: org, ID: "I-deploys", ServiceID: "svc-deploys", Status: "open", Title: "deploys", Revision: 1, StartedAt: started})
	opfixture.InsertIncident(ctx, t, conn, opfixture.Incident{Org: org, ID: "I-moved", ServiceID: "svc-moved", Status: "open", Title: "moved", Revision: 1, StartedAt: started})

	incidentValue := func(repo uuid.UUID) *float64 {
		t.Helper()
		var value *float64
		if err := conn.QueryRow(ctx, `SELECT change_failure_rate_incident FROM repo_metrics_daily
WHERE org_id = ? AND repo_id = ? AND day = ? ORDER BY computed_at DESC LIMIT 1`, org, repo, day).Scan(&value); err != nil {
			t.Fatalf("repo_metrics_daily row of %s: %v", repo, err)
		}
		return value
	}

	// Run 1: the evidence is present.
	computeChangeFailureDay(ctx, t, conn, org, day, day.Add(30*time.Hour), all...)
	for repo, want := range map[uuid.UUID]changefailure.Counts{
		gone:      {IncidentsDirect: 1},
		deploys:   {Deployments: 2, FailedHeuristic: 2, IncidentsDirect: 1},
		movedFrom: {IncidentsDirect: 1},
	} {
		if got, versions := newestChangeFailure(ctx, t, conn, org, repo, day); got != want || versions != 1 {
			t.Fatalf("control, run 1: %s = %+v in %d version(s), want %+v in 1", repo, got, versions, want)
		}
	}
	for _, repo := range []uuid.UUID{movedTo, never} {
		if _, versions := newestChangeFailure(ctx, t, conn, org, repo, day); versions != 0 {
			t.Fatalf("control, run 1: %s has %d row(s) with nothing to count, want none", repo, versions)
		}
	}
	if value := incidentValue(deploys); value == nil || *value != 1 {
		t.Fatalf("control, run 1: repo_metrics_daily.change_failure_rate_incident of the deploying repository = %v, want 1", value)
	}

	// The incidents of gone and deploys are deleted at the source (a newer
	// version with is_deleted = 1), and the service of the third incident is
	// mapped to another repository (a newer version of the mapping).
	opfixture.InsertIncident(ctx, t, conn, opfixture.Incident{Org: org, ID: "I-gone", ServiceID: "svc-gone", Status: "open", Title: "gone", Revision: 2, Deleted: true, StartedAt: started})
	opfixture.InsertIncident(ctx, t, conn, opfixture.Incident{Org: org, ID: "I-deploys", ServiceID: "svc-deploys", Status: "open", Title: "deploys", Revision: 2, Deleted: true, StartedAt: started})
	opfixture.InsertMapping(ctx, t, conn, opfixture.Mapping{Org: org, ID: "m-moved", ServiceID: "svc-moved", RepoID: movedTo.String(), Revision: 2, Active: true, At: day})

	// Run 2: the same day, computed again.
	computeChangeFailureDay(ctx, t, conn, org, day, day.Add(40*time.Hour), all...)
	for name, tc := range map[string]struct {
		repo     uuid.UUID
		want     changefailure.Counts
		versions uint64
	}{
		"repository whose only incident was deleted":           {gone, changefailure.Counts{}, 2},
		"deploying repository whose incident was deleted":      {deploys, changefailure.Counts{Deployments: 2}, 2},
		"repository the service mapping moved away from":       {movedFrom, changefailure.Counts{}, 2},
		"repository the service mapping moved to":              {movedTo, changefailure.Counts{IncidentsDirect: 1}, 1},
		"repository that never had anything to count (no row)": {never, changefailure.Counts{}, 0},
	} {
		if got, versions := newestChangeFailure(ctx, t, conn, org, tc.repo, day); got != tc.want || versions != tc.versions {
			t.Errorf("run 2, %s: newest counts %+v in %d version(s), want %+v in %d", name, got, versions, tc.want, tc.versions)
		}
	}
	// The one-day value on repo_metrics_daily follows: the deploying
	// repository has deployments and no incident evidence now.
	if value := incidentValue(deploys); value != nil {
		t.Errorf("run 2: repo_metrics_daily.change_failure_rate_incident of the deploying repository = %v, want NULL (unknown)", *value)
	}
}
