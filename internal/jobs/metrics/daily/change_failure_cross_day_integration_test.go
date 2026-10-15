//go:build integration

package daily

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/changefailure"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/opfixture"
)

// A native deployment-incident link (a work_graph_deployment_incident_edges
// row without repo_id) can join a deployment of one UTC day to an incident
// that started on another UTC day. The two-day window must count the
// deployment as failed through the native tier, and the incident once, on its
// own start day (CHAOS-9019).
func TestChangeFailureNativeLinkAcrossUTCDayBoundary(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	_, conn := opfixture.Start(ctx, t)
	const org = "5d0c2c1e-0000-4000-8000-0000000009b1"
	day0 := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	day1 := day0.AddDate(0, 0, 1)

	cases := []struct {
		name          string
		repo          uuid.UUID
		id            string
		deployedAt    time.Time
		incidentStart time.Time
	}{
		{"same day control", uuid.MustParse("bbbbbbbb-9019-4111-8111-111111111111"), "same", day1.Add(1 * time.Hour), day1.Add(2 * time.Hour)},
		{"deployment 23:50Z, incident 00:10Z next day", uuid.MustParse("cccccccc-9019-4111-8111-111111111111"), "forward", day0.Add(23*time.Hour + 50*time.Minute), day1.Add(10 * time.Minute)},
		{"incident 23:50Z, deployment 00:10Z next day", uuid.MustParse("dddddddd-9019-4111-8111-111111111111"), "backward", day1.Add(10 * time.Minute), day0.Add(23*time.Hour + 50*time.Minute)},
	}
	for _, tc := range cases {
		for _, statement := range []string{
			"INSERT INTO repos (id, repo, created_at, last_synced, org_id) VALUES ('" + tc.repo.String() + "', 'acme/" + tc.id + "', now64(3), now64(3), '" + org + "')",
			"INSERT INTO deployments (repo_id, deployment_id, status, deployed_at, last_synced, org_id) VALUES ('" + tc.repo.String() + "', 'd-" + tc.id + "', 'success', '" + tc.deployedAt.Format("2006-01-02 15:04:05") + "', now64(3), '" + org + "')",
			fmt.Sprintf("INSERT INTO work_graph_deployment_incident_edges (edge_id, org_id, deployment_id, incident_id, provider, repo_id, confidence, source, evidence, observed_at) VALUES ('e-%s', '%s', 'd-%s', 'I-%s', 'github', NULL, 1, 'native', '{}', now64(3))", tc.id, org, tc.id, tc.id),
		} {
			if err := conn.Exec(ctx, statement); err != nil {
				t.Fatal(err)
			}
		}
		// No service: the incident has no direct tie.
		opfixture.InsertIncident(ctx, t, conn, opfixture.Incident{Org: org, ID: "I-" + tc.id, ServiceID: "", Status: "open", Title: tc.id, Revision: 1, StartedAt: tc.incidentStart})

		computeChangeFailureDay(ctx, t, conn, org, day0, day1.Add(30*time.Hour), tc.repo)
		computeChangeFailureDay(ctx, t, conn, org, day1, day1.Add(31*time.Hour), tc.repo)
		// A day computed again must give the same counts, not add to them.
		computeChangeFailureDay(ctx, t, conn, org, day0, day1.Add(32*time.Hour), tc.repo)
		computeChangeFailureDay(ctx, t, conn, org, day1, day1.Add(33*time.Hour), tc.repo)

		first, _ := newestChangeFailure(ctx, t, conn, org, tc.repo, day0)
		second, _ := newestChangeFailure(ctx, t, conn, org, tc.repo, day1)
		window := first.Add(second)
		out := changefailure.Rate(window)
		t.Logf("%s: day0 %+v day1 %+v window rate %v state %q tier %q", tc.name, first, second, out.Value, out.State, out.LinkTier)
		if window.Deployments != 1 || window.IncidentsViaDeployment != 1 || window.FailedNative != 1 || window.FailedHeuristic != 0 || window.FailedStatus != 0 {
			t.Errorf("%s: window counts %+v, want 1 deployment, 1 incident via deployment, 1 native failed deployment", tc.name, window)
		}
		if out.Value == nil || *out.Value != 1 || out.State != changefailure.StateMeasured || out.LinkTier != changefailure.TierNative {
			t.Errorf("%s: window rate %v state %q tier %q, want a measured 1 with tier native", tc.name, out.Value, out.State, out.LinkTier)
		}
	}
}

// An incident with no start time belongs to no day: a native link to it is no
// evidence for any day, as before the cross-day read.
func TestChangeFailureNativeLinkToIncidentWithoutStartTimeCountsNothing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	_, conn := opfixture.Start(ctx, t)
	const org = "5d0c2c1e-0000-4000-8000-0000000009b2"
	repo := uuid.MustParse("eeeeeeee-9019-4111-8111-111111111111")
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	for _, statement := range []string{
		"INSERT INTO repos (id, repo, created_at, last_synced, org_id) VALUES ('" + repo.String() + "', 'acme/nostart', now64(3), now64(3), '" + org + "')",
		"INSERT INTO deployments (repo_id, deployment_id, status, deployed_at, last_synced, org_id) VALUES ('" + repo.String() + "', 'd-nostart', 'success', '" + day.Add(10*time.Hour).Format("2006-01-02 15:04:05") + "', now64(3), '" + org + "')",
		fmt.Sprintf("INSERT INTO work_graph_deployment_incident_edges (edge_id, org_id, deployment_id, incident_id, provider, repo_id, confidence, source, evidence, observed_at) VALUES ('e-nostart', '%s', 'd-nostart', 'I-nostart', 'github', NULL, 1, 'native', '{}', now64(3))", org),
		fmt.Sprintf(`INSERT INTO operational_incidents
(org_id, provider, provider_instance_id, source_entity_type, external_id, source_version_at,
 source_revision, source_conflict_key, ingest_revision, ordering_contract, id, observed_at, last_synced,
 normalized_status, title, started_at, service_id, is_deleted)
VALUES ('%s','pagerduty','acme','incident','I-nostart', now64(6), 1, 'k1', 1, 2, 'I-nostart', now64(6), now64(6), 'open', 'nostart', NULL, '', 0)`, org),
	} {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	computeChangeFailureDay(ctx, t, conn, org, day, day.Add(30*time.Hour), repo)
	got, _ := newestChangeFailure(ctx, t, conn, org, repo, day)
	if got != (changefailure.Counts{Deployments: 1}) {
		t.Fatalf("counts %+v, want one deployment and no failure, no incident", got)
	}
}
