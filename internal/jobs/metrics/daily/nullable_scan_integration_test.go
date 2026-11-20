//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/opfixture"
)

// clickhouse-go v2.50+ refuses a NULL scanned into a destination that cannot
// hold NULL. Each read here scans a Nullable column into a plain destination,
// and its WHERE drops the NULL row before the scan, so it holds on every
// driver version with a NULL row stored.
func TestFilteredReadsNeverScanANullIntoAPlainDestination(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	_, conn := opfixture.Start(ctx, t)

	const org = "00000000-0000-4000-8000-0000000142f1"
	repo := uuid.MustParse("00000000-0000-4000-8000-0000000142f2")
	day := time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)
	started := day.Add(3 * time.Hour)
	for _, statement := range []string{
		"INSERT INTO repos (id, repo, created_at, last_synced, org_id) VALUES ('" + repo.String() + "', 'acme/api', now64(3), now64(3), '" + org + "')",
		`INSERT INTO operational_incidents (org_id, provider, provider_instance_id, source_entity_type, external_id, source_version_at,
 source_revision, source_conflict_key, ingest_revision, ordering_contract, id, observed_at, last_synced, normalized_status, title, started_at, service_id, is_deleted)
 VALUES ('` + org + `', 'pagerduty', 'acme', 'incident', 'I-null', now64(6), 1, 'k1', 1, 2, 'I-null', now64(6), now64(6), 'open', 'I-null', NULL, 'svc', 0)`,
		`INSERT INTO operational_incidents (org_id, provider, provider_instance_id, source_entity_type, external_id, source_version_at,
 source_revision, source_conflict_key, ingest_revision, ordering_contract, id, observed_at, last_synced, normalized_status, title, started_at, service_id, is_deleted)
 VALUES ('` + org + `', 'pagerduty', 'acme', 'incident', 'I-unmapped', now64(6), 1, 'k1', 1, 2, 'I-unmapped', now64(6), now64(6), 'open', 'I-unmapped', NULL, 'svc-unmapped', 0)`,
		`INSERT INTO work_graph_deployment_incident_edges (org_id, repo_id, incident_id, deployment_id, source)
 VALUES ('` + org + `', NULL, 'I-unmapped', 'dep-1', 'explicit')`,
		`INSERT INTO work_graph_issue_pr (org_id, repo_id, work_item_id, pr_number, provenance, last_synced)
 VALUES ('` + org + `', '` + repo.String() + `', 'gh:acme/api#8', 7, 'native', now64(3))`,
		`INSERT INTO work_item_team_attributions (org_id, repo_id, work_item_id, team_id, is_primary, source, computed_at)
 VALUES ('` + org + `', '` + repo.String() + `', 'gh:acme/api#8', 'team-a', 1, 'native_team', now64(3))`,
		`INSERT INTO deployments (org_id, repo_id, deployment_id, last_synced) VALUES ('` + org + `', '` + repo.String() + `', 'dep-1', now64(3))`,
		`INSERT INTO git_pull_requests (org_id, repo_id, number, merged_at, created_at, last_synced)
 VALUES ('` + org + `', '` + repo.String() + `', 7, NULL, now64(3), now64(3))`,
		`INSERT INTO work_items (org_id, repo_id, work_item_id) VALUES ('` + org + `', '` + repo.String() + `', 'gh:acme/api#7')`,
		`INSERT INTO work_item_team_attributions (org_id, repo_id, work_item_id, team_id, is_primary, source, computed_at)
 VALUES ('` + org + `', '` + repo.String() + `', 'gh:acme/api#7', NULL, 1, 'unassigned', now64(3))`,
	} {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("seed: %v\n%s", err, statement)
		}
	}
	opfixture.InsertMapping(ctx, t, conn, opfixture.Mapping{Org: org, ID: "m-1", ServiceID: "svc", RepoID: repo.String(), Revision: 1, Active: true, At: started})

	t.Run("LoadIncidentsStarted", func(t *testing.T) {
		rows, err := LoadIncidentsStarted(ctx, conn, org, []uuid.UUID{repo}, day, day.Add(24*time.Hour), day.Add(24*time.Hour), nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 0 {
			t.Fatalf("rows = %+v, want none: the incident has no start", rows)
		}
	})
	t.Run("LoadViaDeploymentIncidentLinks", func(t *testing.T) {
		if _, err := LoadViaDeploymentIncidentLinks(ctx, conn, org, []uuid.UUID{repo}, day, day.Add(24*time.Hour), day.Add(24*time.Hour)); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("LoadAIImpactRepoMergedPRTeams", func(t *testing.T) {
		if _, err := LoadAIImpactRepoMergedPRTeams(ctx, conn, org, []uuid.UUID{repo}, day, day.Add(24*time.Hour)); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("loadWorkItemScopeTeamSets", func(t *testing.T) {
		teams, err := loadWorkItemScopeTeamSets(ctx, conn, org, nil, false, false)
		if err != nil {
			t.Fatal(err)
		}
		if len(teams) != 0 {
			t.Fatalf("teams = %v, want none: the only attribution has no team", teams)
		}
	})
}

// LoadWorkGraphEdgeRepoProviders scans tupleElement(latest, 3), the String
// repos.provider, into a string; a NULL settings in the same tuple does not
// make the element NULL.
func TestRepoProvidersReadAProviderBesideNullSettings(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	_, conn := opfixture.Start(ctx, t)
	const org = "00000000-0000-4000-8000-0000000142f3"
	repo := uuid.MustParse("00000000-0000-4000-8000-0000000142f4")
	if err := conn.Exec(ctx, "INSERT INTO repos (id, repo, settings, provider, created_at, last_synced, org_id) VALUES ('"+repo.String()+"', 'acme/api', NULL, 'github', now64(3), now64(3), '"+org+"')"); err != nil {
		t.Fatal(err)
	}
	providers, err := LoadWorkGraphEdgeRepoProviders(ctx, conn, org, "github", nil)
	if err != nil {
		t.Fatal(err)
	}
	if providers[repo.String()] != "github" {
		t.Fatalf("providers = %v, want github for the scanned repo", providers)
	}
}
