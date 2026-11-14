//go:build integration

package syncadmin

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

// TestWorkItemsDatasetRowsReadIsOrgScoped: the read returns the canonical
// work-items row of each asked integration of the asked org, with its enabled
// flag and options, and nothing of another org or another dataset key.
func TestWorkItemsDatasetRowsReadIsOrgScoped(t *testing.T) {
	v := startCascadeVenue(t, false)
	otherOrg := uuid.NewString()
	pgseed.Org(context.Background(), t, v.pool, otherOrg, "enterprise")
	integration := func(org string) uuid.UUID {
		id := uuid.New()
		v.exec(`INSERT INTO integrations (id, org_id, provider, name, config, is_active, created_at, updated_at)
VALUES ($1, $2, 'jira', $3, '{}'::json, true, now(), now())`, id, org, id.String())
		return id
	}
	row := func(org string, integration uuid.UUID, key string, enabled bool, options string) {
		v.exec(`INSERT INTO integration_datasets (id, org_id, integration_id, dataset_key, is_enabled, options) VALUES ($1, $2, $3, $4, $5, $6::json)`,
			uuid.New(), org, integration, key, enabled, options)
	}
	fetchAll, jql, none := integration(v.org), integration(v.org), integration(v.org)
	foreign := integration(otherOrg)
	row(v.org, fetchAll, "work-items", true, `{"fetch_all": true}`)
	row(v.org, fetchAll, "commits", true, `{"fetch_all": true}`)
	row(v.org, jql, "work-items", false, `{"jql_override": "project = X"}`)
	row(v.org, none, "commits", true, `{}`)
	row(otherOrg, foreign, "work-items", true, `{"fetch_all": true}`)

	got, err := store{pool: v.pool}.workItemsDatasetRows(context.Background(), v.org, []uuid.UUID{fetchAll, jql, none, foreign})
	if err != nil {
		t.Fatal(err)
	}
	byIntegration := map[uuid.UUID]workItemsDatasetRow{}
	for _, r := range got {
		byIntegration[r.IntegrationID] = r
	}
	if len(got) != 2 || len(byIntegration) != 2 {
		t.Fatalf("rows = %+v, want the work-items rows of fetchAll and jql only", got)
	}
	if r := byIntegration[fetchAll]; !r.Enabled || string(r.Options) != `{"fetch_all": true}` {
		t.Errorf("fetch_all row = %+v", r)
	}
	if r := byIntegration[jql]; r.Enabled || string(r.Options) != `{"jql_override": "project = X"}` {
		t.Errorf("jql row = %+v", r)
	}
	if _, leaked := byIntegration[foreign]; leaked {
		t.Error("another org's row was returned")
	}
	other, err := store{pool: v.pool}.workItemsDatasetRows(context.Background(), otherOrg, []uuid.UUID{fetchAll, foreign})
	if err != nil || len(other) != 1 || other[0].IntegrationID != foreign {
		t.Fatalf("other org read = %+v, %v; want only its own row", other, err)
	}
}
