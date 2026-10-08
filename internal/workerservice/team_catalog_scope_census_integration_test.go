//go:build integration

package workerservice

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestTeamCatalogScopeCensusCountsTheOrgsOtherActiveIntegrationsOfOneProvider
// pins the read the ownership close gate trusts: the count of every other
// ACTIVE integration of the same provider (any case, any surrounding space) in
// the same org. An inactive integration, another provider, another org and the
// run's own integration are not counted; a failed read is an error, never zero.
func TestTeamCatalogScopeCensusCountsTheOrgsOtherActiveIntegrationsOfOneProvider(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	pgschema.Apply(ctx, t, pool)

	const (
		org       = "org-scope-census"
		self      = "00000000-0000-4000-8000-0000000000a1"
		mixedCase = "00000000-0000-4000-8000-0000000000a2"
		spaced    = "00000000-0000-4000-8000-0000000000a3"
		inactive  = "00000000-0000-4000-8000-0000000000a4"
		otherKind = "00000000-0000-4000-8000-0000000000a5"
		otherOrg  = "00000000-0000-4000-8000-0000000000a6"
	)
	at := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	integration := func(id, orgID, provider string, active bool) {
		t.Helper()
		if _, err := pool.Exec(ctx, `INSERT INTO integrations (id, org_id, provider, name, config, is_active, created_at, updated_at)
VALUES ($1, $2, $3, $4, '{}'::json, $5, $6, $6)`, id, orgID, provider, "int-"+id[len(id)-2:], active, at); err != nil {
			t.Fatal(err)
		}
	}
	integration(self, org, "gitlab", true)
	integration(mixedCase, org, "GitLab", true)
	integration(spaced, org, " gitlab ", true)
	integration(inactive, org, "gitlab", false)
	integration(otherKind, org, "github", true)
	integration(otherOrg, "org-elsewhere", "gitlab", true)

	census := teamCatalogScopeCensus{pool: pool}
	for _, test := range []struct {
		name, provider, integrationID string
		want                          int
	}{
		{"gitlab, from the run's own integration", "gitlab", self, 2},
		{"gitlab, the provider named in another case", " GitLab ", self, 2},
		{"gitlab, from a sibling: the run's own row is the one left out", "gitlab", mixedCase, 2},
		{"github: the org's only github integration", "github", otherKind, 0},
	} {
		got, err := census.CountActiveSiblingIntegrations(ctx, org, test.provider, test.integrationID)
		if err != nil || got != test.want {
			t.Errorf("%s: count = %d, err = %v; want %d", test.name, got, err, test.want)
		}
	}
	if got, err := census.CountActiveSiblingIntegrations(ctx, org, "gitlab", "not-a-uuid"); err == nil {
		t.Fatalf("a read that fails counted %d, want an error", got)
	}
	for _, missing := range [][3]string{{"", "gitlab", self}, {org, "", self}, {org, "gitlab", ""}} {
		if got, err := census.CountActiveSiblingIntegrations(ctx, missing[0], missing[1], missing[2]); err == nil {
			t.Fatalf("census(%q) counted %d, want an error", missing, got)
		}
	}
}
