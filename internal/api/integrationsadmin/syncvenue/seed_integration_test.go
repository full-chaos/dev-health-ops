//go:build integration

package syncvenue

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// integ is one seeded integration with its planner-managed parent and sources.
type integ struct {
	id, cfg  uuid.UUID
	srcs     []uuid.UUID // enabled sources, then the disabled one
	provider string
}

type ids struct {
	orgA, orgB                          uuid.UUID
	adminA, memberA, adminB, adminNoOrg uuid.UUID
	cred, credB                         uuid.UUID
	incremental, full, subset, emptySrc integ
	datasets, nullBody                  integ
	inactive, noConfig, unmanaged       integ
	pending, emptyData                  integ
	otherOrg                            integ
}

func newIDs() ids {
	v := ids{}
	for _, target := range []*uuid.UUID{&v.orgA, &v.orgB, &v.adminA, &v.memberA, &v.adminB, &v.adminNoOrg, &v.cred, &v.credB} {
		*target = uuid.New()
	}
	for _, target := range []*integ{&v.incremental, &v.full, &v.subset, &v.emptySrc, &v.datasets, &v.nullBody, &v.inactive, &v.noConfig, &v.unmanaged, &v.otherOrg, &v.pending, &v.emptyData} {
		target.id, target.cfg = uuid.New(), uuid.New()
		target.srcs = []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
		target.provider = "github"
	}
	return v
}

// seed writes two orgs, their users, a GitHub credential (encrypted by the
// Python plane) and the integrations each request starts from. Every
// integration has its own configuration: a hand-off occurrence is named by its
// configuration and the pinned instant, so one integration takes one trigger.
func seed(t *testing.T, ctx context.Context, admin *pgxpool.Pool, venue *venueoracle.Venue, v ids) {
	t.Helper()
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := admin.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, sql)
		}
	}
	const at = "2026-01-01 00:00:00+00"
	exec(`INSERT INTO organizations (id, slug, name, settings, tier, is_active, created_at, updated_at) VALUES
($1, 'sync-a', 'sync-a', '{}', 'enterprise', true, $3, $3), ($2, 'sync-b', 'sync-b', '{}', 'enterprise', true, $3, $3)`, v.orgA, v.orgB, at)
	for _, user := range []struct {
		id, org uuid.UUID
		email   string
		role    string
	}{{v.adminA, v.orgA, "sync-admin-a@example.com", "admin"}, {v.memberA, v.orgA, "sync-member-a@example.com", "member"},
		{v.adminB, v.orgB, "sync-admin-b@example.com", "admin"}} {
		exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, true, true, false, 0, $3, $3)`, user.id, user.email, at)
		exec(`INSERT INTO memberships (id, user_id, org_id, role, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $5)`,
			uuid.New(), user.id, user.org, user.role, at)
	}
	exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'sync-noorg@example.com', true, true, false, 0, $2, $2)`, v.adminNoOrg, at)

	encoded, _ := json.Marshal(map[string]any{"token": "venue-token"})
	raw := venue.CallPython(t, venueoracle.PythonCall{Target: "dev_health_ops.core.encryption:encrypt_value", Args: []any{string(encoded)}})
	var ciphertext string
	if err := json.Unmarshal(raw[0], &ciphertext); err != nil {
		t.Fatal(err)
	}
	for _, credential := range []struct{ id, org uuid.UUID }{{v.cred, v.orgA}, {v.credB, v.orgB}} {
		exec(`INSERT INTO integration_credentials (id, org_id, provider, name, is_active, credentials_encrypted, config, created_at, updated_at)
VALUES ($1, $2, 'github', 'github cred', true, $3, '{}'::json, $4, $4)`, credential.id, credential.org.String(), ciphertext, at)
	}

	// build writes one integration: a planner-managed parent (unless told
	// otherwise), three sources (two enabled, one not, tagged for the parent)
	// and the commits and pull-requests datasets.
	build := func(i integ, org, credential uuid.UUID, name string, active bool, config string, managed bool) {
		exec(`INSERT INTO integrations (id, org_id, provider, credential_id, name, config, is_active, created_at, updated_at)
VALUES ($1, $2, 'github', $3, $4, '{}'::json, $5, $6, $6)`, i.id, org.String(), credential, name, active, at)
		if config != "none" {
			exec(`INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets, sync_options, is_active, planner_managed, integration_id, created_at, updated_at)
VALUES ($1, $2, $3, 'github', '["git"]'::json, '{}'::json, true, $4, $5, $6, $6)`, i.cfg, org.String(), "cfg-"+name, managed, i.id, at)
		}
		for index, source := range i.srcs {
			exec(`INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata, is_enabled, discovered_at, last_seen_at)
VALUES ($1, $2, $3, 'github', 'repository', $4, $5, $4, $6::json, $7, $8, $8)`, source, org.String(), i.id,
				"acme/"+name+"-"+string(rune('a'+index)), name+"-"+string(rune('a'+index)),
				`{"planner_managed_sync_config_id": "`+i.cfg.String()+`"}`, index < 2, at)
		}
		for _, dataset := range []string{"commits", "prs"} {
			exec(`INSERT INTO integration_datasets (id, org_id, integration_id, dataset_key, is_enabled, options) VALUES ($1, $2, $3, $4, true, '{}'::json)`,
				uuid.New(), org.String(), i.id, dataset)
		}
	}
	build(v.incremental, v.orgA, v.cred, "incremental", true, "", true)
	// An enabled source no configuration tagged: Python's planner takes every
	// enabled source of the integration, the scheduler alone takes the tagged.
	exec(`INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata, is_enabled, discovered_at, last_seen_at)
VALUES ($1, $2, $3, 'github', 'repository', 'acme/incremental-untagged', 'incremental-untagged', 'acme/incremental-untagged', '{}'::json, true, $4, $4)`,
		uuid.New(), v.orgA.String(), v.incremental.id, at)
	build(v.full, v.orgA, v.cred, "full", true, "", true)
	build(v.subset, v.orgA, v.cred, "subset", true, "", true)
	build(v.emptySrc, v.orgA, v.cred, "emptysrc", true, "", true)
	build(v.datasets, v.orgA, v.cred, "datasets", true, "", true)
	build(v.nullBody, v.orgA, v.cred, "nullbody", true, "", true)
	build(v.inactive, v.orgA, v.cred, "inactive", false, "", true)
	build(v.noConfig, v.orgA, v.cred, "noconfig", true, "none", true)
	build(v.unmanaged, v.orgA, v.cred, "unmanaged", true, "", false)
	build(v.emptyData, v.orgA, v.cred, "emptydata", true, "", true)
	build(v.pending, v.orgA, v.cred, "pending", true, "", true)
	build(v.otherOrg, v.orgB, v.credB, "otherorg", true, "", true)
}
