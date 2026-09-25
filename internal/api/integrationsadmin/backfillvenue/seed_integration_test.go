//go:build integration

package backfillvenue

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// integ is one seeded integration with its configuration and sources.
type integ struct {
	id, cfg uuid.UUID
	srcs    []uuid.UUID // two enabled, then one disabled
}

// integration names: one trigger per integration (a hand-off occurrence is
// named by its configuration and the pinned instant).
var (
	sameNames = []string{"flat", "selector", "flatsubset", "selectorempty", "naive", "offset", "dateonly", "chunkedges", "untagged", "number", "selectornull", "upper", "otherorg"}
	// divergingNames are refused by the hand-off (Python plans them).
	refusedNames = []string{"inactive", "noconfig", "unmanaged"}
	// windowNames are the invalid windows: Python and Go both answer 400.
	windowNames = []string{"reversed", "equal"}
	otherNames  = []string{"pending"}
)

type ids struct {
	orgA, orgB                          uuid.UUID
	adminA, memberA, adminB, adminNoOrg uuid.UUID
	cred, credB                         uuid.UUID
	by                                  map[string]integ
}

func (v ids) get(name string) integ { return v.by[name] }

func newIDs() ids {
	v := ids{by: map[string]integ{}}
	for _, target := range []*uuid.UUID{&v.orgA, &v.orgB, &v.adminA, &v.memberA, &v.adminB, &v.adminNoOrg, &v.cred, &v.credB} {
		*target = uuid.New()
	}
	for _, group := range [][]string{sameNames, refusedNames, windowNames, otherNames} {
		for _, name := range group {
			v.by[name] = integ{id: uuid.New(), cfg: uuid.New(), srcs: []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}}
		}
	}
	return v
}

// seed writes two orgs, their users, a GitHub credential (encrypted by the
// Python plane) and one integration per request, each with a planner-managed
// parent, three sources (two enabled and tagged, one disabled) and the commits
// and prs datasets.
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
($1, 'bf-a', 'bf-a', '{}', 'enterprise', true, $3, $3), ($2, 'bf-b', 'bf-b', '{}', 'enterprise', true, $3, $3)`, v.orgA, v.orgB, at)
	for _, user := range []struct {
		id, org uuid.UUID
		email   string
		role    string
	}{{v.adminA, v.orgA, "bf-admin-a@example.com", "admin"}, {v.memberA, v.orgA, "bf-member-a@example.com", "member"},
		{v.adminB, v.orgB, "bf-admin-b@example.com", "admin"}} {
		exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, true, true, false, 0, $3, $3)`, user.id, user.email, at)
		exec(`INSERT INTO memberships (id, user_id, org_id, role, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $5)`,
			uuid.New(), user.id, user.org, user.role, at)
	}
	exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'bf-noorg@example.com', true, true, false, 0, $2, $2)`, v.adminNoOrg, at)

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

	build := func(name string, org, credential uuid.UUID, active, hasConfig, managed bool) {
		i := v.by[name]
		exec(`INSERT INTO integrations (id, org_id, provider, credential_id, name, config, is_active, created_at, updated_at)
VALUES ($1, $2, 'github', $3, $4, '{}'::json, $5, $6, $6)`, i.id, org.String(), credential, name, active, at)
		if hasConfig {
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
	for _, group := range [][]string{sameNames, windowNames, otherNames} {
		for _, name := range group {
			org, credential := v.orgA, v.cred
			if name == "otherorg" {
				org, credential = v.orgB, v.credB
			}
			build(name, org, credential, true, true, true)
		}
	}
	build("inactive", v.orgA, v.cred, false, true, true)
	build("noconfig", v.orgA, v.cred, true, false, true)
	build("unmanaged", v.orgA, v.cred, true, true, false)
	// An enabled source no configuration tagged: Python's planner takes every
	// enabled source of the integration, the scheduler alone takes the tagged.
	exec(`INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata, is_enabled, discovered_at, last_seen_at)
VALUES ($1, $2, $3, 'github', 'repository', 'acme/untagged-x', 'untagged-x', 'acme/untagged-x', '{}'::json, true, $4, $4)`,
		uuid.New(), v.orgA.String(), v.by["untagged"].id, at)
}
