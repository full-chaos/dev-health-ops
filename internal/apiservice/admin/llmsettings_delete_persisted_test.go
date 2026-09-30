//go:build integration

package admin_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// TestLLMSettingsDeleteClearsDerivedRows is CHAOS-6975's persisted-state proof
// (real Postgres, real HTTP route). DELETE /llm-settings must remove the
// credential rows AND the rows derived from them (the readiness record and the
// per-role certification rows), and nothing else: every other key family is
// one executed cell below. Go-only proof: Python's body is the Go-served
// refusal, and the Python leak this closes is the recorded defect itself.
func TestLLMSettingsDeleteClearsDerivedRows(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-llm-delete-32-bytes!"

	orgA, orgB, orgOrphan := uuid.New(), uuid.New(), uuid.New()
	adminUserID := uuid.New()

	type cell struct {
		category, key string
		gone          bool // true: DELETE must remove it; false: it must survive
	}
	// Every family the route touches or must leave alone.
	cells := []cell{
		{"llm", "provider", true},
		{"llm", "model", true},
		{"llm", "api_key", true},
		{"llm", "base_url", true},
		{"llm", "concurrency", true},
		{"llm", "ask_dev_agent_readiness", true},
		{"llm", "ask_dev_role_certification_profile:legacy_agent", true},
		{"llm", "ask_dev_role_certification_profile:intent_classification", true},
		{"llm", "platform_ask_dev_role_certification_profile:legacy_agent", false},
		{"llm", "ask_dev_role_certification_profile", false}, // no role suffix: not a per-role row
		{"llm", "unrelated_key", false},
		{"llm_budget", "limit_micro_usd", false},
		{"general", "site_name", false},
		{"ask_dev", "ask_dev_agent_readiness", false}, // same key, other category
	}

	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root:   root,
		JWTKey: jwtKey,
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, v *venueoracle.Venue) map[string]map[string]any {
			t.Helper()
			exec := func(sql string, args ...any) {
				t.Helper()
				if _, err := admin.Exec(ctx, sql, args...); err != nil {
					t.Fatalf("seed: %v\n%s", err, sql)
				}
			}
			for slug, id := range map[string]uuid.UUID{"a": orgA, "b": orgB, "orphan": orgOrphan} {
				exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, $2, 'team', 'stripe', true, now(), now())`, id, "llmdelete-"+slug)
			}
			exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, true, true, false, 0, now(), now())`, adminUserID, "llmdelete-admin@example.com")
			row := func(org uuid.UUID, category, key string) {
				exec(`INSERT INTO settings (id, org_id, category, key, value, is_encrypted, description, created_at, updated_at)
VALUES ($1, $2, $3, $4, 'v', false, NULL, now(), now())`, uuid.New(), org.String(), category, key)
			}
			for _, org := range []uuid.UUID{orgA, orgB} {
				for _, c := range cells {
					row(org, c.category, c.key)
				}
			}
			// Orphan: derived rows left by an earlier delete, no credentials.
			row(orgOrphan, "llm", "ask_dev_agent_readiness")
			row(orgOrphan, "llm", "ask_dev_role_certification_profile:legacy_agent")
			row(orgOrphan, "general", "site_name")

			tokens := map[string]map[string]any{}
			for slug, org := range map[string]uuid.UUID{"a": orgA, "b": orgB, "orphan": orgOrphan} {
				tokens[slug] = map[string]any{"user_id": adminUserID.String(), "email": "llmdelete-admin@example.com", "org_id": org.String(), "role": "admin"}
			}
			return tokens
		},
	})
	base, pool := startGoServer(t, ctx, venue, jwtKey)
	auth := func(name string) map[string]string {
		return map[string]string{"Authorization": "Bearer " + venue.Tokens[name]}
	}
	exists := func(org uuid.UUID, category, key string) bool {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM settings WHERE org_id = $1 AND category = $2 AND key = $3`, org.String(), category, key).Scan(&n); err != nil {
			t.Fatalf("count %s/%s: %v", category, key, err)
		}
		return n > 0
	}

	t.Run("delete with credentials clears credential and derived rows only", func(t *testing.T) {
		resp := doRouteRequest(t, base, "DELETE", "/api/v1/admin/llm-settings", auth("a"))
		if resp.status != 200 {
			t.Fatalf("DELETE: status=%d body=%s", resp.status, resp.body)
		}
		for _, c := range cells {
			if got := exists(orgA, c.category, c.key); got == c.gone {
				t.Errorf("%s/%s: present=%v after DELETE, want present=%v", c.category, c.key, got, !c.gone)
			}
		}
	})

	t.Run("another org is untouched", func(t *testing.T) {
		for _, c := range cells {
			if !exists(orgB, c.category, c.key) {
				t.Errorf("org B lost %s/%s", c.category, c.key)
			}
		}
	})

	t.Run("the generic settings delete cannot reach llm rows", func(t *testing.T) {
		for _, path := range []string{
			"/api/v1/admin/settings/llm/ask_dev_agent_readiness",
			"/api/v1/admin/settings/llm/ask_dev_role_certification_profile:legacy_agent",
			"/api/v1/admin/settings/llm/api_key",
			"/api/v1/admin/settings/llm_budget/limit_micro_usd",
		} {
			resp := doRouteRequest(t, base, "DELETE", path, auth("b"))
			if resp.status < 400 {
				t.Errorf("DELETE %s: status=%d body=%s, want a refusal", path, resp.status, resp.body)
			}
		}
		for _, c := range cells {
			if !exists(orgB, c.category, c.key) {
				t.Errorf("org B lost %s/%s to a generic settings delete", c.category, c.key)
			}
		}
	})

	t.Run("orphan derived rows with no credentials: 404 and cleared", func(t *testing.T) {
		resp := doRouteRequest(t, base, "DELETE", "/api/v1/admin/llm-settings", auth("orphan"))
		if resp.status != 404 {
			t.Fatalf("DELETE orphan: status=%d body=%s, want 404", resp.status, resp.body)
		}
		if exists(orgOrphan, "llm", "ask_dev_agent_readiness") || exists(orgOrphan, "llm", "ask_dev_role_certification_profile:legacy_agent") {
			t.Errorf("orphan derived rows survived the DELETE")
		}
		if !exists(orgOrphan, "general", "site_name") {
			t.Errorf("orphan org lost general/site_name")
		}
	})

	t.Run("second delete is 404", func(t *testing.T) {
		resp := doRouteRequest(t, base, "DELETE", "/api/v1/admin/llm-settings", auth("a"))
		if resp.status != 404 {
			t.Fatalf("second DELETE: status=%d body=%s, want 404", resp.status, resp.body)
		}
	})

	venueoracle.WriteGoOnlyProof(t, "CHAOS-6975 persisted-state: Python body is the Go-served refusal; the Python leak is the recorded defect this closes")
}
