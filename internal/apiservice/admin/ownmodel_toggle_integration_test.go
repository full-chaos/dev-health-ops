//go:build integration

package admin_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

const ownModelPath = "/api/v1/admin/llm-settings/own-model"

// TestOwnModelToggle is the persisted-state proof of the org-admin own-model
// toggle (real Postgres, real HTTP routes): an org admin turns the org's saved
// BYO LLM config off and on without changing the saved rows; each state change
// writes one audit row with the actor and the old and new value; a member is
// refused; another org is untouched.
func TestOwnModelToggle(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-own-model-32-bytes!"

	orgSaved, orgEmpty := uuid.New(), uuid.New()
	adminID, memberID := uuid.New(), uuid.New()

	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		GoOnly: true,
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
			for slug, org := range map[string]uuid.UUID{"saved": orgSaved, "empty": orgEmpty} {
				exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, $2, 'team', 'stripe', true, now(), now())`, org, "ownmodel-"+slug)
			}
			for id, email := range map[uuid.UUID]string{adminID: "ownmodel-admin@example.com", memberID: "ownmodel-member@example.com"} {
				exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, true, true, false, 0, now(), now())`, id, email)
			}
			for key, value := range map[string]string{"provider": "anthropic", "model": "saved-model", "api_key": "sealed-placeholder"} {
				exec(`INSERT INTO settings (id, org_id, category, key, value, is_encrypted, created_at, updated_at)
VALUES (gen_random_uuid(), $1, 'llm', $2, $3, $4, now() - interval '1 day', now() - interval '1 day')`, orgSaved.String(), key, value, key == "api_key")
			}
			return map[string]map[string]any{
				"saved":  {"user_id": adminID.String(), "email": "ownmodel-admin@example.com", "org_id": orgSaved.String(), "role": "admin"},
				"empty":  {"user_id": adminID.String(), "email": "ownmodel-admin@example.com", "org_id": orgEmpty.String(), "role": "admin"},
				"member": {"user_id": memberID.String(), "email": "ownmodel-member@example.com", "org_id": orgSaved.String(), "role": "member"},
			}
		},
	})
	base, pool := startGoServer(t, ctx, venue, jwtKey)
	auth := func(name string) map[string]string {
		return map[string]string{"Authorization": "Bearer " + venue.Tokens[name]}
	}
	put := func(name string, enabled bool) routeResponse {
		body := `{"enabled": false}`
		if enabled {
			body = `{"enabled": true}`
		}
		return doJSONRequest(t, base, http.MethodPut, ownModelPath, body, auth(name))
	}
	savedRows := func() string {
		t.Helper()
		var rows string
		if err := pool.QueryRow(ctx, `SELECT coalesce(string_agg(key || '=' || coalesce(value, '') || '@' || updated_at::text, ';' ORDER BY key), '')
FROM settings WHERE org_id = $1 AND category = 'llm' AND key <> 'own_model_enabled'`, orgSaved.String()).Scan(&rows); err != nil {
			t.Fatal(err)
		}
		return rows
	}
	toggleRow := func(org uuid.UUID) (string, bool) {
		t.Helper()
		var value string
		err := pool.QueryRow(ctx, `SELECT coalesce(value, '') FROM settings WHERE org_id = $1 AND category = 'llm' AND key = 'own_model_enabled'`, org.String()).Scan(&value)
		if err != nil {
			return "", false
		}
		return value, true
	}
	auditRows := func() int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM audit_logs WHERE org_id = $1 AND resource_id = 'llm/own_model_enabled'`, orgSaved).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	lastAudit := func() (uuid.UUID, string, string, map[string]any) {
		t.Helper()
		var userID uuid.UUID
		var action, resourceType, changes string
		if err := pool.QueryRow(ctx, `SELECT user_id, action, resource_type, changes::text FROM audit_logs
WHERE org_id = $1 AND resource_id = 'llm/own_model_enabled' ORDER BY created_at DESC LIMIT 1`, orgSaved).Scan(&userID, &action, &resourceType, &changes); err != nil {
			t.Fatalf("audit row: %v", err)
		}
		var change map[string]any
		if err := json.Unmarshal([]byte(changes), &change); err != nil {
			t.Fatalf("audit changes %q: %v", changes, err)
		}
		return userID, action, resourceType, change
	}

	before := savedRows()
	t.Run("an org with a saved config reads on and configured", func(t *testing.T) {
		resp := doJSONRequest(t, base, http.MethodGet, ownModelPath, "", auth("saved"))
		if resp.status != http.StatusOK {
			t.Fatalf("GET: status=%d body=%s", resp.status, resp.body)
		}
		if got := decodeObject(t, resp); got["enabled"] != true || got["configured"] != true {
			t.Fatalf("GET body=%s, want enabled=true configured=true", resp.body)
		}
	})
	t.Run("an org with nothing saved reads on and not configured", func(t *testing.T) {
		resp := doJSONRequest(t, base, http.MethodGet, ownModelPath, "", auth("empty"))
		if resp.status != http.StatusOK {
			t.Fatalf("GET: status=%d body=%s", resp.status, resp.body)
		}
		if got := decodeObject(t, resp); got["enabled"] != true || got["configured"] != false {
			t.Fatalf("GET body=%s, want enabled=true configured=false", resp.body)
		}
	})
	t.Run("a member is refused", func(t *testing.T) {
		if resp := put("member", false); resp.status != http.StatusForbidden {
			t.Fatalf("member PUT: status=%d body=%s, want 403", resp.status, resp.body)
		}
		if _, found := toggleRow(orgSaved); found {
			t.Fatal("a member's PUT wrote the toggle row")
		}
	})
	t.Run("a body without enabled is a validation error", func(t *testing.T) {
		if resp := doJSONRequest(t, base, http.MethodPut, ownModelPath, `{}`, auth("saved")); resp.status != http.StatusUnprocessableEntity {
			t.Fatalf("PUT {}: status=%d body=%s, want 422", resp.status, resp.body)
		}
	})
	t.Run("the org admin turns it off: saved rows unchanged, one audit row", func(t *testing.T) {
		resp := put("saved", false)
		if resp.status != http.StatusOK {
			t.Fatalf("PUT off: status=%d body=%s", resp.status, resp.body)
		}
		if got := decodeObject(t, resp); got["enabled"] != false || got["configured"] != true {
			t.Fatalf("PUT off body=%s, want enabled=false configured=true", resp.body)
		}
		if value, found := toggleRow(orgSaved); !found || value != "false" {
			t.Fatalf("toggle row = %q found=%t, want false", value, found)
		}
		if after := savedRows(); after != before {
			t.Fatalf("saved rows changed:\nbefore %s\nafter  %s", before, after)
		}
		userID, action, resourceType, change := lastAudit()
		if userID != adminID || action != "settings_updated" || resourceType != "setting" {
			t.Fatalf("audit user=%s action=%s type=%s, want %s settings_updated setting", userID, action, resourceType, adminID)
		}
		if change["setting"] != "own_model_enabled" || change["source"] != "self_service" || change["old_enabled"] != true || change["new_enabled"] != false {
			t.Fatalf("audit changes=%v, want setting=own_model_enabled source=self_service old_enabled=true new_enabled=false", change)
		}
		if n := auditRows(); n != 1 {
			t.Fatalf("audit rows = %d, want 1", n)
		}
		got := decodeObject(t, doJSONRequest(t, base, http.MethodGet, ownModelPath, "", auth("saved")))
		if got["enabled"] != false {
			t.Fatalf("GET after off = %v, want enabled=false", got)
		}
	})
	t.Run("the same value again writes no audit row", func(t *testing.T) {
		if resp := put("saved", false); resp.status != http.StatusOK {
			t.Fatalf("PUT off again: status=%d body=%s", resp.status, resp.body)
		}
		if n := auditRows(); n != 1 {
			t.Fatalf("audit rows = %d, want 1", n)
		}
	})
	t.Run("the org admin turns it on again", func(t *testing.T) {
		resp := put("saved", true)
		if resp.status != http.StatusOK {
			t.Fatalf("PUT on: status=%d body=%s", resp.status, resp.body)
		}
		if got := decodeObject(t, resp); got["enabled"] != true || got["configured"] != true {
			t.Fatalf("PUT on body=%s, want enabled=true configured=true", resp.body)
		}
		_, _, _, change := lastAudit()
		if change["old_enabled"] != false || change["new_enabled"] != true {
			t.Fatalf("audit changes=%v, want old_enabled=false new_enabled=true", change)
		}
		if n := auditRows(); n != 2 {
			t.Fatalf("audit rows = %d, want 2", n)
		}
		if after := savedRows(); after != before {
			t.Fatalf("saved rows changed:\nbefore %s\nafter  %s", before, after)
		}
	})
	t.Run("another org is untouched", func(t *testing.T) {
		if _, found := toggleRow(orgEmpty); found {
			t.Fatal("the other org has a toggle row")
		}
	})
	venueoracle.WriteGoOnlyProof(t, "org-admin own-model toggle: persisted flag, audit row with actor and old/new value, saved rows unchanged, member refused")
}
