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

const (
	promptCachePath = "/api/v1/admin/llm-settings/prompt-cache"
	llmSettingsPath = "/api/v1/admin/llm-settings"
)

// TestPromptCacheOptIn is the persisted-state proof of the org-admin BYO
// prompt-cache opt-in (real Postgres, real HTTP routes): it reads off until an
// org admin turns it on; each state change writes one audit row with the actor
// and the old and new value; the saved BYO rows are never written; a member is
// refused; another org is untouched; deleting the BYO settings clears it.
func TestPromptCacheOptIn(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-prompt-cache-32-bytes!"

	orgSaved, orgOpenAI, orgEmpty := uuid.New(), uuid.New(), uuid.New()
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
			for slug, org := range map[string]uuid.UUID{"saved": orgSaved, "openai": orgOpenAI, "empty": orgEmpty} {
				exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, $2, 'team', 'stripe', true, now(), now())`, org, "promptcache-"+slug)
			}
			for id, email := range map[uuid.UUID]string{adminID: "promptcache-admin@example.com", memberID: "promptcache-member@example.com"} {
				exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, true, true, false, 0, now(), now())`, id, email)
			}
			for org, provider := range map[uuid.UUID]string{orgSaved: "anthropic", orgOpenAI: "openai"} {
				for key, value := range map[string]string{"provider": provider, "model": "saved-model", "api_key": "sealed-placeholder"} {
					exec(`INSERT INTO settings (id, org_id, category, key, value, is_encrypted, created_at, updated_at)
VALUES (gen_random_uuid(), $1, 'llm', $2, $3, $4, now() - interval '1 day', now() - interval '1 day')`, org.String(), key, value, key == "api_key")
				}
			}
			return map[string]map[string]any{
				"saved":  {"user_id": adminID.String(), "email": "promptcache-admin@example.com", "org_id": orgSaved.String(), "role": "admin"},
				"openai": {"user_id": adminID.String(), "email": "promptcache-admin@example.com", "org_id": orgOpenAI.String(), "role": "admin"},
				"empty":  {"user_id": adminID.String(), "email": "promptcache-admin@example.com", "org_id": orgEmpty.String(), "role": "admin"},
				"member": {"user_id": memberID.String(), "email": "promptcache-member@example.com", "org_id": orgSaved.String(), "role": "member"},
			}
		},
	})
	base, pool := startGoServer(t, ctx, venue, jwtKey)
	auth := func(name string) map[string]string {
		return map[string]string{"Authorization": "Bearer " + venue.Tokens[name]}
	}
	put := func(name, body string) routeResponse {
		return doJSONRequest(t, base, http.MethodPut, promptCachePath, body, auth(name))
	}
	get := func(name string) map[string]any {
		t.Helper()
		resp := doJSONRequest(t, base, http.MethodGet, promptCachePath, "", auth(name))
		if resp.status != http.StatusOK {
			t.Fatalf("GET as %s: status=%d body=%s", name, resp.status, resp.body)
		}
		return decodeObject(t, resp)
	}
	savedRows := func() string {
		t.Helper()
		var rows string
		if err := pool.QueryRow(ctx, `SELECT coalesce(string_agg(key || '=' || coalesce(value, '') || '@' || updated_at::text, ';' ORDER BY key), '')
FROM settings WHERE org_id = $1 AND category = 'llm' AND key <> 'prompt_cache'`, orgSaved.String()).Scan(&rows); err != nil {
			t.Fatal(err)
		}
		return rows
	}
	optInRow := func(org uuid.UUID) (value string, encrypted, found bool) {
		t.Helper()
		err := pool.QueryRow(ctx, `SELECT coalesce(value, ''), is_encrypted FROM settings WHERE org_id = $1 AND category = 'llm' AND key = 'prompt_cache'`, org.String()).Scan(&value, &encrypted)
		return value, encrypted, err == nil
	}
	auditRows := func() int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM audit_logs WHERE org_id = $1 AND resource_id = 'llm/prompt_cache'`, orgSaved).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	lastAudit := func() (uuid.UUID, string, string, map[string]any) {
		t.Helper()
		var userID uuid.UUID
		var action, resourceType, changes string
		if err := pool.QueryRow(ctx, `SELECT user_id, action, resource_type, changes::text FROM audit_logs
WHERE org_id = $1 AND resource_id = 'llm/prompt_cache' ORDER BY created_at DESC LIMIT 1`, orgSaved).Scan(&userID, &action, &resourceType, &changes); err != nil {
			t.Fatalf("audit row: %v", err)
		}
		var change map[string]any
		if err := json.Unmarshal([]byte(changes), &change); err != nil {
			t.Fatalf("audit changes %q: %v", changes, err)
		}
		return userID, action, resourceType, change
	}

	before := savedRows()
	t.Run("it reads off until turned on, with the config facts", func(t *testing.T) {
		for _, tc := range []struct {
			org                            string
			enabled, configured, anthropic bool
		}{
			{"saved", false, true, true},
			{"openai", false, true, false},
			{"empty", false, false, false},
		} {
			got := get(tc.org)
			if got["enabled"] != tc.enabled || got["configured"] != tc.configured || got["anthropic"] != tc.anthropic {
				t.Fatalf("%s: GET = %v, want enabled=%t configured=%t anthropic=%t", tc.org, got, tc.enabled, tc.configured, tc.anthropic)
			}
		}
	})
	t.Run("a member is refused", func(t *testing.T) {
		if resp := doJSONRequest(t, base, http.MethodGet, promptCachePath, "", auth("member")); resp.status != http.StatusForbidden {
			t.Fatalf("member GET: status=%d body=%s, want 403", resp.status, resp.body)
		}
		if resp := put("member", `{"enabled": true}`); resp.status != http.StatusForbidden {
			t.Fatalf("member PUT: status=%d body=%s, want 403", resp.status, resp.body)
		}
		if _, _, found := optInRow(orgSaved); found {
			t.Fatal("a member's PUT wrote the opt-in row")
		}
	})
	t.Run("a body without a boolean enabled is a validation error", func(t *testing.T) {
		for _, body := range []string{`{}`, `{"enabled": [1]}`, `[]`} {
			if resp := put("saved", body); resp.status != http.StatusUnprocessableEntity {
				t.Fatalf("PUT %s: status=%d body=%s, want 422", body, resp.status, resp.body)
			}
		}
	})
	t.Run("turning it off when it was never on writes nothing", func(t *testing.T) {
		if resp := put("saved", `{"enabled": false}`); resp.status != http.StatusOK {
			t.Fatalf("PUT off: status=%d body=%s", resp.status, resp.body)
		}
		if _, _, found := optInRow(orgSaved); found || auditRows() != 0 {
			t.Fatalf("an unchanged off PUT wrote a row or an audit entry (row %t, audit %d)", found, auditRows())
		}
	})
	t.Run("the org admin turns it on: one plain row, saved rows unchanged, one audit row", func(t *testing.T) {
		resp := put("saved", `{"enabled": true}`)
		if resp.status != http.StatusOK {
			t.Fatalf("PUT on: status=%d body=%s", resp.status, resp.body)
		}
		if got := decodeObject(t, resp); got["enabled"] != true || got["configured"] != true || got["anthropic"] != true {
			t.Fatalf("PUT on body=%s, want enabled=true configured=true anthropic=true", resp.body)
		}
		if value, encrypted, found := optInRow(orgSaved); !found || value != "true" || encrypted {
			t.Fatalf("opt-in row = %q encrypted=%t found=%t, want a plain true", value, encrypted, found)
		}
		if after := savedRows(); after != before {
			t.Fatalf("saved rows changed:\nbefore %s\nafter  %s", before, after)
		}
		userID, action, resourceType, change := lastAudit()
		if userID != adminID || action != "settings_updated" || resourceType != "setting" {
			t.Fatalf("audit user=%s action=%s type=%s, want %s settings_updated setting", userID, action, resourceType, adminID)
		}
		if change["setting"] != "prompt_cache" || change["source"] != "self_service" || change["old_enabled"] != false || change["new_enabled"] != true {
			t.Fatalf("audit changes=%v, want setting=prompt_cache source=self_service old_enabled=false new_enabled=true", change)
		}
		if n := auditRows(); n != 1 {
			t.Fatalf("audit rows = %d, want 1", n)
		}
		if got := get("saved"); got["enabled"] != true {
			t.Fatalf("GET after on = %v, want enabled=true", got)
		}
	})
	t.Run("the same value again writes no audit row", func(t *testing.T) {
		if resp := put("saved", `{"enabled": true}`); resp.status != http.StatusOK {
			t.Fatalf("PUT on again: status=%d body=%s", resp.status, resp.body)
		}
		if n := auditRows(); n != 1 {
			t.Fatalf("audit rows = %d, want 1", n)
		}
	})
	t.Run("the org admin turns it off again", func(t *testing.T) {
		if resp := put("saved", `{"enabled": false}`); resp.status != http.StatusOK {
			t.Fatalf("PUT off: status=%d body=%s", resp.status, resp.body)
		}
		if value, _, found := optInRow(orgSaved); !found || value != "false" {
			t.Fatalf("opt-in row = %q found=%t, want false", value, found)
		}
		_, _, _, change := lastAudit()
		if change["old_enabled"] != true || change["new_enabled"] != false {
			t.Fatalf("audit changes=%v, want old_enabled=true new_enabled=false", change)
		}
		if n := auditRows(); n != 2 {
			t.Fatalf("audit rows = %d, want 2", n)
		}
		if after := savedRows(); after != before {
			t.Fatalf("saved rows changed:\nbefore %s\nafter  %s", before, after)
		}
	})
	t.Run("another org is untouched", func(t *testing.T) {
		for _, org := range []uuid.UUID{orgOpenAI, orgEmpty} {
			if _, _, found := optInRow(org); found {
				t.Fatalf("org %s has an opt-in row", org)
			}
		}
	})
	t.Run("deleting the BYO settings clears the opt-in", func(t *testing.T) {
		if resp := put("saved", `{"enabled": true}`); resp.status != http.StatusOK {
			t.Fatalf("PUT on: status=%d body=%s", resp.status, resp.body)
		}
		if resp := doJSONRequest(t, base, http.MethodDelete, llmSettingsPath, "", auth("saved")); resp.status != http.StatusOK {
			t.Fatalf("DELETE: status=%d body=%s", resp.status, resp.body)
		}
		if _, _, found := optInRow(orgSaved); found {
			t.Fatal("the opt-in row survived the delete")
		}
		if got := get("saved"); got["enabled"] != false || got["configured"] != false {
			t.Fatalf("GET after delete = %v, want enabled=false configured=false", got)
		}
	})
	venueoracle.WriteGoOnlyProof(t, "org-admin BYO prompt-cache opt-in: persisted flag, audit row with actor and old/new value, saved rows unchanged, member refused, cleared with the BYO settings")
}
