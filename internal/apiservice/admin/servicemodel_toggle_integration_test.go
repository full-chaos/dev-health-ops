//go:build integration

package admin_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	acrroute "github.com/full-chaos/dev-health-ops/internal/apiservice/acr"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

const serviceModelPath = "/api/v1/admin/llm-settings/service-model"

func doJSONRequest(t *testing.T, base, method, path, body string, headers map[string]string) routeResponse {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = bytes.NewBufferString(body)
	}
	req, err := http.NewRequest(method, base+path, reader)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	return routeResponse{status: resp.StatusCode, body: string(raw)}
}

func decodeObject(t *testing.T, resp routeResponse) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal([]byte(resp.body), &out); err != nil {
		t.Fatalf("decode %q: %v", resp.body, err)
	}
	return out
}

// TestServiceModelSelfServiceToggle is CHAOS-10243's persisted-state proof
// (real Postgres, real HTTP routes): an org admin on a team or enterprise plan
// with agent_context_runtime turns acr_service_model on and off for their own
// org; the acr entitlement route then serves the new value. The toggle never
// overwrites or erases an override that billing or support wrote, and a later
// billing write is never lost to it.
func TestServiceModelSelfServiceToggle(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-svc-model-32-bytes!"

	orgTeam, orgEnterprise, orgCommunity, orgNoACR, orgBilling := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	adminID, memberID, superID := uuid.New(), uuid.New(), uuid.New()
	serviceModelFlag := uuid.New()

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
			for slug, org := range map[string]struct {
				id   uuid.UUID
				tier string
			}{
				"team": {orgTeam, "team"}, "enterprise": {orgEnterprise, "enterprise"}, "community": {orgCommunity, "community"},
				"noacr": {orgNoACR, "team"}, "billing": {orgBilling, "team"},
			} {
				exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, $2, $3, 'stripe', true, now(), now())`, org.id, "svcmodel-"+slug, org.tier)
			}
			for id, email := range map[uuid.UUID]string{adminID: "svcmodel-admin@example.com", memberID: "svcmodel-member@example.com"} {
				exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, true, true, false, 0, now(), now())`, id, email)
			}
			exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'svcmodel-super@example.com', true, true, true, 0, now(), now())`, superID)
			exec(`DELETE FROM feature_flags WHERE key = 'acr_service_model'`)
			exec(`INSERT INTO feature_flags (id, key, name, description, category, min_tier, is_enabled, is_beta, is_deprecated, created_at, updated_at)
VALUES ($1, 'acr_service_model', 'ACR Service Model', NULL, 'integrations', 'community', true, false, false, now(), now())`, serviceModelFlag)
			override := func(org uuid.UUID, key string, enabled bool, reason *string) {
				t.Helper()
				tag, err := admin.Exec(ctx, `INSERT INTO org_feature_overrides (id, org_id, feature_id, is_enabled, config, reason, created_at, updated_at)
SELECT $1, $2, id, $3, '{}'::json, $5, now(), now() FROM feature_flags WHERE key = $4`, uuid.New(), org, enabled, key, reason)
				if err != nil || tag.RowsAffected() != 1 {
					t.Fatalf("seed override %s for %s: rows=%d err=%v", key, org, tag.RowsAffected(), err)
				}
			}
			for _, org := range []uuid.UUID{orgTeam, orgEnterprise, orgCommunity, orgBilling} {
				override(org, "agent_context_runtime", true, nil)
			}
			promo := "billing: annual contract"
			override(orgBilling, "acr_service_model", true, &promo)

			tokens := map[string]map[string]any{
				"member": {"user_id": memberID.String(), "email": "svcmodel-member@example.com", "org_id": orgTeam.String(), "role": "member"},
				"super":  {"user_id": superID.String(), "email": "svcmodel-super@example.com", "is_superuser": true},
			}
			for slug, org := range map[string]uuid.UUID{"team": orgTeam, "enterprise": orgEnterprise, "community": orgCommunity, "noacr": orgNoACR, "billing": orgBilling} {
				tokens[slug] = map[string]any{"user_id": adminID.String(), "email": "svcmodel-admin@example.com", "org_id": org.String(), "role": "admin"}
			}
			return tokens
		},
	})
	base, pool := startGoServer(t, ctx, venue, jwtKey)
	auth := func(name string) map[string]string {
		return map[string]string{"Authorization": "Bearer " + venue.Tokens[name]}
	}
	served := func(org uuid.UUID) bool {
		t.Helper()
		entitlement, err := acrroute.PostgresEntitlementStore{Pool: pool}.Lookup(ctx, org.String())
		if err != nil {
			t.Fatalf("acr entitlement lookup %s: %v", org, err)
		}
		return entitlement.ACRServiceModel
	}
	overrideRow := func(org uuid.UUID) (id uuid.UUID, enabled bool, reason *string, found bool) {
		t.Helper()
		err := pool.QueryRow(ctx, `SELECT o.id, o.is_enabled, o.reason FROM org_feature_overrides o JOIN feature_flags f ON f.id = o.feature_id
WHERE o.org_id = $1 AND f.key = 'acr_service_model'`, org).Scan(&id, &enabled, &reason)
		if err != nil {
			return uuid.Nil, false, nil, false
		}
		return id, enabled, reason, true
	}
	put := func(name string, enabled bool) routeResponse {
		body := `{"enabled": false}`
		if enabled {
			body = `{"enabled": true}`
		}
		return doJSONRequest(t, base, http.MethodPut, serviceModelPath, body, auth(name))
	}

	t.Run("eligible org reads off and self-service", func(t *testing.T) {
		resp := doJSONRequest(t, base, http.MethodGet, serviceModelPath, "", auth("team"))
		if resp.status != 200 {
			t.Fatalf("GET: status=%d body=%s", resp.status, resp.body)
		}
		got := decodeObject(t, resp)
		if got["enabled"] != false || got["eligible"] != true || got["control"] != "none" {
			t.Fatalf("GET body=%s, want enabled=false eligible=true control=none", resp.body)
		}
	})

	t.Run("org admin turns it on: acr route serves true, audit row written", func(t *testing.T) {
		if served(orgTeam) {
			t.Fatal("acr route serves acr_service_model=true before the toggle")
		}
		resp := put("team", true)
		if resp.status != 200 {
			t.Fatalf("PUT on: status=%d body=%s", resp.status, resp.body)
		}
		got := decodeObject(t, resp)
		if got["enabled"] != true || got["control"] != "self_service" {
			t.Fatalf("PUT on body=%s, want enabled=true control=self_service", resp.body)
		}
		if !served(orgTeam) {
			t.Fatal("acr route still serves acr_service_model=false after the toggle")
		}
		var userID uuid.UUID
		var action, resourceType, resourceID, changes string
		if err := pool.QueryRow(ctx, `SELECT user_id, action, resource_type, resource_id, changes::text FROM audit_logs
WHERE org_id = $1 AND resource_id = 'acr_service_model' ORDER BY created_at DESC LIMIT 1`, orgTeam).Scan(&userID, &action, &resourceType, &resourceID, &changes); err != nil {
			t.Fatalf("audit row: %v", err)
		}
		if userID != adminID || action != "feature_enabled" || resourceType != "feature_flag" {
			t.Fatalf("audit row user=%s action=%s type=%s, want %s feature_enabled feature_flag", userID, action, resourceType, adminID)
		}
		var change map[string]any
		if err := json.Unmarshal([]byte(changes), &change); err != nil {
			t.Fatalf("audit changes %q: %v", changes, err)
		}
		if change["source"] != "self_service" || change["old_enabled"] != false || change["new_enabled"] != true {
			t.Fatalf("audit changes=%s, want source=self_service old_enabled=false new_enabled=true", changes)
		}
	})

	t.Run("org admin turns it off: row removed, acr route serves false", func(t *testing.T) {
		resp := put("team", false)
		if resp.status != 200 {
			t.Fatalf("PUT off: status=%d body=%s", resp.status, resp.body)
		}
		if got := decodeObject(t, resp); got["enabled"] != false || got["control"] != "none" {
			t.Fatalf("PUT off body=%s, want enabled=false control=none", resp.body)
		}
		if _, _, _, found := overrideRow(orgTeam); found {
			t.Fatal("the self-service row survived the toggle off")
		}
		if served(orgTeam) {
			t.Fatal("acr route serves acr_service_model=true after the toggle off")
		}
	})

	t.Run("enterprise plan may toggle", func(t *testing.T) {
		if resp := put("enterprise", true); resp.status != 200 {
			t.Fatalf("PUT enterprise: status=%d body=%s", resp.status, resp.body)
		}
		if !served(orgEnterprise) {
			t.Fatal("acr route serves false for the enterprise org after the toggle")
		}
	})

	t.Run("a member may not toggle", func(t *testing.T) {
		if resp := put("member", true); resp.status != 403 {
			t.Fatalf("PUT member: status=%d body=%s, want 403", resp.status, resp.body)
		}
		if _, _, _, found := overrideRow(orgTeam); found {
			t.Fatal("a member's PUT wrote an override")
		}
	})

	t.Run("community plan is refused and reads ineligible", func(t *testing.T) {
		if resp := put("community", true); resp.status != 402 {
			t.Fatalf("PUT community: status=%d body=%s, want 402", resp.status, resp.body)
		}
		if _, _, _, found := overrideRow(orgCommunity); found {
			t.Fatal("a refused PUT wrote an override")
		}
		resp := doJSONRequest(t, base, http.MethodGet, serviceModelPath, "", auth("community"))
		if got := decodeObject(t, resp); resp.status != 200 || got["eligible"] != false {
			t.Fatalf("GET community: status=%d body=%s, want eligible=false", resp.status, resp.body)
		}
	})

	t.Run("no agent_context_runtime is refused", func(t *testing.T) {
		if resp := put("noacr", true); resp.status != 403 {
			t.Fatalf("PUT noacr: status=%d body=%s, want 403", resp.status, resp.body)
		}
		if _, _, _, found := overrideRow(orgNoACR); found {
			t.Fatal("a refused PUT wrote an override")
		}
	})

	t.Run("a billing override is never overwritten or erased", func(t *testing.T) {
		before, _, reasonBefore, _ := overrideRow(orgBilling)
		resp := doJSONRequest(t, base, http.MethodGet, serviceModelPath, "", auth("billing"))
		if got := decodeObject(t, resp); got["control"] != "managed" || got["enabled"] != true {
			t.Fatalf("GET billing body=%s, want control=managed enabled=true", resp.body)
		}
		for _, enabled := range []bool{false, true} {
			if resp := put("billing", enabled); resp.status != 409 {
				t.Fatalf("PUT billing enabled=%t: status=%d body=%s, want 409", enabled, resp.status, resp.body)
			}
		}
		after, enabled, reasonAfter, found := overrideRow(orgBilling)
		if !found || after != before || !enabled || reasonAfter == nil || reasonBefore == nil || *reasonAfter != *reasonBefore {
			t.Fatalf("billing override changed: found=%t id %s->%s enabled=%t reason %v->%v", found, before, after, enabled, reasonBefore, reasonAfter)
		}
	})

	t.Run("a later billing write is never lost to the toggle", func(t *testing.T) {
		if resp := put("team", true); resp.status != 200 {
			t.Fatalf("PUT on: status=%d body=%s", resp.status, resp.body)
		}
		id, _, _, found := overrideRow(orgTeam)
		if !found {
			t.Fatal("no self-service row after the toggle")
		}
		patch := doJSONRequest(t, base, http.MethodPatch, "/api/v1/admin/orgs/"+orgTeam.String()+"/feature-overrides/"+id.String(),
			`{"reason": "billing: contract renewal"}`, auth("super"))
		if patch.status != 200 {
			t.Fatalf("superuser PATCH: status=%d body=%s", patch.status, patch.body)
		}
		if resp := put("team", false); resp.status != 409 {
			t.Fatalf("PUT off after a billing write: status=%d body=%s, want 409", resp.status, resp.body)
		}
		_, enabled, reason, found := overrideRow(orgTeam)
		if !found || !enabled || reason == nil || *reason != "billing: contract renewal" {
			t.Fatalf("billing write lost: found=%t enabled=%t reason=%v", found, enabled, reason)
		}
		if !served(orgTeam) {
			t.Fatal("acr route serves false after the refused toggle")
		}
	})

	venueoracle.WriteGoOnlyProof(t, "CHAOS-10243 self-service acr_service_model toggle: persisted override, audit row and acr entitlement route readback")
}
