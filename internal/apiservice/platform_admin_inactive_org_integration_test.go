//go:build integration

package apiservice

import (
	"context"
	"net/http"
	"testing"
	"time"
)

// CHAOS-13209 D3 (x), acp 2026-11-06: the legacy go-api answer, one real row per platform_admin-flagged action, for a
// superuser with no membership row (EdDSA, org claim B) when B is active and when B is suspended, and, as the
// control, for an admin member of B. A measurement: each answer is a CELL line; the test fails on no answer, and on a
// 401 while B is active (the token did not pass authentication, so the cell measures nothing). A guard refusal is 401 or 403 with the guard's body; any other answer means the request was admitted.
func TestLegacyGoAPIFlaggedActionsInAnInactiveOrganization(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	rig := newParityRig(t, ctx)
	B := parOrgB.String()
	su0 := rig.eddsa(parSU0, parPtr(parOrgB), nil)
	admin := rig.eddsa(parT, parPtr(parOrgB), nil)
	cells := []struct{ action, method, path, body string }{
		{"billing.manage", http.MethodPost, "/api/v1/billing/subscriptions/cancel?org_id=" + B, "{}"},
		{"integrations.read", http.MethodGet, "/api/v1/admin/integrations", ""},
		{"integrations.write", http.MethodPost, "/api/v1/admin/integrations", "{}"},
		{"members.invite", http.MethodPost, "/api/v1/admin/orgs/" + B + "/invites", "{}"},
		{"members.manage", http.MethodPatch, "/api/v1/admin/orgs/" + B + "/members/" + parM.String(), `{"role":"member"}`},
		{"members.read", http.MethodGet, "/api/v1/admin/orgs/" + B + "/members", ""},
		{"settings.read", http.MethodGet, "/api/v1/admin/settings/general", ""},
		{"settings.write", http.MethodPost, "/api/v1/admin/settings", "{}"},
		{"teams.read", http.MethodGet, "/api/v1/admin/teams", ""},
		{"teams.write", http.MethodPost, "/api/v1/admin/teams", "{}"},
	}
	for _, state := range []struct {
		label  string
		active bool
	}{{"active", true}, {"suspended", false}} {
		if _, err := rig.pool.Exec(ctx, `UPDATE organizations SET is_active = $2 WHERE id = $1`, parOrgB, state.active); err != nil {
			t.Fatal(err)
		}
		for _, caller := range []struct{ label, token string }{{"superuser, no membership row", su0}, {"admin member of B", admin}} {
			for _, c := range cells {
				status, body, _ := rig.do(rig.legacy, c.method, c.path, c.body, caller.token)
				if status == 0 {
					t.Fatalf("%s %s %s: no answer", caller.label, c.method, c.path)
				}
				if state.active && status == http.StatusUnauthorized {
					t.Errorf("%s %s %s: 401 %s with org B active, want an answer past authentication", caller.label, c.method, c.path, body)
				}
				t.Logf("CELL|D3x|go-api legacy|org B %s|%s|%s|%s %s|%d|%s", state.label, caller.label, c.action, c.method, c.path, status, body)
			}
		}
	}
}
