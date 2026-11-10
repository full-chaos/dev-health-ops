package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
)

// The prove tools present user access tokens, the class the PDP judges: on an enforced REST read row each role
// gets the answer the legacy path gives it (product rule 1: a platform admin needs no membership row).
func TestUserTokenRolesGetTheLegacyAnswerOnEnforcedReadRows(t *testing.T) {
	quietStdLog(t)
	m := newAccessMinter(t)
	store := newAccessStore()
	if store.roles == nil {
		store.roles = map[[2]uuid.UUID]string{}
	}
	p := newDLPPDP(t)
	org := ecOrg.String()
	type cell struct {
		name      string
		token     string
		needs5890 bool
	}
	var cells []cell
	add := func(name, role string, superuser, member bool, extra map[string]any) {
		u := uuid.NewSHA1(uuid.NameSpaceOID, []byte(name))
		store.states[u] = policy.UserState{IsActive: true, TokenVersion: 0, IsSuperuser: superuser}
		store.found[u] = true
		store.liveSessions[dlpSID(u)] = policy.SessionRow{PrincipalID: u, LegacyTokenVersion: i64(0)}
		snap := dlpSnap{members: map[string]string{}}
		if member {
			store.members[[2]uuid.UUID{u, ecOrg}] = true
			store.roles[[2]uuid.UUID{u, ecOrg}] = role
			snap.members[org] = role
		}
		if superuser {
			snap.platformRole = "platform_admin"
		}
		claims := map[string]any{"sid": sessionid.Wire(dlpSID(u))}
		for k, v := range extra {
			claims[k] = v
		}
		token := m.mint(u, claims, nil)
		p.register(token, name, snap)
		cells = append(cells, cell{name: name, token: token, needs5890: superuser && !member && extra == nil})
	}
	for _, role := range []string{"viewer", "member", "admin", "owner"} {
		add(role, role, false, true, nil)
	}
	add("platform admin with no membership row", "", true, false, nil)
	add("platform admin with no organization", "", true, false, map[string]any{"org_id": nil})
	legacy := dlpHandlerFor(t, store, m, dlpGate(t, pdp.ModeOff, "", nil), dlpRows)
	enforced := dlpHandlerFor(t, store, m, dlpGate(t, pdp.ModeEnforce, dlpReadCells, p), dlpRows)
	serve := func(h http.Handler, row, token string) (int, string) {
		req := httptest.NewRequest(http.MethodGet, row, nil)
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Code, strings.TrimSpace(rec.Body.String())
	}
	entry, err := policycatalog.Default().Action("analytics.read")
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			if c.needs5890 && !entry.PlatformAdmin {
				t.Skip("CHAOS-13240: the org read actions answer a no-row platform admin like legacy once its catalog flag is on this base")
			}
			for _, row := range dlpRows {
				wantCode, wantBody := serve(legacy, row, c.token)
				gotCode, gotBody := serve(enforced, row, c.token)
				if wantCode != http.StatusOK {
					t.Fatalf("GET %s legacy answered %d %s: the cell's caller is not an admitted one", row, wantCode, wantBody)
				}
				if gotCode != wantCode || orgOf(gotBody) != orgOf(wantBody) {
					t.Errorf("GET %s enforced %d %s, legacy %d %s", row, gotCode, gotBody, wantCode, wantBody)
				}
			}
			p.take()
		})
	}
}

// orgOf is the "org" member of a dlpHandler answer.
func orgOf(body string) string {
	_, rest, ok := strings.Cut(body, `"org":"`)
	if !ok {
		return ""
	}
	value, _, _ := strings.Cut(rest, `"`)
	return value
}
