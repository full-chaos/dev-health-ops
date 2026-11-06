package server

import (
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
)

// A superuser with a live go-api impersonation row (its own bearer, EdDSA or HS256, token org ecOrg) reads an enforced
// query-api REST row. The REST path (authenticateRESTRouted) reads no impersonation row on either side, so both serve
// the token organization, not the target's, and the enforce-on answer equals the cells-off answer. The EdDSA bearer
// is asked of the PDP as the admin in the token organization; the HS256 bearer is exempt.
func TestALiveRowCallersReadOnAnEnforcedQueryAPIRowEqualsTheLegacyAnswer(t *testing.T) {
	quietStdLog(t)
	m := newAccessMinter(t)
	store := newAccessStore()
	org := ecOrg.String()
	store.states[dlpSUI] = policy.UserState{IsActive: true, IsSuperuser: true}
	store.found[dlpSUI] = true
	store.liveSessions[dlpSID(dlpSUI)] = policy.SessionRow{PrincipalID: dlpSUI, LegacyTokenVersion: i64(0)}
	if store.roles == nil {
		store.roles = map[[2]uuid.UUID]string{}
	}
	store.members[[2]uuid.UUID{dlpSUI, ecOrg}] = true
	store.roles[[2]uuid.UUID{dlpSUI, ecOrg}] = "admin"
	if store.sessions == nil {
		store.sessions = map[uuid.UUID]*policy.Impersonation{}
	}
	store.sessions[dlpSUI] = &policy.Impersonation{ID: uuid.New(), AdminUserID: dlpSUI, TargetUserID: dlpT, TargetOrgID: ecTarget,
		TargetRole: "admin", ExpiresAt: time.Now().Add(time.Hour)}
	p := newDLPPDP(t)
	snap := dlpSnap{platformRole: "platform_admin", members: map[string]string{org: "admin"}}
	eddsa := m.mint(dlpSUI, map[string]any{"sid": sessionid.Wire(dlpSID(dlpSUI)), "org_id": org}, nil)
	hs256 := ecMintEdgeToken(t, ecEdgeClaims{sub: dlpSUI.String(), orgID: org, role: "admin", isSuperuser: true})
	p.register(eddsa, "SUI/EdDSA/live row", snap)
	p.register(hs256, "SUI/HS256/live row", snap)
	legacy := dlpHandler(t, store, m, dlpGate(t, pdp.ModeOff, "", nil))
	enforced := dlpHandler(t, store, m, dlpGate(t, pdp.ModeEnforce, dlpReadCells, p))
	for _, token := range []struct{ name, bearer string }{{"EdDSA", eddsa}, {"HS256", hs256}} {
		for _, row := range dlpRows {
			answer := func(h http.Handler) dlpAnswer {
				req := httptest.NewRequest(http.MethodGet, row, nil)
				req.Header.Set("Authorization", "Bearer "+token.bearer)
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, req)
				return dlpAnswer{rec.Code, strings.TrimSpace(rec.Body.String())}
			}
			off := answer(legacy)
			on := answer(enforced)
			calls := p.take()
			t.Logf("CELL|Q5|SUI/%s/org=A+liveRow(target %s)|GET %s|enforce-on %d %s|cells-off %d %s|pdp_calls=%d %s", token.name, ecTarget, row,
				on.code, on.body, off.code, off.body, len(calls), strings.Join(calls, "; "))
			// The scope kind says who built the scope (pepgate.Legacy or the PDP allow); the served organization and its
			// reach are the answer.
			if on.code != off.code || withoutScopeKind(on.body) != withoutScopeKind(off.body) || off.code != http.StatusOK ||
				!strings.Contains(off.body, `"org":"`+org+`"`) {
				t.Errorf("%s GET %s: enforce-on %d %s, cells-off %d %s; want equal 200 answers in the token organization", token.name, row, on.code, on.body, off.code, off.body)
			}
		}
	}
}

var scopeKindField = regexp.MustCompile(`"scope_kind":[0-9]+,`)

func withoutScopeKind(body string) string { return scopeKindField.ReplaceAllString(body, "") }
