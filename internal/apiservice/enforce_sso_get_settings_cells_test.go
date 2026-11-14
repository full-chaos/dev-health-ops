package apiservice

import (
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
)

var (
	ssoGetNoRowUser = uuid.MustParse("00000000-0000-4000-8000-0000000000e8")
	ssoGetNoRowSID  = uuid.MustParse("00000000-0000-4000-8000-0000000000e9")
)

func withSSOGetNoRowCaller(store *probeStore) {
	withBoundaryCallers(store)
	store.users[ssoGetNoRowUser] = probeUserRow{state: policy.UserState{IsActive: true}, email: "norow@example.test"}
	store.sessions[ssoGetNoRowSID] = policy.SessionRow{PrincipalID: ssoGetNoRowUser, LegacyTokenVersion: i64ptr(0)}
}

// noSSOProviderRoutes is the go-api routes with AUTH_SSO_PROVIDERS_OWNED off: deps.SSOProviders is nil, so the SSO
// GET rows take the plain gated route with no late checks.
func noSSOProviderRoutes(_ *testing.T, deps Deps) []httpapi.Route {
	deps.SSOProviders = nil
	return Routes(deps, quietLogger())
}

// With the switch off and the settings namespace enforced, the two SSO GET rows (settings.read since #6193) are decided by
// the decision point before the route's own 422 and 402. Each cell compares the enforce-off answer with the settings-on
// answer, status and body.
func TestSSOGetRowsSwitchOffSettingsEnforced(t *testing.T) {
	const list, get = "GET /api/v1/auth/sso/providers", "GET /api/v1/auth/sso/providers/{provider_id}"
	const badQuery = "?limit=abc"
	cells := []struct {
		name, key, query, class string
		mode                    string // evaluate, fail, none (no decision point client)
		wantLegacy, wantNew     int
		legacyHas, newHas       string
	}{
		{"D1 no membership row", list, "", "no membership", "evaluate", 402, 403, "feature_not_licensed", "Access denied"},
		{"D1 no membership row", get, "", "no membership", "evaluate", 402, 403, "feature_not_licensed", "Access denied"},
		{"D2 no membership row, bad query", list, badQuery, "no membership", "evaluate", 422, 403, "", "Access denied"},
		{"D3 PDP down", list, "", "org admin", "fail", 402, 503, "feature_not_licensed", "Authorization service temporarily unavailable"},
		{"D3 PDP down", get, "", "org admin", "fail", 402, 503, "feature_not_licensed", "Authorization service temporarily unavailable"},
		{"D3 PDP down, bad query", list, badQuery, "org admin", "fail", 422, 503, "", "Authorization service temporarily unavailable"},
		{"D3 no PDP client", list, "", "org admin", "none", 402, 503, "feature_not_licensed", "authz_enforce_no_decision_point"},
		{"D3 no PDP client", get, "", "org admin", "none", 402, 503, "feature_not_licensed", "authz_enforce_no_decision_point"},
		{"D3 no PDP client, bad query", list, badQuery, "org admin", "none", 422, 503, "", "authz_enforce_no_decision_point"},
		{"PDP allow keeps 402", list, "", "org admin", "evaluate", 402, 402, "feature_not_licensed", "feature_not_licensed"},
		{"PDP allow keeps 402", get, "", "org admin", "evaluate", 402, 402, "feature_not_licensed", "feature_not_licensed"},
		{"PDP allow keeps 422", list, badQuery, "org admin", "evaluate", 422, 422, "", ""},
		{"platform admin no membership unchanged 402", list, "", "platform admin + no membership", "evaluate", 402, 402, "feature_not_licensed", "feature_not_licensed"},
		{"platform admin no membership unchanged 402", get, "", "platform admin + no membership", "evaluate", 402, 402, "feature_not_licensed", "feature_not_licensed"},
		{"platform admin no membership unchanged 422", list, badQuery, "platform admin + no membership", "evaluate", 422, 422, "", ""},
	}
	withSSOLicence(t, false)
	pdp := newCellPDP(t)
	off, offMinter := adminConfigServerScoped(t, nil, withSSOGetNoRowCaller, noSSOProviderRoutes, nil)
	servers := map[string]http.Handler{}
	onCallers := map[string]map[string]cellCaller{}
	for _, mode := range []string{"evaluate", "fail", "none"} {
		mode := mode
		handler, minter := adminConfigServerScoped(t, func(t *testing.T) *policy.Enforcer {
			if mode == "none" {
				return newCellEnforcer(t, nil, "settings", true)
			}
			return newCellEnforcer(t, pdp, "settings", false)
		}, withSSOGetNoRowCaller, noSSOProviderRoutes, nil)
		servers[mode] = handler
		callers := boundaryCallers(minter)
		callers["no membership"] = cellCaller{name: "no membership", token: minter.mint(ssoGetNoRowUser, map[string]any{"sid": sessionid.Wire(ssoGetNoRowSID)}), snap: cellSnapshot{}}
		onCallers[mode] = callers
	}
	offCallers := boundaryCallers(offMinter)
	offCallers["no membership"] = cellCaller{name: "no membership", token: offMinter.mint(ssoGetNoRowUser, map[string]any{"sid": sessionid.Wire(ssoGetNoRowSID)}), snap: cellSnapshot{}}
	for _, c := range cells {
		pdp.register(onCallers[c.mode])
		pdpMode := c.mode
		if c.mode == "none" {
			pdpMode = "deny"
		}
		pdp.reset(pdpMode)
		legacy := ssoDoWith(off, c.key, offCallers[c.class].token, c.query, "")
		if calls := pdp.calls(); len(calls) != 0 {
			t.Fatalf("%s %s: %d decision point calls with enforce off", c.name, c.key, len(calls))
		}
		got := ssoDoWith(servers[c.mode], c.key, onCallers[c.mode][c.class].token, c.query, "")
		calls := pdp.calls()
		ok := legacy.status == c.wantLegacy && strings.Contains(legacy.body, c.legacyHas) && got.status == c.wantNew && strings.Contains(got.body, c.newHas)
		result := "AS EXPECTED"
		if !ok {
			result = "DIFF"
			t.Errorf("%s %s%s: legacy %d %s, enforced %d %s; want legacy %d, enforced %d", c.name, c.key, c.query, legacy.status, legacy.body, got.status, got.body, c.wantLegacy, c.wantNew)
		}
		if c.mode != "none" && (len(calls) != 1 || calls[0].Action != "settings.read") {
			t.Errorf("%s %s: calls %+v, want one settings.read", c.name, c.key, calls)
		}
		t.Logf("CELL\t%s\t%s%s\t%s\tlegacy=%d:%s\tenforced=%d:%s\t%s", c.name, c.key, c.query, c.mode, legacy.status, strings.TrimSpace(legacy.body), got.status, strings.TrimSpace(got.body), result)
	}
}
