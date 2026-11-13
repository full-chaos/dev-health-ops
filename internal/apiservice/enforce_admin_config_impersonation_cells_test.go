package apiservice

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
)

// The as-built impersonation answers on the 61 settings, integrations, sync and teams rows (leaf 6.8). They pin what
// ships, not what is wanted: impersonation is read-only and narrower than legacy, a documented flaw (CHAOS-13211,
// D6778); the proxy-off write refusal is its item 6. A change to these answers is a product change, not a test fix.

const impersonationRefusalBody = `{"detail":"Not allowed in an impersonation session"}`

var cellLegacyCallerSID = uuid.MustParse("00000000-0000-4000-8000-0000000000f5")

// withLegacyImpersonationCaller gives probeAdminUser (a superuser with a live go-api impersonation row whose target is
// probeTarget in probeTargetOrg) a live session for its own access token.
func withLegacyImpersonationCaller(store *probeStore) {
	store.sessions[cellLegacyCallerSID] = policy.SessionRow{PrincipalID: probeAdminUser, LegacyTokenVersion: i64ptr(0)}
}

func refuseLegacyCaller(on bool) func(*policy.Scope) {
	return func(scope *policy.Scope) { scope.RefuseLegacyImpersonation(on) }
}

// impersonationCellEnforcers are the two paths: every 6.8 cell off (the legacy guard), and on with a decision point that
// allows every call, so an answer the decision point gave would be a pass.
func impersonationCellEnforcers(pdp *cellPDP) map[string]func(*testing.T) *policy.Enforcer {
	return map[string]func(*testing.T) *policy.Enforcer{
		"cells off": nil,
		"cells on":  func(t *testing.T) *policy.Enforcer { return newCellEnforcer(t, pdp, adminConfigNamespaces, false) },
	}
}

// A delegated session's bearer (an act claim) is refused 403 on every 6.8 row, with the proxy on or off and with the
// cells on or off; the decision point is never asked and the session is never applied.
func TestAdminConfigCellsRefuseTheDelegatedBearerOnBothPaths(t *testing.T) {
	pdp := newCellPDP(t)
	for _, proxy := range []bool{false, true} {
		for name, enforcer := range impersonationCellEnforcers(pdp) {
			handler, minter := adminConfigEchoServerScoped(t, enforcer, nil, refuseLegacyCaller(proxy))
			delegated := minter.mint(probeUser, map[string]any{"act": map[string]any{"sub": wirePrincipal(probeAdminUser)}})
			plain := minter.mint(probeUser, nil)
			rows := 0
			for _, key := range sortedAdminConfigKeys() {
				rows++
				pdp.reset("allow")
				got := cellDo(handler, key, delegated, nil)
				if got.status != http.StatusForbidden || got.body != impersonationRefusalBody || got.header.Get("X-Impersonating") != "" {
					t.Errorf("proxy=%v %s %s delegated bearer: %d %s X-Impersonating %q, want 403 %s", proxy, name, key, got.status, got.body,
						got.header.Get("X-Impersonating"), impersonationRefusalBody)
				}
				if calls := pdp.calls(); len(calls) != 0 {
					t.Errorf("proxy=%v %s %s delegated bearer: %d decision point calls, want 0", proxy, name, key, len(calls))
				}
				if control := cellDo(handler, key, plain, nil); control.status != http.StatusOK {
					t.Errorf("proxy=%v %s %s the same caller without act: %d %s, want the 200 echo", proxy, name, key, control.status, control.body)
				}
			}
			if rows != 61 {
				t.Errorf("proxy=%v %s: %d rows, want 61", proxy, name, rows)
			}
		}
	}
}

// With AUTH_PROXY_IMPERSONATION on (prod), the legacy impersonation caller (the admin's own bearer beside a live go-api
// row) is refused by the scope on every 6.8 row, before the guard: no impersonation headers, no decision point call,
// with the cells on or off.
func TestAdminConfigCellsRefuseTheLegacyImpersonationCallerAtTheScopeWithTheProxyOn(t *testing.T) {
	pdp := newCellPDP(t)
	for name, enforcer := range impersonationCellEnforcers(pdp) {
		handler, minter := adminConfigEchoServerScoped(t, enforcer, withLegacyImpersonationCaller, refuseLegacyCaller(true))
		token := minter.mint(probeAdminUser, map[string]any{"sid": sessionid.Wire(cellLegacyCallerSID)})
		rows := 0
		for _, key := range sortedAdminConfigKeys() {
			rows++
			pdp.reset("allow")
			got := cellDo(handler, key, token, nil)
			if got.status != http.StatusForbidden || got.body != impersonationRefusalBody || got.header.Get("X-Impersonating") != "" {
				t.Errorf("%s %s: %d %s X-Impersonating %q, want the scope's 403 %s and no impersonation header", name, key, got.status,
					got.body, got.header.Get("X-Impersonating"), impersonationRefusalBody)
			}
			if calls := pdp.calls(); len(calls) != 0 {
				t.Errorf("%s %s: %d decision point calls, want 0", name, key, len(calls))
			}
		}
		if rows != 61 {
			t.Errorf("%s: %d rows, want 61", name, rows)
		}
	}
}

// With the proxy off, the legacy impersonation caller is served in the target's organization on every 6.8 row with
// the cells off. With the cells on, a read keeps that answer byte for byte; a write is refused 403 after the scope
// applied the session (CHAOS-13211 item 6: stricter than legacy, documented, not fixed). The decision point is never
// asked.
func TestAdminConfigCellsAnswerTheLegacyImpersonationCallerAsBuiltWithTheProxyOff(t *testing.T) {
	pdp := newCellPDP(t)
	off, offMinter := adminConfigEchoServerScoped(t, nil, withLegacyImpersonationCaller, refuseLegacyCaller(false))
	on, onMinter := adminConfigEchoServerScoped(t, impersonationCellEnforcers(pdp)["cells on"], withLegacyImpersonationCaller, refuseLegacyCaller(false))
	offToken := offMinter.mint(probeAdminUser, map[string]any{"sid": sessionid.Wire(cellLegacyCallerSID)})
	onToken := onMinter.mint(probeAdminUser, map[string]any{"sid": sessionid.Wire(cellLegacyCallerSID)})
	reads, writes := 0, 0
	for _, key := range sortedAdminConfigKeys() {
		pdp.reset("allow")
		legacy := cellDo(off, key, offToken, nil)
		var echo map[string]any
		if legacy.status != http.StatusOK || json.Unmarshal([]byte(legacy.body), &echo) != nil ||
			echo["org"] != probeTargetOrg.String() || echo["org_id"] != probeTargetOrg.String() ||
			echo["impersonating"] != probeTarget.String() || legacy.header.Get("X-Impersonating") != "true" {
			t.Errorf("cells off %s: %d %s, want the 200 echo served in the target org %s as %s", key, legacy.status, legacy.body,
				probeTargetOrg, probeTarget)
		}
		got := cellDo(on, key, onToken, nil)
		if method, _, _ := strings.Cut(key, " "); method == http.MethodGet {
			reads++
			if got.status != legacy.status || got.body != legacy.body {
				t.Errorf("cells on %s read: %d %s, want the cells-off answer %d %s", key, got.status, got.body, legacy.status, legacy.body)
			}
		} else {
			writes++
			if got.status != http.StatusForbidden || got.body != impersonationRefusalBody || got.header.Get("X-Impersonating") != "true" {
				t.Errorf("cells on %s write: %d %s X-Impersonating %q, want the enforced 403 %s after the scope applied the session (13211 item 6)",
					key, got.status, got.body, got.header.Get("X-Impersonating"), impersonationRefusalBody)
			}
		}
		if calls := pdp.calls(); len(calls) != 0 {
			t.Errorf("%s: %d decision point calls, want 0", key, len(calls))
		}
	}
	if reads != 24 || writes != 37 {
		t.Errorf("%d reads and %d writes, want 24 and 37", reads, writes)
	}
}
