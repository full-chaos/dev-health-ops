package billing

import (
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
)

var sessionOrgCases = []string{"no org_id", "org_id = the target", "org_id = another org"}

// orgFor is the org_id of orgCase for p; another org is one p is an admin of.
func (rig *routeRig) orgFor(p impersonator, orgCase string) *uuid.UUID {
	switch orgCase {
	case "org_id = the target":
		return &p.target
	case "org_id = another org":
		other := uuid.New()
		rig.store.roles[[2]uuid.UUID{p.id, other}] = "admin"
		return &other
	}
	return nil
}

// revokedImpersonator is an admin whose admin.impersonate was revoked while their go-api impersonation row is still
// live: admin.billing only, an admin membership in their own org and in the target.
func (rig *routeRig) revokedImpersonator(t *testing.T, live bool) impersonator {
	t.Helper()
	billingOnly := rule.PlatformOf(rule.ActionAdminBilling)
	return rig.impersonator(t, &billingOnly, "admin", live, false)
}

// CHAOS-13143 (acp P1): the write refusal follows the session state, not the caller's current actions. A caller
// with a live impersonation row whose admin.impersonate was revoked still gets the session refusal on every billing
// write, and no Stripe call is made.
func TestBillingWritesRefuseALiveRowAfterAdminImpersonateIsRevoked(t *testing.T) {
	for _, write := range billingWrites() {
		for _, orgCase := range sessionOrgCases {
			t.Run(write.name+"/"+orgCase, func(t *testing.T) {
				rig := newRouteRig(t)
				p := rig.revokedImpersonator(t, true)
				status, body, calls := rig.send(t, write, p.bearer, rig.orgFor(p, orgCase))
				if status != http.StatusForbidden || body != impersonationRefusalBody || len(calls) != 0 {
					t.Errorf("answer %d %s, %d Stripe call(s); want 403 %s, none", status, body, len(calls), impersonationRefusalBody)
				}
			})
		}
	}
}

// Control: once the row ends, the same caller gets the answer of a caller that never had a row.
func TestBillingWritesAfterTheRowEndsGetTheGuardAnswer(t *testing.T) {
	for _, write := range billingWrites() {
		t.Run(write.name, func(t *testing.T) {
			rig := newRouteRig(t)
			p := rig.revokedImpersonator(t, true)
			delete(rig.store.sessions, p.id)
			status, body, _ := rig.send(t, write, p.bearer, nil)
			twin := rig.revokedImpersonator(t, false)
			wantStatus, wantBody, _ := rig.send(t, write, twin.bearer, nil)
			wantBody = strings.ReplaceAll(wantBody, twin.own.String(), p.own.String())
			if body == impersonationRefusalBody || status != wantStatus || body != wantBody {
				t.Errorf("after the row ended: %d %s; never in a session: %d %s", status, body, wantStatus, wantBody)
			}
		})
	}
}

// CHAOS-13143 (acp P2): the session refusal is the outermost step of every billing write, so a session gets the one
// refusal body whatever the route guard would answer: a member-role caller on the four Admin routes too.
func TestBillingWritesRefuseASessionBeforeTheRouteGuard(t *testing.T) {
	impersonateOnly := rule.PlatformOf(rule.ActionAdminImpersonate)
	for _, write := range billingWrites() {
		for _, orgCase := range sessionOrgCases {
			t.Run(write.name+"/"+orgCase, func(t *testing.T) {
				rig := newRouteRig(t)
				p := rig.impersonator(t, &impersonateOnly, "admin", true, false)
				rig.store.roles[[2]uuid.UUID{p.id, p.own}] = "member"
				status, body, calls := rig.send(t, write, p.bearer, rig.orgFor(p, orgCase))
				if status != http.StatusForbidden || body != impersonationRefusalBody || len(calls) != 0 {
					t.Errorf("answer %d %s, %d Stripe call(s); want 403 %s, none", status, body, len(calls), impersonationRefusalBody)
				}
			})
		}
	}
}

// bodyFirstWrites are the billing writes whose guard parses the JSON body first.
func bodyFirstWrites() []billingCall {
	var out []billingCall
	for _, write := range billingWrites() {
		switch write.name {
		case "change-plan", "cancel", "refunds", "audit resolve", "checkout", "create plan", "update plan":
			write.body = "{"
			out = append(out, write)
		}
	}
	return out
}

// CHAOS-13143 (acp P2): a session gets the refusal before the body is parsed, so malformed JSON in a session is the
// session 403, not the 422.
func TestBillingBodyFirstWritesRefuseASessionBeforeTheBodyIsParsed(t *testing.T) {
	writes := bodyFirstWrites()
	if len(writes) != 7 {
		t.Fatalf("%d body-first writes, want 7", len(writes))
	}
	for _, write := range writes {
		for _, kind := range append(impersonatorKinds, impersonatorKind{"admin.billing only (admin.impersonate revoked)",
			func() *rule.Platform { p := rule.PlatformOf(rule.ActionAdminBilling); return &p }}) {
			for _, orgCase := range sessionOrgCases {
				t.Run(write.name+"/"+kind.name+"/"+orgCase, func(t *testing.T) {
					rig := newRouteRig(t)
					p := rig.impersonator(t, kind.platform(), "admin", true, false)
					status, body, calls := rig.send(t, write, p.bearer, rig.orgFor(p, orgCase))
					if status != http.StatusForbidden || body != impersonationRefusalBody || len(calls) != 0 {
						t.Errorf("answer %d %s, %d Stripe call(s); want 403 %s, none", status, body, len(calls), impersonationRefusalBody)
					}
				})
			}
		}
	}
}

// Outside a session, malformed JSON keeps today's 422.
func TestBillingBodyFirstWritesWithoutASessionKeepThe422(t *testing.T) {
	for _, write := range bodyFirstWrites() {
		t.Run(write.name, func(t *testing.T) {
			rig := newRouteRig(t)
			p := rig.impersonator(t, nil, "admin", false, false)
			status, body, calls := rig.send(t, write, p.bearer, nil)
			if status != http.StatusUnprocessableEntity || !strings.Contains(body, `"json_invalid"`) || len(calls) != 0 {
				t.Errorf("answer %d %s, %d Stripe call(s); want 422 json_invalid, none", status, body, len(calls))
			}
		})
	}
}

// A revoked caller's reads with a live row keep the answer they get with no row: the row moves no org and makes no
// acting user (the org swap stays behind admin.impersonate).
func TestBillingReadsOfARevokedCallerWithALiveRowKeepTheirAnswer(t *testing.T) {
	for _, orgCase := range []string{"no org_id", "org_id = the target"} {
		for _, read := range billingReads(uuid.New()) {
			t.Run(read.name+"/"+orgCase, func(t *testing.T) {
				rig := newRouteRig(t)
				answer := func(live bool) (int, string) {
					p := rig.revokedImpersonator(t, live)
					var org *uuid.UUID
					if orgCase == "org_id = the target" {
						org = &p.target
					}
					status, body, _ := rig.send(t, read, p.bearer, org)
					return status, body
				}
				status, body := answer(true)
				wantStatus, wantBody := answer(false)
				if status != wantStatus || body != wantBody {
					t.Errorf("live row: %d %s; no row: %d %s", status, body, wantStatus, wantBody)
				}
			})
		}
	}
}

// CHAOS-13143 (acp): pull-stripe's platform gate refuses on the live row too, whatever admin.impersonate says; with
// no row the platform gate answers as today (here the handler runs and the rig's missing pool answers).
func TestPullStripeRefusesALiveRowAfterAdminImpersonateIsRevoked(t *testing.T) {
	pull := billingCall{"pull-stripe", http.MethodPost, prefix + "/plans/pull-stripe", ""}
	for _, c := range []struct {
		name    string
		revoked bool
		live    bool
		refused bool
	}{
		{"admin.impersonate revoked, live row", true, true, true},
		{"admin.impersonate holder, live row (unchanged)", false, true, true},
		{"admin.impersonate revoked, no row", true, false, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			rig := newRouteRig(t)
			var p impersonator
			if c.revoked {
				p = rig.revokedImpersonator(t, c.live)
			} else {
				p = rig.impersonator(t, nil, "admin", c.live, false)
			}
			status, body, calls := rig.send(t, pull, p.bearer, nil)
			if refused := status == http.StatusForbidden && body == impersonationRefusalBody; refused != c.refused || len(calls) != 0 {
				t.Errorf("answer %d %s, %d Stripe call(s); want refused=%v, none", status, body, len(calls), c.refused)
			}
			if !c.refused && status != http.StatusInternalServerError {
				t.Errorf("answer %d %s; want the handler's 500 (no pool)", status, body)
			}
		})
	}
}
