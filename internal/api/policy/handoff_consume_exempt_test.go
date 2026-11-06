package policy

import (
	"context"
	"errors"
	"maps"
	"net/http"
	"slices"
	"testing"
	"time"
)

const handoffConsumeRoute = "/api/v1/admin/impersonate/handoff/consume"

// statusPatternVariants are route templates that are not the status route's.
var statusPatternVariants = []string{
	"HEAD /api/v1/admin/impersonate/status",
	"POST /api/v1/admin/impersonate/status",
	"GET /api/v1/admin/impersonate/status/",
	"GET /api/v1/admin/impersonate/statusx",
}

// handoffConsumePatternVariants are route templates that are not the consume route's.
var handoffConsumePatternVariants = []string{
	"POST /api/v1/admin/impersonate/handoff/consume/",
	"POST /api/v1/admin/impersonate/handoff/consumex",
	"POST /api/v1/admin/impersonate/handoff/",
	"POST /api/v1/admin/impersonate/handoff",
	"POST /api/v1/admin/impersonate/handoff/{action}",
	"POST /api/v1/admin/Impersonate/handoff/consume",
	"GET /api/v1/admin/impersonate/handoff/consume",
	"PUT /api/v1/admin/impersonate/handoff/consume",
	"DELETE /api/v1/admin/impersonate/handoff/consume",
}

func TestTheLiveRowExemptSetIsStatusStopAndTheHandoffConsume(t *testing.T) {
	got := slices.Sorted(maps.Keys(liveRowExemptPatterns))
	want := []string{statusImpersonationPattern, ImpersonationHandoffConsumePattern, stopImpersonationPattern}
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("live-row exempt set %v, want %v", got, want)
	}
	if ImpersonationHandoffConsumePattern != "POST "+handoffConsumeRoute {
		t.Fatalf("consume pattern %q", ImpersonationHandoffConsumePattern)
	}
	for pattern, exempt := range liveRowExemptPatterns {
		if !exempt || !liveRowExempt(pattern) {
			t.Fatalf("%q is in the set but not exempt", pattern)
		}
	}
	for _, v := range append(append(slices.Clone(handoffConsumePatternVariants), stopPatternVariants...), statusPatternVariants...) {
		if liveRowExempt(v) {
			t.Fatalf("variant %q is exempt", v)
		}
	}
}

// The legacy guard (no enforcer): a holder of admin.impersonate with a live session reaches the consume route; every
// other platform route is still refused in the session.
func TestTheHandoffConsumeRouteIsReachableWithALiveSessionInTheLegacyGuard(t *testing.T) {
	guard := NewGuard(routed(t, personaStore(personaSuperuser), &fakeValidator{result: userClaims(nil)}), quiet())
	mux := http.NewServeMux()
	mux.Handle(ImpersonationHandoffConsumePattern, guard.WrapPlatform(ActionAdminImpersonate, echo))
	mux.Handle("POST /api/v1/admin/impersonate", guard.WrapPlatform(ActionAdminImpersonate, echo))
	mux.Handle("GET /api/v1/admin/orgs", guard.WrapPlatform(ActionAdminOrgs, echo))
	if got := enforceRequest(impersonating(mux), http.MethodPost, handoffConsumeRoute); got.status != http.StatusOK {
		t.Errorf("consume with a live session: %d %q, want 200", got.status, got.body)
	}
	for _, cell := range []struct{ method, path string }{
		{http.MethodPost, "/api/v1/admin/impersonate"},
		{http.MethodGet, "/api/v1/admin/orgs"},
	} {
		got := enforceRequest(impersonating(mux), cell.method, cell.path)
		if got.status != http.StatusForbidden || got.body != impersonationRefusalBody {
			t.Errorf("%s %s with a live session: %d %q, want 403 %q", cell.method, cell.path, got.status, got.body, impersonationRefusalBody)
		}
	}
}

// A revoked impersonator (live row, no admin.impersonate) on a platform-wrapped consume route is refused by the
// platform gate: the exemption does not open the route to a caller without the action.
func TestTheHandoffConsumeExemptionNeedsTheAction(t *testing.T) {
	guard := NewGuard(routed(t, revokedImpersonatorStore(true, nil), &fakeValidator{result: userClaims(nil)}), quiet())
	mux := http.NewServeMux()
	mux.Handle(ImpersonationHandoffConsumePattern, guard.WrapPlatform(ActionAdminImpersonate, echo))
	if got := enforceRequest(mux, http.MethodPost, handoffConsumeRoute); got.status != http.StatusForbidden {
		t.Errorf("revoked impersonator: %d %q, want 403", got.status, got.body)
	}
}

func consumeRouteHandler(t *testing.T, store *accessStore, enforced bool) (http.Handler, *testPDP) {
	t.Helper()
	pdp := newTestPDP(t, allowAll)
	rows := maps.Clone(shadowTestRows)
	rows[ImpersonationHandoffConsumePattern] = stopRouteRow(t)
	guard := NewGuard(routed(t, store, &fakeValidator{result: userClaims(nil)}), quiet())
	if enforced {
		enf, err := NewEnforcer(EnforceConfig{Decider: pdp.client(t), Rows: rows, Switch: onFor("admin"), Timeout: time.Second, Logger: quiet()})
		if err != nil {
			t.Fatal(err)
		}
		guard.SetEnforcer(enf)
	}
	mux := http.NewServeMux()
	mux.Handle(ImpersonationHandoffConsumePattern, guard.WrapPlatform(ActionAdminImpersonate, echo))
	return mux, pdp
}

// Enforced: the consume route of an impersonating admin is decided by the decision point, as the stop route is.
func TestTheHandoffConsumeRouteOfAnImpersonatingAdminIsDecidedByThePDP(t *testing.T) {
	rec := recordEnforce(t)
	h, pdp := consumeRouteHandler(t, personaStore(personaSuperuser), true)
	got := enforceRequest(impersonating(h), http.MethodPost, handoffConsumeRoute)
	if got.status != http.StatusOK || len(pdp.calls()) != 1 || !slices.Equal(rec.snapshot(), []string{"admin/allow"}) {
		t.Fatalf("enforced consume: %d %q, %d PDP calls, counted %v; want 200, 1, [admin/allow]", got.status, got.body, len(pdp.calls()), rec.snapshot())
	}
}

// Enforced: the consume route reads no row in enforceVerdict, as the stop route does (a failing row read is not
// reached).
func TestTheHandoffConsumeRouteReadsNoRowUnderEnforcement(t *testing.T) {
	recordEnforce(t)
	store := revokedImpersonatorStore(true, errors.New("impersonation row read failed"))
	h, pdp := consumeRouteHandler(t, store, true)
	got := enforceRequest(h, http.MethodPost, handoffConsumeRoute)
	if got.status != http.StatusOK || len(pdp.calls()) != 1 || store.sessionCalls != 0 {
		t.Fatalf("consume route: %d %q, %d PDP calls, %d row reads; want 200, 1, 0", got.status, got.body, len(pdp.calls()), store.sessionCalls)
	}
}

// decide: only the exact consume template is exempt; every variant refuses an impersonating caller unasked.
func TestTheHandoffConsumeExemptionIsTheExactTemplateOnly(t *testing.T) {
	rows := maps.Clone(shadowTestRows)
	rows[ImpersonationHandoffConsumePattern] = stopRouteRow(t)
	for _, v := range handoffConsumePatternVariants {
		rows[v] = stopRouteRow(t)
	}
	for _, pattern := range append([]string{ImpersonationHandoffConsumePattern}, handoffConsumePatternVariants...) {
		t.Run(pattern, func(t *testing.T) {
			rec := recordEnforce(t)
			pdp := newTestPDP(t, allowAll)
			enf, err := NewEnforcer(EnforceConfig{Decider: pdp.client(t), Rows: rows, Switch: onFor("admin"), Timeout: time.Second, Logger: quiet()})
			if err != nil {
				t.Fatal(err)
			}
			got := enf.decide(context.Background(), enforceObservation{pattern: pattern, bearer: shadowBearer, userAccessToken: true, impersonating: true})
			want, wantCalls, wantCounted := enforceImpersonationRefused, 0, "admin/impersonation_refused"
			if pattern == ImpersonationHandoffConsumePattern {
				want, wantCalls, wantCounted = enforceAllow, 1, "admin/allow"
			}
			if got != want || len(pdp.calls()) != wantCalls || !slices.Equal(rec.snapshot(), []string{wantCounted}) {
				t.Errorf("template %q: verdict %d, %d PDP calls, counted %v; want %d, %d, [%s]", pattern, got, len(pdp.calls()), rec.snapshot(), want, wantCalls, wantCounted)
			}
		})
	}
}

// The shadow's verdict on the consume route agrees with the guard: asked, not impersonation_refused.
func TestShadowTheHandoffConsumeRouteOfAnImpersonatingAdminIsAsked(t *testing.T) {
	rows := maps.Clone(shadowTestRows)
	rows[ImpersonationHandoffConsumePattern] = stopRouteRow(t)
	for _, v := range handoffConsumePatternVariants {
		rows[v] = stopRouteRow(t)
	}
	for _, pattern := range append([]string{ImpersonationHandoffConsumePattern}, handoffConsumePatternVariants...) {
		t.Run(pattern, func(t *testing.T) {
			rec := recordShadow(t)
			pdp := newTestPDP(t, allowAll)
			sh, _ := newShadowFor(t, pdp.client(t), func(c *ShadowConfig) { c.Rows = rows })
			sh.Observe(context.Background(), ShadowObservation{Pattern: pattern, Bearer: shadowBearer, Caller: ShadowCallerSuperuser,
				UserAccessToken: true, Impersonating: true, LegacyAllowed: true})
			sh.wg.Wait()
			want, wantCalls := []string{"impersonation_refused"}, 0
			if pattern == ImpersonationHandoffConsumePattern {
				want, wantCalls = []string{"match"}, 1
			}
			if _, results := rec.snapshot(); !slices.Equal(results, want) || len(pdp.calls()) != wantCalls {
				t.Errorf("template %q: results %v, %d PDP calls; want %v, %d", pattern, results, len(pdp.calls()), want, wantCalls)
			}
		})
	}
}
