package server

import (
	"net/http"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp/pdptest"
)

// acp ruling (2): a route whose 5.17 cell is on is enforced, so the PDP decision is the answer. The REST shadow
// skips it: exactly one PDP call, no 5.12 result, no mismatch count.
func TestRESTShadowSkipsARouteWhoseCellIsOn(t *testing.T) {
	srv := pdptest.New(t)
	g := newGatedREST(t, pdp.ModeShadow, "analytics", srv.URL, true)
	before := shadowCounters(t)
	if got := g.do(http.MethodGet, g.userToken()); got.Status != http.StatusOK {
		t.Fatalf("status %d, want 200", got.Status)
	}
	if n := len(srv.Requests()); n != 1 {
		t.Fatalf("PDP asked %d times, want exactly 1", n)
	}
	if d := settleQuiet(t, before); len(d) != 0 {
		t.Fatalf("5.12 counters moved on an enforced route: %v", d)
	}
}

// Cell off and shadow mode: one shadow ask, the legacy answer served.
func TestRESTShadowAsksOnceWhenTheCellIsOff(t *testing.T) {
	srv := pdptest.New(t)
	g := newGatedREST(t, pdp.ModeShadow, "", srv.URL, true)
	before := shadowCounters(t)
	if got := g.do(http.MethodGet, g.userToken()); got.Status != http.StatusOK {
		t.Fatalf("status %d, want 200", got.Status)
	}
	want := map[string]int64{shadowResultsMetric + "|reason=none,result=match": 1}
	if d := settle(t, before, want); len(d) != 1 || d[shadowResultsMetric+"|reason=none,result=match"] != 1 {
		t.Fatalf("5.12 counters %v, want %v", d, want)
	}
	if n := len(srv.Requests()); n != 1 {
		t.Fatalf("PDP asked %d times, want 1", n)
	}
}
