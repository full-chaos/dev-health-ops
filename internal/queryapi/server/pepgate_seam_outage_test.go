package server

import (
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/pep"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp/pdptest"
)

// The seam routes hold one route of every action namespace of query-api's REST rows.
func TestSeamRoutesCoverEveryRESTNamespace(t *testing.T) {
	rows, err := pdp.RESTShadowRows(authv1.QueryAPIEndpointProfiles)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{}
	for _, row := range rows {
		want[row.Namespace] = true
	}
	got := map[string]bool{}
	for _, route := range seamRoutes() {
		got[namespaceOf(route.action)] = true
	}
	if !maps.Equal(got, want) {
		t.Fatalf("seam namespaces %v, want the REST namespaces %v", slices.Sorted(maps.Keys(got)), slices.Sorted(maps.Keys(want)))
	}
}

// On every namespace, with its cell on, a PDP outage (the listener gone, or a 503 with no decision) refuses the read
// with a 503 before the handler: no read runs. The PDP is asked once, so the refusal is the outage's.
func TestSeamRESTPDPOutageRunsNoRead(t *testing.T) {
	gone := httptest.NewServer(http.NotFoundHandler())
	goneURL := gone.URL
	gone.Close()
	failing := pdptest.New(t)
	failing.Fail(http.StatusServiceUnavailable)
	for _, route := range seamRoutes() {
		for name, base := range map[string]string{"pdp gone": goneURL, "pdp 503": failing.URL} {
			t.Run(route.name+"/"+name, func(t *testing.T) {
				s := newSeamStack(t, route)
				client, err := pep.New(pep.Config{BaseURL: base, AllowPlainHTTP: true, HTTPClient: &http.Client{}, ClientService: pdp.ClientService, Timeout: time.Second})
				if err != nil {
					t.Fatal(err)
				}
				decider := &seamDecider{Decider: client}
				gate, _ := seamGate(t, pdp.ModeEnforce, namespaceOf(route.action), decider)
				got := s.run(gate, s.user)
				if (answer{got.status, strings.TrimSpace(got.body)}) != gatedUnavailable {
					t.Fatalf("answered %d %q, want %+v", got.status, got.body, gatedUnavailable)
				}
				if len(got.statements) != 0 || len(s.seen) != 0 {
					t.Fatalf("a refused request reached the handler: %d statement(s), handler ran %d time(s)", len(got.statements), len(s.seen))
				}
				if n := decider.calls.Load(); n != 1 {
					t.Fatalf("%d PDP asks, want 1", n)
				}
			})
		}
	}
}
