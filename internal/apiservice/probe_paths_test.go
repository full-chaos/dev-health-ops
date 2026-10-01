package apiservice

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/billing"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// probeSegments are the last path segments that mark a route as a liveness or
// readiness probe by convention.
var probeSegments = map[string]bool{
	"health": true, "healthz": true, "ready": true, "readyz": true,
	"live": true, "livez": true, "metrics": true, "ping": true,
}

// tracedHealthRoutes are routes whose last segment looks like a probe but that
// are NOT probes, so they stay traced (they are not in httpapi.ProbePaths).
// Each needs a reason; a new entry is a decision, not a convenience.
var tracedHealthRoutes = map[string]string{
	"/api/v1/webhooks/health":     "webhook-intake status endpoint read by operators, not polled by the kubelet",
	"/api/v1/internal/acr/health": "acr's entitlement client health check: a real caller whose latency and failures are worth a span",
}

// TestEveryProbeRouteOfEveryListenerIsInTheProbeTable walks the routes each
// listener registers and fails on a probe-shaped route that is neither in
// httpapi.ProbePaths (untraced) nor deliberately kept traced above. A new probe
// route would otherwise emit a span for every kubelet probe.
func TestEveryProbeRouteOfEveryListenerIsInTheProbeTable(t *testing.T) {
	inTable := map[string]bool{}
	for _, probe := range httpapi.ProbePaths {
		inTable[probe] = true
	}
	listeners := map[string][]httpapi.Route{
		"public":       Routes(Deps{}, quietLog()),
		"internal":     InternalRoutes(Deps{}, quietLog()),
		"billing-edge": billing.EdgeRoutes(billing.Deps{}),
	}
	probesSeen := map[string]int{}
	for listener, routes := range listeners {
		if len(routes) == 0 {
			t.Fatalf("%s listener registered no routes: the walk examined nothing", listener)
		}
		for _, route := range routes {
			segments := strings.Split(strings.Trim(route.Pattern, "/"), "/")
			last := segments[len(segments)-1]
			if inTable[route.Pattern] {
				probesSeen[route.Pattern]++
				continue
			}
			if !probeSegments[last] {
				continue
			}
			switch {
			case tracedHealthRoutes[route.Pattern] != "":
			default:
				t.Errorf("%s listener registers probe-shaped route %s %s that is not in httpapi.ProbePaths: add it there, or to tracedHealthRoutes with a reason", listener, route.Method, route.Pattern)
			}
		}
	}
	for _, probe := range httpapi.ProbePaths {
		if probesSeen[probe] == 0 {
			t.Errorf("ProbePaths entry %s is registered by no listener: the exclusion is dead", probe)
		}
	}
}
