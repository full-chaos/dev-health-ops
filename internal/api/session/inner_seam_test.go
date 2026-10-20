package session

import (
	"net/http"
	"testing"
)

// Inner is called once per route, in route order, with that route's own
// pattern: a pattern typed wrong beside its route would hand the proxy the
// wrong aggregate.
func TestInnerSeamNamesEveryRouteWithItsOwnPattern(t *testing.T) {
	var seen []string
	deps := completeDeps(t)
	deps.Inner = func(pattern string, local http.Handler) http.Handler {
		if local == nil {
			t.Errorf("Inner got no local handler for %s", pattern)
		}
		seen = append(seen, pattern)
		return local
	}
	routes := Routes(deps)
	if len(seen) != len(routes) {
		t.Fatalf("Inner called %d times for %d routes", len(seen), len(routes))
	}
	for i, route := range routes {
		if seen[i] != route.Pattern {
			t.Errorf("route %d %s %s: Inner named %s", i, route.Method, route.Pattern, seen[i])
		}
	}
}
