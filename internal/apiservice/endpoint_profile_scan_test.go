package apiservice

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/routescan"
)

// registration is one route registration found in the source (routescan.Registration).
type registration = routescan.Registration

// scanRegistrations returns every route registration under dirs (repo-relative to internal/), as routescan.Scan
// finds them.
func scanRegistrations(t *testing.T, dirs ...string) []registration {
	t.Helper()
	regs, err := routescan.Scan(dirs...)
	if err != nil {
		t.Fatal(err)
	}
	return regs
}

// registrationFor returns the registration that mounts m: a resolved pattern
// equal to the route's, else an unresolved one whose literal head or tail the
// route carries; a registration with no method matches any method.
func registrationFor(regs []registration, m mountedRoute) (registration, bool) {
	probe := false
	for _, pattern := range httpapi.ProbePaths {
		probe = probe || pattern == m.Pattern
	}
	var fallback *registration
	for i := range regs {
		reg := regs[i]
		if reg.Method != "" && reg.Method != m.Method {
			continue
		}
		switch {
		case reg.Pattern != "" && reg.Pattern == m.Pattern:
			return reg, true
		case reg.Pattern == "" && reg.Head != "" && strings.HasPrefix(m.Pattern, reg.Head):
			fallback = &regs[i]
		case reg.Pattern == "" && reg.Tail != "" && strings.HasSuffix(m.Pattern, reg.Tail) && fallback == nil:
			fallback = &regs[i]
		case reg.Pattern == "" && reg.Head == "" && reg.Tail == "" && probe && strings.HasSuffix(reg.File, "health/health.go"):
			fallback = &regs[i]
		}
	}
	if fallback != nil {
		return *fallback, true
	}
	return registration{}, false
}
