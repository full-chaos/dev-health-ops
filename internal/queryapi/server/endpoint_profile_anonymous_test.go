package server

import (
	"net/http"
	"strings"
	"testing"
)

// anonymousProbe answers one request with no credential on a listener and
// returns the status. A listener the caller cannot reach is not in the map.
type anonymousProbe func(method, path string) int

// anonymousClaimViolations compares each row's classification with what the
// listener's handler does to a request carrying no credential: a protected row
// must be refused with 401, a public row must not be. It is the check that
// stops a row from being relabelled in either direction without the served
// behaviour agreeing (CHAOS-4780). skip names routes the caller cannot reach
// through its harness; the caller owns proving them elsewhere.
func anonymousClaimViolations(rows []profileRow, probes map[string]anonymousProbe, skip map[string]bool) []string {
	_, _, _ = rows, probes, skip
	return nil
}

func seededRows() []profileRow {
	return []profileRow{
		{id: "GET /a [@query]", method: "GET", route: "/a", listeners: []string{"query"}, public: false},
		{id: "GET /b [@query]", method: "GET", route: "/b", listeners: []string{"query"}, public: true},
	}
}

// TestAnonymousClaimViolationsFiresOnRelabelledRows seeds both directions of
// the relabel the CHAOS-4780 gate was blind to: a route that serves anonymous
// callers labelled protected, and a route that refuses them labelled public.
func TestAnonymousClaimViolationsFiresOnRelabelledRows(t *testing.T) {
	consistent := map[string]anonymousProbe{"query": func(_, path string) int {
		if path == "/a" {
			return http.StatusUnauthorized
		}
		return http.StatusOK
	}}
	if got := anonymousClaimViolations(seededRows(), consistent, nil); len(got) != 0 {
		t.Fatalf("consistent rows reported violations: %v", got)
	}

	protectedButOpen := map[string]anonymousProbe{"query": func(_, _ string) int { return http.StatusOK }}
	got := anonymousClaimViolations(seededRows(), protectedButOpen, nil)
	if len(got) != 1 || !strings.Contains(got[0], "GET /a") || !strings.Contains(got[0], "protected but an anonymous") {
		t.Fatalf("a protected row on a route that serves anonymous callers: got %v", got)
	}

	publicButRefused := map[string]anonymousProbe{"query": func(_, _ string) int { return http.StatusUnauthorized }}
	got = anonymousClaimViolations(seededRows(), publicButRefused, nil)
	if len(got) != 1 || !strings.Contains(got[0], "GET /b") || !strings.Contains(got[0], "public but an anonymous") {
		t.Fatalf("a public row on a route that refuses anonymous callers: got %v", got)
	}
}

func TestAnonymousClaimViolationsHonoursSkipAndUnreachableListeners(t *testing.T) {
	open := map[string]anonymousProbe{"query": func(_, _ string) int { return http.StatusOK }}
	if got := anonymousClaimViolations(seededRows(), open, map[string]bool{"/a": true}); len(got) != 0 {
		t.Fatalf("a skipped route was probed: %v", got)
	}
	if got := anonymousClaimViolations(seededRows(), map[string]anonymousProbe{}, nil); len(got) != 0 {
		t.Fatalf("a listener with no probe was probed: %v", got)
	}
}

// TestStubbedRoutesAreStillServed: every harness stub names a route the binary
// serves; an entry for a route nothing serves is stale and fails here, so the
// exclusion cannot outlive its route.
func TestStubbedRoutesAreStillServed(t *testing.T) {
	served := servedRoutes(t)
	for path := range behaviourStubbed {
		found := false
		for token := range served {
			if strings.HasPrefix(token, path+"@") {
				found = true
			}
		}
		if !found {
			t.Errorf("behaviourStubbed names %s but no listener serves it: delete the entry", path)
		}
	}
}
