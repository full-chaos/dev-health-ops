package server

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// anonymousProbe answers one request with no credential on a listener and
// returns the status. A listener the caller cannot reach is not in the map.
// anonymousProbeMethods is every method an api_route row is probed with: the
// row registers a runtime method list, so any of them can open on its own.
var anonymousProbeMethods = []string{http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete, http.MethodOptions}

type anonymousProbe func(method, path string) int

// anonymousClaimViolations compares each row's classification with what the
// listener's handler does to a request carrying no credential: a protected row
// must be refused with 401, a public row must not be. It is the check that
// stops a row from being relabelled in either direction without the served
// behaviour agreeing (CHAOS-4780). skip names routes the caller cannot reach
// through its harness; the caller owns proving them elsewhere.
func anonymousClaimViolations(rows []profileRow, probes map[string]anonymousProbe, skip map[string]bool) []string {
	var violations []string
	for _, row := range rows {
		if skip[row.route] {
			continue
		}
		methods := []string{row.method}
		if row.method == "api_route" {
			methods = anonymousProbeMethods
		}
		path := pathParameter.ReplaceAllString(row.route, "x")
		for _, listener := range row.listeners {
			probe, ok := probes[listener]
			if !ok {
				continue
			}
			var bad, codes []string
			for _, method := range methods {
				code := probe(method, path)
				if (row.public && code == http.StatusUnauthorized) || (!row.public && code != http.StatusUnauthorized) {
					bad = append(bad, method)
					codes = append(codes, strconv.Itoa(code))
				}
			}
			if len(bad) == 0 {
				continue
			}
			if row.public {
				violations = append(violations, fmt.Sprintf("%s on %s: row is public but an anonymous %s %s is refused with 401", row.id, listener, strings.Join(bad, "/"), path))
			} else {
				violations = append(violations, fmt.Sprintf("%s on %s: row is protected but an anonymous %s %s answers %s, not 401", row.id, listener, strings.Join(bad, "/"), path, strings.Join(codes, "/")))
			}
		}
	}
	sort.Strings(violations)
	return violations
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

func apiRouteRow(public bool) []profileRow {
	return []profileRow{{id: "api_route /r [@query]", method: "api_route", route: "/r", listeners: []string{"query"}, public: public}}
}

func byMethod(codes map[string]int, def int) anonymousProbe {
	return func(method, _ string) int {
		if code, ok := codes[method]; ok {
			return code
		}
		return def
	}
}

func TestAnonymousClaimViolationsProbesEveryMethodOfAPIRoute(t *testing.T) {
	probes := func(p anonymousProbe) map[string]anonymousProbe { return map[string]anonymousProbe{"query": p} }
	got := anonymousClaimViolations(apiRouteRow(false), probes(byMethod(map[string]int{"POST": 200}, 401)), nil)
	if len(got) != 1 || !strings.Contains(got[0], "POST") || !strings.Contains(got[0], "protected but an anonymous") {
		t.Fatalf("protected row open on POST: got %v", got)
	}
	if got := anonymousClaimViolations(apiRouteRow(false), probes(byMethod(nil, 401)), nil); len(got) != 0 {
		t.Fatalf("protected row refusing every method reported: %v", got)
	}
	got = anonymousClaimViolations(apiRouteRow(true), probes(byMethod(map[string]int{"DELETE": 401}, 200)), nil)
	if len(got) != 1 || !strings.Contains(got[0], "DELETE") || !strings.Contains(got[0], "public but an anonymous") {
		t.Fatalf("public row refusing DELETE: got %v", got)
	}
	got = anonymousClaimViolations(apiRouteRow(false), probes(byMethod(map[string]int{"POST": 200, "PUT": 204}, 401)), nil)
	if len(got) != 1 || !strings.Contains(got[0], "POST/PUT") || !strings.Contains(got[0], "200/204") {
		t.Fatalf("row open on POST and PUT: want one line naming both, got %v", got)
	}
	var seen []string
	anonymousClaimViolations(seededRows()[:1], probes(func(method, _ string) int { seen = append(seen, method); return 401 }), nil)
	if len(seen) != 1 || seen[0] != "GET" {
		t.Fatalf("a GET row was probed with %v", seen)
	}
}

// flatRows is every row of the query-api inventory.
func flatRows(t *testing.T) []profileRow {
	t.Helper()
	var rows []profileRow
	for _, perRoute := range queryAPIRowsByRoute(t) {
		rows = append(rows, perRoute...)
	}
	return rows
}

// TestEndpointProfileAnonymousClaimsMatchServedBehaviour: for every non-REST
// row on a listener the harness reaches (the proof and run-operation routes
// included: they are mounted with the real /query handler), an anonymous
// request is refused when the row is protected and is not refused when it is
// public. REST rows are not probed here: CHAOS-10140.
func TestEndpointProfileAnonymousClaimsMatchServedBehaviour(t *testing.T) {
	stack := newBehaviourStack(t)
	restPaths := map[string]bool{"/api/v1/": true}
	for _, route := range RESTRoutes() {
		restPaths[route.Pattern] = true
	}
	var rows []profileRow
	for _, row := range flatRows(t) {
		if !restPaths[row.route] {
			rows = append(rows, row)
		}
	}
	if len(rows) < 8 {
		t.Fatalf("only %d non-REST rows: the inventory or the REST table changed shape", len(rows))
	}
	probes := map[string]anonymousProbe{}
	for _, listener := range []string{"query", "internal", "mcp", "operator"} {
		listener := listener
		probes[listener] = func(method, path string) int { return stack.do(listener, method, path, "") }
	}
	if violations := anonymousClaimViolations(rows, probes, nil); len(violations) > 0 {
		t.Errorf("rows disagree with the anonymous behaviour of the served handlers:\n%s", strings.Join(violations, "\n"))
	}
}
