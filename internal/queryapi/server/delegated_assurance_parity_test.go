package server

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// restGateProfiles are the query-api REST profile rows that reach the PDP gate (authenticateRESTRouted): the /api/v1
// analytics rows with an action. /graphql, /query and /buildinfo take other paths.
func restGateProfiles(t *testing.T) []struct{ method, route string } {
	t.Helper()
	var doc struct {
		Rows []struct {
			Method, Route, Action string
			Surface               string `json:"surface_kind"`
		} `json:"rows"`
	}
	if err := json.Unmarshal(authv1.QueryAPIEndpointProfiles, &doc); err != nil {
		t.Fatal(err)
	}
	var out []struct{ method, route string }
	for _, row := range doc.Rows {
		if row.Surface == "rest" && row.Action != "" && strings.HasPrefix(row.Route, "/api/v1/") {
			out = append(out, struct{ method, route string }{row.Method, row.Route})
		}
	}
	return out
}

// A delegated (act claim) bearer's read on every enforced query-api REST row gets the cells-off answer when the allow
// carries minimum_assurance at the production level of a session (session.AssuranceOf: aal1): the target's
// organization, org-wide.
func TestADelegatedReadWithAnAAL1AllowEqualsTheLegacyAnswerOnEveryRESTRow(t *testing.T) {
	quietStdLog(t)
	profiles := restGateProfiles(t)
	if len(profiles) < 25 {
		t.Fatalf("%d REST gate rows, want the query-api analytics rows (25 or more)", len(profiles))
	}
	var routes []string
	seen := map[string]bool{}
	for _, p := range profiles {
		if !seen[p.route] {
			seen[p.route] = true
			routes = append(routes, p.route)
		}
	}
	h := newDLPRigFor(t, session.AssuranceOf([]string{"pwd"}), routes)
	for _, p := range profiles {
		path := strings.NewReplacer("{person_id}", "x", "{work_unit_id}", "x").Replace(p.route)
		legacy, enforced := h.serveAs(p.method, path, h.legacy), h.serveAs(p.method, path, h.enforced)
		if enforced.code != legacy.code || withoutScopeKind(enforced.body) != withoutScopeKind(legacy.body) || legacy.code != http.StatusOK {
			t.Errorf("%s %s delegated, aal1: enforce-on %d %s, cells-off %d %s; want the cells-off 200", p.method, p.route, enforced.code, enforced.body, legacy.code, legacy.body)
		}
		if n := len(h.pdp.take()); n != 1 {
			t.Errorf("%s %s: %d PDP calls, want 1 (enforce-on only)", p.method, p.route, n)
		}
	}
}

// A level the session cannot meet stays no verdict: an aal3 allow is a 503 on every row of dlpRows (an unknown level
// is pinned at the gate, pdp_test.go).
func TestADelegatedReadWithAnAAL3AllowStaysUnavailableOnQueryAPI(t *testing.T) {
	quietStdLog(t)
	for _, level := range []string{"aal3"} {
		h := newDLPRig(t, level)
		for _, row := range dlpRows {
			if got := h.serve(row, h.enforced); got.code != http.StatusServiceUnavailable {
				t.Errorf("GET %s delegated, %s allow: %d %s, want 503", row, level, got.code, got.body)
			}
			h.pdp.take()
		}
	}
}
