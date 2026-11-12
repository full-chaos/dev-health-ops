package apiservice

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// handlerDecidedActions are the go-api rows CHAOS-13604 (part of CHAOS-13592) gives a catalog action. The target is the
// legacy answer for every caller class, status and body (product rule 1: the same answers as the legacy path).
var handlerDecidedActions = map[string]string{
	"GET /api/v1/orgs/me": "org.read",
	"POST /api/v1/acr/credentials/{credential_id}/revoke": "acr.credential.revoke",
}

const handlerDecidedNamespaces = "org,settings,acr"

// handlerDecidedDo is cellDo, except the revoke route takes no body (EmptyBody: any body is a 413).
func handlerDecidedDo(handler http.Handler, key, token string, headers map[string]string) cellAnswer {
	method, _, _ := strings.Cut(key, " ")
	request := httptest.NewRequest(method, cellPath(key), nil)
	request.Header.Set("Authorization", "Bearer "+token)
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return cellAnswer{status: recorder.Code, body: recorder.Body.String(), header: recorder.Header()}
}

func TestHandlerDecidedRowsMatchTheProfile(t *testing.T) {
	got := map[string]string{}
	for _, row := range loadGoAPIRows(t) {
		route, _ := row["route"].(string)
		method, _ := row["method"].(string)
		key := method + " " + route
		if _, ok := handlerDecidedActions[key]; ok && row["service"] == goAPIAccessService {
			got[key], _ = row["action"].(string)
		}
	}
	for key, want := range handlerDecidedActions {
		if got[key] != want {
			t.Errorf("%s: profile action %q, table %q", key, got[key], want)
		}
	}
}

// For each row and caller class, the answer with the org cell on (the real evaluator decides) equals the legacy answer
// (cells off), on the real handler: status and body. The pool is closed, so a caller the guard and the handler's role
// check let through answers the handler's database 500.
func TestHandlerDecidedRowsAnswerEveryCallerClassAsLegacy(t *testing.T) {
	pdp := newCellPDP(t)
	off, offMinter := adminConfigServerScoped(t, nil, withBoundaryCallers, realRoutes, nil)
	on, onMinter := adminConfigServerScoped(t, func(t *testing.T) *policy.Enforcer {
		return newCellEnforcer(t, pdp, handlerDecidedNamespaces, false)
	}, withBoundaryCallers, realRoutes, nil)
	offCallers, onCallers := boundaryCallers(offMinter), boundaryCallers(onMinter)
	for key, action := range handlerDecidedActions {
		for _, class := range boundaryClasses {
			pdp.register(onCallers)
			pdp.reset("evaluate")
			legacy := handlerDecidedDo(off, key, offCallers[class].token, nil)
			if calls := pdp.calls(); len(calls) != 0 {
				t.Fatalf("%s / %s: %d decision point calls with the cell off", key, class, len(calls))
			}
			got := handlerDecidedDo(on, key, onCallers[class].token, nil)
			calls := pdp.calls()
			result := "EQUAL"
			if got.status != legacy.status || got.body != legacy.body {
				result = "DIFF"
				t.Errorf("%s / %s: new %d %s, legacy %d %s", key, class, got.status, got.body, legacy.status, legacy.body)
			}
			t.Logf("CELL\t%s\t%s\tlegacy=%d:%s\tnew=%d:%s\t%s", key, class, legacy.status, short(legacy.body), got.status, short(got.body), result)
			if len(calls) != 1 || calls[0].Action != action || calls[0].OrganizationID != probeOrg.String() {
				t.Errorf("%s / %s: calls %+v, want one %s call on %s", key, class, calls, action, probeOrg)
			}
		}
	}
}

func TestHandlerDecidedRowsFailClosed(t *testing.T) {
	pdp := newCellPDP(t)
	on, minter := adminConfigServerScoped(t, func(t *testing.T) *policy.Enforcer {
		return newCellEnforcer(t, pdp, handlerDecidedNamespaces, false)
	}, withBoundaryCallers, realRoutes, nil)
	admin := boundaryCallers(minter)["org admin"].token
	for key := range handlerDecidedActions {
		pdp.reset("fail")
		got := handlerDecidedDo(on, key, admin, nil)
		if got.status != http.StatusServiceUnavailable || got.header.Get("Retry-After") != "5" {
			t.Errorf("%s: %d %s, want 503 Retry-After 5", key, got.status, got.body)
		}
	}
}
