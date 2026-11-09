package apiservice

import (
	"encoding/json"
	"net/http"
	"slices"
	"sort"
	"strings"
	"testing"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// serviceAccountKeyKeys are the service account key rows, mounted WrapPlatform(admin.users) in
// admin/serviceaccount_keys.go.
var serviceAccountKeyKeys = []string{
	"GET /api/v1/admin/service-accounts/{service_account_id}/keys",
	"POST /api/v1/admin/service-accounts/keys/{credential_id}/revoke",
	"POST /api/v1/admin/service-accounts/{service_account_id}/keys",
	"POST /api/v1/admin/service-accounts/{service_account_id}/keys/rotate",
}

// The profile names admin.users on exactly the four service account key rows, and the guard's action map reads each
// as a platform row.
func TestServiceAccountKeyRowsNameAdminUsers(t *testing.T) {
	var document struct {
		Rows []struct {
			Service, Method, Route string
			Action                 *string
		} `json:"rows"`
	}
	if err := json.Unmarshal(authv1.GoAPIEndpointProfiles, &document); err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, row := range document.Rows {
		if row.Service != goAPIAccessService || !strings.HasPrefix(row.Route, "/api/v1/admin/service-accounts/") {
			continue
		}
		action := ""
		if row.Action != nil {
			action = *row.Action
		}
		got[row.Method+" "+row.Route] = action
	}
	keys := make([]string, 0, len(got))
	for key := range got {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if !slices.Equal(keys, serviceAccountKeyKeys) {
		t.Fatalf("service account key rows %v, want %v", keys, serviceAccountKeyKeys)
	}
	rows, err := policy.ParseShadowRows(authv1.GoAPIEndpointProfiles, goAPIAccessService)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range serviceAccountKeyKeys {
		if got[key] != policy.ActionAdminUsers {
			t.Errorf("%s: action %q, want %s", key, got[key], policy.ActionAdminUsers)
		}
		if row, ok := rows[key]; !ok || row.Action != policy.ActionAdminUsers || row.ResourceType != "platform" {
			t.Errorf("%s: guard row %+v (present %v), want admin.users on the platform", key, row, ok)
		}
	}
}

const superuserRequiredBody = `{"detail":"Superuser access required"}`

// serviceAccountKeyLegacyClass is the legacy answer class of each caller on the key rows: "handler" (the guard let it
// through; the rig's key switch is off, so the handler answers 503 config_missing), or the exact refusal.
var serviceAccountKeyLegacyClass = map[string]string{
	"platform admin, no membership":  "handler",
	"platform admin, no org claim":   "handler",
	"platform admin + org member":    "handler",
	"org owner":                      superuserRequiredBody,
	"org admin":                      superuserRequiredBody,
	"org member":                     superuserRequiredBody,
	"org viewer":                     superuserRequiredBody,
	"no platform role, no org claim": orgContextBody,
	"impersonating platform admin":   impersonationBody,
	"retired HS256 token":            "401",
	"service key bearer":             "401",
	"anonymous":                      "401",
}

func serviceAccountKeyClass(a cellAnswer) string {
	switch {
	case a.status == http.StatusUnauthorized:
		return "401"
	case a.body == superuserRequiredBody, a.body == orgContextBody, a.body == impersonationBody:
		return a.body
	case strings.Contains(a.body, "Authorization service temporarily unavailable"):
		return "503 no verdict"
	}
	return "handler"
}

// Every caller class on every key row gets the same status and body with no Enforcer, with the admin cell off and
// with it on (the decision point in evaluate mode), and that answer is the legacy class. Each cell is logged as
// "CELL route|caller|base|off|on" so a run on the base commit records today's answers.
func TestServiceAccountKeyRowsAnswerAsLegacyWithTheAdminCellOn(t *testing.T) {
	pdp := newCellPDP(t)
	type server struct {
		h       http.Handler
		callers []usersCaller
	}
	serve := func(namespaces string) server {
		var enforcer func(*testing.T) *policy.Enforcer
		if namespaces != "" {
			enforcer = func(t *testing.T) *policy.Enforcer { return newCellEnforcer(t, pdp, namespaces, false) }
		}
		h, minter := adminConfigServerWith(t, enforcer, withUsersCellCallers)
		callers := usersCallers(t, minter)
		pdp.registerUsers(callers)
		return server{h, callers}
	}
	base, off, on := serve(""), serve("teams"), serve("admin")
	for _, key := range serviceAccountKeyKeys {
		for i, caller := range base.callers {
			pdp.reset("evaluate")
			b := usersCellDo(base.h, key, caller.token)
			f := usersCellDo(off.h, key, off.callers[i].token)
			n := usersCellDo(on.h, key, on.callers[i].token)
			t.Logf("CELL %s|%s|base %d %s|off %d %s|on %d %s", key, caller.name, b.status, strings.TrimSpace(b.body),
				f.status, strings.TrimSpace(f.body), n.status, strings.TrimSpace(n.body))
			if usersAnswersDiffer(b, f) || usersAnswersDiffer(f, n) {
				t.Errorf("%s / %s: base %d %q, off %d %q, on %d %q; want byte-equal", key, caller.name, b.status, b.body,
					f.status, f.body, n.status, n.body)
			}
			if got, want := serviceAccountKeyClass(b), serviceAccountKeyLegacyClass[caller.name]; got != want {
				t.Errorf("%s / %s: legacy class %q, want %q", key, caller.name, got, want)
			}
		}
	}
}

const (
	unavailableBody     = `{"detail":"Authorization service temporarily unavailable"}`
	noDecisionPointBody = `{"detail":{"message":"Authorization service temporarily unavailable","reason_code":"authz_enforce_no_decision_point"}}`
)

// serviceAccountKeyFaultAnswer is a caller class's answer on a key row with the admin cell on and no verdict to be had:
// "unconfigured" (no decision point client) or "dead" (the decision point answers 500). The row's route question is
// asked before the legacy checks, so every judged caller is refused fail-closed (no fallback to the legacy guard);
// with a dead decision point a live impersonation row is refused before the question. A token the authenticator
// refuses is the 401 as before.
func serviceAccountKeyFaultAnswer(fault, caller string) (int, string) {
	switch {
	case serviceAccountKeyLegacyClass[caller] == "401":
		return http.StatusUnauthorized, ""
	case fault == "unconfigured":
		return http.StatusServiceUnavailable, noDecisionPointBody
	case caller == "impersonating platform admin":
		return http.StatusForbidden, impersonationBody
	}
	return http.StatusServiceUnavailable, unavailableBody
}

// Fault cells (admin cell on, no verdict): each caller class on each key row gets serviceAccountKeyFaultAnswer; a
// plain 503 carries Retry-After. Each cell is logged as "FAULT fault|route|caller|status|body" with the cell-off answer
// beside it.
func TestServiceAccountKeyRowsFaultCellsWithTheAdminCellOn(t *testing.T) {
	off, offMinter := adminConfigServerWith(t, nil, withUsersCellCallers)
	offCallers := usersCallers(t, offMinter)
	dead := newCellPDP(t)
	for _, fault := range []string{"unconfigured", "dead"} {
		enforcer := func(t *testing.T) *policy.Enforcer { return newCellEnforcer(t, nil, "admin", true) }
		if fault == "dead" {
			enforcer = func(t *testing.T) *policy.Enforcer { return newCellEnforcer(t, dead, "admin", false) }
		}
		h, minter := adminConfigServerWith(t, enforcer, withUsersCellCallers)
		callers := usersCallers(t, minter)
		for _, key := range serviceAccountKeyKeys {
			for i, caller := range callers {
				dead.reset("fail")
				got := usersCellDo(h, key, caller.token)
				was := usersCellDo(off, key, offCallers[i].token)
				t.Logf("FAULT %s|%s|%s|on %d %s|off %d %s", fault, key, caller.name, got.status, strings.TrimSpace(got.body),
					was.status, strings.TrimSpace(was.body))
				status, body := serviceAccountKeyFaultAnswer(fault, caller.name)
				if got.status != status || (body != "" && got.body != body) {
					t.Errorf("%s %s / %s: %d %q, want %d %q", fault, key, caller.name, got.status, got.body, status, body)
				}
				if got.body == unavailableBody && got.header.Get("Retry-After") != "5" {
					t.Errorf("%s %s / %s: Retry-After %q, want 5", fault, key, caller.name, got.header.Get("Retry-After"))
				}
			}
		}
	}
}
