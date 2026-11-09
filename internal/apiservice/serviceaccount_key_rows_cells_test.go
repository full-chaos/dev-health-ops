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
