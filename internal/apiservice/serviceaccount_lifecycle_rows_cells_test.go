package apiservice

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/google/uuid"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// lifecycleHandlerBody is what cellAccounts answers for every lifecycle verb, so a cell that reaches the handler
// reads as "handler" and nothing else does.
const lifecycleHandlerBody = `{"cell":"lifecycle handler"}`

// cellAccounts is the lifecycle client of the cell rig: the routes mount only with a client (the switch on), and
// each verb answers lifecycleHandlerBody.
type cellAccounts struct{}

func (cellAccounts) CreateAccount(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string) (json.RawMessage, error) {
	return json.RawMessage(lifecycleHandlerBody), nil
}
func (cellAccounts) DisableAccount(context.Context, uuid.UUID, uuid.UUID) (json.RawMessage, error) {
	return json.RawMessage(lifecycleHandlerBody), nil
}
func (cellAccounts) EnableAccount(context.Context, uuid.UUID, uuid.UUID) (json.RawMessage, error) {
	return json.RawMessage(lifecycleHandlerBody), nil
}
func (cellAccounts) SetAccountOwner(context.Context, uuid.UUID, uuid.UUID, uuid.UUID) (json.RawMessage, error) {
	return json.RawMessage(lifecycleHandlerBody), nil
}

// lifecycleRoutes are the real go-api routes with the lifecycle client set.
func lifecycleRoutes(_ *testing.T, deps Deps) []httpapi.Route {
	deps.ServiceAccounts = cellAccounts{}
	return Routes(deps, quietLogger())
}

// lifecycleBodies are bodies the create and owner decoders accept, read after the platform gate.
var lifecycleBodies = map[string]string{
	"POST /api/v1/admin/service-accounts":                            `{"organization_id":"00000000-0000-4000-8000-0000000000e2","owner_principal_id":"00000000-0000-4000-8000-0000000000e3","name":"cell-bot"}`,
	"POST /api/v1/admin/service-accounts/{service_account_id}/owner": `{"owner_principal_id":"00000000-0000-4000-8000-0000000000e3"}`,
}

func lifecycleCellDo(h http.Handler, key, token string) cellAnswer {
	method, _, _ := strings.Cut(key, " ")
	var body io.Reader
	if raw, ok := lifecycleBodies[key]; ok {
		body = strings.NewReader(raw)
	}
	request := httptest.NewRequest(method, cellPath(key), body)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	recorder := httptest.NewRecorder()
	h.ServeHTTP(recorder, request)
	return cellAnswer{status: recorder.Code, body: recorder.Body.String(), header: recorder.Header()}
}

// The profile names admin.users on all 8 service account rows (the 4 key rows and the 4 lifecycle rows), and the
// guard's action map reads each lifecycle row as a platform row.
func TestServiceAccountLifecycleRowsNameAdminUsers(t *testing.T) {
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
		if row.Service != goAPIAccessService {
			continue
		}
		if row.Route != "/api/v1/admin/service-accounts" && !strings.HasPrefix(row.Route, "/api/v1/admin/service-accounts/") {
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
	want := append(append([]string{}, serviceAccountKeyKeys...), serviceAccountLifecycleKeys...)
	sort.Strings(want)
	if !slices.Equal(keys, want) {
		t.Fatalf("service account rows %v, want %v", keys, want)
	}
	for _, key := range keys {
		if got[key] != policy.ActionAdminUsers {
			t.Errorf("%s: action %q, want %s", key, got[key], policy.ActionAdminUsers)
		}
	}
	rows, err := policy.ParseShadowRows(authv1.GoAPIEndpointProfiles, goAPIAccessService)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range serviceAccountLifecycleKeys {
		if row, ok := rows[key]; !ok || row.Action != policy.ActionAdminUsers || row.ResourceType != "platform" {
			t.Errorf("%s: guard row %+v (present %v), want admin.users on the platform", key, row, ok)
		}
	}
}

func lifecycleClass(a cellAnswer) string {
	switch {
	case a.status == http.StatusUnauthorized:
		return "401"
	case a.body == superuserRequiredBody, a.body == orgContextBody, a.body == impersonationBody:
		return a.body
	case a.status == http.StatusOK && strings.TrimSpace(a.body) == lifecycleHandlerBody:
		return "handler"
	}
	return "other " + a.body
}

// lifecycleLegacyClass is the legacy answer class of each caller on the lifecycle rows (executed on the base commit:
// the CELL lines of TestServiceAccountLifecycleRowsAnswerAsLegacyWithTheAdminCellOn): the same classes as the key rows.
var lifecycleLegacyClass = serviceAccountKeyLegacyClass

// Every caller class on every lifecycle row gets the same status and body with no Enforcer, with the admin cell off
// and with it on (the decision point in evaluate mode), and that answer is the legacy class. Each cell is logged as
// "CELL route|caller|base|off|on" so a run on the base commit records today's answers.
func TestServiceAccountLifecycleRowsAnswerAsLegacyWithTheAdminCellOn(t *testing.T) {
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
		h, minter := adminConfigServerOver(t, enforcer, withUsersCellCallers, lifecycleRoutes)
		callers := usersCallers(t, minter)
		pdp.registerUsers(callers)
		return server{h, callers}
	}
	base, off, on := serve(""), serve("teams"), serve("admin")
	for _, key := range serviceAccountLifecycleKeys {
		for i, caller := range base.callers {
			pdp.reset("evaluate")
			b := lifecycleCellDo(base.h, key, caller.token)
			f := lifecycleCellDo(off.h, key, off.callers[i].token)
			n := lifecycleCellDo(on.h, key, on.callers[i].token)
			t.Logf("CELL %s|%s|base %d %s|off %d %s|on %d %s", key, caller.name, b.status, strings.TrimSpace(b.body),
				f.status, strings.TrimSpace(f.body), n.status, strings.TrimSpace(n.body))
			if usersAnswersDiffer(b, f) || usersAnswersDiffer(f, n) {
				t.Errorf("%s / %s: base %d %q, off %d %q, on %d %q; want byte-equal", key, caller.name, b.status, b.body,
					f.status, f.body, n.status, n.body)
			}
			if got, want := lifecycleClass(b), lifecycleLegacyClass[caller.name]; got != want {
				t.Errorf("%s / %s: legacy class %q, want %q", key, caller.name, got, want)
			}
		}
	}
}

// Fault cells (admin cell on, no verdict): each caller class on each lifecycle row gets the key rows' fault answer
// (serviceAccountKeyFaultAnswer); a plain 503 carries Retry-After. Each cell is logged as
// "FAULT fault|route|caller|on|off" with the cell-off answer beside it.
func TestServiceAccountLifecycleRowsFaultCellsWithTheAdminCellOn(t *testing.T) {
	off, offMinter := adminConfigServerOver(t, nil, withUsersCellCallers, lifecycleRoutes)
	offCallers := usersCallers(t, offMinter)
	dead := newCellPDP(t)
	for _, fault := range []string{"unconfigured", "dead"} {
		enforcer := func(t *testing.T) *policy.Enforcer { return newCellEnforcer(t, nil, "admin", true) }
		if fault == "dead" {
			enforcer = func(t *testing.T) *policy.Enforcer { return newCellEnforcer(t, dead, "admin", false) }
		}
		h, minter := adminConfigServerOver(t, enforcer, withUsersCellCallers, lifecycleRoutes)
		callers := usersCallers(t, minter)
		for _, key := range serviceAccountLifecycleKeys {
			for i, caller := range callers {
				dead.reset("fail")
				got := lifecycleCellDo(h, key, caller.token)
				was := lifecycleCellDo(off, key, offCallers[i].token)
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
