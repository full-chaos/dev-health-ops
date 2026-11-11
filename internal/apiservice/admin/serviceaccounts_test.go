package admin

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/edtokens"
)

const (
	saOrg     = "11111111-1111-4111-8111-111111111111"
	saOwner   = "44444444-4444-4444-8444-444444444444"
	saCreated = `{"service_account_id":"` + saKeyAccount + `","organization_id":"` + saOrg + `","name":"ci-bot","status":"active","owner_principal_id":"` + saOwner + `"}`
	saSwitch  = `{"service_account_id":"` + saKeyAccount + `","status":"disabled"}`
	saOwned   = `{"service_account_id":"` + saKeyAccount + `","owner_principal_id":"` + saOwner + `"}`
)

// fakeAccounts records each call and answers it, or fails it with err.
type fakeAccounts struct {
	mu    sync.Mutex
	calls []string
	err   error
}

func (f *fakeAccounts) answer(verb, body string) (json.RawMessage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, verb)
	if f.err != nil {
		return nil, f.err
	}
	return json.RawMessage(body), nil
}

func (f *fakeAccounts) CreateAccount(_ context.Context, org, owner, actor uuid.UUID, name string) (json.RawMessage, error) {
	return f.answer("create "+org.String()+" "+owner.String()+" "+actor.String()+" "+name, saCreated)
}

func (f *fakeAccounts) DisableAccount(_ context.Context, account, actor uuid.UUID) (json.RawMessage, error) {
	return f.answer("disable "+account.String()+" "+actor.String(), saSwitch)
}

func (f *fakeAccounts) EnableAccount(_ context.Context, account, actor uuid.UUID) (json.RawMessage, error) {
	return f.answer("enable "+account.String()+" "+actor.String(), strings.Replace(saSwitch, "disabled", "active", 1))
}

func (f *fakeAccounts) SetAccountOwner(_ context.Context, account, owner, actor uuid.UUID) (json.RawMessage, error) {
	return f.answer("owner "+account.String()+" "+owner.String()+" "+actor.String(), saOwned)
}

func (f *fakeAccounts) taken() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

var saRoutes = []saKeyRoute{
	{"create", http.MethodPost, "/api/v1/admin/service-accounts", `{"organization_id":"` + saOrg + `","owner_principal_id":"` + saOwner + `","name":"ci-bot"}`},
	{"disable", http.MethodPost, "/api/v1/admin/service-accounts/" + saKeyAccount + "/disable", ""},
	{"enable", http.MethodPost, "/api/v1/admin/service-accounts/" + saKeyAccount + "/enable", ""},
	{"owner", http.MethodPost, "/api/v1/admin/service-accounts/" + saKeyAccount + "/owner", `{"owner_principal_id":"` + saOwner + `"}`},
}

var saRoutePatterns = []string{
	"POST /api/v1/admin/service-accounts",
	"POST /api/v1/admin/service-accounts/{service_account_id}/disable",
	"POST /api/v1/admin/service-accounts/{service_account_id}/enable",
	"POST /api/v1/admin/service-accounts/{service_account_id}/owner",
}

// mountedAccountRoutes mounts the real lifecycle routes (platform gate, body,
// handler) over accounts, as mountedKeyRoutes does for the key routes.
func mountedAccountRoutes(t *testing.T, store saKeyStore, accounts authprovision.ServiceAccounts) (*httptest.Server, string, *bool, uuid.UUID) {
	t.Helper()
	tokens := edtokens.New(t)
	logger := slog.New(slog.DiscardHandler)
	auth := tokens.AuthenticatorOver(t, store, logger)
	touched := new(bool)
	h := &handlers{store: pgStore{Pool: touchedDB{touched}}, guard: policy.NewGuard(auth, logger), logger: logger, serviceAccounts: accounts}
	mux := http.NewServeMux()
	for _, route := range h.serviceAccountRoutes() {
		mux.Handle(route.Method+" "+route.Pattern, route.Handler)
	}
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	caller := uuid.New()
	claims := edtokens.Claims{UserID: caller}
	if store.admin {
		claims.OrgID = uuid.NewString()
	}
	bearer := tokens.MustMint(t, claims)
	return server, bearer, touched, caller
}

// The four lifecycle routes mount with the switch on and only then: off,
// admin.Routes holds none of them, so their paths answer main's 404.
func TestServiceAccountRoutesMountOnlyWithTheSwitch(t *testing.T) {
	mounted := func(deps Deps) map[string]bool {
		out := map[string]bool{}
		for _, route := range Routes(deps) {
			out[route.Method+" "+route.Pattern] = true
		}
		return out
	}
	on := mounted(Deps{Guard: policy.NewGuard(nil, nil), ServiceAccounts: &fakeAccounts{}})
	off := mounted(Deps{Guard: policy.NewGuard(nil, nil)})
	for _, want := range saRoutePatterns {
		if !on[want] {
			t.Errorf("switch on: admin.Routes does not mount %q", want)
		}
		if off[want] {
			t.Errorf("switch off: admin.Routes mounts %q", want)
		}
	}
}

// Rule 1: a superuser with no membership in any organization gets 200 on all
// four routes with auth-service's answer unchanged; go-api passes its own id
// as the actor and reads no database.
func TestServiceAccountRoutesSuperuserWithoutMembership(t *testing.T) {
	for _, route := range saRoutes {
		t.Run(route.name, func(t *testing.T) {
			accounts := &fakeAccounts{}
			server, bearer, touched, caller := mountedAccountRoutes(t, saKeyStore{superuser: true}, accounts)
			code, body, _ := sendKeyRoute(t, server, bearer, route)
			want := map[string]string{"create": saCreated, "disable": saSwitch, "enable": strings.Replace(saSwitch, "disabled", "active", 1), "owner": saOwned}[route.name]
			var got, wantValue any
			if code != http.StatusOK || json.Unmarshal([]byte(body), &got) != nil || json.Unmarshal([]byte(want), &wantValue) != nil || !jsonEqual(got, wantValue) {
				t.Fatalf("status %d %s, want 200 %s", code, body, want)
			}
			call := map[string]string{
				"create":  "create " + saOrg + " " + saOwner + " " + caller.String() + " ci-bot",
				"disable": "disable " + saKeyAccount + " " + caller.String(),
				"enable":  "enable " + saKeyAccount + " " + caller.String(),
				"owner":   "owner " + saKeyAccount + " " + saOwner + " " + caller.String(),
			}[route.name]
			if calls := accounts.taken(); len(calls) != 1 || calls[0] != call {
				t.Fatalf("auth calls %q, want [%q]", calls, call)
			}
			if *touched {
				t.Fatal("a lifecycle route read the go-api database")
			}
		})
	}
}

// An org admin who is no superuser is refused at go-api: 403, and no call
// reaches auth-service.
func TestServiceAccountRoutesPlatformOnly(t *testing.T) {
	for _, route := range saRoutes {
		t.Run(route.name, func(t *testing.T) {
			accounts := &fakeAccounts{}
			server, bearer, _, _ := mountedAccountRoutes(t, saKeyStore{admin: true}, accounts)
			if code, body, _ := sendKeyRoute(t, server, bearer, route); code != http.StatusForbidden || body != `{"detail":"Superuser access required"}` {
				t.Fatalf("status %d %s, want 403 Superuser access required", code, body)
			}
			if calls := accounts.taken(); len(calls) != 0 {
				t.Fatalf("an org admin's request reached auth-service: %q", calls)
			}
		})
	}
}

// The platform gate answers before the body is read: an org admin with a
// body that does not decode gets 403, not 422.
func TestServiceAccountRoutesGateBeforeBody(t *testing.T) {
	for _, route := range saRoutes {
		route.body = "{"
		t.Run(route.name, func(t *testing.T) {
			accounts := &fakeAccounts{}
			server, bearer, _, _ := mountedAccountRoutes(t, saKeyStore{admin: true}, accounts)
			if code, body, _ := sendKeyRoute(t, server, bearer, route); code != http.StatusForbidden {
				t.Fatalf("status %d %s, want 403", code, body)
			}
			if calls := accounts.taken(); len(calls) != 0 {
				t.Fatalf("auth calls %q, want none", calls)
			}
		})
	}
}

// The actor is always the caller: an actor_principal_id in the body does not
// reach auth-service.
func TestServiceAccountRoutesActorIsTheCaller(t *testing.T) {
	other := "99999999-9999-4999-8999-999999999999"
	accounts := &fakeAccounts{}
	server, bearer, _, caller := mountedAccountRoutes(t, saKeyStore{superuser: true}, accounts)
	create := saRoutes[0]
	create.body = `{"organization_id":"` + saOrg + `","owner_principal_id":"` + saOwner + `","name":"ci-bot","actor_principal_id":"` + other + `"}`
	owner := saRoutes[3]
	owner.body = `{"owner_principal_id":"` + saOwner + `","actor_principal_id":"` + other + `"}`
	for _, route := range []saKeyRoute{create, owner} {
		if code, body, _ := sendKeyRoute(t, server, bearer, route); code != http.StatusOK {
			t.Fatalf("%s: status %d %s, want 200", route.name, code, body)
		}
	}
	for _, call := range accounts.taken() {
		if strings.Contains(call, other) || !strings.HasSuffix(strings.TrimSuffix(call, " ci-bot"), caller.String()) {
			t.Fatalf("auth call %q: the actor is not the caller %s", call, caller)
		}
	}
}

// Malformed ids are a 400 and a malformed body a 422, before any auth call.
func TestServiceAccountRoutesMalformedInput(t *testing.T) {
	accounts := &fakeAccounts{}
	server, bearer, _, _ := mountedAccountRoutes(t, saKeyStore{superuser: true}, accounts)
	create := func(body string) saKeyRoute {
		return saKeyRoute{"create", http.MethodPost, "/api/v1/admin/service-accounts", body}
	}
	for _, tc := range []struct {
		name   string
		route  saKeyRoute
		status int
	}{
		{"disable bad id", saKeyRoute{"disable", http.MethodPost, "/api/v1/admin/service-accounts/not-a-uuid/disable", ""}, 400},
		{"enable bad id", saKeyRoute{"enable", http.MethodPost, "/api/v1/admin/service-accounts/not-a-uuid/enable", ""}, 400},
		{"owner bad id", saKeyRoute{"owner", http.MethodPost, "/api/v1/admin/service-accounts/not-a-uuid/owner", `{"owner_principal_id":"` + saOwner + `"}`}, 400},
		{"owner no owner", saKeyRoute{"owner", http.MethodPost, "/api/v1/admin/service-accounts/" + saKeyAccount + "/owner", `{}`}, 422},
		{"owner bad owner", saKeyRoute{"owner", http.MethodPost, "/api/v1/admin/service-accounts/" + saKeyAccount + "/owner", `{"owner_principal_id":"x"}`}, 422},
		{"create empty body", create(""), 422},
		{"create no org", create(`{"owner_principal_id":"` + saOwner + `","name":"ci-bot"}`), 422},
		{"create bad org", create(`{"organization_id":"x","owner_principal_id":"` + saOwner + `","name":"ci-bot"}`), 422},
		{"create no owner", create(`{"organization_id":"` + saOrg + `","name":"ci-bot"}`), 422},
		{"create no name", create(`{"organization_id":"` + saOrg + `","owner_principal_id":"` + saOwner + `"}`), 422},
		{"create empty name", create(`{"organization_id":"` + saOrg + `","owner_principal_id":"` + saOwner + `","name":""}`), 422},
		{"create long name", create(`{"organization_id":"` + saOrg + `","owner_principal_id":"` + saOwner + `","name":"` + strings.Repeat("n", 64) + `"}`), 422},
		{"create name not a string", create(`{"organization_id":"` + saOrg + `","owner_principal_id":"` + saOwner + `","name":7}`), 422},
	} {
		if code, body, _ := sendKeyRoute(t, server, bearer, tc.route); code != tc.status {
			t.Fatalf("%s: status %d %s, want %d", tc.name, code, body, tc.status)
		}
	}
	if calls := accounts.taken(); len(calls) != 0 {
		t.Fatalf("malformed input reached auth-service: %q", calls)
	}
}

// Each auth refusal maps to its go-api answer; a 5xx, an unknown reason and
// an unreachable route are the 503.
func TestServiceAccountRoutesRefusalMap(t *testing.T) {
	cases := []struct {
		reason string
		status int
		detail string
	}{
		{"service_account_not_found", 404, "Service account not found"},
		{"organization_not_found", 404, "Organization not found"},
		{"organization_suspended", 409, "Organization is suspended"},
		{"name_invalid", 400, "Invalid service account name"},
		{"name_taken", 409, "Service account name is taken"},
		{"owner_invalid", 409, "Owner must be an active user"},
		{"owner_not_member", 409, "Owner is not a member of the organization"},
		{"already_in_state", 409, "Service account is already in that state"},
		{"state_changed", 409, "Service account changed; try again"},
		{"users_not_owned", 409, "Service account is not in that organization"},
		{"service_account_disabled", 409, "Service account is disabled"},
		{"malformed", 400, "Invalid service account request"},
		{"admin_required", 403, "Admin access required for organization"},
		{"superuser_required", 403, "Superuser access required"},
		{"principal_deactivated", 403, "Account is deactivated"},
	}
	for _, tc := range cases {
		accounts := &fakeAccounts{err: &authprovision.Refused{Status: tc.status, Reason: tc.reason}}
		server, bearer, _, _ := mountedAccountRoutes(t, saKeyStore{superuser: true}, accounts)
		want := `{"detail":"` + tc.detail + `"}`
		for _, route := range saRoutes {
			if code, body, _ := sendKeyRoute(t, server, bearer, route); code != tc.status || body != want {
				t.Fatalf("%s on %s: status %d %s, want %d %s", tc.reason, route.name, code, body, tc.status, want)
			}
		}
	}
	for _, err := range []error{
		&authprovision.Refused{Status: 409, Reason: "something_new"},
		&authprovision.Refused{Status: 503, Reason: "name_taken"},
		errors.Join(authprovision.ErrUnavailable, errors.New("dial")),
	} {
		accounts := &fakeAccounts{err: err}
		server, bearer, _, _ := mountedAccountRoutes(t, saKeyStore{superuser: true}, accounts)
		for _, route := range saRoutes {
			if code, body, _ := sendKeyRoute(t, server, bearer, route); code != http.StatusServiceUnavailable {
				t.Fatalf("%v on %s: status %d %s, want 503", err, route.name, code, body)
			}
		}
	}
}

// A legacy users row (auth_provider "service") is no service account:
// auth-service answers service_account_not_found and go-api answers 404 with
// no read of public.users.
func TestServiceAccountRoutesLegacyServiceUser(t *testing.T) {
	accounts := &fakeAccounts{err: &authprovision.Refused{Status: http.StatusNotFound, Reason: "service_account_not_found"}}
	server, bearer, touched, _ := mountedAccountRoutes(t, saKeyStore{superuser: true}, accounts)
	for _, route := range saRoutes[1:] {
		if code, body, _ := sendKeyRoute(t, server, bearer, route); code != http.StatusNotFound || body != `{"detail":"Service account not found"}` {
			t.Fatalf("%s: status %d %s, want 404 Service account not found", route.name, code, body)
		}
	}
	if *touched {
		t.Fatal("a lifecycle route read public.users")
	}
}
