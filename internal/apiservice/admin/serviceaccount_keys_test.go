package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
)

const (
	saKeySecret  = "fcsak_issued-once-secret-bytes"
	saKeyAccount = "77777777-7777-4777-8777-777777777777"
	saKeyCredID  = "crd_88888888-8888-4888-8888-888888888888"
	saKeyReceipt = `{"schema_version":"credential-receipt.v1","kind":"issue","credential":{"id":"` + saKeyCredID + `","status":"active"},"secret":"` + saKeySecret + `"}`
	saKeyEntry   = `{"schema_version":"credential-entry.v1","id":"` + saKeyCredID + `","status":"revoked"}`
	saKeyList    = `{"schema_version":"credential-list.v1","items":[{"id":"` + saKeyCredID + `","status":"active"}]}`
)

// fakeKeys records each call and answers it, or fails it with err.
type fakeKeys struct {
	mu    sync.Mutex
	calls []string
	err   error
}

func (f *fakeKeys) answer(verb, body string) (json.RawMessage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, verb)
	if f.err != nil {
		return nil, f.err
	}
	return json.RawMessage(body), nil
}

func (f *fakeKeys) IssueKey(_ context.Context, account, actor uuid.UUID, expires *int64) (json.RawMessage, error) {
	return f.answer("issue "+account.String()+" "+actor.String()+" "+seconds(expires), saKeyReceipt)
}

func (f *fakeKeys) RotateKey(_ context.Context, account, actor uuid.UUID, overlap int64, expires *int64) (json.RawMessage, error) {
	return f.answer("rotate "+account.String()+" "+actor.String()+" "+seconds(&overlap)+" "+seconds(expires), strings.Replace(saKeyReceipt, `"issue"`, `"rotate"`, 1))
}

func (f *fakeKeys) RevokeKey(_ context.Context, credential string, actor uuid.UUID, reason string) (json.RawMessage, error) {
	return f.answer("revoke "+credential+" "+actor.String()+" "+reason, saKeyEntry)
}

func (f *fakeKeys) ListKeys(_ context.Context, account, actor uuid.UUID) (json.RawMessage, error) {
	return f.answer("list "+account.String()+" "+actor.String(), saKeyList)
}

func (f *fakeKeys) taken() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

func seconds(v *int64) string {
	if v == nil {
		return "-"
	}
	b, _ := json.Marshal(*v)
	return string(b)
}

// saKeyRoute is one key route as a test drives it.
type saKeyRoute struct {
	name, method, path, body string
}

var saKeyRoutes = []saKeyRoute{
	{"list", http.MethodGet, "/api/v1/admin/service-accounts/" + saKeyAccount + "/keys", ""},
	{"issue", http.MethodPost, "/api/v1/admin/service-accounts/" + saKeyAccount + "/keys", `{"expires_in_seconds":3600}`},
	{"rotate", http.MethodPost, "/api/v1/admin/service-accounts/" + saKeyAccount + "/keys/rotate", `{"overlap_seconds":600}`},
	{"revoke", http.MethodPost, "/api/v1/admin/service-accounts/keys/" + saKeyCredID + "/revoke", `{"reason":"compromised"}`},
}

// saKeyStore is the caller as the authenticator reads it: superuser says
// whether the user is a platform superuser; admin whether it is an admin of
// its token's organization. It has no membership anywhere else.
type saKeyStore struct{ superuser, admin bool }

func (s saKeyStore) UserState(context.Context, uuid.UUID) (policy.UserState, bool, error) {
	return policy.UserState{IsActive: true, IsSuperuser: s.superuser}, true, nil
}

func (s saKeyStore) Membership(context.Context, uuid.UUID, uuid.UUID) (string, bool, error) {
	if s.admin {
		return "admin", true, nil
	}
	return "", false, nil
}

func (s saKeyStore) OrgMembership(ctx context.Context, user, org uuid.UUID) (string, bool, bool, error) {
	role, member, err := s.Membership(ctx, user, org)
	return role, member, true, err
}

func (saKeyStore) ActiveImpersonation(context.Context, uuid.UUID) (*policy.Impersonation, error) {
	return nil, nil
}

func (saKeyStore) ReadsPlatformRoles() bool { return false }

// mountedKeyRoutes mounts the real key routes (body first, platform gate,
// handler) over keys, with a store that records any database call, and
// returns the server, a bearer for the caller store describes, the log and
// the caller's id.
func mountedKeyRoutes(t *testing.T, store saKeyStore, keys authprovision.ServiceAccountKeys) (*httptest.Server, string, *bytes.Buffer, *bool, uuid.UUID) {
	t.Helper()
	const key = "service-account-key-route-test-key-32b!!"
	verifier, err := edgetoken.New(key, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	signer, err := edgetoken.NewSigner(key, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	logs := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	auth, err := policy.NewAuthenticator(verifier, store, logger)
	if err != nil {
		t.Fatal(err)
	}
	touched := new(bool)
	h := &handlers{store: pgStore{Pool: touchedDB{touched}}, guard: policy.NewGuard(auth, logger), logger: logger}
	if keys != nil {
		h.serviceAccountKeys = keys
	}
	mux := http.NewServeMux()
	for _, route := range h.serviceAccountKeyRoutes() {
		mux.Handle(route.Method+" "+route.Pattern, route.Handler)
	}
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	caller := uuid.New()
	claims := edgetoken.AccessClaims{UserID: caller.String(), Email: "caller@example.test", IsSuperuser: store.superuser}
	if store.admin {
		claims.OrgID, claims.Role = uuid.NewString(), "admin"
	}
	bearer, err := signer.Access(claims, time.Now(), uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	return server, bearer, logs, touched, caller
}

func sendKeyRoute(t *testing.T, server *httptest.Server, bearer string, route saKeyRoute) (int, string, http.Header) {
	t.Helper()
	request, err := http.NewRequest(route.method, server.URL+route.path, strings.NewReader(route.body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+bearer)
	if route.body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	raw, _ := io.ReadAll(response.Body)
	return response.StatusCode, string(raw), response.Header
}

// The four key routes are in the admin route table.
func TestServiceAccountKeyRoutesMounted(t *testing.T) {
	mounted := map[string]bool{}
	for _, route := range Routes(Deps{Guard: policy.NewGuard(nil, nil)}) {
		mounted[route.Method+" "+route.Pattern] = true
	}
	for _, want := range []string{
		"GET /api/v1/admin/service-accounts/{service_account_id}/keys",
		"POST /api/v1/admin/service-accounts/{service_account_id}/keys",
		"POST /api/v1/admin/service-accounts/{service_account_id}/keys/rotate",
		"POST /api/v1/admin/service-accounts/keys/{credential_id}/revoke",
	} {
		if !mounted[want] {
			t.Errorf("admin.Routes does not mount %q", want)
		}
	}
}

// Rule 1: a superuser with no membership in any organization gets 200 on all
// four routes; go-api passes its own id as the actor and reads no database.
func TestServiceAccountKeyRoutesSuperuserWithoutMembership(t *testing.T) {
	for _, route := range saKeyRoutes {
		t.Run(route.name, func(t *testing.T) {
			keys := &fakeKeys{}
			server, bearer, _, touched, caller := mountedKeyRoutes(t, saKeyStore{superuser: true}, keys)
			if code, body, _ := sendKeyRoute(t, server, bearer, route); code != http.StatusOK {
				t.Fatalf("status %d %s, want 200", code, body)
			}
			want := map[string]string{
				"list":   "list " + saKeyAccount + " " + caller.String(),
				"issue":  "issue " + saKeyAccount + " " + caller.String() + " 3600",
				"rotate": "rotate " + saKeyAccount + " " + caller.String() + " 600 -",
				"revoke": "revoke " + saKeyCredID + " " + caller.String() + " compromised",
			}[route.name]
			if got := keys.taken(); len(got) != 1 || got[0] != want {
				t.Fatalf("auth calls %q, want [%q]", got, want)
			}
			if *touched {
				t.Fatal("a key route read the go-api database")
			}
		})
	}
}

// An org admin who is no superuser is refused at go-api: 403, and no call
// reaches auth-service.
func TestServiceAccountKeyRoutesPlatformOnly(t *testing.T) {
	for _, route := range saKeyRoutes {
		t.Run(route.name, func(t *testing.T) {
			keys := &fakeKeys{}
			server, bearer, _, _, _ := mountedKeyRoutes(t, saKeyStore{admin: true}, keys)
			if code, body, _ := sendKeyRoute(t, server, bearer, route); code != http.StatusForbidden {
				t.Fatalf("status %d %s, want 403", code, body)
			}
			if got := keys.taken(); len(got) != 0 {
				t.Fatalf("an org admin's request reached auth-service: %q", got)
			}
		})
	}
}

// The issue receipt is answered once, unchanged, never cached and never
// logged; the list carries no secret.
func TestServiceAccountKeyIssueSecretOnce(t *testing.T) {
	keys := &fakeKeys{}
	server, bearer, logs, _, _ := mountedKeyRoutes(t, saKeyStore{superuser: true}, keys)
	code, body, header := sendKeyRoute(t, server, bearer, saKeyRoutes[1])
	if code != http.StatusOK || strings.Count(body, saKeySecret) != 1 {
		t.Fatalf("issue: status %d body %s, want 200 with the secret once", code, body)
	}
	var got, want any
	if json.Unmarshal([]byte(body), &got) != nil || json.Unmarshal([]byte(saKeyReceipt), &want) != nil || !jsonEqual(got, want) {
		t.Fatalf("issue body %s, want the receipt %s", body, saKeyReceipt)
	}
	if cc := header.Get("Cache-Control"); cc != "" && !strings.Contains(cc, "no-store") {
		t.Fatalf("Cache-Control %q", cc)
	}
	if _, list, _ := sendKeyRoute(t, server, bearer, saKeyRoutes[0]); strings.Contains(list, "secret") {
		t.Fatalf("list carries a secret: %s", list)
	}
	keys.err = &authprovision.Refused{Status: http.StatusConflict, Reason: "unknown_reason"}
	_, _, _ = sendKeyRoute(t, server, bearer, saKeyRoutes[1])
	if strings.Contains(logs.String(), saKeySecret) || strings.Contains(logs.String(), "fcsak_") {
		t.Fatalf("the log carries the secret:\n%s", logs.String())
	}
}

func jsonEqual(a, b any) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return bytes.Equal(x, y)
}

// A legacy users row (auth_provider "service") is no service account:
// auth-service answers service_account_not_found and go-api answers 404 with
// no read of public.users.
func TestServiceAccountKeyLegacyServiceUser(t *testing.T) {
	keys := &fakeKeys{err: &authprovision.Refused{Status: http.StatusNotFound, Reason: "service_account_not_found"}}
	server, bearer, _, touched, _ := mountedKeyRoutes(t, saKeyStore{superuser: true}, keys)
	for _, route := range saKeyRoutes[:3] {
		code, body, _ := sendKeyRoute(t, server, bearer, route)
		if code != http.StatusNotFound || body != `{"detail":"Service account not found"}` {
			t.Fatalf("%s: status %d %s, want 404 Service account not found", route.name, code, body)
		}
	}
	if *touched {
		t.Fatal("a key route read public.users")
	}
}

// Each auth refusal maps to its go-api answer; a 5xx, an unknown reason and
// an unreachable route are the 503.
func TestServiceAccountKeyRefusalMap(t *testing.T) {
	cases := []struct {
		err    error
		status int
	}{
		{&authprovision.Refused{Status: 404, Reason: "key_not_found"}, 404},
		{&authprovision.Refused{Status: 403, Reason: "admin_required"}, 403},
		{&authprovision.Refused{Status: 403, Reason: "superuser_required"}, 403},
		{&authprovision.Refused{Status: 409, Reason: "service_account_disabled"}, 409},
		{&authprovision.Refused{Status: 409, Reason: "key_live"}, 409},
		{&authprovision.Refused{Status: 409, Reason: "key_rotating"}, 409},
		{&authprovision.Refused{Status: 409, Reason: "key_none"}, 409},
		{&authprovision.Refused{Status: 400, Reason: "expiry_invalid"}, 400},
		{&authprovision.Refused{Status: 400, Reason: "overlap_invalid"}, 400},
		{&authprovision.Refused{Status: 400, Reason: "reason_invalid"}, 400},
		{&authprovision.Refused{Status: 400, Reason: "malformed"}, 400},
		{&authprovision.Refused{Status: 409, Reason: "something_new"}, 503},
		{&authprovision.Refused{Status: 503, Reason: "key_live"}, 503},
		{errors.Join(authprovision.ErrUnavailable, errors.New("dial")), 503},
	}
	for _, tc := range cases {
		keys := &fakeKeys{err: tc.err}
		server, bearer, _, _, _ := mountedKeyRoutes(t, saKeyStore{superuser: true}, keys)
		for _, route := range saKeyRoutes {
			if code, body, _ := sendKeyRoute(t, server, bearer, route); code != tc.status {
				t.Fatalf("%v on %s: status %d %s, want %d", tc.err, route.name, code, body, tc.status)
			}
		}
	}
}

// With the switch off the routes stay mounted and answer 503 config_missing
// naming the switch, after the platform gate.
func TestServiceAccountKeySwitchOff(t *testing.T) {
	server, bearer, _, _, _ := mountedKeyRoutes(t, saKeyStore{superuser: true}, nil)
	want := `{"detail":{"message":"AUTH_PROXY_SERVICE_ACCOUNT_KEYS is not set","reason_code":"config_missing"}}`
	for _, route := range saKeyRoutes {
		if code, body, _ := sendKeyRoute(t, server, bearer, route); code != http.StatusServiceUnavailable || body != want {
			t.Fatalf("%s: status %d %s, want 503 %s", route.name, code, body, want)
		}
	}
	admin, adminBearer, _, _, _ := mountedKeyRoutes(t, saKeyStore{admin: true}, nil)
	if code, _, _ := sendKeyRoute(t, admin, adminBearer, saKeyRoutes[0]); code != http.StatusForbidden {
		t.Fatalf("switch off, org admin: status %d, want 403 (the gate first)", code)
	}
}

// Malformed ids are a 400 and a malformed body a 422, before any auth call.
func TestServiceAccountKeyMalformedInput(t *testing.T) {
	keys := &fakeKeys{}
	server, bearer, _, _, _ := mountedKeyRoutes(t, saKeyStore{superuser: true}, keys)
	for _, tc := range []struct {
		route  saKeyRoute
		status int
	}{
		{saKeyRoute{"list bad id", http.MethodGet, "/api/v1/admin/service-accounts/not-a-uuid/keys", ""}, 400},
		{saKeyRoute{"revoke bare uuid", http.MethodPost, "/api/v1/admin/service-accounts/keys/88888888-8888-4888-8888-888888888888/revoke", `{"reason":"x"}`}, 400},
		{saKeyRoute{"rotate no overlap", http.MethodPost, "/api/v1/admin/service-accounts/" + saKeyAccount + "/keys/rotate", `{}`}, 422},
		{saKeyRoute{"rotate null overlap", http.MethodPost, "/api/v1/admin/service-accounts/" + saKeyAccount + "/keys/rotate", `{"overlap_seconds":null}`}, 422},
		{saKeyRoute{"issue zero expiry", http.MethodPost, "/api/v1/admin/service-accounts/" + saKeyAccount + "/keys", `{"expires_in_seconds":0}`}, 422},
		{saKeyRoute{"revoke no reason", http.MethodPost, "/api/v1/admin/service-accounts/keys/" + saKeyCredID + "/revoke", `{}`}, 422},
		{saKeyRoute{"revoke long reason", http.MethodPost, "/api/v1/admin/service-accounts/keys/" + saKeyCredID + "/revoke", `{"reason":"` + strings.Repeat("r", 33) + `"}`}, 422},
	} {
		if code, body, _ := sendKeyRoute(t, server, bearer, tc.route); code != tc.status {
			t.Fatalf("%s: status %d %s, want %d", tc.route.name, code, body, tc.status)
		}
	}
	if got := keys.taken(); len(got) != 0 {
		t.Fatalf("malformed input reached auth-service: %q", got)
	}
}

// saKeyBrokenBody is each write route with a body that does not decode.
func saKeyBrokenBody() []saKeyRoute {
	var out []saKeyRoute
	for _, route := range saKeyRoutes[1:] {
		route.body = "{"
		out = append(out, route)
	}
	return out
}

// The platform gate answers before the body is read: an org admin with a
// body that does not decode gets 403, not 422, and reaches no auth call.
func TestServiceAccountKeyGateBeforeBody(t *testing.T) {
	for _, route := range saKeyBrokenBody() {
		t.Run(route.name, func(t *testing.T) {
			keys := &fakeKeys{}
			server, bearer, _, _, _ := mountedKeyRoutes(t, saKeyStore{admin: true}, keys)
			if code, body, _ := sendKeyRoute(t, server, bearer, route); code != http.StatusForbidden {
				t.Fatalf("status %d %s, want 403", code, body)
			}
			if got := keys.taken(); len(got) != 0 {
				t.Fatalf("auth calls %q, want none", got)
			}
		})
	}
}

// With the switch off, a superuser with a body that does not decode gets 503
// config_missing, not 422: the switch answers before the body is read.
func TestServiceAccountKeySwitchBeforeBody(t *testing.T) {
	want := `{"detail":{"message":"AUTH_PROXY_SERVICE_ACCOUNT_KEYS is not set","reason_code":"config_missing"}}`
	for _, route := range saKeyBrokenBody() {
		t.Run(route.name, func(t *testing.T) {
			server, bearer, _, _, _ := mountedKeyRoutes(t, saKeyStore{superuser: true}, nil)
			if code, body, _ := sendKeyRoute(t, server, bearer, route); code != http.StatusServiceUnavailable || body != want {
				t.Fatalf("status %d %s, want 503 %s", code, body, want)
			}
		})
	}
}

// A superuser with the switch on and a body that does not decode gets 422,
// and no call reaches auth-service.
func TestServiceAccountKeyBrokenBodyAuthorized(t *testing.T) {
	for _, route := range saKeyBrokenBody() {
		t.Run(route.name, func(t *testing.T) {
			keys := &fakeKeys{}
			server, bearer, _, _, _ := mountedKeyRoutes(t, saKeyStore{superuser: true}, keys)
			if code, body, _ := sendKeyRoute(t, server, bearer, route); code != http.StatusUnprocessableEntity || !strings.Contains(body, "json_invalid") {
				t.Fatalf("status %d %s, want 422 json_invalid", code, body)
			}
			if got := keys.taken(); len(got) != 0 {
				t.Fatalf("auth calls %q, want none", got)
			}
		})
	}
}

// auth-service's principal_deactivated refusal (an actor deactivated after
// go-api's check) is the 403 the member and invite routes answer, on all four
// routes.
func TestServiceAccountKeyDeactivatedActor(t *testing.T) {
	for _, route := range saKeyRoutes {
		t.Run(route.name, func(t *testing.T) {
			keys := &fakeKeys{err: &authprovision.Refused{Status: http.StatusForbidden, Reason: "principal_deactivated"}}
			server, bearer, _, _, _ := mountedKeyRoutes(t, saKeyStore{superuser: true}, keys)
			if code, body, _ := sendKeyRoute(t, server, bearer, route); code != http.StatusForbidden || body != `{"detail":"Account is deactivated"}` {
				t.Fatalf("status %d %s, want 403 Account is deactivated", code, body)
			}
		})
	}
}
