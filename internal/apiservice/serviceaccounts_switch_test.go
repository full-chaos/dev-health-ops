package apiservice

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// stubAccounts is a lifecycle client that is never called: it only makes the
// lifecycle routes mount, as the switch on does.
type stubAccounts struct{}

func (stubAccounts) CreateAccount(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string) (json.RawMessage, error) {
	return nil, authprovision.ErrUnavailable
}
func (stubAccounts) DisableAccount(context.Context, uuid.UUID, uuid.UUID) (json.RawMessage, error) {
	return nil, authprovision.ErrUnavailable
}
func (stubAccounts) EnableAccount(context.Context, uuid.UUID, uuid.UUID) (json.RawMessage, error) {
	return nil, authprovision.ErrUnavailable
}
func (stubAccounts) SetAccountOwner(context.Context, uuid.UUID, uuid.UUID, uuid.UUID) (json.RawMessage, error) {
	return nil, authprovision.ErrUnavailable
}

// mainNotFoundHeader is what origin/main c23cadad8 answers on the four
// lifecycle paths (executed: logs/legacy-cells-main-unit-1.log), less Date and
// the per-request X-Request-Id.
var mainNotFoundHeader = []string{
	"Content-Length=22",
	"Content-Security-Policy=default-src 'none'; frame-ancestors 'none'; base-uri 'none'",
	"Content-Type=application/json",
	"Referrer-Policy=strict-origin-when-cross-origin",
	"Strict-Transport-Security=max-age=31536000; includeSubDomains",
	"Vary=Origin",
	"X-Content-Type-Options=nosniff",
	"X-Dev-Health-Plane=go",
	"X-Frame-Options=DENY",
}

type servedAnswer struct {
	status int
	header []string
	body   string
}

func lifecycleAnswers(t *testing.T, accounts authprovision.ServiceAccounts, bearer string) map[string]servedAnswer {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), "postgres://cells@127.0.0.1:1/cells?connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	routes := Routes(Deps{Pool: pool, Guard: policy.NewGuard(nil, quietLogger()), ClickHouse: nilConn{}, ServiceAccounts: accounts}, quietLogger())
	server, err := NewServer(apiTestConfig(), quietLogger(), routes)
	if err != nil {
		t.Fatal(err)
	}
	id := "77777777-7777-4777-8777-777777777777"
	out := map[string]servedAnswer{}
	for _, c := range []struct{ name, path, body string }{
		{"create", "/api/v1/admin/service-accounts", `{"organization_id":"` + id + `","owner_principal_id":"` + id + `","name":"x"}`},
		{"disable", "/api/v1/admin/service-accounts/" + id + "/disable", `{}`},
		{"enable", "/api/v1/admin/service-accounts/" + id + "/enable", `{}`},
		{"owner", "/api/v1/admin/service-accounts/" + id + "/owner", `{"owner_principal_id":"` + id + `"}`},
	} {
		request := httptest.NewRequest(http.MethodPost, c.path, strings.NewReader(c.body))
		request.Header.Set("Content-Type", "application/json")
		if bearer != "" {
			request.Header.Set("Authorization", bearer)
		}
		recorder := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, request)
		response := recorder.Result()
		body, _ := io.ReadAll(response.Body)
		var header []string
		for name, values := range response.Header {
			if name != "Date" && name != "X-Request-Id" {
				header = append(header, name+"="+strings.Join(values, ","))
			}
		}
		sort.Strings(header)
		out[c.name] = servedAnswer{response.StatusCode, header, string(body)}
	}
	return out
}

// Switch off: the four lifecycle paths are not routed and answer main's 404,
// byte for byte (status, body and every header but Date and X-Request-Id),
// with or without a bearer. Switch on: they are routed (no-store, and not the
// 404).
func TestServiceAccountLifecyclePathsSwitchOffAnswerMains404(t *testing.T) {
	for _, bearer := range []string{"", "Bearer not-a-token"} {
		for name, got := range lifecycleAnswers(t, nil, bearer) {
			if got.status != http.StatusNotFound || got.body != `{"detail":"Not Found"}` || strings.Join(got.header, "\n") != strings.Join(mainNotFoundHeader, "\n") {
				t.Errorf("switch off, %s (bearer %t): %d %s\n%s\nwant main's 404 {\"detail\":\"Not Found\"}\n%s", name, bearer != "", got.status, got.body,
					strings.Join(got.header, "\n"), strings.Join(mainNotFoundHeader, "\n"))
			}
		}
	}
	for name, got := range lifecycleAnswers(t, stubAccounts{}, "") {
		if got.status == http.StatusNotFound || !strings.Contains(strings.Join(got.header, "\n"), "Cache-Control=no-store") {
			t.Errorf("switch on, %s: %d %s %v; want the routed answer with no-store", name, got.status, got.body, got.header)
		}
	}
}

// The lifecycle client follows the key switch: off builds none, on builds the
// lifecycle client over the workload credential; on with no credential is a
// start-up error naming the switch.
func TestServiceAccountsClientFollowsTheSwitch(t *testing.T) {
	url := "http://auth-service:8095"
	off := mustProxyConfig(t, map[string]string{EnvAuthProxyURL: url})
	cred, _, err := newCredentials(off, issueEnv(workloadEnv(t, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if accounts, err := serviceAccounts(off, cred); err != nil || accounts != nil {
		t.Fatalf("switch off: client %v err %v, want none", accounts, err)
	}
	on := mustProxyConfig(t, map[string]string{EnvAuthProxyURL: url, EnvAuthProxyServiceAccountKeys: "true"})
	cred, _, err = newCredentials(on, issueEnv(workloadEnv(t, nil)))
	if err != nil {
		t.Fatal(err)
	}
	accounts, err := serviceAccounts(on, cred)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := accounts.(*authprovision.AccountClient); !ok {
		t.Fatalf("switch on: client %T, want *authprovision.AccountClient", accounts)
	}
	if _, err := serviceAccounts(on, provisioningCredential{}); err == nil || !strings.Contains(err.Error(), EnvAuthProxyServiceAccountKeys) {
		t.Fatalf("switch on with no credential: err %v, want a start-up error naming %s", err, EnvAuthProxyServiceAccountKeys)
	}
}
