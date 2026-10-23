package acr_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/apiservice/acr"
	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/usertokens"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/usertokens/withissuer"
)

// countingCredentials is the credential store of the svc_acr_ routes with every call counted.
type countingCredentials struct {
	mu                       sync.Mutex
	finds, markUsed, audited int
	outcomes                 []string
	known                    map[string]acr.Credential
}

func (c *countingCredentials) Find(_ context.Context, token string) (acr.Credential, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.finds++
	cred, ok := c.known[token]
	return cred, ok, nil
}
func (c *countingCredentials) MarkUsed(context.Context, string, time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.markUsed++
	return nil
}
func (c *countingCredentials) Audit(_ context.Context, _, _, outcome string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.audited++
	c.outcomes = append(c.outcomes, outcome)
	return nil
}

type countingEntitlements struct {
	mu            sync.Mutex
	lookups, redy int
}

func (e *countingEntitlements) Lookup(context.Context, string) (acr.Entitlement, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.lookups++
	return acr.Entitlement{}, nil
}
func (e *countingEntitlements) Ready(context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.redy++
	return nil
}

type countingLLMConfig struct {
	mu    sync.Mutex
	reads int
}

func (l *countingLLMConfig) OrgConfig(context.Context, string) (llmorgsettings.OrgConfig, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.reads++
	return llmorgsettings.OrgConfig{}, false, nil
}

const acrOrg = "11111111-1111-4111-8111-111111111111"

func acrRequest(path, authorization string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, path, nil)
	r.SetPathValue("org_id", acrOrg)
	if authorization != "" {
		r.Header.Set("Authorization", authorization)
	}
	return r
}

// The three internal acr routes (health, entitlement, org llm-config) check their own svc_acr_ bearer. A
// user token of any kind is refused 401 "Unauthorized" at the bearer gate: no credential lookup (counted),
// no last-used stamp, no entitlement or BYO-config read, no database connection, nothing echoed.
func TestInternalACRRoutesRefuseUserTokens(t *testing.T) {
	tokens := withissuer.All(t)
	pool, connects := usertokens.CountingPool(t)
	creds := &countingCredentials{}
	entitlements := &countingEntitlements{}
	llm := &countingLLMConfig{}
	var logs bytes.Buffer
	routes := acr.Routes(acr.Deps{
		Store: entitlements, Auth: creds, LLMConfig: llm, Credentials: acr.PostgresCredentialVerifier{Pool: pool},
		Logger: slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	})
	byPattern := map[string]http.Handler{}
	for _, route := range routes {
		byPattern[route.Pattern] = route.Handler
	}
	targets := []struct{ key, pattern, path string }{
		{"health", "/api/v1/internal/acr/health", "/api/v1/internal/acr/health"},
		{"entitlement", "/api/v1/internal/acr/entitlements/{org_id}", "/api/v1/internal/acr/entitlements/" + acrOrg},
		{"llm", "/api/v1/internal/acr/orgs/{org_id}/llm-config", "/api/v1/internal/acr/orgs/" + acrOrg + "/llm-config"},
	}
	for _, target := range targets {
		handler := byPattern[target.pattern]
		if handler == nil {
			t.Fatalf("no route %s", target.pattern)
		}
		for _, tok := range tokens {
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, acrRequest(target.path, "Bearer "+tok.Value))
			var body struct {
				Detail string `json:"detail"`
			}
			if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil || recorder.Code != http.StatusUnauthorized || body.Detail != "Unauthorized" {
				t.Errorf("%s with %s: %d %s, want 401 Unauthorized", target.key, tok.Kind, recorder.Code, recorder.Body.String())
			}
			usertokens.NoLeak(t, target.key+" body", recorder.Body.String(), tokens)
		}
	}
	if creds.finds != 0 || creds.markUsed != 0 {
		t.Errorf("a user token reached the credential store: %d finds, %d last-used stamps, want 0", creds.finds, creds.markUsed)
	}
	if entitlements.lookups != 0 || entitlements.redy != 0 {
		t.Errorf("a user token reached the entitlement store: %d lookups, %d ready checks, want 0", entitlements.lookups, entitlements.redy)
	}
	if llm.reads != 0 {
		t.Errorf("a user token read a BYO llm config: %d reads, want 0", llm.reads)
	}
	if got := connects.Count(); got != 0 {
		t.Errorf("a user token reached the database: %d connection attempt(s), want 0", got)
	}
	for _, outcome := range creds.outcomes {
		if outcome != "missing_or_malformed_token" {
			t.Errorf("an audit row says %q for a refused user token, want missing_or_malformed_token", outcome)
		}
	}
	usertokens.NoLeak(t, "the logs", logs.String(), tokens)
}

// The control of the test above: a genuine svc_acr_ credential passes the same gate and is looked up, so
// the zeros above are the gate's work, not a fixture that can never reach the store.
func TestInternalACRControlAServiceCredentialReachesTheStore(t *testing.T) {
	creds := &countingCredentials{known: map[string]acr.Credential{
		"svc_acr_control": {ID: "cred-1", ServiceName: "acr", Scopes: []string{acr.EntitlementScope}},
	}}
	entitlements := &countingEntitlements{}
	routes := acr.Routes(acr.Deps{Store: entitlements, Auth: creds, Logger: slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))})
	for _, route := range routes {
		if route.Pattern != "/api/v1/internal/acr/health" {
			continue
		}
		recorder := httptest.NewRecorder()
		route.Handler.ServeHTTP(recorder, acrRequest("/api/v1/internal/acr/health", "Bearer svc_acr_control"))
		if recorder.Code != http.StatusOK {
			t.Fatalf("health with a service credential: %d %s, want 200", recorder.Code, recorder.Body.String())
		}
	}
	if creds.finds == 0 {
		t.Fatal("a service credential never reached the credential store: the counters detect nothing")
	}
}
