package acr_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/authverify"
	"github.com/full-chaos/dev-health-ops/internal/apiservice/acr"
	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/usertokens"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/usertokens/withissuer"
)

// countingCredentials is the audit store of the internal acr routes with every call counted.
type countingCredentials struct {
	mu       sync.Mutex
	audited  int
	outcomes []string
}

func (c *countingCredentials) Audit(_ context.Context, _, _, _, outcome string) error {
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

type countingAccess struct {
	mu    sync.Mutex
	reads int
}

func (a *countingAccess) Access(context.Context, acr.AccessQuery) (acr.PrincipalAccess, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.reads++
	return acr.PrincipalAccess{}, nil
}

const (
	acrOrg  = "11111111-1111-4111-8111-111111111111"
	acrUser = "22222222-2222-4222-8222-222222222222"
)

func acrRequest(path, authorization string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, path, nil)
	r.SetPathValue("org_id", acrOrg)
	r.SetPathValue("user_id", acrUser)
	if authorization != "" {
		r.Header.Set("Authorization", authorization)
	}
	return r
}

// The four internal acr routes (health, entitlement, org llm-config, principal access) accept only the
// acr-api workload token. A user token of any kind is refused 401 "Unauthorized" at the bearer gate: no
// entitlement, BYO-config or principal access read, no database connection, nothing echoed.
func TestInternalACRRoutesRefuseUserTokens(t *testing.T) {
	tokens := withissuer.All(t)
	creds := &countingCredentials{}
	entitlements := &countingEntitlements{}
	llm := &countingLLMConfig{}
	access := &countingAccess{}
	var logs bytes.Buffer
	routes := acr.Routes(acr.Deps{
		Store: entitlements, Auth: creds, LLMConfig: llm, Workload: controlWorkload(t).auth,
		Access: access, Logger: slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	})
	byPattern := map[string]http.Handler{}
	for _, route := range routes {
		byPattern[route.Pattern] = route.Handler
	}
	targets := []struct{ key, pattern, path string }{
		{"health", "/api/v1/internal/acr/health", "/api/v1/internal/acr/health"},
		{"entitlement", "/api/v1/internal/acr/entitlements/{org_id}", "/api/v1/internal/acr/entitlements/" + acrOrg},
		{"llm", "/api/v1/internal/acr/orgs/{org_id}/llm-config", "/api/v1/internal/acr/orgs/" + acrOrg + "/llm-config"},
		{"access", acr.PrincipalAccessPattern, "/api/v1/internal/acr/orgs/" + acrOrg + "/principals/" + acrUser + "/access"},
	}
	if len(targets) != len(routes) {
		t.Fatalf("%d acr routes are mounted, %d are proved here: add the new route to targets", len(routes), len(targets))
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
	if entitlements.lookups != 0 || entitlements.redy != 0 {
		t.Errorf("a user token reached the entitlement store: %d lookups, %d ready checks, want 0", entitlements.lookups, entitlements.redy)
	}
	if llm.reads != 0 {
		t.Errorf("a user token read a BYO llm config: %d reads, want 0", llm.reads)
	}
	if access.reads != 0 {
		t.Errorf("a user token read a principal's access: %d reads, want 0", access.reads)
	}
	if creds.audited == 0 {
		t.Error("no refusal was audited: the entitlement route measured nothing")
	}
	for _, outcome := range creds.outcomes {
		if outcome != "missing_or_malformed_token" {
			t.Errorf("an audit row says %q for a refused user token, want missing_or_malformed_token", outcome)
		}
	}
	usertokens.NoLeak(t, "the logs", logs.String(), tokens)
}

// controlCase is an acr-api workload verifier and a token it accepts.
type controlCase struct {
	auth  *acr.WorkloadAuth
	token string
}

func controlWorkload(t *testing.T) controlCase {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	const issuer, subject, kid = "https://auth.example.test", "prn_AAAAAAAAAAAAAAAAAAAAAA", "kid-control"
	verifier, err := authverify.NewWorkloadVerifier(authverify.WorkloadVerifierConfig{
		Issuer: issuer, Audience: "dev-health-api", Keys: authverify.StaticKeys{kid: pub},
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_800_000_000, 0)
	enc := base64.RawURLEncoding
	header, _ := json.Marshal(map[string]any{"alg": "EdDSA", "typ": "workload+jwt", "kid": kid})
	claims, _ := json.Marshal(map[string]any{
		"iss": issuer, "aud": "dev-health-api", "sub": subject, "principal_type": "workload",
		"jti": "000000000000000000000001", "iat": now.Unix() - 10, "nbf": now.Unix() - 10, "exp": now.Unix() + 240,
	})
	input := enc.EncodeToString(header) + "." + enc.EncodeToString(claims)
	return controlCase{
		auth:  &acr.WorkloadAuth{Verifier: verifier, Source: acr.StaticSubject(subject), Now: func() time.Time { return now }},
		token: input + "." + enc.EncodeToString(ed25519.Sign(priv, []byte(input))),
	}
}

// The control of the tests here: the acr-api workload token passes the same gate on every route and
// reaches the stores, so the zeros here are the gate's work, not a fixture that can never reach a store.
func TestInternalACRControlAWorkloadTokenReachesTheStores(t *testing.T) {
	control := controlWorkload(t)
	creds := &countingCredentials{}
	entitlements := &countingEntitlements{}
	llm := &countingLLMConfig{}
	access := &countingAccess{}
	routes := acr.Routes(acr.Deps{
		Store: entitlements, Auth: creds, LLMConfig: llm, Access: access, Workload: control.auth,
		Logger: slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)),
	})
	for _, route := range routes {
		path, ok := internalACRPaths[route.Pattern]
		if !ok {
			t.Fatalf("route %s is not proved here", route.Pattern)
		}
		recorder := httptest.NewRecorder()
		route.Handler.ServeHTTP(recorder, acrRequest(path, "Bearer "+control.token))
		if recorder.Code != http.StatusOK && recorder.Code != http.StatusNotFound {
			t.Errorf("%s with the workload token: %d %s, want it served", route.Pattern, recorder.Code, recorder.Body.String())
		}
	}
	if entitlements.lookups == 0 || entitlements.redy == 0 || llm.reads == 0 || access.reads == 0 {
		t.Fatalf("the workload token missed a store (lookups %d, ready %d, llm %d, access %d): the counters detect nothing",
			entitlements.lookups, entitlements.redy, llm.reads, access.reads)
	}
	if len(creds.outcomes) != 1 || creds.outcomes[0] != "success" {
		t.Fatalf("audit outcomes %v, want [success]", creds.outcomes)
	}
}

var internalACRPaths = map[string]string{
	"/api/v1/internal/acr/health":                   "/api/v1/internal/acr/health",
	"/api/v1/internal/acr/entitlements/{org_id}":    "/api/v1/internal/acr/entitlements/" + acrOrg,
	"/api/v1/internal/acr/orgs/{org_id}/llm-config": "/api/v1/internal/acr/orgs/" + acrOrg + "/llm-config",
	acr.PrincipalAccessPattern:                      "/api/v1/internal/acr/orgs/" + acrOrg + "/principals/" + acrUser + "/access",
}

// A genuine svc_acr_ credential (known to the store, every scope, not revoked, not expired) is refused on all
// four internal acr routes with the uniform refusal a user token gets: 401 "Unauthorized", no credential
// lookup or last-used stamp, no entitlement, BYO-config or principal access read, no database connection, and
// the audit row (entitlement route) says missing_or_malformed_token.
func TestInternalACRRoutesRefuseServiceCredential(t *testing.T) {
	const token = "svc_acr_valid-credential-0123456789abcdef"
	creds := &countingCredentials{}
	entitlements := &countingEntitlements{}
	llm := &countingLLMConfig{}
	access := &countingAccess{}
	var logs bytes.Buffer
	routes := acr.Routes(acr.Deps{
		Store: entitlements, Auth: creds, LLMConfig: llm, Workload: controlWorkload(t).auth,
		Access: access, Logger: slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	})
	paths := map[string]string{
		"/api/v1/internal/acr/health":                   "/api/v1/internal/acr/health",
		"/api/v1/internal/acr/entitlements/{org_id}":    "/api/v1/internal/acr/entitlements/" + acrOrg,
		"/api/v1/internal/acr/orgs/{org_id}/llm-config": "/api/v1/internal/acr/orgs/" + acrOrg + "/llm-config",
		acr.PrincipalAccessPattern:                      "/api/v1/internal/acr/orgs/" + acrOrg + "/principals/" + acrUser + "/access",
	}
	if len(paths) != len(routes) {
		t.Fatalf("%d acr routes are mounted, %d are proved here: add the new route", len(routes), len(paths))
	}
	for _, route := range routes {
		path, ok := paths[route.Pattern]
		if !ok {
			t.Fatalf("route %s is not proved here", route.Pattern)
		}
		recorder := httptest.NewRecorder()
		route.Handler.ServeHTTP(recorder, acrRequest(path, "Bearer "+token))
		var body struct {
			Detail string `json:"detail"`
		}
		if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil || recorder.Code != http.StatusUnauthorized || body.Detail != "Unauthorized" {
			t.Errorf("%s with a valid svc_acr_ credential: %d %s, want 401 Unauthorized", route.Pattern, recorder.Code, recorder.Body.String())
		}
	}
	if entitlements.lookups != 0 || entitlements.redy != 0 {
		t.Errorf("a svc_acr_ credential reached the entitlement store: %d lookups, %d ready checks, want 0", entitlements.lookups, entitlements.redy)
	}
	if llm.reads != 0 || access.reads != 0 {
		t.Errorf("a svc_acr_ credential read a BYO llm config (%d) or a principal's access (%d), want 0", llm.reads, access.reads)
	}
	if len(creds.outcomes) != 1 || creds.outcomes[0] != "missing_or_malformed_token" {
		t.Errorf("audit outcomes %v, want exactly [missing_or_malformed_token] (the entitlement route)", creds.outcomes)
	}
	if strings.Contains(logs.String(), token) {
		t.Error("the logs carry the svc_acr_ token")
	}
}
