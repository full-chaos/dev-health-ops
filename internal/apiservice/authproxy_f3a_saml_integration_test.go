//go:build integration

package apiservice

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

// With the F3a prod flags (SAML proxy off), an SSO state secret (so the real
// SAML handlers mount, as in prod) and no SAML provider stored, the SAML
// sign-in routes answer go-api's own 404 "SSO provider not found" and never
// reach auth-service. The ACS calls carry SAMLResponse payloads the SAML
// parser refuses (400 "SAML authentication failed") if it ever ran: a 404 is
// only possible when the provider lookup refuses before any parse.
func TestAuthProxyF3aSAMLRoutesAnswerProviderNotFoundBeforeAnyParse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	pg, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pg.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, pg.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	pgschema.Apply(ctx, t, pool)
	var providers int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM sso_providers`).Scan(&providers); err != nil || providers != 0 {
		t.Fatalf("sso_providers = %d (%v), want 0", providers, err)
	}

	auth, err := policy.NewAuthenticator(policy.PGStore{Pool: pool}, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	stub := newStubUpstream(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })
	routes := Routes(Deps{Pool: pool, Auth: auth, Guard: policy.NewGuard(auth, quietLogger()),
		SSOStateSecret: "f3a-sso-state-secret-f3a-sso-state", AuthProxy: mustProxyConfig(t, f3aProdFlags(stub.URL)),
		Limits: httpapi.NewMemoryCounters(time.Now)}, quietLogger())
	server, err := NewServer(config.Config{APIAddress: "127.0.0.1:0", CORSAllowedOrigins: []string{proxyOrigin}}, quietLogger(), routes)
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()

	provider := uuid.NewString()
	saml := "/api/v1/auth/saml/" + provider
	malformed := []string{
		"%%not-base64%%",
		base64.StdEncoding.EncodeToString([]byte(`<samlp:Response xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol"><unclosed`)),
		base64.StdEncoding.EncodeToString([]byte(`<!DOCTYPE r [<!ENTITY a "aaaaaaaa"><!ENTITY b "&a;&a;&a;&a;">]><r>&b;</r>`)),
	}
	calls := []proxyCall{
		{method: http.MethodGet, target: saml + "/metadata"},
		{method: http.MethodPost, target: saml + "/initiate", body: `{}`},
		{method: http.MethodPost, target: saml + "/initiate", body: `{"relay_state": "/x"}`},
	}
	for _, payload := range malformed {
		for _, relay := range []string{`null`, `"forged-relay-state"`} {
			body, _ := json.Marshal(map[string]json.RawMessage{"SAMLResponse": mustJSON(t, payload), "RelayState": json.RawMessage(relay)})
			calls = append(calls, proxyCall{method: http.MethodPost, target: saml + "/acs", body: string(body)})
		}
	}
	for _, call := range calls {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, call.request(t))
		var detail struct {
			Detail string `json:"detail"`
		}
		_ = json.Unmarshal(recorder.Body.Bytes(), &detail)
		if recorder.Code != http.StatusNotFound || detail.Detail != "SSO provider not found" {
			t.Errorf("%s %s %.60s = %d %s, want 404 SSO provider not found", call.method, call.target, call.body, recorder.Code, strings.TrimSpace(recorder.Body.String()))
		}
	}
	if seen := stub.requests(); len(seen) != 0 {
		t.Errorf("auth-service saw %d requests with the SAML proxy off (first %s %s)", len(seen), seen[0].method, seen[0].path)
	}
}

func mustJSON(t *testing.T, value string) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
