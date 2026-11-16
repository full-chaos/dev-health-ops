//go:build integration

package apiservice

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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

// With SAML forwarding off every valid SAML request answers config_missing without reaching auth-service.
func TestAuthProxySAMLForwardOffAnswersConfigMissingBeforeAnyProviderLookupOrParse(t *testing.T) {
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
		SSOStateSecret: "f3a-sso-state-secret-f3a-sso-state", SessionIssuer: stubSessionIssuer{}, AuthProxy: mustProxyConfig(t, f3aProdFlags(stub.URL)),
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
	want := httptest.NewRecorder()
	policy.WriteConfigMissing(want, "AUTH_PROXY_SAML")
	for _, call := range calls {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, call.request(t))
		if recorder.Code != want.Code || !bytes.Equal(recorder.Body.Bytes(), want.Body.Bytes()) {
			t.Errorf("%s %s %.60s = %d %s, want %d %s", call.method, call.target, call.body, recorder.Code, recorder.Body, want.Code, want.Body)
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
