//go:build integration

package server

import (
	"context"
	"crypto/ed25519"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/golang-jwt/jwt/v5"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
)

// The operator gate, exercised through the real /query handler: the internal
// identity headers state the caller, its claims reach the resolver through
// the real middleware, and the read goes to a real Postgres. No claims are
// built by hand. The effective-principal envelope is a retired class.

func signDataHealthEnvelope(t *testing.T, priv ed25519.PrivateKey, claims principal.Claims, expires time.Time) string {
	t.Helper()
	claims.SchemaVersion = principal.SupportedSchemaVersion
	claims.RegisteredClaims = jwt.RegisteredClaims{
		Issuer:    itTestIssuer,
		Audience:  jwt.ClaimStrings{itTestAudience},
		Subject:   "user-1",
		ExpiresAt: jwt.NewNumericDate(expires),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
	token.Header["kid"] = itTestKID
	signed, err := token.SignedString(priv)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

func TestDataHealthRoute_OperatorGateThroughTheInternalIdentity(t *testing.T) {
	pool := startTestRegistryPostgres(t)
	if _, err := pool.Exec(t.Context(), `INSERT INTO sync_configurations (id, org_id, name, provider, created_at, updated_at) VALUES
		('aaaaaaaa-0000-0000-0000-000000000001', 'org-1', 'gh-main', 'github', now(), now()),
		('aaaaaaaa-0000-0000-0000-000000000002', 'org-2', 'foreign', 'jira', now(), now())`); err != nil {
		t.Fatal(err)
	}

	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := principal.NewVerifier(writeTestJWKS(t, pub), itTestIssuer, itTestAudience)
	if err != nil {
		t.Fatal(err)
	}
	handler, _, _, _, _ := newQueryHandler(emptyCHClient{}, pool, verifier, itTestSchemaDigest, withIssuer)

	post := func(carrier func(*http.Request)) (int, string) {
		rec := postGraphQLWithVariables(t, handler, registeredConnectorsDataHealthDocument, carrier, map[string]any{})
		return rec.Code, rec.Body.String()
	}
	valid := time.Now().Add(time.Hour)

	for _, tc := range []struct {
		name    string
		carrier func(*http.Request)
		code    int
	}{
		{"no credential", nil, http.StatusUnauthorized},
		{"the retired envelope", irBearer(signDataHealthEnvelope(t, priv, principal.Claims{OrgID: "org-1", Role: "admin"}, valid)), http.StatusUnauthorized},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, body := post(tc.carrier)
			if code != tc.code || strings.Contains(body, "gh-main") {
				t.Fatalf("code=%d body=%s", code, body)
			}
		})
	}

	// CHAOS-13950: the header carrier may not state an operator role or superuser: those are refused 403 at the carrier,
	// before the operator gate and any read. The gate itself is reached by an edge token's membership role.
	const refusal = "Data health requires operator access"
	for _, tc := range []struct {
		name             string
		role             string
		super, forbidden bool
	}{
		{"role empty", "", false, false},
		{"role member", "member", false, false},
		{"role viewer", "viewer", false, false},
		{"role operator", "operator", false, true},
		{"role admin", "admin", false, true},
		{"role owner", "owner", false, true},
		{"role ADMIN", "ADMIN", false, true},
		{"superuser, member role", "member", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, body := post(iaHeaders("org-1", tc.role, tc.super, false))
			if tc.forbidden {
				if code != http.StatusForbidden || strings.Contains(body, "github") {
					t.Fatalf("an elevated header claim: code=%d body=%s, want 403 before any read", code, body)
				}
				return
			}
			if code != http.StatusOK {
				t.Fatalf("code=%d body=%s", code, body)
			}
			if !strings.Contains(body, refusal) || !strings.Contains(body, "AUTHORIZATION_ERROR") || strings.Contains(body, "github") {
				t.Fatalf("a non-operator must be refused before any read: %s", body)
			}
		})
	}
}

// emptyCHClient answers every ClickHouse read with no rows, so the test
// isolates the connector read (Postgres) and the gate.
type emptyCHClient struct{}

func (emptyCHClient) Query(context.Context, string, []clickhouse.Binding) (clickhouse.RowScanner, error) {
	return &fakeRows{}, nil
}
