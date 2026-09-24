//go:build integration

package main

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

// The operator gate, exercised through the real /query handler: a signed
// envelope is verified by the real principal.Verifier, its claims reach the
// resolver through the real middleware, and the routing row is read from a
// real Postgres. No claims are built by hand.

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

func TestDataHealthRoute_OperatorGateThroughTheSignedEnvelope(t *testing.T) {
	pool := startTestRegistryPostgres(t)
	if _, err := pool.Exec(t.Context(), dataHealthPostgresDDL); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(t.Context(), `INSERT INTO sync_configurations (id, org_id, name, provider) VALUES
		('aaaaaaaa-0000-0000-0000-000000000001', 'org-1', 'gh-main', 'github'),
		('aaaaaaaa-0000-0000-0000-000000000002', 'org-2', 'foreign', 'jira')`); err != nil {
		t.Fatal(err)
	}

	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	_, otherPriv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := principal.NewVerifier(writeTestJWKS(t, pub), itTestIssuer, itTestAudience)
	if err != nil {
		t.Fatal(err)
	}
	handler, _, _ := newQueryHandler(emptyCHClient{}, pool, verifier, itTestSchemaDigest)
	setRoutingMode(t, pool, digestHex(registeredConnectorsDataHealthDocument), "connectorsDataHealth", "canary")

	post := func(bearer string) (int, string) {
		rec := postGraphQLWithVariables(t, handler, registeredConnectorsDataHealthDocument, bearer, map[string]any{"teamId": "ALL"})
		return rec.Code, rec.Body.String()
	}
	valid := time.Now().Add(time.Hour)

	for _, tc := range []struct {
		name  string
		token string
		code  int
	}{
		{"no envelope", "", http.StatusUnauthorized},
		{"envelope signed by another key", signDataHealthEnvelope(t, otherPriv, principal.Claims{OrgID: "org-1", Role: "admin"}, valid), http.StatusUnauthorized},
		{"expired envelope", signDataHealthEnvelope(t, priv, principal.Claims{OrgID: "org-1", Role: "admin"}, time.Now().Add(-time.Hour)), http.StatusUnauthorized},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, body := post(tc.token)
			if code != tc.code || strings.Contains(body, "gh-main") {
				t.Fatalf("code=%d body=%s", code, body)
			}
		})
	}

	const refusal = "Data health requires operator access"
	for _, tc := range []struct {
		name   string
		claims principal.Claims
		served bool
	}{
		{"role empty", principal.Claims{OrgID: "org-1"}, false},
		{"role member", principal.Claims{OrgID: "org-1", Role: "member"}, false},
		{"role viewer", principal.Claims{OrgID: "org-1", Role: "viewer"}, false},
		{"role operator", principal.Claims{OrgID: "org-1", Role: "operator"}, true},
		{"role admin", principal.Claims{OrgID: "org-1", Role: "admin"}, true},
		{"role owner", principal.Claims{OrgID: "org-1", Role: "owner"}, true},
		{"role ADMIN", principal.Claims{OrgID: "org-1", Role: "ADMIN"}, true},
		{"superuser flag, member role", principal.Claims{OrgID: "org-1", Role: "member", IsSuperuser: true}, true},
		{"superuser verified-only", principal.Claims{OrgID: "org-1", Role: "member", IsSuperuserVerified: true}, false},
		{"superuser verified and raw", principal.Claims{OrgID: "org-1", Role: "member", IsSuperuser: true, IsSuperuserVerified: true}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, body := post(signDataHealthEnvelope(t, priv, tc.claims, valid))
			if code != http.StatusOK {
				t.Fatalf("code=%d body=%s", code, body)
			}
			if tc.served {
				if strings.Contains(body, `"errors"`) || !strings.Contains(body, `"provider":"github"`) || strings.Contains(body, "jira") {
					t.Fatalf("an operator is served only its own org's connectors: %s", body)
				}
				return
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
