//go:build integration

package main

import (
	"crypto/ed25519"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/principal"
)

// The platform gate and the org check, exercised through the real /query
// handler: a signed envelope is verified by the real principal.Verifier and its
// claims reach the resolvers through the real middleware. No claims are built
// by hand.
func TestProductTelemetryRoute_GatesThroughTheSignedEnvelope(t *testing.T) {
	pool := startTestRegistryPostgres(t)
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
	setRoutingMode(t, pool, digestHex(registeredProductTelemetryPlatformDashboardDocument), "productTelemetryPlatformDashboard", "canary")
	setRoutingMode(t, pool, digestHex(registeredProductTelemetryDashboardDocument), "productTelemetryDashboard", "canary")

	input := map[string]any{"startDate": "2026-01-01", "endDate": "2026-01-08"}
	platform := func(bearer string) (int, string) {
		rec := postGraphQLWithVariables(t, handler, registeredProductTelemetryPlatformDashboardDocument, bearer, map[string]any{"input": input})
		return rec.Code, rec.Body.String()
	}
	org := func(bearer, orgID string) (int, string) {
		rec := postGraphQLWithVariables(t, handler, registeredProductTelemetryDashboardDocument, bearer, map[string]any{"orgId": orgID, "input": input})
		return rec.Code, rec.Body.String()
	}
	valid := time.Now().Add(time.Hour)

	for _, tc := range []struct {
		name  string
		token string
	}{
		{"no envelope", ""},
		{"envelope signed by another key", signDataHealthEnvelope(t, otherPriv, principal.Claims{OrgID: "org-1", IsSuperuser: true}, valid)},
		{"expired envelope", signDataHealthEnvelope(t, priv, principal.Claims{OrgID: "org-1", IsSuperuser: true}, time.Now().Add(-time.Hour))},
	} {
		t.Run("platform "+tc.name, func(t *testing.T) {
			if code, body := platform(tc.token); code != http.StatusUnauthorized || strings.Contains(body, "totals") {
				t.Fatalf("code=%d body=%s", code, body)
			}
		})
	}

	impersonator := "admin-user"
	for _, tc := range []struct {
		name   string
		claims principal.Claims
		served bool
	}{
		{"member", principal.Claims{OrgID: "org-1", Role: "owner"}, false},
		{"verified-only flag", principal.Claims{OrgID: "org-1", IsSuperuserVerified: true}, false},
		{"superuser", principal.Claims{OrgID: "org-1", IsSuperuser: true}, true},
		{"superuser without org", principal.Claims{IsSuperuser: true}, true},
		{"superuser impersonating", principal.Claims{OrgID: "org-1", Role: "member", IsSuperuser: true, ImpersonationActive: true, ImpersonatedBy: &impersonator}, false},
		{"impersonating flag alone", principal.Claims{OrgID: "org-1", ImpersonationActive: true}, false},
	} {
		t.Run("platform "+tc.name, func(t *testing.T) {
			code, body := platform(signDataHealthEnvelope(t, priv, tc.claims, valid))
			if code != http.StatusOK {
				t.Fatalf("code=%d body=%s", code, body)
			}
			if tc.served {
				if strings.Contains(body, `"errors"`) || !strings.Contains(body, `"totals"`) {
					t.Fatalf("a platform admin is served: %s", body)
				}
				return
			}
			if !strings.Contains(body, "Platform admin access required") || !strings.Contains(body, "AUTHORIZATION_ERROR") || strings.Contains(body, `"totals":{`) {
				t.Fatalf("a non-admin must be refused before any read: %s", body)
			}
		})
	}

	for _, tc := range []struct {
		name   string
		claims principal.Claims
		orgArg string
		served bool
	}{
		{"own org", principal.Claims{OrgID: "org-1"}, "org-1", true},
		{"other org", principal.Claims{OrgID: "org-1"}, "org-2", false},
		{"superuser other org", principal.Claims{OrgID: "org-1", IsSuperuser: true}, "org-2", false},
	} {
		t.Run("org "+tc.name, func(t *testing.T) {
			code, body := org(signDataHealthEnvelope(t, priv, tc.claims, valid), tc.orgArg)
			if code != http.StatusOK {
				t.Fatalf("code=%d body=%s", code, body)
			}
			if tc.served == strings.Contains(body, `"errors"`) {
				t.Fatalf("served=%v body=%s", tc.served, body)
			}
		})
	}
}
