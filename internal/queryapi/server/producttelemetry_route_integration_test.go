//go:build integration

package server

import (
	"crypto/ed25519"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
)

// The platform gate and the org check, exercised through the real /query
// handler: the internal identity headers state the caller and its claims reach
// the resolvers through the real middleware. No claims are built by hand. The
// effective-principal envelope is a retired class: refused before any read.
func TestProductTelemetryRoute_GatesThroughTheInternalIdentity(t *testing.T) {
	pool := startTestRegistryPostgres(t)
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := principal.NewVerifier(writeTestJWKS(t, pub), itTestIssuer, itTestAudience)
	if err != nil {
		t.Fatal(err)
	}
	handler, _, _, _, _ := newQueryHandler(emptyCHClient{}, pool, verifier, itTestSchemaDigest, os.Getenv)

	input := map[string]any{"startDate": "2026-01-01", "endDate": "2026-01-08"}
	platform := func(carrier func(*http.Request)) (int, string) {
		rec := postGraphQLWithVariables(t, handler, registeredProductTelemetryPlatformDashboardDocument, carrier, map[string]any{"input": input})
		return rec.Code, rec.Body.String()
	}
	org := func(carrier func(*http.Request), orgID string) (int, string) {
		rec := postGraphQLWithVariables(t, handler, registeredProductTelemetryDashboardDocument, carrier, map[string]any{"orgId": orgID, "input": input})
		return rec.Code, rec.Body.String()
	}
	valid := time.Now().Add(time.Hour)

	for _, tc := range []struct {
		name    string
		carrier func(*http.Request)
	}{
		{"no credential", nil},
		{"the retired envelope", irBearer(signDataHealthEnvelope(t, priv, principal.Claims{OrgID: "org-1", IsSuperuser: true, IsSuperuserVerified: true}, valid))},
	} {
		t.Run("platform "+tc.name, func(t *testing.T) {
			if code, body := platform(tc.carrier); code != http.StatusUnauthorized || strings.Contains(body, "totals") {
				t.Fatalf("code=%d body=%s", code, body)
			}
		})
	}

	for _, tc := range []struct {
		name               string
		orgID, role        string
		super, imp, served bool
	}{
		{"member", "org-1", "owner", false, false, false},
		{"superuser", "org-1", "", true, false, true},
		{"superuser without org", "", "", true, false, true},
		{"superuser impersonating", "org-1", "member", true, true, false},
		{"impersonating flag alone", "org-1", "", false, true, false},
	} {
		t.Run("platform "+tc.name, func(t *testing.T) {
			code, body := platform(iaHeaders(tc.orgID, tc.role, tc.super, tc.imp))
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
		name        string
		orgID, role string
		super, imp  bool
		orgArg      string
		served      bool
	}{
		{"own org", "org-1", "", false, false, "org-1", true},
		{"other org", "org-1", "", false, false, "org-2", false},
		// As in Python's OrgIdAuthExtension: a non-impersonating superuser may
		// name another org (the operation then runs as it); an impersonating
		// one may not.
		{"superuser other org", "org-1", "", true, false, "org-2", true},
		{"superuser impersonating other org", "org-1", "member", true, true, "org-2", false},
	} {
		t.Run("org "+tc.name, func(t *testing.T) {
			code, body := org(iaHeaders(tc.orgID, tc.role, tc.super, tc.imp), tc.orgArg)
			if code != http.StatusOK {
				t.Fatalf("code=%d body=%s", code, body)
			}
			if tc.served == strings.Contains(body, `"errors"`) {
				t.Fatalf("served=%v body=%s", tc.served, body)
			}
		})
	}
}
