//go:build integration

package sso_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
)

const emailLeakMarker = "MARKER77"

// wireUnusableEmails are the bad forms that survive the JSON and XML transport of the fake identity providers.
func wireUnusableEmails() map[string]string {
	return map[string]string{
		"replacement char": emailLeakMarker + "�@allowed.example",
		"c1":               emailLeakMarker + "\u0085x@allowed.example",
	}
}

func assertEmailRefused(t *testing.T, ctx context.Context, st stack, providerID uuid.UUID, before rowCounts, status int, body map[string]any) {
	t.Helper()
	detail, _ := body["detail"].(map[string]any)
	if status != http.StatusBadRequest || detail["reason"] != "invalid_provider_email" || body["access_token"] != nil {
		t.Fatalf("status=%d body=%v, want 400 invalid_provider_email and no token", status, body)
	}
	if bytes.Contains([]byte(fmt.Sprint(body)), []byte(emailLeakMarker)) {
		t.Fatalf("the address is in the answer: %v", body)
	}
	assertNoRowsWritten(t, ctx, st, before)
	var providerStatus string
	var lastError *string
	if err := st.pool.QueryRow(ctx, `SELECT status, last_error FROM sso_providers WHERE id = $1`, providerID).Scan(&providerStatus, &lastError); err != nil {
		t.Fatal(err)
	}
	if providerStatus != "active" || lastError != nil {
		t.Fatalf("provider marked failed: status=%s last_error set=%v", providerStatus, lastError != nil)
	}
}

func TestEmailGuardOIDCCallbackRefusesAnUnusableEmail(t *testing.T) {
	for name, email := range wireUnusableEmails() {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
			defer cancel()
			st := startStack(t, ctx)
			idp := startFakeIdP(t)
			orgID := seedOrg(t, ctx, st.pool, "enterprise")
			config := fmt.Sprintf(`{"client_id":"test-client","issuer":%q,"token_endpoint":%q,"jwks_uri":%q,"userinfo_endpoint":%q,"scopes":["openid","email"]}`,
				idp.server.URL, idp.server.URL+"/token", idp.server.URL+"/jwks", idp.server.URL+"/userinfo")
			providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{
				protocol: "oidc", status: "active", config: config, autoProvision: true, clientSecretEncryptedFallback: "s",
			})
			before := snapshotRows(t, ctx, st)
			status, body := oidcSubjectCallback(t, st, idp, providerID, email, "subject-1")
			assertEmailRefused(t, ctx, st, providerID, before, status, body)
		})
	}
}

func TestEmailGuardSAMLCallbackRefusesAnUnusableEmail(t *testing.T) {
	for name, email := range wireUnusableEmails() {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
			defer cancel()
			st := startStack(t, ctx)
			idp := newSAMLIdP(t)
			providerID := seedSAMLOrgProviderAuto(t, ctx, st, idp, nameIDSP, true)
			before := snapshotRows(t, ctx, st)
			response := idp.signedSAMLResponse(t, samlResponseOpts{
				spEntityID: nameIDSP, acsURL: samlACSOverrideFor(providerID), email: email, fullName: "Person", nameID: "name-1",
			})
			status, body := postJSON(t, st.server, "/api/v1/auth/saml/"+providerID.String()+"/acs", map[string]any{"SAMLResponse": response})
			assertEmailRefused(t, ctx, st, providerID, before, status, body)
		})
	}
}

func TestEmailGuardOAuthCallbackRefusesAnUnusableEmail(t *testing.T) {
	for name, email := range map[string]string{
		"replacement char": emailLeakMarker + `�@example.test`,
		"nul":              emailLeakMarker + `\u0000@example.test`,
		"control":          emailLeakMarker + `\u0001@example.test`,
		"c1":               emailLeakMarker + `\u0085x@example.test`,
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
			defer cancel()
			st, _, providerID, _ := oauthStack(t, ctx, oauthProfileWith("7", email, "Fay", ""))
			before := snapshotRows(t, ctx, st)
			status, body := oauthCallbackOnce(t, st, providerID)
			assertEmailRefused(t, ctx, st, providerID, before, status, body)
		})
	}
}
