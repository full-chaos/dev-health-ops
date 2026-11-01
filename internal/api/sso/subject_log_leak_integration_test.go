//go:build integration

package sso_test

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

const (
	leakSubject = "SUBJLEAKID"
	leakName    = "SUBJLEAKNAME"
	leakMailTag = "subjleakmail"
)

// A refused sign-in (an unusable provider subject) reaches the answer, the audit rows and the log lines of the callback
// with the provider id, the protocol and fixed texts only: no subject, display name or address the provider sent.
func assertNothingTheProviderSentIsRecorded(t *testing.T, ctx context.Context, st stack, providerID uuid.UUID, status int, body map[string]any) {
	t.Helper()
	detail, _ := body["detail"].(map[string]any)
	if status != http.StatusBadRequest || detail["reason"] != "invalid_provider_id" {
		t.Fatalf("status=%d body=%v, want 400 invalid_provider_id", status, body)
	}
	audits := providerAudits(t, ctx, st, providerID)
	if len(audits) != 1 {
		t.Fatalf("audit rows %q, want exactly one failure row", audits)
	}
	logs := st.logs.String()
	if !strings.Contains(logs, "sso sign-in refused: the identity provider returned an invalid user id") ||
		!strings.Contains(logs, "callback denied an authenticated caller") {
		t.Fatalf("the refusal wrote no Warn lines; the check below proves nothing:\n%s", logs)
	}
	for name, text := range map[string]string{"answer": fmt.Sprint(body), "audit": audits[0], "logs": logs} {
		lowered := strings.ToLower(text)
		for _, marker := range []string{leakSubject, leakName, leakMailTag} {
			if strings.Contains(lowered, strings.ToLower(marker)) {
				t.Errorf("%s holds a value the provider sent (%s)", name, marker)
			}
		}
	}
}

func TestRefusedSubjectLeavesNoProviderValueInTheOIDCRecords(t *testing.T) {
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
	client := newCookieClient(t)
	status, authResp := postJSONWithClient(t, client, st.server, "/api/v1/auth/oidc/"+providerID.String()+"/authorize", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("authorize: %d %v", status, authResp)
	}
	parsed, err := url.Parse(authResp["authorization_url"].(string))
	if err != nil {
		t.Fatal(err)
	}
	idp.set(parsed.Query().Get("nonce"), leakMailTag+"@allowed.example", leakName, leakSubject+"\x01tail")
	status, body := postJSONWithClient(t, client, st.server, "/api/v1/auth/oidc/"+providerID.String()+"/callback",
		map[string]any{"code": "c", "state": authResp["state"]})
	assertNothingTheProviderSentIsRecorded(t, ctx, st, providerID, status, body)
}

func TestRefusedSubjectLeavesNoProviderValueInTheSAMLRecords(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	idp := newSAMLIdP(t)
	providerID := seedSAMLOrgProviderAuto(t, ctx, st, idp, nameIDSP, true)
	response := idp.signedSAMLResponse(t, samlResponseOpts{
		spEntityID: nameIDSP, acsURL: samlACSOverrideFor(providerID), email: leakMailTag + "@allowed.example", fullName: leakName,
		nameID: leakSubject + "\t",
	})
	status, body := postJSON(t, st.server, "/api/v1/auth/saml/"+providerID.String()+"/acs", map[string]any{"SAMLResponse": response})
	assertNothingTheProviderSentIsRecorded(t, ctx, st, providerID, status, body)
}

func TestRefusedSubjectLeavesNoProviderValueInTheOAuthRecords(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	st, _, providerID, _ := oauthStack(t, ctx, oauthProfileWith(leakSubject+`\u0001x`, leakMailTag+"@example.test", leakName, ""))
	status, body := oauthCallbackOnce(t, st, providerID)
	assertNothingTheProviderSentIsRecorded(t, ctx, st, providerID, status, body)
}
