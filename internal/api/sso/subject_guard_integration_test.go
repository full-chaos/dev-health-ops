//go:build integration

package sso_test

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/google/uuid"
)

// The wire forms of an unusable subject: the fake identity providers marshal JSON and XML, which turn invalid bytes
// into U+FFFD, so the invalid-UTF-8 case is covered at the unit level and these are the forms that survive transport.
func wireUnusableSubjects() map[string]string {
	return map[string]string{
		"empty":        "",
		"nul":          "ab\x00cd",
		"control":      "ab\x01cd",
		"replacement":  "ab�cd",
		"edge space":   " abcd",
		"trailing tab": "abcd\t",
		"spaces only":  "   ",
		"tab only":     "\t",
	}
}

// assertSubjectRefused: the callback answers 400 invalid_provider_id with no token, no row is written, and the
// provider row is not marked failed.
func assertSubjectRefused(t *testing.T, ctx context.Context, st stack, providerID uuid.UUID, before rowCounts, status int, body map[string]any) {
	t.Helper()
	detail, _ := body["detail"].(map[string]any)
	if status != http.StatusBadRequest || detail["reason"] != "invalid_provider_id" || body["access_token"] != nil {
		t.Fatalf("status=%d body=%v, want 400 invalid_provider_id and no token", status, body)
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

func oidcSubjectCallback(t *testing.T, st stack, idp *fakeIdP, providerID uuid.UUID, email, sub string) (int, map[string]any) {
	t.Helper()
	client := newCookieClient(t)
	status, authResp := postJSONWithClient(t, client, st.server, "/api/v1/auth/oidc/"+providerID.String()+"/authorize", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("authorize: %d %v", status, authResp)
	}
	parsed, err := url.Parse(authResp["authorization_url"].(string))
	if err != nil {
		t.Fatal(err)
	}
	idp.set(parsed.Query().Get("nonce"), email, "Person", sub)
	return postJSONWithClient(t, client, st.server, "/api/v1/auth/oidc/"+providerID.String()+"/callback",
		map[string]any{"code": "c", "state": authResp["state"]})
}

func TestSubjectGuardOIDCCallbackRefusesAnUnusableSubject(t *testing.T) {
	for name, sub := range wireUnusableSubjects() {
		for _, held := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/held=%v", name, held), func(t *testing.T) {
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
				const email = "subject.guard@allowed.example"
				if held {
					seedAccount(t, ctx, st.pool, email, "oidc", "held-subject")
				}
				before := snapshotRows(t, ctx, st)
				status, body := oidcSubjectCallback(t, st, idp, providerID, email, sub)
				assertSubjectRefused(t, ctx, st, providerID, before, status, body)
			})
		}
	}
}

func TestSubjectGuardSAMLCallbackRefusesAnUnusableSubject(t *testing.T) {
	for name, nameID := range wireUnusableSubjects() {
		for _, held := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/held=%v", name, held), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
				defer cancel()
				st := startStack(t, ctx)
				idp := newSAMLIdP(t)
				providerID := seedSAMLOrgProviderAuto(t, ctx, st, idp, nameIDSP, true)
				const email = "subject.guard@allowed.example"
				if held {
					seedAccount(t, ctx, st.pool, email, "saml", "held-subject")
				}
				before := snapshotRows(t, ctx, st)
				response := idp.signedSAMLResponse(t, samlResponseOpts{
					spEntityID: nameIDSP, acsURL: samlACSOverrideFor(providerID), email: email, fullName: "Person", nameID: nameID,
				})
				status, body := postJSON(t, st.server, "/api/v1/auth/saml/"+providerID.String()+"/acs", map[string]any{"SAMLResponse": response})
				assertSubjectRefused(t, ctx, st, providerID, before, status, body)
			})
		}
	}
}

func TestSubjectGuardOAuthCallbackRefusesAnUnusableSubject(t *testing.T) {
	for name, id := range map[string]string{"empty": "", "nul": `7\u0000x`, "control": `7\u0001x`, "edge space": ` 7`} {
		for _, held := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/held=%v", name, held), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
				defer cancel()
				const email = "subject.guard@example.test"
				st, _, providerID, _ := oauthStack(t, ctx, oauthProfileWith(id, email, "Fay", ""))
				if held {
					seedAccount(t, ctx, st.pool, email, "gitlab", "42")
				}
				before := snapshotRows(t, ctx, st)
				status, body := oauthCallbackOnce(t, st, providerID)
				assertSubjectRefused(t, ctx, st, providerID, before, status, body)
			})
		}
	}
}

// A sub the provider leaves out or sets to null reaches the claim reader as "" and the edge rule refuses it like an
// empty one. A sub that is a number or a bool never gets that far: the id_token check refuses the token. Either way the
// callback answers 400 with no session and writes no account row. "sub" is read with a blank ok, so the first refusal
// is the edge rule's, not the claim reader's.
func TestSubjectGuardOIDCCallbackRefusesASubThatIsNotAString(t *testing.T) {
	shapes := map[string]struct {
		override func(claims map[string]any)
		edgeRule bool
	}{
		"absent":  {func(claims map[string]any) { delete(claims, "sub") }, true},
		"null":    {func(claims map[string]any) { claims["sub"] = nil }, true},
		"number":  {func(claims map[string]any) { claims["sub"] = 12 }, false},
		"zero":    {func(claims map[string]any) { claims["sub"] = 0 }, false},
		"boolean": {func(claims map[string]any) { claims["sub"] = true }, false},
	}
	for name, shape := range shapes {
		for _, held := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/held=%v", name, held), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
				defer cancel()
				st := startStack(t, ctx)
				idp := startFakeIdP(t)
				idp.SetOverride(shape.override)
				orgID := seedOrg(t, ctx, st.pool, "enterprise")
				config := fmt.Sprintf(`{"client_id":"test-client","issuer":%q,"token_endpoint":%q,"jwks_uri":%q,"userinfo_endpoint":%q,"scopes":["openid","email"]}`,
					idp.server.URL, idp.server.URL+"/token", idp.server.URL+"/jwks", idp.server.URL+"/userinfo")
				providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{
					protocol: "oidc", status: "active", config: config, autoProvision: true, clientSecretEncryptedFallback: "s",
				})
				const email = "subject.guard@allowed.example"
				if held {
					seedAccount(t, ctx, st.pool, email, "oidc", "held-subject")
				}
				before := snapshotRows(t, ctx, st)
				status, body := oidcSubjectCallback(t, st, idp, providerID, email, "unused")
				if shape.edgeRule {
					assertSubjectRefused(t, ctx, st, providerID, before, status, body)
					return
				}
				if status != http.StatusBadRequest || body["access_token"] != nil {
					t.Fatalf("status=%d body=%v, want 400 and no token", status, body)
				}
				assertNoRowsWritten(t, ctx, st, before)
			})
		}
	}
}
