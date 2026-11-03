package oidc

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/social"
)

const emailMarker = "leakmarker"

// invalidProviderEmailDetail is go-api's 400 detail for an unusable provider address (internal/api/sso).
const invalidProviderEmailDetail = `{"message":"The identity provider returned an unusable email address","reason":"invalid_provider_email"}`

// providerEmailForms are the addresses the shared provider email rule refuses, as encoding/json hands them over.
func providerEmailForms() map[string]string {
	return map[string]string{
		"nul":              emailMarker + "\x00@example.test",
		"control":          emailMarker + "\x01@example.test",
		"del":              emailMarker + "\x7f@example.test",
		"c1":               emailMarker + "\u0085x@example.test",
		"replacement char": emailMarker + "�@example.test",
		"invalid utf8":     emailMarker + "\xff@example.test",
	}
}

// orderIdentities finds principal for every key when linked, and counts each identity store call.
type orderIdentities struct {
	linked bool
	calls  int
}

func (o *orderIdentities) IdentityBySubject(context.Context, ExternalKey, LegacyMatch) (Principal, error) {
	o.calls++
	if o.linked {
		return Principal{ID: uuid.New(), Email: "stored@example.test", Active: true}, nil
	}
	return Principal{}, ErrIdentityNotFound
}

func (o *orderIdentities) EmailInUse(context.Context, string) (bool, error) {
	o.calls++
	return false, nil
}

func (o *orderIdentities) ProvisionFederated(context.Context, FederatedAccount) (Principal, error) {
	o.calls++
	return Principal{ID: uuid.New(), Active: true}, nil
}

type refusal struct {
	status int
	reason Reason
	detail string
}

func refusalOf(t *testing.T, failure *Error) refusal {
	t.Helper()
	if failure == nil {
		return refusal{}
	}
	raw, err := json.Marshal(map[string]any{"detail": failure.Detail})
	if err != nil {
		t.Fatal(err)
	}
	return refusal{status: failure.Status, reason: failure.Reason, detail: string(raw)}
}

func wantInvalidProviderEmail(t *testing.T, linked, unknown refusal) {
	t.Helper()
	if linked != unknown {
		t.Fatalf("linked %+v, unknown %+v: the answers differ", linked, unknown)
	}
	want := refusal{status: http.StatusBadRequest, reason: ReasonMalformed, detail: `{"detail":` + invalidProviderEmailDetail + `}`}
	if linked != want {
		t.Fatalf("answer %+v, want %+v", linked, want)
	}
}

// The provider email rule runs before the identity lookup of an OIDC or OAuth sign-in: a linked identity with a
// refused address gets the answer an unknown one gets, and no identity store call runs.
func TestResolveRefusesAnUnusableEmailBeforeTheLookup(t *testing.T) {
	for name, email := range providerEmailForms() {
		for _, protocol := range []string{"oidc", "oauth"} {
			t.Run(protocol+"/"+name, func(t *testing.T) {
				run := func(linked bool) (refusal, *orderIdentities) {
					ids := &orderIdentities{linked: linked}
					svc := &Service{cfg: Config{Identities: ids, Memberships: resolveMembers{found: true, role: "member"},
						Sessions: &resolveSessions{}, MembershipJIT: &resolveJIT{}}}
					row := Provider{ID: uuid.New(), OrgID: uuid.New(), Protocol: protocol, AutoProvision: true}
					_, failure := svc.resolve(context.Background(), row, validated{profile: profile{Email: email, EmailVerified: true}}, nil)
					return refusalOf(t, failure), ids
				}
				linked, linkedIDs := run(true)
				unknown, unknownIDs := run(false)
				wantInvalidProviderEmail(t, linked, unknown)
				if linkedIDs.calls+unknownIDs.calls != 0 {
					t.Fatalf("%d identity store calls ran before the refusal", linkedIDs.calls+unknownIDs.calls)
				}
				if strings.Contains(linked.detail, emailMarker) {
					t.Fatal("the email value is in the answer")
				}
			})
		}
	}
}

// fakeGitLab is a self-hosted GitLab: the token endpoint, the token info and the user, for one account.
func fakeGitLab(t *testing.T, email string, confirmed bool) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	reply := func(w http.ResponseWriter, body any) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(body)
	}
	mux.HandleFunc("POST /oauth/token", func(w http.ResponseWriter, _ *http.Request) {
		reply(w, map[string]any{"access_token": "at-1", "token_type": "bearer"})
	})
	mux.HandleFunc("GET /oauth/token/info", func(w http.ResponseWriter, _ *http.Request) {
		reply(w, map[string]any{"resource_owner_id": 7, "application": map[string]any{"uid": "client-1"}})
	})
	mux.HandleFunc("GET /api/v4/user", func(w http.ResponseWriter, _ *http.Request) {
		var confirmedAt any
		if confirmed {
			confirmedAt = "2026-01-01T00:00:00Z"
		}
		reply(w, map[string]any{"id": 7, "username": "u", "name": "U", "email": email, "confirmed_at": confirmedAt})
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server
}

func oauthCallbackRun(t *testing.T, email string, confirmed, linked bool) (refusal, *orderIdentities) {
	t.Helper()
	return oauthCallbackRunOn(t, fakeGitLab(t, email, confirmed), linked)
}

// oauthCallbackRunOn runs one OAuth callback against server as a self-hosted GitLab.
func oauthCallbackRunOn(t *testing.T, server *httptest.Server, linked bool) (refusal, *orderIdentities) {
	t.Helper()
	return oauthCallbackRunOnWithList(t, server, linked, nil)
}

// oauthCallbackRunWithList is oauthCallbackRun for a provider row whose allowed_domains is the JSON text list (nil: none).
func oauthCallbackRunWithList(t *testing.T, email string, confirmed, linked bool, list *string) (refusal, *orderIdentities) {
	t.Helper()
	return oauthCallbackRunOnWithList(t, fakeGitLab(t, email, confirmed), linked, list)
}

// oauthCallbackRunOnWithList is oauthCallbackRunOn for a provider row whose allowed_domains is the JSON text list.
func oauthCallbackRunOnWithList(t *testing.T, server *httptest.Server, linked bool, list *string) (refusal, *orderIdentities) {
	t.Helper()
	got, ids, _ := oauthCallbackRunAudited(t, server, linked, list, nil)
	return got, ids
}

// oauthCallbackRunAudited is oauthCallbackRunOnWithList with the service's DenialAudit (nil: none); it also returns
// the provider row the callback admitted.
func oauthCallbackRunAudited(t *testing.T, server *httptest.Server, linked bool, list *string, denials DenialAudit) (refusal, *orderIdentities, StoredProvider) {
	t.Helper()
	var secret contracts.SecretString
	raw, _ := json.Marshal(uuid.NewString())
	if err := json.Unmarshal(raw, &secret); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_800_000_000, 0)
	ids := &orderIdentities{linked: linked}
	svc, err := New(Config{
		Providers: &fakeProviders{}, Flows: &scriptedFlows{}, Identities: ids,
		Memberships: resolveMembers{found: true, role: "member"}, Entitlements: &countingEntitlements{allowed: true},
		Sessions: &resolveSessions{}, MembershipJIT: &resolveJIT{}, StateSecret: secret, AppBaseURL: "https://app.example",
		HTTPClient: server.Client(), OAuthEndpoints: &OAuthEndpoints{Social: social.Endpoints{GitLabBase: server.URL}},
		DenialAudit: denials, SignInRecord: noSignInRecord{}, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	row := StoredProvider{ID: uuid.New(), OrgID: uuid.New(), Protocol: "oauth_gitlab", Status: "active", AutoProvision: true,
		Config: `{"client_id":"client-1"}`, AllowedDomains: list}
	secrets := `{"client_secret":"gitlab-test-only"}`
	row.EncryptedSecrets = &secrets
	token, state, err := sealOAuthState(secret.Reveal(), oauthFlowState{ProviderID: row.ID.String(), OrgID: row.OrgID.String(),
		RedirectURI: "https://app.example/cb", NonceHash: hashLoginNonce("cookie-1")}, now)
	if err != nil {
		t.Fatal(err)
	}
	cookieName := oauthLoginNonceCookieName(state.ID)
	_, _, failure := svc.callbackOAuth(context.Background(), row, CallbackInput{Code: "c", State: token,
		Cookie: func(name string) (string, bool) { return "cookie-1", name == cookieName }})
	return refusalOf(t, failure), ids, row
}

// An OAuth callback refuses an unusable provider address before the verified-email check and before the lookup:
// verified or not, linked or not, the answer is go-api's 400 and no identity store call runs.
func TestOAuthCallbackRefusesAnUnusableEmailFirst(t *testing.T) {
	for name, email := range providerEmailForms() {
		for _, confirmed := range []bool{true, false} {
			label := name
			if !confirmed {
				label += "/unverified"
			}
			t.Run(label, func(t *testing.T) {
				linked, linkedIDs := oauthCallbackRun(t, email, confirmed, true)
				unknown, unknownIDs := oauthCallbackRun(t, email, confirmed, false)
				wantInvalidProviderEmail(t, linked, unknown)
				if linkedIDs.calls+unknownIDs.calls != 0 {
					t.Fatalf("%d identity store calls ran before the refusal", linkedIDs.calls+unknownIDs.calls)
				}
			})
		}
	}
}

// A usable address still reaches the lookup (control for the harness).
func TestOAuthCallbackWithAUsableEmailReachesTheLookup(t *testing.T) {
	_, ids := oauthCallbackRun(t, "a@example.test", true, true)
	if ids.calls == 0 {
		t.Fatal("no identity store call ran")
	}
}
