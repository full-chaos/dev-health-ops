package oidc

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/social"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/ssoaudit"
)

type recordedFailures struct {
	rows []ssoaudit.Row
	err  error
}

func (r *recordedFailures) Failed(_ context.Context, row ssoaudit.Row) error {
	r.rows = append(r.rows, row)
	return r.err
}

// providerCase is one OAuth callback against a GitLab whose token or profile endpoint can fail.
type providerCase struct {
	tokenStatus int // 0 = 200
	userStatus  int // 0 = 200
	noToken     bool
	noID        bool
	tokenOwner  string // "" = the client
	infoStatus  int    // the token info endpoint (no go-api twin); 0 = 200
	userBody    string // a raw profile body; "" = the JSON profile
}

func runProviderCase(t *testing.T, c providerCase, failures ssoaudit.ProviderFailure, denials DenialAudit) (refusal, StoredProvider, *Error) {
	t.Helper()
	mux := http.NewServeMux()
	reply := func(w http.ResponseWriter, status int, body any) {
		w.Header().Set("Content-Type", "application/json")
		if status == 0 {
			status = http.StatusOK
		}
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(body)
	}
	mux.HandleFunc("POST /oauth/token", func(w http.ResponseWriter, _ *http.Request) {
		if c.noToken {
			reply(w, 0, map[string]any{"token_type": "bearer"})
			return
		}
		reply(w, c.tokenStatus, map[string]any{"access_token": "at-1", "token_type": "bearer"})
	})
	owner := c.tokenOwner
	if owner == "" {
		owner = "client-1"
	}
	mux.HandleFunc("GET /oauth/token/info", func(w http.ResponseWriter, _ *http.Request) {
		reply(w, c.infoStatus, map[string]any{"resource_owner_id": 7, "application": map[string]any{"uid": owner}})
	})
	mux.HandleFunc("GET /api/v4/user", func(w http.ResponseWriter, _ *http.Request) {
		user := map[string]any{"id": 7, "username": "u", "name": "U", "email": "u@example.test", "confirmed_at": "2026-01-01T00:00:00Z"}
		if c.noID {
			delete(user, "id")
		}
		if c.userBody != "" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(c.userBody))
			return
		}
		reply(w, c.userStatus, user)
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	var secret contracts.SecretString
	raw, _ := json.Marshal(uuid.NewString())
	if err := json.Unmarshal(raw, &secret); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_800_000_000, 0)
	svc, err := New(Config{
		Providers: &fakeProviders{}, Flows: &scriptedFlows{}, Identities: &orderIdentities{linked: true},
		Memberships: resolveMembers{found: true, role: "member"}, Entitlements: &countingEntitlements{allowed: true},
		Sessions: &resolveSessions{}, StateSecret: secret, AppBaseURL: "https://app.example",
		HTTPClient: server.Client(), OAuthEndpoints: &OAuthEndpoints{Social: social.Endpoints{GitLabBase: server.URL}},
		DenialAudit: denials, ProviderFailure: failures, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	row := StoredProvider{ID: uuid.New(), OrgID: uuid.New(), Protocol: "oauth_gitlab", Status: "active", AutoProvision: true,
		Config: `{"client_id":"client-1"}`}
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
	return refusalOf(t, failure), row, failure
}

// A provider error of the OAuth callback that go-api records (recordSSOFailure, protocol oauth, no stage: a failed
// token exchange, and a profile request that fails in transport or with a non-2xx status) records the failure once
// before it answers, with the fixed text of its reason, naming the admitted provider and organization; the answer is
// unchanged.
func TestOAuthProviderErrorsRecordTheProviderFailure(t *testing.T) {
	for name, tc := range map[string]struct {
		c       providerCase
		message string
	}{
		"token exchange refused": {providerCase{tokenStatus: http.StatusBadRequest}, "OAuth token exchange failed"},
		"token exchange 5xx":     {providerCase{tokenStatus: http.StatusBadGateway}, "OAuth token exchange failed"},
		"no access token":        {providerCase{noToken: true}, "OAuth token exchange failed"},
		"profile fetch 5xx":      {providerCase{userStatus: http.StatusBadGateway}, "OAuth userinfo request failed"},
		"profile fetch refused":  {providerCase{userStatus: http.StatusUnauthorized}, "OAuth userinfo request failed"},
	} {
		t.Run(name, func(t *testing.T) {
			without, _, _ := runProviderCase(t, tc.c, nil, nil)
			failures := &recordedFailures{}
			denials := &recordedDenials{}
			got, row, _ := runProviderCase(t, tc.c, failures, denials)
			if got != without || got.status == 0 {
				t.Fatalf("answer %+v, without the record %+v", got, without)
			}
			want := ssoaudit.Row{OrgID: row.OrgID, ProviderID: row.ID, Protocol: "oauth", ErrorMessage: tc.message}
			if len(failures.rows) != 1 || failures.rows[0] != want || len(denials.rows) != 0 {
				t.Fatalf("records %+v denials %+v, want exactly %+v", failures.rows, denials.rows, want)
			}
		})
	}
}

// Only where go-api records: the profile without an id is go-api's provider_profile denial row; a token of another
// client and a failed token info request are checks go-api does not make; a profile body that is not JSON is go-api's
// bare 500 (oauthprovider ErrUnexpected), recorded nowhere; a sign-in that succeeds records nothing.
func TestOAuthOnlyProviderErrorsRecordTheProviderFailure(t *testing.T) {
	for name, c := range map[string]providerCase{
		"profile without an id":   {noID: true},
		"token of another client": {tokenOwner: "client-2"},
		"token info 5xx":          {infoStatus: http.StatusBadGateway},
		"token info refused":      {infoStatus: http.StatusUnauthorized},
		"profile body not JSON":   {userBody: "<html>"},
		"success":                 {},
	} {
		failures := &recordedFailures{}
		runProviderCase(t, c, failures, &recordedDenials{})
		if len(failures.rows) != 0 {
			t.Errorf("%s: records %+v, want none", name, failures.rows)
		}
	}
}

// A record that cannot be written is go-api's 500, never the provider error's answer.
func TestOAuthProviderFailureRecordWriteErrorIsThe500(t *testing.T) {
	for name, c := range map[string]providerCase{
		"token exchange": {tokenStatus: http.StatusBadRequest},
		"profile fetch":  {userStatus: http.StatusBadGateway},
	} {
		_, _, failure := runProviderCase(t, c, &recordedFailures{err: errors.New("permission denied for table sso_providers")}, nil)
		if failure == nil || failure.Status != http.StatusInternalServerError || failure.Reason != ReasonStoreUnavailable {
			t.Errorf("%s: %+v, want the 500 store_unavailable", name, failure)
		}
	}
}
