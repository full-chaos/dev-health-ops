package oidc

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/oauthprovider"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/social"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// stageCase is one OAuth callback: the provider's answer, the account stores, and how the state and cookie come back.
type stageCase struct {
	email         string
	confirmed     bool
	noID          bool
	badSubject    bool
	identities    IdentityStore
	autoProvision bool
	stateOrg      *uuid.UUID
	late          time.Duration
	cookie        string
	consumed      bool
}

type consumedFlows struct{ scriptedFlows }

func (*consumedFlows) Consume(context.Context, [32]byte, string, uuid.UUID, time.Time) (bool, error) {
	return false, nil
}

// fakeGitLabUser serves a GitLab account (user) and a Google account (googleSub, verified email) for one client.
func fakeGitLabUser(t *testing.T, user map[string]any, googleSub string) *httptest.Server {
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
	mux.HandleFunc("GET /api/v4/user", func(w http.ResponseWriter, _ *http.Request) { reply(w, user) })
	mux.HandleFunc("POST /google/token", func(w http.ResponseWriter, _ *http.Request) {
		reply(w, map[string]any{"access_token": "at-g", "token_type": "bearer"})
	})
	mux.HandleFunc("POST /google/tokeninfo", func(w http.ResponseWriter, _ *http.Request) {
		reply(w, map[string]any{"issued_to": "client-1", "audience": "client-1", "user_id": googleSub})
	})
	mux.HandleFunc("GET /google/userinfo", func(w http.ResponseWriter, _ *http.Request) {
		reply(w, map[string]any{"id": googleSub, "email": user["email"], "verified_email": true, "name": "G"})
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server
}

func runStage(t *testing.T, c stageCase, denials DenialAudit) (refusal, StoredProvider, *Error) {
	t.Helper()
	user := map[string]any{"id": 7, "username": "u", "name": "U", "email": c.email}
	if c.confirmed {
		user["confirmed_at"] = "2026-01-01T00:00:00Z"
	}
	if c.noID {
		delete(user, "id")
	}
	server := fakeGitLabUser(t, user, "g-1\x01")
	var secret contracts.SecretString
	raw, _ := json.Marshal(uuid.NewString())
	if err := json.Unmarshal(raw, &secret); err != nil {
		t.Fatal(err)
	}
	sealed := time.Unix(1_800_000_000, 0)
	now := sealed.Add(c.late)
	ids := c.identities
	if ids == nil {
		ids = &orderIdentities{linked: true}
	}
	var flows FlowStore = &scriptedFlows{}
	if c.consumed {
		flows = &consumedFlows{}
	}
	svc, err := New(Config{
		Providers: &fakeProviders{}, Flows: flows, Identities: ids,
		Memberships: resolveMembers{found: true, role: "member"}, Entitlements: &countingEntitlements{allowed: true},
		Sessions: &resolveSessions{}, StateSecret: secret, AppBaseURL: "https://app.example",
		HTTPClient: server.Client(), OAuthEndpoints: &OAuthEndpoints{GoogleToken: server.URL + "/google/token",
			Social: social.Endpoints{GitLabBase: server.URL, GoogleTokenInfo: server.URL + "/google/tokeninfo", GoogleUserInfo: server.URL + "/google/userinfo"}},
		DenialAudit: denials, SignInRecord: noSignInRecord{}, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	protocol := "oauth_gitlab"
	if c.badSubject {
		protocol = "oauth_google"
	}
	row := StoredProvider{ID: uuid.New(), OrgID: uuid.New(), Protocol: protocol, Status: "active", AutoProvision: c.autoProvision,
		Config: `{"client_id":"client-1"}`}
	secrets := `{"client_secret":"gitlab-test-only"}`
	row.EncryptedSecrets = &secrets
	org := row.OrgID
	if c.stateOrg != nil {
		org = *c.stateOrg
	}
	token, state, err := sealOAuthState(secret.Reveal(), oauthFlowState{ProviderID: row.ID.String(), OrgID: org.String(),
		RedirectURI: "https://app.example/cb", NonceHash: hashLoginNonce("cookie-1")}, sealed)
	if err != nil {
		t.Fatal(err)
	}
	cookie := c.cookie
	if cookie == "" {
		cookie = "cookie-1"
	}
	cookieName := oauthLoginNonceCookieName(state.ID)
	_, _, failure := svc.callbackOAuth(context.Background(), row, CallbackInput{Code: "c", State: token,
		Cookie: func(name string) (string, bool) { return cookie, name == cookieName }})
	return refusalOf(t, failure), row, failure
}

// Each OAuth refusal go-api audits writes exactly go-api's row before it answers: the stage and the message of
// go-api's own error, naming the admitted provider and organization, and the answer itself is unchanged.
func TestOAuthRefusalsWriteGoAPIsStageRow(t *testing.T) {
	other := uuid.New()
	for name, tc := range map[string]struct {
		c       stageCase
		stage   string
		message string
		reason  Reason
	}{
		"state expired":                 {stageCase{email: "u@example.test", confirmed: true, late: 11 * time.Minute}, "state_auth", "OAuth state expired", ReasonExpired},
		"state of another org":          {stageCase{email: "u@example.test", confirmed: true, stateOrg: &other}, "state_auth", "OAuth state mismatch", ReasonMalformed},
		"login cookie wrong":            {stageCase{email: "u@example.test", confirmed: true, cookie: "other"}, "state_auth", "OAuth state mismatch", ReasonMalformed},
		"profile without an id":         {stageCase{email: "u@example.test", confirmed: true, noID: true}, "provider_profile", msgMissingProviderID, ReasonInvalidCredentials},
		"unusable email":                {stageCase{email: "u\x00@example.test", confirmed: true}, "provisioning", msgInvalidProviderEmail, ReasonMalformed},
		"unverified email":              {stageCase{email: "u@example.test", confirmed: false}, "email_verification", msgNoVerifiedEmail, ReasonEmailUnverified},
		"subject not a key":             {stageCase{email: "u@example.test", confirmed: true, badSubject: true}, "provisioning", msgInvalidProviderID, ReasonMissingSub},
		"auto-provisioning off":         {stageCase{email: "u@example.test", confirmed: true, identities: &resolveIdentities{err: ErrIdentityNotFound}}, "provisioning", "user not found and auto-provisioning is disabled", ReasonProvisioningDisabled},
		"identity conflict":             {stageCase{email: "u@example.test", confirmed: true, autoProvision: true, identities: &resolveIdentities{err: ErrIdentityAmbiguous}}, "provisioning", "identity conflict", ReasonIdentityConflict},
		"not an org member":             {stageCase{email: "u@example.test", confirmed: true, autoProvision: true, identities: &resolveIdentities{err: ErrNotOrgMember}}, "provisioning", "membership required", ReasonMembershipRequired},
		"new account, no JIT set":       {stageCase{email: "u@example.test", confirmed: true, autoProvision: true, identities: &resolveIdentities{err: ErrIdentityNotFound}}, "provisioning", "membership required", ReasonMembershipRequired},
		"email held by another account": {stageCase{email: "u@example.test", confirmed: true, autoProvision: true, identities: &resolveIdentities{err: ErrIdentityNotFound, inUse: true}}, "provisioning", "identity conflict", ReasonEmailCollision},
	} {
		t.Run(name, func(t *testing.T) {
			without, _, _ := runStage(t, tc.c, nil)
			denials := &recordedDenials{}
			got, row, _ := runStage(t, tc.c, denials)
			if got != without || got.reason != tc.reason {
				t.Fatalf("answer %+v, without the audit %+v, want reason %s", got, without, tc.reason)
			}
			want := DenialRow{OrgID: row.OrgID, ProviderID: row.ID, Protocol: "oauth", Stage: tc.stage,
				ErrorMessage: pythonparity.SanitizeErrorTextHardened(tc.message, 4000)}
			if len(denials.rows) != 1 || denials.rows[0] != want {
				t.Fatalf("rows %+v, want exactly %+v", denials.rows, want)
			}
		})
	}
}

// Refusals go-api does not audit write no row: a replayed flow (go-api keeps no flow), a provider error (go-api's
// recordSSOFailure, out of this change) and a sign-in that succeeds.
func TestOAuthRefusalsGoAPIDoesNotAuditWriteNoRow(t *testing.T) {
	for name, c := range map[string]stageCase{
		"replayed flow": {email: "u@example.test", confirmed: true, consumed: true},
		"success":       {email: "u@example.test", confirmed: true},
	} {
		denials := &recordedDenials{}
		runStage(t, c, denials)
		if len(denials.rows) != 0 {
			t.Errorf("%s: rows %+v, want none", name, denials.rows)
		}
	}
}

// A row that cannot be written is go-api's 500 for every stage, never the refusal.
func TestOAuthStageRowWriteFailureIsThe500(t *testing.T) {
	for name, c := range map[string]stageCase{
		"state_auth":         {email: "u@example.test", confirmed: true, cookie: "other"},
		"provider_profile":   {email: "u@example.test", confirmed: true, noID: true},
		"email_verification": {email: "u@example.test", confirmed: false},
		"provisioning":       {email: "u@example.test", confirmed: true, identities: &resolveIdentities{err: ErrIdentityNotFound}},
	} {
		_, _, failure := runStage(t, c, &recordedDenials{err: errors.New("permission denied for table audit_logs")})
		if failure == nil || failure.Status != http.StatusInternalServerError || failure.Reason != ReasonStoreUnavailable {
			t.Errorf("%s: %+v, want the 500 store_unavailable", name, failure)
		}
	}
}

// The messages are go-api's own error texts, byte for byte: the exported ones by value, the unexported ones as the
// literal go-api declares (internal/api/sso).
func TestOAuthStageMessagesAreGoAPIsErrorTexts(t *testing.T) {
	if msgMissingProviderID != oauthprovider.ErrMissingProviderID.Error() {
		t.Errorf("missing provider id %q, go-api %q", msgMissingProviderID, oauthprovider.ErrMissingProviderID.Error())
	}
	if msgInvalidProviderID != identity.ErrInvalidProviderID.Error() {
		t.Errorf("invalid provider id %q, go-api %q", msgInvalidProviderID, identity.ErrInvalidProviderID.Error())
	}
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	var source strings.Builder
	for _, f := range []string{"oauth.go", "provision.go"} {
		raw, err := os.ReadFile(filepath.Join(root, "internal", "api", "sso", f))
		if err != nil {
			t.Fatal(err)
		}
		source.Write(raw)
	}
	declared := func(pattern string) bool { return regexp.MustCompile(pattern).MatchString(source.String()) }
	for name, pattern := range map[string]string{
		"invalid provider email": `var errInvalidProviderEmail = errors\.New\(` + regexp.QuoteMeta(`"`+msgInvalidProviderEmail+`"`) + `\)`,
		"no verified email":      `var errOAuthNoVerifiedEmail = errors\.New\(` + regexp.QuoteMeta(`"`+msgNoVerifiedEmail+`"`) + `\)`,
		"auto-provisioning off":  `var errOAuthAutoProvisionDisabled = errors\.New\(` + regexp.QuoteMeta(`"`+provisioningMessages[ReasonProvisioningDisabled]+`"`) + `\)`,
		"identity conflict":      `var errIdentityConflict = errors\.New\(` + regexp.QuoteMeta(`"`+provisioningMessages[ReasonIdentityConflict]+`"`) + `\)`,
		"membership required":    `var errMembershipRequired = errors\.New\(` + regexp.QuoteMeta(`"`+provisioningMessages[ReasonMembershipRequired]+`"`) + `\)`,
		"state expired":          `ssoAuthErr\(` + regexp.QuoteMeta(`"`+msgStateExpired+`"`) + `\)`,
		"state mismatch":         `ssoAuthErr\(` + regexp.QuoteMeta(`"`+msgStateMismatch+`"`) + `\)`,
	} {
		if !declared(pattern) {
			t.Errorf("%s: go-api declares no %s", name, pattern)
		}
	}
}
