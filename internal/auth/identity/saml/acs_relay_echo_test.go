package saml_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity/saml"
)

// memFlows keeps saved flows and consumes one only on an exact provider,
// request id and binding hash match.
type memFlows struct{ saved []saml.Flow }

func (m *memFlows) SaveFlow(_ context.Context, f saml.Flow) error {
	m.saved = append(m.saved, f)
	return nil
}

func (m *memFlows) ConsumeFlow(_ context.Context, providerID uuid.UUID, requestID string, binding [32]byte, now time.Time) error {
	for i, f := range m.saved {
		if f.ProviderID == providerID && f.RequestID == requestID && f.BindingHash == binding && now.Before(f.ExpiresAt) {
			m.saved = append(m.saved[:i], m.saved[i+1:]...)
			return nil
		}
	}
	return saml.ErrFlowNotFound
}

type echoRun struct {
	t        *testing.T
	idp      *testIdP
	p        saml.Provider
	flows    *memFlows
	sessions *sessionsSpy
	mux      *http.ServeMux
}

func newEchoRun(t *testing.T, allowIdP bool) *echoRun {
	t.Helper()
	idp := newTestIdP(t)
	p := saml.Provider{
		ID: uuid.New(), OrgID: uuid.New(), Protocol: "saml", Status: "active", AllowIdPInitiated: allowIdP,
		Config: saml.Config{EntityID: idp.entityID, SSOURL: "https://idp.example/sso", Certificate: encodeCert(idp)},
	}
	id := uuid.New()
	e := &echoRun{t: t, idp: idp, p: p, flows: &memFlows{}, sessions: &sessionsSpy{}, mux: http.NewServeMux()}
	svc := &saml.Service{
		Providers: staticProvider{p}, Entitlements: ssoAllowed{}, Flows: e.flows, Uses: &usesOnce{used: map[string]bool{}},
		Store: &lookupStore{hit: &saml.Principal{ID: id}}, Memberships: &memberSet{ids: map[uuid.UUID]bool{id: true}},
		BaseURL: func() string { return refusalBase }, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	h := saml.Handlers{Service: svc, Sessions: e.sessions, Logger: svc.Logger}
	e.mux.HandleFunc("POST /saml/{provider_id}/initiate", h.Initiate)
	e.mux.HandleFunc("POST /saml/{provider_id}/acs", h.ACS)
	return e
}

// initiate starts a sign-in and returns the request id, the issued RelayState
// ("" when none) and the binding cookie.
func (e *echoRun) initiate(body string) (string, string, *http.Cookie) {
	e.t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/saml/"+e.p.ID.String()+"/initiate", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	e.mux.ServeHTTP(rec, req)
	var out struct {
		RedirectURL string `json:"redirect_url"`
	}
	if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &out) != nil || len(rec.Result().Cookies()) != 1 {
		e.t.Fatalf("initiate: %d %s", rec.Code, rec.Body)
	}
	u, err := url.Parse(out.RedirectURL)
	if err != nil {
		e.t.Fatal(err)
	}
	return requestIDFromRedirect(e.t, out.RedirectURL), u.Query().Get("RelayState"), rec.Result().Cookies()[0]
}

// acs posts a response to requestID signed by signer, with relay as the
// RelayState echo when echo is true.
func (e *echoRun) acs(signer *testIdP, requestID string, cookie *http.Cookie, relay string, echo bool) *httptest.ResponseRecorder {
	e.t.Helper()
	acsURL := e.p.ACSURL(refusalBase)
	form := url.Values{"SAMLResponse": {b64(signer.response(e.t, resp{
		nameID: saml.NameID{Value: "user-" + uuid.NewString()}, email: "member@example.com", inResponseTo: requestID,
		destination: strp(acsURL), recipient: strp(acsURL), audiences: [][]string{{e.p.SPEntityID(refusalBase)}},
	}))}}
	if echo {
		form.Set("RelayState", relay)
	}
	req := httptest.NewRequest(http.MethodPost, "/saml/"+e.p.ID.String()+"/acs", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(cookie)
	rec := httptest.NewRecorder()
	e.mux.ServeHTTP(rec, req)
	return rec
}

const authFailed = `{"detail":"SAML authentication failed"}`

func assertAuthFailed(t *testing.T, name string, rec *httptest.ResponseRecorder) {
	t.Helper()
	if rec.Code != http.StatusBadRequest || strings.TrimSpace(rec.Body.String()) != authFailed {
		t.Errorf("%s: %d %s, want 400 %s", name, rec.Code, rec.Body, authFailed)
	}
}

// go-api (internal/api/sso/saml.go processSAMLResponse) refuses a missing or
// unequal RelayState on a provider without IdP-initiated sign-in with 400
// "SAML authentication failed"; the issued one signs in.
func TestACSChecksTheRelayStateEchoWhenIdPInitiatedSignInIsOff(t *testing.T) {
	for name, tc := range map[string]struct {
		relay func(issued string) string
		echo  bool
	}{
		"missing echo":    {func(string) string { return "" }, false},
		"empty echo":      {func(string) string { return "" }, true},
		"mismatched echo": {func(string) string { return "mismatched-relay-state" }, true},
		"changed echo":    {func(issued string) string { return issued + "x" }, true},
	} {
		e := newEchoRun(t, false)
		requestID, issued, cookie := e.initiate(`{"relay_state":"/after"}`)
		assertAuthFailed(t, name, e.acs(e.idp, requestID, cookie, tc.relay(issued), tc.echo))
		if len(e.sessions.got) != 0 {
			t.Errorf("%s: a session was issued", name)
		}
		if rec := e.acs(e.idp, requestID, cookie, issued, true); rec.Code != http.StatusOK || len(e.sessions.got) != 1 {
			t.Errorf("%s, then the issued echo: %d %s sessions %d", name, rec.Code, rec.Body, len(e.sessions.got))
		}
	}
}

// A provider that allows IdP-initiated sign-in does not read the echo, and its
// flow binds the browser nonce alone, as before.
func TestACSIgnoresTheRelayStateEchoWhenIdPInitiatedSignInIsOn(t *testing.T) {
	for name, tc := range map[string]struct {
		relay string
		echo  bool
	}{
		"missing echo":   {"", false},
		"forwarded echo": {"/after", true},
		"other echo":     {"other", true},
	} {
		e := newEchoRun(t, true)
		requestID, issued, cookie := e.initiate(`{"relay_state":"/after"}`)
		if issued != "/after" {
			t.Fatalf("%s: RelayState %q, want the forwarded /after", name, issued)
		}
		if rec := e.acs(e.idp, requestID, cookie, tc.relay, tc.echo); rec.Code != http.StatusOK || len(e.sessions.got) != 1 {
			t.Errorf("%s: %d %s sessions %d", name, rec.Code, rec.Body, len(e.sessions.got))
		}
	}
}

// The stored binding bytes: sha256(nonce) on a provider with IdP-initiated
// sign-in (unchanged), sha256(nonce, 0x00, issued RelayState) without it.
func TestFlowBindingBytes(t *testing.T) {
	for _, allowIdP := range []bool{true, false} {
		e := newEchoRun(t, allowIdP)
		_, issued, cookie := e.initiate(`{"relay_state":"/after"}`)
		want := sha256.Sum256([]byte(cookie.Value))
		if !allowIdP {
			want = sha256.Sum256([]byte(cookie.Value + "\x00" + issued))
		}
		if len(e.flows.saved) != 1 || e.flows.saved[0].BindingHash != want {
			t.Errorf("allow_idp_initiated=%v: stored binding %x, want %x", allowIdP, e.flows.saved, want)
		}
	}
}

// go-api checks the RelayState before the signature, auth-service after it: a
// response with a bad signature AND a bad RelayState gets the same route answer
// on both (go-api's cell: internal/api/sso TestSAMLACSBadSignatureAndBadRelayStateAnswer).
func TestACSBadSignatureAndBadRelayStateAnswer(t *testing.T) {
	other := newTestIdP(t)
	for name, tc := range map[string]struct {
		relay string
		echo  bool
	}{
		"bad signature, missing echo":    {"", false},
		"bad signature, mismatched echo": {"mismatched-relay-state", true},
	} {
		e := newEchoRun(t, false)
		requestID, _, cookie := e.initiate(`{}`)
		assertAuthFailed(t, name, e.acs(other, requestID, cookie, tc.relay, tc.echo))
		if len(e.sessions.got) != 0 {
			t.Errorf("%s: a session was issued", name)
		}
	}
}
