package saml_test

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity/saml"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/ssoaudit"
)

const emailMarker = "leakmarker"

// orderStore holds one principal for every identity key when linked, and counts each store call.
type orderStore struct {
	linked *saml.Principal
	calls  int
}

func (s *orderStore) IdentityBySubject(context.Context, saml.Identity, *saml.LegacyMatch) (saml.Principal, bool, error) {
	s.calls++
	if s.linked != nil {
		return *s.linked, true, nil
	}
	return saml.Principal{}, false, nil
}

func (s *orderStore) NormalizeEmail(raw string) (string, error) {
	s.calls++
	return strings.ToLower(raw), nil
}

func (s *orderStore) PrincipalByEmail(context.Context, string) (saml.EmailOwner, bool, error) {
	s.calls++
	return saml.EmailOwner{}, false, nil
}

type acsAnswer struct {
	status   int
	body     string
	logs     string
	calls    int
	sessions int
	provider saml.Provider
}

// postACSEmail posts one signed IdP-initiated response asserting email through the real ACS handler.
func postACSEmail(t *testing.T, email string, linked bool) acsAnswer {
	t.Helper()
	return postACSEmailWithList(t, email, linked, nil)
}

// postACSEmailWithList is postACSEmail for a provider with allowed_domains list.
func postACSEmailWithList(t *testing.T, email string, linked bool, list []string) acsAnswer {
	t.Helper()
	return postACSEmailRecorded(t, email, linked, list, nil)
}

// postACSEmailRecorded is postACSEmailWithList with the provider failure seam.
func postACSEmailRecorded(t *testing.T, email string, linked bool, list []string, failures ssoaudit.ProviderFailure) acsAnswer {
	t.Helper()
	return postACSCase(t, acsCase{email: email, linked: linked, list: list, failures: failures})
}

// acsCase is one post to the ACS handler: the assertion, the account, the provider row and the failure seam.
type acsCase struct {
	email    string
	linked   bool
	list     []string
	failures ssoaudit.ProviderFailure
	provider func(*saml.Provider)
	response func(*resp)
	raw      string
}

func postACSCase(t *testing.T, c acsCase) acsAnswer {
	t.Helper()
	email, linked, list, failures := c.email, c.linked, c.list, c.failures
	idp := newTestIdP(t)
	p := saml.Provider{
		ID: uuid.New(), OrgID: uuid.New(), Protocol: "saml", Status: "active", AllowIdPInitiated: true, AutoProvision: true,
		Config: saml.Config{EntityID: idp.entityID, Certificate: encodeCert(idp)}, AllowedDomains: list,
	}
	if c.provider != nil {
		c.provider(&p)
	}
	store := &orderStore{}
	members := &memberSet{ids: map[uuid.UUID]bool{}}
	if linked {
		id := uuid.New()
		store.linked = &saml.Principal{ID: id}
		members.ids[id] = true
	}
	sessions := &sessionsSpy{}
	var logs bytes.Buffer
	svc := &saml.Service{
		Providers: staticProvider{p}, Entitlements: ssoAllowed{}, Flows: openFlows{}, Uses: freshAssertions{},
		Store: store, Memberships: members, JIT: &joinSeam{members: members}, BaseURL: func() string { return refusalBase },
		Logger: slog.New(slog.NewJSONHandler(&logs, nil)), ProviderFailure: failures,
	}
	h := saml.Handlers{Service: svc, Sessions: sessions, Logger: svc.Logger}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /saml/{provider_id}/acs", h.ACS)
	acs := p.ACSURL(refusalBase)
	o := resp{
		nameID: saml.NameID{Value: "user-" + uuid.NewString()}, email: email,
		destination: strp(acs), recipient: strp(acs), audiences: [][]string{{p.SPEntityID(refusalBase)}},
	}
	if c.response != nil {
		c.response(&o)
	}
	posted := b64(idp.response(t, o))
	if c.raw != "" {
		posted = c.raw
	}
	req := httptest.NewRequest(http.MethodPost, "/saml/"+p.ID.String()+"/acs", strings.NewReader(url.Values{"SAMLResponse": {posted}}.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return acsAnswer{status: rec.Code, body: rec.Body.String(), logs: logs.String(), calls: store.calls, sessions: len(sessions.got),
		provider: p}
}

// The provider email rule runs before the identity lookup of a SAML sign-in: a linked NameID with a refused address
// gets the answer an unknown one gets, byte for byte, and no store call runs. Only forms XML 1.0 can carry are sent.
func TestSAMLACSRefusesAnUnusableEmailBeforeTheLookup(t *testing.T) {
	const want = `{"detail":{"message":"The identity provider returned an unusable email address","reason":"invalid_provider_email"}}`
	for name, email := range map[string]string{
		"del":              emailMarker + "\x7f@example.test",
		"c1":               emailMarker + "\u0085x@example.test",
		"replacement char": emailMarker + "�@example.test",
	} {
		t.Run(name, func(t *testing.T) {
			linked := postACSEmail(t, email, true)
			unknown := postACSEmail(t, email, false)
			if linked.status != unknown.status || linked.body != unknown.body {
				t.Fatalf("linked %d %s, unknown %d %s: the answers differ", linked.status, linked.body, unknown.status, unknown.body)
			}
			for _, a := range []acsAnswer{linked, unknown} {
				if a.status != http.StatusBadRequest || strings.TrimSpace(a.body) != want {
					t.Fatalf("ACS = %d %s, want 400 %s", a.status, a.body, want)
				}
				if a.calls != 0 || a.sessions != 0 {
					t.Fatalf("%d store calls and %d sessions ran before the refusal", a.calls, a.sessions)
				}
				if !strings.Contains(a.logs, `"reason_code":"malformed"`) || strings.Contains(a.logs, emailMarker) {
					t.Fatalf("refusal log: %s", a.logs)
				}
			}
		})
	}
}

// A usable address still reaches the lookup (control for the harness).
func TestSAMLACSWithAUsableEmailReachesTheLookup(t *testing.T) {
	if a := postACSEmail(t, "a@example.test", true); a.calls == 0 || a.status != http.StatusOK {
		t.Fatalf("ACS = %d %s with %d store calls", a.status, a.body, a.calls)
	}
}
