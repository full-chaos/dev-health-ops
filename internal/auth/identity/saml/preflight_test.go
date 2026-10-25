package saml_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/saml"
)

// usesOnce keeps each assertion single-use and counts every UseAssertion.
type usesOnce struct {
	used  map[string]bool
	calls int
}

func (u *usesOnce) UseAssertion(_ context.Context, _ uuid.UUID, issuer, id string, _ time.Time) error {
	u.calls++
	if u.used[issuer+"\x00"+id] {
		return saml.ErrAssertionReplayed
	}
	u.used[issuer+"\x00"+id] = true
	return nil
}

// countedFlows accepts every SP-initiated request and counts ConsumeFlow.
type countedFlows struct{ consumes int }

func (f *countedFlows) SaveFlow(context.Context, saml.Flow) error { return nil }

func (f *countedFlows) ConsumeFlow(context.Context, uuid.UUID, string, [32]byte, time.Time) error {
	f.consumes++
	return nil
}

// gatedSessions is a SessionIssuer whose Preflight answers err.
type gatedSessions struct {
	sessionsSpy
	err    error
	checks int
}

func (g *gatedSessions) Preflight(context.Context) error {
	g.checks++
	return g.err
}

func TestSAMLUnavailableTextIsGoAPIs(t *testing.T) {
	if saml.UnavailableText != sessionissue.UnavailableText {
		t.Fatalf("UnavailableText %q is not go-api's %q", saml.UnavailableText, sessionissue.UnavailableText)
	}
}

// ACS checks the signer after the body is read and before the response is
// consumed: on a failed check it answers go-api's 503, consumes neither the
// SP-initiated request nor the assertion and keeps the binding cookie, and the
// same response with the same cookie signs in once the signer is back.
func TestSAMLACSChecksTheSignerBeforeTheAssertionIsUsed(t *testing.T) {
	idp := newTestIdP(t)
	p := saml.Provider{
		ID: uuid.New(), OrgID: uuid.New(), Protocol: "saml", Status: "active",
		Config: saml.Config{EntityID: idp.entityID, Certificate: encodeCert(idp)},
	}
	id := uuid.New()
	uses := &usesOnce{used: map[string]bool{}}
	flows := &countedFlows{}
	sessions := &gatedSessions{err: errors.New("signer unavailable")}
	var logs bytes.Buffer
	svc := &saml.Service{
		Providers: staticProvider{p}, Entitlements: ssoAllowed{}, Flows: flows, Uses: uses,
		Store: &lookupStore{hit: &saml.Principal{ID: id}}, Memberships: &memberSet{ids: map[uuid.UUID]bool{id: true}},
		BaseURL: func() string { return refusalBase }, Logger: slog.New(slog.NewJSONHandler(&logs, nil)),
	}
	h := saml.Handlers{Service: svc, Sessions: sessions, Logger: svc.Logger}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /saml/{provider_id}/acs", h.ACS)
	acs := p.ACSURL(refusalBase)
	posted := b64(idp.response(t, resp{
		nameID: saml.NameID{Value: "user-" + uuid.NewString()}, email: "member@example.com", inResponseTo: "id-request-1",
		destination: strp(acs), recipient: strp(acs), audiences: [][]string{{p.SPEntityID(refusalBase)}},
	}))
	post := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/saml/"+p.ID.String()+"/acs",
			strings.NewReader(url.Values{"SAMLResponse": {posted}}.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.AddCookie(&http.Cookie{Name: saml.BindingCookieName("id-request-1"), Value: "nonce-1"})
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		return rec
	}

	rec := post()
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503: %s", rec.Code, rec.Body)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if want := map[string]any{"detail": map[string]any{"message": sessionissue.UnavailableText}}; !reflect.DeepEqual(body, want) {
		t.Fatalf("body %s, want %v", rec.Body, want)
	}
	if got := rec.Header().Values("Set-Cookie"); len(got) != 0 {
		t.Fatalf("Set-Cookie %v on a refusal that consumed nothing: the binding cookie must stay", got)
	}
	if uses.calls != 0 || flows.consumes != 0 || len(sessions.got) != 0 || sessions.checks != 1 {
		t.Fatalf("assertion uses %d, flow consumes %d, sessions %d, checks %d", uses.calls, flows.consumes, len(sessions.got), sessions.checks)
	}
	if !strings.Contains(logs.String(), `"reason_code":"dependency_unavailable"`) {
		t.Fatalf("refusal log lacks reason_code dependency_unavailable: %s", logs.String())
	}

	sessions.err = nil
	rec = post()
	if rec.Code != http.StatusOK || uses.calls != 1 || flows.consumes != 1 || len(sessions.got) != 1 || sessions.got[0].PrincipalID != id {
		t.Fatalf("retry: status %d, uses %d, flow consumes %d, sessions %+v; want the same response and binding cookie to sign in",
			rec.Code, uses.calls, flows.consumes, sessions.got)
	}
}
