package saml_test

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity/saml"
)

const heldConflictBody = `{"detail":{"message":"` + saml.ConflictMessage + `"}}`

// heldEmailStore: owner holds every asserted email; byKey, when set, is the account the identity key resolves to.
type heldEmailStore struct {
	owner   saml.EmailOwner
	byKey   *saml.Principal
	byEmail int
}

func (s *heldEmailStore) IdentityBySubject(context.Context, saml.Identity, *saml.LegacyMatch) (saml.Principal, bool, error) {
	if s.byKey != nil {
		return *s.byKey, true, nil
	}
	return saml.Principal{}, false, nil
}

func (s *heldEmailStore) NormalizeEmail(raw string) (string, error) { return strings.ToLower(raw), nil }

func (s *heldEmailStore) PrincipalByEmail(context.Context, string) (saml.EmailOwner, bool, error) {
	s.byEmail++
	return s.owner, true, nil
}

// postHeldACS posts one signed IdP-initiated response for email, with auto_provision on, through the real ACS
// handler.
func postHeldACS(t *testing.T, store saml.Store, members *memberSet, jit *joinSeam, email string) (int, string, *sessionsSpy) {
	t.Helper()
	idp := newTestIdP(t)
	p := saml.Provider{
		ID: uuid.New(), OrgID: uuid.New(), Protocol: "saml", Status: "active",
		AllowIdPInitiated: true, AutoProvision: true, AllowedDomains: []string{"example.com"},
		Config: saml.Config{EntityID: idp.entityID, Certificate: encodeCert(idp)},
	}
	sessions := &sessionsSpy{}
	svc := &saml.Service{
		Providers: staticProvider{p}, Entitlements: ssoAllowed{}, Flows: openFlows{}, Uses: freshAssertions{},
		Store: store, Memberships: members, JIT: jit, BaseURL: func() string { return refusalBase },
		Logger: slog.New(slog.DiscardHandler),
	}
	h := saml.Handlers{Service: svc, Sessions: sessions, Logger: svc.Logger}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /saml/{provider_id}/acs", h.ACS)
	acs := p.ACSURL(refusalBase)
	posted := b64(idp.response(t, resp{
		nameID: saml.NameID{Value: "new-subject-8984"}, email: email,
		destination: strp(acs), recipient: strp(acs), audiences: [][]string{{p.SPEntityID(refusalBase)}},
	}))
	req := httptest.NewRequest(http.MethodPost, "/saml/"+p.ID.String()+"/acs",
		strings.NewReader(url.Values{"SAMLResponse": {posted}}.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec.Code, rec.Body.String(), sessions
}

// An ACS sign-in whose NameID holds no account and whose asserted email an account holds is refused with the one
// generic 409, and nothing is provisioned, joined or issued, with auto_provision on. The email owner carries no
// account class, so a platform admin's account gets the same answer as any other: the email is never a link to an
// existing account.
func TestSAMLACSRefusesAnEmailAPlatformAdminHoldsAsItRefusesAnyAccount(t *testing.T) {
	answers := map[string]string{}
	for name, method := range map[string]string{"platform admin, password": "local", "platform admin, no method": "", "member, github": "github"} {
		store := &heldEmailStore{owner: saml.EmailOwner{PrincipalID: uuid.New(), Method: method}}
		members := &memberSet{ids: map[uuid.UUID]bool{}}
		jit := &joinSeam{members: members}
		status, body, sessions := postHeldACS(t, store, members, jit, "Root.Admin@example.com")
		if status != http.StatusConflict || body != heldConflictBody {
			t.Fatalf("%s: ACS = %d %s, want 409 %s", name, status, body, heldConflictBody)
		}
		if store.byEmail != 1 || len(jit.provisioned) != 0 || jit.joins != 0 || len(sessions.got) != 0 {
			t.Fatalf("%s: email reads %d, provisioned %d, joins %d, sessions %d; want 1, 0, 0, 0", name, store.byEmail, len(jit.provisioned), jit.joins, len(sessions.got))
		}
		answers[name] = body
	}
	for name, body := range answers {
		if body != answers["member, github"] {
			t.Fatalf("%s answer %s differs from another holder's %s", name, body, answers["member, github"])
		}
	}
}

// A platform admin whose account holds the NameID key still signs in by that key, without an email read.
func TestSAMLACSKeySignInOfAPlatformAdminStillSignsIn(t *testing.T) {
	admin := saml.Principal{ID: uuid.New()}
	store := &heldEmailStore{byKey: &admin}
	members := &memberSet{ids: map[uuid.UUID]bool{admin.ID: true}}
	jit := &joinSeam{members: members}
	status, body, sessions := postHeldACS(t, store, members, jit, "root.admin@example.com")
	if status != http.StatusOK || len(sessions.got) != 1 || sessions.got[0].PrincipalID != admin.ID {
		t.Fatalf("ACS = %d %s sessions %+v, want the platform admin %v signed in", status, body, sessions.got, admin.ID)
	}
	if store.byEmail != 0 || len(jit.provisioned) != 0 {
		t.Fatalf("email reads %d, provisioned %d; want 0 and 0", store.byEmail, len(jit.provisioned))
	}
}
