package saml_test

import (
	"bytes"
	"context"
	"encoding/json"
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

const refusalBase = "https://sp.example.test/api/v1/auth"

type staticProvider struct{ p saml.Provider }

func (s staticProvider) Provider(context.Context, uuid.UUID) (saml.Provider, error) { return s.p, nil }

type ssoAllowed struct{}

func (ssoAllowed) OrgHasSSO(context.Context, uuid.UUID) (bool, error) { return true, nil }

type openFlows struct{}

func (openFlows) SaveFlow(context.Context, saml.Flow) error { return nil }

func (openFlows) ConsumeFlow(context.Context, uuid.UUID, string, [32]byte, time.Time) error {
	return nil
}

type freshAssertions struct{}

func (freshAssertions) UseAssertion(context.Context, uuid.UUID, string, string, time.Time) error {
	return nil
}

type noMembers struct{ calls int }

func (m *noMembers) Member(context.Context, uuid.UUID, uuid.UUID) (bool, error) {
	m.calls++
	return false, nil
}

// lookupStore answers IdentityBySubject with err and counts every call after it.
type lookupStore struct {
	err                             error
	normalize, byEmail, provisioned int
}

func (s *lookupStore) IdentityBySubject(context.Context, saml.Identity, *saml.LegacyMatch) (saml.Principal, bool, error) {
	return saml.Principal{}, false, s.err
}

func (s *lookupStore) NormalizeEmail(raw string) (string, error) {
	s.normalize++
	return strings.ToLower(raw), nil
}

func (s *lookupStore) PrincipalByEmail(context.Context, string) (saml.EmailOwner, bool, error) {
	s.byEmail++
	return saml.EmailOwner{}, false, nil
}

func (s *lookupStore) ProvisionFederated(context.Context, saml.NewFederatedUser) (uuid.UUID, error) {
	s.provisioned++
	return uuid.New(), nil
}

type sessionsSpy struct{ issued int }

func (s *sessionsSpy) IssueFederatedSession(w http.ResponseWriter, _ *http.Request, _ saml.SignIn) {
	s.issued++
	w.WriteHeader(http.StatusOK)
}

// TestSAMLACSRefusesANonMemberAndASharedNameIDLikeOIDC: the identity lookup
// answers "not a member" or "more than one account"; the ACS answers 403
// membership_required or 409 identity_conflict with the OIDC details, reads no
// email, provisions nothing and issues no session, with auto-provisioning on
// and a free asserted email.
func TestSAMLACSRefusesANonMemberAndASharedNameIDLikeOIDC(t *testing.T) {
	for name, tc := range map[string]struct {
		lookup error
		status int
		detail string
		reason string
	}{
		"not a member of the provider organization": {saml.ErrNotOrgMember, http.StatusForbidden,
			"This account is not a member of the provider's organization.", "membership_required"},
		"NameID held by more than one account": {saml.ErrIdentityAmbiguous, http.StatusConflict,
			"More than one account carries this sign-in. Contact your organization administrator.", "identity_conflict"},
	} {
		t.Run(name, func(t *testing.T) {
			idp := newTestIdP(t)
			p := saml.Provider{
				ID: uuid.New(), OrgID: uuid.New(), Protocol: "saml", Status: "active",
				AllowIdPInitiated: true, AutoProvision: true,
				Config: saml.Config{EntityID: idp.entityID, Certificate: encodeCert(idp)},
			}
			store := &lookupStore{err: tc.lookup}
			members := &noMembers{}
			sessions := &sessionsSpy{}
			var logs bytes.Buffer
			svc := &saml.Service{
				Providers: staticProvider{p}, Entitlements: ssoAllowed{}, Flows: openFlows{}, Uses: freshAssertions{},
				Store: store, Memberships: members, BaseURL: func() string { return refusalBase },
				Logger: slog.New(slog.NewJSONHandler(&logs, nil)),
			}
			h := saml.Handlers{Service: svc, Sessions: sessions, Logger: svc.Logger}
			mux := http.NewServeMux()
			mux.HandleFunc("POST /saml/{provider_id}/acs", h.ACS)

			acs := p.ACSURL(refusalBase)
			posted := b64(idp.response(t, resp{
				nameID: saml.NameID{Value: "user-" + uuid.NewString()}, email: "free-" + uuid.NewString() + "@example.com",
				destination: strp(acs), recipient: strp(acs), audiences: [][]string{{p.SPEntityID(refusalBase)}},
			}))
			req := httptest.NewRequest(http.MethodPost, "/saml/"+p.ID.String()+"/acs",
				strings.NewReader(url.Values{"SAMLResponse": {posted}}.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)

			var body struct {
				Detail string `json:"detail"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || rec.Code != tc.status || body.Detail != tc.detail {
				t.Fatalf("ACS = %d %s, want %d detail %q", rec.Code, rec.Body, tc.status, tc.detail)
			}
			if store.normalize+store.byEmail+store.provisioned+members.calls+sessions.issued != 0 {
				t.Fatalf("normalize %d, email lookups %d, provisions %d, membership reads %d, sessions %d; want none",
					store.normalize, store.byEmail, store.provisioned, members.calls, sessions.issued)
			}
			if !strings.Contains(logs.String(), `"reason_code":"`+tc.reason+`"`) {
				t.Fatalf("refusal log lacks reason_code %s: %s", tc.reason, logs.String())
			}
		})
	}
}
