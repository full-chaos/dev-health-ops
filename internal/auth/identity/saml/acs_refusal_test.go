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

const (
	detailNotMember = "This account is not a member of the provider's organization."
	detailAmbiguous = "More than one account carries this sign-in. Contact your organization administrator."
)

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

// memberSet answers Member from the principals it holds and counts reads.
type memberSet struct {
	ids   map[uuid.UUID]bool
	reads int
}

func (m *memberSet) Member(_ context.Context, principalID, _ uuid.UUID) (bool, error) {
	m.reads++
	return m.ids[principalID], nil
}

// joinAll adds every principal it is asked about to members.
type joinAll struct {
	members *memberSet
	joins   int
}

func (j *joinAll) JoinOnSignIn(_ context.Context, principalID uuid.UUID, _ saml.Provider) error {
	j.joins++
	j.members.ids[principalID] = true
	return nil
}

// lookupStore answers IdentityBySubject with hit (when set) or err, and
// counts every call after it.
type lookupStore struct {
	hit                             *saml.Principal
	err                             error
	normalize, byEmail, provisioned int
}

func (s *lookupStore) IdentityBySubject(context.Context, saml.Identity, *saml.LegacyMatch) (saml.Principal, bool, error) {
	if s.hit != nil {
		return *s.hit, true, nil
	}
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

type sessionsSpy struct{ got []saml.SignIn }

func (s *sessionsSpy) IssueFederatedSession(w http.ResponseWriter, _ *http.Request, signIn saml.SignIn) {
	s.got = append(s.got, signIn)
	w.WriteHeader(http.StatusOK)
}

type acsRun struct {
	status   int
	detail   string
	logs     string
	store    *lookupStore
	members  *memberSet
	sessions *sessionsSpy
	org      uuid.UUID
}

// postACS posts one signed IdP-initiated response (auto-provisioning on, a free
// asserted email) through the real ACS handler.
func postACS(t *testing.T, store *lookupStore, members *memberSet, jit saml.MembershipJIT) acsRun {
	t.Helper()
	idp := newTestIdP(t)
	p := saml.Provider{
		ID: uuid.New(), OrgID: uuid.New(), Protocol: "saml", Status: "active",
		AllowIdPInitiated: true, AutoProvision: true,
		Config: saml.Config{EntityID: idp.entityID, Certificate: encodeCert(idp)},
	}
	sessions := &sessionsSpy{}
	var logs bytes.Buffer
	svc := &saml.Service{
		Providers: staticProvider{p}, Entitlements: ssoAllowed{}, Flows: openFlows{}, Uses: freshAssertions{},
		Store: store, Memberships: members, JIT: jit, BaseURL: func() string { return refusalBase },
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
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	return acsRun{status: rec.Code, detail: body.Detail, logs: logs.String(), store: store, members: members, sessions: sessions, org: p.OrgID}
}

func (r acsRun) refused(t *testing.T, status int, detail, reason string) {
	t.Helper()
	if r.status != status || r.detail != detail {
		t.Fatalf("ACS = %d detail %q, want %d detail %q", r.status, r.detail, status, detail)
	}
	if r.store.provisioned != 0 || len(r.sessions.got) != 0 {
		t.Fatalf("provisions %d, sessions %d; want none", r.store.provisioned, len(r.sessions.got))
	}
	if !strings.Contains(r.logs, `"reason_code":"`+reason+`"`) {
		t.Fatalf("refusal log lacks reason_code %s: %s", reason, r.logs)
	}
}

// TestSAMLACSRefusesANonMemberAndASharedNameIDLikeOIDC: the identity lookup
// answers "not a member" or "more than one account"; the ACS answers 403
// membership_required or 409 identity_conflict with the OIDC details, reads no
// email, reads no membership, provisions nothing and issues no session.
func TestSAMLACSRefusesANonMemberAndASharedNameIDLikeOIDC(t *testing.T) {
	for name, tc := range map[string]struct {
		lookup error
		status int
		detail string
		reason string
	}{
		"not a member of the provider organization": {saml.ErrNotOrgMember, http.StatusForbidden, detailNotMember, "membership_required"},
		"NameID held by more than one account":      {saml.ErrIdentityAmbiguous, http.StatusConflict, detailAmbiguous, "identity_conflict"},
	} {
		t.Run(name, func(t *testing.T) {
			members := &memberSet{ids: map[uuid.UUID]bool{}}
			run := postACS(t, &lookupStore{err: tc.lookup}, members, &joinAll{members: members})
			run.refused(t, tc.status, tc.detail, tc.reason)
			if run.store.normalize+run.store.byEmail+run.members.reads != 0 {
				t.Fatalf("normalize %d, email lookups %d, membership reads %d; want none",
					run.store.normalize, run.store.byEmail, run.members.reads)
			}
		})
	}
}

// TestSAMLACSRefusesEverySignInThatEndsWithoutMembership: a linked identity
// whose account is not a member, and a new account with no membership seam to
// join it, are refused with 403 membership_required; the new account is
// refused before it is provisioned. No session is issued.
func TestSAMLACSRefusesEverySignInThatEndsWithoutMembership(t *testing.T) {
	t.Run("linked identity, not a member", func(t *testing.T) {
		run := postACS(t, &lookupStore{hit: &saml.Principal{ID: uuid.New()}}, &memberSet{ids: map[uuid.UUID]bool{}}, nil)
		run.refused(t, http.StatusForbidden, detailNotMember, "membership_required")
	})
	t.Run("new account, no membership seam", func(t *testing.T) {
		run := postACS(t, &lookupStore{}, &memberSet{ids: map[uuid.UUID]bool{}}, nil)
		run.refused(t, http.StatusForbidden, detailNotMember, "membership_required")
		if run.members.reads != 0 {
			t.Fatalf("membership reads %d before a refused provisioning, want none", run.members.reads)
		}
	})
}

// TestSAMLACSIssuesAnOrgSessionToAMember: a member signs in, and a new account
// joined by the membership seam signs in; both sessions carry the provider's
// organization.
func TestSAMLACSIssuesAnOrgSessionToAMember(t *testing.T) {
	t.Run("linked identity, member", func(t *testing.T) {
		id := uuid.New()
		run := postACS(t, &lookupStore{hit: &saml.Principal{ID: id}}, &memberSet{ids: map[uuid.UUID]bool{id: true}}, nil)
		if run.status != http.StatusOK || len(run.sessions.got) != 1 || run.sessions.got[0].PrincipalID != id ||
			run.sessions.got[0].OrganizationID == nil || *run.sessions.got[0].OrganizationID != run.org {
			t.Fatalf("ACS = %d sessions %+v, want one session in org %s", run.status, run.sessions.got, run.org)
		}
	})
	t.Run("new account joined on sign-in", func(t *testing.T) {
		members := &memberSet{ids: map[uuid.UUID]bool{}}
		jit := &joinAll{members: members}
		run := postACS(t, &lookupStore{}, members, jit)
		if run.status != http.StatusOK || run.store.provisioned != 1 || jit.joins != 1 || len(run.sessions.got) != 1 ||
			!run.sessions.got[0].Created || run.sessions.got[0].OrganizationID == nil || *run.sessions.got[0].OrganizationID != run.org {
			t.Fatalf("ACS = %d provisions %d joins %d sessions %+v, want one new account in org %s",
				run.status, run.store.provisioned, jit.joins, run.sessions.got, run.org)
		}
	})
}
