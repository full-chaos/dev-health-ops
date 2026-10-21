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

// joinSeam is the membership seam: JoinOnSignIn adds a principal to members
// (unless noJoin) or fails with joinErr.
type joinSeam struct {
	members *memberSet
	joinErr error
	noJoin  bool
	joins   int
}

func (j *joinSeam) JoinOnSignIn(_ context.Context, principalID uuid.UUID, _ saml.Provider) error {
	j.joins++
	if j.joinErr != nil {
		return j.joinErr
	}
	if !j.noJoin {
		j.members.ids[principalID] = true
	}
	return nil
}

// lookupStore answers IdentityBySubject with hit (when set) or err, and
// counts every call after it; provisioned counts account writes.
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
			run := postACS(t, &lookupStore{err: tc.lookup}, members, &joinSeam{members: members})
			run.refused(t, tc.status, tc.detail, tc.reason)
			if run.store.normalize+run.store.byEmail+run.members.reads != 0 {
				t.Fatalf("normalize %d, email lookups %d, membership reads %d; want none",
					run.store.normalize, run.store.byEmail, run.members.reads)
			}
		})
	}
}

// TestSAMLACSIssuesAnOrgSessionToAMember: a linked account that is a member,
// or that the membership seam joins, gets one session in the provider's
// organization.
func TestSAMLACSIssuesAnOrgSessionToAMember(t *testing.T) {
	for name, joined := range map[string]bool{"member": false, "joined on sign-in": true} {
		t.Run(name, func(t *testing.T) {
			id := uuid.New()
			members := &memberSet{ids: map[uuid.UUID]bool{id: !joined}}
			var jit saml.MembershipJIT
			if joined {
				jit = &joinSeam{members: members}
			}
			run := postACS(t, &lookupStore{hit: &saml.Principal{ID: id}}, members, jit)
			if run.status != http.StatusOK || len(run.sessions.got) != 1 || run.sessions.got[0].PrincipalID != id ||
				run.sessions.got[0].OrganizationID == nil || *run.sessions.got[0].OrganizationID != run.org {
				t.Fatalf("ACS = %d sessions %+v, want one session in org %s", run.status, run.sessions.got, run.org)
			}
		})
	}
}

// TestSAMLACSRefusesANewAccountWithoutWrites: a sign-in that would need a new
// account is refused with 403 membership_required before any account or
// membership write, with or without a membership seam, whatever the seam
// would answer; a linked account the seam leaves outside the organization is
// refused with 403, and a seam error is a 500. No session is issued.
func TestSAMLACSRefusesANewAccountWithoutWrites(t *testing.T) {
	down := errors.New("membership store down")
	for name, tc := range map[string]struct {
		hit    bool
		seam   *joinSeam
		status int
		detail string
	}{
		"new account, no seam":                   {status: http.StatusForbidden, detail: detailNotMember},
		"new account, the join would fail":       {seam: &joinSeam{joinErr: down}, status: http.StatusForbidden, detail: detailNotMember},
		"new account, the join would not join":   {seam: &joinSeam{noJoin: true}, status: http.StatusForbidden, detail: detailNotMember},
		"new account, the join would succeed":    {seam: &joinSeam{}, status: http.StatusForbidden, detail: detailNotMember},
		"linked account, not a member, no seam":  {hit: true, status: http.StatusForbidden, detail: detailNotMember},
		"linked account, the join does not join": {hit: true, seam: &joinSeam{noJoin: true}, status: http.StatusForbidden, detail: detailNotMember},
		"linked account, the join fails":         {hit: true, seam: &joinSeam{joinErr: down}, status: http.StatusInternalServerError, detail: "Internal Server Error"},
	} {
		t.Run(name, func(t *testing.T) {
			members := &memberSet{ids: map[uuid.UUID]bool{}}
			store := &lookupStore{}
			if tc.hit {
				store.hit = &saml.Principal{ID: uuid.New()}
			}
			var jit saml.MembershipJIT
			if tc.seam != nil {
				tc.seam.members = members
				jit = tc.seam
			}
			run := postACS(t, store, members, jit)
			if run.status != tc.status || run.detail != tc.detail {
				t.Fatalf("ACS = %d detail %q, want %d detail %q", run.status, run.detail, tc.status, tc.detail)
			}
			if run.store.provisioned != 0 || len(run.sessions.got) != 0 {
				t.Fatalf("accounts written %d, sessions %d; want none", run.store.provisioned, len(run.sessions.got))
			}
			if !tc.hit && tc.seam != nil && tc.seam.joins != 0 {
				t.Fatalf("membership joins %d for a refused new account, want none", tc.seam.joins)
			}
			if tc.status == http.StatusForbidden && !strings.Contains(run.logs, `"reason_code":"membership_required"`) {
				t.Fatalf("refusal log lacks reason_code membership_required: %s", run.logs)
			}
		})
	}
}
