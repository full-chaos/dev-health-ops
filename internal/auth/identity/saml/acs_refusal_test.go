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
	members      *memberSet
	joinErr      error
	noJoin       bool
	joins        int
	provisionErr error
	provisioned  []saml.NewAccount
	newID        uuid.UUID
}

// ProvisionOnSignIn is all-or-nothing like the real seam: on provisionErr no
// account and no membership exist afterwards.
func (j *joinSeam) ProvisionOnSignIn(_ context.Context, account saml.NewAccount, _ saml.Provider) (uuid.UUID, error) {
	if j.provisionErr != nil {
		return uuid.Nil, j.provisionErr
	}
	j.newID = uuid.New()
	j.provisioned = append(j.provisioned, account)
	j.members.ids[j.newID] = true
	return j.newID, nil
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
	return postACSAuto(t, store, members, jit, true)
}

func postACSAuto(t *testing.T, store *lookupStore, members *memberSet, jit saml.MembershipJIT, autoProvision bool) acsRun {
	t.Helper()
	idp := newTestIdP(t)
	p := saml.Provider{
		ID: uuid.New(), OrgID: uuid.New(), Protocol: "saml", Status: "active",
		AllowIdPInitiated: true, AutoProvision: autoProvision,
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
			seam := &joinSeam{members: members}
			run := postACS(t, &lookupStore{err: tc.lookup}, members, seam)
			run.refused(t, tc.status, tc.detail, tc.reason)
			if seam.joins != 0 || len(seam.provisioned) != 0 || len(members.ids) != 0 {
				t.Fatalf("joins %d, provisions %d, members %d with JIT on; want none", seam.joins, len(seam.provisioned), len(members.ids))
			}
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

// TestSAMLACSRefusesWithoutWrites: a sign-in that would need a new account is
// refused with 403 membership_required before any account or membership write
// when no membership seam is set; a linked account the seam leaves outside the
// organization is refused with 403, and a seam error is a 500. No session is
// issued.
func TestSAMLACSRefusesWithoutWrites(t *testing.T) {
	down := errors.New("membership store down")
	for name, tc := range map[string]struct {
		hit    bool
		seam   *joinSeam
		status int
		detail string
	}{
		"new account, no seam":                   {status: http.StatusForbidden, detail: detailNotMember},
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
			if run.store.provisioned != 0 || len(run.sessions.got) != 0 || len(members.ids) != 0 {
				t.Fatalf("accounts written %d, sessions %d, members %d; want none", run.store.provisioned, len(run.sessions.got), len(members.ids))
			}
			if tc.seam != nil && len(tc.seam.provisioned) != 0 {
				t.Fatalf("provisions %d for a linked account, want none", len(tc.seam.provisioned))
			}
			if tc.status == http.StatusForbidden && !strings.Contains(run.logs, `"reason_code":"membership_required"`) {
				t.Fatalf("refusal log lacks reason_code membership_required: %s", run.logs)
			}
		})
	}
}

// TestSAMLACSProvisionsANewAccountWhenJITIsOn: a new account with the seam
// set is created, joined and signed in to the provider's organization; the
// seam gets the asserted email and the identity key.
func TestSAMLACSProvisionsANewAccountWhenJITIsOn(t *testing.T) {
	members := &memberSet{ids: map[uuid.UUID]bool{}}
	seam := &joinSeam{members: members}
	run := postACS(t, &lookupStore{}, members, seam)
	if run.status != http.StatusOK || len(seam.provisioned) != 1 || seam.joins != 0 {
		t.Fatalf("ACS = %d provisions %d joins %d, want 200, 1, 0", run.status, len(seam.provisioned), seam.joins)
	}
	if len(run.sessions.got) != 1 || run.sessions.got[0].PrincipalID != seam.newID ||
		run.sessions.got[0].OrganizationID == nil || *run.sessions.got[0].OrganizationID != run.org {
		t.Fatalf("sessions %+v, want one in org %s for the new account", run.sessions.got, run.org)
	}
	acct := seam.provisioned[0]
	if !strings.HasPrefix(acct.Email, "free-") || acct.Identity.Subject == "" || acct.Identity.Provider == "" || acct.Identity.Issuer == "" {
		t.Fatalf("seam account %+v lacks email or identity key", acct)
	}
}

// TestSAMLACSProvisionFailureWritesNothing: a seam that fails answers its
// mapped status, leaves no account, membership or session.
func TestSAMLACSProvisionFailureWritesNothing(t *testing.T) {
	for name, tc := range map[string]struct {
		err    error
		status int
		detail string
		reason string
	}{
		"store down":       {errors.New("store down"), http.StatusInternalServerError, "Internal Server Error", "dependency_unavailable"},
		"sign-up closed":   {saml.ErrSignupNotAvailable, http.StatusForbidden, "Sign-up through SSO is not available", "signup_unavailable"},
		"email now in use": {saml.ErrEmailTaken, http.StatusConflict, "", "email_collision"},
	} {
		t.Run(name, func(t *testing.T) {
			members := &memberSet{ids: map[uuid.UUID]bool{}}
			seam := &joinSeam{members: members, provisionErr: tc.err}
			run := postACS(t, &lookupStore{}, members, seam)
			if run.status != tc.status || (tc.detail != "" && run.detail != tc.detail) {
				t.Fatalf("ACS = %d detail %q, want %d detail %q", run.status, run.detail, tc.status, tc.detail)
			}
			if len(run.sessions.got) != 0 || len(members.ids) != 0 || len(seam.provisioned) != 0 || run.store.provisioned != 0 {
				t.Fatalf("status %d sessions %d members %d provisions %d; want no writes",
					run.status, len(run.sessions.got), len(members.ids), len(seam.provisioned))
			}
			if !strings.Contains(run.logs, `"reason_code":"`+tc.reason+`"`) {
				t.Fatalf("log lacks reason_code %s: %s", tc.reason, run.logs)
			}
		})
	}
}

// TestSAMLACSNewAccountWithJITOnAndProviderAutoProvisionOffIsRefused: the
// provider's own auto-provisioning switch still gates a new account; the seam
// is never called.
func TestSAMLACSNewAccountWithProviderAutoProvisionOffIsRefused(t *testing.T) {
	members := &memberSet{ids: map[uuid.UUID]bool{}}
	seam := &joinSeam{members: members}
	run := postACSAuto(t, &lookupStore{}, members, seam, false)
	if run.status != http.StatusBadRequest || len(seam.provisioned) != 0 || len(run.sessions.got) != 0 || len(members.ids) != 0 {
		t.Fatalf("ACS = %d provisions %d sessions %d members %d; want 400 and no writes",
			run.status, len(seam.provisioned), len(run.sessions.got), len(members.ids))
	}
}
