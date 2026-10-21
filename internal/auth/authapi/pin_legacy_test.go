package authapi

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

func pinned(record session.Record, pin *int64) session.Record {
	record.LegacyTokenVersion = pin
	return record
}

func accepted(record session.Record) session.Result {
	return session.Result{Reason: session.ReasonAccepted, Issued: session.Issued{
		Session: record, Credential: contracts.NewSecretString(credential()), ExpiresAt: time.Now().Add(time.Hour)}}
}

func TestRefreshRevokesASessionPinnedToAnOlderTokenVersion(t *testing.T) {
	f := newFixture(t)
	older := int64(3)
	f.sessions.result = accepted(pinned(f.record, &older))
	response := f.do(http.MethodPost, PatternRefresh, refreshBody(credential()), nil)
	expectError(t, response, http.StatusUnauthorized, reasonRevisionStale)
	if len(f.sessions.revoked) != 1 || f.sessions.revoked[0] != f.record.ID {
		t.Fatalf("revoked %v, want the session", f.sessions.revoked)
	}
}

func TestRefreshRefusesASessionWithoutAPinAndLeavesIt(t *testing.T) {
	f := newFixture(t)
	f.sessions.result = accepted(pinned(f.record, nil))
	response := f.do(http.MethodPost, PatternRefresh, refreshBody(credential()), nil)
	expectError(t, response, http.StatusUnauthorized, reasonRevisionStale)
	if len(f.sessions.revoked) != 0 {
		t.Fatalf("revoked %v, want nothing", f.sessions.revoked)
	}
	if !strings.Contains(f.logs.String(), "session has no token_version pin") {
		t.Fatalf("no log names the missing pin: %s", f.logs)
	}
}

func TestCompatRefreshRevokesASessionPinnedToAnOlderTokenVersion(t *testing.T) {
	f := newCompatFixture(t)
	older := int64(3)
	f.sessions.result = accepted(pinned(f.record, &older))
	response := f.do(http.MethodPost, PatternCompatRefresh, `{"refresh_token":"`+credential()+`"}`, nil)
	expectGolden(t, response, response.Body.Bytes(), "refresh: original outside the grace window (reuse)")
	if len(f.sessions.revoked) != 1 || f.sessions.revoked[0] != f.record.ID {
		t.Fatalf("revoked %v, want the session", f.sessions.revoked)
	}
}

func TestCompatRefreshRefusesASessionWithoutAPinAndLeavesIt(t *testing.T) {
	f := newCompatFixture(t)
	f.sessions.result = accepted(pinned(f.record, nil))
	response := f.do(http.MethodPost, PatternCompatRefresh, `{"refresh_token":"`+credential()+`"}`, nil)
	expectGolden(t, response, response.Body.Bytes(), "refresh: garbage")
	if len(f.sessions.revoked) != 0 {
		t.Fatalf("revoked %v, want nothing", f.sessions.revoked)
	}
	if !strings.Contains(f.logs.String(), "session has no token_version pin") {
		t.Fatalf("no log names the missing pin: %s", f.logs)
	}
}

// legacyToken is a go-api refresh JWT for the fixture's principal, signed with
// the access key, as go-api's create_refresh_token mints it.
func legacyToken(t *testing.T, f *compatFixture, org string, secret string) string {
	t.Helper()
	signer, err := edgetoken.NewSigner(secret, testIssuer, testAudience)
	if err != nil {
		t.Fatal(err)
	}
	token, err := signer.Refresh(edgetoken.RefreshClaims{UserID: f.principal.String(), OrgID: org, FamilyID: uuid.NewString()},
		time.Now(), "legacy-jti-0001")
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func TestCompatRefreshExchangesAGoAPIRefreshTokenForASession(t *testing.T) {
	f := newCompatFixture(t)
	f.sessions.legacyResult = accepted(f.record)
	response := f.do(http.MethodPost, PatternCompatRefresh, `{"refresh_token":"`+legacyToken(t, f, f.org.String(), testSecret)+`"}`, nil)
	if response.Code != http.StatusOK {
		t.Fatalf("status %d: %s", response.Code, response.Body)
	}
	if len(f.sessions.legacy) != 1 {
		t.Fatalf("%d legacy exchanges, want 1", len(f.sessions.legacy))
	}
	got := f.sessions.legacy[0]
	if got.PrincipalID != f.principal || got.OrganizationID == nil || *got.OrganizationID != f.org || got.JTI.Reveal() != "legacy-jti-0001" {
		t.Fatalf("exchanged %+v (jti %q), want the token's sub, org_id and jti", got, got.JTI.Reveal())
	}
	if len(f.projector.userCalls) != 1 || f.projector.userCalls[0] != f.principal || len(f.projector.orgCalls) != 1 || f.projector.orgCalls[0] != f.org {
		t.Fatalf("projected users %v orgs %v, want the token's principal and organization first", f.projector.userCalls, f.projector.orgCalls)
	}
	body := decodeObject(t, response.Body.Bytes())
	if body["refresh_token"] != credential() || body["token_type"] != "bearer" {
		t.Fatalf("body %s, want the session's opaque credential", response.Body)
	}
	bearer, _, err := f.issuer.Verify(body["access_token"].(string))
	if err != nil || bearer.SessionID != f.record.ID {
		t.Fatalf("access token %+v %v, want the new session's sid", bearer, err)
	}
}

func TestCompatRefreshOfAGoAPIRefreshTokenWithoutAnOrganization(t *testing.T) {
	f := newCompatFixture(t)
	record := f.record
	record.OrganizationID = nil
	f.sessions.legacyResult = accepted(record)
	response := f.do(http.MethodPost, PatternCompatRefresh, `{"refresh_token":"`+legacyToken(t, f, "", testSecret)+`"}`, nil)
	if response.Code != http.StatusOK || len(f.sessions.legacy) != 1 || f.sessions.legacy[0].OrganizationID != nil || len(f.projector.orgCalls) != 0 {
		t.Fatalf("status %d legacy %+v orgs %v: %s", response.Code, f.sessions.legacy, f.projector.orgCalls, response.Body)
	}
}

func TestCompatRefreshAnswersGoAPIsRefusalsForAGoAPIRefreshToken(t *testing.T) {
	cases := map[session.Reason]string{
		session.ReasonRefreshUnknown:       "refresh: garbage",
		session.ReasonRefreshExpired:       "refresh: garbage",
		session.ReasonSessionRevoked:       "refresh: garbage",
		session.ReasonRefreshReused:        "refresh: original outside the grace window (reuse)",
		session.ReasonPrincipalDeactivated: "refresh: judy deactivated",
	}
	for reason, golden := range cases {
		f := newCompatFixture(t)
		f.sessions.legacyResult = session.Result{Reason: reason}
		response := f.do(http.MethodPost, PatternCompatRefresh, `{"refresh_token":"`+legacyToken(t, f, f.org.String(), testSecret)+`"}`, nil)
		expectGolden(t, response, response.Body.Bytes(), golden)
		if len(f.sessions.revoked) != 0 {
			t.Fatalf("%s: revoked %v; the store decides legacy revocations", reason, f.sessions.revoked)
		}
	}
}

func TestCompatRefreshRefusesAGoAPIRefreshTokenItCannotVerify(t *testing.T) {
	for name, token := range map[string]func(*compatFixture) string{
		"another key": func(f *compatFixture) string {
			return legacyToken(t, f, f.org.String(), "another-key-0123456789abcdef-0123456")
		},
		"an access token": func(f *compatFixture) string {
			access, err := f.issuer.Access(Grant{PrincipalID: f.principal, SessionID: f.record.ID, User: f.users.users[f.principal]})
			if err != nil {
				t.Fatal(err)
			}
			return access
		},
		"three dots of nothing": func(*compatFixture) string { return "a.b.c" },
	} {
		f := newCompatFixture(t)
		response := f.do(http.MethodPost, PatternCompatRefresh, `{"refresh_token":"`+token(f)+`"}`, nil)
		expectGolden(t, response, response.Body.Bytes(), "refresh: garbage")
		if len(f.sessions.legacy) != 0 || len(f.projector.userCalls) != 0 {
			t.Fatalf("%s: exchanged %d, projected %v; an unverified token reaches nothing", name, len(f.sessions.legacy), f.projector.userCalls)
		}
	}
}

func TestCompatRefreshRefusesAGoAPIRefreshTokenOfAnUnknownUserOrOrganization(t *testing.T) {
	for name, mutate := range map[string]func(*compatFixture){
		"user":         func(f *compatFixture) { f.projector.gone[f.principal] = true },
		"organization": func(f *compatFixture) { delete(f.projector.orgs, f.org) },
	} {
		f := newCompatFixture(t)
		mutate(f)
		response := f.do(http.MethodPost, PatternCompatRefresh, `{"refresh_token":"`+legacyToken(t, f, f.org.String(), testSecret)+`"}`, nil)
		expectGolden(t, response, response.Body.Bytes(), "refresh: garbage")
		if len(f.sessions.legacy) != 0 {
			t.Fatalf("%s: exchanged a token whose %s is unknown", name, name)
		}
	}
}

func TestVerifyLegacyRefreshReadsSubOrgAndJTI(t *testing.T) {
	issuer, err := NewIssuer(testSecret, testIssuer, testAudience)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := edgetoken.NewSigner(testSecret, testIssuer, testAudience)
	if err != nil {
		t.Fatal(err)
	}
	principal, org := uuid.New(), uuid.New()
	for name, tc := range map[string]struct {
		claims edgetoken.RefreshClaims
		jti    string
		ok     bool
	}{
		"org":          {edgetoken.RefreshClaims{UserID: principal.String(), OrgID: org.String()}, "j1", true},
		"no org":       {edgetoken.RefreshClaims{UserID: principal.String()}, "j2", true},
		"sub not uuid": {edgetoken.RefreshClaims{UserID: "alice"}, "j3", false},
		"org not uuid": {edgetoken.RefreshClaims{UserID: principal.String(), OrgID: "acme"}, "j4", false},
		"empty jti":    {edgetoken.RefreshClaims{UserID: principal.String()}, "", false},
	} {
		token, err := signer.Refresh(tc.claims, time.Now(), tc.jti)
		if err != nil {
			t.Fatal(err)
		}
		legacy, err := issuer.VerifyLegacyRefresh(token)
		if (err == nil) != tc.ok {
			t.Fatalf("%s: err %v, want ok=%t", name, err, tc.ok)
		}
		if !tc.ok {
			continue
		}
		if legacy.PrincipalID != principal || legacy.JTI.Reveal() != tc.jti || (tc.claims.OrgID == "") != (legacy.OrganizationID == nil) {
			t.Fatalf("%s: %+v", name, legacy)
		}
	}
}

func TestRefreshOfAPinlessSessionAnswersRevisionStale(t *testing.T) {
	f := newFixture(t)
	f.sessions.result = session.Result{Reason: session.ReasonRevisionStale}
	response := f.do(http.MethodPost, PatternRefresh, refreshBody(credential()), nil)
	expectError(t, response, http.StatusUnauthorized, reasonRevisionStale)
	if len(f.sessions.revoked) != 0 {
		t.Fatalf("revoked %v, want nothing", f.sessions.revoked)
	}
}

func TestCompatRefreshOfAPinlessSessionAnswersGoAPIsInvalidRefresh(t *testing.T) {
	f := newCompatFixture(t)
	f.sessions.result = session.Result{Reason: session.ReasonRevisionStale}
	response := f.do(http.MethodPost, PatternCompatRefresh, `{"refresh_token":"`+credential()+`"}`, nil)
	expectGolden(t, response, response.Body.Bytes(), "refresh: garbage")
	if len(f.sessions.revoked) != 0 {
		t.Fatalf("revoked %v, want nothing", f.sessions.revoked)
	}
}
