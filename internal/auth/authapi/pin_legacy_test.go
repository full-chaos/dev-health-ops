package authapi

import (
	"errors"
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

// The HS256 refresh exchange is retired: every go-api refresh JWT, one the legacy key verifies included, gets go-api's
// answer for a refresh token that does not verify, and reaches no store or projector. Logout of a verified one still
// answers 200 (TestCompatLogoutKeySetVerifiedLegacyTokenRevokesAndCounts).
func TestCompatRefreshRefusesEveryGoAPIRefreshJWT(t *testing.T) {
	for name, token := range map[string]func(*compatFixture) string{
		"verified with the legacy key": func(f *compatFixture) string {
			return legacyToken(t, f, f.org.String(), testSecret)
		},
		"another key": func(f *compatFixture) string {
			return legacyToken(t, f, f.org.String(), "another-key-0123456789abcdef-0123456")
		},
		"an access token": func(f *compatFixture) string {
			return legacyAccess(t, Grant{PrincipalID: f.principal, SessionID: f.record.ID, User: f.users.users[f.principal]})
		},
		"three dots of nothing": func(*compatFixture) string { return "a.b.c" },
	} {
		f := newCompatFixture(t)
		response := f.do(http.MethodPost, PatternCompatRefresh, `{"refresh_token":"`+token(f)+`"}`, nil)
		expectGolden(t, response, response.Body.Bytes(), "refresh: garbage")
		if f.sessions.exchanges != 0 || len(f.sessions.created) != 0 || len(f.projector.userCalls) != 0 || len(f.projector.orgCalls) != 0 {
			t.Fatalf("%s: %d exchanges, %d created, projected %v %v; a go-api refresh JWT reaches nothing", name,
				f.sessions.exchanges, len(f.sessions.created), f.projector.userCalls, f.projector.orgCalls)
		}
	}
}

func TestVerifyLegacyRefreshReadsSubOrgAndJTI(t *testing.T) {
	issuer := newTestIssuer(t)
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

func TestCompatLogoutEndsTheLineageOfAGoAPIRefreshToken(t *testing.T) {
	f := newCompatFixture(t)
	response := f.do(http.MethodPost, PatternCompatLogout, `{"refresh_token":"`+legacyToken(t, f, f.org.String(), testSecret)+`"}`, nil)
	if response.Code != http.StatusOK {
		t.Fatalf("status %d: %s", response.Code, response.Body)
	}
	if len(f.sessions.revokedLegacy) != 1 {
		t.Fatalf("%d legacy revocations, want 1", len(f.sessions.revokedLegacy))
	}
	got := f.sessions.revokedLegacy[0]
	if got.PrincipalID != f.principal || got.OrganizationID == nil || *got.OrganizationID != f.org || got.JTI.Reveal() != "legacy-jti-0001" {
		t.Fatalf("revoked %+v", got)
	}
	if len(f.sessions.byCredential) != 0 || len(f.projector.userCalls) != 0 {
		t.Fatalf("by credential %v, projected %v; a legacy logout reaches neither", f.sessions.byCredential, f.projector.userCalls)
	}
	if f.sessions.actors[0].RequestID == nil {
		t.Fatal("the revocation carries no request id")
	}
}

func TestCompatLogoutAnswersTheSame200ForAnUnverifiedGoAPIRefreshToken(t *testing.T) {
	for name, token := range map[string]func(*compatFixture) string{
		"another key": func(f *compatFixture) string {
			return legacyToken(t, f, f.org.String(), "another-key-0123456789abcdef-0123456")
		},
		"three dots of nothing": func(*compatFixture) string { return "a.b.c" },
	} {
		f := newCompatFixture(t)
		response := f.do(http.MethodPost, PatternCompatLogout, `{"refresh_token":"`+token(f)+`"}`, nil)
		if response.Code != http.StatusOK {
			t.Fatalf("%s: status %d", name, response.Code)
		}
		if len(f.sessions.revokedLegacy) != 0 || len(f.sessions.byCredential) != 0 {
			t.Fatalf("%s: revoked legacy %d, by credential %d; an unverified token reaches nothing", name, len(f.sessions.revokedLegacy), len(f.sessions.byCredential))
		}
	}
}

func TestCompatLogoutOfAGoAPIRefreshTokenFailsLikeTheOpaqueCase(t *testing.T) {
	f := newCompatFixture(t)
	f.sessions.revokeLegacyErr = errors.New("store down")
	response := f.do(http.MethodPost, PatternCompatLogout, `{"refresh_token":"`+legacyToken(t, f, f.org.String(), testSecret)+`"}`, nil)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("a store failure answered %d", response.Code)
	}
}
