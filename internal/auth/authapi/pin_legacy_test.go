package authapi

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/legacyjwt"
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
	signer, err := legacyjwt.NewSigner(secret, testIssuer, testAudience)
	if err != nil {
		t.Fatal(err)
	}
	token, err := signer.Refresh(legacyjwt.RefreshClaims{UserID: f.principal.String(), OrgID: org, FamilyID: uuid.NewString()},
		time.Now(), "legacy-jti-0001")
	if err != nil {
		t.Fatal(err)
	}
	return token
}

// The HS256 refresh exchange is retired: every go-api refresh JWT, one the legacy key verifies included, gets go-api's
// answer for a refresh token that does not verify, and reaches no store or projector. Logout of a verified one
// answers 200 and revokes nothing (TestCompatLogoutVerifiedLegacyTokenRevokesNothing).
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
		if len(f.sessions.byCredential) != 0 {
			t.Fatalf("%s: by credential %d; an unverified token reaches nothing", name, len(f.sessions.byCredential))
		}
	}
}
