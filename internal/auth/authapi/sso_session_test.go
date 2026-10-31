package authapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/oidc"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/saml"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/ssoaudit"
)

func (f *compatFixture) ssoSessions(t *testing.T) SSOSessions {
	t.Helper()
	sessions, err := NewSSOSessions(CompatDeps{
		Sessions: f.sessions, Users: f.users, Issuer: f.issuer,
		Logger:    slog.New(slog.NewJSONHandler(f.logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Projector: f.projector,
		AppendAudit: func(_ context.Context, event audit.AuditEvent) error {
			f.appended = append(f.appended, event)
			return nil
		},
	}, &f.signIns)
	if err != nil {
		t.Fatal(err)
	}
	return sessions
}

// signInSpy records each call and how many sessions existed when it ran.
type signInSpy struct {
	got      []ssoaudit.Success
	err      error
	sessions func() int
	seen     int
}

func (s *signInSpy) Succeeded(_ context.Context, success ssoaudit.Success) error {
	s.got = append(s.got, success)
	if s.sessions != nil {
		s.seen = s.sessions()
	}
	return s.err
}

func TestSSOSessionsRequireTheirDependencies(t *testing.T) {
	if _, err := NewSSOSessions(CompatDeps{}, &signInSpy{}); err == nil {
		t.Fatal("sso sessions were built without dependencies")
	}
	f := newCompatFixture(t)
	if _, err := NewSSOSessions(CompatDeps{Sessions: f.sessions, Users: f.users, Issuer: f.issuer, Projector: f.projector,
		AppendAudit: func(context.Context, audit.AuditEvent) error { return nil }}, nil); err == nil {
		t.Fatal("sso sessions were built without a sign-in record")
	}
}

// A completed SAML sign-in writes one record, with its session already there, naming the provider, its organization
// and the saml protocol; the answer carries the tokens.
func TestSAMLSessionRecordsTheSuccessBeforeItAnswers(t *testing.T) {
	f := newCompatFixture(t)
	f.signIns.sessions = func() int { return len(f.sessions.created) }
	provider := uuid.New()
	w, r := samlRequest()
	f.ssoSessions(t).SAML().IssueFederatedSession(w, r, saml.SignIn{PrincipalID: f.principal, OrganizationID: &f.org, ProviderID: provider, AMR: []string{"fed"}})
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "access_token") {
		t.Fatalf("status %d: %s", w.Code, w.Body)
	}
	want := []ssoaudit.Success{{OrgID: f.org, ProviderID: provider, PrincipalID: f.principal, Protocol: "saml"}}
	if !reflect.DeepEqual(f.signIns.got, want) {
		t.Fatalf("records %+v, want %+v", f.signIns.got, want)
	}
	if f.signIns.seen != 1 {
		t.Fatalf("%d sessions when the record ran, want 1", f.signIns.seen)
	}
}

// A record that cannot be written is the 500: no token reaches the answer.
func TestSAMLSessionRecordFailureWithholdsTheTokens(t *testing.T) {
	f := newCompatFixture(t)
	f.signIns.err = errors.New("permission denied")
	w, r := samlRequest()
	f.ssoSessions(t).SAML().IssueFederatedSession(w, r, saml.SignIn{PrincipalID: f.principal, OrganizationID: &f.org, AMR: []string{"fed"}})
	if w.Code != http.StatusInternalServerError || strings.Contains(w.Body.String(), "token") {
		t.Fatalf("status %d: %s", w.Code, w.Body)
	}
}

func TestSAMLSessionWithoutAnOrganizationWithholdsTheTokens(t *testing.T) {
	f := newCompatFixture(t)
	w, r := samlRequest()
	f.ssoSessions(t).SAML().IssueFederatedSession(w, r, saml.SignIn{PrincipalID: f.principal, AMR: []string{"fed"}})
	if w.Code != http.StatusInternalServerError || strings.Contains(w.Body.String(), "token") || len(f.signIns.got) != 0 {
		t.Fatalf("status %d, records %d: %s", w.Code, len(f.signIns.got), w.Body)
	}
}

// A sign-in that started no session writes no record.
func TestSAMLSessionThatStartedNothingRecordsNothing(t *testing.T) {
	t.Run("no public.users row", func(t *testing.T) {
		f := newCompatFixture(t)
		ghost := uuid.New()
		member := "member"
		f.users.roles[[2]uuid.UUID{ghost, f.org}] = &member
		w, r := samlRequest()
		f.ssoSessions(t).SAML().IssueFederatedSession(w, r, saml.SignIn{PrincipalID: ghost, OrganizationID: &f.org})
		if w.Code != http.StatusInternalServerError || len(f.sessions.created) != 0 || len(f.signIns.got) != 0 {
			t.Fatalf("status %d, sessions %d, records %d", w.Code, len(f.sessions.created), len(f.signIns.got))
		}
	})
	t.Run("membership gone", func(t *testing.T) {
		f := newCompatFixture(t)
		other := uuid.New()
		w, r := samlRequest()
		f.ssoSessions(t).SAML().IssueFederatedSession(w, r, saml.SignIn{PrincipalID: f.principal, OrganizationID: &other})
		if w.Code != http.StatusInternalServerError || len(f.signIns.got) != 0 {
			t.Fatalf("status %d, records %d", w.Code, len(f.signIns.got))
		}
	})
}

func TestOIDCSessionStartsAFederatedSessionInTheSignInOrganization(t *testing.T) {
	f := newCompatFixture(t)
	issued, err := f.ssoSessions(t).OIDC().Issue(context.Background(), oidc.SessionRequest{
		PrincipalID: f.principal, OrgID: &f.org, Role: "owner", AMR: []string{"fed"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if issued.ExpiresIn != int64(AccessExpiresIn) || issued.RefreshToken == "" {
		t.Fatalf("tokens %+v", issued)
	}
	bearer, _, err := f.issuer.Verify(issued.AccessToken)
	if err != nil || bearer.PrincipalID != f.principal {
		t.Fatalf("access token: %+v, %v", bearer, err)
	}
	created := f.sessions.created[0]
	if !reflect.DeepEqual(created.amr, []string{"fed"}) || *created.org != f.org ||
		created.event.EventType != EventLoginSucceeded || created.event.Attributes["provider"] != "sso" {
		t.Fatalf("session %+v", created)
	}
}

func TestOIDCSessionOfAnUnknownUserStartsNothing(t *testing.T) {
	f := newCompatFixture(t)
	_, err := f.ssoSessions(t).OIDC().Issue(context.Background(), oidc.SessionRequest{
		PrincipalID: uuid.New(), OrgID: &f.org, Role: "member", AMR: []string{"fed"},
	})
	if err == nil || len(f.sessions.created) != 0 {
		t.Fatalf("err %v, sessions %d: a session started for a user with no row", err, len(f.sessions.created))
	}
}

func samlRequest() (*httptest.ResponseRecorder, *http.Request) {
	return httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/api/v1/auth/saml/x/acs", strings.NewReader(""))
}

func TestSAMLSessionAnswersTheLoginModelForTheMembersRole(t *testing.T) {
	f := newCompatFixture(t)
	w, r := samlRequest()
	f.ssoSessions(t).SAML().IssueFederatedSession(w, r, saml.SignIn{PrincipalID: f.principal, OrganizationID: &f.org, AMR: []string{"fed"}})
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body)
	}
	body := decodeObject(t, w.Body.Bytes())
	if body["token_type"] != "bearer" || body["org_id"] != f.org.String() || body["role"] != "admin" ||
		body["user_id"] != f.principal.String() || body["access_token"] == "" || body["refresh_token"] == "" || body["email"] == "" {
		t.Fatalf("body keys %v", body)
	}
	if w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("the session answer is cacheable")
	}
	created := f.sessions.created[0]
	if *created.org != f.org || created.event.Attributes["provider"] != "saml" {
		t.Fatalf("session %+v", created)
	}
}

func TestSAMLSessionWithoutAMembershipStartsNothing(t *testing.T) {
	f := newCompatFixture(t)
	other := uuid.New()
	w, r := samlRequest()
	f.ssoSessions(t).SAML().IssueFederatedSession(w, r, saml.SignIn{PrincipalID: f.principal, OrganizationID: &other})
	if w.Code != http.StatusInternalServerError || len(f.sessions.created) != 0 {
		t.Fatalf("status %d, sessions %d", w.Code, len(f.sessions.created))
	}
	if strings.Contains(w.Body.String(), f.principal.String()) || strings.Contains(f.logs.String(), f.principal.String()) {
		t.Fatal("an account id reached the answer or the log")
	}
}
