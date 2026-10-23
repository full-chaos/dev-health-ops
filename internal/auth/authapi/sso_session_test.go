package authapi

import (
	"context"
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
	})
	if err != nil {
		t.Fatal(err)
	}
	return sessions
}

func TestSSOSessionsRequireTheirDependencies(t *testing.T) {
	if _, err := NewSSOSessions(CompatDeps{}); err == nil {
		t.Fatal("sso sessions were built without dependencies")
	}
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
	if err := audit.ValidateAttributes(created.event.Attributes); err != nil {
		t.Fatalf("the sign-in audit attributes are outside the allowlist: %v", err)
	}
	if !reflect.DeepEqual(created.amr, []string{"fed"}) || *created.org != f.org ||
		created.event.EventType != EventLoginSucceeded || created.event.Attributes["provider"] != "oidc" {
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
	if err := audit.ValidateAttributes(created.event.Attributes); err != nil {
		t.Fatalf("the sign-in audit attributes are outside the allowlist: %v", err)
	}
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
