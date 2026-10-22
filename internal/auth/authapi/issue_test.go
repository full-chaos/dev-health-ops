package authapi

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/servicecred"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// fakeServiceCredentials accepts the token "good" unless refusal or err is set.
type fakeServiceCredentials struct {
	id      string
	refusal servicecred.Refusal
	err     error
	seen    []string
}

func (f *fakeServiceCredentials) Check(_ context.Context, token contracts.SecretString) (servicecred.Credential, servicecred.Refusal, error) {
	f.seen = append(f.seen, token.Reveal())
	if f.err != nil {
		return servicecred.Credential{}, "", f.err
	}
	if f.refusal != "" {
		return servicecred.Credential{}, f.refusal, nil
	}
	return servicecred.Credential{ID: f.id}, "", nil
}

var issuerBearer = map[string]string{"Authorization": "Bearer good"}

func issueBody(principal uuid.UUID, org string, amr string) string {
	orgJSON := "null"
	if org != "" {
		orgJSON = `"` + org + `"`
	}
	return `{"principal_id":"` + principal.String() + `","organization_id":` + orgJSON + `,"amr":["` + amr + `"]}`
}

func TestIssueSessionCreatesAnAuditedSessionAndAnswersThePair(t *testing.T) {
	f := newCompatFixture(t)
	response := f.do(http.MethodPost, PatternIssueSession, issueBody(f.principal, f.org.String(), "pwd"), issuerBearer)
	if response.Code != http.StatusOK {
		t.Fatalf("status %d: %s", response.Code, response.Body)
	}
	if len(f.sessions.created) != 1 {
		t.Fatalf("%d sessions created", len(f.sessions.created))
	}
	created := f.sessions.created[0]
	if created.principal != f.principal || created.org == nil || *created.org != f.org || strings.Join(created.amr, ",") != "pwd" {
		t.Fatalf("created %+v", created)
	}
	event := created.event
	if event.EventType != EventSessionIssued || event.SubjectPrincipalID == nil || *event.SubjectPrincipalID != f.principal.String() ||
		event.OrganizationID == nil || *event.OrganizationID != f.org.String() || event.RequestID == nil {
		t.Fatalf("audit event %+v", event)
	}
	attrs := event.Attributes
	if attrs["reason_code"] != "accepted" || attrs["target_kind"] != "session" || attrs["source"] != "go-api-delegated" ||
		attrs["credential_id"] != f.creds.id || strings.Join(attrs["amr"].([]string), ",") != "pwd" {
		t.Fatalf("audit attributes %v", attrs)
	}
	body := decodeObject(t, response.Body.Bytes())
	if body["refresh_token"] != created.issued.Credential.Reveal() || body["token_type"] != "bearer" || body["expires_in"] != float64(AccessExpiresIn) {
		t.Fatalf("body %s", response.Body)
	}
	bearer, _, err := f.issuer.Verify(body["access_token"].(string))
	if err != nil || bearer.SessionID != created.issued.Session.ID || bearer.PrincipalID != f.principal {
		t.Fatalf("access token %+v %v, want the issued session", bearer, err)
	}
	if response.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("a token answer is cacheable")
	}
	if len(f.projector.userCalls) != 1 || len(f.projector.orgCalls) != 1 {
		t.Fatalf("projected users %v orgs %v", f.projector.userCalls, f.projector.orgCalls)
	}
}

func TestIssueSessionWithoutAnOrganization(t *testing.T) {
	f := newCompatFixture(t)
	response := f.do(http.MethodPost, PatternIssueSession, issueBody(f.principal, "", "fed"), issuerBearer)
	if response.Code != http.StatusOK || len(f.sessions.created) != 1 || f.sessions.created[0].org != nil ||
		strings.Join(f.sessions.created[0].amr, ",") != "fed" || f.sessions.created[0].event.OrganizationID != nil {
		t.Fatalf("status %d created %+v: %s", response.Code, f.sessions.created, response.Body)
	}
}

func TestIssueSessionRefusesAndCreatesNothing(t *testing.T) {
	stranger := uuid.New()
	for name, tc := range map[string]struct {
		mutate func(*compatFixture)
		header map[string]string
		body   func(*compatFixture) string
		status int
		reason string
	}{
		"no bearer":             {header: map[string]string{}, status: http.StatusUnauthorized, reason: "invalid_credentials"},
		"basic auth":            {header: map[string]string{"Authorization": "Basic Z29vZA=="}, status: http.StatusUnauthorized, reason: "invalid_credentials"},
		"refused credential":    {mutate: func(f *compatFixture) { f.creds.refusal = servicecred.RefusedRevoked }, status: http.StatusUnauthorized, reason: "invalid_credentials"},
		"credential store down": {mutate: func(f *compatFixture) { f.creds.err = errors.New("db down") }, status: http.StatusServiceUnavailable, reason: "store_unavailable"},
		"unknown user":          {body: func(*compatFixture) string { return issueBody(stranger, "", "pwd") }, status: http.StatusForbidden, reason: "principal_not_found"},
		"inactive user": {mutate: func(f *compatFixture) {
			user := f.users.users[f.principal]
			user.IsActive = false
			f.users.users[f.principal] = user
		}, status: http.StatusForbidden, reason: "principal_deactivated"},
		"not a member": {body: func(f *compatFixture) string { return issueBody(f.principal, uuid.NewString(), "pwd") }, status: http.StatusForbidden, reason: "not_entitled"},
		"extra member": {body: func(f *compatFixture) string {
			return strings.TrimSuffix(issueBody(f.principal, "", "pwd"), "}") + `,"role":"owner"}`
		}, status: http.StatusUnauthorized, reason: "malformed"},
		"no org member": {body: func(f *compatFixture) string {
			return `{"principal_id":"` + f.principal.String() + `","amr":["pwd"],"x":1}`
		}, status: http.StatusUnauthorized, reason: "malformed"},
		"bad uuid": {body: func(*compatFixture) string { return `{"principal_id":"alice","organization_id":null,"amr":["pwd"]}` }, status: http.StatusUnauthorized, reason: "malformed"},
		"upper uuid": {body: func(f *compatFixture) string {
			return `{"principal_id":"` + strings.ToUpper(f.principal.String()) + `","organization_id":null,"amr":["pwd"]}`
		}, status: http.StatusUnauthorized, reason: "malformed"},
		"empty amr": {body: func(f *compatFixture) string {
			return strings.Replace(issueBody(f.principal, "", "pwd"), `["pwd"]`, `[]`, 1)
		}, status: http.StatusUnauthorized, reason: "malformed"},
		"two documents": {body: func(f *compatFixture) string { return issueBody(f.principal, "", "pwd") + "{}" }, status: http.StatusUnauthorized, reason: "malformed"},
		"duplicate member": {body: func(f *compatFixture) string {
			return `{"principal_id":"` + f.principal.String() + `","organization_id":null,"amr":["pwd"],"amr":["fed"]}`
		}, status: http.StatusUnauthorized, reason: "malformed"},
		"duplicate principal": {body: func(f *compatFixture) string {
			return `{"principal_id":"` + stranger.String() + `","principal_id":"` + f.principal.String() + `","organization_id":null,"amr":["pwd"]}`
		}, status: http.StatusUnauthorized, reason: "malformed"},
		"unknown amr": {body: func(f *compatFixture) string { return issueBody(f.principal, "", "password") }, status: http.StatusUnauthorized, reason: "malformed"},
		"repeated amr": {body: func(f *compatFixture) string {
			return strings.Replace(issueBody(f.principal, "", "pwd"), `["pwd"]`, `["pwd","pwd"]`, 1)
		}, status: http.StatusUnauthorized, reason: "malformed"},
	} {
		f := newCompatFixture(t)
		if tc.mutate != nil {
			tc.mutate(f)
		}
		header := issuerBearer
		if tc.header != nil {
			header = tc.header
		}
		body := issueBody(f.principal, "", "pwd")
		if tc.body != nil {
			body = tc.body(f)
		}
		response := f.do(http.MethodPost, PatternIssueSession, body, header)
		expectError(t, response, tc.status, tc.reason)
		if len(f.sessions.created) != 0 {
			t.Fatalf("%s: a refused issue created %d sessions", name, len(f.sessions.created))
		}
		if tc.status == http.StatusUnauthorized && (len(f.projector.userCalls) != 0 || len(f.projector.orgCalls) != 0) {
			t.Fatalf("%s: a refused request projected users %v orgs %v", name, f.projector.userCalls, f.projector.orgCalls)
		}
	}
}

func TestIssueSessionMapsAnInvalidAMRAndAStoreFailure(t *testing.T) {
	f := newCompatFixture(t)
	f.sessions.createErr = session.ErrInvalidAMR
	expectError(t, f.do(http.MethodPost, PatternIssueSession, issueBody(f.principal, "", "pwd"), issuerBearer), http.StatusUnauthorized, "malformed")
	f = newCompatFixture(t)
	f.sessions.createErr = errors.Join(session.ErrStore, errors.New("down"))
	expectError(t, f.do(http.MethodPost, PatternIssueSession, issueBody(f.principal, "", "pwd"), issuerBearer), http.StatusServiceUnavailable, "store_unavailable")
}

func TestIssueSessionIsMountedOnlyWithServiceCredentials(t *testing.T) {
	f := newCompatFixture(t)
	f.creds = nil
	f.buildCompat()
	if response := f.do(http.MethodPost, PatternIssueSession, issueBody(f.principal, "", "pwd"), issuerBearer); response.Code != http.StatusNotFound {
		t.Fatalf("without service credentials the route answers %d, want 404", response.Code)
	}
}
