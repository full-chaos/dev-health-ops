package authapi

import (
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

func issueBodyWithPredecessor(principal uuid.UUID, org string, predecessor string) string {
	return strings.TrimSuffix(issueBody(principal, org, "pwd"), "}") + `,"predecessor_session_id":` + predecessor + `}`
}

// A body that names predecessor_session_id supersedes that session
// (CHAOS-12085): the route calls CreateSupersedingAudited with it and the same
// issued event, and answers the pair. A body without it never reaches the
// superseding path.
func TestIssueSessionWithAPredecessorSupersedesIt(t *testing.T) {
	f := newCompatFixture(t)
	predecessor := uuid.New()
	response := f.do(http.MethodPost, PatternIssueSession, issueBodyWithPredecessor(f.principal, f.org.String(), `"`+predecessor.String()+`"`), issuerBearer)
	if response.Code != http.StatusOK {
		t.Fatalf("status %d: %s", response.Code, response.Body)
	}
	if len(f.sessions.created) != 1 {
		t.Fatalf("%d sessions created", len(f.sessions.created))
	}
	created := f.sessions.created[0]
	if created.predecessor == nil || *created.predecessor != predecessor || created.org == nil || *created.org != f.org {
		t.Fatalf("created %+v, want the predecessor %s superseded", created, predecessor)
	}
	if created.event.EventType != EventSessionIssued {
		t.Fatalf("event %+v", created.event)
	}
	g := newCompatFixture(t)
	if response := g.do(http.MethodPost, PatternIssueSession, issueBody(g.principal, g.org.String(), "pwd"), issuerBearer); response.Code != http.StatusOK ||
		len(g.sessions.created) != 1 || g.sessions.created[0].predecessor != nil {
		t.Fatalf("a body without a predecessor: status %d created %+v", response.Code, g.sessions.created)
	}
}

func TestIssueSessionRefusesAMalformedPredecessor(t *testing.T) {
	for name, value := range map[string]string{
		"null":         `null`,
		"empty":        `""`,
		"not a uuid":   `"ses_abc"`,
		"upper case":   `"` + strings.ToUpper(uuid.NewString()) + `"`,
		"a number":     `7`,
		"an object":    `{}`,
		"wire form id": `"ses_` + strings.Repeat("A", 22) + `"`,
	} {
		f := newCompatFixture(t)
		expectError(t, f.do(http.MethodPost, PatternIssueSession, issueBodyWithPredecessor(f.principal, "", value), issuerBearer),
			http.StatusUnauthorized, "malformed")
		if len(f.sessions.created) != 0 {
			t.Fatalf("%s: a malformed predecessor created a session", name)
		}
	}
	f := newCompatFixture(t)
	body := strings.TrimSuffix(issueBodyWithPredecessor(f.principal, "", `"`+uuid.NewString()+`"`), "}") + `,"x":1}`
	expectError(t, f.do(http.MethodPost, PatternIssueSession, body, issuerBearer), http.StatusUnauthorized, "malformed")
	body = `{"principal_id":"` + f.principal.String() + `","amr":["pwd"],"predecessor_session_id":"` + uuid.NewString() + `","x":1}`
	expectError(t, f.do(http.MethodPost, PatternIssueSession, body, issuerBearer), http.StatusUnauthorized, "malformed")
	if len(f.sessions.created) != 0 {
		t.Fatal("a fifth member or a missing organization_id created a session")
	}
}

// The store's refusals: an ended predecessor is 401 session_revoked (go-api
// maps it to a 401, not a 503), another principal's is 403 not_entitled.
func TestIssueSessionMapsThePredecessorRefusals(t *testing.T) {
	for _, tc := range []struct {
		err    error
		status int
		reason string
	}{
		{session.ErrPredecessorNotLive, http.StatusUnauthorized, "session_revoked"},
		{session.ErrPredecessorOtherPrincipal, http.StatusForbidden, "not_entitled"},
		{errors.Join(session.ErrStore, errors.New("down")), http.StatusServiceUnavailable, "store_unavailable"},
	} {
		f := newCompatFixture(t)
		f.sessions.createErr = tc.err
		expectError(t, f.do(http.MethodPost, PatternIssueSession, issueBodyWithPredecessor(f.principal, "", `"`+uuid.NewString()+`"`), issuerBearer),
			tc.status, tc.reason)
	}
}
