package authapi

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

// A sign-in whose organization copy meets a slug another auth organization holds is a 409 organization_conflict on
// the live plane (the compatibility login, go-api's detail shape) and on the issue plane (error.v1), logged at ERROR
// with that reason; it creates no session. Any other projection failure keeps its answer (500 and 503).
func TestAnOrganizationSlugClashIsAConflict(t *testing.T) {
	t.Parallel()
	clash := fmt.Errorf("identity: insert organization: %w", identity.ErrOrganizationSlugCollision)

	f := newCompatFixture(t)
	f.projector.orgErr = clash
	login := f.do(http.MethodPost, PatternCompatLogin, loginBody(compatEmail, compatPassword, f.org.String()), nil)
	if login.Code != http.StatusConflict || !strings.Contains(login.Body.String(), organizationConflictText) {
		t.Fatalf("login with a slug clash = %d %s, want 409 %q", login.Code, login.Body, organizationConflictText)
	}
	wantConflictLogged(t, f, "login")

	f = newCompatFixture(t)
	f.projector.orgErr = clash
	issue := f.do(http.MethodPost, PatternIssueSession, issueBody(f.principal, f.org.String(), "pwd"), issuerBearer())
	expectError(t, issue, http.StatusConflict, reasonOrganizationConflict)
	wantConflictLogged(t, f, "issue")

	f = newCompatFixture(t)
	f.projector.orgErr = errors.New("projection store down")
	expectError(t, f.do(http.MethodPost, PatternIssueSession, issueBody(f.principal, f.org.String(), "pwd"), issuerBearer()),
		http.StatusServiceUnavailable, reasonDependency)
	f = newCompatFixture(t)
	f.projector.orgErr = errors.New("projection store down")
	if other := f.do(http.MethodPost, PatternCompatLogin, loginBody(compatEmail, compatPassword, f.org.String()), nil); other.Code != http.StatusInternalServerError {
		t.Fatalf("login with another projection failure = %d %s, want 500", other.Code, other.Body)
	}
}

func wantConflictLogged(t *testing.T, f *compatFixture, plane string) {
	t.Helper()
	if len(f.sessions.created) != 0 {
		t.Fatalf("%s: a slug clash created %d sessions", plane, len(f.sessions.created))
	}
	logs := f.logs.String()
	if !strings.Contains(logs, `"level":"ERROR"`) || !strings.Contains(logs, `"reason_code":"organization_conflict"`) {
		t.Fatalf("%s: want an ERROR line with reason organization_conflict, logs:\n%s", plane, logs)
	}
}
