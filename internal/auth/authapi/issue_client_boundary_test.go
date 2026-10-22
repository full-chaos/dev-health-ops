package authapi

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
	"github.com/full-chaos/dev-health-ops/internal/auth/servicecred"
)

// go-api's session issue client against this route, in process: what the
// client sends is what the route accepts, and every refusal reaches go-api as
// ErrUnavailable (its 503).
func TestGoAPIsIssueClientAgainstTheIssueRoute(t *testing.T) {
	random := make([]byte, 32)
	if _, err := rand.Read(random); err != nil {
		t.Fatal(err)
	}
	token := "svc_authsess_" + base64.RawURLEncoding.EncodeToString(random)
	endUser := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", nil)
	endUser.RemoteAddr = "198.51.100.30:5000"
	endUser.Header.Set("User-Agent", "boundary-test/1.0")
	issue := func(t *testing.T, f *compatFixture, amr string) (sessionissue.Pair, error) {
		t.Helper()
		server := httptest.NewServer(f.server)
		t.Cleanup(server.Close)
		base, err := url.Parse(server.URL)
		if err != nil {
			t.Fatal(err)
		}
		client, err := sessionissue.New(base, token, 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		org := f.org
		return client.Issue(context.Background(), endUser, sessionissue.Request{PrincipalID: f.principal, OrganizationID: &org, AMR: []string{amr}})
	}

	f := newCompatFixture(t)
	pair, err := issue(t, f, "pwd")
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	if len(f.sessions.created) != 1 || pair.Refresh != f.sessions.created[0].issued.Credential.Reveal() {
		t.Fatalf("pair %+v does not carry the created session's credential", pair)
	}
	bearer, _, err := f.issuer.Verify(pair.Access)
	if err != nil || bearer.SessionID != f.sessions.created[0].issued.Session.ID || bearer.PrincipalID != f.principal {
		t.Fatalf("access token %+v %v, want the created session", bearer, err)
	}
	if len(f.creds.seen) != 1 || f.creds.seen[0] != token {
		t.Fatalf("the route checked %v, want the client's credential", f.creds.seen)
	}

	for name, setup := range map[string]func(*compatFixture) string{
		"refused credential":        func(f *compatFixture) string { f.creds.refusal = servicecred.RefusedRevoked; return "pwd" },
		"request the route refuses": func(*compatFixture) string { return "password" },
		"route not mounted": func(f *compatFixture) string {
			f.creds = nil
			f.buildCompat()
			return "pwd"
		},
	} {
		f := newCompatFixture(t)
		amr := setup(f)
		pair, err := issue(t, f, amr)
		if !errors.Is(err, sessionissue.ErrUnavailable) || pair.Access != "" || len(f.sessions.created) != 0 {
			t.Fatalf("%s: pair %+v err %v created %d; want ErrUnavailable and nothing created", name, pair, err, len(f.sessions.created))
		}
	}
}
