package authapi

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/workloadtoken"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// go-api's session issue client against this route, in process: what the
// client sends (the go-api workload token it took from its exchange) is what
// the route accepts, and every refusal reaches go-api as ErrUnavailable (its
// 503).
func TestGoAPIsIssueClientAgainstTheIssueRoute(t *testing.T) {
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
		client, err := sessionissue.NewWorkload(base, exchangeAnswering(t, strings.TrimPrefix(issuerBearer()["Authorization"], "Bearer ")), 2*time.Second)
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
	if f.sessions.signIns != 0 {
		t.Fatalf("the issue route made %d sign-ins; it must not stamp a last login", f.sessions.signIns)
	}
	if len(f.sessions.created) != 1 || pair.Refresh != f.sessions.created[0].issued.Credential.Reveal() {
		t.Fatalf("pair %+v does not carry the created session's credential", pair)
	}
	bearer, _, err := f.issuer.Verify(pair.Access)
	if err != nil || bearer.SessionID != f.sessions.created[0].issued.Session.ID || bearer.PrincipalID != f.principal {
		t.Fatalf("access token %+v %v, want the created session", bearer, err)
	}
	if actor := f.sessions.created[0].event.ActorPrincipalID; actor == nil || *actor != testIssueSubjectID().String() {
		t.Fatalf("the session was issued to %v, want the go-api workload principal", actor)
	}

	for name, setup := range map[string]func(*compatFixture) string{
		"refused workload token": func(f *compatFixture) string {
			other, err := NewSessionIssueWorkload(SessionIssueWorkloadConfig{IssuerURL: goldenIssuer, Keys: testIssue.keys, Subject: session.WirePrincipalID(uuid.New())})
			if err != nil {
				t.Fatal(err)
			}
			f.workload = other
			f.buildCompat()
			return "pwd"
		},
		"request the route refuses": func(*compatFixture) string { return "password" },
		"route not mounted": func(f *compatFixture) string {
			f.workload = nil
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

// exchangeAnswering is a go-api workload token client whose exchange (a server of its own) always answers token.
func exchangeAnswering(t *testing.T, token string) *workloadtoken.Client {
	t.Helper()
	exchange := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		body, _ := json.Marshal(map[string]any{"access_token": token, "issued_token_type": workloadtoken.TokenTypeJWT, "token_type": "Bearer", "expires_in": 300})
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))
	t.Cleanup(exchange.Close)
	random := make([]byte, 32)
	if _, err := rand.Read(random); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "bootstrap")
	if err := os.WriteFile(path, []byte(workloadtoken.BootstrapPrefix+base64.RawURLEncoding.EncodeToString(random)), 0o600); err != nil {
		t.Fatal(err)
	}
	client, err := workloadtoken.New(workloadtoken.Config{ExchangeURL: exchange.URL, AllowPlainHTTP: true, Issuer: goldenIssuer,
		Subject: workloadtoken.BootstrapCredentialFile(path), Transport: &http.Transport{}})
	if err != nil {
		t.Fatal(err)
	}
	return client
}
