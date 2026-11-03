package sessionissue_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/workloadtoken"

	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/redirectprobe"
)

const workloadIssuer = "https://auth.example.test"

// mintWorkloadToken is a workload-token.v1 access token for auth-service, valid now for 300 s (the client checks its
// claims, not its signature).
func mintWorkloadToken(t *testing.T) string {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	enc := base64.RawURLEncoding
	now := time.Now().Unix()
	payload := fmt.Sprintf(`{"iss":%q,"aud":"auth-service","sub":"prn_AAAAAAAAAAAAAAAAAAAAAA","principal_type":"workload","jti":"%024d","iat":%d,"nbf":%d,"exp":%d}`,
		workloadIssuer, 11, now, now, now+300)
	input := enc.EncodeToString([]byte(`{"alg":"EdDSA","typ":"workload+jwt","kid":"test-key"}`)) + "." + enc.EncodeToString([]byte(payload))
	return input + "." + enc.EncodeToString(ed25519.Sign(key, []byte(input)))
}

func issuedAnswer(token string) func() (int, string) {
	return func() (int, string) {
		body, _ := json.Marshal(map[string]any{"access_token": token, "issued_token_type": workloadtoken.TokenTypeJWT, "token_type": "Bearer", "expires_in": 300})
		return http.StatusOK, string(body)
	}
}

// newWorkloadClient is a workloadtoken.Client over a bootstrap credential file that exchanges at s.
func newWorkloadClient(t *testing.T, s *issueServer) *workloadtoken.Client {
	t.Helper()
	random := make([]byte, 32)
	if _, err := rand.Read(random); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "bootstrap")
	if err := os.WriteFile(path, []byte(workloadtoken.BootstrapPrefix+base64.RawURLEncoding.EncodeToString(random)), 0o600); err != nil {
		t.Fatal(err)
	}
	client, err := workloadtoken.New(workloadtoken.Config{ExchangeURL: s.URL.String(), AllowPlainHTTP: true, Issuer: workloadIssuer,
		Subject: workloadtoken.BootstrapCredentialFile(path), Transport: &http.Transport{}})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// With the session credential every issue carries exactly the AUTH_SESSION_ISSUE_TOKEN value as its one Bearer.
func TestSessionIssueSendsExactlyTheSessionCredential(t *testing.T) {
	s := newIssueServer(t)
	client, err := sessionissue.New(s.URL, goldenToken, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	runIssueCases(t, s, client)
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.requests) != len(issueCases()) {
		t.Fatalf("%d requests, want %d", len(s.requests), len(issueCases()))
	}
	for i, r := range s.requests {
		if len(r.Auth) != 1 || r.Auth[0] != "Bearer "+goldenToken {
			t.Errorf("request %d: Authorization is not exactly the session credential", i)
		}
	}
	if s.exchanges != 0 {
		t.Errorf("%d exchanges, want 0", s.exchanges)
	}
}

// With the workload credential every issue carries exactly one Bearer, the workload token, and no byte of a session
// credential; one exchange serves every issue of the token's lifetime.
func TestWorkloadIssueCarriesOnlyTheWorkloadToken(t *testing.T) {
	s := newIssueServer(t)
	token := mintWorkloadToken(t)
	s.exchange = issuedAnswer(token)
	client, err := sessionissue.NewWorkload(s.URL, newWorkloadClient(t, s), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for round := 0; round < 3; round++ {
		runIssueCases(t, s, client)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if want := 3 * len(issueCases()); len(s.requests) != want {
		t.Fatalf("%d issue requests, want %d", len(s.requests), want)
	}
	for i, r := range s.requests {
		if len(r.Auth) != 1 || r.Auth[0] != "Bearer "+token {
			t.Errorf("request %d: Authorization is not exactly the workload token", i)
		}
		if strings.Contains(r.Dump, "svc_authsess_") {
			t.Errorf("request %d holds svc_authsess_ bytes", i)
		}
	}
	if s.exchanges != 1 {
		t.Errorf("%d exchanges for %d issues, want 1", s.exchanges, len(s.requests))
	}
}

// The body, path and every header but the bearer are what the session credential sends.
func TestWorkloadIssueRequestsDifferFromTheGoldenOnlyInTheBearer(t *testing.T) {
	s := newIssueServer(t)
	s.exchange = issuedAnswer(mintWorkloadToken(t))
	client, err := sessionissue.NewWorkload(s.URL, newWorkloadClient(t, s), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var want []goldenRequest
	raw, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatal(err)
	}
	got := runIssueCases(t, s, client)
	for i := range got {
		got[i].Headers["Authorization"] = want[i].Headers["Authorization"]
	}
	gotJSON, _ := json.Marshal(got)
	wantJSON, _ := json.Marshal(want)
	if string(gotJSON) != string(wantJSON) {
		t.Fatalf("workload issue requests differ from the golden beyond the bearer\n got %s\nwant %s", gotJSON, wantJSON)
	}
}

// A failed exchange fails the issue as unavailable and sends nothing to the issue route: no session credential is held.
func TestWorkloadIssueExchangeFailureSendsNoRequest(t *testing.T) {
	s := newIssueServer(t)
	s.exchange = func() (int, string) { return http.StatusServiceUnavailable, `{"error":"temporarily_unavailable"}` }
	client, err := sessionissue.NewWorkload(s.URL, newWorkloadClient(t, s), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range issueCases() {
		_, err := client.Issue(context.Background(), endUserRequest(), c.req)
		if !errors.Is(err, sessionissue.ErrUnavailable) {
			t.Errorf("%s: error %v, want unavailable", c.name, err)
		}
		if err != nil && (strings.Contains(err.Error(), "Bearer") || strings.Contains(err.Error(), "svc_authsess_")) {
			t.Errorf("%s: the error carries a credential: %v", c.name, err)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.requests) != 0 {
		t.Fatalf("%d issue requests after a failed exchange, want 0", len(s.requests))
	}
}

func TestNewWorkloadRefusesNoClientAndNoBase(t *testing.T) {
	s := newIssueServer(t)
	if _, err := sessionissue.NewWorkload(s.URL, nil, time.Second); err == nil {
		t.Error("NewWorkload accepted no workload client")
	}
	if _, err := sessionissue.NewWorkload(nil, newWorkloadClient(t, s), time.Second); err == nil {
		t.Error("NewWorkload accepted no URL")
	}
}

// The workload issue client answers a redirect without following it: the second origin sees no request.
func TestWorkloadIssueNeverFollowsARedirectToAnotherOrigin(t *testing.T) {
	probe := redirectprobe.New(t)
	exchange := newIssueServer(t)
	exchange.exchange = issuedAnswer(mintWorkloadToken(t))
	client, err := sessionissue.NewWorkload(mustURL(t, probe.Base.URL), newWorkloadClient(t, exchange), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Issue(context.Background(), endUserRequest(), issueCases()[0].req); err == nil {
		t.Fatal("a redirect answer was taken for a session")
	}
	probe.Assert(t)
}

func mustURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u
}
