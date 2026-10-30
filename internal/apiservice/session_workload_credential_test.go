package apiservice

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/full-chaos/dev-health-go/workloadtoken"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/redirectprobe"
)

const issueGoldenFile = "../api/sessionissue/testdata/outgoing_issue.golden.json"

// newProvisioningCredential is the provisioning side of newCredentials, for the cells that read only it.
func newProvisioningCredential(cfg AuthProxyConfig, lookup func(string) (string, bool)) (provisioningCredential, error) {
	provisioning, _, err := newCredentials(cfg, lookup)
	return provisioning, err
}

func TestSessionCredentialEnvName(t *testing.T) {
	if EnvAuthSessionIssueCredential != "AUTH_SESSION_ISSUE_CREDENTIAL" {
		t.Errorf("env name %q", EnvAuthSessionIssueCredential)
	}
}

func sessionsOn(t *testing.T, upstream string) AuthProxyConfig {
	t.Helper()
	return mustProxyConfig(t, map[string]string{EnvAuthProxyURL: upstream, AuthProxyFlagEnv(AuthSessions): "true"})
}

func sessionAndProvisioningOn(t *testing.T, upstream string) AuthProxyConfig {
	t.Helper()
	return mustProxyConfig(t, map[string]string{EnvAuthProxyURL: upstream, AuthProxyFlagEnv(AuthSessions): "true", EnvAuthProxyOrgProvisioning: "true"})
}

func workloadEnv(t *testing.T, extra map[string]string) map[string]string {
	t.Helper()
	env := map[string]string{EnvAuthWorkloadIssuer: testWorkloadIssuer, EnvAuthWorkloadBootstrapFile: bootstrapFile(t)}
	for k, v := range extra {
		env[k] = v
	}
	return env
}

// Unset, empty and session_compat build no workload client: today's constructor runs.
func TestSessionCredentialDefaultsToTheSessionCredential(t *testing.T) {
	cfg := sessionsOn(t, "http://auth-service:8095")
	for name, env := range map[string]map[string]string{
		"unset":                {},
		"empty":                {EnvAuthSessionIssueCredential: " "},
		"session_compat":       {EnvAuthSessionIssueCredential: "session_compat"},
		"session_compat+files": {EnvAuthSessionIssueCredential: "session_compat", EnvAuthWorkloadSubjectFile: "/nonexistent", EnvAuthWorkloadIssuer: "x"},
	} {
		_, session, err := newCredentials(cfg, issueEnv(env))
		if err != nil || session.workload != nil {
			t.Errorf("%s: %v %v, want no workload client and no error", name, session.workload, err)
		}
	}
}

// The selector is read only with AUTH_PROXY_SESSIONS on.
func TestSessionCredentialIsNotReadWithSessionsOff(t *testing.T) {
	off := mustProxyConfig(t, map[string]string{EnvAuthProxyURL: "http://auth-service:8095"})
	_, session, err := newCredentials(off, issueEnv(map[string]string{EnvAuthSessionIssueCredential: "workload"}))
	if err != nil || session.workload != nil {
		t.Fatalf("sessions off, selector workload with no inputs: %v %v, want no client and no error", session.workload, err)
	}
}

func TestSessionCredentialWorkloadNeedsItsInputsByName(t *testing.T) {
	cfg := sessionsOn(t, "http://auth-service:8095")
	dir := t.TempDir()
	directory := filepath.Join(dir, "directory-secret-name")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	selector := map[string]string{EnvAuthSessionIssueCredential: "workload"}
	good := workloadEnv(t, selector)
	if _, session, err := newCredentials(cfg, issueEnv(good)); err != nil || session.workload == nil {
		t.Fatalf("bootstrap file: %v %v, want a client", session.workload, err)
	}
	withSubject := workloadEnv(t, map[string]string{EnvAuthSessionIssueCredential: "workload", EnvAuthWorkloadSubjectFile: subjectFile(t)})
	delete(withSubject, EnvAuthWorkloadBootstrapFile)
	if _, session, err := newCredentials(cfg, issueEnv(withSubject)); err != nil || session.workload == nil {
		t.Fatalf("subject file: %v %v, want a client", session.workload, err)
	}
	for name, tc := range map[string]struct {
		extra  map[string]string
		drop   string
		expect string
	}{
		"both subject files":   {map[string]string{EnvAuthWorkloadSubjectFile: subjectFile(t)}, "", EnvAuthWorkloadSubjectFile},
		"no subject file":      {nil, EnvAuthWorkloadBootstrapFile, EnvAuthWorkloadSubjectFile},
		"no issuer":            {nil, EnvAuthWorkloadIssuer, EnvAuthWorkloadIssuer},
		"issuer not https":     {map[string]string{EnvAuthWorkloadIssuer: "http://auth.example.test"}, "", EnvAuthWorkloadIssuer},
		"missing file":         {map[string]string{EnvAuthWorkloadBootstrapFile: filepath.Join(dir, "absent-secret-name")}, "", EnvAuthWorkloadBootstrapFile},
		"directory":            {map[string]string{EnvAuthWorkloadBootstrapFile: directory}, "", EnvAuthWorkloadBootstrapFile},
		"bad selector":         {map[string]string{EnvAuthSessionIssueCredential: "Workload"}, "", EnvAuthSessionIssueCredential},
		"bool selector":        {map[string]string{EnvAuthSessionIssueCredential: "true"}, "", EnvAuthSessionIssueCredential},
		"selector with spaces": {map[string]string{EnvAuthSessionIssueCredential: "workload session_compat"}, "", EnvAuthSessionIssueCredential},
	} {
		env := workloadEnv(t, selector)
		for k, v := range tc.extra {
			env[k] = v
		}
		if tc.drop != "" {
			delete(env, tc.drop)
		}
		_, session, err := newCredentials(cfg, issueEnv(env))
		if err == nil || session.workload != nil {
			t.Errorf("%s: %v %v, want a start-up error and no client", name, session.workload, err)
			continue
		}
		msg := err.Error()
		if !strings.Contains(msg, tc.expect) || !strings.Contains(msg, AuthProxyFlagEnv(AuthSessions)) ||
			strings.Contains(msg, "secret-name") || strings.Contains(msg, dir) {
			t.Errorf("%s: error %q must name %s and the sessions flag and no byte of a path", name, msg, tc.expect)
		}
	}
}

// With the workload credential the session issuer needs no AUTH_SESSION_ISSUE_TOKEN.
func TestSessionIssuerWorkloadNeedsNoSessionToken(t *testing.T) {
	cfg := sessionsOn(t, "http://auth-service:8095")
	_, session, err := newCredentials(cfg, issueEnv(workloadEnv(t, map[string]string{EnvAuthSessionIssueCredential: "workload"})))
	if err != nil {
		t.Fatal(err)
	}
	if issuer, err := sessionIssuer(cfg, issueEnv(map[string]string{}), session); issuer == nil || err != nil {
		t.Fatalf("workload session issuer: %v %v, want one", issuer, err)
	}
	off := mustProxyConfig(t, map[string]string{EnvAuthProxyURL: "http://auth-service:8095"})
	if issuer, err := sessionIssuer(off, issueEnv(map[string]string{}), session); issuer != nil || err != nil {
		t.Fatalf("sessions off: %v %v, want none", issuer, err)
	}
}

// ---- requests through the wiring ----

type credServer struct {
	mu        sync.Mutex
	URL       string
	exchanges int
	exchange  func() (int, string)
	issue     []credRequest
	prov      []credRequest
}

type credRequest struct {
	Case    string              `json:"case"`
	Method  string              `json:"method"`
	Path    string              `json:"path"`
	Headers map[string][]string `json:"headers"`
	Body    string              `json:"body"`
	auth    []string
	dump    string
}

func newCredServer(t *testing.T) *credServer {
	t.Helper()
	s := &credServer{}
	org := "11111111-1111-4111-8111-111111111111"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		s.mu.Lock()
		defer s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == workloadtoken.ExchangePath {
			s.exchanges++
			if s.exchange == nil {
				http.NotFound(w, r)
				return
			}
			status, body := s.exchange()
			w.WriteHeader(status)
			_, _ = io.WriteString(w, body)
			return
		}
		rec := credRequest{Method: r.Method, Path: r.URL.Path, Headers: map[string][]string{}, Body: string(raw), auth: append([]string(nil), r.Header.Values("Authorization")...)}
		var dump strings.Builder
		for name, values := range r.Header {
			rec.Headers[name] = append([]string(nil), values...)
			sort.Strings(rec.Headers[name])
			dump.WriteString(name + ": " + strings.Join(values, ",") + "\n")
			if name == "Authorization" {
				for i, value := range values {
					if strings.HasPrefix(value, "Bearer svc_authsess_") {
						rec.Headers[name][i] = "Bearer svc_authsess_<PLACEHOLDER>"
					} else {
						rec.Headers[name][i] = "Bearer <NOT-svc_authsess_>"
					}
				}
			}
		}
		dump.WriteString(string(raw))
		rec.dump = dump.String()
		if r.URL.Path == sessionissue.Path {
			s.issue = append(s.issue, rec)
			_, _ = io.WriteString(w, `{"access_token":"a.b.c","refresh_token":"dhr1_x","token_type":"bearer","expires_in":3600}`)
			return
		}
		s.prov = append(s.prov, rec)
		_, _ = io.WriteString(w, `{"organization_id":"`+org+`"}`)
	}))
	t.Cleanup(server.Close)
	s.URL = server.URL
	return s
}

func issueRequestFixture() *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", nil)
	r.RemoteAddr = "198.51.100.23:4711"
	r.Header.Set("User-Agent", "Mozilla/5.0 (golden)")
	return r
}

func issueFixtures() []struct {
	name string
	req  sessionissue.Request
} {
	principal := uuid.MustParse("22222222-2222-4222-8222-222222222222")
	org := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	predecessor := uuid.MustParse("55555555-5555-4555-8555-555555555555")
	return []struct {
		name string
		req  sessionissue.Request
	}{
		{"no_org", sessionissue.Request{PrincipalID: principal, AMR: []string{"pwd"}}},
		{"org", sessionissue.Request{PrincipalID: principal, OrganizationID: &org, AMR: []string{"pwd", "otp"}}},
		{"org_predecessor", sessionissue.Request{PrincipalID: principal, OrganizationID: &org, AMR: []string{"pwd"}, PredecessorSessionID: &predecessor}},
	}
}

func (s *credServer) issueAll(t *testing.T, issuer sessionissue.Issuer) {
	t.Helper()
	for _, c := range issueFixtures() {
		if _, err := issuer.Issue(context.Background(), issueRequestFixture(), c.req); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		s.mu.Lock()
		s.issue[len(s.issue)-1].Case = c.name
		s.mu.Unlock()
	}
}

// Unset and session_compat send the exact bytes the golden pins, and exactly the AUTH_SESSION_ISSUE_TOKEN value.
func TestUnsetAndSessionCompatSendTheIssueGolden(t *testing.T) {
	raw, err := os.ReadFile(issueGoldenFile)
	if err != nil {
		t.Fatal(err)
	}
	var want []credRequest
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatal(err)
	}
	for name, selector := range map[string]map[string]string{"unset": {}, "session_compat": {EnvAuthSessionIssueCredential: "session_compat"}} {
		s := newCredServer(t)
		token := testIssueToken(t)
		env := map[string]string{sessionissue.EnvToken: token}
		for k, v := range selector {
			env[k] = v
		}
		cfg := sessionsOn(t, s.URL)
		_, session, err := newCredentials(cfg, issueEnv(env))
		if err != nil {
			t.Fatal(err)
		}
		issuer, err := sessionIssuer(cfg, issueEnv(env), session)
		if err != nil {
			t.Fatal(err)
		}
		s.issueAll(t, issuer)
		s.mu.Lock()
		gotJSON, _ := json.Marshal(s.issue)
		wantJSON, _ := json.Marshal(want)
		if string(gotJSON) != string(wantJSON) {
			t.Errorf("%s: outgoing issue requests differ from the golden\n got %s\nwant %s", name, gotJSON, wantJSON)
		}
		for i, r := range s.issue {
			if len(r.auth) != 1 || r.auth[0] != "Bearer "+token {
				t.Errorf("%s: request %d Authorization is not exactly the AUTH_SESSION_ISSUE_TOKEN value", name, i)
			}
		}
		if s.exchanges != 0 {
			t.Errorf("%s: %d exchanges, want 0", name, s.exchanges)
		}
		s.mu.Unlock()
	}
}

func (s *credServer) tokenAnswer(t *testing.T) string {
	t.Helper()
	token := testWorkloadToken(t)
	s.exchange = func() (int, string) {
		body, _ := json.Marshal(map[string]any{"access_token": token, "issued_token_type": workloadtoken.TokenTypeJWT, "token_type": "Bearer", "expires_in": 300})
		return http.StatusOK, string(body)
	}
	return token
}

// Selector workload: one Bearer, the workload token, no svc_authsess_ byte even with AUTH_SESSION_ISSUE_TOKEN set, and one
// exchange for every issue.
func TestWorkloadSessionSelectorSendsOnlyTheWorkloadToken(t *testing.T) {
	s := newCredServer(t)
	token := s.tokenAnswer(t)
	env := workloadEnv(t, map[string]string{EnvAuthSessionIssueCredential: "workload", sessionissue.EnvToken: testIssueToken(t)})
	cfg := sessionsOn(t, s.URL)
	_, session, err := newCredentials(cfg, issueEnv(env))
	if err != nil {
		t.Fatal(err)
	}
	issuer, err := sessionIssuer(cfg, issueEnv(env), session)
	if err != nil {
		t.Fatal(err)
	}
	s.issueAll(t, issuer)
	s.issueAll(t, issuer)
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.issue) != 2*len(issueFixtures()) {
		t.Fatalf("%d issue requests, want %d", len(s.issue), 2*len(issueFixtures()))
	}
	for i, r := range s.issue {
		if len(r.auth) != 1 || r.auth[0] != "Bearer "+token {
			t.Errorf("request %d: Authorization is not exactly the workload token", i)
		}
		if strings.Contains(r.dump, "svc_authsess_") {
			t.Errorf("request %d holds svc_authsess_ bytes", i)
		}
	}
	if s.exchanges != 1 {
		t.Errorf("%d exchanges for %d issues, want 1", s.exchanges, len(s.issue))
	}
}

// A failed exchange: every issue fails and nothing reaches the issue route.
func TestWorkloadSessionSelectorExchangeFailureSendsNothing(t *testing.T) {
	s := newCredServer(t)
	s.exchange = func() (int, string) { return http.StatusServiceUnavailable, `{}` }
	env := workloadEnv(t, map[string]string{EnvAuthSessionIssueCredential: "workload", sessionissue.EnvToken: testIssueToken(t)})
	cfg := sessionsOn(t, s.URL)
	_, session, _ := newCredentials(cfg, issueEnv(env))
	issuer, err := sessionIssuer(cfg, issueEnv(env), session)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range issueFixtures() {
		if _, err := issuer.Issue(context.Background(), issueRequestFixture(), c.req); err == nil {
			t.Errorf("%s: no error after a failed exchange", c.name)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.issue) != 0 {
		t.Fatalf("%d issue requests after a failed exchange, want 0", len(s.issue))
	}
}

// Both selectors workload: ONE workload client and one exchange serve the session issue and a provisioning call.
func TestBothSelectorsWorkloadShareOneClientAndOneExchange(t *testing.T) {
	s := newCredServer(t)
	token := s.tokenAnswer(t)
	env := workloadEnv(t, map[string]string{EnvAuthSessionIssueCredential: "workload", EnvAuthProvisioningCredential: "workload"})
	cfg := sessionAndProvisioningOn(t, s.URL)
	provisioning, session, err := newCredentials(cfg, issueEnv(env))
	if err != nil {
		t.Fatal(err)
	}
	if provisioning.workload == nil || provisioning.workload != session.workload {
		t.Fatal("the session and provisioning credentials do not share one workload client")
	}
	issuer, err := sessionIssuer(cfg, issueEnv(env), session)
	if err != nil {
		t.Fatal(err)
	}
	prov, err := orgProvisioner(cfg, issueEnv(env), provisioning)
	if err != nil {
		t.Fatal(err)
	}
	s.issueAll(t, issuer)
	if err := prov.Deprovision(context.Background(), uuid.MustParse("11111111-1111-4111-8111-111111111111"), uuid.New()); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range append(append([]credRequest{}, s.issue...), s.prov...) {
		if len(r.auth) != 1 || r.auth[0] != "Bearer "+token {
			t.Errorf("%s: Authorization is not exactly the workload token", r.Path)
		}
	}
	if len(s.prov) != 1 {
		t.Errorf("%d provisioning requests, want 1", len(s.prov))
	}
	if s.exchanges != 1 {
		t.Errorf("%d exchanges for the session issue and the provisioning call, want 1", s.exchanges)
	}
}

// One workload and one session_compat: each client presents its own class.
func TestMixedSelectorsEachClientPresentsItsOwnClass(t *testing.T) {
	for name, selectors := range map[string]struct{ session, provisioning string }{
		"session workload, provisioning compat": {"workload", "session_compat"},
		"session compat, provisioning workload": {"session_compat", "workload"},
	} {
		s := newCredServer(t)
		workload := s.tokenAnswer(t)
		sessionToken := testIssueToken(t)
		env := workloadEnv(t, map[string]string{EnvAuthSessionIssueCredential: selectors.session, EnvAuthProvisioningCredential: selectors.provisioning,
			sessionissue.EnvToken: sessionToken})
		cfg := sessionAndProvisioningOn(t, s.URL)
		provisioning, session, err := newCredentials(cfg, issueEnv(env))
		if err != nil {
			t.Fatal(err)
		}
		issuer, err := sessionIssuer(cfg, issueEnv(env), session)
		if err != nil {
			t.Fatal(err)
		}
		prov, err := orgProvisioner(cfg, issueEnv(env), provisioning)
		if err != nil {
			t.Fatal(err)
		}
		s.issueAll(t, issuer)
		if err := prov.Deprovision(context.Background(), uuid.MustParse("11111111-1111-4111-8111-111111111111"), uuid.New()); err != nil {
			t.Fatal(err)
		}
		wantIssue, wantProv := "Bearer "+workload, "Bearer "+sessionToken
		if selectors.session != "workload" {
			wantIssue, wantProv = "Bearer "+sessionToken, "Bearer "+workload
		}
		s.mu.Lock()
		for _, r := range s.issue {
			if len(r.auth) != 1 || r.auth[0] != wantIssue {
				t.Errorf("%s: an issue request carries the wrong class", name)
			}
		}
		for _, r := range s.prov {
			if len(r.auth) != 1 || r.auth[0] != wantProv {
				t.Errorf("%s: a provisioning request carries the wrong class", name)
			}
		}
		s.mu.Unlock()
	}
}

// With the session selector on workload the exchange answers a redirect without following it.
func TestSessionWorkloadExchangeNeverFollowsARedirectToAnotherOrigin(t *testing.T) {
	probe := redirectprobe.New(t)
	env := workloadEnv(t, map[string]string{EnvAuthSessionIssueCredential: "workload"})
	cfg := sessionsOn(t, probe.Base.URL)
	_, session, err := newCredentials(cfg, issueEnv(env))
	if err != nil {
		t.Fatal(err)
	}
	issuer, err := sessionIssuer(cfg, issueEnv(env), session)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := issuer.Issue(context.Background(), issueRequestFixture(), issueFixtures()[0].req); err == nil {
		t.Fatal("a redirect answer was taken for a token")
	}
	probe.Assert(t)
}
