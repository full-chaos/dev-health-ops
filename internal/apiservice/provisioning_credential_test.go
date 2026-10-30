package apiservice

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/workloadtoken"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
	"github.com/full-chaos/dev-health-ops/internal/api/orgreactivate"
	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
)

const (
	testWorkloadIssuer = "https://auth.example.test"
	goldenFile         = "../api/authprovision/testdata/outgoing_requests.golden.json"
)

func TestProvisioningCredentialEnvNames(t *testing.T) {
	for got, want := range map[string]string{
		EnvAuthProvisioningCredential: "AUTH_PROVISIONING_CREDENTIAL",
		EnvAuthWorkloadSubjectFile:    "AUTH_WORKLOAD_SUBJECT_FILE",
		EnvAuthWorkloadBootstrapFile:  "AUTH_WORKLOAD_BOOTSTRAP_FILE",
		EnvAuthWorkloadIssuer:         "AUTH_WORKLOAD_ISSUER",
	} {
		if got != want {
			t.Errorf("env name %q, want %q", got, want)
		}
	}
}

func provisioningOn(t *testing.T, upstream string) AuthProxyConfig {
	t.Helper()
	return mustProxyConfig(t, map[string]string{EnvAuthProxyURL: upstream, EnvAuthProxyOrgProvisioning: "true"})
}

func bootstrapFile(t *testing.T) string {
	t.Helper()
	random := make([]byte, 32)
	if _, err := rand.Read(random); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "bootstrap")
	if err := os.WriteFile(path, []byte(workloadtoken.BootstrapPrefix+base64.RawURLEncoding.EncodeToString(random)), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func subjectFile(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "subject")
	if err := os.WriteFile(path, []byte("eyJhbGciOiJSUzI1NiJ9.c3ViamVjdA.c2ln"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// Unset and session_compat build no workload client: today's constructors run.
func TestProvisioningCredentialDefaultsToTheSessionCredential(t *testing.T) {
	cfg := provisioningOn(t, "http://auth-service:8095")
	for name, env := range map[string]map[string]string{
		"unset":                {},
		"empty":                {EnvAuthProvisioningCredential: " "},
		"session_compat":       {EnvAuthProvisioningCredential: "session_compat"},
		"session_compat+files": {EnvAuthProvisioningCredential: "session_compat", EnvAuthWorkloadSubjectFile: "/nonexistent", EnvAuthWorkloadIssuer: "x"},
	} {
		cred, err := newProvisioningCredential(cfg, issueEnv(env))
		if err != nil {
			t.Errorf("%s: %v", name, err)
		}
		if cred.workload != nil {
			t.Errorf("%s: a workload client was built", name)
		}
	}
}

// The selector is read only when the switch is on: with it off no client is built.
func TestProvisioningCredentialIsNotReadWithTheSwitchOff(t *testing.T) {
	off := mustProxyConfig(t, map[string]string{EnvAuthProxyURL: "http://auth-service:8095"})
	cred, err := newProvisioningCredential(off, issueEnv(map[string]string{EnvAuthProvisioningCredential: "workload"}))
	if err != nil || cred.workload != nil {
		t.Fatalf("switch off, selector workload with no inputs: %v %v, want no client and no error", cred.workload, err)
	}
}

func TestProvisioningCredentialWorkloadNeedsItsInputsByName(t *testing.T) {
	cfg := provisioningOn(t, "http://auth-service:8095")
	subject, bootstrap := subjectFile(t), bootstrapFile(t)
	good := func(extra map[string]string) map[string]string {
		env := map[string]string{EnvAuthProvisioningCredential: "workload", EnvAuthWorkloadIssuer: testWorkloadIssuer, EnvAuthWorkloadBootstrapFile: bootstrap}
		for k, v := range extra {
			env[k] = v
		}
		return env
	}
	if cred, err := newProvisioningCredential(cfg, issueEnv(good(nil))); err != nil || cred.workload == nil {
		t.Fatalf("bootstrap file: %v %v, want a client", cred.workload, err)
	}
	withSubject := good(map[string]string{EnvAuthWorkloadSubjectFile: subject})
	delete(withSubject, EnvAuthWorkloadBootstrapFile)
	if cred, err := newProvisioningCredential(cfg, issueEnv(withSubject)); err != nil || cred.workload == nil {
		t.Fatalf("subject file: %v %v, want a client", cred.workload, err)
	}
	noIssuer := good(nil)
	delete(noIssuer, EnvAuthWorkloadIssuer)
	noSubject := good(nil)
	delete(noSubject, EnvAuthWorkloadBootstrapFile)
	for name, tc := range map[string]struct {
		env  map[string]string
		name string
	}{
		"both subject files":  {good(map[string]string{EnvAuthWorkloadSubjectFile: subject}), EnvAuthWorkloadSubjectFile},
		"no subject file":     {noSubject, EnvAuthWorkloadSubjectFile},
		"no issuer":           {noIssuer, EnvAuthWorkloadIssuer},
		"empty issuer":        {good(map[string]string{EnvAuthWorkloadIssuer: " "}), EnvAuthWorkloadIssuer},
		"issuer not https":    {good(map[string]string{EnvAuthWorkloadIssuer: "http://auth.example.test"}), EnvAuthWorkloadIssuer},
		"missing bootstrap":   {good(map[string]string{EnvAuthWorkloadBootstrapFile: filepath.Join(t.TempDir(), "absent")}), EnvAuthWorkloadBootstrapFile},
		"bad selector":        {good(map[string]string{EnvAuthProvisioningCredential: "Workload"}), EnvAuthProvisioningCredential},
		"bool selector":       {good(map[string]string{EnvAuthProvisioningCredential: "true"}), EnvAuthProvisioningCredential},
		"session selector":    {good(map[string]string{EnvAuthProvisioningCredential: "session"}), EnvAuthProvisioningCredential},
		"selector with space": {good(map[string]string{EnvAuthProvisioningCredential: "workload session_compat"}), EnvAuthProvisioningCredential},
	} {
		cred, err := newProvisioningCredential(cfg, issueEnv(tc.env))
		if err == nil || cred.workload != nil {
			t.Errorf("%s: %v %v, want a start-up error and no client", name, cred.workload, err)
			continue
		}
		if !strings.Contains(err.Error(), tc.name) || !strings.Contains(err.Error(), EnvAuthProxyOrgProvisioning) {
			t.Errorf("%s: error %q must name %s and the switch", name, err, tc.name)
		}
	}
	if err := func() error { _, err := newProvisioningCredential(cfg, issueEnv(good(nil))); return err }(); err != nil {
		t.Fatal(err)
	}
}

// With the workload credential the clients need no AUTH_SESSION_ISSUE_TOKEN.
func TestWorkloadClientsNeedNoSessionCredential(t *testing.T) {
	cfg := provisioningOn(t, "http://auth-service:8095")
	cred, err := newProvisioningCredential(cfg, issueEnv(map[string]string{EnvAuthProvisioningCredential: "workload",
		EnvAuthWorkloadIssuer: testWorkloadIssuer, EnvAuthWorkloadBootstrapFile: bootstrapFile(t)}))
	if err != nil {
		t.Fatal(err)
	}
	if c, err := orgLifecycleClient(cfg, issueEnv(map[string]string{}), cred); c == nil || err != nil {
		t.Errorf("lifecycle client: %v %v, want a client", c, err)
	}
	if p, err := orgProvisioner(cfg, issueEnv(map[string]string{}), cred); p == nil || err != nil {
		t.Errorf("provisioner: %v %v, want a client", p, err)
	}
	off := mustProxyConfig(t, map[string]string{EnvAuthProxyURL: "http://auth-service:8095"})
	if c, err := orgLifecycleClient(off, issueEnv(map[string]string{}), cred); c != nil || err != nil {
		t.Errorf("switch off: %v %v, want no client", c, err)
	}
}

// ---- outgoing requests through the wiring ----

type wiringServer struct {
	mu           sync.Mutex
	last         wiringRequest
	exchanges    int
	provisioning int
	auths        [][]string
	dumps        []string
	exchange     func() (int, string)
	URL          string
}

type wiringRequest struct {
	Verb    string              `json:"verb"`
	Method  string              `json:"method"`
	Path    string              `json:"path"`
	Headers map[string][]string `json:"headers"`
	Body    string              `json:"body"`
}

func newWiringServer(t *testing.T) *wiringServer {
	t.Helper()
	w := &wiringServer{}
	org, row := "11111111-1111-4111-8111-111111111111", "66666666-6666-4666-8666-666666666666"
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		w.mu.Lock()
		defer w.mu.Unlock()
		rw.Header().Set("Content-Type", "application/json")
		if r.URL.Path == workloadtoken.ExchangePath {
			w.exchanges++
			if w.exchange == nil {
				http.NotFound(rw, r)
				return
			}
			status, body := w.exchange()
			rw.WriteHeader(status)
			_, _ = io.WriteString(rw, body)
			return
		}
		w.provisioning++
		headers := map[string][]string{}
		var dump strings.Builder
		for name, values := range r.Header {
			headers[name] = append([]string(nil), values...)
			sort.Strings(headers[name])
			dump.WriteString(name + ": " + strings.Join(values, ",") + "\n")
		}
		dump.WriteString(string(raw))
		w.auths = append(w.auths, append([]string(nil), r.Header.Values("Authorization")...))
		w.dumps = append(w.dumps, dump.String())
		if auth := headers["Authorization"]; len(auth) == 1 {
			if strings.HasPrefix(auth[0], "Bearer svc_authsess_") {
				headers["Authorization"] = []string{"Bearer svc_authsess_<PLACEHOLDER>"}
			} else {
				headers["Authorization"] = []string{"Bearer <NOT-svc_authsess_>"}
			}
		}
		w.last = wiringRequest{Method: r.Method, Path: r.URL.Path, Headers: headers, Body: string(raw)}
		member := func(role, status string) string {
			return `{"membership_id":"` + row + `","organization_id":"` + org + `","principal_id":"22222222-2222-4222-8222-222222222222","role":"` + role + `","status":"` + status + `"}`
		}
		answers := map[string]string{
			authprovision.PathMembershipAdd:      member("member", "active"),
			authprovision.PathMembershipRole:     member("admin", "active"),
			authprovision.PathMembershipRemove:   member("member", "removed"),
			authprovision.PathMembershipTransfer: `{"organization_id":"` + org + `","from_principal_id":"33333333-3333-4333-8333-333333333333","owner_principal_id":"44444444-4444-4444-8444-444444444444"}`,
			authprovision.PathInviteCreate:       `{"invite_id":"` + row + `","token":"invite-token","expires_at":"2026-11-01T00:00:00Z"}`,
			authprovision.PathInviteAccept:       `{"invite_id":"` + row + `","organization_id":"` + org + `","membership_id":"` + row + `","role":"member"}`,
			authprovision.PathOrgDeprovision:     `{"organization_id":"` + org + `"}`,
			orgreactivate.Path:                   `{"organization_id":"` + org + `","status":"active"}`,
			orgreactivate.SuspendPath:            `{"organization_id":"` + org + `","status":"suspended"}`,
			orgreactivate.StatePath:              `{"organization_id":"` + org + `","status":"active","revision":4}`,
		}
		_, _ = io.WriteString(rw, answers[r.URL.Path])
	}))
	t.Cleanup(server.Close)
	w.URL = server.URL
	return w
}

type wiringCall struct {
	verb string
	do   func() error
}

type wiringLifecycle interface {
	orgreactivate.Reactivator
	orgreactivate.Suspender
	orgreactivate.StateReader
}

func wiringCalls(prov authprovision.Provisioner, life wiringLifecycle) []wiringCall {
	ctx := context.Background()
	org, principal := uuid.MustParse("11111111-1111-4111-8111-111111111111"), uuid.MustParse("22222222-2222-4222-8222-222222222222")
	actor, to := uuid.MustParse("33333333-3333-4333-8333-333333333333"), uuid.MustParse("44444444-4444-4444-8444-444444444444")
	invitedBy := uuid.MustParse("55555555-5555-4555-8555-555555555555")
	metadata := json.RawMessage(`{"source":"golden"}`)
	reactivateRevision, suspendRevision := int64(3), int64(5)
	return []wiringCall{
		{"memberships_add", func() error { _, err := prov.AddMember(ctx, org, principal, actor, "member", &invitedBy); return err }},
		{"memberships_role", func() error { _, err := prov.ChangeRole(ctx, org, principal, actor, "admin"); return err }},
		{"memberships_remove", func() error { return prov.RemoveMember(ctx, org, principal, actor) }},
		{"memberships_transfer", func() error { return prov.TransferOwnership(ctx, org, actor, to, actor) }},
		{"invites_create", func() error {
			_, err := prov.CreateInvite(ctx, authprovision.InviteRequest{OrgID: org, Email: "golden@example.test", Role: "member",
				TTL: 72 * time.Hour, Actor: actor, Description: "golden invite", RequestMetadata: metadata})
			return err
		}},
		{"invites_accept", func() error {
			_, err := prov.AcceptInvite(ctx, authprovision.AcceptRequest{Token: "invite-token", PrincipalID: principal,
				Description: "golden accept", RequestMetadata: metadata})
			return err
		}},
		{"organizations_deprovision", func() error { return prov.Deprovision(ctx, org, actor) }},
		{"organizations_reactivate", func() error { return life.Reactivate(ctx, org, actor, &reactivateRevision) }},
		{"organizations_suspend", func() error { return life.Suspend(ctx, org, actor, &suspendRevision) }},
		{"organizations_state", func() error { _, err := life.State(ctx, org); return err }},
	}
}

func (w *wiringServer) runAll(t *testing.T, cfg AuthProxyConfig, env map[string]string) []wiringRequest {
	t.Helper()
	lookup := issueEnv(env)
	cred, err := newProvisioningCredential(cfg, lookup)
	if err != nil {
		t.Fatal(err)
	}
	life, err := orgLifecycleClient(cfg, lookup, cred)
	if err != nil {
		t.Fatal(err)
	}
	prov, err := orgProvisioner(cfg, lookup, cred)
	if err != nil {
		t.Fatal(err)
	}
	var out []wiringRequest
	for _, call := range wiringCalls(prov, life) {
		if err := call.do(); err != nil {
			t.Fatalf("%s: %v", call.verb, err)
		}
		w.mu.Lock()
		got := w.last
		w.mu.Unlock()
		got.Verb = call.verb
		out = append(out, got)
	}
	return out
}

// Selector unset, and session_compat, send the exact bytes the golden pins.
func TestUnsetAndSessionCompatSendTheGoldenRequests(t *testing.T) {
	raw, err := os.ReadFile(goldenFile)
	if err != nil {
		t.Fatal(err)
	}
	var want []wiringRequest
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatal(err)
	}
	for name, selector := range map[string]map[string]string{"unset": {}, "session_compat": {EnvAuthProvisioningCredential: "session_compat"}} {
		w := newWiringServer(t)
		token := testIssueToken(t)
		env := map[string]string{sessionissue.EnvToken: token}
		for k, v := range selector {
			env[k] = v
		}
		got := w.runAll(t, provisioningOn(t, w.URL), env)
		w.mu.Lock()
		if len(w.auths) != 10 {
			t.Errorf("%s: %d provisioning requests, want 10", name, len(w.auths))
		}
		for i, auth := range w.auths {
			if len(auth) != 1 || auth[0] != "Bearer "+token {
				t.Errorf("%s: request %d Authorization is not exactly the AUTH_SESSION_ISSUE_TOKEN value", name, i)
			}
		}
		w.mu.Unlock()
		gotJSON, _ := json.Marshal(got)
		wantJSON, _ := json.Marshal(want)
		if string(gotJSON) != string(wantJSON) {
			t.Errorf("%s: outgoing requests differ from the golden\n got %s\nwant %s", name, gotJSON, wantJSON)
		}
		if w.exchanges != 0 {
			t.Errorf("%s: %d exchanges, want 0", name, w.exchanges)
		}
	}
}

func testWorkloadToken(t *testing.T) string {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	enc := base64.RawURLEncoding
	now := time.Now().Unix()
	payload := fmt.Sprintf(`{"iss":%q,"aud":"auth-service","sub":"prn_AAAAAAAAAAAAAAAAAAAAAA","principal_type":"workload","jti":"%024d","iat":%d,"nbf":%d,"exp":%d}`,
		testWorkloadIssuer, 9, now, now, now+300)
	input := enc.EncodeToString([]byte(`{"alg":"EdDSA","typ":"workload+jwt","kid":"test-key"}`)) + "." + enc.EncodeToString([]byte(payload))
	return input + "." + enc.EncodeToString(ed25519.Sign(key, []byte(input)))
}

// Selector workload: one Bearer, the workload token, no svc_authsess_ byte even
// with AUTH_SESSION_ISSUE_TOKEN set, and one exchange for every call of both clients.
func TestWorkloadSelectorSendsOnlyTheWorkloadToken(t *testing.T) {
	w := newWiringServer(t)
	token := testWorkloadToken(t)
	w.exchange = func() (int, string) {
		body, _ := json.Marshal(map[string]any{"access_token": token, "issued_token_type": workloadtoken.TokenTypeJWT, "token_type": "Bearer", "expires_in": 300})
		return http.StatusOK, string(body)
	}
	env := map[string]string{EnvAuthProvisioningCredential: "workload", EnvAuthWorkloadIssuer: testWorkloadIssuer,
		EnvAuthWorkloadBootstrapFile: bootstrapFile(t), sessionissue.EnvToken: testIssueToken(t)}
	cfg := provisioningOn(t, w.URL)
	w.runAll(t, cfg, env)
	w.runAll(t, cfg, env)
	w.mu.Lock()
	defer w.mu.Unlock()
	if want := 2 * 10; w.provisioning != want {
		t.Fatalf("%d provisioning requests, want %d", w.provisioning, want)
	}
	for i, auth := range w.auths {
		if len(auth) != 1 || auth[0] != "Bearer "+token {
			t.Errorf("request %d: Authorization %q, want exactly the workload token", i, auth)
		}
		if strings.Contains(w.dumps[i], "svc_authsess_") {
			t.Errorf("request %d holds svc_authsess_ bytes", i)
		}
	}
	// Two runs build two shared clients (one per run): one exchange each.
	if w.exchanges != 2 {
		t.Errorf("%d exchanges for 2 shared clients, want 2", w.exchanges)
	}
}

// Selector workload and a failed exchange: every verb fails and nothing is sent.
func TestWorkloadSelectorExchangeFailureSendsNothing(t *testing.T) {
	w := newWiringServer(t)
	w.exchange = func() (int, string) { return http.StatusServiceUnavailable, `{}` }
	env := map[string]string{EnvAuthProvisioningCredential: "workload", EnvAuthWorkloadIssuer: testWorkloadIssuer,
		EnvAuthWorkloadBootstrapFile: bootstrapFile(t), sessionissue.EnvToken: testIssueToken(t)}
	cfg := provisioningOn(t, w.URL)
	lookup := issueEnv(env)
	cred, err := newProvisioningCredential(cfg, lookup)
	if err != nil {
		t.Fatal(err)
	}
	life, _ := orgLifecycleClient(cfg, lookup, cred)
	prov, _ := orgProvisioner(cfg, lookup, cred)
	for _, call := range wiringCalls(prov, life) {
		if err := call.do(); err == nil {
			t.Errorf("%s: no error after a failed exchange", call.verb)
		}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.provisioning != 0 {
		t.Fatalf("%d provisioning requests after a failed exchange, want 0", w.provisioning)
	}
}

// A SET subject or bootstrap path that is missing, unreadable or a directory is
// a start-up error that names the variable and holds no byte of the path.
func TestProvisioningCredentialSetFileMustBeReadable(t *testing.T) {
	cfg := provisioningOn(t, "http://auth-service:8095")
	dir := t.TempDir()
	unreadable := filepath.Join(dir, "unreadable-secret-name")
	if err := os.WriteFile(unreadable, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(unreadable, 0); err != nil {
		t.Fatal(err)
	}
	paths := map[string]string{
		"missing":   filepath.Join(dir, "missing-secret-name"),
		"directory": filepath.Join(dir, "directory-secret-name"),
	}
	if err := os.Mkdir(paths["directory"], 0o700); err != nil {
		t.Fatal(err)
	}
	if os.Getuid() != 0 {
		paths["unreadable"] = unreadable
	}
	for _, variable := range []string{EnvAuthWorkloadSubjectFile, EnvAuthWorkloadBootstrapFile} {
		for name, path := range paths {
			env := map[string]string{EnvAuthProvisioningCredential: "workload", EnvAuthWorkloadIssuer: testWorkloadIssuer, variable: path}
			cred, err := newProvisioningCredential(cfg, issueEnv(env))
			if err == nil || cred.workload != nil {
				t.Errorf("%s %s: %v %v, want a start-up error and no client", variable, name, cred.workload, err)
				continue
			}
			if !strings.Contains(err.Error(), variable) || strings.Contains(err.Error(), filepath.Base(path)) || strings.Contains(err.Error(), dir) {
				t.Errorf("%s %s: error %q must name the variable and no byte of the path", variable, name, err)
			}
		}
	}
}

// A file removed after start-up is not a start-up error: the first exchange
// fails, every verb fails and nothing reaches the provisioning routes.
func TestWorkloadSubjectRemovedAfterStartFailsTheCallWithNoRequest(t *testing.T) {
	w := newWiringServer(t)
	w.exchange = func() (int, string) { return http.StatusInternalServerError, `{}` }
	path := bootstrapFile(t)
	env := map[string]string{EnvAuthProvisioningCredential: "workload", EnvAuthWorkloadIssuer: testWorkloadIssuer, EnvAuthWorkloadBootstrapFile: path}
	cfg := provisioningOn(t, w.URL)
	lookup := issueEnv(env)
	cred, err := newProvisioningCredential(cfg, lookup)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	life, _ := orgLifecycleClient(cfg, lookup, cred)
	prov, _ := orgProvisioner(cfg, lookup, cred)
	for _, call := range wiringCalls(prov, life) {
		if err := call.do(); err == nil {
			t.Errorf("%s: no error with the subject file gone", call.verb)
		}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.provisioning != 0 || w.exchanges != 0 {
		t.Fatalf("%d provisioning requests and %d exchanges with the subject file gone, want 0 and 0", w.provisioning, w.exchanges)
	}
}
