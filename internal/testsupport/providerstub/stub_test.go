package providerstub

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func get(t *testing.T, handler http.Handler, host, method, target string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, target, nil)
	request.Host = host
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}

func TestProviderForSelectsByHost(t *testing.T) {
	for host, want := range map[string]string{
		"api.github.com": "github", "GITHUB.com:443": "github", "gitlab.com": "gitlab", "api.linear.app": "linear",
		"api.pagerduty.com": "pagerduty", "identity.pagerduty.com": "pagerduty", "api.eu.pagerduty.com": "pagerduty",
		"zz-venue.atlassian.net": "jira", "example.com": "", "atlassian.net": "", "evil-atlassian.net": "",
	} {
		if got := ProviderFor(host); got != want {
			t.Errorf("ProviderFor(%q) = %q, want %q", host, got, want)
		}
	}
}

func TestStubAnswersTheMatchingFixtureAndRefusesTheRest(t *testing.T) {
	stub, err := New(
		Fixture{Provider: "github", Method: "GET", Path: "/repos/zz/one", Status: 200, Body: json.RawMessage(`{"name":"one"}`)},
		Fixture{Provider: "github", Method: "GET", Path: "/orgs/zz/repos", Query: map[string]string{"page": "2"}, Status: 200, Body: json.RawMessage(`[]`)},
		Fixture{Provider: "github", Method: "GET", Path: "/orgs/zz/repos", Status: 200, Headers: map[string]string{"Link": "<x>; rel=next"}, Body: json.RawMessage(`[{"name":"a"}]`)},
		Fixture{Provider: "gitlab", Method: "GET", Path: "/api/v4/groups/*", Status: 404, Body: json.RawMessage(`{"message":"404 Group Not Found"}`)},
	)
	if err != nil {
		t.Fatal(err)
	}
	if r := get(t, stub, "api.github.com", "GET", "/repos/zz/one", nil); r.Code != 200 || !strings.Contains(r.Body.String(), `"one"`) {
		t.Fatalf("exact path: %d %s", r.Code, r.Body)
	}
	// The query-constrained fixture comes first in table order for page=2, the plain one otherwise.
	if r := get(t, stub, "api.github.com", "GET", "/orgs/zz/repos?page=2", nil); r.Body.String() != "[]" {
		t.Fatalf("query match: %s", r.Body)
	}
	if r := get(t, stub, "api.github.com", "GET", "/orgs/zz/repos", nil); r.Header().Get("Link") == "" || !strings.Contains(r.Body.String(), `"a"`) {
		t.Fatalf("no-query fixture: %v %s", r.Header(), r.Body)
	}
	// A query parameter with the wrong value must not match the constrained fixture.
	if r := get(t, stub, "api.github.com", "GET", "/orgs/zz/repos?page=3", nil); r.Body.String() == "[]" || !strings.Contains(r.Body.String(), `"a"`) {
		t.Fatalf("page=3 matched the page=2 fixture: %s", r.Body)
	}
	if r := get(t, stub, "gitlab.com", "GET", "/api/v4/groups/anything/projects", nil); r.Code != 404 {
		t.Fatalf("prefix path: %d", r.Code)
	}
	// Wrong method, wrong provider host, unknown host: all 599 and recorded as unmatched.
	for _, tc := range []struct{ host, method, target string }{
		{"api.github.com", "POST", "/repos/zz/one"}, {"gitlab.com", "GET", "/repos/zz/one"}, {"example.com", "GET", "/"},
	} {
		r := get(t, stub, tc.host, tc.method, tc.target, nil)
		if r.Code != 599 || r.Header().Get("X-Providerstub") != "unstubbed" {
			t.Errorf("%v: status %d, want the loud 599", tc, r.Code)
		}
	}
	if got := len(stub.Unmatched()); got != 3 {
		t.Fatalf("unmatched = %d, want 3", got)
	}
	if got := len(stub.Requests()); got != 8 {
		t.Fatalf("recorded = %d, want 8", got)
	}
}

func TestRecorderKeepsOnlyTheKindOfCredentialNeverItsValue(t *testing.T) {
	stub, _ := New(Fixture{Provider: "github", Method: "GET", Path: "/user", Status: 200, Body: json.RawMessage(`{}`)})
	const secret = "s3cr3t-value-that-must-not-be-kept"
	get(t, stub, "api.github.com", "GET", "/user", map[string]string{"Authorization": "token " + secret})
	get(t, stub, "gitlab.com", "GET", "/x", map[string]string{"PRIVATE-TOKEN": secret})
	get(t, stub, "api.github.com", "GET", "/user", nil)
	// A token carried as a query parameter (some provider APIs accept one) must not be kept either.
	get(t, stub, "api.github.com", "GET", "/user?access_token="+secret+"&page=1", nil)
	encoded, _ := json.Marshal(stub.Requests())
	if strings.Contains(string(encoded), secret) {
		t.Fatal("a credential value reached the recorder")
	}
	kinds := []string{}
	for _, r := range stub.Requests() {
		kinds = append(kinds, r.AuthKind)
	}
	if strings.Join(kinds, ",") != "token,PRIVATE-TOKEN,none,none" {
		t.Fatalf("auth kinds = %v", kinds)
	}
	if last := stub.Requests()[3]; strings.Join(last.QueryKeys, ",") != "access_token,page" {
		t.Fatalf("query keys = %v, want the names only", last.QueryKeys)
	}
}

func TestFixtureValidationRefusesUnusableFixtures(t *testing.T) {
	for name, fixture := range map[string]Fixture{
		"unknown provider": {Provider: "bitbucket", Method: "GET", Path: "/x", Status: 200},
		"no method":        {Provider: "github", Path: "/x", Status: 200},
		"relative path":    {Provider: "github", Method: "GET", Path: "x", Status: 200},
		"reserved 599":     {Provider: "github", Method: "GET", Path: "/x", Status: 599},
		"bad status":       {Provider: "github", Method: "GET", Path: "/x", Status: 42},
		"both bodies":      {Provider: "github", Method: "GET", Path: "/x", Status: 200, Body: json.RawMessage(`{}`), BodyFile: "f"},
	} {
		if _, err := New(fixture); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestLoadDirReadsFilesAndBodyFilesAndTheBuiltinStarterLoads(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "body.raw"), []byte("recorded-bytes\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "a.json"), []byte(`[{"provider":"gitlab","method":"GET","path":"/p","status":200,"body_file":"body.raw"}]`), 0o600); err != nil {
		t.Fatal(err)
	}
	stub, err := LoadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if r := get(t, stub, "gitlab.com", "GET", "/p", nil); r.Body.String() != "recorded-bytes\n" {
		t.Fatalf("body_file: %q", r.Body)
	}
	if _, err := LoadDir(t.TempDir()); err == nil {
		t.Fatal("an empty fixture directory must be refused")
	}
	if err := os.WriteFile(filepath.Join(dir, "b.json"), []byte(`[{"provider":"github","method":"GET","path":"/q","status":200,"body_file":"missing.raw"}]`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadDir(dir); err == nil {
		t.Fatal("a missing body_file must be refused")
	}
	starter, err := LoadDir("fixtures")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ host, method, target string }{
		{"api.github.com", "GET", "/user"}, {"gitlab.com", "GET", "/api/v4/groups/zz-venue/projects"},
		{"zz-venue.atlassian.net", "GET", "/rest/api/3/serverInfo"}, {"api.linear.app", "POST", "/graphql"},
		{"identity.pagerduty.com", "POST", "/oauth/token"}, {"api.pagerduty.com", "GET", "/services"},
	} {
		if r := get(t, starter, tc.host, tc.method, tc.target, nil); r.Code != 200 {
			t.Errorf("starter fixture %v answered %d", tc, r.Code)
		}
	}
}

func TestAdminHandlerServesTheRecorderOnItsOwnHandler(t *testing.T) {
	stub, _ := New(Fixture{Provider: "github", Method: "GET", Path: "/user", Status: 200, Body: json.RawMessage(`{}`)})
	get(t, stub, "api.github.com", "GET", "/user", nil)
	get(t, stub, "api.github.com", "GET", "/user/orgs", nil)
	admin := stub.AdminHandler()
	var all, unmatched []Recorded
	if err := json.Unmarshal(get(t, admin, "x", "GET", "/requests", nil).Body.Bytes(), &all); err != nil || len(all) != 2 {
		t.Fatalf("requests: %v %v", all, err)
	}
	if err := json.Unmarshal(get(t, admin, "x", "GET", "/unmatched", nil).Body.Bytes(), &unmatched); err != nil || len(unmatched) != 1 || unmatched[0].Path != "/user/orgs" {
		t.Fatalf("unmatched: %v %v", unmatched, err)
	}
	if r := get(t, admin, "x", "POST", "/reset", nil); r.Code != http.StatusNoContent || len(stub.Requests()) != 0 {
		t.Fatalf("reset: %d, %d left", r.Code, len(stub.Requests()))
	}
	if r := get(t, admin, "x", "GET", "/user", nil); r.Code != http.StatusNotFound {
		t.Fatalf("a provider path must not be served by the admin handler: %d", r.Code)
	}
}

// TestTLSEndToEndWithAHostsOverrideAndTheIssuedCA is the venue's transport in
// miniature: the client resolves api.github.com to the stub (what the hosts
// file does), trusts only the venue CA, and the request reaches the fixture.
// A client that does NOT trust the CA is refused.
func TestTLSEndToEndWithAHostsOverrideAndTheIssuedCA(t *testing.T) {
	ca, err := NewCA(time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	certPEM, keyPEM, err := ca.IssueServer(append(Hosts(), "zz-venue.atlassian.net"), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	stub, _ := New(Fixture{Provider: "github", Method: "GET", Path: "/user", Status: 200, Body: json.RawMessage(`{"login":"zz"}`)},
		Fixture{Provider: "jira", Method: "GET", Path: "/rest/api/3/serverInfo", Status: 200, Body: json.RawMessage(`{"ok":true}`)})
	server := httptest.NewUnstartedServer(stub)
	server.TLS = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	server.StartTLS()
	defer server.Close()
	backend := server.Listener.Addr().String()

	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(ca.CertPEM)
	client := func(roots *x509.CertPool) *http.Client {
		return &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{
			TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12},
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, backend) // every host resolves to the stub
			},
		}}
	}
	for _, target := range []string{"https://api.github.com/user", "https://zz-venue.atlassian.net/rest/api/3/serverInfo"} {
		response, err := client(pool).Get(target)
		if err != nil {
			t.Fatalf("%s: %v", target, err)
		}
		body, _ := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if response.StatusCode != 200 || len(body) == 0 {
			t.Fatalf("%s: %d %s", target, response.StatusCode, body)
		}
	}
	if _, err := client(x509.NewCertPool()).Get("https://api.github.com/user"); err == nil {
		t.Fatal("a client that does not trust the venue CA must be refused")
	}
	if n := len(stub.Requests()); n != 2 {
		t.Fatalf("recorded %d requests, want 2", n)
	}
}

// ---- r1 (CHAOS-6718) findings: each test fails on the pre-fix stub ----

func TestRecorderAndUnstubbedBodyNeverCarryATokenInThePath(t *testing.T) {
	stub, _ := New(Fixture{Provider: "gitlab", Method: "GET", Path: "/api/v4/jobs/*", Status: 200, Body: json.RawMessage(`{}`)})
	canary := "review-path-canary-not-a-real-credential"
	miss := get(t, stub, "gitlab.com", "POST", "/api/v4/runners/"+canary, nil)
	if miss.Code != 599 || strings.Contains(miss.Body.String(), canary) {
		t.Fatalf("unstubbed body echoes the path token: %d %s", miss.Code, miss.Body.String())
	}
	hit := get(t, stub, "gitlab.com", "GET", "/api/v4/jobs/"+canary, nil)
	if hit.Code != 200 {
		t.Fatalf("matched status = %d", hit.Code)
	}
	raw, _ := json.Marshal(stub.Requests())
	if strings.Contains(string(raw), canary) {
		t.Fatalf("recorder kept the path token: %s", raw)
	}
	// the operator still learns WHICH endpoint was unstubbed: the vocabulary segments survive
	if !strings.Contains(string(raw), "/api/v4/runners/") {
		t.Fatalf("recorder lost the endpoint shape: %s", raw)
	}
}

func TestPrefixFixturesRefuseTraversalAndDoubledSlashPaths(t *testing.T) {
	stub, _ := New(Fixture{Provider: "gitlab", Method: "GET", Path: "/api/v4/groups/*", Status: 202, Body: json.RawMessage(`{"fixture":"prefix"}`)})
	if got := get(t, stub, "gitlab.com", "GET", "/api/v4/groups/7", nil); got.Code != 202 {
		t.Fatalf("plain path = %d", got.Code)
	}
	for _, target := range []string{"/api/v4/groups/../admin", "/api/v4/groups/%2e%2e/admin", "/api/v4/groups//admin", "/api/v4/groups/./x", "/api/v4/groups/a%2Fb"} {
		if got := get(t, stub, "gitlab.com", "GET", target, nil); got.Code != 599 {
			t.Errorf("%s answered %d, want 599", target, got.Code)
		}
	}
}

func TestADuplicateQueryKeyCannotSmuggleAValuePastAFixture(t *testing.T) {
	stub, _ := New(Fixture{Provider: "github", Method: "GET", Path: "/x", Query: map[string]string{"scope": "read"}, Status: 201, Body: json.RawMessage(`{}`)})
	if got := get(t, stub, "api.github.com", "GET", "/x?scope=read", nil); got.Code != 201 {
		t.Fatalf("exact query = %d", got.Code)
	}
	for _, target := range []string{"/x?scope=read&scope=admin", "/x?scope=admin&scope=read"} {
		if got := get(t, stub, "api.github.com", "GET", target, nil); got.Code != 599 {
			t.Errorf("%s answered %d, want 599", target, got.Code)
		}
	}
}

func TestMethodMatchingIsCaseSensitive(t *testing.T) {
	stub, _ := New(Fixture{Provider: "github", Method: "GET", Path: "/x", Status: 200, Body: json.RawMessage(`{}`)})
	if got := get(t, stub, "api.github.com", "get", "/x", nil); got.Code != 599 {
		t.Fatalf("lowercase method answered %d, want 599", got.Code)
	}
	if _, err := New(Fixture{Provider: "github", Method: "get", Path: "/x", Status: 200}); err == nil {
		t.Fatal("a lowercase fixture method must be refused")
	}
}

func TestOnlyAValidTLSPortSelectsAProvider(t *testing.T) {
	for host, want := range map[string]string{"api.github.com": "github", "api.github.com:443": "github", "api.github.com:notaport": "", "api.github.com:8443": "", "api.github.com:": "", "API.GITHUB.COM": "github"} {
		if got := ProviderFor(host); got != want {
			t.Errorf("ProviderFor(%q) = %q, want %q", host, got, want)
		}
	}
}

func TestFixtureStatusMustBeAFinalStatus(t *testing.T) {
	for _, status := range []int{0, 99, 100, 101, 199, 599, 600} {
		if _, err := New(Fixture{Provider: "github", Method: "GET", Path: "/x", Status: status}); err == nil {
			t.Errorf("status %d must be refused", status)
		}
	}
	if _, err := New(Fixture{Provider: "github", Method: "GET", Path: "/x", Status: 200}); err != nil {
		t.Fatal(err)
	}
}

func TestBodyFileCannotLeaveTheFixtureDirectory(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "fx")
	_ = os.Mkdir(dir, 0o755)
	_ = os.WriteFile(filepath.Join(root, "outside.raw"), []byte("outside-marker"), 0o600)
	_ = os.WriteFile(filepath.Join(dir, "inside.raw"), []byte("inside-marker"), 0o600)
	load := func(bodyFile string) error {
		fixture := `[{"provider":"github","method":"GET","path":"/x","status":200,"body_file":"` + bodyFile + `"}]`
		if err := os.WriteFile(filepath.Join(dir, "a.json"), []byte(fixture), 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := LoadDir(dir)
		return err
	}
	if err := load("inside.raw"); err != nil {
		t.Fatalf("a body_file inside the directory must load: %v", err)
	}
	if err := load("../outside.raw"); err == nil {
		t.Error("../outside.raw must be refused")
	}
	if err := load(filepath.Join(root, "outside.raw")); err == nil {
		t.Error("an absolute body_file must be refused")
	}
	if err := os.Symlink(filepath.Join(root, "outside.raw"), filepath.Join(dir, "link.raw")); err == nil {
		if err := load("link.raw"); err == nil {
			t.Error("a symlink out of the directory must be refused")
		}
	}
}

func TestRecorderStoresNoUnknownHostOrOpaqueAuthScheme(t *testing.T) {
	stub, _ := New()
	get(t, stub, "leak-host-canary.example", "GET", "/x", map[string]string{"Authorization": "canary-scheme-not-a-credential value"})
	get(t, stub, "gitlab.com", "CANARYMETHOD", "/x", nil)
	raw, _ := json.Marshal(stub.Requests())
	if strings.Contains(string(raw), "CANARYMETHOD") || !strings.Contains(string(raw), `"method":"OTHER"`) {
		t.Fatalf("recorder kept a non-HTTP method token: %s", raw)
	}
	if strings.Contains(string(raw), "leak-host-canary") || strings.Contains(string(raw), "canary-scheme") {
		t.Fatalf("recorder kept an unvetted header value: %s", raw)
	}
}

func TestEveryRequestIsLoggedWithoutPathOrValues(t *testing.T) {
	stub, _ := New()
	var lines []string
	stub.Log = func(line string) { lines = append(lines, line) }
	get(t, stub, "gitlab.com", "GET", "/api/v4/x/secret-canary-value?token=q-canary", map[string]string{"PRIVATE-TOKEN": "h-canary"})
	if len(lines) != 1 || !strings.Contains(lines[0], "599") || !strings.Contains(lines[0], "gitlab") {
		t.Fatalf("log lines = %q", lines)
	}
	for _, canary := range []string{"secret-canary-value", "q-canary", "h-canary"} {
		if strings.Contains(lines[0], canary) {
			t.Fatalf("log line carries %s: %s", canary, lines[0])
		}
	}
}

func TestIssueServerCoversOnlyProviderHostsAndJiraTenants(t *testing.T) {
	ca, _ := NewCA(time.Hour)
	if _, _, err := ca.IssueServer([]string{"api.github.com", "example.com"}, time.Hour); err == nil {
		t.Fatal("a host that is not a provider host or Jira tenant must be refused")
	}
	certPEM, _, err := ca.IssueServer([]string{"api.github.com", "zz-venue.atlassian.net"}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(certPEM)
	cert, _ := x509.ParseCertificate(block.Bytes)
	if len(cert.IPAddresses) != 0 {
		t.Fatalf("the certificate must not carry IP SANs: %v", cert.IPAddresses)
	}
}

// ---- r2 (CHAOS-6718) findings: each test fails on the d7deb43e stub ----

func TestPathShapeKeepsOnlyKnownVocabularyNotShortAlphabeticTokens(t *testing.T) {
	stub, _ := New(Fixture{Provider: "gitlab", Method: "GET", Path: "/api/v4/groups/*", Status: 200, Body: json.RawMessage(`{}`)})
	miss := get(t, stub, "gitlab.com", "POST", "/api/v4/runners/secretvalue", nil)
	raw, _ := json.Marshal(stub.Requests())
	if strings.Contains(miss.Body.String(), "secretvalue") || strings.Contains(string(raw), "secretvalue") {
		t.Fatalf("a short alphabetic path token survived: %s / %s", miss.Body.String(), raw)
	}
	if !strings.Contains(string(raw), "/api/v4/") {
		t.Fatalf("recorder lost the api prefix: %s", raw)
	}
	// a word the fixtures name stays readable
	get(t, stub, "gitlab.com", "DELETE", "/api/v4/groups/abcdefgh", nil)
	raw, _ = json.Marshal(stub.Requests())
	if !strings.Contains(string(raw), "/api/v4/groups/{x}") {
		t.Fatalf("fixture vocabulary was masked: %s", raw)
	}
}

func TestRepeatedlyEncodedPathsAndQueryKeysAreRefused(t *testing.T) {
	stub, _ := New(
		Fixture{Provider: "gitlab", Method: "GET", Path: "/api/v4/groups/*", Status: 202, Body: json.RawMessage(`{}`)},
		Fixture{Provider: "github", Method: "GET", Path: "/x", Query: map[string]string{"scope": "read"}, Status: 201, Body: json.RawMessage(`{}`)})
	for _, target := range []string{"/api/v4/groups/%252e%252e/admin", "/api/v4/groups/%252E%252E/admin", "/api/v4/groups/%252Fadmin", "/api/v4/groups/100%25"} {
		if got := get(t, stub, "gitlab.com", "GET", target, nil); got.Code != 599 {
			t.Errorf("%s answered %d, want 599", target, got.Code)
		}
	}
	for _, target := range []string{"/x?scope=read&%2573cope=admin", "/x?scope=read&%73cope=admin"} {
		if got := get(t, stub, "api.github.com", "GET", target, nil); got.Code != 599 {
			t.Errorf("%s answered %d, want 599", target, got.Code)
		}
	}
}

func TestBodyFileIsReadThroughARootSoASwapCannotEscape(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "fx")
	_ = os.Mkdir(dir, 0o755)
	_ = os.WriteFile(filepath.Join(root, "outside.raw"), []byte("outside-marker"), 0o600)
	_ = os.WriteFile(filepath.Join(dir, "a.json"), []byte(`[{"provider":"github","method":"GET","path":"/x","status":200,"body_file":"sub/link.raw"}]`), 0o600)
	_ = os.Mkdir(filepath.Join(dir, "sub"), 0o755)
	// a directory symlink that points out of the fixture directory
	if err := os.Symlink(root, filepath.Join(dir, "sub", "up")); err != nil {
		t.Skip("no symlinks")
	}
	_ = os.WriteFile(filepath.Join(dir, "a.json"), []byte(`[{"provider":"github","method":"GET","path":"/x","status":200,"body_file":"sub/up/outside.raw"}]`), 0o600)
	if _, err := LoadDir(dir); err == nil {
		t.Fatal("a body_file reached through a symlinked directory out of the fixture directory must be refused")
	}
}

func TestBodyFileSymlinkSwapRaceNeverReadsOutside(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "fx")
	_ = os.Mkdir(dir, 0o755)
	outside := filepath.Join(root, "outside.raw")
	_ = os.WriteFile(outside, []byte("outside-marker"), 0o600)
	inside := filepath.Join(dir, "body.raw")
	_ = os.WriteFile(inside, []byte("inside-marker"), 0o600)
	_ = os.WriteFile(filepath.Join(dir, "a.json"), []byte(`[{"provider":"github","method":"GET","path":"/x","status":200,"body_file":"body.raw"}]`), 0o600)
	if err := os.Symlink(outside, filepath.Join(root, "probe")); err != nil {
		t.Skip("no symlinks")
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = os.Remove(inside)
			if err := os.Symlink(outside, inside); err != nil {
				return
			}
			_ = os.Remove(inside)
			_ = os.WriteFile(inside, []byte("inside-marker"), 0o600)
		}
	}()
	defer func() { close(stop); <-done }()
	deadline := time.Now().Add(1500 * time.Millisecond)
	for time.Now().Before(deadline) {
		stub, err := LoadDir(dir)
		if err != nil {
			continue // a refused swap is fine
		}
		if got := get(t, stub, "api.github.com", "GET", "/x", nil).Body.String(); strings.Contains(got, "outside-marker") {
			t.Fatal("LoadDir read a file outside the fixture directory during a symlink swap")
		}
	}
}

func TestTenantNamesMustBeConcreteSingleLabelJiraHosts(t *testing.T) {
	for host, want := range map[string]string{
		"zz-venue.atlassian.net": "jira", "a1.atlassian.net": "jira",
		"*.atlassian.net": "", ".atlassian.net": "", "foo..atlassian.net": "", "a.b.atlassian.net": "", "-x.atlassian.net": "", "x-.atlassian.net": "",
		"evil-atlassian.net": "", "atlassian.net.evil.com": "",
	} {
		if got := ProviderFor(host); got != want {
			t.Errorf("ProviderFor(%q) = %q, want %q", host, got, want)
		}
	}
	ca, _ := NewCA(time.Hour)
	for _, host := range []string{"*.atlassian.net", ".atlassian.net", "foo..atlassian.net", "api.github.com."} {
		if _, _, err := ca.IssueServer([]string{"api.github.com", host}, time.Hour); err == nil {
			t.Errorf("IssueServer accepted %q", host)
		}
	}
}

func TestTrailingDotProviderHostIsAnswered(t *testing.T) {
	stub, _ := New(Fixture{Provider: "github", Method: "GET", Path: "/user", Status: 200, Body: json.RawMessage(`{}`)})
	for _, host := range []string{"api.github.com.", "api.github.com.:443", "API.GITHUB.COM."} {
		if got := get(t, stub, host, "GET", "/user", nil); got.Code != 200 {
			t.Errorf("Host %q answered %d, want 200", host, got.Code)
		}
	}
	if got := get(t, stub, "api.github.com..", "GET", "/user", nil); got.Code != 599 {
		t.Errorf("a doubled trailing dot answered %d, want 599", got.Code)
	}
}

func TestAdminHandlerRefusesAProviderHostname(t *testing.T) {
	stub, _ := New()
	for _, host := range []string{"api.github.com", "api.github.com:9090", "zz-venue.atlassian.net"} {
		if got := get(t, stub.AdminHandler(), host, "POST", "/reset", nil); got.Code != 404 {
			t.Errorf("admin via Host %q answered %d, want 404", host, got.Code)
		}
	}
	if got := get(t, stub.AdminHandler(), "127.0.0.1:9090", "GET", "/requests", nil); got.Code != 200 {
		t.Errorf("admin via loopback answered %d", got.Code)
	}
}

func TestRequestLogCarriesTheHostAndPathShape(t *testing.T) {
	stub, _ := New()
	var lines []string
	stub.Log = func(line string) { lines = append(lines, line) }
	get(t, stub, "gitlab.com", "GET", "/api/v4/runners/secretvalue?token=q-canary", nil)
	if len(lines) != 1 || !strings.Contains(lines[0], "host=gitlab.com") || !strings.Contains(lines[0], "path=/api/v4/") {
		t.Fatalf("log line = %q", lines)
	}
	if strings.Contains(lines[0], "secretvalue") || strings.Contains(lines[0], "q-canary") {
		t.Fatalf("log line carries a value: %s", lines[0])
	}
}

func TestCredentialTestFixturesAnswerEachCaseOnTheRightProvider(t *testing.T) {
	fixtures, err := LoadDir("fixtures")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		host, target string
		want         int
	}{
		{"api.github.com", "/c/ok/user", 200}, {"api.github.com", "/c/401/user", 401}, {"api.github.com", "/c/500/user", 500}, {"api.github.com", "/c/array/user", 200},
		{"gitlab.com", "/c/ok/api/v4/user", 200}, {"gitlab.com", "/c/401/api/v4/user", 401},
		{"zz-venue.atlassian.net", "/c/ok/rest/api/3/myself", 200}, {"zz-venue.atlassian.net", "/c/401/rest/api/3/myself", 401},
		{"gitlab.com", "/c/ok/user", 599}, {"api.github.com", "/c/ok/api/v4/user", 599},
	} {
		if got := get(t, fixtures, tc.host, "GET", tc.target, nil); got.Code != tc.want {
			t.Errorf("%s %s answered %d, want %d", tc.host, tc.target, got.Code, tc.want)
		}
	}
}
