package providerstub

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
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
	encoded, _ := json.Marshal(stub.Requests())
	if strings.Contains(string(encoded), secret) {
		t.Fatal("a credential value reached the recorder")
	}
	kinds := []string{}
	for _, r := range stub.Requests() {
		kinds = append(kinds, r.AuthKind)
	}
	if strings.Join(kinds, ",") != "token,PRIVATE-TOKEN,none" {
		t.Fatalf("auth kinds = %v", kinds)
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
	get(t, stub, "api.github.com", "GET", "/nope", nil)
	admin := stub.AdminHandler()
	var all, unmatched []Recorded
	if err := json.Unmarshal(get(t, admin, "x", "GET", "/requests", nil).Body.Bytes(), &all); err != nil || len(all) != 2 {
		t.Fatalf("requests: %v %v", all, err)
	}
	if err := json.Unmarshal(get(t, admin, "x", "GET", "/unmatched", nil).Body.Bytes(), &unmatched); err != nil || len(unmatched) != 1 || unmatched[0].Path != "/nope" {
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
