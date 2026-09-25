//go:build integration

package apiservice

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// probeStub is the provider both planes reach for the credential connection
// test. Responses are chosen by the credential the request carries (its token
// names the case), so one stub answers every provider and every outcome.
type probeStub struct {
	server *httptest.Server
	mu     sync.Mutex
	seen   []string
}

func newProbeStub(t *testing.T) *probeStub {
	t.Helper()
	stub := &probeStub{}
	stub.server = httptest.NewServer(http.HandlerFunc(stub.handle))
	t.Cleanup(stub.server.Close)
	return stub
}

func (s *probeStub) port() string {
	_, port, _ := net.SplitHostPort(strings.TrimPrefix(s.server.URL, "http://"))
	return port
}

func (s *probeStub) take() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := append([]string(nil), s.seen...)
	s.seen = nil
	sort.Strings(out)
	return out
}

func probeQuery(values url.Values) string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var parts []string
	for _, key := range keys {
		for _, value := range values[key] {
			parts = append(parts, url.QueryEscape(key)+"="+url.QueryEscape(value))
		}
	}
	return strings.Join(parts, "&")
}

func authKind(r *http.Request) string {
	if value := r.Header.Get("Authorization"); value != "" {
		kind, _, _ := strings.Cut(value, " ")
		if kind == "Bearer" || kind == "Basic" {
			return kind
		}
		return "raw"
	}
	if r.Header.Get("PRIVATE-TOKEN") != "" {
		return "private-token"
	}
	return "none"
}

// bearer returns the credential a request carries, whichever header holds it.
func probeCredential(r *http.Request) string {
	if value := r.Header.Get("PRIVATE-TOKEN"); value != "" {
		return value
	}
	value := r.Header.Get("Authorization")
	if rest, ok := strings.CutPrefix(value, "Bearer "); ok {
		return rest
	}
	if rest, ok := strings.CutPrefix(value, "Basic "); ok {
		if decoded, err := base64.StdEncoding.DecodeString(rest); err == nil {
			_, token, _ := strings.Cut(string(decoded), ":")
			return token
		}
	}
	return value
}

func (s *probeStub) handle(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	signature := r.Method + " " + r.URL.Path
	if encoded := probeQuery(query); encoded != "" {
		signature += "?" + encoded
	}
	signature += " auth=" + authKind(r)
	s.mu.Lock()
	s.seen = append(s.seen, signature)
	s.mu.Unlock()

	write := func(status int, body string) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}
	token := probeCredential(r)
	long := strings.Repeat("diagnostic detail ", 30)
	switch {
	case r.Method == "GET" && r.URL.Path == "/user": // GitHub PAT
		switch token {
		case "ghp_ok":
			write(200, `{"login":"octo","name":"Octo Cat"}`)
		case "ghp_nullname":
			write(200, `{"login":"octo","name":null}`)
		case "ghp_denied":
			write(401, `{"message":"Bad credentials: Authorization: Bearer ghp_abcdefghijklmnopqrstuvwxyz0123"}`)
		case "ghp_junk":
			write(200, `not json`)
		case "ghp_junk_open":
			write(200, `{`)
		case "ghp_junk_comma":
			write(200, "{\"login\":\"octo\",\n}")
		case "ghp_junk_colon":
			write(200, `{"login" "octo"}`)
		case "ghp_junk_extra":
			write(200, `{"login":"octo"} tail`)
		case "ghp_junk_cut":
			write(200, `{"login":"oc`)
		case "ghp_junk_escape":
			write(200, `{"login":"\q"}`)
		case "ghp_junk_utf":
			write(200, "{\"login\":\"\u00e9\u65e5\",\n \u65e5}")
		case "ghp_list":
			write(200, `[1,2]`)
		case "ghp_long":
			write(502, `{"message":"`+long+` token=secretvalue123 `+long+`"}`)
		case "ghp_redirect":
			w.Header().Set("Location", "/elsewhere")
			write(302, `moved`)
		default:
			write(403, `{"message":"forbidden"}`)
		}
	case r.Method == "GET" && r.URL.Path == "/installation/repositories":
		if token == "inst_ok" {
			write(200, `{"total_count":3,"repositories":[]}`)
			return
		}
		write(403, `{"message":"Resource not accessible by integration"}`)
	case r.Method == "POST" && strings.HasPrefix(r.URL.Path, "/app/installations/") && strings.HasSuffix(r.URL.Path, "/access_tokens"):
		switch strings.Split(r.URL.Path, "/")[3] {
		case "111":
			write(201, `{"token":"inst_ok","expires_at":"2099-01-01T00:00:00Z"}`)
		case "333":
			write(201, `{"expires_at":"2099-01-01T00:00:00Z"}`)
		case "444": // a transient exchange failure: GitHubAppTokenProvider retries it
			write(503, `{"message":"unavailable"}`)
		default:
			write(401, `{"message":"A JSON web token could not be decoded"}`)
		}
	case r.Method == "GET" && r.URL.Path == "/api/v4/user": // GitLab
		switch token {
		case "gl_ok":
			write(200, `{"username":"gluser","name":"GL User"}`)
		case "gl_partial":
			write(200, `{"id":1}`)
		default:
			write(401, `{"message":"401 Unauthorized"}`)
		}
	case r.Method == "GET" && r.URL.Path == "/rest/api/3/myself": // Jira
		switch token {
		case "jira_ok":
			write(200, `{"emailAddress":"j@example.test","displayName":"Jay"}`)
		default:
			write(401, `{"errorMessages":["Client must be authenticated"]}`)
		}
	case r.Method == "POST" && r.URL.Path == "/graphql": // Linear
		switch token {
		case "lin_ok":
			write(200, `{"data":{"viewer":{"id":"1","email":"l@example.test","name":"Lin"}}}`)
		case "lin_emptyobject":
			write(200, `{"data":{"viewer":{}}}`)
		case "lin_empty":
			write(200, `{"data":{"viewer":null}}`)
		case "lin_errors":
			write(200, `{"errors":[{"message":"nope"}]}`)
		default:
			write(400, `{"errors":[{"message":"Authentication required"}]}`)
		}
	case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/api/v2/flags/"): // LaunchDarkly
		project := strings.TrimPrefix(r.URL.Path, "/api/v2/flags/")
		if token != "ld_ok" {
			switch token {
			case "ld_forbidden":
				write(403, `{"message":"forbidden by policy"}`)
			case "ld_bad":
				write(400, `{"message":"bad request"}`)
			default:
				write(401, `{"message":"invalid access token"}`)
			}
			return
		}
		items := func(n int) string {
			parts := make([]string, n)
			for i := range parts {
				parts[i] = `{"key":"f"}`
			}
			return strings.Join(parts, ",")
		}
		switch project {
		case "big":
			if query.Get("offset") == "0" {
				write(200, `{"items":[`+items(50)+`],"totalCount":60}`)
			} else {
				write(200, `{"items":[`+items(10)+`],"totalCount":60}`)
			}
		case "nototal":
			write(200, `{"items":[`+items(2)+`]}`)
		default:
			write(200, `{"items":[`+items(3)+`],"totalCount":3}`)
		}
	default:
		write(404, `{"message":"venue stub: no route"}`)
	}
}

// probeHosts are the hostnames the planes send to the stub; probeAddresses
// the fixed public addresses they resolve to for the SSRF guard.
var (
	probeHosts     = map[string]bool{"provider.example.test": true, "api.github.com": true, "gitlab.com": true, "api.linear.app": true, "app.launchdarkly.com": true}
	probeAddresses = map[string]string{"provider.example.test": "93.184.216.34", "api.github.com": "140.82.112.5", "gitlab.com": "172.65.251.78"}
)

// rewriteToStub sends a request for one of probeHosts to the stub, keeping its
// path and query.
type rewriteToStub struct{ target *url.URL }

func (t rewriteToStub) RoundTrip(request *http.Request) (*http.Response, error) {
	if probeHosts[request.URL.Hostname()] {
		clone := request.Clone(request.Context())
		clone.URL.Scheme, clone.URL.Host, clone.Host = t.target.Scheme, t.target.Host, t.target.Host
		return http.DefaultTransport.RoundTrip(clone)
	}
	return http.DefaultTransport.RoundTrip(request)
}

func generatePEM(t *testing.T) string {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))
}

type probeSeedRow struct {
	provider, name string
	secrets        map[string]any
	config         string
	ciphertext     string // overrides secrets
	noPayload      bool
}

func probeSeedRows(pemKey string) []probeSeedRow {
	base := `{"base_url": "https://provider.example.test"}`
	return []probeSeedRow{
		{provider: "github", name: "default", secrets: map[string]any{"token": "ghp_ok"}},
		{provider: "github", name: "denied", secrets: map[string]any{"token": "ghp_denied"}},
		{provider: "github", name: "app", secrets: map[string]any{"app_id": "1", "private_key": pemKey, "installation_id": "111"}},
		{provider: "github", name: "app-refused", secrets: map[string]any{"appId": "1", "privateKey": pemKey, "installationId": "222"}},
		{provider: "github", name: "app-badkey", secrets: map[string]any{"app_id": "1", "private_key": "not a key", "installation_id": "111"}},
		{provider: "github", name: "app-notoken", secrets: map[string]any{"app_id": "1", "private_key": pemKey, "installation_id": "333"}},
		{provider: "github", name: "app-transient", secrets: map[string]any{"app_id": "1", "private_key": pemKey, "installation_id": "444"}},
		{provider: "github", name: "both", secrets: map[string]any{"token": "ghp_ok", "app_id": "1"}},
		{provider: "github", name: "ghe", secrets: map[string]any{"token": "ghp_ok", "base_url": "https://provider.example.test/ghe/api"}},
		{provider: "gitlab", name: "default", secrets: map[string]any{"token": "gl_ok"}},
		{provider: "gitlab", name: "denied", secrets: map[string]any{"token": "gl_bad", "url": "https://provider.example.test/gitlab/"}},
		{provider: "jira", name: "default", secrets: map[string]any{"email": "j@example.test", "api_token": "jira_ok", "base_url": "https://provider.example.test"}},
		{provider: "jira", name: "aliases", secrets: map[string]any{"email": "j@example.test", "token": "jira_ok", "url": "https://provider.example.test"}},
		{provider: "jira", name: "incomplete", secrets: map[string]any{"email": "j@example.test"}},
		{provider: "linear", name: "default", secrets: map[string]any{"apiKey": "lin_ok"}},
		{provider: "launchdarkly", name: "default", secrets: map[string]any{"api_key": "ld_ok", "project_key": "p1"}},
		{provider: "custom", name: "default", secrets: map[string]any{"a": "b"}},
		{provider: "github", name: "nopayload", noPayload: true},
		{provider: "github", name: "unreadable", ciphertext: "v1:not-a-fernet-token"},
		{provider: "github", name: "emptypayload", secrets: map[string]any{}},
		{provider: "gitlab", name: "cfg", secrets: map[string]any{"token": "gl_ok"}, config: base},
	}
}

func seedProbeCredentials(t *testing.T, ctx context.Context, admin *pgxpool.Pool, venue *venueoracle.Venue, org uuid.UUID, rows []probeSeedRow) map[string]string {
	t.Helper()
	var calls []venueoracle.PythonCall
	var callRows []int
	for i, row := range rows {
		if row.noPayload || row.ciphertext != "" {
			continue
		}
		encoded, err := json.Marshal(row.secrets)
		if err != nil {
			t.Fatal(err)
		}
		calls = append(calls, venueoracle.PythonCall{Target: "dev_health_ops.core.encryption:encrypt_value", Args: []any{string(encoded)}})
		callRows = append(callRows, i)
	}
	sealed := map[int]string{}
	for k, raw := range venue.CallPython(t, calls...) {
		var text string
		if err := json.Unmarshal(raw, &text); err != nil {
			t.Fatal(err)
		}
		sealed[callRows[k]] = text
	}
	ids := map[string]string{}
	for i, row := range rows {
		var ciphertext any
		switch {
		case row.noPayload:
		case row.ciphertext != "":
			ciphertext = row.ciphertext
		default:
			ciphertext = sealed[i]
		}
		config := "{}"
		if row.config != "" {
			config = row.config
		}
		id := uuid.New()
		if _, err := admin.Exec(ctx, `INSERT INTO integration_credentials (id, org_id, provider, name, is_active, credentials_encrypted, config, created_at, updated_at)
			VALUES ($1, $2, $3, $4, true, $5, $6::json, '2026-09-01T09:00:00Z', '2026-09-01T09:00:00Z')`,
			id, org.String(), row.provider, row.name, ciphertext, config); err != nil {
			t.Fatalf("seed %s/%s: %v", row.provider, row.name, err)
		}
		ids[row.provider+"/"+row.name] = id.String()
	}
	return ids
}

func probeRequests(tokens map[string]string, ids map[string]string, pemKey string) []venueoracle.Request {
	bearerH := func(name string) map[string]string {
		return map[string]string{"Authorization": "Bearer " + tokens[name], "Content-Type": "application/json"}
	}
	admin := bearerH("admin")
	var out []venueoracle.Request
	add := func(name string, headers map[string]string, body string) {
		out = append(out, venueoracle.Request{Name: name, Method: "POST", Path: "/api/v1/admin/credentials/test", Headers: headers, Body: venueoracle.B64(body)})
	}
	stored := func(name, provider, row string) {
		add(name, admin, fmt.Sprintf(`{"provider":%q,"name":%q}`, provider, row))
	}
	byID := func(name, row string, provider string) {
		add(name, admin, fmt.Sprintf(`{"provider":%q,"credential_id":%q}`, provider, ids[row]))
	}
	inline := func(name, provider, creds string) {
		add(name, admin, fmt.Sprintf(`{"provider":%q,"credentials":%s}`, provider, creds))
	}
	add("test: anonymous", map[string]string{"Content-Type": "application/json"}, `{"provider":"github"}`)
	add("test: member", bearerH("member"), `{"provider":"github"}`)
	add("test: bad json anonymous", map[string]string{"Content-Type": "application/json"}, `{`)
	add("test: body not an object", admin, `[1]`)
	add("test: empty object", admin, `{}`)
	add("test: wrong types", admin, `{"provider":5,"name":null,"credential_id":7,"credentials":[]}`)
	add("test: unknown stored credential", admin, `{"provider":"github","name":"nobody"}`)
	add("test: malformed credential id", admin, `{"provider":"github","credential_id":"not-a-uuid"}`)
	add("test: unknown credential id", admin, `{"provider":"github","credential_id":"11111111-1111-4111-8111-111111111111"}`)
	byID("test: credential without a payload", "github/nopayload", "github")
	byID("test: unreadable credential", "github/unreadable", "github")
	stored("test: stored row without a payload by name", "github", "nopayload")
	stored("test: stored unreadable row by name", "github", "unreadable")
	byID("test: stored empty payload by id", "github/emptypayload", "github")
	stored("test: stored empty payload by name", "github", "emptypayload")
	add("test: empty inline credentials fall back to the stored row", admin, `{"provider":"github","credentials":{}}`)
	add("test: null inline credentials fall back to the stored row", admin, `{"provider":"github","credentials":null}`)
	stored("test: unknown provider stored", "custom", "default")
	inline("test: unknown provider inline", "bitbucket", `{"a":1}`)

	// GitHub
	stored("test: github stored PAT", "github", "default")
	byID("test: github by id", "github/default", "github")
	byID("test: by id ignores the request's provider", "github/default", "gitlab")
	stored("test: github denied (body redacted)", "github", "denied")
	inline("test: github inline ok", "github", `{"token":"ghp_ok"}`)
	inline("test: github inline null name", "github", `{"token":"ghp_nullname"}`)
	inline("test: github inline junk body", "github", `{"token":"ghp_junk"}`)
	for _, kind := range []string{"open", "comma", "colon", "extra", "cut", "escape", "utf"} {
		inline("test: github malformed json body "+kind, "github", `{"token":"ghp_junk_`+kind+`"}`)
	}
	inline("test: github inline list body", "github", `{"token":"ghp_list"}`)
	inline("test: github inline long error body", "github", `{"token":"ghp_long"}`)
	inline("test: github inline redirect", "github", `{"token":"ghp_redirect"}`)
	inline("test: github inline forbidden", "github", `{"token":"ghp_other"}`)
	inline("test: github inline nothing usable", "github", `{"note":"x"}`)
	inline("test: github inline null values only", "github", `{"token":null}`)
	inline("test: github inline token and app fields", "github", `{"token":"ghp_ok","app_id":"1"}`)
	inline("test: github inline incomplete app", "github", `{"app_id":"1","private_key":"k"}`)
	inline("test: github inline empty token", "github", `{"token":""}`)
	stored("test: github token and app fields stored", "github", "both")
	stored("test: github enterprise base url", "github", "ghe")
	inline("test: github base url blocked scheme", "github", `{"token":"ghp_ok","base_url":"ftp://provider.example.test"}`)
	inline("test: github base url localhost", "github", `{"token":"ghp_ok","baseUrl":"http://localhost:9"}`)
	inline("test: github base url private", "github", `{"token":"ghp_ok","base_url":"http://10.0.0.5"}`)
	inline("test: github base url with userinfo", "github", `{"token":"ghp_ok","base_url":"https://u:p@provider.example.test"}`)
	inline("test: github base url unresolvable", "github", `{"token":"ghp_ok","base_url":"https://unresolvable.invalid"}`)
	inline("test: github base url query is dropped", "github", `{"token":"ghp_ok","base_url":"https://provider.example.test/x?y=1#z"}`)
	stored("test: github App installation", "github", "app")
	stored("test: github App token exchange refused", "github", "app-refused")
	stored("test: github App with an unusable key", "github", "app-badkey")
	stored("test: github App token response without a token", "github", "app-notoken")
	stored("test: github App token exchange 5xx is retried", "github", "app-transient")
	inline("test: github App inline", "github", fmt.Sprintf(`{"app_id":"9","private_key":%q,"installation_id":"111"}`, pemKey))

	// GitLab
	stored("test: gitlab stored", "gitlab", "default")
	stored("test: gitlab denied with a subpath url", "gitlab", "denied")
	stored("test: gitlab url in config is not read", "gitlab", "cfg")
	inline("test: gitlab inline ok", "gitlab", `{"token":"gl_ok"}`)
	inline("test: gitlab inline partial body", "gitlab", `{"token":"gl_partial"}`)
	inline("test: gitlab inline no token", "gitlab", `{"url":"https://provider.example.test"}`)
	inline("test: gitlab inline url alias precedence", "gitlab", `{"token":"gl_ok","gitlab_url":"https://provider.example.test/a","url":"http://localhost","base_url":"http://10.0.0.1"}`)
	inline("test: gitlab inline private url", "gitlab", `{"token":"gl_ok","base_url":"http://192.168.1.1"}`)
	inline("test: gitlab inline null values", "gitlab", `{"token":"gl_ok","url":null}`)

	// Jira
	stored("test: jira stored", "jira", "default")
	stored("test: jira stored with aliases", "jira", "aliases")
	stored("test: jira stored incomplete", "jira", "incomplete")
	inline("test: jira inline denied", "jira", `{"email":"j@example.test","api_token":"nope","base_url":"https://provider.example.test"}`)
	inline("test: jira inline api_token outranks the token alias", "jira", `{"email":"j@example.test","api_token":"jira_ok","token":"nope","base_url":"https://provider.example.test"}`)
	inline("test: jira inline camelCase", "jira", `{"email":"j@example.test","apiToken":"jira_ok","baseUrl":"https://provider.example.test"}`)
	inline("test: jira inline server_url", "jira", `{"email":"j@example.test","token":"jira_ok","server_url":"https://provider.example.test/jira/"}`)
	inline("test: jira inline private url", "jira", `{"email":"j@example.test","api_token":"jira_ok","base_url":"http://172.16.0.1"}`)
	inline("test: jira inline blocked scheme", "jira", `{"email":"j@example.test","api_token":"jira_ok","base_url":"file:///etc/passwd"}`)
	inline("test: jira inline no email", "jira", `{"api_token":"jira_ok","base_url":"https://provider.example.test"}`)
	inline("test: jira inline empty values", "jira", `{"email":"","api_token":"","base_url":""}`)

	// Linear
	stored("test: linear stored", "linear", "default")
	inline("test: linear inline snake case key", "linear", `{"api_key":"lin_ok"}`)
	inline("test: linear inline empty viewer", "linear", `{"apiKey":"lin_empty"}`)
	inline("test: linear inline empty viewer object", "linear", `{"apiKey":"lin_emptyobject"}`)
	inline("test: linear inline graphql errors", "linear", `{"apiKey":"lin_errors"}`)
	inline("test: linear inline rejected", "linear", `{"apiKey":"lin_bad"}`)
	inline("test: linear inline no key", "linear", `{"apiKey":""}`)
	inline("test: linear inline non-string key", "linear", `{"apiKey":5}`)

	// LaunchDarkly
	stored("test: launchdarkly stored", "launchdarkly", "default")
	inline("test: launchdarkly two pages", "launchdarkly", `{"api_key":"ld_ok","project_key":"big"}`)
	inline("test: launchdarkly no total", "launchdarkly", `{"api_key":"ld_ok","project_key":"nototal"}`)
	inline("test: launchdarkly unauthorised", "launchdarkly", `{"api_key":"ld_bad_key","project_key":"p1"}`)
	inline("test: launchdarkly forbidden", "launchdarkly", `{"api_key":"ld_forbidden","project_key":"p1"}`)
	inline("test: launchdarkly bad request", "launchdarkly", `{"api_key":"ld_bad","project_key":"p1"}`)
	inline("test: launchdarkly missing project key", "launchdarkly", `{"api_key":"ld_ok"}`)
	inline("test: launchdarkly non-string key", "launchdarkly", `{"api_key":5,"project_key":"p1"}`)

	add("test: GET is not a route", map[string]string{"Authorization": "Bearer " + tokens["admin"]}, ``)
	out[len(out)-1].Method, out[len(out)-1].Body = "GET", nil
	return out
}

// TestVenueOracleCredentialConnectionTest is the connection test's
// differential: the real Python api and this Go api answer every request the
// same, send the same requests to the provider stub, and leave the same
// last_test_* columns on the rows a test records into.
func TestVenueOracleCredentialConnectionTest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	stub := newProbeStub(t)
	stubURL, err := url.Parse(stub.server.URL)
	if err != nil {
		t.Fatal(err)
	}
	pemKey := generatePEM(t)
	_, thisFile, _, _ := runtime.Caller(0)
	siteDir := filepath.Join(filepath.Dir(thisFile), "testdata", "provider_stub")

	previousClient, previousLookup := credentialProbeClient, credentialHostLookup
	credentialProbeClient = &http.Client{Transport: rewriteToStub{target: stubURL},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	credentialHostLookup = func(ctx context.Context, host string) ([]netip.Addr, error) {
		if address, ok := probeAddresses[host]; ok {
			return []netip.Addr{netip.MustParseAddr(address)}, nil
		}
		if literal, err := netip.ParseAddr(host); err == nil {
			return []netip.Addr{literal}, nil
		}
		return nil, fmt.Errorf("lookup %s: no such host", host)
	}
	t.Cleanup(func() { credentialProbeClient, credentialHostLookup = previousClient, previousLookup })

	var seed venueFixture
	var ids map[string]string
	rows := probeSeedRows(pemKey)
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root: venueRoot(), JWTKey: venueKey, Logger: quietLogger(),
		PythonEnv: []string{
			"SETTINGS_ENCRYPTION_KEY=" + credentialsVenueKey,
			"VENUE_PROVIDER_STUB_PORT=" + stub.port(),
			"PYTHONPATH=" + siteDir + ":" + filepath.Join(venueRoot(), "src"),
		},
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, venue *venueoracle.Venue) map[string]map[string]any {
			seed = venueSeed(t, ctx, admin)
			ids = seedProbeCredentials(t, ctx, admin, venue, seed.orgA, rows)
			return seed.tokenSpecs()
		},
	})
	cfg := config.Config{
		APIAddress: "127.0.0.1:0", RiverDatabaseSchema: "river", APIDatabaseRole: venue.Roles["api"],
		APIDatabaseURI: secrets.NewValue(venue.GoAPIDatabaseURI(t)),
		APIJWTSecret:   secrets.NewValue(venueKey), APIJWTIssuer: "dev-health-ops", APIJWTAudience: "dev-health-api",
		CORSAllowedOrigins:    []string{"http://localhost:3000"},
		SettingsEncryptionKey: secrets.NewValue(credentialsVenueKey),
	}
	base := startVenueAPI(t, ctx, cfg, venue)
	requests := probeRequests(venue.Tokens, ids, pemKey)
	pythonResponses := venue.ServePython(t, requests)
	pythonProvider := stub.take()
	receipt := venueoracle.Diff(t, base, requests, pythonResponses, venueoracle.DiffOptions{})
	goProvider := stub.take()

	// PagerDuty is the one provider this plane does not serve: 501 for an inline
	// test whatever its auth mode (a by-name test of an absent row is the
	// same 404 as Python's).
	for _, body := range []string{
		`{"provider":"pagerduty","credentials":{"auth_mode":"client_credentials","client_id":"c","client_secret":"s","subdomain":"acme","region":"us"}}`,
		`{"provider":"pagerduty","name":"default","credentials":{"auth_mode":"oauth","oauth_credential_name":"x","oauth_binding_id":"y"}}`,
	} {
		pagerDuty := venueoracle.Do(t, base, venueoracle.Request{Name: "pagerduty test is not served", Method: "POST",
			Path: "/api/v1/admin/credentials/test", Body: venueoracle.B64(body),
			Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens["admin"], "Content-Type": "application/json"}})
		if pagerDuty.Status != http.StatusNotImplemented || !strings.Contains(pagerDuty.Body, "not served by this API plane") {
			t.Errorf("pagerduty test %s answered %d %s, want 501", body, pagerDuty.Status, pagerDuty.Body)
		}
	}

	providerSame := len(goProvider) > 0 && stripJWT(pythonProvider) == stripJWT(goProvider)
	if !providerSame {
		t.Errorf("provider requests differ:\n python:\n%s\n go:\n%s", strings.Join(pythonProvider, "\n"), strings.Join(goProvider, "\n"))
	}

	rowsByPlane := map[string]string{}
	for _, plane := range []struct{ name, uri string }{{"python", venue.AdminURI(t, venue.SourceDB)}, {"go", venue.AdminURI(t, venue.GoDB)}} {
		pool, err := pgxpool.New(ctx, plane.uri)
		if err != nil {
			t.Fatal(err)
		}
		result, err := pool.Query(ctx, `SELECT provider, name, coalesce(last_test_success::text, '<null>'), coalesce(last_test_error, '<null>'),
			last_test_at IS NOT NULL, updated_at > created_at FROM integration_credentials ORDER BY provider, name`)
		if err != nil {
			t.Fatal(err)
		}
		var lines []string
		for result.Next() {
			var provider, name, success, lastError string
			var tested, touched bool
			if err := result.Scan(&provider, &name, &success, &lastError, &tested, &touched); err != nil {
				t.Fatal(err)
			}
			lines = append(lines, fmt.Sprintf("%s/%s success=%s error=%s tested=%v touched=%v", provider, name, success, lastError, tested, touched))
		}
		result.Close()
		pool.Close()
		rowsByPlane[plane.name] = strings.Join(lines, "\n")
	}
	rowsSame := rowsByPlane["python"] == rowsByPlane["go"]
	if !rowsSame {
		t.Errorf("last_test_* columns differ:\n python:\n%s\n go:\n%s", rowsByPlane["python"], rowsByPlane["go"])
	}
	t.Logf("\n%sprovider requests (%d): %s\nlast_test_* columns: %s\n", receipt, len(goProvider), venueoracle.Mark(providerSame), venueoracle.Mark(rowsSame))
}

// stripJWT drops the App installation exchange's request signature detail:
// nothing in it varies, but the two planes' JWTs do, and the stub records
// only the scheme, so the lists compare directly.
func stripJWT(lines []string) string { return strings.Join(lines, "\n") }
