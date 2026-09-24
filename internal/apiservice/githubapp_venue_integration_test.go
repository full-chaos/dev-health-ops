//go:build integration

package apiservice

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/githubapp"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// githubStub is github.com and api.github.com for both planes. The OAuth code
// a request carries picks the installer token, and the token picks the
// installations answer, so one stub serves every outcome.
type githubStub struct {
	server *httptest.Server
	mu     sync.Mutex
	seen   []string
}

func newGitHubStub(t *testing.T) *githubStub {
	t.Helper()
	stub := &githubStub{}
	stub.server = httptest.NewServer(http.HandlerFunc(stub.handle))
	t.Cleanup(stub.server.Close)
	return stub
}

func (s *githubStub) take() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := append([]string(nil), s.seen...)
	s.seen = nil
	return out
}

func (s *githubStub) handle(w http.ResponseWriter, r *http.Request) {
	write := func(status int, body string) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}
	if r.Method == http.MethodPost && r.URL.Path == "/login/oauth/access_token" {
		raw, _ := io.ReadAll(r.Body)
		form, _ := url.ParseQuery(string(raw))
		s.mu.Lock()
		s.seen = append(s.seen, fmt.Sprintf("POST %s accept=%s client_id=%s client_secret=%s code=%s",
			r.URL.Path, r.Header.Get("Accept"), form.Get("client_id"), form.Get("client_secret"), form.Get("code")))
		s.mu.Unlock()
		switch code := form.Get("code"); code {
		case "denied":
			write(401, `{"error":"bad_verification_code"}`)
		case "notoken":
			write(200, `{"error":"bad_verification_code"}`)
		case "emptytoken":
			write(200, `{"access_token":""}`)
		case "numtoken":
			write(200, `{"access_token":5}`)
		case "badjson":
			write(200, `not json`)
		case "list":
			write(200, `[1]`)
		case "redirect":
			w.Header().Set("Location", "/elsewhere")
			write(302, ``)
		default:
			write(200, `{"access_token":"tok-`+code+`","token_type":"bearer"}`)
		}
		return
	}
	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	s.mu.Lock()
	s.seen = append(s.seen, fmt.Sprintf("%s %s?%s auth=%s accept=%s version=%s", r.Method, r.URL.Path, r.URL.RawQuery, token,
		r.Header.Get("Accept"), r.Header.Get("X-GitHub-Api-Version")))
	s.mu.Unlock()
	if r.Method != http.MethodGet || r.URL.Path != "/user/installations" {
		write(404, `{}`)
		return
	}
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	switch token {
	case "tok-good":
		write(200, `{"installations":[
			{"id":101,"account":{"login":"octo-org","type":"Organization"}},
			{"id":102,"account":null},
			{"id":103,"account":{"login":5,"type":null}},
			{"id":104,"account":"x"},
			"not an object",
			{"id":202,"account":{"login":"taken","type":"User"}},
			{"id":203,"account":{"login":"free","type":"User"}},
			{"id":204,"account":{"login":"back","type":"Organization"}},
			{"id":401.0,"account":{"login":"float-id","type":"User"}},
			{"id":true,"account":{"login":"bool-id","type":"User"}},
			{"id":"301","account":{"login":"string-id","type":"User"}}]}`)
	case "tok-many":
		var items []string
		if page <= 1 {
			for i := 0; i < 100; i++ {
				items = append(items, fmt.Sprintf(`{"id":%d,"account":{"login":"bulk-%d","type":"User"}}`, 1000+i, i))
			}
		} else {
			items = append(items, `{"id":1150,"account":{"login":"second-page","type":"Organization"}}`)
		}
		write(200, `{"installations":[`+strings.Join(items, ",")+`]}`)
	case "tok-noaccess":
		write(200, `{"installations":[]}`)
	case "tok-badlist":
		write(200, `{"installations":"x"}`)
	case "tok-apierr":
		write(401, `{"message":"Bad credentials"}`)
	case "tok-apiobj":
		write(200, `[1]`)
	default:
		write(200, `{"installations":[{"id":101,"account":{"login":"octo-org","type":"Organization"}}]}`)
	}
}

// githubStateClaims are the claims Mint signs, for a test to break one.
func githubStateClaims(org string, now time.Time) jwt.MapClaims {
	return jwt.MapClaims{
		"org_id": org, "jti": uuid.NewString(), "purpose": "github_app_install", "iss": "dev-health-ops", "aud": "dev-health-api",
		"iat": now.Unix(), "exp": now.Add(15 * time.Minute).Unix(),
	}
}

func signGitHubState(t *testing.T, method jwt.SigningMethod, key any, claims jwt.MapClaims) string {
	t.Helper()
	signed, err := jwt.NewWithClaims(method, claims).SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

type githubVenueVariant struct {
	name string
	env  map[string]string
}

var githubStateSecret = []byte(venueKey)

var stateQuery = regexp.MustCompile(`state=[^&"]+`)

func githubAppRequests(t *testing.T, tokens map[string]string, org, otherOrg, pythonState string, full bool) []venueoracle.Request {
	t.Helper()
	admin := map[string]string{"Authorization": "Bearer " + tokens["admin"], "Content-Type": "application/json"}
	var out []venueoracle.Request
	add := func(name, path string, headers map[string]string, body string) {
		out = append(out, venueoracle.Request{Name: name, Method: "POST", Path: "/api/v1/admin/integrations/github/" + path, Headers: headers, Body: venueoracle.B64(body)})
	}
	now := time.Now()
	signer := githubapp.Signer{Secret: venueKey, Issuer: "dev-health-ops", Audience: "dev-health-api"}
	fresh := func(returnTo *string) string {
		state, err := signer.Mint(org, returnTo, now)
		if err != nil {
			t.Fatal(err)
		}
		return state
	}
	ptr := func(s string) *string { return &s }
	callback := func(name, state string, installationID int, code string, extra string) {
		body := fmt.Sprintf(`{"installation_id":%d,"state":%q,"code":%q%s}`, installationID, state, code, extra)
		add(name, "install-callback", admin, body)
	}

	add("url: anonymous", "install-url", map[string]string{"Content-Type": "application/json"}, `{}`)
	add("url: member", "install-url", map[string]string{"Authorization": "Bearer " + tokens["member"], "Content-Type": "application/json"}, `{}`)
	add("url: no body", "install-url", admin, ``)
	add("url: null body", "install-url", admin, `null`)
	add("url: empty object", "install-url", admin, `{}`)
	add("url: onboarding return_to", "install-url", admin, `{"return_to":"/auth/onboard/integration"}`)
	add("url: admin return_to", "install-url", admin, `{"return_to":"/org/admin/integrations/github"}`)
	add("url: foreign return_to", "install-url", admin, `{"return_to":"https://evil.example/x"}`)
	add("url: null return_to", "install-url", admin, `{"return_to":null}`)
	add("url: wrong type return_to", "install-url", admin, `{"return_to":5}`)
	add("url: not an object", "install-url", admin, `[1]`)
	add("url: bad json", "install-url", admin, `{`)
	if !full {
		callback("callback: not configured", fresh(nil), 101, "good", "")
		return out
	}

	add("callback: anonymous", "install-callback", map[string]string{"Content-Type": "application/json"}, `{}`)
	add("callback: member", "install-callback", map[string]string{"Authorization": "Bearer " + tokens["member"], "Content-Type": "application/json"}, `{}`)
	add("callback: no body", "install-callback", admin, ``)
	add("callback: not an object", "install-callback", admin, `[1]`)
	add("callback: empty object", "install-callback", admin, `{}`)
	add("callback: wrong types", "install-callback", admin, `{"installation_id":"x","state":5,"code":5,"setup_action":5}`)
	add("callback: installation id zero", "install-callback", admin, `{"installation_id":0,"state":"x"}`)
	add("callback: installation id true", "install-callback", admin, `{"installation_id":true,"state":"x"}`)
	add("callback: installation id float", "install-callback", admin, `{"installation_id":101.0,"state":"x"}`)
	add("callback: installation id fraction", "install-callback", admin, `{"installation_id":101.5,"state":"x"}`)
	add("callback: null state", "install-callback", admin, `{"installation_id":101,"state":null}`)

	// State defects, each answered before any GitHub call.
	callback("callback: garbage state", "not-a-jwt", 101, "good", "")
	callback("callback: empty state", "", 101, "good", "")
	tampered := fresh(nil)
	callback("callback: tampered signature", tampered[:len(tampered)-2]+"xx", 101, "good", "")
	claims := func(mutate func(jwt.MapClaims)) string {
		c := githubStateClaims(org, now)
		mutate(c)
		return signGitHubState(t, jwt.SigningMethodHS256, githubStateSecret, c)
	}
	callback("callback: expired state", claims(func(c jwt.MapClaims) { c["exp"] = now.Add(-time.Hour).Unix() }), 101, "good", "")
	callback("callback: future iat", claims(func(c jwt.MapClaims) { c["iat"] = now.Add(time.Hour).Unix() }), 101, "good", "")
	callback("callback: not yet valid", claims(func(c jwt.MapClaims) { c["nbf"] = now.Add(time.Hour).Unix() }), 101, "good", "")
	callback("callback: wrong issuer", claims(func(c jwt.MapClaims) { c["iss"] = "someone-else" }), 101, "good", "")
	callback("callback: wrong audience", claims(func(c jwt.MapClaims) { c["aud"] = "someone-else" }), 101, "good", "")
	callback("callback: audience list", claims(func(c jwt.MapClaims) { c["aud"] = []string{"x", "dev-health-api"} }), 101, "good", "")
	callback("callback: no audience", claims(func(c jwt.MapClaims) { delete(c, "aud") }), 101, "good", "")
	callback("callback: no issuer", claims(func(c jwt.MapClaims) { delete(c, "iss") }), 101, "good", "")
	callback("callback: no expiry", claims(func(c jwt.MapClaims) { delete(c, "exp") }), 101, "denied", "")
	callback("callback: wrong purpose", claims(func(c jwt.MapClaims) { c["purpose"] = "access" }), 101, "good", "")
	callback("callback: no purpose", claims(func(c jwt.MapClaims) { delete(c, "purpose") }), 101, "good", "")
	callback("callback: no org", claims(func(c jwt.MapClaims) { delete(c, "org_id") }), 101, "good", "")
	callback("callback: empty org", claims(func(c jwt.MapClaims) { c["org_id"] = "" }), 101, "good", "")
	callback("callback: numeric org", claims(func(c jwt.MapClaims) { c["org_id"] = 5 }), 101, "good", "")
	callback("callback: no jti", claims(func(c jwt.MapClaims) { delete(c, "jti") }), 101, "good", "")
	callback("callback: empty jti", claims(func(c jwt.MapClaims) { c["jti"] = "" }), 101, "good", "")
	callback("callback: numeric jti", claims(func(c jwt.MapClaims) { c["jti"] = 5 }), 101, "good", "")
	callback("callback: numeric subject", claims(func(c jwt.MapClaims) { c["sub"] = 5 }), 101, "denied", "")
	callback("callback: string subject", claims(func(c jwt.MapClaims) { c["sub"] = "someone" }), 101, "denied", "")
	callback("callback: string expiry", claims(func(c jwt.MapClaims) { c["exp"] = "soon" }), 101, "denied", "")
	callback("callback: string issued-at", claims(func(c jwt.MapClaims) { c["iat"] = "now" }), 101, "denied", "")
	callback("callback: string not-before", claims(func(c jwt.MapClaims) { c["nbf"] = "now" }), 101, "denied", "")
	callback("callback: numeric issuer", claims(func(c jwt.MapClaims) { c["iss"] = 5 }), 101, "denied", "")
	callback("callback: numeric audience", claims(func(c jwt.MapClaims) { c["aud"] = 5 }), 101, "denied", "")
	callback("callback: audience list of numbers", claims(func(c jwt.MapClaims) { c["aud"] = []int{1} }), 101, "denied", "")
	callback("callback: fractional expiry", claims(func(c jwt.MapClaims) { c["exp"] = float64(now.Add(time.Hour).Unix()) + 0.5 }), 101, "denied", "")
	callback("callback: expiry just passed", claims(func(c jwt.MapClaims) { c["exp"] = now.Unix() - 1 }), 101, "denied", "")
	callback("callback: other algorithm", signGitHubState(t, jwt.SigningMethodHS512, githubStateSecret, githubStateClaims(org, now)), 101, "good", "")
	callback("callback: signed with another secret", signGitHubState(t, jwt.SigningMethodHS256, []byte("another-venue-secret-another-venue-secret"), githubStateClaims(org, now)), 101, "good", "")
	callback("callback: other org", claims(func(c jwt.MapClaims) { c["org_id"] = otherOrg }), 101, "good", "")
	add("callback: no code", "install-callback", admin, fmt.Sprintf(`{"installation_id":101,"state":%q}`, fresh(nil)))
	add("callback: null code", "install-callback", admin, fmt.Sprintf(`{"installation_id":101,"state":%q,"code":null}`, fresh(nil)))
	callback("callback: empty code", fresh(nil), 101, "", "")

	// GitHub answers.
	callback("callback: code denied", fresh(nil), 101, "denied", "")
	callback("callback: no access token", fresh(nil), 101, "notoken", "")
	callback("callback: empty access token", fresh(nil), 101, "emptytoken", "")
	callback("callback: numeric access token", fresh(nil), 101, "numtoken", "")
	callback("callback: token answer not json", fresh(nil), 101, "badjson", "")
	callback("callback: token answer a list", fresh(nil), 101, "list", "")
	callback("callback: token answer redirects", fresh(nil), 101, "redirect", "")
	callback("callback: installer sees nothing", fresh(nil), 101, "noaccess", "")
	callback("callback: installations not a list", fresh(nil), 101, "badlist", "")
	callback("callback: installations refused", fresh(nil), 101, "apierr", "")
	callback("callback: installations answer a list", fresh(nil), 101, "apiobj", "")
	callback("callback: installation not accessible", fresh(nil), 999, "good", "")
	callback("callback: installation on the second page", fresh(ptr("/auth/onboard/integration")), 1150, "many", "")
	callback("callback: installation missing from all pages", fresh(nil), 1200, "many", "")

	// Successful installs and claims.
	good := fresh(nil)
	callback("callback: install", good, 101, "good", "")
	callback("callback: state replay", good, 101, "good", "")
	callback("callback: state minted by the Python plane", pythonState, 101, "good", `,"setup_action":"install"`)
	callback("callback: install again keeps the credential", fresh(nil), 101, "good", "")
	callback("callback: setup action", fresh(ptr("/auth/onboard/integration")), 101, "good", `,"setup_action":"update"`)
	callback("callback: foreign return_to in the state", fresh(ptr("https://evil.example/x")), 101, "good", "")
	callback("callback: numeric return_to in the state", claims(func(c jwt.MapClaims) { c["return_to"] = 5 }), 101, "good", "")
	callback("callback: account null", fresh(nil), 102, "good", "")
	callback("callback: account fields of the wrong type", fresh(nil), 103, "good", "")
	callback("callback: account not an object", fresh(nil), 104, "good", "")
	callback("callback: installation owned by another org", fresh(nil), 202, "good", "")
	callback("callback: unowned installation is claimed", fresh(nil), 203, "good", "")
	callback("callback: own suspended installation is revived", fresh(nil), 204, "good", "")
	callback("callback: float installation id from GitHub", fresh(nil), 401, "good", "")
	callback("callback: boolean installation id from GitHub", fresh(nil), 1, "good", "")
	callback("callback: string installation id from GitHub", fresh(nil), 301, "good", "")
	return out
}

func normalizeGitHubState(_ venueoracle.Request, body string) string {
	return stateQuery.ReplaceAllString(body, "state=STATE")
}

// runGitHubAppVariant sets the process environment both planes read, starts
// a Go api under it, and diffs the requests.
func runGitHubAppVariant(t *testing.T, ctx context.Context, venue *venueoracle.Venue, stub *githubStub, name string, env map[string]string, full bool, orgs [2]string) string {
	t.Helper()
	for _, key := range []string{"GITHUB_APP_SLUG", "GITHUB_APP_ID", "GITHUB_APP_PRIVATE_KEY", "GITHUB_APP_PRIVATE_KEY_PATH", "GITHUB_APP_CLIENT_ID", "GITHUB_APP_CLIENT_SECRET", "GITHUB_APP_CALLBACK_URL"} {
		t.Setenv(key, env[key])
	}
	cfg := config.Config{
		APIAddress: "127.0.0.1:0", RiverDatabaseSchema: "river", APIDatabaseRole: venue.Roles["api"],
		APIDatabaseURI: secrets.NewValue(venue.GoAPIDatabaseURI(t)),
		APIJWTSecret:   secrets.NewValue(venueKey), APIJWTIssuer: "dev-health-ops", APIJWTAudience: "dev-health-api",
		CORSAllowedOrigins:    []string{"http://localhost:3000"},
		SettingsEncryptionKey: secrets.NewValue(credentialsVenueKey),
		ValkeyURI:             secrets.NewValue(venue.ValkeyURI),
	}
	base := startVenueAPI(t, ctx, cfg, venue)
	var pythonState string
	if raw := venue.CallPython(t, venueoracle.PythonCall{Target: "dev_health_ops.api.integrations.github_app_state:mint_github_app_install_state", Args: []any{orgs[0], "/auth/onboard/integration"}}); len(raw) == 1 {
		if err := json.Unmarshal(raw[0], &pythonState); err != nil || pythonState == "" {
			t.Fatalf("mint a state on the Python plane: %v (%s)", err, raw[0])
		}
	}
	requests := githubAppRequests(t, venue.Tokens, orgs[0], orgs[1], pythonState, full)
	pythonResponses := venue.ServePython(t, requests)
	pythonSeen := stub.take()
	receipt := venueoracle.Diff(t, base, requests, pythonResponses, venueoracle.DiffOptions{Normalize: normalizeGitHubState})
	goSeen := stub.take()
	same := strings.Join(pythonSeen, "\n") == strings.Join(goSeen, "\n")
	if !same {
		t.Errorf("%s: GitHub requests differ:\n python:\n%s\n go:\n%s", name, strings.Join(pythonSeen, "\n"), strings.Join(goSeen, "\n"))
	}
	return fmt.Sprintf("--- %s\n%sGitHub requests (%d): %s\n", name, receipt, len(goSeen), venueoracle.Mark(same && (!full || len(goSeen) > 0)))
}

// TestVenueOracleGitHubAppInstall is the GitHub App install routes'
// differential: the real Python api and this Go api answer every request the
// same, send GitHub the same requests, and leave the same installation and
// credential rows -- under a fully configured App and under each way the App
// can be misconfigured.
func TestVenueOracleGitHubAppInstall(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	stub := newGitHubStub(t)
	stubURL, err := url.Parse(stub.server.URL)
	if err != nil {
		t.Fatal(err)
	}
	siteDir := venueStubDir()
	previous := githubAppHTTPClient
	githubAppHTTPClient = &http.Client{Transport: rewriteToStub{target: stubURL},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	t.Cleanup(func() { githubAppHTTPClient = previous })

	var seed venueFixture
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root: venueRoot(), JWTKey: venueKey, Logger: quietLogger(),
		PythonEnv: []string{
			"SETTINGS_ENCRYPTION_KEY=" + credentialsVenueKey,
			"VENUE_PROVIDER_STUB_PORT=" + stubPort(stub.server.URL),
			"PYTHONPATH=" + siteDir + ":" + filepath.Join(venueRoot(), "src"),
		},
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, venue *venueoracle.Venue) map[string]map[string]any {
			seed = venueSeed(t, ctx, admin)
			for _, row := range []struct {
				id  int
				org any
				at  string
			}{{202, seed.orgB.String(), "NULL"}, {203, nil, "NULL"}, {204, seed.orgA.String(), "now() - interval '1 day'"}} {
				if _, err := admin.Exec(ctx, `INSERT INTO github_app_installations (id, installation_id, org_id, suspended_at, created_at, updated_at)
					VALUES (gen_random_uuid(), $1, $2, `+row.at+`, now() - interval '2 days', now() - interval '2 days')`, row.id, row.org); err != nil {
					t.Fatal(err)
				}
			}
			return seed.tokenSpecs()
		},
	})
	pemKey := strings.ReplaceAll(generatePEM(t), "\n", `\n`)
	full := map[string]string{
		"GITHUB_APP_SLUG": "dev health app/v2", "GITHUB_APP_ID": "12345", "GITHUB_APP_PRIVATE_KEY": pemKey,
		"GITHUB_APP_CLIENT_ID": "Iv1.client", "GITHUB_APP_CLIENT_SECRET": "client-secret", "GITHUB_APP_CALLBACK_URL": "https://app.example.test/cb?x=1&y=é",
	}
	orgs := [2]string{seed.orgA.String(), seed.orgB.String()}
	var receipt strings.Builder
	receipt.WriteString(runGitHubAppVariant(t, ctx, venue, stub, "configured", full, true, orgs))
	compareGitHubAppRows(t, ctx, venue, &receipt)

	noCallback := map[string]string{}
	for key, value := range full {
		noCallback[key] = value
	}
	noCallback["GITHUB_APP_CALLBACK_URL"] = ""
	receipt.WriteString(runGitHubAppVariant(t, ctx, venue, stub, "no callback URL", noCallback, false, orgs))
	receipt.WriteString(runGitHubAppVariant(t, ctx, venue, stub, "nothing configured", map[string]string{}, false, orgs))
	receipt.WriteString(runGitHubAppVariant(t, ctx, venue, stub, "no OAuth client", map[string]string{
		"GITHUB_APP_SLUG": "app", "GITHUB_APP_ID": "12345", "GITHUB_APP_PRIVATE_KEY": pemKey,
	}, false, orgs))
	receipt.WriteString(runGitHubAppVariant(t, ctx, venue, stub, "unreadable key file", map[string]string{
		"GITHUB_APP_SLUG": "app", "GITHUB_APP_ID": "12345", "GITHUB_APP_PRIVATE_KEY_PATH": "/nonexistent/github-app.pem",
		"GITHUB_APP_CLIENT_ID": "c", "GITHUB_APP_CLIENT_SECRET": "s",
	}, false, orgs))
	t.Logf("\n%s", receipt.String())
}

// compareGitHubAppRows compares the installation rows and the github-app
// credential each plane holds after the configured run, the credential by its
// decrypted payload (Fernet ciphertexts differ by construction).
func compareGitHubAppRows(t *testing.T, ctx context.Context, venue *venueoracle.Venue, receipt *strings.Builder) {
	t.Helper()
	installations := `SELECT installation_id, coalesce(account_login, '<null>'), coalesce(account_type, '<null>'), coalesce(org_id, '<null>'),
		suspended_at IS NOT NULL, updated_at > created_at FROM github_app_installations ORDER BY installation_id`
	pythonRows := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.SourceDB), installations)
	goRows := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.GoDB), installations)
	if pythonRows != goRows || pythonRows == "" {
		t.Errorf("github_app_installations differ (or are empty):\n python %s\n go     %s", pythonRows, goRows)
	}
	fmt.Fprintf(receipt, "github_app_installations rows: %s\n", venueoracle.Mark(pythonRows == goRows && pythonRows != ""))

	rendered := map[string]string{}
	for _, plane := range []struct{ name, uri string }{{"python", venue.AdminURI(t, venue.SourceDB)}, {"go", venue.AdminURI(t, venue.GoDB)}} {
		pool, err := pgxpool.New(ctx, plane.uri)
		if err != nil {
			t.Fatal(err)
		}
		var org, provider, name, config, ciphertext string
		var active bool
		err = pool.QueryRow(ctx, `SELECT org_id, provider, name, is_active, config::text, credentials_encrypted FROM integration_credentials WHERE provider = 'github' AND name = 'github-app'`).
			Scan(&org, &provider, &name, &active, &config, &ciphertext)
		pool.Close()
		if err != nil {
			t.Fatalf("%s: read the github-app credential: %v", plane.name, err)
		}
		results := venue.CallPython(t, venueoracle.PythonCall{Target: "dev_health_ops.core.encryption:decrypt_value", Args: []any{ciphertext}})
		var plaintext string
		if err := json.Unmarshal(results[0], &plaintext); err != nil {
			t.Fatalf("%s: decrypt: %v", plane.name, err)
		}
		rendered[plane.name] = fmt.Sprintf("%s|%s|%s|active=%v|config=%s|secret=%s", org, provider, name, active, config, plaintext)
	}
	same := rendered["python"] == rendered["go"]
	if !same {
		t.Errorf("the github-app credential differs:\n python %s\n go     %s", rendered["python"], rendered["go"])
	}
	fmt.Fprintf(receipt, "github-app credential (decrypted): %s\n", venueoracle.Mark(same))
}

func venueStubDir() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "testdata", "provider_stub")
}

func stubPort(rawURL string) string {
	parsed, _ := url.Parse(rawURL)
	_, port, _ := net.SplitHostPort(parsed.Host)
	return port
}
