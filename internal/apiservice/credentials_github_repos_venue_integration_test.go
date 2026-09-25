//go:build integration

package apiservice

import (
	"context"
	"encoding/json"
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

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// githubRepoStub is the GitHub both planes list repositories from. The token
// a request carries names the case, the path and query the listing. A
// GitHub Enterprise base URL puts a prefix before the route, which the stub
// ignores but records.
type githubRepoStub struct {
	server *httptest.Server
	mu     sync.Mutex
	seen   []string
}

func newGitHubRepoStub(t *testing.T) *githubRepoStub {
	t.Helper()
	stub := &githubRepoStub{}
	stub.server = httptest.NewServer(http.HandlerFunc(stub.handle))
	t.Cleanup(stub.server.Close)
	return stub
}

func (s *githubRepoStub) port() string {
	_, port, _ := net.SplitHostPort(strings.TrimPrefix(s.server.URL, "http://"))
	return port
}

func (s *githubRepoStub) take() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.seen
	s.seen = nil
	return out
}

func ghItem(id int, name string) string {
	return fmt.Sprintf(`{"id": %d, "name": "%s", "full_name": "Org/%s", "description": "about %s", "html_url": "https://github.example.test/Org/%s", "default_branch": "main", "stargazers_count": %d}`, id, name, name, name, name, id)
}

func ghItems(from, to int) string {
	items := make([]string, 0, to-from+1)
	for id := from; id <= to; id++ {
		items = append(items, ghItem(id, fmt.Sprintf("p%d", id)))
	}
	return "[" + strings.Join(items, ",") + "]"
}

var ghNamed = "[" + ghItem(1, "api") + "," + ghItem(2, "web") + `,{"id": 3, "name": "docs", "full_name": "other/Docs", "description": null, "html_url": null, "url": "https://api.github.example.test/repos/other/docs"}]`

// githubRoute is the path from the first route segment on, and what came
// before it.
func githubRoute(path string) (prefix, route string) {
	best := -1
	for _, marker := range []string{"/app/installations/", "/installation/repositories", "/search/repositories", "/orgs/", "/user/repos"} {
		if index := strings.Index(path, marker); index >= 0 && (best < 0 || index < best) {
			best = index
		}
	}
	if best < 0 {
		return "", path
	}
	return path[:best], path[best:]
}

func (s *githubRepoStub) handle(w http.ResponseWriter, r *http.Request) {
	kind, credential, _ := strings.Cut(r.Header.Get("Authorization"), " ")
	query := r.URL.Query()
	keys := make([]string, 0, len(query))
	for key := range query {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var parts []string
	for _, key := range keys {
		for _, value := range query[key] {
			parts = append(parts, url.QueryEscape(key)+"="+url.QueryEscape(value))
		}
	}
	signature := r.Method + " " + r.URL.Path
	if len(parts) > 0 {
		signature += "?" + strings.Join(parts, "&")
	}
	// A signed JWT differs between the planes; only its scheme is compared.
	if kind == "Bearer" {
		credential = "<jwt>"
	}
	signature += " auth=" + kind + " " + credential + " accept=" + r.Header.Get("Accept")
	s.mu.Lock()
	s.seen = append(s.seen, signature)
	s.mu.Unlock()

	write := func(status int, body string, headers ...[2]string) {
		w.Header().Set("Content-Type", "application/json")
		for _, pair := range headers {
			w.Header().Set(pair[0], pair[1])
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}
	prefix, route := githubRoute(r.URL.Path)
	if r.Method == http.MethodPost && strings.HasPrefix(route, "/app/installations/") && strings.HasSuffix(route, "/access_tokens") {
		switch strings.Split(route, "/")[3] {
		case "111":
			write(201, `{"token":"inst_ok","expires_at":"2099-01-01T00:00:00Z"}`)
		case "333":
			write(201, `{"expires_at":"2099-01-01T00:00:00Z"}`)
		case "444":
			write(503, `{"message":"unavailable"}`)
		case "555":
			write(201, `{"token":"inst_denied","expires_at":"2099-01-01T00:00:00Z"}`)
		default:
			write(401, `{"message":"A JSON web token could not be decoded"}`)
		}
		return
	}
	if r.Method != http.MethodGet {
		write(404, `{"message":"venue stub: no route"}`)
		return
	}
	switch credential {
	case "gh_denied", "inst_denied":
		write(401, `{"message":"Bad credentials"}`)
		return
	case "gh_forbidden":
		write(403, `{"message":"Resource not accessible by personal access token"}`)
		return
	case "gh_limited":
		write(403, `{"message":"API rate limit exceeded"}`, [2]string{"Retry-After", "1"}, [2]string{"X-RateLimit-Remaining", "0"}, [2]string{"X-GitHub-Request-Id", "AB'CD"})
		return
	case "gh_429":
		write(429, `{}`, [2]string{"Retry-After", "1"})
		return
	case "gh_notfound":
		write(404, `{"message":"Not Found"}`)
		return
	case "gh_teapot":
		write(418, `teapot`)
		return
	case "gh_junk":
		write(200, `oops`)
		return
	case "gh_dict":
		write(200, `{"a": 1}`)
		return
	case "gh_odd":
		write(200, `[1, {"id": 1, "name": 5, "full_name": "O/five", "description": 5, "html_url": 0, "url": "https://api.github.example.test/x"}, {"name": "n", "description": [1, "a"], "html_url": null}, {"full_name": 12, "description": true}]`)
		return
	case "gh_infinity":
		write(200, `[{"id": Infinity, "name": "a", "full_name": "O/a"}]`)
		return
	case "gh_paged":
		host := "https://api.github.com"
		if prefix != "" {
			host = "https://provider.example.test"
		}
		if query.Get("page") == "" {
			write(200, ghItems(1, 100), [2]string{"Link", `<` + host + prefix + route + `?per_page=100&page=2>; rel="next", <` + host + prefix + route + `?per_page=100&page=2>; rel="last"`})
			return
		}
		write(200, ghItems(101, 105))
		return
	}
	switch {
	case strings.HasPrefix(route, "/orgs/"):
		switch strings.TrimSuffix(strings.TrimPrefix(route, "/orgs/"), "/repos") {
		case "missing":
			write(404, `{"message":"Not Found"}`)
		case "empty":
			write(200, `[]`)
		case "acme":
			write(200, "["+ghItem(10, "acme-api")+"]")
		default:
			write(200, ghNamed)
		}
	case route == "/user/repos":
		write(200, ghNamed)
	case route == "/search/repositories":
		write(200, `{"total_count": 2, "items": [`+ghItem(20, "found")+","+ghItem(21, "also")+`]}`)
	case route == "/installation/repositories":
		write(200, `{"total_count": 3, "repositories": `+ghNamed+`}`)
	default:
		write(404, `{"message":"venue stub: no route"}`)
	}
}

func githubRepoSeedRows(pemKey string) []repoSeedRow {
	row := func(name, payload, config string) repoSeedRow {
		return repoSeedRow{provider: "github", name: name, payload: payload, config: config}
	}
	app := func(installation string, extra string) string {
		key, _ := json.Marshal(pemKey)
		return `{"app_id":"1","private_key":` + string(key) + `,"installation_id":"` + installation + `"` + extra + `}`
	}
	const host = "https://provider.example.test"
	return []repoSeedRow{
		row("default", `{"token":"gh_ok"}`, ""),
		row("paged", `{"token":"gh_paged"}`, ""),
		row("denied", `{"token":"gh_denied"}`, ""),
		row("forbidden", `{"token":"gh_forbidden"}`, ""),
		row("limited", `{"token":"gh_limited"}`, ""),
		row("toomany", `{"token":"gh_429"}`, ""),
		row("notfound", `{"token":"gh_notfound"}`, ""),
		row("teapot", `{"token":"gh_teapot"}`, ""),
		row("junk", `{"token":"gh_junk"}`, ""),
		row("dict", `{"token":"gh_dict"}`, ""),
		row("odd", `{"token":"gh_odd"}`, ""),
		row("infinity", `{"token":"gh_infinity"}`, ""),
		row("numtoken", `{"token":5}`, ""),
		row("orgcfg", `{"token":"gh_ok"}`, `{"org":"acme"}`),
		row("orgdec", `{"token":"gh_ok","org":"eng"}`, ""),
		row("orgboth", `{"token":"gh_ok","org":"eng"}`, `{"org":"acme"}`),
		row("orgnonstr", `{"token":"gh_ok","org":"eng"}`, `{"org":5}`),
		row("orgemptycfg", `{"token":"gh_ok","org":"eng"}`, `{"org":""}`),
		row("orgnonstrboth", `{"token":"gh_ok","org":7}`, `{"org":5}`),
		row("ghe", `{"token":"gh_ok","base_url":"`+host+`/ghe/api/v3/"}`, ""),
		row("ghepaged", `{"token":"gh_paged","base_url":"`+host+`/ghe/api/v3"}`, ""),
		row("gheorg", `{"token":"gh_ok","base_url":"`+host+`/ghe/api/v3"}`, `{"org":"acme"}`),
		row("cfgbase", `{"token":"gh_ok"}`, `{"base_url":"`+host+`/cfg/api/v3"}`),
		row("emptybase", `{"token":"gh_ok","base_url":""}`, `{"base_url":"`+host+`/cfg/api/v3"}`),
		row("nullbase", `{"token":"gh_ok","base_url":null}`, `{"base_url":"`+host+`/cfg/api/v3"}`),
		row("aliasbase", `{"token":"gh_ok","baseUrl":"`+host+`/alias/api/v3"}`, `{"base_url":"`+host+`/cfg/api/v3"}`),
		row("bothbase", `{"token":"gh_ok","baseUrl":"`+host+`/alias/api/v3","base_url":"`+host+`/snake/api/v3"}`, ""),
		row("aliasandcfg", `{"token":"gh_ok","base_url":"`+host+`/snake/api/v3","baseUrl":"`+host+`/alias/api/v3"}`, `{"base_url":"`+host+`/cfg/api/v3"}`),
		row("nonstrbase", `{"token":"gh_ok","base_url":5}`, ""),
		row("listbase", `{"token":"gh_ok"}`, `{"base_url":["x"]}`),
		row("localhost", `{"token":"gh_ok","base_url":"http://localhost"}`, ""),
		row("private", `{"token":"gh_ok","base_url":"http://10.0.0.5"}`, ""),
		row("userinfo", `{"token":"gh_ok","base_url":"https://u:p@provider.example.test"}`, ""),
		row("unresolvable", `{"token":"gh_ok","base_url":"https://unresolvable.invalid"}`, ""),
		row("scheme", `{"token":"gh_ok","base_url":"ftp://provider.example.test"}`, ""),
		row("emptypayload", `{}`, ""),
		row("nullpayload", `null`, ""),
		row("listpayload", `[1]`, ""),
		row("stringpayload", `"text"`, ""),
		row("nulltoken", `{"token":null}`, ""),
		row("emptytoken", `{"token":""}`, ""),
		row("noauth", `{"note":"x"}`, ""),
		row("both", `{"token":"gh_ok","app_id":"1"}`, ""),
		row("incompleteapp", `{"app_id":"1","private_key":"k"}`, ""),
		row("cfglist", `{"token":"gh_ok"}`, `[1]`),
		row("cfglistowner", `{"token":"gh_ok","base_url":"`+host+`"}`, `[1]`),
		row("cfgfalsy", `{"token":"gh_ok"}`, `[]`),
		row("cfgstring", `{"token":"gh_ok"}`, `"x"`),
		row("app", app("111", ""), ""),
		row("apphost", app("111", `,"base_url":"`+host+`/ghe/api/v3"`), ""),
		row("appowner", app("111", ""), `{"org":"acme"}`),
		row("apprefused", app("222", ""), ""),
		row("appnotoken", app("333", ""), ""),
		row("apptransient", app("444", ""), ""),
		row("appdenied", app("555", ""), ""),
		row("appbadkey", `{"app_id":"1","private_key":"not a key","installation_id":"111"}`, ""),
		row("appcamel", `{"appId":"1","privateKey":`+strings.TrimSuffix(strings.TrimPrefix(app("111", ""), `{"app_id":"1","private_key":`), `,"installation_id":"111"}`)+`,"installationId":"111"}`, ""),
	}
}

func githubRepoRequests(tokens map[string]string, ids map[string]string) []venueoracle.Request {
	admin := map[string]string{"Authorization": "Bearer " + tokens["admin"]}
	var out []venueoracle.Request
	list := func(name, row, query string) {
		path := "/api/v1/admin/credentials/" + ids["github/"+row] + "/repos"
		if query != "" {
			path += "?" + query
		}
		out = append(out, venueoracle.Request{Name: "github repos: " + name, Method: "GET", Path: path, Headers: admin})
	}
	list("member", "default", "")
	out[len(out)-1].Headers = map[string]string{"Authorization": "Bearer " + tokens["member"]}

	for _, query := range []string{"", "owner=acme", "owner=missing", "owner=empty", "owner=", "owner=a%2Fb%20c", "owner=g%C3%BCppe%3F%26%23", "owner=a&owner=acme",
		"search=api", "search=API", "search=%5Ba", "search=a%20b", "search=", "search=%2A", "search=a&search=web",
		"owner=acme&search=w", "owner=acme&search=", "owner=acme&search=x%20y%2Bz~*%3A", "owner=other&search=a%20b"} {
		list(query, "default", query)
	}
	for _, query := range []string{"max_repos=1", "max_repos=2", "max_repos=3", "max_repos=4", "max_repos=0", "max_repos=-1", "max_repos=-3", "max_repos=100000", "max_repos=99999999999999999999999999", "max_repos=-99999999999999999999999999", "max_repos=abc"} {
		list("user "+query, "default", query)
		list("org "+query, "default", query+"&owner=other")
		list("search "+query, "default", query+"&search=o")
	}
	for _, query := range []string{"", "max_repos=1", "max_repos=100", "max_repos=101", "max_repos=150", "max_repos=0", "max_repos=-1", "max_repos=1000", "search=p1", "search=P1&max_repos=3", "search=zzz", "owner=acme", "owner=acme&max_repos=150"} {
		list("paged "+query, "paged", query)
		list("ghe paged "+query, "ghepaged", query)
	}

	// Which stored value names the owner and the instance.
	for _, name := range []string{"orgcfg", "orgdec", "orgboth", "orgnonstr", "orgemptycfg", "orgnonstrboth", "ghe", "gheorg", "cfgbase", "emptybase", "nullbase", "aliasbase", "bothbase", "aliasandcfg"} {
		list("credential "+name, name, "")
		list("credential "+name+" with an owner", name, "owner=other")
	}
	// Problems with the credential.
	for _, name := range []string{"nonstrbase", "listbase", "localhost", "private", "userinfo", "unresolvable", "scheme", "emptypayload", "nullpayload", "listpayload", "stringpayload",
		"nulltoken", "emptytoken", "noauth", "both", "incompleteapp", "cfglist", "cfglistowner", "cfgfalsy", "cfgstring", "numtoken"} {
		list("credential "+name, name, "")
	}
	list("config list with an owner reads no config org", "cfglist", "owner=acme")
	list("config list with an owner and a base url", "cfglistowner", "owner=acme")
	// Provider answers.
	for _, name := range []string{"denied", "forbidden", "limited", "toomany", "notfound", "teapot", "junk", "dict", "odd", "infinity"} {
		list("provider answer "+name, name, "")
		list("provider answer "+name+" for an org", name, "owner=acme")
		list("provider answer "+name+" for a search", name, "owner=acme&search=w")
	}
	// GitHub Apps.
	for _, name := range []string{"app", "apphost", "appowner", "apprefused", "appnotoken", "apptransient", "appdenied", "appbadkey", "appcamel"} {
		list("app "+name, name, "")
		list("app "+name+" with a search", name, "search=api")
		list("app "+name+" with an owner", name, "owner=acme")
	}
	list("app max_repos=1", "app", "max_repos=1")
	list("app max_repos=0", "app", "max_repos=0")
	list("app max_repos=-1", "app", "max_repos=-1")
	return out
}

// TestVenueOracleCredentialGitHubRepos is the GitHub repository listing's
// differential: the real Python api and this Go api answer every request the
// same and send the same requests to the GitHub stub.
func TestVenueOracleCredentialGitHubRepos(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	stub := newGitHubRepoStub(t)
	stubURL, err := url.Parse(stub.server.URL)
	if err != nil {
		t.Fatal(err)
	}
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

	pemKey := generatePEM(t)
	var ids map[string]string
	rows := githubRepoSeedRows(pemKey)
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root: venueRoot(), JWTKey: venueKey, Logger: quietLogger(),
		PythonEnv: []string{
			"SETTINGS_ENCRYPTION_KEY=" + credentialsVenueKey,
			"VENUE_PROVIDER_STUB_PORT=" + stub.port(),
			"PYTHONPATH=" + siteDir + ":" + filepath.Join(venueRoot(), "src"),
		},
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, venue *venueoracle.Venue) map[string]map[string]any {
			seed := venueSeed(t, ctx, admin)
			ids = seedRepoCredentials(t, ctx, admin, venue, seed.orgA, rows)
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
	requests := githubRepoRequests(venue.Tokens, ids)
	pythonResponses := venue.ServePython(t, requests)
	pythonProvider := stub.take()
	receipt := venueoracle.Diff(t, base, requests, pythonResponses, venueoracle.DiffOptions{})
	goProvider := stub.take()

	providerSame := len(goProvider) > 0 && strings.Join(pythonProvider, "\n") == strings.Join(goProvider, "\n")
	if !providerSame {
		t.Errorf("provider requests differ:\n python:\n%s\n go:\n%s", strings.Join(pythonProvider, "\n"), strings.Join(goProvider, "\n"))
	}
	t.Logf("\n%sprovider requests (%d): %s\n", receipt, len(goProvider), venueoracle.Mark(providerSame))
}
