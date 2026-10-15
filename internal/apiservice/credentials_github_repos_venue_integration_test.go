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
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
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
		row("basequery", `{"token":"gh_ok","base_url":"`+host+`/ghe/api/v3?scope=all"}`, ""),
		row("basefrag", `{"token":"gh_ok","base_url":"`+host+`/ghe/api/v3#frag"}`, ""),
		row("basequeryfrag", `{"token":"gh_ok","base_url":"`+host+`/ghe/api/v3/?scope=all#frag"}`, ""),
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
	// A base URL with a query or a fragment is joined the way httpx joins it.
	for _, name := range []string{"basequery", "basefrag", "basequeryfrag"} {
		list("credential "+name, name, "")
		list("credential "+name+" with an owner", name, "owner=other")
		list("credential "+name+" with a search", name, "owner=other&search=x")
		list("credential "+name+" with a pattern", name, "search=a")
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
	golden := venueoracle.OpenGolden(t, venueGolden("venue-oracle-credential-git-hub-repos", t.Name(), "ee92b71ba4ed644d5b2b1cfc3ed05868fc954de5f9b2bcdaa6e866840b52884e"))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	stub := newGitHubRepoStub(t)
	stubURL, err := url.Parse(stub.server.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, thisFile, _, _ := moduleroot.Caller(0)
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
		Golden: golden, Root: golden.PythonRoot(t, venueRoot()), JWTKey: venueKey, Logger: quietLogger(),
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
	pythonResponses := golden.Python(t, venue, requests)
	pythonProvider := strings.Split(golden.InspectRows(t, "python provider requests", func() string {
		return strings.Join(stub.take(), "\n")
	}), "\n")
	pin := venueoracle.OpenGoPin(t, venueoracle.GoPinSpec{Path: "testdata/venue/credential-github-repos.go-pin.json", SHA256: browsePinSHA256, Ruling: browseRuling})
	goCalls := map[string][]string{}
	goStatus := map[string]int{}
	receipt := venueoracle.Diff(t, base, requests, pythonResponses, venueoracle.DiffOptions{
		Golden:    golden,
		Normalize: dropGoOnlyBrowseScope,
		Retire:    retiredBrowseRequest,
		Pin:       pin,
		Inspect: func(request venueoracle.Request, goResponse venueoracle.Response) {
			calls := stub.take()
			goCalls[request.Name], goStatus[request.Name] = calls, goResponse.Status
			assertBrowseScopeFields(t, request, goResponse)
			if retiredBrowseRequest(request) {
				assertRetiredBrowse(t, request, goResponse, calls)
				pin.Check(t, "provider: "+request.Name, strings.Join(calls, "\n"))
			}
		},
	})
	providerSame := checkBrowseProviderRequests(t, requests, goCalls, goStatus, pythonProvider)
	t.Logf("\n%sprovider requests: %s\n", receipt, venueoracle.Mark(providerSame))
	pin.Finish(t)
	golden.Finish(t)
}

// browseScopeFields are the three fields the Go browse adds to every GitHub repo listing (CHAOS-9787). The Python
// reference of this pair is a frozen golden (its route no longer exists in src/), so it cannot emit them: they are a
// ruled Go-only extension, as the legacy ingest, webhook intake, customer push and GraphQL edge oracles rule their own
// differences through Normalize. assertBrowseScopeFields checks them on every raw Go response instead.
var browseScopeFields = []string{"owner_type", "installation_scoped", "diagnostic"}

// dropGoOnlyBrowseScope removes the Go-only scope fields from a 200 listing body; every other byte still has to agree.
func dropGoOnlyBrowseScope(request venueoracle.Request, body string) string {
	if !strings.HasPrefix(request.Name, "github repos: ") || retiredBrowseRequest(request) {
		return body
	}
	var decoded map[string]json.RawMessage
	if json.Unmarshal([]byte(body), &decoded) != nil {
		return body
	}
	removed := false
	for _, field := range browseScopeFields {
		if _, present := decoded[field]; present {
			delete(decoded, field)
			removed = true
		}
	}
	if !removed {
		return body
	}
	out, err := json.Marshal(decoded)
	if err != nil {
		return body
	}
	return string(out)
}

// assertBrowseScopeFields checks, on every raw Go 200 listing, that all three scope fields are present with a valid
// value, and that installation_scoped is true exactly for the GitHub App credentials.
func assertBrowseScopeFields(t *testing.T, request venueoracle.Request, goResponse venueoracle.Response) {
	t.Helper()
	if !strings.HasPrefix(request.Name, "github repos: ") || goResponse.Status != http.StatusOK {
		return
	}
	var body map[string]any
	if json.Unmarshal([]byte(goResponse.Body), &body) != nil {
		return
	}
	if _, listing := body["repos"]; !listing {
		return
	}
	switch value := body["owner_type"].(type) {
	case nil:
	case string:
		if value != "org" && value != "user" {
			t.Errorf("%s: owner_type %q is not org, user or null", request.Name, value)
		}
	default:
		t.Errorf("%s: owner_type %v is not a string or null", request.Name, value)
	}
	scoped, isBool := body["installation_scoped"].(bool)
	if !isBool {
		t.Errorf("%s: installation_scoped %v is not a bool", request.Name, body["installation_scoped"])
	}
	if wantScoped := strings.HasPrefix(request.Name, "github repos: app "); scoped != wantScoped {
		t.Errorf("%s: installation_scoped=%v, want %v (true exactly for the App credentials)", request.Name, scoped, wantScoped)
	}
	switch value := body["diagnostic"].(type) {
	case nil:
	case string:
		if value != "owner_not_found" {
			t.Errorf("%s: diagnostic %q is not owner_not_found or null", request.Name, value)
		}
	default:
		t.Errorf("%s: diagnostic %v is not a string or null", request.Name, value)
	}
}

// browseRuling is the one-line ruling the pinned requests carry: GWC D6026 (the oracle's Python reference is the frozen
// golden of D6025, so it cannot express a personal-owner browse).
const browseRuling = "GWC D6026 (CHAOS-9787): an owner browse unions the token's /user/repos with the owner's own listing, calls /user/repos first and falls back to /users; the frozen Python reference of D6025 lists the owner route only"

// browsePinSHA256 is the digest of testdata/venue/credential-github-repos.go-pin.json.
const browsePinSHA256 = "e3b3b8c2952c4ca55064fe4cea3ace316ec12b2346931b039d55d5045118a0a2"

// retiredBrowseNames are the owner requests whose Python answer the ruling retires: the 35 the frozen reference cannot
// express (the union adds the token's repositories, a token listing that 404s is a credential fault, the owner's login
// is redacted from the failure text, and the failing call named is /user/repos). Every other request, search with an
// owner and every non-owner request included, is still compared byte for byte.
var retiredBrowseNames = map[string]bool{
	"github repos: org max_repos=1":                           true,
	"github repos: org max_repos=2":                           true,
	"github repos: org max_repos=3":                           true,
	"github repos: org max_repos=4":                           true,
	"github repos: org max_repos=0":                           true,
	"github repos: org max_repos=-1":                          true,
	"github repos: org max_repos=-3":                          true,
	"github repos: org max_repos=100000":                      true,
	"github repos: org max_repos=99999999999999999999999999":  true,
	"github repos: org max_repos=-99999999999999999999999999": true,
	"github repos: credential orgcfg with an owner":           true,
	"github repos: credential orgdec with an owner":           true,
	"github repos: credential orgboth with an owner":          true,
	"github repos: credential orgnonstr with an owner":        true,
	"github repos: credential orgemptycfg with an owner":      true,
	"github repos: credential orgnonstrboth with an owner":    true,
	"github repos: credential ghe with an owner":              true,
	"github repos: credential gheorg with an owner":           true,
	"github repos: credential cfgbase with an owner":          true,
	"github repos: credential emptybase with an owner":        true,
	"github repos: credential nullbase with an owner":         true,
	"github repos: credential aliasbase with an owner":        true,
	"github repos: credential bothbase with an owner":         true,
	"github repos: credential aliasandcfg with an owner":      true,
	"github repos: credential basequery with an owner":        true,
	"github repos: credential basefrag with an owner":         true,
	"github repos: credential basequeryfrag with an owner":    true,
	"github repos: provider answer denied for an org":         true,
	"github repos: provider answer forbidden for an org":      true,
	"github repos: provider answer limited for an org":        true,
	"github repos: provider answer toomany for an org":        true,
	"github repos: provider answer notfound for an org":       true,
	"github repos: provider answer teapot for an org":         true,
	"github repos: provider answer dict for an org":           true,
	"github repos: app appdenied with an owner":               true,
}

func retiredBrowseRequest(request venueoracle.Request) bool { return retiredBrowseNames[request.Name] }

const browseBaseURLText = "github token repository listing was not found; check the credential's base URL"

var browseCallPath = regexp.MustCompile(`^GET (/[^?\s]*)`)

var browseRouteMarkers = []string{"/app/installations/", "/installation/repositories", "/search/repositories", "/orgs/", "/users/", "/user/repos"}

// callRoute is the route of a recorded call, past a GitHub Enterprise base prefix; routeless is set when the call shows
// no route at all (a base URL holding a query or a fragment folds the route into the query).
func callRoute(call string) (route string, routeless bool) {
	match := browseCallPath.FindStringSubmatch(call)
	if match == nil {
		return call, true
	}
	best := -1
	for _, marker := range browseRouteMarkers {
		if index := strings.Index(match[1], marker); index >= 0 && (best < 0 || index < best) {
			best = index
		}
	}
	if best < 0 {
		return match[1], true
	}
	return match[1][best:], false
}

func callPath(call string) string {
	route, _ := callRoute(call)
	return route
}

// userReposCall reports a call to the token's own listing: the route /user/repos, or a call that shows no route at all.
func userReposCall(call string) bool {
	if !strings.HasPrefix(call, "GET ") {
		return false
	}
	route, routeless := callRoute(call)
	return routeless || route == "/user/repos"
}

// assertRetiredBrowse states, for a retired request, the specific Go behaviour the pin freezes, so a change to any clause
// fails here with its name and not only as a pin diff: the owner is unioned with the token's repositories (token's
// first), /user/repos is called before the owner's listing for a token credential and never for an App, a token
// listing that 404s is a credential fault with the base URL text, and a failure text names the call that failed and
// never the owner's login.
func assertRetiredBrowse(t *testing.T, request venueoracle.Request, response venueoracle.Response, calls []string) {
	t.Helper()
	name := strings.TrimPrefix(request.Name, "github repos: ")
	app := strings.HasPrefix(name, "app ")
	paths := make([]string, 0, len(calls))
	for _, call := range calls {
		paths = append(paths, callPath(call))
	}
	if app {
		for _, call := range calls {
			if userReposCall(call) {
				t.Errorf("%s: an App credential called /user/repos: %v", request.Name, calls)
			}
		}
	} else if len(calls) == 0 || !userReposCall(calls[0]) {
		t.Errorf("%s: a token credential's first provider call is %v, want /user/repos before the owner's listing", request.Name, calls)
	}
	for index, path := range paths {
		if strings.HasPrefix(path, "/users/") && (index == 0 || !strings.HasPrefix(paths[index-1], "/orgs/")) {
			t.Errorf("%s: %s is not the fallback after an /orgs call: %v", request.Name, path, paths)
		}
	}
	var body struct {
		Detail     string `json:"detail"`
		Diagnostic string `json:"diagnostic"`
		Repos      []struct {
			FullName string `json:"full_name"`
		} `json:"repos"`
	}
	if json.Unmarshal([]byte(response.Body), &body) != nil {
		t.Errorf("%s: the answer is not JSON: %s", request.Name, response.Body)
		return
	}
	if app && response.Status >= http.StatusBadRequest {
		last := len(paths) - 1
		if last < 1 || !strings.HasPrefix(paths[last], "/users/") || !strings.HasPrefix(paths[last-1], "/orgs/") {
			t.Errorf("%s: an owner route that failed is not retried under /users/{owner}/repos: %v", request.Name, paths)
		}
	}
	if body.Diagnostic == "owner_not_found" && !app && len(calls) < 2 {
		t.Errorf("%s: owner_not_found after %v: a token listing that did not answer is not a missing owner", request.Name, calls)
	}
	switch response.Status {
	case http.StatusOK:
		if !app && strings.Contains(request.Path, "owner=other") && (len(body.Repos) == 0 || body.Repos[0].FullName != "other/Docs") {
			t.Errorf("%s: the listing %v does not start with the token's own other/Docs: the union lists the token's repositories first", request.Name, body.Repos)
		}
	case http.StatusBadGateway:
		if strings.HasPrefix(body.Detail, "github token repository listing") {
			if body.Detail != browseBaseURLText || len(calls) != 1 || !userReposCall(calls[0]) {
				t.Errorf("%s: a token listing 404 answered %q after %v, want %q after /user/repos only", request.Name, body.Detail, calls, browseBaseURLText)
			}
		}
	}
	if response.Status >= 400 && response.Status != http.StatusBadGateway || strings.HasPrefix(body.Detail, "github ") {
		switch {
		case strings.Contains(body.Detail, "acme"):
			t.Errorf("%s: the failure text names the owner's login: %s", request.Name, body.Detail)
		case app && strings.Contains(body.Detail, "/orgs/") && !strings.Contains(body.Detail, "/orgs/{login}/"):
			t.Errorf("%s: an owner route failure does not carry {login}: %s", request.Name, body.Detail)
		case !app && body.Detail != browseBaseURLText && strings.Contains(body.Detail, "repo:GET ") && !strings.Contains(body.Detail, "repo:GET /user/repos"):
			t.Errorf("%s: the failure names %q, want the first failing call /user/repos", request.Name, body.Detail)
		}
	}
}

// checkBrowseProviderRequests holds the provider traffic of every request the ruling does not retire to Python's: a
// request's Go calls equal Python's for it once the /user/repos and /users/{owner}/repos calls the ruling adds are
// set aside. The retired requests' Python calls are unknown in number, so the check lines the other requests up
// against Python's flat sequence, skipping a retired request's calls where it must.
func checkBrowseProviderRequests(t *testing.T, requests []venueoracle.Request, goCalls map[string][]string, goStatus map[string]int, python []string) bool {
	t.Helper()
	expected := make([][]string, len(requests))
	// free marks the requests whose Python calls are not lined up: the retired ones, and a request that failed on the
	// token's own listing before the owner's (Go's only calls are /user/repos, and Python's call went to the owner).
	free := make([]bool, len(requests))
	maybeFree := make([]bool, len(requests))
	for index, request := range requests {
		if retiredBrowseRequest(request) {
			free[index] = true
			continue
		}
		onlyTokenListing := len(goCalls[request.Name]) > 0 && goStatus[request.Name] >= http.StatusBadRequest
		for _, call := range goCalls[request.Name] {
			onlyTokenListing = onlyTokenListing && callPath(call) == "/user/repos"
		}
		if onlyTokenListing {
			maybeFree[index] = true
		}
		calls := goCalls[request.Name]
		firstOrg := -1
		for position, call := range calls {
			if strings.HasPrefix(callPath(call), "/orgs/") {
				firstOrg = position
				break
			}
		}
		for position, call := range calls {
			path := callPath(call)
			// The ruling's two added calls: every /user/repos page before the owner's /orgs listing, and the
			// /users/{owner}/repos fallback after it.
			if path == "/user/repos" && firstOrg > position {
				continue
			}
			if strings.HasPrefix(path, "/users/") && strings.HasSuffix(path, "/repos") && firstOrg >= 0 && firstOrg < position {
				continue
			}
			expected[index] = append(expected[index], call)
		}
	}
	type state struct{ request, cursor int }
	failed := map[state]bool{}
	deepest := state{}
	var match func(request, cursor int) bool
	match = func(request, cursor int) bool {
		if request > deepest.request {
			deepest = state{request, cursor}
		}
		if request == len(requests) {
			return cursor == len(python)
		}
		key := state{request, cursor}
		if failed[key] {
			return false
		}
		if free[request] {
			for skip := 0; skip <= 12 && cursor+skip <= len(python); skip++ {
				if match(request+1, cursor+skip) {
					return true
				}
			}
		} else {
			if cursor+len(expected[request]) <= len(python) && strings.Join(python[cursor:cursor+len(expected[request])], "\n") == strings.Join(expected[request], "\n") {
				if match(request+1, cursor+len(expected[request])) {
					return true
				}
			}
			if maybeFree[request] {
				for skip := 0; skip <= 12 && cursor+skip <= len(python); skip++ {
					if match(request+1, cursor+skip) {
						return true
					}
				}
			}
		}
		failed[key] = true
		return false
	}
	if !match(0, 0) {
		next := ""
		if deepest.request < len(requests) {
			next = requests[deepest.request].Name + " expects " + strings.Join(expected[deepest.request], " | ") + " at python line " + strconv.Itoa(deepest.cursor)
		}
		t.Errorf("the provider traffic of the requests that are not retired differs from Python's; no alignment past: %s", next)
		return false
	}
	return true
}
