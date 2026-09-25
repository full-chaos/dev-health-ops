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

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// repoStub is the GitLab both planes list repositories from. The token a
// request carries names the case, the path and query the listing.
type repoStub struct {
	server *httptest.Server
	mu     sync.Mutex
	seen   []string
}

func newRepoStub(t *testing.T) *repoStub {
	t.Helper()
	stub := &repoStub{}
	stub.server = httptest.NewServer(http.HandlerFunc(stub.handle))
	t.Cleanup(stub.server.Close)
	return stub
}

func (s *repoStub) port() string {
	_, port, _ := net.SplitHostPort(strings.TrimPrefix(s.server.URL, "http://"))
	return port
}

func (s *repoStub) take() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := append([]string(nil), s.seen...)
	s.seen = nil
	return out
}

func repoProjects(from, to int) string {
	items := make([]string, 0, to-from+1)
	for id := from; id <= to; id++ {
		items = append(items, fmt.Sprintf(`{"id": %d, "name": "p%d", "path_with_namespace": "org/P%d", "description": "project %d", "web_url": "https://gitlab.example.test/org/p%d"}`, id, id, id, id, id))
	}
	return "[" + strings.Join(items, ",") + "]"
}

const repoNamed = `[{"id": 1, "name": "api", "path_with_namespace": "grp/api", "description": "the api", "web_url": "https://gitlab.example.test/grp/api"},
{"id": 2, "name": "web", "path_with_namespace": "grp/sub/Web", "description": null, "web_url": null},
{"id": 3, "name": "docs", "path_with_namespace": "other/docs"}]`

func (s *repoStub) handle(w http.ResponseWriter, r *http.Request) {
	token := r.Header.Get("PRIVATE-TOKEN")
	query := r.URL.Query()
	signature := r.Method + " " + r.URL.Path
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
	if len(parts) > 0 {
		signature += "?" + strings.Join(parts, "&")
	}
	signature += " token=" + token
	if r.Header.Get("Authorization") != "" {
		signature += " +authorization"
	}
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
	index := strings.Index(r.URL.Path, "/api/v4/")
	if r.Method != http.MethodGet || index < 0 {
		write(404, `{"message":"venue stub: no route"}`)
		return
	}
	api := r.URL.Path[index+len("/api/v4"):]
	switch token {
	case "gl_denied":
		write(401, `{"message":"401 Unauthorized"}`)
		return
	case "gl_forbidden":
		write(403, `{"message":"403 Forbidden"}`)
		return
	case "gl_notfound":
		write(404, `{"message":"404 Not Found"}`)
		return
	case "gl_ratelimit":
		write(429, `{}`, [2]string{"Retry-After", "1"})
		return
	case "gl_teapot":
		write(418, `teapot`)
		return
	case "gl_junk":
		write(200, `oops`)
		return
	case "gl_dict":
		write(200, `{"a": 1}`)
		return
	case "gl_baddesc":
		write(200, `[{"id": 1, "name": "a", "path_with_namespace": "g/a", "description": 5, "web_url": null}]`)
		return
	case "gl_badurl":
		write(200, `[{"id": 1, "name": "a", "path_with_namespace": "g/a", "description": null, "web_url": ["x"]}]`)
		return
	case "gl_infinity":
		write(200, `[{"id": 1, "name": "a", "path_with_namespace": "g/a"}, {"id": 2, "name": "b", "star_count": Infinity}]`)
		return
	case "gl_paged":
		if query.Get("page") == "1" {
			write(200, repoProjects(1, 100), [2]string{"X-Next-Page", "2"})
			return
		}
		write(200, repoProjects(101, 105))
		return
	}
	if strings.HasPrefix(api, "/groups/") {
		switch strings.TrimSuffix(strings.TrimPrefix(api, "/groups/"), "/projects") {
		case "missing":
			write(404, `{"message":"404 Group Not Found"}`)
		case "empty":
			write(200, `[]`)
		case "engineering":
			write(200, `[{"id": 10, "name": "eng-api", "path_with_namespace": "engineering/api", "description": "e", "web_url": "https://gitlab.example.test/engineering/api"}]`)
		default:
			write(200, repoNamed)
		}
		return
	}
	if api == "/projects" {
		write(200, repoNamed)
		return
	}
	write(404, `{"message":"venue stub: no route"}`)
}

// repoSeedRow is one stored credential: payload is the raw JSON the row
// seals (the empty string seals none), config the raw JSON of its config
// column, ciphertext a literal stored value that is not a sealed payload.
type repoSeedRow struct {
	provider, name string
	payload        string
	config         string
	ciphertext     string
	noPayload      bool
}

func repoSeedRows() []repoSeedRow {
	row := func(name, payload, config string) repoSeedRow {
		return repoSeedRow{provider: "gitlab", name: name, payload: payload, config: config}
	}
	const host = "https://provider.example.test"
	return []repoSeedRow{
		row("default", `{"token":"gl_ok"}`, ""),
		row("paged", `{"token":"gl_paged"}`, ""),
		row("denied", `{"token":"gl_denied"}`, ""),
		row("forbidden", `{"token":"gl_forbidden"}`, ""),
		row("notfound", `{"token":"gl_notfound"}`, ""),
		row("ratelimit", `{"token":"gl_ratelimit"}`, ""),
		row("teapot", `{"token":"gl_teapot"}`, ""),
		row("junk", `{"token":"gl_junk"}`, ""),
		row("dict", `{"token":"gl_dict"}`, ""),
		row("baddesc", `{"token":"gl_baddesc"}`, ""),
		row("badurl", `{"token":"gl_badurl"}`, ""),
		row("infinity", `{"token":"gl_infinity"}`, ""),
		row("decurl", `{"token":"gl_ok","url":"`+host+`/dec/","base_url":"`+host+`/decb"}`, `{"url":"`+host+`/cfg"}`),
		row("decbase", `{"token":"gl_ok","url":"","base_url":"`+host+`/decb"}`, `{"url":"`+host+`/cfg"}`),
		row("cfgurl", `{"token":"gl_ok"}`, `{"url":"`+host+`/cfg","base_url":"`+host+`/cfgb"}`),
		row("cfgbase", `{"token":"gl_ok"}`, `{"url":"","base_url":"`+host+`/cfgb"}`),
		row("nonstrurl", `{"token":"gl_ok","url":5,"base_url":["x"]}`, `{"url":{"a":1},"base_url":"`+host+`/fallback"}`),
		row("group", `{"token":"gl_ok"}`, `{"group":"engineering"}`),
		row("groupempty", `{"token":"gl_ok"}`, `{"group":""}`),
		row("groupnonstr", `{"token":"gl_ok"}`, `{"group":5}`),
		row("notoken", `{"url":"`+host+`"}`, ""),
		row("emptytoken", `{"token":"","url":"`+host+`"}`, ""),
		row("nulltoken", `{"token":null}`, ""),
		row("numtoken", `{"token":5,"url":"`+host+`"}`, ""),
		row("localhost", `{"token":"gl_ok","url":"http://localhost"}`, ""),
		row("private", `{"token":"gl_ok","url":"http://10.0.0.5"}`, ""),
		row("userinfo", `{"token":"gl_ok","url":"https://u:p@provider.example.test"}`, ""),
		row("unresolvable", `{"token":"gl_ok","url":"https://unresolvable.invalid"}`, ""),
		row("scheme", `{"token":"gl_ok","url":"ftp://provider.example.test"}`, ""),
		row("badurlfirst", `{"token":"","url":"http://localhost"}`, ""),
		row("emptypayload", `{}`, ""),
		row("nullpayload", `null`, ""),
		row("listpayload", `[1]`, ""),
		row("stringpayload", `"text"`, ""),
		row("cfglist", `{"token":"gl_ok"}`, `[1]`),
		row("cfglisturl", `{"token":"gl_ok","url":"`+host+`"}`, `[1]`),
		row("cfgfalsy", `{"token":"gl_ok"}`, `[]`),
		row("cfgnull", `{"token":"gl_ok"}`, `null`),
		row("cfgstring", `{"token":"gl_ok"}`, `"x"`),
		{provider: "gitlab", name: "nopayload", noPayload: true},
		{provider: "gitlab", name: "unreadable", ciphertext: "v1:not-a-fernet-token"},
		{provider: "github", name: "default", payload: `{"token":"ghp_ok"}`},
		{provider: "jira", name: "default", payload: `{"email":"j@example.test","api_token":"jira_ok","base_url":"` + host + `"}`},
		{provider: "jira", name: "listpayload", payload: `[1]`},
		{provider: "custom", name: "default", payload: `{"a":"b"}`},
		{provider: "pagerduty", name: "default", payload: `{"auth_mode":"oauth","oauth_credential_name":"x","oauth_binding_id":"y"}`},
		{provider: "linear", name: "default", payload: `{"apiKey":"lin_ok"}`},
	}
}

func seedRepoCredentials(t *testing.T, ctx context.Context, admin *pgxpool.Pool, venue *venueoracle.Venue, org uuid.UUID, rows []repoSeedRow) map[string]string {
	t.Helper()
	var calls []venueoracle.PythonCall
	var callRows []int
	for i, row := range rows {
		if row.noPayload || row.ciphertext != "" {
			continue
		}
		calls = append(calls, venueoracle.PythonCall{Target: "dev_health_ops.core.encryption:encrypt_value", Args: []any{row.payload}})
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
		config := any("{}")
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

func repoRequests(tokens map[string]string, ids map[string]string, otherOrgID string) []venueoracle.Request {
	admin := map[string]string{"Authorization": "Bearer " + tokens["admin"]}
	var out []venueoracle.Request
	add := func(name string, headers map[string]string, path string) {
		out = append(out, venueoracle.Request{Name: name, Method: "GET", Path: path, Headers: headers})
	}
	repos := func(row, query string) string {
		path := "/api/v1/admin/credentials/" + ids[row] + "/repos"
		if query != "" {
			path += "?" + query
		}
		return path
	}
	list := func(name, row, query string) { add("repos: "+name, admin, repos(row, query)) }

	id := ids["gitlab/default"]
	add("repos: anonymous", nil, repos("gitlab/default", ""))
	add("repos: member", map[string]string{"Authorization": "Bearer " + tokens["member"]}, repos("gitlab/default", ""))
	add("repos: other org's admin", map[string]string{"Authorization": "Bearer " + tokens["superuser"]}, repos("gitlab/default", ""))
	add("repos: a credential of another org", admin, "/api/v1/admin/credentials/"+otherOrgID+"/repos")
	add("repos: malformed id", admin, "/api/v1/admin/credentials/not-a-uuid/repos")
	add("repos: unknown id", admin, "/api/v1/admin/credentials/11111111-1111-4111-8111-111111111111/repos")
	add("repos: id in braces", admin, "/api/v1/admin/credentials/%7B"+id+"%7D/repos")
	add("repos: id upper case", admin, "/api/v1/admin/credentials/"+strings.ToUpper(id)+"/repos")
	add("repos: id without dashes", admin, "/api/v1/admin/credentials/"+strings.ReplaceAll(id, "-", "")+"/repos")
	add("repos: id as a urn", admin, "/api/v1/admin/credentials/urn:uuid:"+id+"/repos")
	add("repos: bad max_repos beats a missing credential", admin, "/api/v1/admin/credentials/not-a-uuid/repos?max_repos=abc")

	// Query validation: max_repos is a pydantic int.
	for _, value := range []string{"abc", "1.5", "", "%207%20", "1_0", "%2B3", "0x10", "1e2", "%D9%A3", "-", "99999999999999999999999999", "5.0", "true"} {
		list("max_repos="+value, "gitlab/default", "max_repos="+value)
	}
	list("max_repos repeated keeps the last", "gitlab/default", "max_repos=abc&max_repos=2")

	// The membership listing and the group listing.
	list("membership listing", "gitlab/default", "")
	for _, query := range []string{"search=api", "search=API", "search=%5Ba", "search=a%20b", "search=", "search=%2A", "search=a&search=web"} {
		list("membership "+query, "gitlab/default", query)
	}
	for _, query := range []string{"owner=grp", "owner=", "owner=grp&search=web", "owner=grp&search=", "owner=missing", "owner=empty", "owner=a%2Fb%20c", "owner=g%C3%BCppe%3F%26%23",
		"owner=a&owner=grp", "owner=grp&search=x%20y%2Bz~*", "owner=engineering&max_repos=1"} {
		list(query, "gitlab/default", query)
	}
	for _, query := range []string{"max_repos=1", "max_repos=2", "max_repos=3", "max_repos=4", "max_repos=0", "max_repos=-1", "max_repos=-2", "max_repos=-3", "max_repos=-4", "max_repos=100000", "max_repos=99999999999999999999999999", "max_repos=-99999999999999999999999999"} {
		list("membership "+query, "gitlab/default", query)
		list("group "+query, "gitlab/default", query+"&owner=grp")
	}
	// Pages: the cap decides how many are fetched and what is cut.
	for _, query := range []string{"", "max_repos=1", "max_repos=100", "max_repos=101", "max_repos=150", "max_repos=0", "max_repos=-1", "max_repos=-5", "max_repos=-104", "max_repos=1000",
		"search=p1", "search=P1&max_repos=3", "search=p1&max_repos=0", "search=p1&max_repos=-1", "search=zzz", "search=%2A5&max_repos=2", "owner=grp&max_repos=150"} {
		list("paged "+query, "gitlab/paged", query)
	}

	// The stored group, and which source names the instance.
	list("configured group", "gitlab/group", "")
	list("configured group with search", "gitlab/group", "search=eng")
	list("owner beats the configured group", "gitlab/group", "owner=grp")
	list("empty owner falls back to the configured group", "gitlab/group", "owner=")
	list("empty configured group", "gitlab/groupempty", "")
	list("non-string configured group", "gitlab/groupnonstr", "")
	for _, name := range []string{"decurl", "decbase", "cfgurl", "cfgbase", "nonstrurl"} {
		list("url from "+name, "gitlab/"+name, "")
		list("group listing with url from "+name, "gitlab/"+name, "owner=grp")
	}

	// Credential problems.
	for _, name := range []string{"notoken", "emptytoken", "nulltoken", "numtoken", "localhost", "private", "userinfo", "unresolvable", "scheme", "badurlfirst",
		"emptypayload", "nullpayload", "listpayload", "stringpayload", "nopayload", "unreadable", "cfglist", "cfglisturl", "cfgfalsy", "cfgnull", "cfgstring"} {
		list("credential "+name, "gitlab/"+name, "")
	}
	list("config list with an owner reads no config group", "gitlab/cfglist", "owner=grp")
	list("credential emptypayload with an owner", "gitlab/emptypayload", "owner=grp")

	// Provider answers.
	for _, name := range []string{"denied", "forbidden", "notfound", "teapot", "junk", "dict", "baddesc", "badurl", "infinity", "ratelimit"} {
		list("provider answer "+name, "gitlab/"+name, "")
	}
	list("provider forbidden group listing", "gitlab/forbidden", "owner=grp")
	list("provider infinity capped away", "gitlab/infinity", "max_repos=1")
	list("provider infinity reached", "gitlab/infinity", "max_repos=2")

	// Other providers.
	for _, row := range []string{"jira/default", "jira/listpayload", "custom/default", "pagerduty/default", "linear/default"} {
		list("provider "+row, row, "")
		list("provider "+row+" with a bad max_repos", row, "max_repos=x")
	}

	// A method the route does not have.
	out = append(out, venueoracle.Request{Name: "repos: PUT is not a route", Method: "PUT", Path: repos("gitlab/default", ""), Headers: admin})
	out = append(out, venueoracle.Request{Name: "repos: POST is not a route", Method: "POST", Path: repos("gitlab/default", ""), Headers: admin})
	return out
}

// TestVenueOracleCredentialRepos is the repository listing's differential: the
// real Python api and this Go api answer every request the same and send the
// same requests to the GitLab stub.
func TestVenueOracleCredentialRepos(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	stub := newRepoStub(t)
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

	var seed venueFixture
	var ids map[string]string
	var otherOrgID string
	rows := repoSeedRows()
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root: venueRoot(), JWTKey: venueKey, Logger: quietLogger(),
		PythonEnv: []string{
			"SETTINGS_ENCRYPTION_KEY=" + credentialsVenueKey,
			"VENUE_PROVIDER_STUB_PORT=" + stub.port(),
			"PYTHONPATH=" + siteDir + ":" + filepath.Join(venueRoot(), "src"),
		},
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, venue *venueoracle.Venue) map[string]map[string]any {
			seed = venueSeed(t, ctx, admin)
			ids = seedRepoCredentials(t, ctx, admin, venue, seed.orgA, rows)
			other := seedRepoCredentials(t, ctx, admin, venue, seed.orgB, []repoSeedRow{{provider: "gitlab", name: "default", payload: `{"token":"gl_ok"}`}})
			otherOrgID = other["gitlab/default"]
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
	requests := repoRequests(venue.Tokens, ids, otherOrgID)
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
