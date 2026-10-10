// Package websmoke is the bigboy web-path smoke (CHAOS-6987/R460, ported to Go by CHAOS-8647):
// a REAL browser session, end to end. It is the `dho smoke web-path` verb.
//
// Every request goes to the router (Config.Target) with the PUBLIC Host header
// (Config.PublicHost), exactly as cloudflared delivers a browser request, so each call takes
// the path a real user's browser takes:
//
//	router (public-host rule) -> web -> proxy.ts (session cookie -> bearer)
//	  -> BACKEND_URL (router, Host: traefik) -> plane-split router -> go-api / query-api
//
// The session is a real Auth.js session: /api/auth/csrf, then the Credentials provider
// callback, which calls the backend login exactly as web/src/lib/auth.ts does. No hand-minted
// token is used.
//
// GraphQL documents sent are the registered wire-form documents of the Go plane
// (Config.Documents), never re-printed here. Web's own document text is read from its source
// (Config.WebSrc, `export const NAME = `...`;`) and after urql's formatDocument rule must carry exactly the
// GraphQL tokens of the registered document (whitespace and commas aside), and the
// registered document's sha256 must be a digest the edge catalog (Config.Catalog) holds for its
// operation (current or legacy); either failing is document_digest_mismatch, so a change in web's
// document is caught. Web's document may equal ANY document the build registers for the operation,
// the current one or a legacy one (a two-step pin: this ops with the older web); a legacy match is
// named in the receipt (legacy_documents), never reported as a mismatch (CHAOS-9146). REST paths must still appear as a literal in the web source file that
// calls them.
//
// KNOWN-MISSING operations come from the bigboy routing-ops list (ci/bigboy). A known-missing check is
// reported by name and makes the exit code 3, never 0; if it starts passing, the smoke fails so
// the marker is removed.
//
// Credentials: the email in Config.Email and the password FILE (the file's content is the
// password, the *_FILE convention, CHAOS-6972). The password, session cookies, access token and
// org id live in memory only: never printed, never in the receipt. The receipt carries
// structural facts only (status, plane, counts, booleans).
package websmoke

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/goapidigest"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/server"
)

// Exit codes of the verb.
const (
	ExitPass      = 0
	ExitFail      = 1
	ExitRefused   = 2
	ExitKnownGap  = 3
	planeHeader   = "X-Dev-Health-Plane"
	unavailable   = "Data service unavailable" // web/src/components/ServiceUnavailable.tsx
	defaultPlane  = "go"
	healthHost    = "traefik"
	receiptFormat = "2006-01-02T15:04:05Z"
)

var (
	allowedPublicHosts = map[string]bool{"www.commanderkeen.dev": true, "commanderkeen.dev": true}
	allowedHostHeaders = map[string]bool{"traefik": true, "www.commanderkeen.dev": true, "commanderkeen.dev": true}
)

// restSources maps each REST path to the web source file (relative to web/src) that calls it.
var restSources = map[string]string{
	"/api/v1/investment/explain": "lib/api/investment.ts",
	"/api/v1/work-units":         "lib/api/investment.ts",
	"/api/v1/drilldown/prs":      "lib/api/investment.ts",
	"/api/v1/investment":         "lib/api/investment.ts",
	"/api/v1/filters/options":    "components/filters/useFilterOptions.ts",
	"/api/v1/flame":              "lib/api/visuals.ts",
	"/health":                    "lib/api/system.ts",
}

// graphqlSources maps each GraphQL operation to (web source file, exported constant). The
// constant must still exist in web; the document sent is the registered one.
var graphqlSources = []struct{ Op, File, Const string }{
	{"complexityTimeseries", "lib/graphql/queries.ts", "COMPLEXITY_TIMESERIES_QUERY"},
	{"workGraphFlow", "lib/graphql/queries.ts", "WORK_GRAPH_FLOW_QUERY"},
	{"testopsRisk", "lib/testops/queries.ts", "TESTOPS_RISK_QUERY"},
	{"home", "lib/graphql/queries.ts", "HOME_QUERY"},
	{"recommendations", "lib/graphql/queries.ts", "RECOMMENDATIONS_QUERY"},
	{"workItemTeamAttributions", "lib/graphql/queries.ts", "WORK_ITEM_TEAM_ATTRIBUTIONS_QUERY"},
}

// rowKeys are the response keys that carry rows, in the order the counter tries them.
var rowKeys = []string{"tiles", "rows", "items", "points", "opportunities", "frames", "theme_distribution"}

// Target is the router the smoke connects to.
type Target struct {
	Host string
	Port int
}

// ParseBaseURL accepts only http://traefik[:port] and names the rule a refused target broke.
func ParseBaseURL(raw string) (Target, error) {
	parts, err := url.Parse(raw)
	if err != nil {
		return Target{}, errors.New("base_url_scheme_not_allowed=missing")
	}
	if parts.Scheme != "http" {
		scheme := parts.Scheme
		if scheme == "" {
			scheme = "missing"
		}
		return Target{}, fmt.Errorf("base_url_scheme_not_allowed=%s", scheme)
	}
	host := parts.Hostname()
	if host != "traefik" {
		if host == "" {
			host = "missing"
		}
		return Target{}, fmt.Errorf("base_url_host_not_allowed=%s", host)
	}
	if parts.User != nil || parts.RawQuery != "" || parts.Fragment != "" || (parts.Path != "" && parts.Path != "/") {
		return Target{}, errors.New("base_url_must_be_scheme_host_port_only")
	}
	port := 80
	if p := parts.Port(); p != "" {
		n, err := strconv.Atoi(p)
		if err != nil || n <= 0 || n > 65535 {
			return Target{}, errors.New("base_url_port_invalid")
		}
		port = n
	}
	return Target{Host: host, Port: port}, nil
}

// PublicHostAllowed reports whether host may travel in the Host header.
func PublicHostAllowed(host string) bool { return allowedPublicHosts[host] }

// Config is everything one run needs. It holds no credential value: the password stays in
// PasswordFile until login reads it.
type Config struct {
	Target       Target
	PublicHost   string
	Email        string
	PasswordFile string
	WebSrc       string
	Catalog      string
	RoutingOps   string
	ReceiptPath  string
	// Documents returns the registered wire-form document of an operation.
	Documents func(operation string) (string, bool)
	// RegisteredDocuments returns every document the build registers for an operation, the
	// current one first, then the legacy ones (CHAOS-9146). Web's document passes when it
	// carries the tokens of any of them and that text's digest is in the catalog. Nil means the
	// current document only (Documents).
	RegisteredDocuments func(operation string) ([]server.RegisteredDocument, bool)
	// Now is the clock; nil means time.Now.
	Now func() time.Time
}

func (c Config) now() time.Time {
	if c.Now != nil {
		return c.Now().UTC()
	}
	return time.Now().UTC()
}

// smokeFailure is a short, named reason, never a credential or body value.
type smokeFailure struct{ reason string }

func (e *smokeFailure) Error() string { return e.reason }

func fail(format string, a ...any) error { return &smokeFailure{fmt.Sprintf(format, a...)} }

// knownMissing marks a check for a KNOWN-MISSING operation that did not pass (expected).
type knownMissing struct{ reason string }

func (e *knownMissing) Error() string { return e.reason }

// ---------------------------------------------------------------------------------------------
// Pure helpers
// ---------------------------------------------------------------------------------------------

// defaultFilter is web's defaultMetricFilter after normalizeFilters (team scope with no ids ->
// org scope): web/src/lib/filters/defaults.ts and lib/api/_shared.ts.
func defaultFilter(days int) map[string]any {
	return map[string]any{
		"time":  map[string]any{"range_days": days, "compare_days": days},
		"scope": map[string]any{"level": "org", "ids": []any{}},
		"who":   map[string]any{},
		"what":  map[string]any{},
		"why":   map[string]any{},
		"how":   map[string]any{},
	}
}

// EncodeFilter is web/src/lib/filters/encode.ts encodeFilter: sorted-key compact JSON, base64url
// without padding.
func EncodeFilter(filters map[string]any) string {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(filters) // map[string]any of JSON scalars cannot fail
	return base64.RawURLEncoding.EncodeToString(bytes.TrimRight(buf.Bytes(), "\n"))
}

// DocumentDigest is the go_api_document_digest of a document text.
func DocumentDigest(text string) string { return goapidigest.Document(text) }

// ParseKnownMissing reads routing-ops.txt: "<op> KNOWN-MISSING <reason>" lines.
func ParseKnownMissing(text string) map[string]string {
	out := map[string]string{}
	for _, raw := range strings.Split(text, "\n") {
		line, _, _ := strings.Cut(raw, "#")
		parts := strings.Fields(line)
		if len(parts) >= 3 && parts[1] == "KNOWN-MISSING" {
			out[parts[0]] = parts[2]
		}
	}
	return out
}

// ReadSecretFile is the *_FILE convention: the content minus one trailing CR/LF and surrounding
// whitespace. It returns the value and the file's byte length (the only fact that may be
// reported).
func ReadSecretFile(path string) (string, int, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", 0, err
	}
	text := string(raw)
	switch {
	case strings.HasSuffix(text, "\r\n"):
		text = text[:len(text)-2]
	case strings.HasSuffix(text, "\n"), strings.HasSuffix(text, "\r"):
		text = text[:len(text)-1]
	}
	return strings.TrimSpace(text), len(raw), nil
}

func pyTypeName(v any) string {
	switch v.(type) {
	case nil:
		return "NoneType"
	case string:
		return "str"
	case bool:
		return "bool"
	case json.Number:
		return "number"
	default:
		return fmt.Sprintf("%T", v)
	}
}

func isEmptyValue(v any) bool {
	switch t := v.(type) {
	case nil:
		return true
	case bool:
		return !t
	case string:
		return t == ""
	case json.Number:
		f, err := t.Float64()
		return err == nil && f == 0
	case []any:
		return len(t) == 0
	case map[string]any:
		return len(t) == 0
	}
	return false
}

// CountRows is the structural entry count by known response shape. An unknown shape FAILS (it
// names the top-level keys): counting zero for a shape the counter does not know would read as
// "no data" when it is "not measured".
func CountRows(body any) (int, error) {
	switch t := body.(type) {
	case []any:
		return len(t), nil
	case map[string]any:
		for _, key := range rowKeys {
			switch v := t[key].(type) {
			case []any:
				return len(v), nil
			case map[string]any:
				n := 0
				for _, e := range v {
					if !isEmptyValue(e) {
						n++
					}
				}
				return n, nil
			}
		}
		if data, ok := t["data"].(map[string]any); ok {
			return CountRows(data)
		}
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		joined := strings.Join(keys, ",")
		if len(joined) > 200 {
			joined = joined[:200]
		}
		return 0, fail("unknown_response_shape keys=%s", joined)
	default:
		return 0, fail("unknown_response_shape type=%s", pyTypeName(body))
	}
}

// ---------------------------------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------------------------------

type cookie struct{ name, value string }

// cookieJar is name -> value, kept in first-set order. Secure cookies are kept although the hop
// to the router is plain http: the browser's hop is https, and this jar stands in for the browser.
type cookieJar struct{ items []cookie }

func (j *cookieJar) set(name, value string) {
	for i := range j.items {
		if j.items[i].name == name {
			j.items[i].value = value
			return
		}
	}
	j.items = append(j.items, cookie{name, value})
}

func (j *cookieJar) drop(name string) {
	for i := range j.items {
		if j.items[i].name == name {
			j.items = append(j.items[:i], j.items[i+1:]...)
			return
		}
	}
}

func (j *cookieJar) update(setCookies []string) {
	for _, raw := range setCookies {
		first, attrs, _ := strings.Cut(raw, ";")
		name, value, _ := strings.Cut(strings.TrimSpace(first), "=")
		attrText := strings.ReplaceAll(strings.ToLower(attrs), " ", "")
		expired := strings.Contains(attrText, "max-age=0") || strings.Contains(attrText, "expires=thu,01jan1970")
		if expired || value == "" {
			j.drop(name)
		} else {
			j.set(name, value)
		}
	}
}

func (j *cookieJar) header() string {
	parts := make([]string, len(j.items))
	for i, c := range j.items {
		parts[i] = c.name + "=" + c.value
	}
	return strings.Join(parts, "; ")
}

func (j *cookieJar) find(suffix string) (string, bool) {
	for _, c := range j.items {
		if strings.HasSuffix(c.name, suffix) {
			return c.value, true
		}
	}
	return "", false
}

func (j *cookieJar) hasSession() bool {
	for _, c := range j.items {
		if strings.Contains(c.name, "session-token") {
			return true
		}
	}
	return false
}

type response struct {
	status int
	body   any // decoded JSON; nil when the body is not JSON (an empty body decodes to {})
	header http.Header
	rawLen int
	text   string
}

func (r *response) plane() string { return r.header.Get(planeHeader) }

type client struct {
	cfg  Config
	http *http.Client
}

func newClient(cfg Config) *client {
	addr := net.JoinHostPort(cfg.Target.Host, strconv.Itoa(cfg.Target.Port))
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	return &client{cfg: cfg, http: &http.Client{
		// A redirect is a result the session checks read, never followed.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
				return dialer.DialContext(ctx, network, addr)
			},
			DisableKeepAlives: true,
		},
	}}
}

type reqOpts struct {
	host    string
	jar     *cookieJar
	jsonVal any
	form    url.Values
	timeout time.Duration
}

func (c *client) do(method, path string, o reqOpts) (*response, error) {
	if !strings.HasPrefix(path, "/") {
		return nil, errors.New("request_path_must_be_absolute")
	}
	host := o.host
	if host == "" {
		host = c.cfg.PublicHost
	}
	if !allowedHostHeaders[host] {
		return nil, errors.New("host_header_not_allowed")
	}
	timeout := o.timeout
	if timeout == 0 {
		timeout = 60 * time.Second
	}
	var body io.Reader
	contentType := ""
	switch {
	case o.jsonVal != nil:
		b, err := json.Marshal(o.jsonVal)
		if err != nil {
			return nil, err
		}
		body, contentType = bytes.NewReader(b), "application/json"
	case o.form != nil:
		body, contentType = strings.NewReader(o.form.Encode()), "application/x-www-form-urlencoded"
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, "http://"+host+path, body)
	if err != nil {
		return nil, err
	}
	req.Host = host
	req.Header.Set("X-Forwarded-Proto", "https")
	req.Header.Set("Accept", "application/json, text/html")
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if o.jar != nil && len(o.jar.items) > 0 {
		req.Header.Set("Cookie", o.jar.header())
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if o.jar != nil {
		o.jar.update(resp.Header.Values("Set-Cookie"))
	}
	r := &response{status: resp.StatusCode, header: resp.Header, rawLen: len(raw), text: string(raw)}
	if len(raw) == 0 {
		r.body = map[string]any{}
	} else {
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		var v any
		if err := dec.Decode(&v); err == nil && !dec.More() {
			r.body = v
		}
	}
	return r, nil
}

// ---------------------------------------------------------------------------------------------
// Checks
// ---------------------------------------------------------------------------------------------

type result map[string]any

type runner struct {
	cfg          Config
	c            *client
	jar          *cookieJar
	orgID        string
	documents    map[string]string
	known        map[string]string
	flameEntity  string
	teamID       string
	publicOrigin string
}

func asMap(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

func planeValue(r *response) any {
	if p := r.plane(); p != "" {
		return p
	}
	return nil
}

func orMissing(s string) string {
	if s == "" {
		return "missing"
	}
	return s
}

// assertGo is the gate every Go-plane check runs: 200, plane go, then (optionally) rows. Rows
// are counted only after status and plane passed, so an error body is reported as the status it
// is, not as an unknown shape.
func assertGo(name string, r *response, rows func() (int, error), needRows bool) (result, error) {
	plane := r.plane()
	res := result{"status": r.status, "plane": planeValue(r)}
	if r.status != 200 {
		return nil, fail("%s_status_%d", name, r.status)
	}
	if plane != defaultPlane {
		return nil, fail("%s_plane_%s", name, orMissing(plane))
	}
	if rows == nil {
		return res, nil
	}
	n, err := rows()
	if err != nil {
		return nil, fail("%s_%s", name, err.Error())
	}
	res["row_count"] = n
	if needRows && n <= 0 {
		return nil, fail("%s_zero_rows", name)
	}
	return res, nil
}

func fixed(n int) func() (int, error) { return func() (int, error) { return n, nil } }

func (s *runner) webSource() (result, error) {
	var missing []string
	paths := make([]string, 0, len(restSources))
	for p := range restSources {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, p := range paths {
		text, err := os.ReadFile(filepath.Join(s.cfg.WebSrc, restSources[p]))
		if err != nil || !strings.Contains(string(text), `"`+p+`"`) {
			missing = append(missing, p)
		}
	}
	if len(missing) > 0 {
		return nil, fail("web_source_path_missing=%s", strings.Join(missing, ","))
	}
	rawCatalog, err := os.ReadFile(s.cfg.Catalog)
	if err != nil {
		return nil, fail("catalog_unreadable")
	}
	var entries []struct {
		Operation string
		Digest    string
	}
	if err := json.Unmarshal(rawCatalog, &entries); err != nil {
		return nil, fail("catalog_unparsable")
	}
	// Every digest of an operation is accepted (current and legacy rows), whatever their order.
	catalog := map[string]map[string]bool{}
	for _, e := range entries {
		if catalog[e.Operation] == nil {
			catalog[e.Operation] = map[string]bool{}
		}
		catalog[e.Operation][e.Digest] = true
	}
	var mismatched, legacy []string
	for _, g := range graphqlSources {
		text, err := os.ReadFile(filepath.Join(s.cfg.WebSrc, g.File))
		if err != nil {
			return nil, fail("web_source_document_missing=%s", g.Const)
		}
		webDoc, ok := tsDocument(string(text), g.Const)
		if !ok {
			return nil, fail("web_source_document_missing=%s", g.Const)
		}
		doc, ok := s.cfg.Documents(g.Op)
		if !ok {
			return nil, fail("registered_document_missing=%s", g.Op)
		}
		// Web's own text, formatted as urql formats it (the Python urql_format rule), must carry the
		// GraphQL tokens of a registered document, __typename placement included, and that document must hash to
		// the catalog: so a change in web's document is a mismatch, exactly as the printed-document digest was.
		// The registered documents are the current one and the legacy ones (CHAOS-9146): an older web
		// of a two-step pin sends a legacy text, which the edge accepts, and the receipt says "legacy".
		candidates := []server.RegisteredDocument{{Text: doc}}
		if s.cfg.RegisteredDocuments != nil {
			all, known := s.cfg.RegisteredDocuments(g.Op)
			if !known || len(all) == 0 {
				return nil, fail("registered_document_missing=%s", g.Op)
			}
			candidates = all
		}
		matched, found := server.RegisteredDocument{}, false
		for _, candidate := range candidates {
			if sameTokens(webDoc, candidate.Text) && catalog[g.Op][DocumentDigest(candidate.Text)] {
				matched, found = candidate, true
				break
			}
		}
		if !found {
			mismatched = append(mismatched, g.Op)
		} else if matched.Legacy {
			legacy = append(legacy, g.Op)
		}
		// The smoke always SENDS the current registered document, whichever web carries.
		s.documents[g.Op] = doc
	}
	if len(mismatched) > 0 {
		return nil, fail("document_digest_mismatch=%s", strings.Join(mismatched, ","))
	}
	out := result{"rest_paths": len(restSources), "graphql_documents": len(graphqlSources), "digests_match": true}
	if len(legacy) > 0 {
		out["legacy_documents"] = strings.Join(legacy, ",")
	}
	return out, nil
}

// publicHostUnauth (D2735): a browser request without a session must land on web (303), never a plane.
func (s *runner) publicHostUnauth() (result, error) {
	r, err := s.c.do("GET", "/api/v1/work-units?f="+EncodeFilter(defaultFilter(14)), reqOpts{})
	if err != nil {
		return nil, err
	}
	location := ""
	if u, err := url.Parse(r.header.Get("Location")); err == nil {
		location = u.Path
	}
	plane := r.plane()
	if r.status != 303 || location != "/auth/signin" || plane != "" {
		return nil, fail("public_host_unauth_not_on_web status=%d plane=%s", r.status, orNone(plane))
	}
	return result{"status": r.status, "plane": planeValue(r), "location_path": location}, nil
}

func orNone(s string) string {
	if s == "" {
		return "none"
	}
	return s
}

func (s *runner) login() (result, error) {
	password, fileBytes, err := ReadSecretFile(s.cfg.PasswordFile)
	if err != nil {
		return nil, fail("password_file_unreadable")
	}
	usedLen := len(password)
	secrets.Register("DHO_SMOKE_ADMIN_PASSWORD", password) // the process logger redacts it from here on
	csrf, err := s.c.do("GET", "/api/auth/csrf", reqOpts{jar: s.jar})
	if err != nil {
		return nil, err
	}
	token, _ := asMap(csrf.body)["csrfToken"].(string)
	if csrf.status != 200 || token == "" {
		return nil, fail("csrf_failed status=%d", csrf.status)
	}
	form := url.Values{
		"csrfToken":   {token},
		"email":       {s.cfg.Email},
		"password":    {password},
		"org_id":      {""},
		"callbackUrl": {s.publicOrigin + "/dashboard"},
	}
	password = ""
	r, err := s.c.do("POST", "/api/auth/callback/credentials", reqOpts{jar: s.jar, form: form})
	form = nil
	if err != nil {
		return nil, err
	}
	location := r.header.Get("Location")
	if (r.status != 302 && r.status != 303) || strings.Contains(location, "error=") || !s.jar.hasSession() {
		code := ""
		if u, err := url.Parse(location); err == nil {
			code = u.Query().Get("error")
		}
		return nil, fail("login_failed status=%d session_cookie=%t error=%s password_file_bytes=%d password_used_len=%d",
			r.status, s.jar.hasSession(), orNone(code), fileBytes, usedLen)
	}
	sess, err := s.c.do("GET", "/api/auth/session", reqOpts{jar: s.jar})
	if err != nil {
		return nil, err
	}
	s.orgID, _ = asMap(asMap(sess.body)["user"])["org_id"].(string)
	if sess.status != 200 || s.orgID == "" {
		return nil, fail("session_missing_org status=%d", sess.status)
	}
	return result{"status": "ok", "org_id_present": true}, nil
}

// callbackOrigin (D2736): Auth.js's callback-url cookie must carry the public origin, not the Node bind address.
func (s *runner) callbackOrigin() (result, error) {
	raw, _ := s.jar.find("authjs.callback-url")
	decoded, err := url.QueryUnescape(raw)
	if err != nil {
		decoded = raw
	}
	origin := ""
	if u, err := url.Parse(decoded); err == nil && u.Scheme != "" {
		origin = u.Scheme + "://" + u.Host
	}
	if origin != s.publicOrigin {
		return nil, fail("callback_url_origin=%s expected=%s", orMissing(origin), s.publicOrigin)
	}
	return result{"cookie_present": true, "origin_is_public": true}, nil
}

// backendHealth is lib/api/system.ts checkApiHealth, as web's server side sends it.
func (s *runner) backendHealth() (result, error) {
	r, err := s.c.do("GET", "/health", reqOpts{host: healthHost})
	if err != nil {
		return nil, err
	}
	ok := asMap(r.body)["status"] == "ok"
	if r.status != 200 || !ok {
		return nil, fail("backend_health status=%d status_ok=%t", r.status, ok)
	}
	return result{"status": r.status, "plane": planeValue(r), "status_ok": ok}, nil
}

// restThread is the Cockpit thread call, the URL CockpitClient.tsx buildThreadApiUrl builds.
func (s *runner) restThread(path, thread string) func() (result, error) {
	return func() (result, error) {
		qs := url.Values{"scope_type": {"org"}, "range_days": {"30"}, "compare_days": {"30"}, "thread": {thread}, "scope_id": {s.orgID}}
		r, err := s.c.do("GET", path+"?"+qs.Encode(), reqOpts{jar: s.jar})
		if err != nil {
			return nil, err
		}
		res, err := assertGo("rest_"+thread, r, func() (int, error) { return CountRows(r.body) }, true)
		if err != nil {
			return nil, err
		}
		res["path"] = path
		return res, nil
	}
}

func (s *runner) filterOptions() (result, error) {
	r, err := s.c.do("GET", "/api/v1/filters/options", reqOpts{jar: s.jar})
	if err != nil {
		return nil, err
	}
	body := asMap(r.body)
	nonEmpty := 0
	for _, v := range body {
		if l, ok := v.([]any); ok && len(l) > 0 {
			nonEmpty++
		}
	}
	if teams, ok := body["teams"].([]any); ok && len(teams) > 0 {
		if first, ok := teams[0].(map[string]any); ok {
			if id, ok := first["id"]; ok && id != nil {
				s.teamID = fmt.Sprint(id)
			}
		} else {
			s.teamID = fmt.Sprint(teams[0])
		}
	}
	return assertGo("filters_options", r, fixed(nonEmpty), true)
}

// investmentExplain is lib/api/investment.ts explainInvestmentMix: POST body + f= and llm_provider=auto.
func (s *runner) investmentExplain() (result, error) {
	qs := url.Values{"f": {EncodeFilter(defaultFilter(14))}, "llm_provider": {"auto"}}
	r, err := s.c.do("POST", "/api/v1/investment/explain?"+qs.Encode(), reqOpts{
		jar:     s.jar,
		jsonVal: map[string]any{"filters": defaultFilter(14), "theme": nil, "subcategory": nil},
		timeout: 180 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	return assertGo("investment_explain", r, fixed(len(asMap(r.body))), true)
}

// workUnits is lib/api/investment.ts getWorkUnits with include_textual=true.
func (s *runner) workUnits() (result, error) {
	qs := url.Values{"f": {EncodeFilter(defaultFilter(14))}, "include_textual": {"true"}}
	r, err := s.c.do("POST", "/api/v1/work-units?"+qs.Encode(), reqOpts{
		jar:     s.jar,
		jsonVal: map[string]any{"filters": defaultFilter(14), "include_textual": true},
		timeout: 120 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	return assertGo("work_units", r, func() (int, error) { return CountRows(r.body) }, true)
}

// drilldownPRs is lib/api/investment.ts getDrilldown; the first PR row feeds the flame check
// (explore/page.tsx builds the flame link as repo_id:number).
func (s *runner) drilldownPRs() (result, error) {
	for _, days := range []int{14, 90} {
		r, err := s.c.do("POST", "/api/v1/drilldown/prs", reqOpts{
			jar:     s.jar,
			jsonVal: map[string]any{"filters": defaultFilter(days), "limit": 50},
		})
		if err != nil {
			return nil, err
		}
		items, _ := asMap(r.body)["items"].([]any)
		res, err := assertGo("drilldown_prs", r, fixed(len(items)), false)
		if err != nil {
			return nil, err
		}
		res["range_days"] = days
		for _, it := range items {
			item := asMap(it)
			repo, isStr := item["repo_id"].(string)
			num, isNum := item["number"].(json.Number)
			if _, err := num.Int64(); isStr && isNum && err == nil {
				s.flameEntity = repo + ":" + num.String()
				return res, nil
			}
		}
	}
	return nil, fail("drilldown_prs_no_pr_row_for_flame")
}

// flame is lib/api/visuals.ts getFlame, entity from a real drilldown row.
func (s *runner) flame() (result, error) {
	if s.flameEntity == "" {
		return nil, fail("flame_no_entity")
	}
	qs := url.Values{"entity_type": {"pr"}, "entity_id": {s.flameEntity}}
	r, err := s.c.do("GET", "/api/v1/flame?"+qs.Encode(), reqOpts{jar: s.jar})
	if err != nil {
		return nil, err
	}
	return assertGo("flame", r, func() (int, error) { return CountRows(r.body) }, true)
}

func (s *runner) graphql(op string, variables map[string]any) (*response, error) {
	return s.c.do("POST", "/graphql", reqOpts{
		jar:     s.jar,
		jsonVal: map[string]any{"query": s.documents[op], "variables": variables},
	})
}

func hasErrors(r *response) bool {
	errs, ok := asMap(r.body)["errors"]
	return ok && !isEmptyValue(errs)
}

func gqlData(r *response, op string) map[string]any {
	return asMap(asMap(asMap(r.body)["data"])[op])
}

func (s *runner) complexity() (result, error) {
	now := s.cfg.now()
	r, err := s.graphql("complexityTimeseries", map[string]any{"input": map[string]any{
		"orgId":       s.orgID,
		"sinceUtc":    now.AddDate(0, 0, -30).Format(receiptFormat),
		"untilUtc":    now.Format(receiptFormat),
		"granularity": "DAY",
		"scope":       "REPO",
		"limit":       50,
	}})
	if err != nil {
		return nil, err
	}
	if hasErrors(r) {
		return nil, fail("complexity_graphql_errors")
	}
	points, _ := gqlData(r, "complexityTimeseries")["points"].([]any)
	// Thin history is legitimate (North Star #13): the row count is reported, not gated.
	return assertGo("complexity", r, fixed(len(points)), false)
}

// workGraphFlow is Diagnose > Work Graph > Inflow-Outflow (useWorkGraphFlow: orgId + filters).
func (s *runner) workGraphFlow() (result, error) {
	r, err := s.graphql("workGraphFlow", map[string]any{"orgId": s.orgID})
	if err != nil {
		return nil, err
	}
	if hasErrors(r) {
		return nil, fail("work_graph_flow_graphql_errors")
	}
	rows, _ := gqlData(r, "workGraphFlow")["rows"].([]any)
	res, err := assertGo("work_graph_flow", r, fixed(len(rows)), true)
	if err != nil {
		return nil, err
	}
	empty := 0
	for _, row := range rows {
		if isEmptyValue(asMap(row)["nodeType"]) {
			empty++
		}
	}
	res["empty_node_type"] = empty
	if empty > 0 {
		return nil, fail("work_graph_flow_empty_node_type=%d", empty)
	}
	return res, nil
}

type dataShape int

const (
	shapeObject dataShape = iota
	shapeList
)

// seededRead (CHAOS-7190): a seeded read op must be answered by the Go plane through the web
// path. Order matters: status and plane first (a Python-plane answer to a Go-only field is the
// D3057 signature and is named <op>_plane_python), then GraphQL errors, then the shape of
// data.<op>. Row counts are reported, not gated (thin history is legitimate).
func (s *runner) seededRead(op string, variables map[string]any, shape dataShape, requiredField string) (result, error) {
	r, err := s.graphql(op, variables)
	if err != nil {
		return nil, err
	}
	res, err := assertGo(op, r, nil, false)
	if err != nil {
		return nil, err
	}
	if hasErrors(r) {
		return nil, fail("%s_graphql_errors", op)
	}
	value := asMap(asMap(r.body)["data"])[op]
	switch shape {
	case shapeObject:
		obj, ok := value.(map[string]any)
		if !ok {
			return nil, fail("%s_data_shape", op)
		}
		if requiredField != "" && obj[requiredField] == nil {
			return nil, fail("%s_field_missing=%s", op, requiredField)
		}
	case shapeList:
		list, ok := value.([]any)
		if !ok {
			return nil, fail("%s_data_shape", op)
		}
		res["row_count"] = len(list)
	}
	return res, nil
}

func (s *runner) home() (result, error) {
	// `freshness` is the block the home page renders first (toHomeResponse in web).
	return s.seededRead("home", map[string]any{
		"orgId":  s.orgID,
		"window": map[string]any{"rangeDays": 14, "compareDays": 14},
	}, shapeObject, "freshness")
}

func (s *runner) recommendations() (result, error) {
	team := s.teamID
	if team == "" {
		team = "smoke-probe"
	}
	return s.seededRead("recommendations", map[string]any{
		"orgId":  s.orgID,
		"team":   team,
		"window": map[string]any{"unit": "DAY", "value": 14},
	}, shapeList, "")
}

func (s *runner) workItemTeamAttributions() (result, error) {
	// A non-empty id that matches no row: an empty list would skip the resolver's work-item
	// filter and scan the whole org on every run (mirrors internal/goapiproof/operations.go).
	return s.seededRead("workItemTeamAttributions", map[string]any{
		"orgId":       s.orgID,
		"workItemIds": []any{"smoke-probe-no-such-item"},
		"teamId":      nil,
	}, shapeList, "")
}

// testopsRisk is the testops/risk page's fetchRiskMetrics (lib/testops/fetchers.ts) with the page's dateRange.
func (s *runner) testopsRisk() (result, error) {
	today := s.cfg.now()
	r, err := s.graphql("testopsRisk", map[string]any{
		"orgId": s.orgID,
		"input": map[string]any{
			"startDate": today.AddDate(0, 0, -14).Format("2006-01-02"),
			"endDate":   today.Format("2006-01-02"),
		},
	})
	if err != nil {
		return nil, err
	}
	errs := hasErrors(r)
	plane := r.plane()
	known := s.known["testopsRisk"]
	if r.status != 200 || errs || plane != defaultPlane {
		if known != "" {
			return nil, &knownMissing{fmt.Sprintf("graphql:testopsRisk (%s) status=%d plane=%s", known, r.status, orMissing(plane))}
		}
		return nil, fail("testops_risk status=%d plane=%s errors=%t", r.status, orMissing(plane), errs)
	}
	if known != "" {
		return nil, fail("testops_risk_known_missing_now_served_by_go: remove the marker in routing-ops.txt")
	}
	return result{"status": r.status, "plane": planeValue(r), "has_errors": errs}, nil
}

// testopsRiskPage is the page that failed in the field: 200 and no ServiceUnavailable panel.
func (s *runner) testopsRiskPage() (result, error) {
	r, err := s.c.do("GET", "/testops/risk", reqOpts{jar: s.jar, timeout: 120 * time.Second})
	if err != nil {
		return nil, err
	}
	gone := strings.Contains(r.text, unavailable)
	if r.status != 200 || gone {
		if known := s.known["testopsRisk"]; known != "" {
			return nil, &knownMissing{fmt.Sprintf("page:testops/risk (%s) status=%d unavailable=%t", known, r.status, gone)}
		}
		return nil, fail("testops_risk_page status=%d unavailable=%t", r.status, gone)
	}
	return result{"status": r.status, "service_unavailable": false, "html_bytes": r.rawLen}, nil
}

// logout: sign-out must redirect to the public origin (D2736), not the Node bind address.
func (s *runner) logout() (result, error) {
	csrf, err := s.c.do("GET", "/api/auth/csrf", reqOpts{jar: s.jar})
	if err != nil {
		return nil, err
	}
	token, _ := asMap(csrf.body)["csrfToken"].(string)
	if token == "" {
		return nil, fail("logout_csrf_failed status=%d", csrf.status)
	}
	r, err := s.c.do("POST", "/api/auth/signout", reqOpts{jar: s.jar, form: url.Values{
		"csrfToken": {token}, "callbackUrl": {s.publicOrigin + "/"},
	}})
	if err != nil {
		return nil, err
	}
	origin := ""
	if u, err := url.Parse(r.header.Get("Location")); err == nil && u.Scheme != "" {
		origin = u.Scheme + "://" + u.Host
	}
	if (r.status != 302 && r.status != 303) || origin != s.publicOrigin {
		return nil, fail("logout_redirect_origin=%s status=%d", orMissing(origin), r.status)
	}
	if s.jar.hasSession() {
		return nil, fail("logout_session_cookie_not_cleared")
	}
	return result{"status": r.status, "redirect_origin_is_public": true, "session_cleared": true}, nil
}

// ---------------------------------------------------------------------------------------------
// Run
// ---------------------------------------------------------------------------------------------

// Receipt is the receipt JSON written to Config.ReceiptPath: the same schema the Python smoke
// wrote (the cut's step and record judges read it). Pinned by a golden test.
type Receipt struct {
	OK           bool             `json:"ok"`
	Failures     []string         `json:"failures"`
	KnownMissing []string         `json:"known_missing"`
	GeneratedAt  string           `json:"generated_at"`
	Checks       []map[string]any `json:"checks"`
}

// Outcome is one run's verdict.
type Outcome struct {
	Receipt Receipt
	Passed  int
}

// ExitCode maps the outcome to the verb's exit code: 1 any failure, 3 only known-missing, else 0.
func (o Outcome) ExitCode() int {
	switch {
	case len(o.Receipt.Failures) > 0:
		return ExitFail
	case len(o.Receipt.KnownMissing) > 0:
		return ExitKnownGap
	}
	return ExitPass
}

// Verdict is the verdict word of the summary line.
func (o Outcome) Verdict() string {
	switch o.ExitCode() {
	case ExitFail:
		return "FAIL"
	case ExitKnownGap:
		return "KNOWN-GAP"
	}
	return "PASS"
}

type step struct {
	name  string
	fn    func() (result, error)
	fatal bool
}

// Run executes every check in order and returns the outcome. A fatal step that fails ends the run.
func Run(cfg Config) Outcome {
	ops, err := os.ReadFile(cfg.RoutingOps)
	s := &runner{
		cfg:          cfg,
		c:            newClient(cfg),
		jar:          &cookieJar{},
		documents:    map[string]string{},
		known:        map[string]string{},
		publicOrigin: "https://" + cfg.PublicHost,
	}
	rec := Receipt{Failures: []string{}, KnownMissing: []string{}, Checks: []map[string]any{}}
	record := func(name, reason string) {
		rec.Failures = append(rec.Failures, name+": "+reason)
		rec.Checks = append(rec.Checks, map[string]any{"check": name, "ok": false, "reason": reason})
	}
	if err != nil {
		// A measurement that did not happen must fail loudly: the known-missing list is unread.
		record("routing_ops", "routing_ops_unreadable")
		rec.OK, rec.GeneratedAt = false, cfg.now().Format(receiptFormat)
		return Outcome{Receipt: rec}
	}
	s.known = ParseKnownMissing(string(ops))
	steps := []step{
		{"web_source", s.webSource, true},
		{"public_host_unauth_on_web", s.publicHostUnauth, false},
		{"login", s.login, true},
		{"callback_url_origin", s.callbackOrigin, false},
		{"backend_health", s.backendHealth, false},
		{"rest:understand", s.restThread("/api/v1/home", "understand"), false},
		{"rest:align", s.restThread("/api/v1/investment", "align"), false},
		{"rest:execute", s.restThread("/api/v1/opportunities", "execute"), false},
		{"rest:filters_options", s.filterOptions, false},
		{"rest:investment_explain", s.investmentExplain, false},
		{"rest:work_units", s.workUnits, false},
		{"rest:drilldown_prs", s.drilldownPRs, false},
		{"rest:flame", s.flame, false},
		{"graphql:complexityTimeseries", s.complexity, false},
		{"graphql:workGraphFlow", s.workGraphFlow, false},
		{"graphql:home", s.home, false},
		{"graphql:recommendations", s.recommendations, false},
		{"graphql:workItemTeamAttributions", s.workItemTeamAttributions, false},
		{"graphql:testopsRisk", s.testopsRisk, false},
		{"page:testops_risk", s.testopsRiskPage, false},
		{"logout", s.logout, false},
	}
	passed := 0
	for _, st := range steps {
		res, err := runStep(st.fn)
		var known *knownMissing
		var failed *smokeFailure
		switch {
		case err == nil:
			entry := map[string]any{"check": st.name, "ok": true}
			for k, v := range res {
				entry[k] = v
			}
			rec.Checks = append(rec.Checks, entry)
			passed++
			continue
		case errors.As(err, &known):
			rec.KnownMissing = append(rec.KnownMissing, known.reason)
			rec.Checks = append(rec.Checks, map[string]any{"check": st.name, "ok": false, "known_missing": true})
			continue
		case errors.As(err, &failed):
			record(st.name, failed.reason)
		default: // an unnamed failure must still fail loud, by type only: the text may carry a URL
			record(st.name, fmt.Sprintf("unexpected_error=%s", errType(err)))
		}
		if st.fatal {
			break
		}
	}
	rec.OK = len(rec.Failures) == 0
	rec.GeneratedAt = cfg.now().Format(receiptFormat)
	return Outcome{Receipt: rec, Passed: passed}
}

func errType(err error) string {
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return fmt.Sprintf("%T", urlErr.Err)
	}
	return fmt.Sprintf("%T", err)
}

func runStep(fn func() (result, error)) (res result, err error) {
	defer func() {
		if r := recover(); r != nil {
			res, err = nil, fmt.Errorf("panic %v", fmt.Sprintf("%T", r))
		}
	}()
	return fn()
}

// WriteReceipt writes the receipt as indented JSON, creating the directory.
func WriteReceipt(path string, rec Receipt) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

// tsDocument is the text of `export const NAME = `...`;` with no ${} interpolation, the same
// extraction the Python smoke used.
func tsDocument(source, name string) (string, bool) {
	re := regexp.MustCompile("export const " + regexp.QuoteMeta(name) + " = `([^`]*)`;")
	m := re.FindStringSubmatch(source)
	if m == nil || strings.Contains(m[1], "${") {
		return "", false
	}
	return m[1], true
}

// graphqlTokens splits a document into its lexical tokens: names, numbers, strings and
// punctuators. Whitespace, commas and comments are insignificant, so a printer's layout is not
// compared.
func graphqlTokens(doc string) []string {
	var out []string
	rs := []rune(doc)
	isName := func(r rune, first bool) bool {
		return r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (!first && r >= '0' && r <= '9')
	}
	for i := 0; i < len(rs); {
		r := rs[i]
		switch {
		case r == ' ' || r == '\t' || r == '\n' || r == '\r' || r == ',' || r == '\ufeff':
			i++
		case r == '#':
			for i < len(rs) && rs[i] != '\n' && rs[i] != '\r' {
				i++
			}
		case r == '.' && i+2 < len(rs) && rs[i+1] == '.' && rs[i+2] == '.':
			out = append(out, "...")
			i += 3
		case r == '"':
			j := i + 1
			if i+2 < len(rs) && rs[i+1] == '"' && rs[i+2] == '"' {
				j = i + 3
				for j+2 < len(rs) && !(rs[j] == '"' && rs[j+1] == '"' && rs[j+2] == '"') {
					j++
				}
				j += 3
			} else {
				for j < len(rs) && rs[j] != '"' {
					if rs[j] == '\\' {
						j++
					}
					j++
				}
				j++
			}
			if j > len(rs) {
				j = len(rs)
			}
			out = append(out, string(rs[i:j]))
			i = j
		case isName(r, true):
			j := i
			for j < len(rs) && isName(rs[j], false) {
				j++
			}
			out = append(out, string(rs[i:j]))
			i = j
		case r == '-' || (r >= '0' && r <= '9'):
			j := i + 1
			for j < len(rs) && (rs[j] == '.' || rs[j] == 'e' || rs[j] == 'E' || rs[j] == '+' || rs[j] == '-' || (rs[j] >= '0' && rs[j] <= '9')) {
				j++
			}
			out = append(out, string(rs[i:j]))
			i = j
		default:
			out = append(out, string(r))
			i++
		}
	}
	return out
}

// urqlFormat is urql's formatDocument rule (urql-core formatNode) applied to a token stream, as
// the Python smoke's urql_format did on the parsed document: `@_name(...)` directives are
// dropped, and `__typename` is appended before the closing brace of every selection set that is
// not an operation's root and has no unaliased `__typename` of its own. Braces inside parentheses
// are object values, not selection sets; fragment definitions and inline fragments are selection
// sets like any other.
func urqlFormat(in []string) []string {
	type frame struct{ root, object, typename bool }
	var out []string
	var stack []frame
	parens := 0
	defStart := 0 // index in `in` of the first token of the current top-level definition
	for i := 0; i < len(in); i++ {
		tok := in[i]
		switch tok {
		case "@":
			if parens == 0 && i+1 < len(in) && strings.HasPrefix(in[i+1], "_") {
				i++ // the directive name
				if i+1 < len(in) && in[i+1] == "(" {
					depth := 0
					for i+1 < len(in) {
						i++
						if in[i] == "(" {
							depth++
						} else if in[i] == ")" {
							depth--
							if depth == 0 {
								break
							}
						}
					}
				}
				continue
			}
		case "(":
			parens++
		case ")":
			parens--
		case "{":
			switch {
			case parens > 0:
				stack = append(stack, frame{object: true})
			case len(stack) == 0:
				stack = append(stack, frame{root: in[defStart] != "fragment"})
			default:
				stack = append(stack, frame{})
			}
		case "}":
			if n := len(stack); n > 0 {
				top := stack[n-1]
				stack = stack[:n-1]
				if !top.object && !top.root && !top.typename {
					out = append(out, "__typename")
				}
				if len(stack) == 0 && !top.object {
					defStart = i + 1
				}
			}
		case "__typename":
			if n := len(stack); n > 0 && parens == 0 && !stack[n-1].object {
				aliased := (len(out) > 0 && out[len(out)-1] == ":") || (i+1 < len(in) && in[i+1] == ":")
				if !aliased {
					stack[n-1].typename = true
				}
			}
		}
		out = append(out, tok)
	}
	return out
}

// sameTokens reports whether web's document text, after urql's formatting, carries exactly the
// GraphQL tokens of the registered (wire-form) document, `__typename` placement included.
func sameTokens(web, registered string) bool {
	x, y := urqlFormat(graphqlTokens(web)), graphqlTokens(registered)
	if len(x) != len(y) {
		return false
	}
	for i := range x {
		if x[i] != y[i] {
			return false
		}
	}
	return true
}
