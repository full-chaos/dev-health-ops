// Package providerstub is the provider stub of the provider-stub venue: ONE
// local HTTP(S) server that answers, from a table of fixtures, the requests
// the Python and Go planes would otherwise send to GitHub, GitLab, Jira,
// Linear and PagerDuty, and RECORDS every request it gets.
//
// Why it exists. Several admin routes (integration discovery, sync-config
// create with discovery, OAuth callbacks, PagerDuty preflight/services/bindings
// with a stored credential) call a provider. Against the real provider such a
// case cannot be a pass row: it is a live call, its answer is not ours to
// control, and the Python plane falls back to PROCESS ENV credentials by
// provider when a stored credential is absent (so a case can silently become a
// real call). In the venue both planes resolve every provider host to this
// stub (hosts file) and trust its CA, every provider credential env key is
// blank, and the containers have no route out: an unstubbed request fails
// LOUDLY (599 + recorded as unmatched) instead of reaching anything real.
//
// What it is not. It does not validate credentials, emulate rate limits or
// pagination beyond what a fixture states, and it is not a provider
// implementation: a fixture answers exactly the request it names. A row that
// needs another endpoint adds one fixture; the recorder shows what was asked.
//
// Credentials are never stored: a recorded request keeps only the KIND of
// authorization header it carried, never its value.
package providerstub

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

// Providers the stub knows, and the hosts that select them. A host that is not
// listed is answered 599 (unstubbed) and recorded with provider "unknown".
var providerHosts = map[string]string{
	"api.github.com":         "github",
	"github.com":             "github",
	"gitlab.com":             "gitlab",
	"api.linear.app":         "linear",
	"api.pagerduty.com":      "pagerduty",
	"api.eu.pagerduty.com":   "pagerduty",
	"identity.pagerduty.com": "pagerduty",
}

// ProviderFor names the provider a Host header selects ("" for none): the
// exact provider hosts above, and any tenant host under .atlassian.net (Jira).
func ProviderFor(host string) string {
	host = strings.ToLower(host)
	if strings.Contains(host, ":") {
		// only the TLS port selects a provider: a bad or other port is not a provider request
		h, port, err := net.SplitHostPort(host)
		if err != nil || port != "443" {
			return ""
		}
		host = h
	}
	// the absolute spelling of a DNS name ("api.github.com.") is the same host; ".." is not
	host = strings.TrimSuffix(host, ".")
	if provider, ok := providerHosts[host]; ok {
		return provider
	}
	if jiraTenant.MatchString(host) {
		return "jira"
	}
	return ""
}

// jiraTenant is one concrete tenant label under atlassian.net: never a wildcard, an empty
// label, a dotted sub-domain or a label that starts or ends with a hyphen.
var jiraTenant = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?\.atlassian\.net$`)

// Hosts lists the fixed provider hosts (for certificate SANs and the hosts
// file). Jira tenant hosts are added by the caller with the tenants it uses.
func Hosts() []string {
	hosts := make([]string, 0, len(providerHosts))
	for host := range providerHosts {
		hosts = append(hosts, host)
	}
	sort.Strings(hosts)
	return hosts
}

// builtinVocabulary is the path vocabulary of the provider APIs a venue row is likely to hit; a
// segment outside it and outside the loaded fixtures is recorded as {x}. It holds no token-like word.
var builtinVocabulary = strings.Fields(`api rest v3 v4 graphql oauth authorize token access_token user users org orgs organizations
	repos repositories repository projects project groups group issues issue pulls pull merge_requests search services service
	incidents teams team members member webhooks hooks installation installations app apps applications integrations abilities
	runners jobs pipelines commits branches tags releases oncalls schedules escalation_policies vendors extensions status serverInfo myself field priority`)

// Fixture answers one request. A request matches when its provider, method and
// path match and every Query entry is present with that value. Path is exact,
// or a prefix when it ends in "*".
type Fixture struct {
	Provider string            `json:"provider"`
	Method   string            `json:"method"`
	Path     string            `json:"path"`
	Query    map[string]string `json:"query,omitempty"`
	Status   int               `json:"status"`
	Headers  map[string]string `json:"headers,omitempty"`
	// Body is the response body as JSON, or BodyFile names a file relative to
	// the fixture file's directory (a recorded body, byte for byte). Not both.
	Body     json.RawMessage `json:"body,omitempty"`
	BodyFile string          `json:"body_file,omitempty"`
	// BodyContains, when set, requires the REQUEST body to contain this text (a POST/PUT/PATCH fixture: e.g. which GraphQL
	// operation a Linear call sends). RequestHeaders, when set, requires each named request header to equal the value exactly
	// (e.g. the Authorization a probe sends, to answer a 401 for one key). Both are compared inside the stub and never recorded.
	BodyContains   string            `json:"body_contains,omitempty"`
	RequestHeaders map[string]string `json:"request_headers,omitempty"`

	body []byte
}

// matches reports whether the request selects this fixture. body is called at most once per request, and only when every other
// selector already matched and this fixture has a body_contains: a fixture with no body selector never waits on the request body.
func (f *Fixture) matches(provider, method, path string, query map[string][]string, header http.Header, body func() ([]byte, bool)) bool {
	if f.Provider != provider || f.Method != method {
		return false
	}
	if strings.HasSuffix(f.Path, "*") {
		if !strings.HasPrefix(path, strings.TrimSuffix(f.Path, "*")) {
			return false
		}
	} else if f.Path != path {
		return false
	}
	for key, want := range f.Query {
		// exactly one value: a duplicate key must not smuggle a second value past the fixture
		got, ok := query[key]
		if !ok || len(got) != 1 || got[0] != want {
			return false
		}
	}
	for name, want := range f.RequestHeaders {
		// exactly one value, like a query key: a repeated header must not smuggle a second value past the fixture.
		// Values canonicalizes the name.
		if got := header.Values(name); len(got) != 1 || got[0] != want {
			return false
		}
	}
	if f.BodyContains != "" {
		raw, complete := body()
		if !complete || !strings.Contains(string(raw), f.BodyContains) {
			return false // a body that was cut short (over the limit or a read error) never matches: its tail is unseen
		}
	}
	return true
}

// maxMatchBody bounds the request body the stub reads to decide a body_contains match; a longer body never matches one.
const maxMatchBody = 1 << 20

var validMethods = map[string]bool{"GET": true, "POST": true, "PUT": true, "PATCH": true, "DELETE": true, "HEAD": true, "OPTIONS": true}

// validate refuses a fixture that cannot answer anything sensibly.
func (f *Fixture) validate(where string) error {
	known := false
	for _, provider := range providerHosts {
		if provider == f.Provider {
			known = true
		}
	}
	switch {
	case !known && f.Provider != "jira":
		return fmt.Errorf("%s: unknown provider %q", where, f.Provider)
	case !validMethods[f.Method]:
		return fmt.Errorf("%s: method %q must be an uppercase HTTP method", where, f.Method)
	case !strings.HasPrefix(f.Path, "/"):
		return fmt.Errorf("%s: path %q must start with /", where, f.Path)
	case f.Status < 200 || f.Status > 598:
		return fmt.Errorf("%s: status %d must be a final status in 200..598 (599 is reserved for unstubbed requests)", where, f.Status)
	case f.BodyContains != "" && f.Method != "POST" && f.Method != "PUT" && f.Method != "PATCH":
		return fmt.Errorf("%s: body_contains needs a POST, PUT or PATCH fixture", where)
	case len(f.BodyContains) > maxMatchBody:
		return fmt.Errorf("%s: body_contains is longer than the %d byte match limit", where, maxMatchBody)
	case len(f.Body) > 0 && f.BodyFile != "":
		return fmt.Errorf("%s: both body and body_file", where)
	}
	for name, value := range f.RequestHeaders {
		if name == "" || value == "" {
			return fmt.Errorf("%s: request_headers needs a non-empty header name and value", where)
		}
		// net/http keeps these out of the request header map (Host in r.Host, the framing in r.TransferEncoding), so a
		// fixture naming one could never match.
		switch strings.ToLower(name) {
		case "host", "transfer-encoding":
			return fmt.Errorf("%s: request_headers cannot select on %q (net/http does not expose it as a header)", where, name)
		}
	}
	return nil
}

// matchBodyTimeout bounds how long the stub waits for a request body it needs for a body_contains match.
const matchBodyTimeout = 5 * time.Second

// readMatchBody reads at most maxMatchBody bytes of the request body, and reports whether that was ALL of it: a read error
// (a stalled or broken client, the read deadline) or a body over the limit reports false, so it never matches.
func readMatchBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	if r.Body == nil {
		return nil, true
	}
	_ = http.NewResponseController(w).SetReadDeadline(time.Now().Add(matchBodyTimeout)) // best effort: not every writer supports it
	raw, err := io.ReadAll(io.LimitReader(r.Body, maxMatchBody+1))
	if err != nil || len(raw) > maxMatchBody {
		if len(raw) > maxMatchBody {
			raw = raw[:maxMatchBody]
		}
		return raw, false
	}
	return raw, true
}

// Recorded is one request the stub answered (or refused).
type Recorded struct {
	At       time.Time `json:"at"`
	Provider string    `json:"provider"`
	Host     string    `json:"host"`
	Method   string    `json:"method"`
	// Path is the path SHAPE: short vocabulary words and api versions are kept, every other segment
	// (an id, a slug, a token) is "{x}". Never the raw path.
	Path string `json:"path"`
	// QueryKeys are the query parameter NAMES, sorted: never their values (some
	// provider APIs accept a token as a query parameter, so a value can be a
	// credential).
	QueryKeys []string `json:"query_keys"`
	// AuthKind is the kind of Authorization-style header the request carried
	// ("Bearer", "Basic", "token", "PRIVATE-TOKEN", "none"): never its value.
	AuthKind string `json:"auth_kind"`
	Status   int    `json:"status"`
	Matched  bool   `json:"matched"`
}

// Stub is the fixture table and the recorder. Safe for concurrent use.
type Stub struct {
	mu       sync.Mutex
	fixtures []*Fixture
	recorded []Recorded
	// Log, when set, receives one line per request (provider, method, status, matched: no path, no values).
	Log func(string)
	// vocab is every literal path segment the fixtures name: the only words a recorded path keeps.
	vocab map[string]bool
}

// New builds a stub from fixtures already in memory.
func New(fixtures ...Fixture) (*Stub, error) {
	stub := &Stub{}
	for i := range fixtures {
		fixture := fixtures[i]
		if err := fixture.validate(fmt.Sprintf("fixture %d", i)); err != nil {
			return nil, err
		}
		if len(fixture.Body) > 0 {
			fixture.body = fixture.Body
		}
		stub.fixtures = append(stub.fixtures, &fixture)
	}
	stub.buildVocab()
	return stub, nil
}

// LoadDir builds a stub from every *.json file in dir: each holds a JSON array
// of fixtures; a body_file is read relative to the file's directory.
func LoadDir(dir string) (*Stub, error) {
	files, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	if len(files) == 0 {
		return nil, fmt.Errorf("providerstub: no fixture files (*.json) in %s", dir)
	}
	stub := &Stub{}
	for _, file := range files {
		raw, err := os.ReadFile(file) // #nosec G304 -- fixture directory chosen by the operator
		if err != nil {
			return nil, err
		}
		var fixtures []Fixture
		if err := json.Unmarshal(raw, &fixtures); err != nil {
			return nil, fmt.Errorf("providerstub: %s: %w", file, err)
		}
		for i := range fixtures {
			fixture := fixtures[i]
			where := fmt.Sprintf("%s[%d]", filepath.Base(file), i)
			if err := fixture.validate(where); err != nil {
				return nil, err
			}
			switch {
			case fixture.BodyFile != "":
				body, err := readConfined(filepath.Dir(file), fixture.BodyFile)
				if err != nil {
					return nil, fmt.Errorf("providerstub: %s: %w", where, err)
				}
				fixture.body = body
			case len(fixture.Body) > 0:
				fixture.body = fixture.Body
			}
			stub.fixtures = append(stub.fixtures, &fixture)
		}
	}
	stub.buildVocab()
	return stub, nil
}

// ServeHTTP answers a provider request: the first matching fixture, else 599.
func (s *Stub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	provider := ProviderFor(r.Host)
	host := "(unknown)"
	if provider != "" {
		host = strings.ToLower(strings.TrimSuffix(r.Host, ":443"))
	}
	record := Recorded{
		At: time.Now().UTC(), Provider: provider, Host: host, Method: knownMethod(r.Method),
		Path: s.shapePath(r.URL.Path), QueryKeys: queryKeys(r), AuthKind: authKind(r),
	}
	var matched *Fixture
	if provider != "" && safePath(r) {
		// the request body is read (bounded, with a deadline) only when a fixture that matches on everything else has a
		// body_contains, and once per request: never recorded, logged or echoed. The fixture table is immutable after
		// New/LoadDir, so no lock is held while a slow client sends its body.
		var (
			body     []byte
			complete bool
			read     bool
		)
		lazyBody := func() ([]byte, bool) {
			if !read {
				body, complete = readMatchBody(w, r)
				read = true
			}
			return body, complete
		}
		query := r.URL.Query()
		for _, fixture := range s.fixtures {
			if fixture.matches(provider, r.Method, r.URL.Path, query, r.Header, lazyBody) {
				matched = fixture
				break
			}
		}
	}
	if matched == nil {
		record.Status = 599
		s.record(record)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Providerstub", "unstubbed")
		w.WriteHeader(599)
		// no path and no query: they can carry a token
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error": "providerstub: unstubbed request", "provider": provider, "method": record.Method,
			"path_shape": record.Path,
		})
		return
	}
	record.Status, record.Matched = matched.Status, true
	s.record(record)
	for name, value := range matched.Headers {
		w.Header().Set(name, value)
	}
	if w.Header().Get("Content-Type") == "" && len(matched.body) > 0 {
		w.Header().Set("Content-Type", "application/json")
	}
	w.WriteHeader(matched.Status)
	// a fixture body is served as recorded bytes, exactly as a real provider would send them
	_, _ = io.Copy(w, bytes.NewReader(matched.body))
}

func (s *Stub) record(r Recorded) {
	s.mu.Lock()
	s.recorded = append(s.recorded, r)
	logf := s.Log
	s.mu.Unlock()
	if logf != nil {
		provider := r.Provider
		if provider == "" {
			provider = "unknown"
		}
		logf(fmt.Sprintf("providerstub request provider=%s host=%s method=%s path=%s status=%d matched=%t auth=%s", provider, r.Host, r.Method, r.Path, r.Status, r.Matched, r.AuthKind))
	}
}

// queryKeys returns the sorted query parameter names of a request (never values).
func queryKeys(r *http.Request) []string {
	keys := make([]string, 0)
	for key := range r.URL.Query() {
		if !keyName.MatchString(key) {
			key = "{key}"
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// authKind names the kind of credential header a request carried, never its
// value.
func authKind(r *http.Request) string {
	if v := r.Header.Get("Authorization"); v != "" {
		if scheme, _, ok := strings.Cut(v, " "); ok {
			switch strings.ToLower(scheme) {
			case "bearer":
				return "Bearer"
			case "basic":
				return "Basic"
			case "token":
				return "token"
			}
			return "other"
		}
		return "opaque"
	}
	if r.Header.Get("PRIVATE-TOKEN") != "" {
		return "PRIVATE-TOKEN"
	}
	return "none"
}

// Requests returns a copy of what was recorded, in arrival order.
func (s *Stub) Requests() []Recorded {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Recorded(nil), s.recorded...)
}

// Unmatched returns the requests no fixture answered.
func (s *Stub) Unmatched() []Recorded {
	var out []Recorded
	for _, r := range s.Requests() {
		if !r.Matched {
			out = append(out, r)
		}
	}
	return out
}

// Reset forgets the recorded requests (the fixtures stay).
func (s *Stub) Reset() {
	s.mu.Lock()
	s.recorded = nil
	s.mu.Unlock()
}

// AdminHandler serves the recorder on its own listener (never the provider
// listener, so a provider path can never collide with it): GET /requests as
// JSON, GET /unmatched as JSON, POST /reset.
func (s *Stub) AdminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /requests", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, s.Requests()) })
	mux.HandleFunc("GET /unmatched", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, s.Unmatched()) })
	mux.HandleFunc("POST /reset", func(w http.ResponseWriter, _ *http.Request) { s.Reset(); w.WriteHeader(http.StatusNoContent) })
	// the recorder is for the operator: a request that arrives under a provider hostname (the venue's
	// hosts mapping) is a plane talking to the wrong port, never an operator
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host := r.Host
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}
		if ProviderFor(host) != "" { // any port: 9090 under a provider name is still a plane at the wrong door
			http.NotFound(w, r)
			return
		}
		mux.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, v any) {
	if v == nil {
		v = []Recorded{}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Stub) buildVocab() {
	s.vocab = map[string]bool{}
	for _, word := range builtinVocabulary {
		s.vocab[word] = true
	}
	for _, fixture := range s.fixtures {
		for _, segment := range strings.Split(strings.TrimSuffix(fixture.Path, "*"), "/") {
			if segment != "" {
				s.vocab[segment] = true
			}
		}
	}
}

// readConfined reads name from dir, refusing an absolute name, a name that
// climbs out of dir, and any symlink that resolves outside it.
func readConfined(dir, name string) ([]byte, error) {
	if !filepath.IsLocal(name) {
		return nil, fmt.Errorf("body_file %q must be a relative path inside the fixture directory", name)
	}
	// os.Root resolves every component inside dir, symlinks included, with no check-then-read gap
	// for a swap to slip through.
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	defer func() { _ = root.Close() }()
	return root.ReadFile(name)
}

var (
	versionSegment = regexp.MustCompile(`^v?[0-9]{1,2}$`)
	keyName        = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_.\[\]-]{0,31}$`)
)

// safePath reports whether the request path can be matched at all: no dot
// segments, no doubled slash, no encoded slash or backslash (each could reach
// a fixture prefix through a path the client did not really mean).
func safePath(r *http.Request) bool {
	path := r.URL.Path
	if !strings.HasPrefix(path, "/") || strings.Contains(path, "\\") {
		return false
	}
	// no fixture path holds a percent sign: a "%" left after one decode is a repeated encoding
	if strings.Contains(path, "%") {
		return false
	}
	for key := range r.URL.Query() {
		if strings.Contains(key, "%") {
			return false // a query key encoded twice would read as a duplicate one layer down
		}
	}
	escaped := strings.ToLower(r.URL.EscapedPath())
	if strings.Contains(escaped, "%2f") || strings.Contains(escaped, "%5c") {
		return false
	}
	segments := strings.Split(path[1:], "/")
	for i, segment := range segments {
		if segment == "." || segment == ".." || (segment == "" && i != len(segments)-1) {
			return false
		}
	}
	return true
}

// shapePath keeps only the vocabulary of a path (the words the fixtures and the builtin list name,
// api versions): every other segment (an id, a slug, a token) becomes {x}, so the recorder can
// name the endpoint that was hit without ever holding a credential.
func (s *Stub) shapePath(path string) string {
	segments := strings.Split(path, "/")
	for i, segment := range segments {
		if segment != "" && !s.vocab[segment] && !versionSegment.MatchString(segment) {
			segments[i] = "{x}"
		}
	}
	return strings.Join(segments, "/")
}

func knownMethod(method string) string {
	if validMethods[method] {
		return method
	}
	return "OTHER"
}
