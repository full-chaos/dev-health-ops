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
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
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
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	if provider, ok := providerHosts[host]; ok {
		return provider
	}
	if strings.HasSuffix(host, ".atlassian.net") {
		return "jira"
	}
	return ""
}

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

	body []byte
}

func (f *Fixture) matches(provider, method, path string, query map[string][]string) bool {
	if f.Provider != provider || !strings.EqualFold(f.Method, method) {
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
		got, ok := query[key]
		if !ok || len(got) == 0 || got[0] != want {
			return false
		}
	}
	return true
}

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
	case f.Method == "":
		return fmt.Errorf("%s: no method", where)
	case !strings.HasPrefix(f.Path, "/"):
		return fmt.Errorf("%s: path %q must start with /", where, f.Path)
	case f.Status < 100 || f.Status > 599 || f.Status == 599:
		return fmt.Errorf("%s: status %d (599 is reserved for unstubbed requests)", where, f.Status)
	case len(f.Body) > 0 && f.BodyFile != "":
		return fmt.Errorf("%s: both body and body_file", where)
	}
	return nil
}

// Recorded is one request the stub answered (or refused).
type Recorded struct {
	At       time.Time `json:"at"`
	Provider string    `json:"provider"`
	Host     string    `json:"host"`
	Method   string    `json:"method"`
	Path     string    `json:"path"`
	Query    string    `json:"query"`
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
				body, err := os.ReadFile(filepath.Join(filepath.Dir(file), fixture.BodyFile)) // #nosec G304 -- relative to the fixture file
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
	return stub, nil
}

// ServeHTTP answers a provider request: the first matching fixture, else 599.
func (s *Stub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	provider := ProviderFor(r.Host)
	record := Recorded{
		At: time.Now().UTC(), Provider: provider, Host: r.Host, Method: r.Method,
		Path: r.URL.Path, Query: r.URL.RawQuery, AuthKind: authKind(r),
	}
	var matched *Fixture
	if provider != "" {
		s.mu.Lock()
		for _, fixture := range s.fixtures {
			if fixture.matches(provider, r.Method, r.URL.Path, r.URL.Query()) {
				matched = fixture
				break
			}
		}
		s.mu.Unlock()
	}
	if matched == nil {
		record.Status = 599
		s.record(record)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Providerstub", "unstubbed")
		w.WriteHeader(599)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error": "providerstub: unstubbed request", "provider": provider, "host": r.Host,
			"method": r.Method, "path": r.URL.Path,
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
	_, _ = w.Write(matched.body)
}

func (s *Stub) record(r Recorded) {
	s.mu.Lock()
	s.recorded = append(s.recorded, r)
	s.mu.Unlock()
}

// authKind names the kind of credential header a request carried, never its
// value.
func authKind(r *http.Request) string {
	if v := r.Header.Get("Authorization"); v != "" {
		if scheme, _, ok := strings.Cut(v, " "); ok {
			return scheme
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
	return mux
}

func writeJSON(w http.ResponseWriter, v any) {
	if v == nil {
		v = []Recorded{}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
