package routing_test

import (
	"bytes"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cigates/routing"
)

// ---- route coverage (R467): every routed path has a handler in the running build ----

// plane is a fake Go plane: ROUTEPROBE gets the status registered for the path, 404 for any other path.
type plane struct {
	*httptest.Server
	mu      sync.Mutex
	methods []string
}

func newPlane(t *testing.T, registered map[string]int) *plane {
	t.Helper()
	p := &plane{}
	p.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.mu.Lock()
		p.methods = append(p.methods, r.Method+" "+r.RequestURI)
		p.mu.Unlock()
		status, ok := registered[r.RequestURI]
		if !ok {
			status = 404
		}
		w.WriteHeader(status)
	}))
	t.Cleanup(p.Close)
	return p
}

func coverageValues(t *testing.T) string {
	return writeFile(t, t.TempDir(), "v.yaml", dump(t, pinnedValues()))
}

func TestCoveragePassesWhenEveryPathRegistered(t *testing.T) {
	p := newPlane(t, map[string]int{"/api/v1/known": 405, "/api/v1/things/routeprobe": 405, "/api/v1/q": 401})
	r := run(coverage, coverageValues(t), "--go-api-url", p.URL, "--query-api-url", p.URL)
	wantCode(t, r, 0)
	wantIn(t, r.stdout, "plane=go-api routed=2 registered=2 missing=0")
	for _, m := range p.methods {
		if !strings.HasPrefix(m, "ROUTEPROBE ") {
			t.Fatalf("a probe used %s", m)
		}
	}
}

func TestCoverageNamesAMissingHandler(t *testing.T) {
	p := newPlane(t, map[string]int{"/api/v1/known": 405, "/api/v1/q": 405})
	r := run(coverage, coverageValues(t), "--go-api-url", p.URL, "--query-api-url", p.URL)
	wantCode(t, r, 1)
	wantIn(t, r.stderr, "go-api has NO handler for routed path /api/v1/things/routeprobe")
}

func TestCoverageFlagsUnexpectedStatus(t *testing.T) {
	p := newPlane(t, map[string]int{"/api/v1/known": 405, "/api/v1/things/routeprobe": 500, "/api/v1/q": 405})
	r := run(coverage, coverageValues(t), "--go-api-url", p.URL, "--query-api-url", p.URL)
	wantCode(t, r, 1)
	wantIn(t, r.stderr, "/api/v1/things/routeprobe=500")
}

func TestCoverageRefusesNonLocalTargets(t *testing.T) {
	for _, url := range []string{"https://127.0.0.1:1", "http://evil.example:1", "http://127.0.0.1:1/x"} {
		r := run(coverage, coverageValues(t), "--go-api-url", url)
		wantCode(t, r, 1)
		wantIn(t, r.stderr, "check-route-coverage: plane URL must be http://127.0.0.1:<port>, got '"+url+"'")
	}
}

func TestCoverageRefusesValuesWithNoPaths(t *testing.T) {
	v := pinnedValues()
	v["ingress"].(m)["queryApiPaths"] = l{}
	r := run(coverage, writeFile(t, t.TempDir(), "v.yaml", dump(t, v)))
	wantCode(t, r, 1)
	wantIn(t, r.stderr, "values has ZERO goApiPaths or queryApiPaths -- refusing to pass")
	wantCode(t, run(coverage), 2)
}

// ---- query-api flags: names from deploy values, written to --out only ----

func flagValues(extra ...m) m {
	env := l{
		m{"name": "QUERY_API_INTERNAL_ADDR", "value": ":8091"},
		m{"name": "GO_API_B_ENABLED", "value": "true"},
		m{"name": "GO_API_A_ENABLED", "value": "true"},
		m{"name": "GO_API_OFF_ENABLED", "value": "false"},
		m{"name": "OTEL_EXPORTER_OTLP_ENDPOINT", "valueFrom": m{"secretKeyRef": m{"key": "OTEL"}}},
		m{"name": "SETTINGS_ENCRYPTION_KEY", "valueFrom": m{"secretKeyRef": m{"name": "s", "key": "SETTINGS_ENCRYPTION_KEY"}}},
		m{"name": "GO_API_EDGE_JWT_SECRET", "valueFrom": m{"secretKeyRef": m{"name": "s", "key": "JWT_SECRET_KEY"}}},
		m{"name": "LLM_MODEL", "valueFrom": m{"configMapKeyRef": m{"key": "LLM_MODEL"}}},
	}
	for _, e := range extra {
		env = append(env, e)
	}
	return m{"ops": m{"queryApi": m{"extraEnv": env}}}
}

func runFlags(t *testing.T, v any, extra ...string) (result, string) {
	t.Helper()
	dir := t.TempDir()
	out := filepath.Join(dir, "flags.out")
	r := run(flagsTool, append([]string{writeFile(t, dir, "v.yaml", dump(t, v)), "--out", out}, extra...)...)
	text, _ := os.ReadFile(out)
	return r, string(text)
}

func TestQueryAPIFlagsRenderSortedFlagsAndRefs(t *testing.T) {
	r, text := runFlags(t, flagValues())
	wantCode(t, r, 0)
	want := "      GO_API_A_ENABLED: \"true\"\n      GO_API_B_ENABLED: \"true\"\n" +
		"      GO_API_EDGE_JWT_SECRET: ${JWT_SECRET_KEY}\n      SETTINGS_ENCRYPTION_KEY: ${SETTINGS_ENCRYPTION_KEY}\n"
	if text != want {
		t.Fatalf("rendered:\n%s\nwant:\n%s", text, want)
	}
	if r.stdout != "" {
		t.Fatalf("the tool printed to stdout: %q", r.stdout)
	}
	if !strings.HasPrefix(r.stderr, "# 2 flags, 2 secret refs -> ") || strings.Contains(r.stderr, "JWT") {
		t.Fatalf("stderr %q", r.stderr)
	}
	r, text = runFlags(t, flagValues(), "--format", "compose-env")
	wantCode(t, r, 0)
	if text != "GO_API_A_ENABLED=true\nGO_API_B_ENABLED=true\nGO_API_EDGE_JWT_SECRET=${JWT_SECRET_KEY}\nSETTINGS_ENCRYPTION_KEY=${SETTINGS_ENCRYPTION_KEY}\n" {
		t.Fatalf("compose-env:\n%s", text)
	}
}

// str(True) is "True": an unquoted YAML true is not the literal "true" the flag class needs.
func TestQueryAPIFlagsReadValuesTheWayPyYAMLLoadsThem(t *testing.T) {
	dir := t.TempDir()
	values := "ops:\n  queryApi:\n    extraEnv:\n" +
		"      - {name: GO_API_A_ENABLED, value: \"true\"}\n" +
		"      - {name: GO_API_B_ENABLED, value: true}\n" +
		"      - {name: GO_API_C_ENABLED, value: 'true'}\n" +
		"      - {name: GO_API_D_ENABLED, value: yes}\n" +
		"      - {name: S, valueFrom: {secretKeyRef: {key: K}}}\n" +
		"      - {name: T, valueFrom: {secretKeyRef: {key: ''}}}\n"
	out := filepath.Join(dir, "o")
	r := run(flagsTool, writeFile(t, dir, "v.yaml", []byte(values)), "--out", out)
	wantCode(t, r, 0)
	text, _ := os.ReadFile(out)
	if string(text) != "      GO_API_A_ENABLED: \"true\"\n      GO_API_C_ENABLED: \"true\"\n      S: ${K}\n" {
		t.Fatalf("rendered:\n%s", text)
	}
}

func TestQueryAPIFlagsRefuseAnEmptyClass(t *testing.T) {
	r, _ := runFlags(t, m{"ops": m{"queryApi": m{"extraEnv": l{m{"name": "S", "valueFrom": m{"secretKeyRef": m{"key": "K"}}}}}}})
	wantCode(t, r, 1)
	wantIn(t, r.stderr, "found ZERO GO_API_*_ENABLED=true entries")
	r, _ = runFlags(t, m{"ops": m{"queryApi": m{"extraEnv": l{m{"name": "GO_API_A_ENABLED", "value": "true"}}}}})
	wantCode(t, r, 1)
	wantIn(t, r.stderr, "found ZERO valueFrom.secretKeyRef entries")
	r, _ = runFlags(t, m{"ops": m{}})
	wantCode(t, r, 1)
	wantIn(t, r.stderr, "KeyError: 'queryApi'")
}

func TestQueryAPIFlagsNeedOut(t *testing.T) {
	r := run(flagsTool, writeFile(t, t.TempDir(), "v.yaml", dump(t, flagValues())))
	wantCode(t, r, 2)
	wantIn(t, r.stderr, "--out <path> is required")
	if r.stdout != "" {
		t.Fatalf("stdout %q", r.stdout)
	}
	wantCode(t, run(flagsTool), 2)
}

// ---- web env parity (D2736): names only, prod against the overlay ----

func prodWeb() m {
	extra := l{
		m{"name": "AUTH_URL", "value": "https://prod.example"},
		m{"name": "TRUST_PROXY", "value": "true"},
		m{"name": "TRUSTED_PROXY_HOPS", "value": "1"},
		m{"name": "ACR_API_ORIGIN", "value": "x"},
	}
	for _, n := range []string{"ACR_WEB_ASSERTION_ISSUER", "ACR_WEB_ASSERTION_AUDIENCE", "ACR_WEB_ASSERTION_KID", "ACR_WEB_ASSERTION_KEY_FILE",
		"AUTH_GITHUB_ID", "AUTH_GITHUB_SECRET", "AUTH_GOOGLE_ID", "AUTH_GOOGLE_SECRET"} {
		extra = append(extra, m{"name": n, "value": "x"})
	}
	return m{"ops": m{"web": m{"env": m{"BACKEND_URL": "http://ingress"}, "extraEnv": extra}}}
}

func trackedOverlay(t *testing.T) string {
	return filepath.Join(routingRepoRoot(t), "ci", "bigboy", "compose.bigboy.yml")
}

// overlayWith is the tracked compose file with one web.environment line replaced ("" drops it).
func overlayWith(t *testing.T, line, replacement string) string {
	t.Helper()
	raw, err := os.ReadFile(trackedOverlay(t))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), line) {
		t.Fatalf("the tracked overlay has no %q", line)
	}
	return writeFile(t, t.TempDir(), "o.yml", []byte(strings.Replace(string(raw), line, replacement, 1)))
}

func parity(t *testing.T, prod any, overlay string, extra ...string) result {
	t.Helper()
	return run(webEnv, append([]string{writeFile(t, t.TempDir(), "v.yaml", dump(t, prod)), overlay}, extra...)...)
}

func TestTrackedOverlayIsInParity(t *testing.T) {
	r := parity(t, prodWeb(), trackedOverlay(t))
	wantCode(t, r, 0)
	if r.stderr != "" || r.stdout != "" {
		t.Fatalf("output %q %q", r.stdout, r.stderr)
	}
}

func TestNewProdNameFailsNamed(t *testing.T) {
	for _, name := range []string{"NEW_WEB_KNOB", "TRUST_PROXY_NEW"} {
		v := prodWeb()
		web := v["ops"].(m)["web"].(m)
		web["extraEnv"] = append(web["extraEnv"].(l), m{"name": name, "value": "1"})
		r := parity(t, v, trackedOverlay(t))
		wantCode(t, r, 1)
		wantIn(t, r.stderr, "DRIFT: prod web env name "+name+" is not classified")
	}
}

func TestMissingOverlayNameFails(t *testing.T) {
	for _, line := range []string{"    - AUTH_URL=https://www.commanderkeen.dev\n", "    - TRUST_PROXY=true\n", "    - TRUSTED_PROXY_HOPS=1\n"} {
		name := strings.TrimPrefix(strings.SplitN(line, "=", 2)[0], "    - ")
		r := parity(t, prodWeb(), overlayWith(t, line, ""))
		wantCode(t, r, 1)
		wantIn(t, r.stderr, "DRIFT: overlay name "+name+" missing from compose.bigboy.yml web.environment")
	}
}

func TestWrongOverlayValueFails(t *testing.T) {
	for _, c := range [][3]string{
		{"    - BACKEND_URL=http://traefik:3000", "    - BACKEND_URL=http://api:8000", "BACKEND_URL"},
		{"    - TRUST_PROXY=true", "    - TRUST_PROXY=false", "TRUST_PROXY"},
		{"    - TRUSTED_PROXY_HOPS=1", "    - TRUSTED_PROXY_HOPS=2", "TRUSTED_PROXY_HOPS"},
	} {
		r := parity(t, prodWeb(), overlayWith(t, c[0], c[1]))
		wantCode(t, r, 1)
		wantIn(t, r.stderr, "DRIFT: overlay "+c[2]+" is not the expected public value")
	}
}

func TestStaleClassificationFails(t *testing.T) {
	v := prodWeb()
	web := v["ops"].(m)["web"].(m)
	var kept l
	for _, e := range web["extraEnv"].(l) {
		if e.(m)["name"] != "ACR_API_ORIGIN" {
			kept = append(kept, e)
		}
	}
	web["extraEnv"] = kept
	r := parity(t, v, trackedOverlay(t))
	wantCode(t, r, 1)
	wantIn(t, r.stderr, "DRIFT: classified name ACR_API_ORIGIN no longer exists in prod web env")
}

func TestLiveNamesMissingFails(t *testing.T) {
	names := writeFile(t, t.TempDir(), "names", []byte("BACKEND_URL\r\n  ACR_API_ORIGIN \n"))
	r := parity(t, prodWeb(), trackedOverlay(t), "--live-names", names)
	wantCode(t, r, 1)
	wantIn(t, r.stderr, "DRIFT: running web container has no env var named AUTH_URL\n")
	wantNotIn(t, r.stderr, "named BACKEND_URL")
	wantNotIn(t, r.stderr, "named ACR_API_ORIGIN")
	// Every line boundary of Python's splitlines() and every character its strip() drops.
	names = writeFile(t, t.TempDir(), "names", []byte("AUTH_URL\x1cBACKEND_URL\x1f\u2028TRUST_PROXY\n"))
	r = parity(t, prodWeb(), trackedOverlay(t), "--live-names", names)
	for _, name := range []string{"AUTH_URL", "BACKEND_URL", "TRUST_PROXY"} {
		wantNotIn(t, r.stderr, "named "+name+"\n")
	}
	wantIn(t, r.stderr, "named TRUSTED_PROXY_HOPS\n")
}

func TestEmptyLiveNamesFileIsNotAPass(t *testing.T) {
	empty := writeFile(t, t.TempDir(), "names", []byte("\n  \n"))
	r := parity(t, prodWeb(), trackedOverlay(t), "--live-names", empty)
	wantCode(t, r, 2)
	wantIn(t, r.stderr, "--live-names file is empty")
	wantCode(t, run(webEnv, "a", "b", "--live-names"), 2)
	wantCode(t, run(webEnv, "only-one"), 2)
}

func TestEmptyProdWebEnvIsNotAPass(t *testing.T) {
	r := parity(t, m{"ops": m{"web": m{}}}, trackedOverlay(t))
	wantCode(t, r, 1)
	wantIn(t, r.stderr, "prod web has ZERO env names -- refusing to pass on an empty set")
	r = parity(t, m{"ops": l{}}, trackedOverlay(t))
	wantCode(t, r, 1)
	wantIn(t, r.stderr, "values has no ops.web block (shape changed?)")
}

func TestSocialSignInNamesAreClassified(t *testing.T) {
	wantCode(t, parity(t, prodWeb(), trackedOverlay(t)), 0)
	v := prodWeb()
	web := v["ops"].(m)["web"].(m)
	var kept l
	for _, e := range web["extraEnv"].(l) {
		if !strings.HasPrefix(e.(m)["name"].(string), "AUTH_GOOGLE") {
			kept = append(kept, e)
		}
	}
	web["extraEnv"] = kept
	r := parity(t, v, trackedOverlay(t))
	wantIn(t, r.stderr, "classified name AUTH_GOOGLE_ID no longer exists")
	wantIn(t, r.stderr, "classified name AUTH_GOOGLE_SECRET no longer exists")
}

func TestQueryAPICarriesLLMModelExplain(t *testing.T) {
	raw, err := os.ReadFile(trackedOverlay(t))
	if err != nil {
		t.Fatal(err)
	}
	doc, err := routing.LoadYAML(raw, true)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range routing.Lookup(doc, "services", "query-api", "environment").([]any) {
		if item == "LLM_MODEL_EXPLAIN=${LLM_MODEL_EXPLAIN:-}" {
			return
		}
	}
	t.Fatal("query-api does not pass LLM_MODEL_EXPLAIN through with an empty default")
}

// HTTPProber's client sets CheckRedirect to return http.ErrUseLastResponse: a plane that answers a probe with a
// redirect gives the redirect status, and the redirect target sees no request.
func TestTheProberNeverFollowsARedirect(t *testing.T) {
	var followed sync.Mutex
	hits := 0
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		followed.Lock()
		hits++
		followed.Unlock()
		w.WriteHeader(405)
	}))
	t.Cleanup(other.Close)
	redirecting := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL+r.RequestURI, http.StatusFound)
	}))
	t.Cleanup(redirecting.Close)
	host, port := strings.Split(strings.TrimPrefix(redirecting.URL, "http://"), ":")[0], redirecting.Listener.Addr().(*net.TCPAddr).Port
	status, err := routing.OSDeps().Prober.Probe(host, port, "ROUTEPROBE", "/api/v1/known")
	if err != nil {
		t.Fatal(err)
	}
	followed.Lock()
	defer followed.Unlock()
	if status != http.StatusFound || hits != 0 {
		t.Fatalf("status %d, redirect target hit %d times: the prober followed the redirect (its client's CheckRedirect must return http.ErrUseLastResponse)", status, hits)
	}
}

// A plain timestamp secret key is written as Python's str() of the datetime (recorded: python3 + PyYAML 6.0.1,
// generate-query-api-enabled-flags.py at e557a21dc4).
func TestQueryAPIFlagsWriteATimestampKeyAsPythonDid(t *testing.T) {
	dir := t.TempDir()
	values := "ops:\n  queryApi:\n    extraEnv:\n      - {name: GO_API_A_ENABLED, value: \"true\"}\n" +
		"      - {name: T, valueFrom: {secretKeyRef: {key: 2002-1-1}}}\n" +
		"      - {name: U, valueFrom: {secretKeyRef: {key: 2001-12-14t21:59:43.10-05:00}}}\n"
	out := filepath.Join(dir, "o")
	r := run(flagsTool, writeFile(t, dir, "v.yaml", []byte(values)), "--out", out)
	wantCode(t, r, 0)
	text, _ := os.ReadFile(out)
	// 2002-1-1 has no time part, so PyYAML's resolver leaves it a string; the full timestamp is a datetime.
	if want := "      GO_API_A_ENABLED: \"true\"\n      T: ${2002-1-1}\n      U: ${2001-12-14 21:59:43.100000-05:00}\n"; string(text) != want {
		t.Fatalf("rendered:\n%s\nwant:\n%s", text, want)
	}
}

type portProber struct{ ports []int }

func (p *portProber) Probe(_ string, port int, _, _ string) (int, error) {
	p.ports = append(p.ports, port)
	return 405, nil
}

// The plane port is read as Python read it, `parts.port or 80`: no port and an explicit :0 both probe port 80.
func TestCoverageProbesThePortPythonDid(t *testing.T) {
	values := coverageValues(t)
	for url, want := range map[string]int{"http://127.0.0.1:0": 80, "http://localhost": 80, "http://127.0.0.1:8093": 8093, "http://LOCALHOST:00": 80} {
		p := &portProber{}
		var so, se bytes.Buffer
		code := routing.RunRouteCoverage([]string{values, "--go-api-url", url, "--query-api-url", url}, routing.Deps{Prober: p, Files: routing.OSFiles{}}, &so, &se)
		if code != 0 || len(p.ports) == 0 {
			t.Fatalf("%s: exit %d, %d probes: %s", url, code, len(p.ports), se.String())
		}
		for _, got := range p.ports {
			if got != want {
				t.Fatalf("%s probed port %d, want %d (Python: parts.port or 80)", url, got, want)
			}
		}
	}
}

type countingProber struct{ calls int }

func (p *countingProber) Probe(string, int, string, string) (int, error) { p.calls++; return 405, nil }

// A routed path Python's http.client could not send (UnicodeEncodeError on a non-ASCII character, InvalidURL on a
// control or space character) is refused before any probe: exit 1, one stderr line naming the path, nothing on stdout.
func TestCoverageRefusesAPathPythonCouldNotSendBeforeAnyProbe(t *testing.T) {
	for _, c := range []struct{ goPath, named string }{
		{"/café/{id}", "'/café/routeprobe'"},
		{"/n\u00a0b/{x}", `'/n\xa0b/routeprobe'`},
		{"/a b", "'/a b'"},
		{"/tab\there", `'/tab\there'`},
	} {
		v := m{"ingress": m{
			"goApiPaths":    l{m{"path": "/api/v1/known"}, m{"path": c.goPath}},
			"queryApiPaths": l{m{"path": "/api/v1/q", "pathType": "Exact"}},
		}}
		p := &countingProber{}
		var so, se bytes.Buffer
		code := routing.RunRouteCoverage([]string{writeFile(t, t.TempDir(), "v.yaml", dump(t, v))}, routing.Deps{Prober: p, Files: routing.OSFiles{}}, &so, &se)
		want := "check-route-coverage: routed path " + c.named + " cannot be sent as a request target (a non-ASCII, control or space character): refusing before any probe\n"
		if code != 1 || so.String() != "" || se.String() != want || p.calls != 0 {
			t.Errorf("%q: exit %d stdout %q stderr %q probes %d, want exit 1, no stdout, stderr %q, 0 probes", c.goPath, code, so.String(), se.String(), p.calls, want)
		}
	}
}
