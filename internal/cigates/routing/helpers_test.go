package routing_test

import (
	"bytes"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/full-chaos/dev-health-ops/internal/cigates/routing"
)

func routingRepoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above the test directory")
		}
		dir = parent
	}
}

type m = map[string]any
type l = []any

// baseValues is the values the Python suite started from (VALUES).
func baseValues() m {
	return m{"ingress": m{
		"goApiPaths": l{
			m{"path": "/api/v1/external-ingest/schemas"},
			m{"path": "/api/v1/external-ingest/schemas/{schema_id}"},
			m{"path": "/health"},
		},
		"queryApiPaths": l{
			m{"path": "/api/v1/work-units", "pathType": "Exact"},
			m{"path": "/api/v1/people/[^/]+/metric", "pathType": "ImplementationSpecific"},
		},
	}}
}

type pt struct{ path, pathType string }

type spec struct {
	goPaths []string
	query   []pt
	allow   []pt // nil: no shared list
	hosts   l    // nil: no hosts key
	noAllow bool
}

func entries(ps []pt) l {
	out := l{}
	for _, p := range ps {
		out = append(out, m{"path": p.path, "pathType": p.pathType})
	}
	return out
}

// valuesOf is the Python suite's _values(): baseValues plus extra Go plane paths, a shared allow-list and hosts.
func valuesOf(s spec) m {
	doc := baseValues()
	ing := doc["ingress"].(m)
	for _, p := range s.goPaths {
		ing["goApiPaths"] = append(ing["goApiPaths"].(l), m{"path": p})
	}
	ing["queryApiPaths"] = append(ing["queryApiPaths"].(l), entries(s.query)...)
	ops := m{}
	if s.allow != nil {
		ops["pythonAllowList"] = entries(s.allow)
	}
	if s.hosts != nil {
		ops["hosts"] = s.hosts
	}
	if len(ops) > 0 {
		doc["ops"] = m{"ingress": ops}
	}
	return doc
}

func dump(t *testing.T, v any) []byte {
	t.Helper()
	out, err := yaml.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// load is the values as the tools read them: dumped to YAML and loaded back the PyYAML way.
func load(t *testing.T, v any) any {
	t.Helper()
	doc, err := routing.LoadYAML(dump(t, v), false)
	if err != nil {
		t.Fatal(err)
	}
	return doc
}

func writeFile(t *testing.T, dir, name string, data []byte) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

type result struct {
	code           int
	stdout, stderr string
}

func run(f func([]string, routing.Deps, *bytes.Buffer, *bytes.Buffer) int, args ...string) result {
	var so, se bytes.Buffer
	code := f(args, routing.OSDeps(), &so, &se)
	return result{code, so.String(), se.String()}
}

func gen(args []string, d routing.Deps, so, se *bytes.Buffer) int {
	return routing.RunPlaneSplitRouter(args, d, so, se)
}

func flagsTool(args []string, d routing.Deps, so, se *bytes.Buffer) int {
	return routing.RunQueryAPIFlags(args, d, so, se)
}

func coverage(args []string, d routing.Deps, so, se *bytes.Buffer) int {
	return routing.RunRouteCoverage(args, d, so, se)
}

func webEnv(args []string, d routing.Deps, so, se *bytes.Buffer) int {
	return routing.RunWebEnvParity(args, d, so, se)
}

// runValues writes the values and runs the generator on them with --format dynamic (the Python suite's _run).
func runValues(t *testing.T, v any) result {
	t.Helper()
	return run(gen, writeFile(t, t.TempDir(), "values.prod.yaml", dump(t, v)), "--format", "dynamic")
}

// emit is the dynamic router file for the values, loaded (the Python suite's _emit).
func emit(t *testing.T, v any) any {
	t.Helper()
	doc := load(t, v)
	var goPaths, queryPaths []string
	if msg := routing.Raised(func() { goPaths, queryPaths = routing.Paths(doc) }); msg != "" {
		t.Fatal(msg)
	}
	text := routing.EmitDynamic(routing.CombinedRegex(goPaths), routing.CombinedRegex(queryPaths), routing.PythonAllow(doc))
	return loadText(t, text)
}

func loadText(t *testing.T, text string) any {
	t.Helper()
	doc, err := routing.LoadYAML([]byte(text), false)
	if err != nil {
		t.Fatalf("the emitted router does not load: %v\n%s", err, text)
	}
	return doc
}

type router struct {
	name, rule, service string
	priority            int
}

func routers(t *testing.T, routed any) []router {
	t.Helper()
	rs := routing.Lookup(routed, "http", "routers")
	var out []router
	for _, k := range routing.MapKeys(rs) {
		r := routing.Lookup(rs, k)
		var prio int
		if p := routing.Lookup(r, "priority"); p != nil {
			prio = atoi(t, routing.PyStr(p))
		}
		out = append(out, router{routing.PyStr(k), routing.PyStr(routing.Lookup(r, "rule")), routing.PyStr(routing.Lookup(r, "service")), prio})
	}
	return out
}

func atoi(t *testing.T, s string) int {
	t.Helper()
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			t.Fatalf("not a priority: %q", s)
		}
		n = n*10 + int(c-'0')
	}
	return n
}

var termRe = regexp.MustCompile("(PathRegexp|PathPrefix|Path)\\(`([^`]+)`\\)")

// route resolves path on Host(`traefik`) the way traefik does: the highest-priority matching router wins (the
// first of equal ones). It understands only the rule forms the generator emits.
func route(t *testing.T, routed any, path string) string {
	t.Helper()
	var best *router
	for _, r := range routers(t, routed) {
		terms := termRe.FindAllStringSubmatch(r.rule, -1)
		ok := len(terms) == 0
		for _, term := range terms {
			switch term[1] {
			case "PathRegexp":
				ok = ok || regexp.MustCompile(term[2]).MatchString(path)
			case "Path":
				ok = ok || path == term[2]
			case "PathPrefix":
				ok = ok || strings.HasPrefix(path, term[2])
			}
		}
		if ok && (best == nil || r.priority > best.priority) {
			r := r
			best = &r
		}
	}
	if best == nil {
		t.Fatalf("no router matches %s", path)
	}
	servers := routing.Lookup(routed, "http", "services", best.service, "loadBalancer", "servers").([]any)
	return routing.PyStr(routing.Lookup(servers[0], "url"))
}

var pathRegexpRe = regexp.MustCompile("PathRegexp\\(`([^`]+)`\\)")
var literalTermRe = regexp.MustCompile("(Path|PathPrefix)\\(`([^`]+)`\\)")

// planesMatching is the routers whose own path rule matches path (the catch-all is not a path rule).
func planesMatching(t *testing.T, routed any, path string) map[string]bool {
	t.Helper()
	hit := map[string]bool{}
	for _, r := range routers(t, routed) {
		if mm := pathRegexpRe.FindStringSubmatch(r.rule); mm != nil && !strings.Contains(r.rule, "Path(`") {
			if regexp.MustCompile(mm[1]).MatchString(path) {
				hit[r.name] = true
			}
			continue
		}
		if !strings.Contains(r.rule, "&&") {
			continue
		}
		for _, term := range termRe.FindAllStringSubmatch(r.rule, -1) {
			if (term[1] == "Path" && path == term[2]) || (term[1] == "PathPrefix" && strings.HasPrefix(path, term[2])) ||
				(term[1] == "PathRegexp" && regexp.MustCompile(term[2]).MatchString(path)) {
				hit[r.name] = true
			}
		}
	}
	return hit
}

func wantCode(t *testing.T, r result, code int) {
	t.Helper()
	if r.code != code {
		t.Fatalf("exit %d, want %d\nstdout:\n%s\nstderr:\n%s", r.code, code, r.stdout, r.stderr)
	}
}

func wantIn(t *testing.T, text, want string) {
	t.Helper()
	if !strings.Contains(text, want) {
		t.Fatalf("missing %q in:\n%s", want, text)
	}
}

func wantNotIn(t *testing.T, text, unwanted string) {
	t.Helper()
	if strings.Contains(text, unwanted) {
		t.Fatalf("unexpected %q in:\n%s", unwanted, text)
	}
}
