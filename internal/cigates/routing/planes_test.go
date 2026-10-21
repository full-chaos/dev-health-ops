package routing_test

import (
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cigates/routing"
)

// CHAOS-6987 (D2735): bigboy's plane-split router rules match Host(`traefik`) ONLY. Traefik on bigboy has one
// entrypoint shared by browser traffic and web's server-side BACKEND_URL traffic, so the Host() clause is the only thing
// that keeps a browser request on web (whose proxy.ts turns the session cookie into a bearer).

var (
	hostCallRe  = regexp.MustCompile("\\b(Host|HostRegexp|HostSNI|HostSNIRegexp|ClientIP|Header|HeaderRegexp)\\(`([^`]*)`\\)")
	pathCallRe  = regexp.MustCompile("PathRegexp\\(`[^`]*`\\)")
	pathGroupRe = regexp.MustCompile("\\((?:Path|PathPrefix)\\(`[^`]+`\\)(?: \\|\\| (?:Path|PathPrefix)\\(`[^`]+`\\))*\\)")
)

// internalOnly is "" when the rule is Host(`traefik`), optionally ANDed with a path regex or a group of Path terms.
func internalOnly(name, rule string) string {
	matchers := hostCallRe.FindAllStringSubmatch(rule, -1)
	if len(matchers) == 0 {
		return name + ": rule has no Host() clause at all: " + rule
	}
	if len(matchers) != 1 || matchers[0][1] != "Host" || matchers[0][2] != "traefik" {
		return name + ": rule must name Host(`traefik`) only"
	}
	skeleton := strings.TrimSpace(pathCallRe.ReplaceAllString(rule, "PathRegexp(P)"))
	skeleton = pathGroupRe.ReplaceAllString(skeleton, "(PATHS)")
	switch skeleton {
	case "Host(`traefik`)", "Host(`traefik`) && PathRegexp(P)", "Host(`traefik`) && (PATHS)":
		return ""
	}
	return name + ": rule can match without the internal Host clause: " + skeleton
}

func TestDynamicConfigRulesAreInternalOnly(t *testing.T) {
	routed := emit(t, baseValues())
	var names []string
	for _, r := range routers(t, routed) {
		names = append(names, r.name)
		if msg := internalOnly(r.name, r.rule); msg != "" {
			t.Error(msg)
		}
		if ep := routing.PyStr(routing.Lookup(routed, "http", "routers", r.name, "entryPoints")); ep != "['web']" {
			t.Errorf("%s entryPoints %s", r.name, ep)
		}
	}
	sort.Strings(names)
	if want := []string{"api-internal-catchall", "go-api-paths", "query-api-paths"}; !reflect.DeepEqual(names, want) {
		t.Fatalf("routers %v, want %v", names, want)
	}
}

func TestLabelRulesAreInternalOnly(t *testing.T) {
	goPaths, queryPaths := routing.Paths(load(t, baseValues()))
	text := routing.EmitLabels(routing.CombinedRegex(goPaths), routing.CombinedRegex(queryPaths))
	rules := regexp.MustCompile(`(?m)traefik\.http\.routers\.([\w-]+)\.rule: "(.*)"$`).FindAllStringSubmatch(text, -1)
	var names []string
	for _, r := range rules {
		names = append(names, r[1])
		if msg := internalOnly(r[1], r[2]); msg != "" {
			t.Error(msg)
		}
	}
	if !reflect.DeepEqual(names, []string{"go-api-paths", "query-api-paths"}) {
		t.Fatalf("label routers %v", names)
	}
}

func TestCheckedInExampleIsInternalOnly(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(routingRepoRoot(t), "ci", "bigboy", "traefik-dynamic-planes.yml.example"))
	if err != nil {
		t.Fatal(err)
	}
	rs := routers(t, loadText(t, string(raw)))
	if len(rs) == 0 {
		t.Fatal("example has no routers")
	}
	for _, r := range rs {
		if msg := internalOnly(r.name, r.rule); msg != "" {
			t.Error(msg)
		}
	}
}

func TestCheckerRejectsPublicOrHostlessRules(t *testing.T) {
	for _, bad := range []string{
		"Host(`www.commanderkeen.dev`) || Host(`traefik`) && PathRegexp(`^(/a)$`)",
		"Host(`commanderkeen.dev`) && PathRegexp(`^(/a)$`)",
		"PathRegexp(`^(/a)$`)",
		"Host(`traefik`) || PathRegexp(`^(/a)$`)",
		"HostRegexp(`.+`) && PathRegexp(`^(/a)$`)",
	} {
		if internalOnly("planted", bad) == "" {
			t.Errorf("the checker passed %s", bad)
		}
	}
}

func TestGeneratedPathsCoverTheValues(t *testing.T) {
	goPaths, queryPaths := routing.Paths(load(t, baseValues()))
	goRe, queryRe := regexp.MustCompile(routing.CombinedRegex(goPaths)), regexp.MustCompile(routing.CombinedRegex(queryPaths))
	for path, want := range map[string]bool{"/api/v1/external-ingest/schemas/abc": true, "/api/v1/external-ingest/schemas/abc/def": false} {
		if goRe.MatchString(path) != want {
			t.Errorf("go-api %s: %v", path, !want)
		}
	}
	for path, want := range map[string]bool{"/api/v1/work-units": true, "/api/v1/work-units/extra": false, "/api/v1/people/p1/metric": true} {
		if queryRe.MatchString(path) != want {
			t.Errorf("query-api %s: %v", path, !want)
		}
	}
}

const (
	goAPI    = "http://go-api:8000"
	queryAPI = "http://query-api:8090"
	python   = "http://api:8000"
)

func wantRoutes(t *testing.T, routed any, want map[string]string) {
	t.Helper()
	for path, backend := range want {
		if got := route(t, routed, path); got != backend {
			t.Errorf("%s routes to %s, want %s", path, got, backend)
		}
	}
}

// CHAOS-7047, CHAOS-6263, CHAOS-7255, CHAOS-7198: an unknown path reaches the Go api; nothing is Python's by default.
func TestDefaultBackendIsGoAndAllowListStaysPython(t *testing.T) {
	wantRoutes(t, emit(t, baseValues()), map[string]string{
		"/no/such/path": goAPI, "/graphqlx": goAPI, "/api/v1/work-units": queryAPI, "/health": goAPI, "/docs": goAPI,
		"/openapi.json": goAPI, "/graphql": goAPI, "/api/v1/internal/acr/health": goAPI,
		"/api/v1/admin/llm-settings/readiness": goAPI,
	})
	if got := routing.PythonAllow(load(t, baseValues())); len(got) != 0 {
		t.Fatalf("the default allow-list is %v, want empty", got)
	}
}

func TestAllowListOverrideNeedsNoInternalCover(t *testing.T) {
	for _, list := range [][]pt{
		{{"/graphql", "Prefix"}},
		{{"/graphql$", "ImplementationSpecific"}},
		{{"/graphql$", "ImplementationSpecific"}, {"/api/v1/internal", "Prefix"}},
	} {
		doc := load(t, m{"ops": m{"ingress": m{"pythonAllowList": entries(list)}}})
		var want [][2]any
		for _, p := range list {
			want = append(want, [2]any{p.path, p.pathType})
		}
		if got := routing.PythonAllow(doc); !reflect.DeepEqual(got, want) {
			t.Errorf("%v: %v", list, got)
		}
	}
}

func TestAnchoredAllowListEntryEmitsOneExactPathTerm(t *testing.T) {
	rule := routing.TraefikPathRule([][2]any{{"/graphql$", "ImplementationSpecific"}, {`/openapi\.json$`, "ImplementationSpecific"}, {"/api/v1/internal", "Prefix"}})
	wantIn(t, rule, "PathRegexp(`(?i)^/graphql$`)")
	wantNotIn(t, rule, "PathPrefix(`/graphql/`)")
	wantIn(t, rule, "PathRegexp(`(?i)^/openapi[.]json$`)")
	wantNotIn(t, rule, `\`)
	wantIn(t, rule, "PathPrefix(`/api/v1/internal/`)")
}

func graphqlValues(allow []m) m {
	doc := baseValues()
	ing := doc["ingress"].(m)
	ing["queryApiPaths"] = append(ing["queryApiPaths"].(l), m{"path": "/graphql", "pathType": "Exact"})
	if allow != nil {
		list := l{}
		for _, e := range allow {
			list = append(list, e)
		}
		doc["ops"] = m{"ingress": m{"pythonAllowList": list}}
	}
	return doc
}

func TestGraphqlInQueryAPIPathsRoutesToQueryAPI(t *testing.T) {
	v := graphqlValues(nil)
	if got := routing.PathsOnTwoPlanes(load(t, v)); len(got) != 0 {
		t.Fatal(got)
	}
	wantRoutes(t, emit(t, v), map[string]string{"/graphql": queryAPI, "/graphqlx": goAPI, "/graphql/": goAPI, "/graphql/x": goAPI})
}

func TestAPathOnTheAllowListAndAGoPlaneIsRefused(t *testing.T) {
	for _, allow := range [][]m{
		{{"path": "/graphql$", "pathType": "ImplementationSpecific"}},
		{{"path": "/graphql", "pathType": "Exact"}},
		{{"path": "/graphql", "pathType": "Prefix"}},
		{{"path": "/api/v1/internal", "pathType": "Prefix"}, {"path": "/graphql$", "pathType": "ImplementationSpecific"}},
	} {
		r := runValues(t, graphqlValues(allow))
		wantCode(t, r, 4)
		if r.stdout != "" {
			t.Fatalf("stdout %q", r.stdout)
		}
		wantIn(t, r.stderr, "REFUSED: one path, two backends")
		wantIn(t, r.stderr, "ops.ingress.pythonAllowList entry {path: /graphql")
		wantIn(t, r.stderr, "and ingress.queryApiPaths entry /graphql")
	}
}

func TestAnAllowListWithoutTheMovedPathIsAccepted(t *testing.T) {
	wantCode(t, runValues(t, graphqlValues([]m{{"path": "/metrics$", "pathType": "ImplementationSpecific"}})), 0)
}

type overlapCase struct {
	s         spec
	bothMatch string // "" when only prod's ingress would see the overlap
}

func hostsWithInClusterGraphql(inCluster string) l {
	return l{
		m{"host": "public.example", "pythonAllowList": true},
		m{"host": "in-cluster.example", "pythonAllowList": l{m{"path": inCluster, "pathType": "ImplementationSpecific"}}},
	}
}

var overlaps = map[string]overlapCase{
	"a Prefix over a Go path under it":                  {spec{query: []pt{{"/graphql/sub", "Exact"}}, allow: []pt{{"/graphql", "Prefix"}}}, "/graphql/sub"},
	"a Prefix over a deeper Go path":                    {spec{goPaths: []string{"/graphql/a/b"}, allow: []pt{{"/graphql", "Prefix"}}}, "/graphql/a/b"},
	"a Prefix over a Go path with a wildcard under it":  {spec{allow: []pt{{"/api/v1/people", "Prefix"}}}, "/api/v1/people/p1/metric"},
	"a Prefix that ends inside a wildcard segment":      {spec{allow: []pt{{"/api/v1/people/p1", "Prefix"}}}, "/api/v1/people/p1/metric"},
	"a Prefix with a trailing slash":                    {spec{query: []pt{{"/graphql/sub", "Exact"}}, allow: []pt{{"/graphql/", "Prefix"}}}, "/graphql/sub"},
	"a Prefix over the same path":                       {spec{query: []pt{{"/graphql", "Exact"}}, allow: []pt{{"/graphql", "Prefix"}}}, "/graphql"},
	"a Prefix read as starts-with (regex mode in prod)": {spec{query: []pt{{"/graphqlx", "Exact"}}, allow: []pt{{"/graphql", "Prefix"}}}, ""},
	"an Exact entry inside a wildcard path":             {spec{allow: []pt{{"/api/v1/people/p1/metric", "Exact"}}}, "/api/v1/people/p1/metric"},
	"an anchored entry inside a {param} path": {spec{allow: []pt{{"/api/v1/external-ingest/schemas/s1$", "ImplementationSpecific"}}},
		"/api/v1/external-ingest/schemas/s1"},
	"a host's own list, with the shared list clean": {spec{query: []pt{{"/graphql", "Exact"}}, allow: []pt{}, hosts: hostsWithInClusterGraphql("/graphql$")}, ""},
}

// The guard decides on the paths the rules MATCH, not the text of an entry. Each row is refused (exit 4, nothing
// emitted), and where bigboy's own rules see it, the two generated rules really match one request.
func TestEveryOverlapOfAPythonPathAndAGoPlanePathIsRefused(t *testing.T) {
	for name, c := range overlaps {
		t.Run(name, func(t *testing.T) {
			v := valuesOf(c.s)
			r := runValues(t, v)
			wantCode(t, r, 4)
			if r.stdout != "" {
				t.Fatalf("stdout %q", r.stdout)
			}
			wantIn(t, r.stderr, "REFUSED: one path, two backends")
			if c.bothMatch == "" {
				return
			}
			planes := planesMatching(t, emit(t, v), c.bothMatch)
			if !planes["python-allowlist"] || !(planes["go-api-paths"] || planes["query-api-paths"]) {
				t.Fatalf("the generated rules do not both match %s: %v", c.bothMatch, planes)
			}
		})
	}
}

func TestARefusalNamesTheListTheEntryAndTheGoPlanePath(t *testing.T) {
	r := runValues(t, valuesOf(overlaps["a host's own list, with the shared list clean"].s))
	wantCode(t, r, 4)
	wantIn(t, r.stderr, "ops.ingress.hosts[in-cluster.example].pythonAllowList entry {path: /graphql$, pathType: ImplementationSpecific} and ingress.queryApiPaths entry /graphql")
	wantNotIn(t, r.stderr, "public.example")
	wantNotIn(t, r.stderr, "ops.ingress.pythonAllowList entry")
	r = runValues(t, valuesOf(overlaps["a Prefix over a deeper Go path"].s))
	wantCode(t, r, 4)
	wantIn(t, r.stderr, "ops.ingress.pythonAllowList entry {path: /graphql, pathType: Prefix} and ingress.goApiPaths entry /graphql/a/b")
}

var disjoint = map[string]spec{
	"a Prefix beside an unrelated Go path":                         {allow: []pt{{"/api/v1/internal", "Prefix"}}},
	"a Prefix longer than the Go path":                             {query: []pt{{"/graphql", "Exact"}}, allow: []pt{{"/graphql/sub", "Prefix"}}},
	"a Prefix whose last segment a Go literal does not start with": {query: []pt{{"/graphq", "Exact"}}, allow: []pt{{"/graphql", "Prefix"}}},
	"a Prefix that differs in an earlier segment":                  {allow: []pt{{"/api/v2/work", "Prefix"}}},
	"an Exact entry longer than a Go literal it starts with":       {query: []pt{{"/graphq", "Exact"}}, allow: []pt{{"/graphql", "Exact"}}},
	"an Exact entry one segment short of a wildcard path":          {allow: []pt{{"/api/v1/people/p1", "Exact"}}},
	"an Exact entry one segment past a Go path":                    {allow: []pt{{"/api/v1/work-units/extra", "Exact"}}},
	"an anchored entry beside a look-alike Go path":                {query: []pt{{"/graphqlx", "Exact"}}, allow: []pt{{"/graphql$", "ImplementationSpecific"}}},
	"a wildcard is not an empty segment":                           {allow: []pt{{"/api/v1/people//metric", "Exact"}}},
	"the flip done on every list": {query: []pt{{"/graphql", "Exact"}}, allow: []pt{{"/api/v1/internal", "Prefix"}},
		hosts: hostsWithInClusterGraphql("/metrics$")},
}

var samplePaths = []string{
	"/graphql", "/graphql/", "/graphql/sub", "/graphqlx", "/graphq", "/metrics", "/docs", "/docs/intro", "/redoc",
	"/openapi.json", "/health", "/api/v1/internal", "/api/v1/internal/acr/health", "/api/v1/work-units",
	"/api/v1/work-units/extra", "/api/v1/people/p1", "/api/v1/people/p1/metric", "/api/v1/people//metric",
	"/api/v1/external-ingest/schemas", "/api/v1/external-ingest/schemas/s1",
}

func TestDisjointPythonAndGoPathsAreAcceptedAndNoRequestMatchesBoth(t *testing.T) {
	for name, s := range disjoint {
		t.Run(name, func(t *testing.T) {
			v := valuesOf(s)
			if got := routing.PathsOnTwoPlanes(load(t, v)); len(got) != 0 {
				t.Fatal(got)
			}
			wantCode(t, runValues(t, v), 0)
			routed := emit(t, v)
			for _, path := range samplePaths {
				planes := planesMatching(t, routed, path)
				if planes["python-allowlist"] && (planes["go-api-paths"] || planes["query-api-paths"]) {
					t.Errorf("%s matches the Python rule and a Go plane rule: %v", path, planes)
				}
			}
		})
	}
}

// The check and the emitted PathRegexp are derived from one form of a Go plane path: for every Go plane path and every
// sample request, "an Exact Python entry for this request overlaps" is exactly "the emitted regex matches it".
func TestTheCheckAndTheEmittedRegexAgreeOnEverySamplePath(t *testing.T) {
	doc := load(t, valuesOf(spec{goPaths: []string{"/docs/{page}", "/a.b/{x}-{y}/c"}, query: []pt{{"/metrics", "Exact"}}}))
	claimed := routing.GoPlaneSegments(doc)
	goPaths, queryPaths := routing.Paths(doc)
	var regexes []string
	for _, s := range claimed {
		regexes = append(regexes, routing.SegmentsRegex(s))
	}
	if !reflect.DeepEqual(regexes, append(goPaths, queryPaths...)) {
		t.Fatalf("claimed %v, emitted %v", regexes, append(goPaths, queryPaths...))
	}
	compared := 0
	extra := []string{"/a.b/1-2/c", "/aXb/1-2/c", "/a.b/1-/c", "/a.b/12/c"}
	for _, s := range claimed {
		emitted := regexp.MustCompile(routing.CombinedRegex([]string{routing.SegmentsRegex(s)}))
		for _, path := range append(append([]string{}, samplePaths...), extra...) {
			if routing.PythonEntryOverlaps(path, "Exact", s) != emitted.MatchString(path) {
				t.Errorf("%v %s: the check and the emitted regex disagree", s, path)
			}
			compared++
		}
	}
	if compared != len(claimed)*(len(samplePaths)+4) || compared <= 100 {
		t.Fatalf("compared %d", compared)
	}
}

func TestTheCheckReadsTheEntriesThePythonRuleIsGeneratedFrom(t *testing.T) {
	v := valuesOf(spec{allow: []pt{{"/api/v1/internal", "Prefix"}}, hosts: l{
		m{"host": "h1", "pythonAllowList": true},
		m{"host": "h2"},
		m{"host": "h3", "pythonAllowList": l{m{"path": "/x", "pathType": "Exact"}}},
	}})
	doc := load(t, v)
	sources := routing.PythonSources(doc)
	var names []string
	for _, s := range sources {
		names = append(names, s.Name)
	}
	if want := []string{"ops.ingress.pythonAllowList", "ops.ingress.hosts[h3].pythonAllowList", routing.BigboyLocalSource}; !reflect.DeepEqual(names, want) {
		t.Fatalf("sources %v", names)
	}
	allow := routing.PythonAllow(doc)
	if !reflect.DeepEqual(sources[0].Entries, allow) || len(sources[2].Entries) != 0 {
		t.Fatalf("the shared source %v is not the routed list %v", sources[0].Entries, allow)
	}
	if !reflect.DeepEqual(sources[1].Entries, [][2]any{{"/x", "Exact"}}) {
		t.Fatalf("h3 %v", sources[1].Entries)
	}
	goPaths, queryPaths := routing.Paths(doc)
	emitted := routing.EmitDynamic(routing.CombinedRegex(goPaths), routing.CombinedRegex(queryPaths), allow)
	wantIn(t, emitted, `rule: "`+routing.HostRule+" && "+routing.TraefikPathRule(allow)+`"`)
	if got := routing.PythonSources(load(t, baseValues()))[0].Entries; len(got) != 0 {
		t.Fatalf("with no shared list the default (empty) list is read, got %v", got)
	}
}

func TestAnEntryOfAShapeTheChartDoesNotAcceptIsRefused(t *testing.T) {
	for _, e := range []pt{
		{"/graphql", "ImplementationSpecific"},
		{"/graphql.*$", "ImplementationSpecific"},
		{"/graphql$$", "ImplementationSpecific"},
		{"/api/v1/[^/]+$", "ImplementationSpecific"},
		{"/graphql", "Regex"},
	} {
		r := runValues(t, valuesOf(spec{allow: []pt{e}}))
		wantCode(t, r, 4)
		wantIn(t, r.stderr, "{path: "+e.path+", pathType: "+e.pathType+"} is not a shape the ops chart accepts")
	}
}

func TestADotIsALiteralDotOnBothSides(t *testing.T) {
	dotted := routing.QueryAPIEntrySegments("/openapi.json", "Exact")
	if !routing.PythonEntryOverlaps(`/openapi\.json$`, "ImplementationSpecific", dotted) || !routing.PythonEntryOverlaps("/openapi.json", "Exact", dotted) {
		t.Fatal("a dot does not match a dot")
	}
	if routing.PythonEntryOverlaps("/openapiXjson", "Exact", dotted) ||
		routing.PythonEntryOverlaps(`/openapi\.json$`, "ImplementationSpecific", routing.QueryAPIEntrySegments("/openapiXjson", "Exact")) {
		t.Fatal("a dot matches any character")
	}
}

func TestAnExactGoPlanePathIsLiteralEvenWhereItLooksLikeAWildcard(t *testing.T) {
	exact := regexp.MustCompile("^(?:" + routing.QueryAPIEntryRegex("/lit/[^/]+", "Exact") + ")$")
	if !exact.MatchString("/lit/[^/]+") || exact.MatchString("/lit/x") {
		t.Fatal("an Exact path is not its own text")
	}
	segs := routing.QueryAPIEntrySegments("/lit/[^/]+", "Exact")
	if !routing.PythonEntryOverlaps("/lit/[^/]+", "Exact", segs) || routing.PythonEntryOverlaps("/lit/x", "Exact", segs) {
		t.Fatal("the check reads an Exact path as a wildcard")
	}
	if !routing.PythonEntryOverlaps("/lit/x", "Exact", routing.QueryAPIEntrySegments("/lit/[^/]+", "ImplementationSpecific")) {
		t.Fatal("an ImplementationSpecific wildcard does not match")
	}
}

func TestAPathWhoseRuleCannotBeWrittenIsRefusedNotEmitted(t *testing.T) {
	for _, format := range []string{"dynamic", "labels"} {
		for _, c := range []struct {
			s     spec
			named string
		}{
			{spec{goPaths: []string{"/.well-known/x"}}, "ingress.goApiPaths entry /.well-known/x"},
			{spec{query: []pt{{"/openapi.json", "Exact"}}}, "ingress.queryApiPaths entry /openapi.json"},
			{spec{query: []pt{{"/a+b/[^/]+", "ImplementationSpecific"}}}, "ingress.queryApiPaths entry /a+b/[^/]+"},
		} {
			r := run(gen, writeFile(t, t.TempDir(), "values.prod.yaml", dump(t, valuesOf(c.s))), "--format", format)
			wantCode(t, r, 5)
			if r.stdout != "" {
				t.Fatalf("stdout %q", r.stdout)
			}
			wantIn(t, r.stderr, "REFUSED: no valid rule can be emitted for "+c.named)
		}
	}
}

func TestEveryEmittedRouterFileLoads(t *testing.T) {
	r := runValues(t, valuesOf(spec{goPaths: []string{"/a-b/{x}_y"}, query: []pt{{"/c-d", "Exact"}}}))
	wantCode(t, r, 0)
	wantRoutes(t, loadText(t, r.stdout), map[string]string{"/a-b/1_y": goAPI, "/c-d": queryAPI})
}

// ---- an allow-list entry that names its backend (`service: query-api`) ----

func graphqlEntry(service any) m {
	e := m{"path": "/graphql$", "pathType": "ImplementationSpecific"}
	if service != "" {
		e["service"] = service
	}
	return e
}

var metricsEntry = m{"path": "/metrics$", "pathType": "ImplementationSpecific"}

// namedBackendValues is baseValues with a shared list and, when own is not nil, a second host with its own list.
func namedBackendValues(shared, own l) m {
	doc := baseValues()
	hosts := l{m{"host": "api.example", "pythonAllowList": true}}
	if own != nil {
		hosts = append(hosts, m{"host": "in-cluster.example", "pythonAllowList": own})
	}
	doc["ops"] = m{"ingress": m{"pythonAllowList": shared, "hosts": hosts}}
	return doc
}

func TestAnEntryThatNamesQueryAPIIsRoutedToQueryAPI(t *testing.T) {
	v := namedBackendValues(l{graphqlEntry("query-api")}, l{graphqlEntry("query-api"), metricsEntry})
	doc := load(t, v)
	if a, b, c := routing.PathsOnTwoPlanes(doc), routing.QueryAPIPathsThatDifferByHost(doc), routing.PathsInTwoObjects(doc); len(a)+len(b)+len(c) != 0 {
		t.Fatal(a, b, c)
	}
	wantCode(t, runValues(t, v), 0)
	routed := emit(t, v)
	wantRoutes(t, routed, map[string]string{"/graphql": queryAPI, "/graphqlx": goAPI, "/graphql/": goAPI, "/graphql/x": goAPI})
	for _, r := range routers(t, routed) {
		if r.name == "python-allowlist" {
			t.Fatal("a Python router is emitted with no Python entry")
		}
	}
}

func TestAnEntryWithServiceAPIOrNoKeyStaysOnPython(t *testing.T) {
	for _, e := range []m{graphqlEntry(""), graphqlEntry("api")} {
		wantRoutes(t, emit(t, namedBackendValues(l{e}, l{e, metricsEntry})), map[string]string{"/graphql": python})
	}
}

// CHAOS-7925: both ways of sending /graphql to query-api route it and its lookalikes the same way, except case: an
// anchored entry is case-insensitive (the chart's regex mode), a table path is an exact string.
func TestTheNamedBackendGivesTheRouterOfAQueryAPIPathsEntry(t *testing.T) {
	byEntry := emit(t, namedBackendValues(l{graphqlEntry("query-api")}, nil))
	table := namedBackendValues(l{}, nil)
	ing := table["ingress"].(m)
	ing["queryApiPaths"] = append(ing["queryApiPaths"].(l), m{"path": "/graphql", "pathType": "Exact"})
	byTable := emit(t, table)
	for _, path := range []string{"/graphql", "/graphqlx", "/graphql/", "/graphql/x", "/other"} {
		if a, b := route(t, byEntry, path), route(t, byTable, path); a != b {
			t.Errorf("%s: %s by entry, %s by table", path, a, b)
		}
	}
	wantRoutes(t, byEntry, map[string]string{"/GRAPHQL": queryAPI})
	wantRoutes(t, byTable, map[string]string{"/GRAPHQL": goAPI})
}

const (
	sharedListName = "ops.ingress.pythonAllowList"
	ownListName    = "ops.ingress.hosts[in-cluster.example].pythonAllowList"
)

func TestHalfABackendChangeIsRefused(t *testing.T) {
	for _, c := range []struct {
		shared, own        l
		pythonList, goList string
	}{
		{l{graphqlEntry("query-api")}, l{graphqlEntry(""), metricsEntry}, ownListName, sharedListName + " (service: query-api)"},
		{l{graphqlEntry("")}, l{graphqlEntry("query-api"), metricsEntry}, sharedListName, ownListName + " (service: query-api)"},
	} {
		r := runValues(t, namedBackendValues(c.shared, c.own))
		wantCode(t, r, 4)
		if r.stdout != "" {
			t.Fatalf("stdout %q", r.stdout)
		}
		wantIn(t, r.stderr, "REFUSED: one path, two backends")
		wantIn(t, r.stderr, c.pythonList+" entry {path: /graphql$")
		wantIn(t, r.stderr, "and "+c.goList+" entry /graphql$")
	}
}

func TestANamedBackendThatDiffersByHostIsRefused(t *testing.T) {
	for _, c := range []struct {
		shared, own      l
		has, lacks, host string
	}{
		{l{}, l{graphqlEntry("query-api"), metricsEntry}, ownListName, sharedListName, "api.example"},
		{l{graphqlEntry("query-api")}, l{metricsEntry}, sharedListName, ownListName, "in-cluster.example"},
		{l{graphqlEntry("query-api")}, l{}, sharedListName, ownListName, "in-cluster.example"},
	} {
		r := runValues(t, namedBackendValues(c.shared, c.own))
		wantCode(t, r, 4)
		if r.stdout != "" {
			t.Fatalf("stdout %q", r.stdout)
		}
		wantIn(t, r.stderr, "REFUSED: one path, a backend that differs by host")
		wantIn(t, r.stderr, "/graphql$ is sent to query-api by "+c.has+" and not by "+c.lacks+" (host "+c.host+")\n")
	}
}

var (
	goDefault = l{m{"path": "/", "pathType": "Prefix", "service": "go-api"}}
	webHost   = m{"host": "www.example", "paths": l{m{"path": "/", "pathType": "Prefix", "service": "web"}}}
)

// hostsValues is baseValues with exactly this shared list and these hosts (nil leaves the key out).
func hostsValues(shared l, hosts l) m {
	doc := baseValues()
	ops := m{}
	if shared != nil {
		ops["pythonAllowList"] = shared
	}
	if hosts != nil {
		ops["hosts"] = hosts
	}
	doc["ops"] = m{"ingress": ops}
	return doc
}

func TestTheShippedHostShapeIsAccepted(t *testing.T) {
	v := hostsValues(l{graphqlEntry("query-api")}, l{
		m{"host": "api.example", "pythonAllowList": true, "paths": goDefault},
		webHost,
		m{"host": "in-cluster.example", "pythonAllowList": l{graphqlEntry("query-api"), metricsEntry}, "paths": goDefault},
	})
	if got := routing.QueryAPIPathsThatDifferByHost(load(t, v)); len(got) != 0 {
		t.Fatal(got)
	}
	wantCode(t, runValues(t, v), 0)
	routed := emit(t, v)
	wantRoutes(t, routed, map[string]string{"/graphql": queryAPI})
	table := hostsValues(l{}, l{webHost})
	ing := table["ingress"].(m)
	ing["queryApiPaths"] = append(ing["queryApiPaths"].(l), m{"path": "/graphql", "pathType": "Exact"})
	byTable := emit(t, table)
	for _, path := range []string{"/graphql", "/graphqlx", "/graphql/", "/graphql/x"} {
		if a, b := route(t, routed, path), route(t, byTable, path); a != b {
			t.Errorf("%s: %s by entry, %s by table", path, a, b)
		}
	}
}

func TestAHostWithNoAllowListThatIsNotAWebHostIsRefused(t *testing.T) {
	for _, c := range []struct {
		other    m
		services string
	}{
		{m{"host": "python.example", "paths": l{m{"path": "/graphql", "pathType": "Exact", "service": "api"}}}, "api"},
		{m{"host": "python.example", "paths": l{m{"path": "/", "pathType": "Prefix", "service": "api"}}}, "api"},
		{m{"host": "python.example", "paths": l{m{"path": "/", "pathType": "Prefix", "service": "web"}, m{"path": "/graphql", "pathType": "Exact", "service": "api"}}}, "api, web"},
		{m{"host": "python.example"}, "none"},
		{m{"host": "python.example", "pythonAllowList": false, "paths": goDefault}, "go-api"},
	} {
		r := runValues(t, hostsValues(l{graphqlEntry("query-api")}, l{m{"host": "api.example", "pythonAllowList": true, "paths": goDefault}, c.other}))
		wantCode(t, r, 4)
		if r.stdout != "" {
			t.Fatalf("stdout %q", r.stdout)
		}
		wantIn(t, r.stderr, "REFUSED: one path, a backend that differs by host")
		wantIn(t, r.stderr, "/graphql$ is sent to query-api by ops.ingress.pythonAllowList, and host python.example has no allow-list: its own paths answer it (services: "+c.services+")\n")
	}
}

func TestAHostThatCarriesTheEntryHasNoPathOfItsOwnBesideTheDefault(t *testing.T) {
	for _, own := range []m{
		{"path": "/graphql", "pathType": "Exact", "service": "api"},
		{"path": "/graph", "pathType": "Prefix", "service": "web"},
		{"path": "/api", "pathType": "Prefix", "service": "api"},
	} {
		r := runValues(t, hostsValues(l{graphqlEntry("query-api")}, l{
			m{"host": "api.example", "pythonAllowList": true, "paths": goDefault},
			m{"host": "in-cluster.example", "pythonAllowList": l{graphqlEntry("query-api"), metricsEntry}, "paths": append(append(l{}, goDefault...), own)},
		}))
		wantCode(t, r, 4)
		if r.stdout != "" {
			t.Fatalf("stdout %q", r.stdout)
		}
		wantIn(t, r.stderr, "/graphql$ is sent to query-api by "+sharedListName+", "+ownListName+", and host in-cluster.example has paths of its own beside `/`: "+own["path"].(string)+"\n")
	}
}

func TestAListThatNoHostUsesIsNotRead(t *testing.T) {
	ownOnly := hostsValues(l{}, l{m{"host": "api.example", "pythonAllowList": l{graphqlEntry("query-api")}, "paths": goDefault}})
	wantCode(t, runValues(t, ownOnly), 0)
	wantRoutes(t, emit(t, ownOnly), map[string]string{"/graphql": queryAPI})
	unused := hostsValues(l{graphqlEntry("query-api")}, l{m{"host": "api.example", "pythonAllowList": l{metricsEntry}, "paths": goDefault}})
	wantCode(t, runValues(t, unused), 0)
	wantRoutes(t, emit(t, unused), map[string]string{"/graphql": goAPI})
}

func TestValuesThatListNoHostReadTheSharedList(t *testing.T) {
	v := hostsValues(l{graphqlEntry("query-api")}, nil)
	wantCode(t, runValues(t, v), 0)
	wantRoutes(t, emit(t, v), map[string]string{"/graphql": queryAPI})
}

func TestEveryHostThatLacksTheEntryIsNamed(t *testing.T) {
	r := runValues(t, hostsValues(l{}, l{
		m{"host": "a.example", "pythonAllowList": true},
		m{"host": "b.example", "pythonAllowList": true},
		m{"host": "in-cluster.example", "pythonAllowList": l{graphqlEntry("query-api")}},
	}))
	wantCode(t, r, 4)
	for _, host := range []string{"a.example", "b.example"} {
		line := "/graphql$ is sent to query-api by " + ownListName + " and not by " + sharedListName + " (host " + host + ")\n"
		if n := strings.Count(r.stderr, line); n != 1 {
			t.Errorf("%s named %d times", host, n)
		}
	}
	wantNotIn(t, r.stderr, "(host in-cluster.example)")
}

func TestANamedBackendAndAPathTableForOnePathAreRefused(t *testing.T) {
	for _, c := range []struct {
		table string
		entry m
	}{
		{"queryApiPaths", m{"path": "/graphql", "pathType": "Exact"}},
		{"goApiPaths", m{"path": "/graphql"}},
		{"queryApiPaths", m{"path": "/graph[^/]+", "pathType": "ImplementationSpecific"}},
	} {
		v := namedBackendValues(l{graphqlEntry("query-api")}, nil)
		ing := v["ingress"].(m)
		ing[c.table] = append(ing[c.table].(l), c.entry)
		r := runValues(t, v)
		wantCode(t, r, 4)
		if r.stdout != "" {
			t.Fatalf("stdout %q", r.stdout)
		}
		wantIn(t, r.stderr, "REFUSED: one path, two Ingress objects")
		wantIn(t, r.stderr, "service: query-api} and ingress."+c.table)
	}
}

func TestANamedBackendThisRouterCannotRouteIsRefused(t *testing.T) {
	type c struct {
		entry  m
		reason string
	}
	const notAnchored = "is not an anchored entry"
	const badService = "routes to api (the Python api, the default) or to query-api"
	cases := []c{
		{m{"path": "/graphql", "pathType": "Prefix", "service": "query-api"}, notAnchored},
		{m{"path": "/graphql", "pathType": "Exact", "service": "query-api"}, notAnchored},
		{m{"path": "/graphql[^/]+", "pathType": "ImplementationSpecific", "service": "query-api"}, notAnchored},
		{graphqlEntry("go-api"), badService},
		{m{"path": "/graphql$", "pathType": "ImplementationSpecific", "service": nil}, badService},
		{m{"path": "/graphql$", "pathType": "ImplementationSpecific", "service": ""}, badService},
	}
	for _, path := range []string{"/", "", "/a/../graphql", "//graphql", "graphql"} {
		cases = append(cases, c{m{"path": path, "pathType": "Exact", "service": "query-api"}, notAnchored})
	}
	for _, pathType := range []string{"Exact", "Prefix"} {
		cases = append(cases, c{m{"path": "/graphql$", "pathType": pathType, "service": "query-api"}, notAnchored})
	}
	for _, path := range []string{`/a/\.\./graphql$`, `/\./graphql$`, `/graphql/\.\.$`} {
		cases = append(cases, c{m{"path": path, "pathType": "ImplementationSpecific", "service": "query-api"}, "has a `.` or `..` segment"})
	}
	for _, tc := range cases {
		for _, v := range []m{namedBackendValues(l{tc.entry}, nil), namedBackendValues(l{}, l{tc.entry})} {
			r := runValues(t, v)
			wantCode(t, r, 1)
			wantIn(t, r.stderr, tc.reason)
		}
	}
	// The service is named the way Python's repr names it.
	r := runValues(t, namedBackendValues(l{m{"path": "/graphql$", "pathType": "ImplementationSpecific", "service": ""}}, nil))
	wantIn(t, r.stderr, "ops.ingress.pythonAllowList entry '/graphql$' has service '':")
	r = runValues(t, namedBackendValues(l{m{"path": "/graphql$", "pathType": "ImplementationSpecific", "service": nil}}, nil))
	wantIn(t, r.stderr, "has service None:")
}
