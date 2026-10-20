package bigboy_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

// bigboy-cut.sh runs generate-plane-split-router.py to write the live Traefik config. These guards run the REAL generator
// (a real python3, a temporary dependency until the generator is ported) on a values file and require every rule it emits
// to match Host(`traefik`) only, in both of its output formats. A python3 or script that is missing fails the guard.

const composeRouterGenerator = "ci/bigboy/generate-plane-split-router.py"

const composeRouterValues = `ingress:
  goApiPaths:
    - path: /api/v1/external-ingest/schemas
    - path: /api/v1/external-ingest/schemas/{schema_id}
    - path: /health
  queryApiPaths:
    - path: /api/v1/work-units
      pathType: Exact
    - path: /api/v1/people/[^/]+/metric
      pathType: ImplementationSpecific
`

func composeRunGenerator(g *guard, format string) string {
	g.expect(composeRealBin("python3") != "", "python3 is not installed: the router generator cannot run, and this guard must not skip")
	dir := composeTempDir(g, g.tmp, "router-")
	values := filepath.Join(dir, "values.prod.yaml")
	composeWrite(g, values, composeRouterValues, 0o644)
	env := []string{"PATH=" + os.Getenv("PATH")}
	r, err := composeExec(g.root, env, 30*time.Second, "python3", g.path(composeRouterGenerator), values, "--format", format)
	if err != nil {
		g.failf("the router generator could not run: %v", err)
		return ""
	}
	if r.code != 0 {
		g.failf("the router generator failed with --format %s (rc=%d): %s", format, r.code, r.stderr)
		return ""
	}
	return r.stdout
}

func composeGeneratedDynamicInternalOnly(g *guard) {
	var doc map[string]any
	if err := yaml.Unmarshal([]byte(composeRunGenerator(g, "dynamic")), &doc); err != nil {
		g.failf("the generated dynamic config is not YAML: %v", err)
		return
	}
	routers := mp(at(doc, "http", "routers"))
	want := []string{"api-internal-catchall", "go-api-paths", "query-api-paths"}
	g.expect(equal(composeSorted(routers), want), "the generated dynamic config has routers %v, want exactly %v", composeSorted(routers), want)
	for _, name := range composeSorted(routers) {
		composeInternalOnly(g, "generated "+name, str(at(routers[name], "rule")))
		g.expect(equal(strs(at(routers[name], "entryPoints")), []string{"web"}), "generated router %s has entryPoints %v, want [web]", name, strs(at(routers[name], "entryPoints")))
	}
}

var composePathRegexpRe = regexp.MustCompile("PathRegexp\\(`([^`]*)`\\)")

func composeGeneratedPathsCoverTheValues(g *guard) {
	var doc map[string]any
	if err := yaml.Unmarshal([]byte(composeRunGenerator(g, "dynamic")), &doc); err != nil {
		g.failf("the generated dynamic config is not YAML: %v", err)
		return
	}
	regex := func(router string) *regexp.Regexp {
		m := composePathRegexpRe.FindStringSubmatch(str(at(doc, "http", "routers", router, "rule")))
		if m == nil {
			g.failf("generated router %s has no PathRegexp", router)
			return nil
		}
		re, err := regexp.Compile(m[1])
		if err != nil {
			g.failf("generated router %s has an invalid path regex %q: %v", router, m[1], err)
			return nil
		}
		return re
	}
	goRe, queryRe := regex("go-api-paths"), regex("query-api-paths")
	if goRe == nil || queryRe == nil {
		return
	}
	for _, c := range []struct {
		re   *regexp.Regexp
		path string
		want bool
	}{
		{goRe, "/api/v1/external-ingest/schemas/abc", true},
		{goRe, "/api/v1/external-ingest/schemas/abc/def", false},
		{queryRe, "/api/v1/work-units", true},
		{queryRe, "/api/v1/work-units/extra", false},
		{queryRe, "/api/v1/people/p1/metric", true},
	} {
		g.expect(c.re.MatchString(c.path) == c.want, "generated path regex %s: %s matches=%v, want %v", c.re, c.path, !c.want, c.want)
	}
}

var composeLabelRuleRe = regexp.MustCompile(`(?m)traefik\.http\.routers\.([\w-]+)\.rule: "(.*)"$`)

func composeGeneratedLabelsInternalOnly(g *guard) {
	rules := composeLabelRuleRe.FindAllStringSubmatch(composeRunGenerator(g, "labels"), -1)
	g.expect(len(rules) == 2, "the generated labels carry %d rules, want go-api-paths and query-api-paths", len(rules))
	for _, m := range rules {
		composeInternalOnly(g, "generated "+m[1], strings.TrimSpace(m[2]))
	}
}

func TestComposeRouterGeneratedReal(t *testing.T) {
	runReal(t,
		check{"dynamic_config_rules_are_internal_only", composeGeneratedDynamicInternalOnly},
		check{"label_rules_are_internal_only", composeGeneratedLabelsInternalOnly},
		check{"generated_paths_cover_the_values", composeGeneratedPathsCoverTheValues},
	)
}

func TestComposeRouterGeneratedPlanted(t *testing.T) {
	gen := []string{composeRouterGenerator}
	host := func(repl string) func(*testing.T, string) {
		return mut(composeRouterGenerator, "HOST_RULE = \"Host(`traefik`)\"", "HOST_RULE = \""+repl+"\"")
	}
	runPlanted(t, []planted{
		{name: "a-router-is-renamed", copy: gen, check: composeGeneratedDynamicInternalOnly, want: "want exactly",
			mutate: mut(composeRouterGenerator, "    api-internal-catchall:\n      rule:", "    api-internal-other:\n      rule:")},
		{name: "a-router-is-added", copy: gen, check: composeGeneratedDynamicInternalOnly, want: "want exactly",
			mutate: mut(composeRouterGenerator, "    query-api-paths:\n      rule:", "    extra-router:\n      rule: \"Host(`traefik`)\"\n      entryPoints: [web]\n      priority: 2\n      service: api-internal-catchall\n    query-api-paths:\n      rule:")},
		{name: "entry-points-change", copy: gen, check: composeGeneratedDynamicInternalOnly, want: "want [web]",
			mutate: mut(composeRouterGenerator, "entryPoints: [web]\n      priority: 1000\n      service: go-api-paths", "entryPoints: [websecure]\n      priority: 1000\n      service: go-api-paths")},
		{name: "the-combined-regex-matches-every-path", copy: gen, check: composeGeneratedPathsCoverTheValues, want: "matches=",
			mutate: mut(composeRouterGenerator, `return "^(" + "|".join(paths) + ")$"`, `return "^(.*)$"`)},
		{name: "the-combined-regex-loses-its-end-anchor", copy: gen, check: composeGeneratedPathsCoverTheValues, want: "matches=",
			mutate: mut(composeRouterGenerator, `return "^(" + "|".join(paths) + ")$"`, `return "^(" + "|".join(paths) + ")"`)},
	})
	for _, f := range []struct {
		name  string
		check func(*guard)
	}{{"dynamic", composeGeneratedDynamicInternalOnly}, {"labels", composeGeneratedLabelsInternalOnly}} {
		runPlanted(t, []planted{
			{name: f.name + "-public-host", copy: gen, mutate: host("Host(`www.commanderkeen.dev`)"), check: f.check, want: "Host(`traefik`) only"},
			{name: f.name + "-internal-or-public-host", copy: gen, mutate: host("Host(`traefik`) || Host(`www.commanderkeen.dev`)"), check: f.check, want: "Host(`traefik`) only"},
			{name: f.name + "-no-host-clause", copy: gen, mutate: host("PathPrefix(`/`)"), check: f.check, want: "no Host() clause"},
			{name: f.name + "-generator-missing", copy: nil, check: f.check, want: "the router generator failed"},
		})
	}
}
