package bigboy_test

import (
	"regexp"
	"strings"
	"testing"
)

const composeRouterExample = "ci/bigboy/traefik-dynamic-planes.yml.example"

var (
	composeHostCallRe = regexp.MustCompile("\\b(Host|HostRegexp|HostSNI|HostSNIRegexp|ClientIP|Header|HeaderRegexp)\\(`([^`]*)`\\)")
	composePathCallRe = regexp.MustCompile("PathRegexp\\(`[^`]*`\\)")
	composePathsRe    = regexp.MustCompile("\\((?:Path|PathPrefix)\\(`[^`]+`\\)(?: \\|\\| (?:Path|PathPrefix)\\(`[^`]+`\\))*\\)")
)

func composeInternalOnly(g *guard, name, rule string) {
	matchers := composeHostCallRe.FindAllStringSubmatch(rule, -1)
	if len(matchers) == 0 {
		g.failf("router %s: rule has no Host() clause at all: %.120s", name, rule)
		return
	}
	if len(matchers) != 1 || matchers[0][1] != "Host" || matchers[0][2] != "traefik" {
		var got []string
		for _, m := range matchers {
			got = append(got, m[1]+"("+m[2]+")")
		}
		g.failf("router %s: rule must name Host(`traefik`) only, got %v", name, got)
		return
	}
	skeleton := strings.TrimSpace(composePathCallRe.ReplaceAllString(rule, "PathRegexp(P)"))
	skeleton = composePathsRe.ReplaceAllString(skeleton, "(PATHS)")
	switch skeleton {
	case "Host(`traefik`)", "Host(`traefik`) && PathRegexp(P)", "Host(`traefik`) && (PATHS)":
	default:
		g.failf("router %s: rule can match without the internal Host clause: %s", name, skeleton)
	}
}

func composeExampleInternalOnly(g *guard) {
	routers := mp(at(g.yamlFile(composeRouterExample), "http", "routers"))
	g.expect(len(routers) > 0, "%s has no routers", composeRouterExample)
	for _, name := range composeSorted(routers) {
		composeInternalOnly(g, name, str(at(routers[name], "rule")))
	}
}

var composeWebExpected = map[string]string{
	"BACKEND_URL":        "http://traefik:3000",
	"AUTH_URL":           "https://www.commanderkeen.dev",
	"TRUST_PROXY":        "true",
	"TRUSTED_PROXY_HOPS": "1",
}

func composeWebEnvCarriesOverlayValues(g *guard) {
	env := mp(composeMust(g, composeIn{}).services["web"]["environment"])
	for _, name := range composeSorted(composeWebExpected) {
		v, ok := env[name]
		if !ok {
			g.failf("web.environment is missing %s", name)
			continue
		}
		g.expect(v == composeWebExpected[name], "web.environment %s=%q, expected %q", name, v, composeWebExpected[name])
	}
}

func composeQueryAPICarriesLLMModelExplain(g *guard) {
	env := at(g.yamlFile(composeFile), "services", "query-api", "environment")
	items := strs(env)
	for k, v := range mp(env) {
		items = append(items, k+"="+str(v))
	}
	g.expect(composeSet(items...)["LLM_MODEL_EXPLAIN=${LLM_MODEL_EXPLAIN:-}"], "query-api does not pass LLM_MODEL_EXPLAIN through with an empty default (LLM_MODEL_EXPLAIN=${LLM_MODEL_EXPLAIN:-})")
}

var composeRouterChecks = []check{
	{"plane_split_router/checked_in_example_is_internal_only", composeExampleInternalOnly},
	{"web_env_parity/tracked_overlay_carries_the_expected_web_values", composeWebEnvCarriesOverlayValues},
	{"web_env_parity/query_api_carries_llm_model_explain", composeQueryAPICarriesLLMModelExplain},
}

func TestComposeRouterReal(t *testing.T) { runReal(t, composeRouterChecks...) }

func TestComposeRouterPlanted(t *testing.T) {
	const goRule = "rule: \"Host(`traefik`) && PathRegexp(`^("
	ex := []string{composeRouterExample}
	bad := func(prefix string) func(*testing.T, string) {
		return mut(composeRouterExample, goRule, "rule: \""+prefix+"PathRegexp(`^(")
	}
	runPlanted(t, []planted{
		{name: "public host or internal host", copy: ex, mutate: bad("Host(`www.commanderkeen.dev`) || Host(`traefik`) && "),
			check: composeExampleInternalOnly, want: "Host(`traefik`) only"},
		{name: "public host only", copy: ex, mutate: bad("Host(`commanderkeen.dev`) && "),
			check: composeExampleInternalOnly, want: "Host(`traefik`) only"},
		{name: "no host clause", copy: ex, mutate: bad(""),
			check: composeExampleInternalOnly, want: "no Host() clause"},
		{name: "host OR path", copy: ex, mutate: bad("Host(`traefik`) || "),
			check: composeExampleInternalOnly, want: "without the internal Host clause"},
		{name: "any host regexp", copy: ex, mutate: bad("HostRegexp(`.+`) && "),
			check: composeExampleInternalOnly, want: "Host(`traefik`) only"},
		{name: "catch-all opened to the public host", copy: ex, mutate: mut(composeRouterExample, "rule: \"Host(`traefik`)\"", "rule: \"Host(`traefik`) || Host(`commanderkeen.dev`)\""),
			check: composeExampleInternalOnly, want: "api-internal-catchall"},
		{name: "no routers", copy: ex, mutate: func(t *testing.T, r string) { writeFile(t, r, composeRouterExample, "http:\n  routers: {}\n") },
			check: composeExampleInternalOnly, want: "has no routers"},
		{name: "web AUTH_URL missing", copy: composeFiles, mutate: mut(composeFile, "    - AUTH_URL=https://www.commanderkeen.dev\n", ""),
			check: composeWebEnvCarriesOverlayValues, want: "missing AUTH_URL"},
		{name: "web BACKEND_URL at the api", copy: composeFiles, mutate: mut(composeFile, "    - BACKEND_URL=http://traefik:3000", "    - BACKEND_URL=http://api:8000"),
			check: composeWebEnvCarriesOverlayValues, want: "BACKEND_URL"},
		{name: "web TRUST_PROXY missing", copy: composeFiles, mutate: mut(composeFile, "    - TRUST_PROXY=true\n", ""),
			check: composeWebEnvCarriesOverlayValues, want: "missing TRUST_PROXY"},
		{name: "web TRUSTED_PROXY_HOPS missing", copy: composeFiles, mutate: mut(composeFile, "    - TRUSTED_PROXY_HOPS=1\n", ""),
			check: composeWebEnvCarriesOverlayValues, want: "missing TRUSTED_PROXY_HOPS"},
		{name: "web TRUST_PROXY false", copy: composeFiles, mutate: mut(composeFile, "    - TRUST_PROXY=true", "    - TRUST_PROXY=false"),
			check: composeWebEnvCarriesOverlayValues, want: "TRUST_PROXY=\"false\", expected"},
		{name: "web TRUSTED_PROXY_HOPS 2", copy: composeFiles, mutate: mut(composeFile, "    - TRUSTED_PROXY_HOPS=1", "    - TRUSTED_PROXY_HOPS=2"),
			check: composeWebEnvCarriesOverlayValues, want: "TRUSTED_PROXY_HOPS=\"2\", expected"},
		{name: "explain model hard-wired", copy: composeFiles, mutate: mut(composeFile, "LLM_MODEL_EXPLAIN=${LLM_MODEL_EXPLAIN:-}", "LLM_MODEL_EXPLAIN=${LLM_MODEL_EXPLAIN:-gpt-5-nano}"),
			check: composeQueryAPICarriesLLMModelExplain, want: "LLM_MODEL_EXPLAIN"},
	})
}
