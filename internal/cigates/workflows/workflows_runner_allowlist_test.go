package workflows_test

import (
	"fmt"
	"sort"
	"strings"
	"testing"
)

// CHAOS-10177: Go port of tests/tooling/test_self_hosted_runner_allowlist.py (CHAOS-5197, 9949). Self-hosted ARC
// routing is an ALLOW-list of safe event shapes; every routing site must carry that condition EXACTLY (equality,
// never substring or suffix: a prepended `merge_group ||` disjunct must fail), the hosted twin is its literal
// negation, and the draft-gated job set is pinned both ways.

const allowlistCondition = "vars.SELF_HOSTED_RUNNERS == 'enabled' && " +
	"((github.event_name == 'pull_request' && github.event.pull_request.head.repo.full_name == github.repository) || " +
	"(github.event_name == 'push' && github.ref == 'refs/heads/main') || " +
	"(github.event_name == 'workflow_dispatch' && github.event.inputs.ref == '') || " +
	"github.event_name == 'release' || " +
	"github.event_name == 'schedule')"

// racePoolTerm is the extra go-quality-leg route (CHAOS-10294): the legs flagged `pool` take the
// CI_RACE_RUNNER pool on a push to main of this repository, never on a fork or a pull_request event.
const racePoolTerm = "matrix.pool && vars.CI_RACE_RUNNER != '' && github.event_name == 'push' && " +
	"github.ref == 'refs/heads/main' && github.repository == 'full-chaos/dev-health-ops'"

var imageAllowlistCondition = "(vars.SELF_HOSTED_IMAGE_BUILDS == 'enabled' || vars.SELF_HOSTED_RUNNERS == 'enabled')" +
	strings.TrimPrefix(allowlistCondition, "vars.SELF_HOSTED_RUNNERS == 'enabled'")

var imageRoutingTernary = "${{ (" + imageAllowlistCondition + ") " + `&& fromJSON('["self-hosted","oci-arc-runners"]') || 'ubuntu-latest' }}`

// imageBuildJobs are the only jobs that may read SELF_HOSTED_IMAGE_BUILDS (CHAOS-9643).
var imageBuildJobs = map[string]bool{"go-build": true, "dind-smoke-test": true, "go-build-dho-arm64": true, "go-api-tools-build": true}

const (
	routingTernary = "${{ (" + allowlistCondition + ") " + `&& fromJSON('["self-hosted","oci-arc-runners"]') || 'ubuntu-latest' }}`
	cacheNegation  = "${{ !(" + allowlistCondition + ") }}"

	goQualityRoutingTernary = "${{ (" + racePoolTerm + ") && vars.CI_RACE_RUNNER || " +
		`(` + allowlistCondition + `) && fromJSON('["self-hosted","oci-arc-runners"]') || 'ubuntu-latest' }}`
	goQualityCacheNegation = "${{ !((" + allowlistCondition + ") || (" + racePoolTerm + ")) }}"
	denyFragment           = "github.event_name != 'pull_request'"
	draftGate              = "(!(github.event_name == 'pull_request' && github.event.pull_request.draft && !contains(github.event.pull_request.labels.*.name, 'ci:full')))"
)

var routedWorkflowFiles = []string{"go-quality.yml", "test.yml", "live-e2e.yml", "go.yml", "docker-images.yml"}

var draftGatedJobs = [][2]string{
	{"codeql-analysis.yml", "analyze"},
	{"docker-images.yml", "dind-smoke-test"},
	{"docker-images.yml", "go-api-tools-build"},
	{"docker-images.yml", "go-build"},
	{"docker-images.yml", "go-build-dho-arm64"},
	{"go-auth-service.yml", "auth-changes"},
	{"go-workers-compose-proof.yml", "go-workers-compose-proof"},
	{"go.yml", "dind-smoke-test"},
	{"go.yml", "go-auth-compose-proof"},
	{"go.yml", "go-auth-web-proxy-proof"},
	{"go.yml", "go-arm64-numeric-parity"},
	{"go.yml", "go-container-reproducibility"},
	{"go.yml", "go-container-reproducibility-self-hosted"},
	{"go.yml", "go-container-security"},
	{"go.yml", "go-container-smoke"},
	{"go.yml", "go-storage-integration-plan"},
	{"go.yml", "graphql-wire-parity"},
	{"live-e2e.yml", "live-e2e"},
	{"live-e2e.yml", "metrics-executed-proof"},
	{"security-scan.yml", "semgrep"},
	{"venue-oracles-changed.yml", "venue-oracles-changed"},
}

var requiredContextJobs = map[string][]string{
	"go-quality.yml": {"go-quality-leg", "go-quality"},
	"test.yml":       {"changes", "chart-started", "test"},
	"live-e2e.yml":   {"changes", "metrics-executed-proof"},
}

var cacheNegationJobs = map[string]string{"go-quality.yml": "go-quality-leg", "test.yml": "chart-started", "live-e2e.yml": "metrics-executed-proof"}

type cachePathSite struct{ job, step, ifWant string }

var cachePathSites = map[string]cachePathSite{
	"go-quality.yml": {"go-quality-leg", "Configure self-hosted Go cache paths", "steps.relevance.outputs.relevant == 'true' && ((" + allowlistCondition + ") || (" + racePoolTerm + "))"},
	"test.yml":       {"chart-started", "Configure self-hosted Go cache paths", allowlistCondition},
	"live-e2e.yml":   {"metrics-executed-proof", "Configure self-hosted Go cache paths", allowlistCondition},
}

func ungated(v string) string {
	prefix := draftGate + " && ("
	if strings.HasPrefix(v, prefix) && strings.HasSuffix(v, ")") {
		return v[len(prefix) : len(v)-1]
	}
	return v
}

func stringValues(node any, path string, visit func(path, v string)) {
	switch n := node.(type) {
	case yamlMap:
		for _, k := range sortedKeys(n) {
			stringValues(n[k], path+"."+k, visit)
		}
	case []any:
		for i, v := range n {
			stringValues(v, fmt.Sprintf("%s[%d]", path, i), visit)
		}
	case string:
		visit(path, n)
	}
}

func str(v any) string {
	if v == nil {
		return ""
	}
	return fmt.Sprint(v)
}

func runnerAllowlistViolations(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	add := func(f string, a ...any) { out = append(out, fmt.Sprintf(f, a...)) }
	docs := map[string]yamlMap{}
	doc := func(name string) yamlMap {
		if d, ok := docs[name]; ok {
			return d
		}
		docs[name] = loadWf(t, root, name)
		return docs[name]
	}
	job := func(file, name string) (yamlMap, bool) {
		j := asMap(jobsOf(doc(file))[name])
		if j == nil {
			add("%s: job %q not found: renamed or removed; update the guard's job constants rather than letting it go stale", file, name)
		}
		return j, j != nil
	}

	// draft gate on each pinned job, and the pinned set equals the found set
	pinned := map[[2]string]bool{}
	for _, p := range draftGatedJobs {
		pinned[p] = true
		if j, ok := job(p[0], p[1]); ok {
			v := str(j["if"])
			if !(v == draftGate || strings.HasPrefix(v, draftGate+" && (")) {
				add("%s:%s lost the draft gate: got %q", p[0], p[1], v)
			}
		}
	}
	found := map[[2]string]bool{}
	for _, name := range workflowNames(t, root) {
		jobs := jobsOf(doc(name))
		for _, jn := range sortedKeys(jobs) {
			if strings.Contains(str(asMap(jobs[jn])["if"]), "'ci:full'") {
				found[[2]string{name, jn}] = true
			}
		}
	}
	var unpinned, notGated []string
	for p := range found {
		if !pinned[p] {
			unpinned = append(unpinned, p[0]+":"+p[1])
		}
	}
	for p := range pinned {
		if !found[p] {
			notGated = append(notGated, p[0]+":"+p[1])
		}
	}
	sort.Strings(unpinned)
	sort.Strings(notGated)
	if len(unpinned) > 0 || len(notGated) > 0 {
		add("draft-gated job set drifted: unpinned %v; pinned but not gated %v", unpinned, notGated)
	}

	// no deny-list fragment next to SELF_HOSTED_RUNNERS in a routed workflow
	for _, name := range routedWorkflowFiles {
		stringValues(doc(name), "", func(path, v string) {
			if strings.Contains(v, "SELF_HOSTED_RUNNERS") && strings.Contains(v, denyFragment) {
				add("%s%s: deny-list fragment reintroduced alongside SELF_HOSTED_RUNNERS: every non-pull_request event (merge_group included) becomes self-hosted-eligible: %q", name, path, v)
			}
		})
	}

	// the required-context jobs route by the exact ternary
	for _, name := range sortedKeys(toAnyMap(requiredContextJobs)) {
		for _, jn := range requiredContextJobs[name] {
			if j, ok := job(name, jn); ok {
				want := routingTernary
				if name == "go-quality.yml" && jn == "go-quality-leg" {
					want = goQualityRoutingTernary
				}
				if got := str(j["runs-on"]); got != want {
					add("%s:%s runs-on does not EXACTLY match the routing ternary: got %q, want %q", name, jn, got, want)
				}
			}
		}
	}

	// setup-go cache negation
	for _, name := range sortedKeys(toAnyMap2(cacheNegationJobs)) {
		jn := cacheNegationJobs[name]
		j, ok := job(name, jn)
		if !ok {
			continue
		}
		var cache any
		for _, s := range stepsOf(j) {
			if strings.HasPrefix(asString(s["uses"]), "actions/setup-go@") {
				cache = asMap(s["with"])["cache"]
				break
			}
		}
		if off, ok := cache.(bool); ok && !off && name == "go-quality.yml" {
			// CHAOS-10486: off on every runner; the GitHub-hosted legs restore their own per-leg cache instead
			continue
		}
		if cache == nil {
			add("%s:%s: no actions/setup-go step with a cache input found", name, jn)
		} else if want := map[bool]string{true: goQualityCacheNegation, false: cacheNegation}[name == "go-quality.yml"]; str(cache) != want {
			add("%s:%s cache negation does not EXACTLY match the allow-list negation: got %q, want %q", name, jn, str(cache), want)
		}
	}

	// the reproducibility twin pair: the hosted leg is the LITERAL negation
	if hosted, ok := job("go.yml", "go-container-reproducibility"); ok {
		if sh, ok := job("go.yml", "go-container-reproducibility-self-hosted"); ok {
			if got := ungated(str(sh["if"])); got != allowlistCondition {
				add("go-container-reproducibility-self-hosted's if does not EXACTLY match the allow-list condition: got %q", got)
			}
			if got := ungated(str(hosted["if"])); got != "!("+allowlistCondition+")" {
				add("go-container-reproducibility's if must be the literal negation !(...) of the self-hosted leg's condition: got %q", got)
			}
		}
	}

	// dind-smoke-test routing sites
	for _, f := range []string{"go.yml", "docker-images.yml"} {
		if j, ok := job(f, "dind-smoke-test"); ok {
			if got := ungated(str(j["if"])); got != dindWant(f) {
				add("%s:dind-smoke-test if does not EXACTLY match the allow-list condition: got %q", f, got)
			}
		}
	}

	// the docker-images go-build linux/arm64 matrix leg
	if j, ok := job("docker-images.yml", "go-build"); ok {
		var arm []yamlMap
		for _, e := range asList(asMap(asMap(j["strategy"])["matrix"])["include"]) {
			if m := asMap(e); asString(m["platform"]) == "linux/arm64" {
				arm = append(arm, m)
			}
		}
		if len(arm) != 1 {
			add("docker-images.yml:go-build: expected exactly one linux/arm64 matrix include entry, found %d", len(arm))
		} else if got := str(arm[0]["runner"]); got != imageRoutingTernary {
			add("docker-images.yml:go-build's linux/arm64 matrix runner does not EXACTLY match the routing ternary: got %q", got)
		}
	}

	// the self-hosted cache-path env steps, exact `if:`
	for _, name := range sortedKeys(toAnyMap3(cachePathSites)) {
		site := cachePathSites[name]
		j, ok := job(name, site.job)
		if !ok {
			continue
		}
		var matches []yamlMap
		for _, s := range stepsOf(j) {
			if asString(s["name"]) == site.step {
				matches = append(matches, s)
			}
		}
		if len(matches) != 1 {
			add("%s:%s: expected exactly one step named %q, found %d", name, site.job, site.step, len(matches))
		} else if got := str(matches[0]["if"]); got != site.ifWant {
			add("%s:%s:%q if does not EXACTLY match the expected condition: got %q, want %q", name, site.job, site.step, got, site.ifWant)
		}
	}
	return out
}

func toAnyMap(m map[string][]string) yamlMap {
	o := yamlMap{}
	for k := range m {
		o[k] = nil
	}
	return o
}
func toAnyMap2(m map[string]string) yamlMap {
	o := yamlMap{}
	for k := range m {
		o[k] = nil
	}
	return o
}
func toAnyMap3(m map[string]cachePathSite) yamlMap {
	o := yamlMap{}
	for k := range m {
		o[k] = nil
	}
	return o
}

func TestSelfHostedRunnerAllowlist(t *testing.T) {
	expectNoViolations(t, runnerAllowlistViolations(t, repoRoot(t)))
}

// The exact-match defence itself: a suffix check would accept a prepended unsafe disjunct, equality must not.
func TestAllowlistExactMatchRejectsAPrependedDisjunct(t *testing.T) {
	for _, want := range []string{cachePathSites["go-quality.yml"].ifWant, allowlistCondition} {
		regressed := "github.event_name == 'merge_group' || " + want
		if !strings.HasSuffix(regressed, want) {
			t.Fatal("positive control failed: the regressed string no longer ends with the expected condition")
		}
		if regressed == want {
			t.Fatal("equality accepted a prepended disjunct")
		}
	}
}

func TestSelfHostedRunnerAllowlistPlantedDefects(t *testing.T) {
	root := repoRoot(t)
	var files []string
	for _, n := range workflowNames(t, root) {
		files = append(files, ".github/workflows/"+n)
	}
	const (
		test_  = ".github/workflows/test.yml"
		goYml  = ".github/workflows/go.yml"
		dock   = ".github/workflows/docker-images.yml"
		live   = ".github/workflows/live-e2e.yml"
		secscn = ".github/workflows/security-scan.yml"
	)
	merge := "github.event_name == 'schedule' || github.event_name == 'merge_group')"
	cases := []struct {
		name string
		muts []mutation
		want []string
	}{
		{"routing ternary gains a merge_group term (test.yml)", []mutation{{test_, "github.event_name == 'schedule')", merge}}, []string{"test.yml", "does not EXACTLY match the routing ternary"}},
		{"deny-list fragment returns beside SELF_HOSTED_RUNNERS", []mutation{{live, "(vars.SELF_HOSTED_RUNNERS == 'enabled' && ((github.event_name == 'pull_request'", "(vars.SELF_HOSTED_RUNNERS == 'enabled' && ((github.event_name != 'pull_request'"}}, []string{"EXACTLY", "SELF_HOSTED_RUNNERS"}},
		{"a go.yml job loses its draft gate", []mutation{{goYml, "if: " + draftGate + " && (vars.SELF_HOSTED_RUNNERS", "if: (vars.SELF_HOSTED_RUNNERS"}}, []string{"go.yml", "lost the draft gate"}},
		{"a job gains ci:full without being pinned", []mutation{{test_, "|| needs.changes.outputs.code == 'true'\n    # Same ARC", "|| needs.changes.outputs.code == 'true' || contains(github.event.pull_request.labels.*.name, 'ci:full')\n    # Same ARC"}}, []string{"draft-gated job set drifted", "test.yml:chart-started"}},
		{"hosted twin stops being the literal negation", []mutation{{goYml, "go-container-reproducibility:\n", "go-container-reproducibility:\n    # planted\n"}, {goYml, "!(vars.SELF_HOSTED_RUNNERS == 'enabled'", "(vars.SELF_HOSTED_RUNNERS != 'enabled'"}}, []string{"literal negation"}},
		{"setup-go cache negation loses its negation", []mutation{{test_, "cache: ${{ !(vars.SELF_HOSTED_RUNNERS", "cache: ${{ (vars.SELF_HOSTED_RUNNERS"}}, []string{"cache negation does not EXACTLY match"}},
		{"cache-path step gets a prepended disjunct", []mutation{{test_, "if: vars.SELF_HOSTED_RUNNERS == 'enabled' && ((github.event_name == 'pull_request'", "if: github.event_name == 'merge_group' || vars.SELF_HOSTED_RUNNERS == 'enabled' && ((github.event_name == 'pull_request'"}}, []string{"Configure self-hosted Go cache paths", "does not EXACTLY match the expected condition"}},
		{"docker arm64 matrix runner is hosted-only", []mutation{{dock, "runner: ${{ ((vars.SELF_HOSTED_IMAGE_BUILDS", "runner: ${{ (vars.SELF_HOSTED_RUNNERS != 'enabled' && (vars.SELF_HOSTED_IMAGE_BUILDS"}}, []string{"linux/arm64 matrix runner"}},
		{"a draft-gated job is renamed away", []mutation{{secscn, "  semgrep:\n", "  semgrep-renamed:\n"}}, []string{"security-scan.yml", "semgrep"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tree := plantTree(t, root, files, c.muts...)
			expectViolation(t, runnerAllowlistViolations(t, tree), c.want...)
		})
	}
}

func dindWant(file string) string {
	if file == "docker-images.yml" {
		return imageAllowlistCondition
	}
	return allowlistCondition
}

// imageVarViolations: SELF_HOSTED_IMAGE_BUILDS may appear only in the four docker-images.yml image-build jobs, in the
// exact image routing forms, and every one of those jobs must carry it; no other workflow may read it.
func imageVarViolations(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	add := func(f string, a ...any) { out = append(out, fmt.Sprintf(f, a...)) }
	const imgVar = "SELF_HOSTED_IMAGE_BUILDS"
	seen := map[string]bool{}
	for _, name := range workflowNames(t, root) {
		doc := loadWf(t, root, name)
		stringValues(doc, "", func(path, v string) {
			if !strings.Contains(v, imgVar) {
				return
			}
			parts := strings.Split(path, ".")
			jn := ""
			if len(parts) > 2 && parts[1] == "jobs" {
				jn = strings.SplitN(parts[2], "[", 2)[0]
			}
			if name != "docker-images.yml" || !imageBuildJobs[jn] {
				add("%s%s: %s is read outside the docker-images.yml image-build jobs", name, path, imgVar)
				return
			}
			seen[jn] = true
			if !strings.Contains(v, imageAllowlistCondition) {
				add("%s%s: %s is not the exact image allow-list condition: %q", name, path, imgVar, v)
			}
		})
	}
	for jn := range imageBuildJobs {
		if !seen[jn] {
			add("docker-images.yml:%s does not route on %s", jn, imgVar)
		}
	}
	d := loadWf(t, root, "docker-images.yml")
	if got := str(asMap(jobsOf(d)["go-build-dho-arm64"])["runs-on"]); got != imageRoutingTernary {
		add("docker-images.yml:go-build-dho-arm64 runs-on does not EXACTLY match the image routing ternary: got %q", got)
	}
	api := asMap(jobsOf(d)["go-api-tools-build"])
	n := 0
	for _, e := range asList(asMap(asMap(api["strategy"])["matrix"])["include"]) {
		if m := asMap(e); asString(m["platform"]) == "linux/arm64" {
			n++
			if got := str(m["runner"]); got != imageRoutingTernary {
				add("docker-images.yml:go-api-tools-build linux/arm64 runner does not EXACTLY match the image routing ternary: got %q", got)
			}
		}
	}
	if n != 1 {
		add("docker-images.yml:go-api-tools-build: expected exactly one linux/arm64 include, found %d", n)
	}
	return out
}

func TestImageBuildsVariableIsConfinedToImageJobs(t *testing.T) {
	root := repoRoot(t)
	expectNoViolations(t, imageVarViolations(t, root))
	files := []string{}
	for _, n := range workflowNames(t, root) {
		files = append(files, ".github/workflows/"+n)
	}
	cases := []struct {
		name string
		muts []mutation
		want []string
	}{
		{"new variable leaks into test.yml", []mutation{{".github/workflows/test.yml", "vars.SELF_HOSTED_RUNNERS == 'enabled'", "vars.SELF_HOSTED_IMAGE_BUILDS == 'enabled'"}}, []string{"test.yml", "read outside the docker-images.yml image-build jobs"}},
		{"image routing drops the RUNNERS fallback", []mutation{{".github/workflows/docker-images.yml", "(vars.SELF_HOSTED_IMAGE_BUILDS == 'enabled' || vars.SELF_HOSTED_RUNNERS == 'enabled')", "(vars.SELF_HOSTED_IMAGE_BUILDS == 'enabled')"}}, []string{"docker-images.yml"}},
		{"image routing gains a merge_group term", []mutation{{".github/workflows/docker-images.yml", "github.event_name == 'schedule'))", "github.event_name == 'schedule' || github.event_name == 'merge_group'))"}}, []string{"docker-images.yml"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			expectViolation(t, imageVarViolations(t, plantTree(t, root, files, c.muts...)), c.want...)
		})
	}
}
