package workflows_test

import (
	"fmt"
	"sort"
	"strings"
	"testing"
)

// The ARC pool is retired (D6994): every workflow runs on GitHub-hosted runners. No workflow value may read the dead
// routing variables (SELF_HOSTED_RUNNERS, SELF_HOSTED_IMAGE_BUILDS, CI_RACE_RUNNER) or name a self-hosted runner label,
// the required-context jobs run on the exact hosted label, setup-go keeps its cache where the pool used to turn it off,
// and the draft-gated and heavy-gated job sets stay pinned both ways.

// deadRunnerTokens may not appear in any workflow value (comments are not values).
var deadRunnerTokens = []string{"SELF_HOSTED_RUNNERS", "SELF_HOSTED_IMAGE_BUILDS", "CI_RACE_RUNNER", "matrix.pool", "oci-arc-runners", "/mnt/go-cache"}

const (
	hostedLabel    = "ubuntu-latest"
	hostedArmLabel = "ubuntu-24.04-arm"
	draftGate      = "(!(github.event_name == 'pull_request' && github.event.pull_request.draft && !contains(github.event.pull_request.labels.*.name, 'ci:full')))"
	// heavyGate: a heavy job runs in the merge queue, on the main push and on a pull request only with ci:full (drafts
	// included). imageHeavyGate adds the non-draft hotfix-image pull request, which publishes tree-<tree> images.
	heavyGate      = "(!(github.event_name == 'pull_request' && !contains(github.event.pull_request.labels.*.name, 'ci:full')))"
	imageHeavyGate = "(!(github.event_name == 'pull_request' && !contains(github.event.pull_request.labels.*.name, 'ci:full') && (github.event.pull_request.draft || !contains(github.event.pull_request.labels.*.name, 'hotfix-image'))))"
	goDocsSkip     = "needs.go-storage-integration-plan.outputs.docs_only != 'true'"
	// proofHeavyGate: live-e2e.yml's metrics-executed-proof-run also runs on a pull request that touches a
	// metrics_proof path of `changes`, with no ci:full label.
	proofHeavyGate = "(!(github.event_name == 'pull_request' && !contains(github.event.pull_request.labels.*.name, 'ci:full') && needs.changes.outputs.metrics_proof != 'true'))"
	// ciFullLabelEvent is live-e2e.yml's `labeled` filter (only ci:full starts work), not a heavy gate.
	ciFullLabelEvent = "github.event.label.name == 'ci:full'"
)

var draftGatedJobs = [][2]string{
	{"codeql-analysis.yml", "analyze"},
	{"codeql-analysis.yml", "analyze-docs-only"},
	{"go-auth-service.yml", "auth-changes"},
	{"go-workers-compose-proof.yml", "go-workers-compose-proof"},
	{"go.yml", "go-arm64-numeric-parity"},
	{"go.yml", "graphql-wire-parity"},
	{"security-scan.yml", "semgrep"},
	{"venue-oracles-changed.yml", "venue-oracles-changed"},
}

// heavyGatedJobs: the heavy jobs, which run in the merge queue and never on a pull request without ci:full, each with the
// gate its `if:` must start with.
var heavyGatedJobs = map[[2]string]string{
	{"docker-images.yml", "go-api-tools-build"}:    imageHeavyGate,
	{"docker-images.yml", "go-build"}:              imageHeavyGate,
	{"docker-images.yml", "go-build-dho-arm64"}:    imageHeavyGate,
	{"go.yml", "go-auth-compose-proof"}:            heavyGate + " && " + goDocsSkip,
	{"go.yml", "go-auth-web-proxy-proof"}:          heavyGate + " && " + goDocsSkip,
	{"go.yml", "go-container-build"}:               heavyGate + " && " + goDocsSkip,
	{"go.yml", "go-container-reproducibility"}:     heavyGate + " && " + goDocsSkip,
	{"go.yml", "go-container-security"}:            heavyGate + " && " + goDocsSkip,
	{"go.yml", "go-container-smoke"}:               heavyGate + " && " + goDocsSkip,
	{"live-e2e.yml", "live-e2e-run"}:               heavyGate + " && needs.changes.outputs.docs_only != 'true'",
	{"live-e2e.yml", "metrics-executed-proof-run"}: proofHeavyGate + " && needs.changes.outputs.docs_only != 'true'",
}

// hostedJobs: the jobs that used to route to the pool, each with the exact label it must run on now.
var hostedJobs = map[[2]string]string{
	{"go-quality.yml", "go-quality-leg"}:                   hostedLabel,
	{"go-quality.yml", "go-quality"}:                       hostedLabel,
	{"test.yml", "changes"}:                                hostedLabel,
	{"test.yml", "chart-started"}:                          hostedLabel,
	{"test.yml", "test"}:                                   hostedLabel,
	{"live-e2e.yml", "changes"}:                            hostedLabel,
	{"live-e2e.yml", "metrics-executed-proof-run"}:         hostedLabel,
	{"live-e2e.yml", "metrics-executed-proof"}:             hostedLabel,
	{"live-e2e.yml", "live-e2e"}:                           hostedLabel,
	{"docker-images.yml", "go-build"}:                      hostedLabel,
	{"docker-images.yml", "go-build-dho-arm64"}:            hostedLabel,
	{"go.yml", "go-container-reproducibility"}:             hostedLabel,
	{"go.yml", "go-arm64-numeric-parity"}:                  hostedArmLabel,
	{"arc-runner-image.yml", "build"}:                      hostedLabel,
	{"go-quality.yml", "go-quality-plan"}:                  hostedLabel,
	{"live-e2e.yml", "live-e2e-run"}:                       hostedLabel,
	{"docker-images.yml", "cancel-stacked-queue-children"}: hostedLabel,
}

// setupGoCacheOn: the setup-go steps whose cache the pool used to route; hosted, the cache input is a literal true or false.
var setupGoCacheOn = [][2]string{{"test.yml", "chart-started"}, {"live-e2e.yml", "metrics-executed-proof-run"}, {"go-quality.yml", "go-quality-plan"}, {"go.yml", "go-arm64-numeric-parity"}}

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

	// draft gate or heavy gate on each pinned job, and the pinned set equals the found set
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
	for _, p := range sortedPairs(heavyGatedJobs) {
		pinned[p] = true
		if j, ok := job(p[0], p[1]); ok {
			v, gate := str(j["if"]), heavyGatedJobs[p]
			if !(v == gate || strings.HasPrefix(v, gate+" && (")) {
				add("%s:%s lost the heavy gate (merge queue, main push, ci:full pull request): got %q, want it to start with %q", p[0], p[1], v, gate)
			}
		}
	}
	found := map[[2]string]bool{}
	for _, name := range workflowNames(t, root) {
		jobs := jobsOf(doc(name))
		for _, jn := range sortedKeys(jobs) {
			if strings.Contains(strings.ReplaceAll(str(asMap(jobs[jn])["if"]), ciFullLabelEvent, ""), "'ci:full'") {
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
		add("ci:full-gated job set drifted: unpinned %v; pinned but not gated %v", unpinned, notGated)
	}

	// no workflow value reads a dead routing variable or names a self-hosted label
	for _, name := range workflowNames(t, root) {
		stringValues(doc(name), "", func(path, v string) {
			for _, tok := range deadRunnerTokens {
				if strings.Contains(v, tok) {
					add("%s%s: %s is retired with the ARC pool: %q", name, path, tok, v)
				}
			}
			if (strings.HasSuffix(path, ".runs-on") || strings.Contains(path, ".runs-on[") || strings.HasSuffix(path, ".runner")) && strings.Contains(v, "self-hosted") {
				add("%s%s: a self-hosted runner label is back: %q", name, path, v)
			}
		})
	}

	// the formerly routed jobs run on the exact hosted label
	for _, p := range sortedPairs(hostedJobs) {
		if j, ok := job(p[0], p[1]); ok {
			if got := str(j["runs-on"]); got != hostedJobs[p] {
				add("%s:%s runs-on is not exactly %q: got %q", p[0], p[1], hostedJobs[p], got)
			}
		}
	}
	if j, ok := job("docker-images.yml", "go-api-tools-build"); ok {
		n := 0
		for _, e := range asList(asMap(asMap(j["strategy"])["matrix"])["include"]) {
			if m := asMap(e); asString(m["platform"]) == "linux/arm64" {
				n++
				if got := str(m["runner"]); got != hostedLabel {
					add("docker-images.yml:go-api-tools-build linux/arm64 runner is not exactly %q: got %q", hostedLabel, got)
				}
			}
		}
		if n != 1 {
			add("docker-images.yml:go-api-tools-build: expected exactly one linux/arm64 include, found %d", n)
		}
	}

	// setup-go keeps its own cache where the pool used to turn it off
	for _, p := range setupGoCacheOn {
		j, ok := job(p[0], p[1])
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
		if _, ok := cache.(bool); !ok {
			add("%s:%s: setup-go cache must be a literal true or false on the hosted runner, got %v", p[0], p[1], cache)
		}
	}

	// the deleted pool-only jobs stay deleted
	for _, p := range [][2]string{{"go.yml", "dind-smoke-test"}, {"go.yml", "go-container-reproducibility-self-hosted"}, {"docker-images.yml", "dind-smoke-test"}} {
		if jobsOf(doc(p[0]))[p[1]] != nil {
			add("%s:%s is a pool-only job and must stay deleted", p[0], p[1])
		}
	}
	return out
}

func sortedPairs(m map[[2]string]string) [][2]string {
	out := make([][2]string, 0, len(m))
	for p := range m {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i][0]+":"+out[i][1] < out[j][0]+":"+out[j][1] })
	return out
}

func toAnyMap(m map[string][]string) yamlMap {
	o := yamlMap{}
	for k := range m {
		o[k] = nil
	}
	return o
}

func TestHostedRunnersOnly(t *testing.T) {
	expectNoViolations(t, runnerAllowlistViolations(t, repoRoot(t)))
}

func TestHostedRunnersOnlyPlantedDefects(t *testing.T) {
	t.Parallel()
	root := repoRoot(t)
	var files []string
	for _, n := range workflowNames(t, root) {
		files = append(files, ".github/workflows/"+n)
	}
	const (
		test_  = ".github/workflows/test.yml"
		goYml  = ".github/workflows/go.yml"
		gq     = ".github/workflows/go-quality.yml"
		dock   = ".github/workflows/docker-images.yml"
		live   = ".github/workflows/live-e2e.yml"
		secscn = ".github/workflows/security-scan.yml"
	)
	pool := `${{ (vars.SELF_HOSTED_RUNNERS == 'enabled' && github.event_name == 'push') && fromJSON('["self-hosted","oci-arc-runners"]') || 'ubuntu-latest' }}`
	cases := []struct {
		name string
		muts []mutation
		want []string
	}{
		{"the pool ternary returns (test.yml)", []mutation{{test_, "    runs-on: ubuntu-latest\n", "    runs-on: " + pool + "\n"}}, []string{"test.yml", "SELF_HOSTED_RUNNERS is retired"}},
		{"a job lists the self-hosted labels (go.yml)", []mutation{{goYml, "    runs-on: ubuntu-24.04-arm\n", "    runs-on: [self-hosted, linux]\n"}}, []string{"go.yml", "self-hosted runner label is back"}},
		{"the race pool variable returns (go-quality.yml)", []mutation{{gq, "      CHECK_GO_JOB_CAP_MIN: 45\n", "      CHECK_GO_JOB_CAP_MIN: ${{ vars.CI_RACE_RUNNER != '' && 45 || 45 }}\n"}}, []string{"go-quality.yml", "CI_RACE_RUNNER is retired"}},
		{"the image variable returns (docker-images.yml)", []mutation{{dock, "    needs: [changes, resolve]\n", "    needs: [changes, resolve]\n    env:\n      PLANTED: ${{ vars.SELF_HOSTED_IMAGE_BUILDS }}\n"}}, []string{"docker-images.yml", "SELF_HOSTED_IMAGE_BUILDS is retired"}},
		{"the pool cache path returns (live-e2e.yml)", []mutation{{live, "          cache: false\n", "          cache: false\n        env:\n          GOMODCACHE: /mnt/go-cache/mod\n"}}, []string{"live-e2e.yml", "/mnt/go-cache is retired"}},
		{"setup-go cache is a pool expression again (test.yml)", []mutation{{test_, "          cache: false\n", "          cache: ${{ !(vars.SELF_HOSTED_RUNNERS == 'enabled') }}\n"}}, []string{"test.yml:chart-started", "setup-go cache must be a literal"}},
		{"arm64 parity falls back to an x64 runner", []mutation{{goYml, "    runs-on: ubuntu-24.04-arm\n", "    runs-on: ubuntu-latest\n"}}, []string{"go.yml:go-arm64-numeric-parity", "runs-on is not exactly"}},
		{"a deleted pool-only job returns", []mutation{{goYml, "  go-container-reproducibility:\n", "  dind-smoke-test:\n    runs-on: ubuntu-latest\n    steps:\n      - run: docker info\n  go-container-reproducibility:\n"}}, []string{"go.yml:dind-smoke-test", "must stay deleted"}},
		{"a heavy job loses its heavy gate", []mutation{{goYml, "    if: >-\n      " + heavyGate + " &&\n      " + goDocsSkip + "\n", "    if: >-\n      " + goDocsSkip + "\n"}}, []string{"go.yml", "lost the heavy gate"}},
		{"a job gains ci:full without being pinned", []mutation{{test_, "|| needs.changes.outputs.code == 'true'\n", "|| needs.changes.outputs.code == 'true' || contains(github.event.pull_request.labels.*.name, 'ci:full')\n"}}, []string{"ci:full-gated job set drifted"}},
		{"a draft-gated job is renamed away", []mutation{{secscn, "  semgrep:\n", "  semgrep-renamed:\n"}}, []string{"security-scan.yml", "semgrep"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tree := plantTree(t, root, files, c.muts...)
			expectViolation(t, runnerAllowlistViolations(t, tree), c.want...)
		})
	}
}

func toAnyMap2(m map[string]string) yamlMap {
	o := yamlMap{}
	for k := range m {
		o[k] = nil
	}
	return o
}
