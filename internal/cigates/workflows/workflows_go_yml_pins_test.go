package workflows_test

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
)

// Ported from tests/tooling (test_go_self_hosted_setup_go_cache_disabled, test_go_arm64_numeric_parity_inner_test_timeout,
// test_go_arm64_and_docker_build_pool_routing, test_go_storage_and_reproducibility_pilot), re-cut when the ARC pool was
// retired (D6994): the arm64 parity job runs on the hosted arm64 runner, the dho arm64 image build and the reproducibility
// job run hosted, and the pool-only jobs (dind-smoke-test, the reproducibility twin) stay deleted. Each guard is a
// function of parsed workflow documents, so a constructed document (a decoy job) or a mutated copy of the real file
// plants one defect.

const eligibilityCondition = "github.event_name == 'workflow_dispatch' || " +
	"github.event_name == 'release' || " +
	"(github.event_name == 'merge_group' && needs.changes.outputs.docs_only != 'true') || " +
	"(github.event_name == 'push' && github.ref == 'refs/heads/main') || " +
	"needs.changes.outputs.code == 'true'"

const draftGatePrefix = "(!(github.event_name == 'pull_request' && github.event.pull_request.draft && " +
	"!contains(github.event.pull_request.labels.*.name, 'ci:full'))) && ("

// goHeavyGatePrefix: go.yml's heavy jobs run in the merge queue, on the main push and on a pull request only with ci:full,
// and a docs-only change skips them. imageHeavyGatePrefix: docker-images.yml's builds, which a non-draft hotfix-image
// pull request also runs (it publishes tree-<tree> images for promotion).
const goHeavyGatePrefix = "(!(github.event_name == 'pull_request' && !contains(github.event.pull_request.labels.*.name, 'ci:full'))) && " +
	"needs.go-storage-integration-plan.outputs.docs_only != 'true' && ("

const imageHeavyGatePrefix = "(!(github.event_name == 'pull_request' && !contains(github.event.pull_request.labels.*.name, 'ci:full') && " +
	"(github.event.pull_request.draft || !contains(github.event.pull_request.labels.*.name, 'hotfix-image')))) && ("

func withoutDraftGate(v string) string {
	for _, prefix := range []string{draftGatePrefix, goHeavyGatePrefix, imageHeavyGatePrefix} {
		if strings.HasPrefix(v, prefix) && strings.HasSuffix(v, ")") {
			return v[len(prefix) : len(v)-1]
		}
	}
	return v
}

func pinJob(doc yamlMap, name string) yamlMap { return asMap(jobsOf(doc)[name]) }

func pinNeeds(job yamlMap) []string {
	switch n := job["needs"].(type) {
	case string:
		return []string{n}
	case []any:
		var out []string
		for _, x := range n {
			out = append(out, fmt.Sprint(x))
		}
		return out
	}
	return nil
}

func stepNamed(job yamlMap, name string) (yamlMap, int) {
	var found yamlMap
	n := 0
	for _, s := range stepsOf(job) {
		if asString(s["name"]) == name {
			found = s
			n++
		}
	}
	return found, n
}

func isSelfHosted(runsOn any) bool {
	switch v := runsOn.(type) {
	case string:
		return strings.Contains(v, "self-hosted")
	case []any:
		for _, l := range v {
			if strings.Contains(asString(l), "self-hosted") {
				return true
			}
		}
	case yamlMap:
		return isSelfHosted(v["labels"])
	}
	return false
}

func isSelfHostedARC(runsOn any) bool {
	switch v := runsOn.(type) {
	case []any:
		have := map[string]bool{}
		for _, l := range v {
			have[asString(l)] = true
		}
		return have["self-hosted"] && have["oci-arc-runners"]
	case string:
		return strings.Contains(v, "self-hosted") && strings.Contains(v, "oci-arc-runners")
	}
	return false
}

// selfHostedSetupGoDefects: every setup-go step of a self-hosted job sets `cache` to the literal boolean false.
func selfHostedSetupGoDefects(doc yamlMap) []string {
	var out []string
	for _, name := range sortedKeys(jobsOf(doc)) {
		job := asMap(jobsOf(doc)[name])
		if !isSelfHosted(job["runs-on"]) {
			continue
		}
		for _, step := range stepsOf(job) {
			if !strings.HasPrefix(asString(step["uses"]), "actions/setup-go@") {
				continue
			}
			if v, ok := asMap(step["with"])["cache"].(bool); !ok || v {
				out = append(out, fmt.Sprintf("job %q runs on self-hosted and its actions/setup-go step has cache %v (want the literal YAML boolean false): the pool shares GOMODCACHE/GOCACHE on a hostPath", name, asMap(step["with"])["cache"]))
			}
		}
	}
	return out
}

var (
	envAssignment    = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*=`)
	leadingKeywords  = map[string]bool{"if": true, "elif": true, "while": true, "until": true, "!": true}
	goTestTimeoutRe  = regexp.MustCompile(`go test` + pyAfterLine + `[^\n]*?-timeout[ =](` + pyNotS + `+)`)
	trailingBackslsh = regexp.MustCompile(`\\+$`)
)

// leadingCommandWords: the first count command words of a script line after env-assignment prefixes and control-flow
// keywords; a comment line yields nil. Tokens split on whitespace and keep their quotes, so a quoted lookalike of an
// assignment stays a command name.
func leadingCommandWords(line string, count int) []string {
	// shlex splits on ' \t\r\n' only, and a '#' starts a comment only at the start of a word: a non-breaking space or other
	// Unicode space stays inside its word, exactly as in the python guard.
	if strings.HasPrefix(strings.TrimLeft(line, " \t\r\n"), "#") {
		return nil
	}
	line = strings.TrimRightFunc(line, isPySpace)
	if m := trailingBackslsh.FindString(line); len(m)%2 == 1 {
		line = strings.TrimSuffix(line, `\`)
	}
	tokens := strings.FieldsFunc(line, func(r rune) bool { return r == ' ' || r == '\t' || r == '\r' || r == '\n' })
	for len(tokens) > 0 && (leadingKeywords[tokens[0]] || envAssignment.MatchString(tokens[0])) {
		tokens = tokens[1:]
	}
	if len(tokens) < count {
		return nil
	}
	return tokens[:count]
}

func theGoTestLine(script string) (string, error) {
	var matches []string
	for _, l := range strings.Split(script, "\n") {
		w := leadingCommandWords(l, 2)
		if len(w) == 2 && w[0] == "go" && w[1] == "test" {
			matches = append(matches, l)
		}
	}
	if len(matches) != 1 {
		return "", fmt.Errorf("%d lines are themselves a `go test` invocation, expected exactly 1", len(matches))
	}
	return matches[0], nil
}

// innerTimeoutDefect: the one go test line carries `-timeout <N>m` strictly shorter than the job cap.
func innerTimeoutDefect(script string, outer int) string {
	line, err := theGoTestLine(script)
	if err != nil {
		return err.Error()
	}
	m := goTestTimeoutRe.FindStringSubmatch(line)
	if m == nil {
		return "the go test invocation has no `-timeout` flag"
	}
	raw := m[1]
	if !strings.HasSuffix(raw, "m") {
		return fmt.Sprintf("go test -timeout value %q is not a plain <N>m form", raw)
	}
	inner, ok := pyAtoi(strings.TrimSuffix(raw, "m"))
	if !ok {
		return fmt.Sprintf("go test -timeout value %q is not a plain <N>m form", raw)
	}
	if inner >= outer {
		return fmt.Sprintf("inner go test -timeout (%dm) is not strictly shorter than the job timeout-minutes (%dm)", inner, outer)
	}
	return ""
}

func TestSelfHostedJobsDisableSetupGoCache(t *testing.T) {
	root := rootOf(t)
	if d := selfHostedSetupGoDefects(loadWf(t, root, "go.yml")); len(d) > 0 {
		t.Fatalf("%s", strings.Join(d, "\n"))
	}
	setupGo := func(runsOn any, with yamlMap) yamlMap {
		return yamlMap{"jobs": yamlMap{"j": yamlMap{"runs-on": runsOn, "steps": []any{yamlMap{"uses": "actions/setup-go@deadbeef", "with": with}}}}}
	}
	for _, c := range []struct {
		name string
		doc  yamlMap
		want int
	}{
		{"self-hosted with cache left on by default", setupGo([]any{"self-hosted", "oci-arc-runners"}, yamlMap{"go-version-file": "go.mod"}), 1},
		{"a truthy string lookalike", setupGo("self-hosted", yamlMap{"cache": "false"}), 1},
		{"a hosted job with cache on", setupGo("ubuntu-latest", yamlMap{"go-version-file": "go.mod"}), 0},
		{"self-hosted with cache disabled", setupGo([]any{"self-hosted", "oci-arc-runners"}, yamlMap{"cache": false}), 0},
	} {
		if got := selfHostedSetupGoDefects(c.doc); len(got) != c.want {
			t.Errorf("%s: %d defects %v, want %d", c.name, len(got), got, c.want)
		}
	}
}

func TestArm64ParityGoTestCarriesAnInnerTimeoutShorterThanTheJobCap(t *testing.T) {
	root := rootOf(t)
	const jobName, stepName = "go-arm64-numeric-parity", "Run FMA bit-pattern goldens on real arm64"
	job := pinJob(loadWf(t, root, "go.yml"), jobName)
	if job == nil {
		t.Fatalf("job %s not found", jobName)
	}
	step, n := stepNamed(job, stepName)
	if n != 1 {
		t.Fatalf("job %s must have exactly one step named %q (found %d)", jobName, stepName, n)
	}
	outer, ok := job["timeout-minutes"].(int)
	if !ok {
		t.Fatalf("job %s has no integer timeout-minutes", jobName)
	}
	if d := innerTimeoutDefect(asString(step["run"]), outer); d != "" {
		t.Fatal(d)
	}
	for _, c := range []struct{ name, script, want string }{
		{"a timeout mentioned only in a comment", "# required form: go test -timeout 8m\ngo test -mod=readonly -count=1 ./internal/jobs/... -run pattern\n", "no `-timeout` flag"},
		{"a timeout decoy inside a dead branch", "if false; then\n  go test -timeout 8m ./x -run p\nfi\nCI_STAGE=arm64 go test -timeout 8m ./x -run p\n", "expected exactly 1"},
		{"a quoted assignment lookalike beside a real if invocation", "if false; then\n  'CI_STAGE=arm64' go test -timeout 8m ./x -run p\nfi\nif go test -timeout 8m ./x -run p; then :; fi\n", ""},
		{"an inner timeout as long as the cap", "go test -timeout 10m ./x\n", "not strictly shorter"},
	} {
		got := innerTimeoutDefect(c.script, 10)
		if c.want == "" && got != "" || c.want != "" && !strings.Contains(got, c.want) {
			t.Errorf("%s: got %q, want containing %q", c.name, got, c.want)
		}
	}
	if line, err := theGoTestLine("if false; then\n  'CI_STAGE=arm64' go test -timeout 8m ./x\nfi\nif go test -timeout 8m ./x; then :; fi\n"); err != nil || !strings.HasPrefix(strings.TrimSpace(line), "if go test") {
		t.Errorf("the real conditional invocation was not selected: %q %v", line, err)
	}
}

// poolRoutingDefects pins the arm64 parity job (go.yml) to the hosted arm64 runner and the dho arm64 image build
// (docker-images.yml) to the hosted x64 runner with QEMU.
func poolRoutingDefects(goDoc, dockerDoc yamlMap) []string {
	var out []string
	add := func(f string, a ...any) { out = append(out, fmt.Sprintf(f, a...)) }
	const dind, build, merge = "dind-smoke-test", "go-build-dho-arm64", "go-merge"

	parity := pinJob(goDoc, "go-arm64-numeric-parity")
	if got := asString(parity["if"]); got != strings.TrimSuffix(draftGatePrefix, " && (") {
		add("go-arm64-numeric-parity if %q is not exactly the draft gate", got)
	}
	if got := parity["runs-on"]; got != "ubuntu-24.04-arm" {
		add("go-arm64-numeric-parity runs-on %v is not the hosted arm64 runner ubuntu-24.04-arm", got)
	}
	if contains(pinNeeds(parity), dind) {
		add("go-arm64-numeric-parity needs %s but never uses Docker", dind)
	}
	if parity["timeout-minutes"] != 10 {
		add("go-arm64-numeric-parity timeout-minutes %v is not 10", parity["timeout-minutes"])
	}
	noPoll := func(label string, job yamlMap) {
		if contains(pinNeeds(job), "pick-runner") {
			add("%s still needs pick-runner", label)
		}
		for _, s := range stepsOf(job) {
			run := asString(s["run"])
			if strings.Contains(run, "job_status()") || strings.Contains(run, "run_here") {
				add("%s still has the fallback poller", label)
			}
		}
	}
	noPoll("go-arm64-numeric-parity", parity)

	db := pinJob(dockerDoc, build)
	expectedIf := "always() && needs.changes.result == 'success' && needs.resolve.result == 'success' && needs.resolve.outputs.promoted != 'true' && (" + eligibilityCondition + ")"
	if got := withoutDraftGate(asString(db["if"])); got != expectedIf {
		add("%s if %q is not the eligibility condition", build, got)
	}
	if got := asString(db["runs-on"]); got != "ubuntu-latest" {
		add("%s runs-on %q is not the hosted runner ubuntu-latest", build, got)
	}
	if _, n := stepNamed(db, "Set up QEMU"); n == 0 {
		add("%s must set up QEMU: the hosted x64 runner cross-builds linux/arm64", build)
	}
	if contains(pinNeeds(db), dind) {
		add("%s needs the deleted %s", build, dind)
	}
	if db["timeout-minutes"] != 25 {
		add("%s timeout-minutes %v is not 25", build, db["timeout-minutes"])
	}
	step, _ := stepNamed(db, "Build and push by digest")
	st, ok1 := step["timeout-minutes"].(int)
	jt, ok2 := db["timeout-minutes"].(int)
	if !ok1 || !ok2 || st >= jt {
		add("%s step timeout %v is not strictly shorter than the job timeout %v", build, step["timeout-minutes"], db["timeout-minutes"])
	}
	noPoll(build, db)

	excluded := false
	for _, e := range asList(asMap(asMap(pinJob(dockerDoc, "go-build")["strategy"])["matrix"])["exclude"]) {
		m := asMap(e)
		if asString(m["target"]) == "dho" && asString(m["platform"]) == "linux/arm64" {
			excluded = true
		}
	}
	if !excluded {
		add("go-build matrix must exclude (dho, linux/arm64): the dedicated leg builds it")
	}
	for _, doc := range []yamlMap{goDoc, dockerDoc} {
		if _, has := jobsOf(doc)[dind]; has {
			add("the pool-only %s job is back", dind)
		}
	}

	gm := pinJob(dockerDoc, merge)
	if !contains(pinNeeds(gm), build) {
		add("%s must need %s", merge, build)
	}
	gif := asString(gm["if"])
	if !strings.Contains(gif, "always()") || !strings.Contains(gif, "needs."+build+".result != 'failure'") || !strings.Contains(gif, "needs."+build+".result != 'cancelled'") {
		add("%s if %q must use always() and tolerate %s skipping while rejecting a failure or cancellation", merge, gif, build)
	}
	return out
}

func TestPoolRoutingOfArm64ParityAndDhoImageBuild(t *testing.T) {
	root := rootOf(t)
	goDoc, dockerDoc := loadWf(t, root, "go.yml"), loadWf(t, root, "docker-images.yml")
	if d := poolRoutingDefects(goDoc, dockerDoc); len(d) > 0 {
		t.Fatalf("%s", strings.Join(d, "\n"))
	}
	files := []string{".github/workflows/go.yml", ".github/workflows/docker-images.yml"}
	for _, m := range []mutation{
		{files[0], "runs-on: ubuntu-24.04-arm\n    timeout-minutes: 10", "runs-on: ubuntu-24.04-arm\n    timeout-minutes: 11"},
		{files[0], "runs-on: ubuntu-24.04-arm\n", "runs-on: ubuntu-latest\n"},
		{files[1], "timeout-minutes: 25", "timeout-minutes: 26"},
	} {
		tmp := plantTree(t, root, files, m)
		if d := poolRoutingDefects(loadWf(t, tmp, "go.yml"), loadWf(t, tmp, "docker-images.yml")); len(d) == 0 {
			t.Errorf("mutation %q -> %q in %s was not caught", m.old, m.new, m.file)
		}
	}
}

// pilotDefects pins go-container-reproducibility (hosted, the only leg now) and the hosted-only storage shards.
func pilotDefects(doc yamlMap) []string {
	var out []string
	add := func(f string, a ...any) { out = append(out, fmt.Sprintf(f, a...)) }
	const plan, shard, shardRemoved, agg = "go-storage-integration-plan", "go-storage-integration-shard", "go-storage-integration-shard-self-hosted", "go-storage-integration"
	const hosted = "go-container-reproducibility"

	repro := pinJob(doc, hosted)
	if got, want := asString(repro["if"]), strings.TrimSuffix(goHeavyGatePrefix, " && ("); got != want {
		add("%s if %q is not exactly the heavy gate %q", hosted, got, want)
	}
	if repro["runs-on"] != "ubuntu-latest" {
		add("%s must run on ubuntu-latest, got %v", hosted, repro["runs-on"])
	}
	if repro["name"] != hosted {
		add("%s must keep its required-check name, got %v", hosted, repro["name"])
	}
	for _, j := range []string{"go-container-reproducibility-self-hosted", "dind-smoke-test"} {
		if _, has := jobsOf(doc)[j]; has {
			add("the pool-only %s job is back", j)
		}
	}
	if _, has := repro["concurrency"]; has {
		add("%s has a job-level concurrency group (tried and reverted: a required check was cancelled)", hosted)
	}
	if _, has := jobsOf(doc)["pick-runner"]; has {
		add("a pick-runner job still exists")
	}
	ss := pinJob(doc, shard)
	wantIf := "needs.go-storage-integration-plan.result == 'success' && needs.go-storage-integration-plan.outputs.legs != '0'"
	if got, has := ss["if"]; has && got != wantIf || strings.Contains(asString(ss["if"]), "SELF_HOSTED_RUNNERS") {
		add("%s carries an unexpected if %v", shard, ss["if"])
	}
	if ss["runs-on"] != "ubuntu-latest" {
		add("%s must run on ubuntu-latest, got %v", shard, ss["runs-on"])
	}
	if _, has := jobsOf(doc)[shardRemoved]; has {
		add("%s still exists", shardRemoved)
	}
	needs := pinNeeds(pinJob(doc, agg))
	if len(needs) != 2 || !contains(needs, plan) || !contains(needs, shard) {
		add("%s must need exactly %s and %s, got %v", agg, plan, shard, needs)
	}
	return out
}

func TestReproducibilityAndStorageShardPins(t *testing.T) {
	root := rootOf(t)
	if d := pilotDefects(loadWf(t, root, "go.yml")); len(d) > 0 {
		t.Fatalf("%s", strings.Join(d, "\n"))
	}
	f := ".github/workflows/go.yml"
	for _, m := range []mutation{
		{f, "  go-container-reproducibility:\n", "  go-container-reproducibility-self-hosted:\n    runs-on: ubuntu-latest\n    steps:\n      - run: true\n  go-container-reproducibility:\n"},
		{f, "      needs.go-storage-integration-plan.outputs.docs_only != 'true'\n    runs-on: ubuntu-latest\n    timeout-minutes: 25", "      needs.go-storage-integration-plan.outputs.docs_only != 'true'\n    runs-on: ubuntu-latest\n    concurrency: repro\n    timeout-minutes: 25"},
	} {
		if m.new == "" {
			continue
		}
		tmp := plantTree(t, root, []string{f}, m)
		if d := pilotDefects(loadWf(t, tmp, "go.yml")); len(d) == 0 {
			t.Errorf("mutation %q -> %q was not caught", m.old, m.new)
		}
	}
}
