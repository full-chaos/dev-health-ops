package cigates_test

import (
	"fmt"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// CHAOS-10177: Go port of the YAML-shape tests of tests/tooling/test_go_auth_service_workflow.py (CHAOS-4887).
// The gate logic is `ci/check_go.sh auth`; the workflow only schedules it. What can rot silently: a job reusing a
// required context name (CHAOS-4834 class), the fuzz matrix drifting from the registry in check_go.sh, a fan-in
// that stops failing on a skipped relevant job. The exec test test_closure_check_executes_and_fails_on_a_forbidden_or_stale_module
// runs `go list` over four build configurations and is not a pure file read: it moves to PR C (script contracts).

var authFiles = []string{".github/workflows/go-auth-service.yml", "ci/check_go.sh", "ci/auth_closure_allow.tsv", "docker/go-worker.Dockerfile"}

var requiredContexts = map[string]bool{"test": true, "lint": true, "go-quality": true, "metrics-executed-proof": true}

var (
	authFuzzBlock  = regexp.MustCompile(`(?s)AUTH_FUZZ_TARGETS=\((.*?)\n\)`)
	authFuzzRow    = regexp.MustCompile(`"\S+ (Fuzz\w+)"`)
	authStageCall  = regexp.MustCompile(`^\s*bash ci/check_go\.sh auth ([a-z-]+)`)
	authClosureFn  = regexp.MustCompile(`(?s)check_auth_closure\(\) \{(.*?)\n\}`)
	authStagesWant = []string{"fmt", "vet", "test", "race", "fuzz", "history", "vuln", "middleware", "fuzz-registry", "paths-selftest", "scope-selftest"}
)

func authViolations(t *testing.T, root string) []string {
	t.Helper()
	wfText := readFile(t, filepath.Join(root, ".github/workflows/go-auth-service.yml"))
	checkGo := readFile(t, filepath.Join(root, "ci/check_go.sh"))
	doc := parseYAML(t, wfText, "go-auth-service.yml")
	jobs := jobsOf(doc)
	var out []string

	for _, key := range sortedKeys(jobs) {
		name := key
		if n := asString(asMap(jobs[key])["name"]); n != "" {
			name = n
		}
		if requiredContexts[key] || requiredContexts[name] {
			out = append(out, fmt.Sprintf("job %s reuses a required context name (CHAOS-4834 class: a second producer of a required check)", key))
		}
		if !strings.HasPrefix(key, "auth-") {
			out = append(out, fmt.Sprintf("job %s must carry the auth- prefix", key))
		}
	}

	for event, cfg := range asMap(doc["on"]) {
		if m := asMap(cfg); m != nil {
			if _, p := m["paths"]; p {
				out = append(out, fmt.Sprintf("trigger %s has a paths filter: relevance is decided inside (auth-changes)", event))
			}
			if _, p := m["paths-ignore"]; p {
				out = append(out, fmt.Sprintf("trigger %s has a paths-ignore filter: relevance is decided inside (auth-changes)", event))
			}
		}
	}

	var matrix []string
	for _, x := range asList(asMap(asMap(asMap(jobs["auth-fuzz"])["strategy"])["matrix"])["target"]) {
		matrix = append(matrix, asString(x))
	}
	var registry []string
	if blk := authFuzzBlock.FindStringSubmatch(checkGo); blk == nil {
		out = append(out, "AUTH_FUZZ_TARGETS is no longer one literal array in ci/check_go.sh")
	} else {
		for _, m := range authFuzzRow.FindAllStringSubmatch(blk[1], -1) {
			registry = append(registry, m[1])
		}
	}
	sm, sr := append([]string{}, matrix...), append([]string{}, registry...)
	sort.Strings(sm)
	sort.Strings(sr)
	if !reflect.DeepEqual(sm, sr) {
		out = append(out, fmt.Sprintf("the auth-fuzz matrix %v differs from the AUTH_FUZZ_TARGETS registry %v: every registered parser needs its own CI-visible fuzz job, and no job may name a missing target", sm, sr))
	}
	uniq := map[string]bool{}
	for _, m := range matrix {
		uniq[m] = true
	}
	if len(uniq) != len(matrix) {
		out = append(out, "the auth-fuzz matrix repeats a target")
	}

	for _, key := range []string{"auth-go-quality", "auth-test", "auth-fuzz"} {
		j := asMap(jobs[key])
		if !reflect.DeepEqual(asList(j["needs"]), []any{"auth-changes"}) {
			out = append(out, fmt.Sprintf("%s.needs must be [auth-changes], got %v", key, j["needs"]))
		}
		if !strings.Contains(asString(j["if"]), "relevant == 'true'") {
			out = append(out, fmt.Sprintf("%s.if must gate on relevant == 'true', got %q", key, asString(j["if"])))
		}
	}
	gate := asMap(jobs["auth-gate"])
	if asString(gate["if"]) != "always()" {
		out = append(out, fmt.Sprintf("auth-gate.if must be always(), got %q", asString(gate["if"])))
	}
	var needs []string
	for _, n := range asList(gate["needs"]) {
		needs = append(needs, asString(n))
	}
	sort.Strings(needs)
	if !reflect.DeepEqual(needs, []string{"auth-changes", "auth-fuzz", "auth-go-quality", "auth-test"}) {
		out = append(out, fmt.Sprintf("auth-gate.needs must fan in auth-changes, auth-go-quality, auth-test, auth-fuzz, got %v", needs))
	}
	gateSteps := stepsOf(gate)
	if len(gateSteps) == 0 || !strings.Contains(asString(gateSteps[0]["run"]), `[ "${pair#*=}" = "success" ]`) {
		out = append(out, "auth-gate must fail a relevant change whose gate job did not succeed (failed, cancelled OR skipped)")
	}

	testSteps := stepsOf(jobs["auth-test"])
	if len(testSteps) == 0 || fmt.Sprint(asMap(testSteps[0]["with"])["fetch-depth"]) != "0" {
		out = append(out, "auth-test must check out with fetch-depth 0: the source commit history check needs history")
	}
	history := false
	for _, s := range testSteps {
		if asString(s["run"]) == "bash ci/check_go.sh auth history" {
			history = true
		}
	}
	if !history {
		out = append(out, "auth-test must run `bash ci/check_go.sh auth history`")
	}
	for _, key := range []string{"auth-go-quality", "auth-fuzz"} {
		for _, s := range stepsOf(jobs[key]) {
			if strings.Contains(asString(s["run"]), "auth history") {
				out = append(out, fmt.Sprintf("%s runs the history check where history is not fetched", key))
			}
		}
	}

	stages := map[string]bool{}
	for _, jn := range sortedKeys(jobs) {
		for _, st := range stepsOf(jobs[jn]) {
			for _, m := range authStageCall.FindAllStringSubmatch(str(st["run"]), 1) {
				stages[m[1]] = true
			}
		}
	}
	for _, w := range authStagesWant {
		if !stages[w] {
			out = append(out, fmt.Sprintf("the workflow no longer calls auth stage %q", w))
		}
	}
	for st := range stages {
		if !regexp.MustCompile(`(?m)^    ` + regexp.QuoteMeta(st) + `\) `).MatchString(checkGo) {
			out = append(out, fmt.Sprintf("the workflow calls auth stage %q that ci/check_go.sh does not define", st))
		}
	}

	closure := false
	for _, s := range testSteps {
		if asString(s["run"]) == "bash ci/check_go.sh auth closure" {
			closure = true
		}
	}
	if !closure {
		out = append(out, "auth-test must schedule `bash ci/check_go.sh auth closure`")
	}
	rows := 0
	for _, line := range strings.Split(readFile(t, filepath.Join(root, "ci/auth_closure_allow.tsv")), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		rows++
		f := strings.Split(line, "\t")
		if len(f) != 2 || strings.TrimSpace(f[1]) == "" {
			out = append(out, fmt.Sprintf("auth_closure_allow.tsv row %q needs <module><TAB><reason>", line))
		}
		if strings.Contains(strings.ToLower(f[0]), "clickhouse") {
			out = append(out, fmt.Sprintf("auth_closure_allow.tsv allows %q: ClickHouse must never be in the auth closure", f[0]))
		}
	}
	if rows == 0 {
		out = append(out, "auth_closure_allow.tsv has no rows")
	}

	if !strings.Contains(readFile(t, filepath.Join(root, "docker/go-worker.Dockerfile")), "CGO_ENABLED=0") {
		out = append(out, "docker/go-worker.Dockerfile no longer builds with CGO_ENABLED=0: the closure check models that configuration")
	}
	if body := authClosureFn.FindStringSubmatch(checkGo); body == nil {
		out = append(out, "check_auth_closure is no longer one function")
	} else if !strings.Contains(body[1], "for cgo in 0 1") || !strings.Contains(body[1], "for arch in amd64 arm64") {
		out = append(out, "check_auth_closure no longer covers cgo 0/1 and amd64/arm64")
	}
	return out
}

func TestAuthServiceWorkflow(t *testing.T) {
	expectNoViolations(t, authViolations(t, repoRoot(t)))
}

func TestAuthServiceWorkflowPlantedDefects(t *testing.T) {
	root := repoRoot(t)
	const wf, cg, tsv = ".github/workflows/go-auth-service.yml", "ci/check_go.sh", "ci/auth_closure_allow.tsv"
	cases := []struct {
		name string
		muts []mutation
		want []string
	}{
		{"job reuses required context lint", []mutation{{wf, "\n  auth-go-quality:", "\n  lint:"}}, []string{"required context", "auth- prefix"}},
		{"trigger gains a path filter", []mutation{{wf, "pull_request:", "pull_request:\n    paths: ['internal/auth/**']"}}, []string{"paths filter"}},
		{"registry gains a target with no fuzz job", []mutation{{cg, "AUTH_FUZZ_TARGETS=(\n", "AUTH_FUZZ_TARGETS=(\n  \"./internal/auth/x FuzzPlanted\"\n"}}, []string{"differs from the AUTH_FUZZ_TARGETS registry", "FuzzPlanted"}},
		{"gate stops failing on a skipped job", []mutation{{wf, `[ "${pair#*=}" = "success" ]`, `[ "${pair#*=}" != "failure" ]`}}, []string{"failed, cancelled OR skipped"}},
		{"gate job drops its always()", []mutation{{wf, "if: always()", "if: success()"}}, []string{"always()"}},
		{"the auth race step echoes the command instead of running it", []mutation{{wf, "run: bash ci/check_go.sh auth race\n", "run: 'echo \"run: bash ci/check_go.sh auth race\"'\n"}}, []string{"no longer calls auth stage \"race\""}},
		{"closure allow row loses its reason", []mutation{{tsv, "github.com/jackc/pgx/v5\tACP-ADR-01 item 5: PostgreSQL driver", "github.com/jackc/pgx/v5\t "}}, []string{"needs <module><TAB><reason>"}},
		{"closure allow list admits clickhouse", []mutation{{tsv, "github.com/jackc/pgx/v5\t", "github.com/ClickHouse/clickhouse-go/v2\tplanted\ngithub.com/jackc/pgx/v5\t"}}, []string{"ClickHouse must never"}},
		{"closure check stops covering arm64", []mutation{{cg, "for arch in amd64 arm64", "for arch in amd64"}}, []string{"no longer covers cgo"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tree := plantTree(t, root, authFiles, c.muts...)
			expectViolation(t, authViolations(t, tree), c.want...)
		})
	}
}
