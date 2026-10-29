package workflows_test

import (
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// CHAOS-11596: the external auth corpus (ci/auth_corpus_external.sh, 3 legs) runs on a merge queue entry that touches
// its path set (ci/auth_corpus_paths.txt), as a job of go-auth-service.yml that fans into the required context
// `auth-gate`. These tests pin the job, the fan-in and the path set.

const (
	corpusQueueWorkflow = ".github/workflows/go-auth-service.yml"
	corpusQueuePaths    = "ci/auth_corpus_paths.txt"
	corpusQueueIf       = "github.event_name == 'merge_group' && needs.auth-changes.outputs.corpus == 'true'"
)

var corpusQueueFiles = []string{corpusQueueWorkflow, corpusQueuePaths}

// corpusRowRegexp turns a row of ci/auth_corpus_paths.txt into a regexp: a star crosses slashes, except in a row that
// starts with "=", which matches one directory only (a star stays inside the last path element).
func corpusRowRegexp(row string) *regexp.Regexp {
	star := ".*"
	if rest, flat := strings.CutPrefix(row, "="); flat {
		row, star = rest, "[^/]*"
	}
	return regexp.MustCompile("^" + strings.ReplaceAll(regexp.QuoteMeta(row), `\*`, star) + "$")
}

// corpusRows returns the rows of ci/auth_corpus_paths.txt exactly as written: only an empty line and a line that starts
// with # are skipped. Nothing is trimmed, so a row with trailing whitespace or a CR stays a row (and is a problem).
func corpusRows(t *testing.T, root string) []string {
	t.Helper()
	var rows []string
	for _, line := range strings.Split(readFile(t, filepath.Join(root, corpusQueuePaths)), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		rows = append(rows, line)
	}
	return rows
}

// corpusRowProblem is the model of the script's refusal of a malformed row (ci/auth_corpus_queue_relevance.sh): the
// script prints corpus=true for each row this function reports. It returns "" for a well-formed row.
func corpusRowProblem(row string) string {
	switch {
	case strings.ContainsRune(row, '\r'):
		return "carriage return"
	case row != strings.TrimLeft(row, " \t") || row != strings.TrimRight(row, " \t"):
		return "leading or trailing whitespace"
	case row == "=":
		return "only the = prefix"
	case strings.Contains(row[1:], "="):
		return "= after the first character"
	}
	flat, ok := strings.CutPrefix(row, "=")
	if !ok {
		return ""
	}
	i := strings.LastIndex(flat, "/")
	switch {
	case i < 0:
		return "= row without a slash"
	case i == 0:
		return "= row with an empty directory part"
	case strings.ContainsAny(flat[:i], "*?["):
		return "= row with a glob character in its directory part"
	case strings.Contains(flat, "//"):
		return "= row with an empty path element"
	case i == len(flat)-1:
		return "= row whose last element is empty"
	}
	return ""
}

// authCorpusQueueViolations reads the workflow shape: job, legs, timeouts, no retry, inverted no-keys leg.
func authCorpusQueueViolations(t *testing.T, root string) []string {
	t.Helper()
	text := readFile(t, filepath.Join(root, corpusQueueWorkflow))
	jobs := jobsOf(parseYAML(t, text, "go-auth-service.yml"))
	var out []string
	job := asMap(jobs["auth-corpus-queue"])
	if job == nil {
		return []string{"go-auth-service.yml has no auth-corpus-queue job"}
	}
	if !reflect.DeepEqual(asList(job["needs"]), []any{"auth-changes"}) {
		out = append(out, fmt.Sprintf("auth-corpus-queue.needs must be [auth-changes], got %v", job["needs"]))
	}
	if got := asString(job["if"]); got != corpusQueueIf {
		out = append(out, fmt.Sprintf("auth-corpus-queue.if must be exactly %q, got %q", corpusQueueIf, got))
	}
	if tm, ok := job["timeout-minutes"].(int); !ok || tm <= 0 || tm > 60 {
		out = append(out, fmt.Sprintf("auth-corpus-queue needs timeout-minutes 1..60, got %v", job["timeout-minutes"]))
	}
	strategy := asMap(job["strategy"])
	if ff, ok := strategy["fail-fast"].(bool); !ok || ff {
		out = append(out, "auth-corpus-queue strategy.fail-fast must be false")
	}
	var legs []string
	for _, l := range asList(asMap(strategy["matrix"])["leg"]) {
		legs = append(legs, asString(l))
	}
	if strings.Join(legs, ",") != "go-api,acp,acp-nokeys,acp-social-unowned" {
		out = append(out, fmt.Sprintf("auth-corpus-queue legs must be go-api,acp,acp-nokeys,acp-social-unowned, got %v", legs))
	}
	if _, ok := job["continue-on-error"]; ok {
		out = append(out, "auth-corpus-queue sets continue-on-error: a red leg must stay red")
	}
	start := strings.Index(text, "\n  auth-corpus-queue:")
	end := strings.Index(text[start+1:], "\n  auth-gate:")
	if start < 0 || end < 0 {
		return append(out, "auth-corpus-queue text not found before auth-gate")
	}
	body := shellCode(text[start+1 : start+1+end])
	if re := regexp.MustCompile(`(?i)retry|retries|attempt|nick-fields|continue-on-error|\|\| *true|set \+e`); re.MatchString(body) {
		out = append(out, "auth-corpus-queue has a retry or error-hiding construct: "+re.FindString(body))
	}
	if n := strings.Count(body, `bash ci/auth_corpus_external.sh "${{ matrix.leg }}"`); n != 1 {
		out = append(out, fmt.Sprintf("auth-corpus-queue runs the corpus script more than once or not at all (%d): a second run is a retry", n))
	}
	for _, want := range []string{
		"matrix.leg == 'acp-nokeys'",
		"AUTH_CORPUS_OMIT_AUTH_KEYS=1 bash ci/auth_corpus_external.sh acp",
		`if [ "${status}" -eq 0 ]`,
		"required aggregate sessions is not served by auth-service",
	} {
		if !strings.Contains(body, want) {
			out = append(out, fmt.Sprintf("auth-corpus-queue no-keys leg lacks the inverted exit form %q", want))
		}
	}
	for _, want := range []string{
		"if: matrix.leg != 'acp-nokeys' && matrix.leg != 'acp-social-unowned'",
		"matrix.leg == 'acp-social-unowned'",
		"AUTH_CORPUS_USERS_UNOWNED=1 AUTH_CORPUS_FORCE_SOCIAL=1 bash ci/auth_corpus_external.sh acp",
		"social-login through the provider stub: answered 403 with",
	} {
		if !strings.Contains(body, want) {
			out = append(out, fmt.Sprintf("auth-corpus-queue forced-social leg lacks %q: the leg must be a negative leg (exit 1 plus the by-name line)", want))
		}
	}
	if n := strings.Count(body, `if [ "${status}" -eq 0 ]`); n != 2 {
		out = append(out, fmt.Sprintf("auth-corpus-queue needs the inverted exit check in both negative legs, found %d", n))
	}
	gate := asMap(jobs["auth-gate"])
	var needs []string
	for _, n := range asList(gate["needs"]) {
		needs = append(needs, asString(n))
	}
	if !containsStr(needs, "auth-corpus-queue") {
		out = append(out, "auth-gate.needs must include auth-corpus-queue")
	}
	if got := asString(asMap(asMap(jobs["auth-changes"])["outputs"])["corpus"]); got != "${{ steps.corpus.outputs.corpus }}" {
		out = append(out, fmt.Sprintf("auth-changes must output corpus from the corpus step, got %q", got))
	}
	for _, k := range []string{"CORPUS_RELEVANT: ${{ needs.auth-changes.outputs.corpus }}", "CORPUS: ${{ needs.auth-corpus-queue.result }}"} {
		if !strings.Contains(text, k) {
			out = append(out, "auth-gate env lacks "+k)
		}
	}
	return out
}

func containsStr(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

// authCorpusGateViolations EXECUTES the auth-gate fan-in step over the verdict matrix.
func authCorpusGateViolations(t *testing.T, root string) []string {
	t.Helper()
	jobs := jobsOf(parseYAML(t, readFile(t, filepath.Join(root, corpusQueueWorkflow)), "go-auth-service.yml"))
	steps := stepsOf(jobs["auth-gate"])
	if len(steps) == 0 {
		return []string{"auth-gate has no step"}
	}
	run := asString(steps[0]["run"])
	type c struct {
		name                                                 string
		changes, rel, corpusRel, quality, test, fuzz, corpus string
		wantFail                                             bool
	}
	ok := "success"
	cases := []c{
		{"relevant, corpus relevant and green", ok, "true", "true", ok, ok, ok, ok, false},
		{"corpus relevant but skipped (skip with relevant=true)", ok, "true", "true", ok, ok, ok, "skipped", true},
		{"corpus relevant but failed", ok, "true", "true", ok, ok, ok, "failure", true},
		{"corpus relevant but cancelled", ok, "true", "true", ok, ok, ok, "cancelled", true},
		{"corpus-only change (auth gates not relevant), corpus skipped", ok, "false", "true", "skipped", "skipped", "skipped", "skipped", true},
		{"corpus-only change, corpus green", ok, "false", "true", "skipped", "skipped", "skipped", ok, false},
		{"corpus not relevant, skipped, auth not relevant", ok, "false", "false", "skipped", "skipped", "skipped", "skipped", false},
		{"corpus not relevant, skipped, auth relevant and green", ok, "true", "false", ok, ok, ok, "skipped", false},
		{"no corpus verdict", ok, "false", "", "skipped", "skipped", "skipped", "skipped", true},
		{"relevant auth job red, corpus green", ok, "true", "true", "failure", ok, ok, ok, true},
		{"draft pull request", "skipped", "", "", "skipped", "skipped", "skipped", "skipped", false},
	}
	var out []string
	for _, tc := range cases {
		cmd := exec.Command("bash", "-c", run)
		cmd.Env = []string{"PATH=/usr/bin:/bin", "CHANGES=" + tc.changes, "RELEVANT=" + tc.rel, "CORPUS_RELEVANT=" + tc.corpusRel,
			"QUALITY=" + tc.quality, "TEST=" + tc.test, "FUZZ=" + tc.fuzz, "CORPUS=" + tc.corpus}
		raw, err := cmd.CombinedOutput()
		failed := err != nil
		if failed != tc.wantFail {
			out = append(out, fmt.Sprintf("auth-gate case %q: failed=%v want %v (a skipped corpus leg with relevant=true must fail): %s", tc.name, failed, tc.wantFail, strings.TrimSpace(string(raw))))
		}
	}
	return out
}

// corpusRouteFilesOutsideSet lists every non-test Go file that declares an auth-service route (httpapi.Route{ of internal/auth/httpapi) and
// does not match a pattern of ci/auth_corpus_paths.txt (only directories `scanned` accepts: go-api declares routes with the
// same httpapi package, so the real scan is limited to the import closure of cmd/auth-service); a pattern's literal prefix must exist.
func corpusRouteFilesOutsideSet(t *testing.T, root string, scanned func(dir string) bool) []string {
	t.Helper()
	var pats []*regexp.Regexp
	var out []string
	for _, line := range corpusRows(t, root) {
		pats = append(pats, corpusRowRegexp(line))
		prefix := strings.TrimPrefix(line, "=")
		if i := strings.Index(prefix, "*"); i >= 0 {
			prefix = strings.TrimRight(prefix[:i], "/")
		}
		if _, err := os.Stat(filepath.Join(root, prefix)); err != nil {
			out = append(out, fmt.Sprintf("path set row %q names %q, which does not exist", line, prefix))
		}
	}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && (d.Name() == ".git" || d.Name() == "node_modules" || d.Name() == "vendor") {
			return filepath.SkipDir
		}
		if d.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			return nil
		}
		raw, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		if !strings.Contains(string(raw), `/internal/auth/httpapi"`) || !strings.Contains(string(raw), "httpapi.Route{") {
			return nil
		}
		rel, _ := filepath.Rel(root, p)
		rel = filepath.ToSlash(rel)
		if !scanned(filepath.ToSlash(filepath.Dir(rel))) {
			return nil
		}
		for _, re := range pats {
			if re.MatchString(rel) {
				return nil
			}
		}
		out = append(out, fmt.Sprintf("route file %s declares an auth-service httpapi.Route{ and is outside ci/auth_corpus_paths.txt: a change to it would skip the external corpus", rel))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(out)
	return out
}

func TestAuthCorpusQueueJobShape(t *testing.T) {
	expectNoViolations(t, authCorpusQueueViolations(t, repoRoot(t)))
}

func TestAuthCorpusQueueGateFanIn(t *testing.T) {
	expectNoViolations(t, authCorpusGateViolations(t, repoRoot(t)))
}

// authServiceClosureDirs: the module directories of the import closure of the auth-service binary, relative to root.
func authServiceClosureDirs(t *testing.T, root string) map[string]bool {
	t.Helper()
	cmd := exec.Command("go", "list", "-mod=readonly", "-deps", "-f", "{{if not .Standard}}{{.Dir}}{{end}}", "./cmd/auth-service")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOWORK=off", "GOFLAGS=-mod=readonly")
	raw, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list the auth-service closure: %v", err)
	}
	dirs := map[string]bool{}
	for _, d := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if rel, err := filepath.Rel(root, d); err == nil && !strings.HasPrefix(rel, "..") {
			dirs[filepath.ToSlash(rel)] = true
		}
	}
	if len(dirs) < 10 {
		t.Fatalf("the auth-service closure has only %d module directories: go list misread", len(dirs))
	}
	return dirs
}

func TestAuthCorpusQueuePathSetCoversEveryRouteFile(t *testing.T) {
	root := repoRoot(t)
	closure := authServiceClosureDirs(t, root)
	expectNoViolations(t, corpusRouteFilesOutsideSet(t, root, func(dir string) bool { return closure[dir] }))
}

func TestAuthCorpusQueueNewRouteFileIsInScope(t *testing.T) {
	root := repoRoot(t)
	tree := plantTree(t, root, corpusQueueFiles)
	good := filepath.Join(tree, "internal", "auth", "authapi", "newroutes.go")
	bad := filepath.Join(tree, "internal", "elsewhere", "newroutes.go")
	for _, p := range []string{good, bad} {
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("package x\n\nimport \"github.com/full-chaos/dev-health-ops/internal/auth/httpapi\"\n\nvar _ = httpapi.Route{}\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for _, d := range []string{"cmd/auth-service", "internal/auth", "internal/apiservice", "internal/testsupport", "ci", "docker"} {
		if err := os.MkdirAll(filepath.Join(tree, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	got := corpusRouteFilesOutsideSet(t, tree, func(string) bool { return true })
	joined := strings.Join(got, "\n")
	if !strings.Contains(joined, "internal/elsewhere/newroutes.go") {
		t.Fatalf("a route file outside the set was not reported:\n%s", joined)
	}
	if strings.Contains(joined, "internal/auth/authapi/newroutes.go") {
		t.Fatalf("a new route file under internal/auth was reported as out of scope:\n%s", joined)
	}
}

func TestAuthCorpusQueuePlantedDefects(t *testing.T) {
	root := repoRoot(t)
	const wf = corpusQueueWorkflow
	shape := []struct {
		name string
		muts []mutation
		want []string
	}{
		{"job runs on pull requests too", []mutation{{wf, corpusQueueIf, "needs.auth-changes.outputs.corpus == 'true'"}}, []string{"auth-corpus-queue.if must be exactly"}},
		{"job loses its timeout", []mutation{{wf, "  auth-corpus-queue:\n    name: auth-corpus-queue (${{ matrix.leg }})\n    needs: [auth-changes]\n    if: " + corpusQueueIf + "\n    runs-on: ubuntu-latest\n    timeout-minutes: 30\n", "  auth-corpus-queue:\n    name: auth-corpus-queue (${{ matrix.leg }})\n    needs: [auth-changes]\n    if: " + corpusQueueIf + "\n    runs-on: ubuntu-latest\n"}}, []string{"timeout-minutes"}},
		{"job hides a red leg behind continue-on-error", []mutation{{wf, "    timeout-minutes: 30\n    env:\n      CHECK_GO_JOB_CAP_MIN: 30\n    strategy:\n      fail-fast: false\n      matrix:\n        leg: [go-api, acp, acp-nokeys, acp-social-unowned]", "    timeout-minutes: 30\n    continue-on-error: true\n    strategy:\n      fail-fast: false\n      matrix:\n        leg: [go-api, acp, acp-nokeys, acp-social-unowned]"}}, []string{"continue-on-error"}},
		{"a leg is retried", []mutation{{wf, `run: bash ci/auth_corpus_external.sh "${{ matrix.leg }}"`, `run: bash ci/auth_corpus_external.sh "${{ matrix.leg }}" || bash ci/auth_corpus_external.sh "${{ matrix.leg }}"`}}, []string{"runs the corpus script more than once"}},
		{"a leg is hidden behind || true", []mutation{{wf, `run: bash ci/auth_corpus_external.sh "${{ matrix.leg }}"`, `run: bash ci/auth_corpus_external.sh "${{ matrix.leg }}" || true`}}, []string{"retry or error-hiding"}},
		{"no-keys leg loses the by-name grep", []mutation{{wf, "required aggregate sessions is not served by auth-service", "required aggregate"}}, []string{"inverted exit form"}},
		{"forced-social leg loses its by-name grep", []mutation{{wf, "social-login through the provider stub: answered 403 with", "social-login"}}, []string{"forced-social leg lacks"}},
		{"forced-social leg is no longer inverted", []mutation{{wf, `if [ "${status}" -eq 0 ]; then
            echo "ERROR: leg acp passed with social forwarded and users not owned"`, `if [ "${status}" -eq 99 ]; then
            echo "ERROR: leg acp passed with social forwarded and users not owned"`}}, []string{"inverted exit check in both negative legs"}},
		{"the plain run step also runs the forced-social leg", []mutation{{wf, "if: matrix.leg != 'acp-nokeys' && matrix.leg != 'acp-social-unowned'", "if: matrix.leg != 'acp-nokeys'"}}, []string{"forced-social leg lacks"}},
		{"forced-social leg loses the force switch", []mutation{{wf, "AUTH_CORPUS_USERS_UNOWNED=1 AUTH_CORPUS_FORCE_SOCIAL=1 bash", "AUTH_CORPUS_USERS_UNOWNED=1 bash"}}, []string{"forced-social leg lacks"}},
		{"gate stops waiting for the corpus job", []mutation{{wf, "needs: [auth-changes, auth-go-quality, auth-test, auth-fuzz, auth-corpus-queue]", "needs: [auth-changes, auth-go-quality, auth-test, auth-fuzz]"}}, []string{"auth-gate.needs must include auth-corpus-queue"}},
	}
	for _, c := range shape {
		t.Run(c.name, func(t *testing.T) {
			expectViolation(t, authCorpusQueueViolations(t, plantTree(t, root, corpusQueueFiles, c.muts...)), c.want...)
		})
	}
	gate := []struct {
		name string
		muts []mutation
		want []string
	}{
		{"gate passes a skipped corpus leg whatever the relevance", []mutation{{wf, `[ "${CORPUS}" = "success" ]`, `[ "${CORPUS}" != "failure" ]`}}, []string{"skipped corpus leg with relevant=true"}},
	}
	for _, c := range gate {
		t.Run(c.name, func(t *testing.T) {
			expectViolation(t, authCorpusGateViolations(t, plantTree(t, root, corpusQueueFiles, c.muts...)), c.want...)
		})
	}
}

// corpusDirectInputs lists the repo paths the corpus reads or builds from directly, outside the Go import closures:
// the ${ROOT}/ literals of the corpus script, the Dockerfile and bind-mount sources of compose.yml and of the two
// overrides, and what docker/go-worker.Dockerfile copies into the image.
func corpusDirectInputs(t *testing.T, root string) []string {
	t.Helper()
	seen := map[string]bool{}
	add := func(p string) {
		p = strings.TrimPrefix(strings.TrimSuffix(strings.TrimPrefix(p, "./"), "/"), "/")
		if p != "" && p != "." {
			seen[p] = true
		}
	}
	for _, m := range regexp.MustCompile(`\$\{ROOT\}/([A-Za-z0-9_./-]+)`).FindAllStringSubmatch(readFile(t, filepath.Join(root, "ci/auth_corpus_external.sh")), -1) {
		add(m[1])
	}
	mount := regexp.MustCompile(`(?m)^\s+- (\./docker/[^:\s]+):`)
	dockerfile := regexp.MustCompile(`(?m)^\s+dockerfile: (\S+)`)
	local := regexp.MustCompile(`(?m)^\s+- (\./ci/[^:\s]+):`)
	for _, f := range []string{"compose.yml", "ci/auth_corpus_external.override.yml", "ci/auth_corpus_external.acp.override.yml"} {
		text := readFile(t, filepath.Join(root, f))
		for _, re := range []*regexp.Regexp{mount, dockerfile, local} {
			for _, m := range re.FindAllStringSubmatch(text, -1) {
				add(m[1])
			}
		}
	}
	for _, line := range strings.Split(readFile(t, filepath.Join(root, "docker/go-worker.Dockerfile")), "\n") {
		f := strings.Fields(line)
		if len(f) < 3 || f[0] != "COPY" || strings.Contains(line, "--from=") {
			continue
		}
		for _, src := range f[1 : len(f)-1] {
			if !strings.HasPrefix(src, "-") {
				add(src)
			}
		}
	}
	out := make([]string, 0, len(seen))
	for p := range seen {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

// corpusInputsOutsideSet returns the direct inputs that no row of the path list matches (a directory input is matched
// by a row for something under it).
func corpusInputsOutsideSet(t *testing.T, root string) []string {
	t.Helper()
	var pats []*regexp.Regexp
	for _, line := range corpusRows(t, root) {
		pats = append(pats, corpusRowRegexp(line))
	}
	// Go and contract trees the images copy whole: go-api's own closure is not part of the set (cost; see the path list).
	closureRoots := map[string]bool{"cmd": true, "internal": true, "contracts": true}
	var out []string
	for _, in := range corpusDirectInputs(t, root) {
		if closureRoots[in] {
			continue
		}
		probe := []string{in, in + "/x"}
		ok := false
		for _, re := range pats {
			for _, p := range probe {
				if re.MatchString(p) {
					ok = true
				}
			}
		}
		if !ok {
			out = append(out, fmt.Sprintf("corpus input %s matches no row of ci/auth_corpus_paths.txt: a change to it would skip the external corpus", in))
		}
	}
	return out
}

func TestAuthCorpusQueuePathSetCoversEveryDirectInput(t *testing.T) {
	root := repoRoot(t)
	inputs := corpusDirectInputs(t, root)
	for _, want := range []string{"internal/testsupport/sessionscenario/sessionscenario.go", "docker/go-worker.Dockerfile", "docker/providerstub.Dockerfile", "docker/smtpsink.Dockerfile", "go.mod", "go.sum", "docker/init-extra-dbs.sh"} {
		if !containsStr(inputs, want) {
			t.Fatalf("the input scan no longer finds %s (it found %v): the scan is vacuous", want, inputs)
		}
	}
	expectNoViolations(t, corpusInputsOutsideSet(t, root))
}

func TestAuthCorpusQueueDirectInputScanCatchesAMissingRow(t *testing.T) {
	root := repoRoot(t)
	tree := plantTree(t, root, append([]string{"ci/auth_corpus_external.sh", "compose.yml", "ci/auth_corpus_external.override.yml", "ci/auth_corpus_external.acp.override.yml", "docker/go-worker.Dockerfile"}, corpusQueuePaths))
	rows := strings.Replace(readFile(t, filepath.Join(tree, corpusQueuePaths)), "internal/testsupport/sessionscenario/*\n", "", 1)
	if err := os.WriteFile(filepath.Join(tree, corpusQueuePaths), []byte(rows+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	expectViolation(t, corpusInputsOutsideSet(t, tree), "internal/testsupport/sessionscenario/sessionscenario.go")
}

func TestAuthCorpusQueuePathSetHasTheGoAPIRouteAssembly(t *testing.T) {
	root := repoRoot(t)
	// The api command is defined in internal/apiservice/service.go and listed in cmd/dho/main.go: the corpus runs `dho api`.
	if !strings.Contains(readFile(t, filepath.Join(root, "internal/apiservice/service.go")), "func Command() cli.Command") ||
		!strings.Contains(readFile(t, filepath.Join(root, "internal/apiservice/service.go")), "func Routes(deps Deps") {
		t.Fatal("internal/apiservice/service.go no longer defines the api command and Routes: the pin below is stale")
	}
	if !strings.Contains(readFile(t, filepath.Join(root, "cmd/dho/main.go")), "apiservice.Command()") {
		t.Fatal("cmd/dho/main.go no longer lists apiservice.Command(): the pin below is stale")
	}
	var pats []*regexp.Regexp
	for _, line := range corpusRows(t, root) {
		pats = append(pats, corpusRowRegexp(line))
	}
	matches := func(p string) bool {
		for _, re := range pats {
			if re.MatchString(p) {
				return true
			}
		}
		return false
	}
	for _, in := range []string{"internal/apiservice/service.go", "internal/apiservice/authproxy.go", "internal/apiservice/service_test.go", "cmd/dho/main.go", "internal/apiservice/authflowvenue/x.go"} {
		if !matches(in) {
			t.Errorf("%s matches no row of ci/auth_corpus_paths.txt: a go-api route assembly change would skip the external corpus", in)
		}
	}
	for _, out := range []string{"internal/apiservice/admin/x.go", "internal/queryapi/x.go", "cmd/dho/other_test.go"} {
		if matches(out) {
			t.Errorf("%s matches a row of ci/auth_corpus_paths.txt: the go-api rows must stay flat", out)
		}
	}
}

// Rows the script refuses (it prints corpus=true) and rows it accepts; the same table is run against the script in
// internal/cigates/scripts (TestAuthCorpusQueueMalformedRowsFailClosed).
var corpusRowTable = []struct {
	row     string
	problem bool
}{
	{"=", true}, {"a/=b/*", true}, {"=*.go", true}, {"=/", true}, {"=a/", true}, {"=internal/apiservice/", true},
	{"=/x.go", true}, {"=a/*/x.go", true}, {"=a/?/x.go", true}, {"=a/[b]/x.go", true}, {"=a//x.go", true},
	{"a/*\r", true}, {"=a/*.go\r", true}, {"a/* ", true}, {"=a/*.go\t", true}, {" a/*", true}, {"\ta/*", true},
	{"=a/*.go", false}, {"=internal/apiservice/*.go", false}, {"a/*", false}, {"cmd/dho/main.go", false}, {"compose.yml", false},
}

func TestAuthCorpusQueueRowModelRefusesWhatTheScriptRefuses(t *testing.T) {
	for _, c := range corpusRowTable {
		if got := corpusRowProblem(c.row) != ""; got != c.problem {
			t.Errorf("row %q: model says problem=%v, want %v", c.row, got, c.problem)
		}
	}
}

func TestAuthCorpusQueuePathsFileRowsAreWellFormed(t *testing.T) {
	var out []string
	for _, row := range corpusRows(t, repoRoot(t)) {
		if p := corpusRowProblem(row); p != "" {
			out = append(out, fmt.Sprintf("row %q of ci/auth_corpus_paths.txt is malformed (%s): the script would run the corpus on every entry", row, p))
		}
	}
	expectNoViolations(t, out)
}

func TestAuthCorpusQueueRowReaderKeepsWhitespaceRows(t *testing.T) {
	root := repoRoot(t)
	tree := plantTree(t, root, corpusQueueFiles)
	rows := readFile(t, filepath.Join(tree, corpusQueuePaths)) + "internal/auth/x/* \n"
	if err := os.WriteFile(filepath.Join(tree, corpusQueuePaths), []byte(rows), 0o644); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, r := range corpusRows(t, tree) {
		if r == "internal/auth/x/* " && corpusRowProblem(r) != "" {
			found = true
		}
	}
	if !found {
		t.Fatal("a row with trailing whitespace was trimmed away or not reported: a bad row would hide")
	}
}

// service.go mounts the go-api auth routes from internal/api/session and internal/api/sso, and wraps every route in
// internal/api/policy's guard: a change in any of them changes what the corpus sees.
func TestAuthCorpusQueuePathSetHasTheGoAPIAuthRouteDeclarations(t *testing.T) {
	root := repoRoot(t)
	service := readFile(t, filepath.Join(root, "internal/apiservice/service.go"))
	var pats []*regexp.Regexp
	for _, row := range corpusRows(t, root) {
		pats = append(pats, corpusRowRegexp(row))
	}
	matches := func(p string) bool {
		for _, re := range pats {
			if re.MatchString(p) {
				return true
			}
		}
		return false
	}
	for _, pkg := range []string{"session", "sso", "policy"} {
		if !strings.Contains(service, `/internal/api/`+pkg+`"`) {
			t.Fatalf("internal/apiservice/service.go no longer imports internal/api/%s: the pin below is stale", pkg)
		}
		entries, err := os.ReadDir(filepath.Join(root, "internal/api", pkg))
		if err != nil {
			t.Fatal(err)
		}
		seen := 0
		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") {
				continue
			}
			seen++
			if p := "internal/api/" + pkg + "/" + e.Name(); !matches(p) {
				t.Errorf("%s matches no row of ci/auth_corpus_paths.txt: the go-api auth routes, guard and limiters would change with no corpus run", p)
			}
		}
		if seen == 0 {
			t.Fatalf("internal/api/%s has no Go file: the pin is vacuous", pkg)
		}
	}
}
