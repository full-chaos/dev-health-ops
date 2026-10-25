package workflows_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// When a merge queue entry goes red, GitHub rebuilds the entries behind it but leaves their old runs running. The
// go.yml job cancel-stacked-queue-children cancels those stale runs. These tests pin the job's shape (the only job with
// actions: write, no code, no secrets, not a required context), the script's only write (POST .../cancel), and which
// runs the script selects from a fixture run list.

const (
	cancelGoYml   = ".github/workflows/go.yml"
	cancelJobName = "cancel-stacked-queue-children"
	cancelScript  = "ci/cancel_stacked_queue_runs.sh"
	cancelIf      = "failure() && github.event_name == 'merge_group'"
)

func cancelJobDefects(doc yamlMap) []string {
	var out []string
	jobs := jobsOf(doc)
	raw, ok := jobs[cancelJobName]
	if !ok {
		return []string{"job " + cancelJobName + " is absent"}
	}
	job := asMap(raw)
	if got := fmt.Sprint(job["needs"]); got != "[go-storage-integration]" {
		out = append(out, "needs is "+got+", want exactly [go-storage-integration]")
	}
	if got := asString(job["if"]); got != cancelIf {
		out = append(out, fmt.Sprintf("if is %q, want %q", got, cancelIf))
	}
	if job["continue-on-error"] != true {
		out = append(out, "continue-on-error must be true")
	}
	perms := asMap(job["permissions"])
	if len(perms) != 2 || perms["actions"] != "write" || perms["contents"] != "read" {
		out = append(out, fmt.Sprintf("permissions is %v, want exactly actions: write, contents: read", perms))
	}
	if _, has := job["env"]; has {
		out = append(out, "job-level env is not allowed")
	}
	if _, has := job["services"]; has {
		out = append(out, "services are not allowed")
	}
	if _, has := job["container"]; has {
		out = append(out, "container is not allowed")
	}
	steps := asList(job["steps"])
	if len(steps) != 2 {
		return append(out, fmt.Sprintf("job has %d steps, want exactly 2 (sparse checkout, run the script)", len(steps)))
	}
	co, run := asMap(steps[0]), asMap(steps[1])
	if !strings.HasPrefix(asString(co["uses"]), "actions/checkout@") {
		out = append(out, "step 1 must be actions/checkout")
	}
	with := asMap(co["with"])
	if with["sparse-checkout"] != cancelScript {
		out = append(out, fmt.Sprintf("checkout sparse-checkout is %v, want only %s", with["sparse-checkout"], cancelScript))
	}
	if with["persist-credentials"] != false {
		out = append(out, "checkout persist-credentials must be false")
	}
	if asString(run["run"]) != "bash "+cancelScript {
		out = append(out, fmt.Sprintf("step 2 run is %q, want exactly %q", asString(run["run"]), "bash "+cancelScript))
	}
	if _, has := run["uses"]; has {
		out = append(out, "step 2 must not use an action")
	}
	renv := asMap(run["env"])
	want := map[string]string{"GH_TOKEN": "${{ github.token }}", "REPO": "${{ github.repository }}", "HEAD_SHA": "${{ github.sha }}", "RUN_ID": "${{ github.run_id }}"}
	if len(renv) != len(want) {
		out = append(out, fmt.Sprintf("step 2 env has %d keys, want %d", len(renv), len(want)))
	}
	for k, v := range want {
		if renv[k] != v {
			out = append(out, fmt.Sprintf("step 2 env %s is %v, want %s", k, renv[k], v))
		}
	}
	return out
}

func cancelOtherDefects(doc yamlMap) []string {
	var out []string
	for _, n := range asList(asMap(pinJob(doc, "go-queue-gate"))["needs"]) {
		if n == cancelJobName {
			out = append(out, "go-queue-gate must not need "+cancelJobName+" (it must not become a required context)")
		}
	}
	perms := asMap(pinJob(doc, aggregateJobName)["permissions"])
	if perms["actions"] != "read" {
		out = append(out, fmt.Sprintf("%s keeps actions: read, got %v", aggregateJobName, perms["actions"]))
	}
	for name, j := range jobsOf(doc) {
		if name == cancelJobName {
			continue
		}
		for k, v := range asMap(asMap(j)["permissions"]) {
			if k == "actions" && v == "write" {
				out = append(out, name+" has actions: write; only "+cancelJobName+" may")
			}
		}
	}
	return out
}

func TestStackedQueueCancelJobShape(t *testing.T) {
	root := rootOf(t)
	doc := loadWf(t, root, "go.yml")
	if d := append(cancelJobDefects(doc), cancelOtherDefects(doc)...); len(d) > 0 {
		t.Fatalf("%s", strings.Join(d, "\n"))
	}
	for _, m := range []mutation{
		{cancelGoYml, "    permissions:\n      actions: write\n      contents: read\n    steps:\n      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1\n        with:\n          sparse-checkout: " + cancelScript, "    permissions:\n      actions: write\n      contents: read\n      packages: read\n    steps:\n      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1\n        with:\n          sparse-checkout: " + cancelScript},
		{cancelGoYml, "    permissions:\n      actions: write\n      contents: read\n    steps:\n      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1\n        with:\n          sparse-checkout: " + cancelScript, "    permissions:\n      actions: write\n      contents: write\n    steps:\n      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1\n        with:\n          sparse-checkout: " + cancelScript},
		{cancelGoYml, "        run: bash " + cancelScript + "\n", "        run: bash " + cancelScript + "\n      - name: Test\n        run: go test ./...\n"},
		{cancelGoYml, "    if: " + cancelIf + "\n", "    if: failure()\n"},
		{cancelGoYml, "    if: " + cancelIf + "\n", "    if: always() && github.event_name == 'merge_group'\n"},
		{cancelGoYml, "    continue-on-error: true\n    permissions:\n      actions: write", "    permissions:\n      actions: write"},
		{cancelGoYml, "    needs: [go-storage-integration]\n    if: " + cancelIf, "    needs: [go-storage-integration, go-container-smoke]\n    if: " + cancelIf},
		{cancelGoYml, "          persist-credentials: false\n          sparse-checkout-cone-mode: false\n", "          sparse-checkout-cone-mode: false\n"},
		{cancelGoYml, "          sparse-checkout: " + cancelScript + "\n", "          sparse-checkout: ci\n"},
		{cancelGoYml, "          HEAD_SHA: ${{ github.sha }}\n", "          HEAD_SHA: ${{ github.sha }}\n          SECRET: ${{ secrets.GITHUB_TOKEN }}\n"},
		{cancelGoYml, "      - go-auth-web-proxy-proof\n      - graphql-wire-parity\n    runs-on: ubuntu-latest\n    timeout-minutes: 5\n    steps:\n      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1\n\n      - name: Aggregate the heavy Go results", "      - go-auth-web-proxy-proof\n      - graphql-wire-parity\n      - " + cancelJobName + "\n    runs-on: ubuntu-latest\n    timeout-minutes: 5\n    steps:\n      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1\n\n      - name: Aggregate the heavy Go results"},
		{cancelGoYml, "    permissions:\n      contents: read\n      actions: read\n    runs-on: ubuntu-latest\n    timeout-minutes: 5\n    env:\n      PLAN_RESULT", "    permissions:\n      contents: read\n      actions: write\n    runs-on: ubuntu-latest\n    timeout-minutes: 5\n    env:\n      PLAN_RESULT"},
	} {
		tmp := plantTree(t, root, []string{cancelGoYml}, m)
		d := loadWf(t, tmp, "go.yml")
		if len(append(cancelJobDefects(d), cancelOtherDefects(d)...)) == 0 {
			t.Errorf("planted job defect was not caught: %q -> %q", m.old, m.new)
		}
	}
}

// The script may write once, to POST .../cancel. No rerun, dispatch, dequeue or other verb.
func TestStackedQueueCancelScriptOnlyCancels(t *testing.T) {
	text := readFile(t, filepath.Join(rootOf(t), cancelScript))
	if d := scriptVerbDefects(text); len(d) > 0 {
		t.Fatalf("%s", strings.Join(d, "\n"))
	}
	for _, m := range []struct{ old, new string }{
		{`gh api -X POST "repos/${repo}/actions/runs/${id}/cancel"`, `gh api -X POST "repos/${repo}/actions/runs/${id}/rerun"`},
		{`gh api -X POST "repos/${repo}/actions/runs/${id}/cancel"`, `gh api -X DELETE "repos/${repo}/actions/runs/${id}"`},
		{`gh api -X POST "repos/${repo}/actions/runs/${id}/cancel"`, `gh run rerun "${id}" -R "${repo}"`},
		{`exit 0
fi

for id`, `exit 0
fi
gh api -X POST "repos/${repo}/actions/workflows/go.yml/dispatches" -f ref=main
for id`},
		{`exit 0
fi

for id`, `exit 0
fi
gh api -X PUT "repos/${repo}/actions/runs/1/cancel"
for id`},
	} {
		if !strings.Contains(text, m.old) {
			t.Fatalf("planted defect did not land: %q", m.old)
		}
		if len(scriptVerbDefects(strings.Replace(text, m.old, m.new, 1))) == 0 {
			t.Errorf("planted verb defect was not caught: %q", m.new)
		}
	}
}

var (
	verbRe     = regexp.MustCompile(`(?m)^[^#\n]*\b(?:gh|curl)\b[^\n]*`)
	xMethodRe  = regexp.MustCompile(`(?:-X|--method)\s+([A-Za-z]+)`)
	cancelPath = regexp.MustCompile(`-X POST "repos/\$\{repo\}/actions/runs/\$\{id\}/cancel"`)
)

func scriptVerbDefects(text string) []string {
	var out []string
	posts := 0
	var code strings.Builder
	for _, l := range strings.Split(text, "\n") {
		if !strings.HasPrefix(strings.TrimSpace(l), "#") {
			code.WriteString(l + "\n")
		}
	}
	for _, line := range verbRe.FindAllString(code.String(), -1) {
		if strings.Contains(line, "gh run ") || strings.Contains(line, "gh workflow") || strings.Contains(line, "curl") {
			out = append(out, "forbidden command: "+strings.TrimSpace(line))
		}
		for _, m := range xMethodRe.FindAllStringSubmatch(line, -1) {
			if m[1] != "POST" || !cancelPath.MatchString(line) {
				out = append(out, "only POST .../cancel is allowed: "+strings.TrimSpace(line))
			} else {
				posts++
			}
		}
		if strings.Contains(line, " -f ") || strings.Contains(line, " -F ") || strings.Contains(line, "--input") {
			out = append(out, "no request body is allowed: "+strings.TrimSpace(line))
		}
	}
	if posts != 1 {
		out = append(out, fmt.Sprintf("want exactly 1 POST .../cancel call, found %d", posts))
	}
	for _, bad := range []string{"rerun", "dispatches", "dequeue", "force-cancel"} {
		if strings.Contains(code.String(), bad) {
			out = append(out, "forbidden word in script code: "+bad)
		}
	}
	return out
}

// ---- selection behaviour against a fixture run list (gh is a stub that applies the real --jq filter with jq) ----

const (
	shaRed = "1111111111111111111111111111111111111111" // the red entry's head_sha
	shaB   = "2222222222222222222222222222222222222222" // child stacked on red
	shaC   = "3333333333333333333333333333333333333333" // grandchild stacked on B
	shaD   = "4444444444444444444444444444444444444444" // unrelated entry
	shaMn  = "5555555555555555555555555555555555555555" // main tip
)

const fixtureQueued = `{"workflow_runs":[
 {"id":101,"name":"Go","head_branch":"gh-readonly-queue/main/pr-20-` + shaRed + `","head_sha":"` + shaB + `"},
 {"id":102,"name":"Go Quality","head_branch":"gh-readonly-queue/main/pr-20-` + shaRed + `","head_sha":"` + shaB + `"},
 {"id":103,"name":"Go","head_branch":"gh-readonly-queue/main/pr-30-` + shaB + `","head_sha":"` + shaC + `"},
 {"id":104,"name":"Go","head_branch":"gh-readonly-queue/main/pr-40-` + shaMn + `","head_sha":"` + shaD + `"},
 {"id":105,"name":"Go","head_branch":"gh-readonly-queue/main/pr-10-` + shaRed + `","head_sha":"` + shaRed + `"}
]}`

var fixtureInProgress = `{"workflow_runs":[
 {"id":201,"name":"Auth","head_branch":"gh-readonly-queue/main/pr-30-` + shaB + `","head_sha":"` + shaC + `"},
 {"id":202,"name":"Go","head_branch":"gh-readonly-queue/other/pr-20-` + shaRed + `","head_sha":"` + shaD + `"},
 {"id":203,"name":"Go","head_branch":"feature/gh-readonly-queue/main/pr-20-` + shaRed + `","head_sha":"` + shaD + `"},
 {"id":204,"name":"Go","head_branch":"gh-readonly-queue/main/pr-20-` + shaRed + `x","head_sha":"` + shaD + `"},
 {"id":205,"name":"Go","head_branch":"gh-readonly-queue/main/pr-20-` + strings.ToUpper(shaRed) + `","head_sha":"` + shaD + `"},
 {"id":206,"name":"Go","head_branch":"main","head_sha":"` + shaRed + `"}
]}`

const ghStub = `#!/bin/bash
set -u
args=("$@")
for a in "${args[@]}"; do
  if [ "$a" = "POST" ]; then
    echo "${args[*]}" >> "$STUB_LOG"
    exit "${STUB_POST_RC:-0}"
  fi
done
jqexpr=""
for ((i = 0; i < ${#args[@]}; i++)); do
  [ "${args[i]}" = "--jq" ] && jqexpr="${args[i+1]}"
done
case "${args[*]}" in
  *status=queued*) f="$STUB_DIR/queued.json" ;;
  *status=in_progress*) f="$STUB_DIR/in_progress.json" ;;
  *) echo "unexpected gh call: ${args[*]}" >&2; exit 9 ;;
esac
[ -n "${STUB_LIST_FAIL:-}" ] && exit 1
jq -r "$jqexpr" "$f"
`

func runCancelScript(t *testing.T, scriptRoot string, env ...string) (string, []string) {
	t.Helper()
	dir := t.TempDir()
	for name, body := range map[string]string{"gh": ghStub, "queued.json": fixtureQueued, "in_progress.json": fixtureInProgress} {
		mode := os.FileMode(0o644)
		if name == "gh" {
			mode = 0o755
		}
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), mode); err != nil {
			t.Fatal(err)
		}
	}
	logf := filepath.Join(dir, "calls.log")
	cmd := exec.Command("bash", filepath.Join(scriptRoot, cancelScript))
	cmd.Env = append([]string{"PATH=" + withShims(dir+":/usr/bin:/bin:/usr/local/bin"), "STUB_DIR=" + dir, "STUB_LOG=" + logf,
		"REPO=o/r", "HEAD_SHA=" + shaRed, "RUN_ID=105"}, env...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("script must exit 0, got %v\n%s", err, out)
	}
	var ids []string
	if b, err := os.ReadFile(logf); err == nil {
		for _, l := range strings.Split(strings.TrimSpace(string(b)), "\n") {
			if l == "" {
				continue
			}
			m := regexp.MustCompile(`^api -X POST repos/o/r/actions/runs/(\d+)/cancel$`).FindStringSubmatch(l)
			if m == nil {
				t.Fatalf("unexpected write call: %q", l)
			}
			ids = append(ids, m[1])
		}
	}
	sort.Strings(ids)
	return string(out), ids
}

func TestStackedQueueCancelSelectsOnlyTheChain(t *testing.T) {
	root := rootOf(t)
	_, ids := runCancelScript(t, root)
	if got, want := strings.Join(ids, ","), "101,102,103,201"; got != want {
		t.Fatalf("cancelled runs %s, want %s (children, grandchildren; never the red run 105, an unrelated entry 104, a non-queue or look-alike branch 202-206)", got, want)
	}
}

func TestStackedQueueCancelDryRunWritesNothing(t *testing.T) {
	out, ids := runCancelScript(t, rootOf(t), "DRY_RUN=1")
	if len(ids) != 0 {
		t.Fatalf("dry run wrote: %v", ids)
	}
	if !strings.Contains(out, "would cancel run 101") {
		t.Fatalf("dry run does not name run 101:\n%s", out)
	}
}

func TestStackedQueueCancelSurvivesFailures(t *testing.T) {
	root := rootOf(t)
	if _, ids := runCancelScript(t, root, "STUB_POST_RC=1"); len(ids) != 4 {
		t.Fatalf("a failed cancel must not stop the others: attempted %v", ids)
	}
	if _, ids := runCancelScript(t, root, "STUB_LIST_FAIL=1"); len(ids) != 0 {
		t.Fatalf("an unreadable run list must cancel nothing: %v", ids)
	}
	if _, ids := runCancelScript(t, root, "HEAD_SHA=not-a-sha"); len(ids) != 0 {
		t.Fatalf("a malformed HEAD_SHA must cancel nothing: %v", ids)
	}
}

func TestStackedQueueCancelPlantedSelectionFaults(t *testing.T) {
	root := rootOf(t)
	for _, m := range []mutation{
		{cancelScript, `branch_re='^gh-readonly-queue/main/pr-[0-9]+-([0-9a-f]{40})$'`, `branch_re='^gh-readonly-queue/.*/pr-[0-9]+-([0-9a-f]{40})'`},
		{cancelScript, `branch_re='^gh-readonly-queue/main/pr-[0-9]+-([0-9a-f]{40})$'`, `branch_re='pr-[0-9]+-([0-9a-f]{40})'`},
		{cancelScript, `    frontier["${sha}"]=1
`, ``},
		{cancelScript, `    [ "${id}" != "${self_id}" ] || continue
`, ``},
		{cancelScript, `    [ -n "${frontier[${BASH_REMATCH[1]}]:-}" ] || continue
`, ``},
		{cancelScript, `for status in queued in_progress; do`, `for status in queued; do`},
	} {
		tmp := plantTree(t, root, []string{cancelScript}, m)
		_, ids := runCancelScript(t, tmp)
		if strings.Join(ids, ",") == "101,102,103,201" {
			t.Errorf("planted selection fault was not caught: %q -> %q", m.old, m.new)
		}
	}
}
