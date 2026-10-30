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

	cancelCheckoutRef = "${{ github.event.merge_group.base_ref }}"
)

func cancelJobDefects(doc yamlMap) []string {
	return cancelJobDefectsNeeds(doc, "[go-storage-integration]")
}

func cancelJobDefectsNeeds(doc yamlMap, wantNeeds string) []string {
	var out []string
	jobs := jobsOf(doc)
	raw, ok := jobs[cancelJobName]
	if !ok {
		return []string{"job " + cancelJobName + " is absent"}
	}
	job := asMap(raw)
	if got := fmt.Sprint(job["needs"]); got != wantNeeds {
		out = append(out, "needs is "+got+", want exactly "+wantNeeds)
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
	if with["ref"] != cancelCheckoutRef {
		out = append(out, fmt.Sprintf("checkout ref is %v, want exactly %s (an immutable revision named by the event, never the group head)", with["ref"], cancelCheckoutRef))
	}
	if with["sparse-checkout-cone-mode"] != false {
		out = append(out, fmt.Sprintf("checkout sparse-checkout-cone-mode is %v, want false", with["sparse-checkout-cone-mode"]))
	}
	for k := range with {
		if k != "ref" && k != "sparse-checkout" && k != "persist-credentials" && k != "sparse-checkout-cone-mode" {
			out = append(out, fmt.Sprintf("checkout with: key %q is not allowed (only ref, sparse-checkout, persist-credentials, sparse-checkout-cone-mode)", k))
		}
	}
	if len(with) != 4 {
		out = append(out, fmt.Sprintf("checkout with: has %d keys, want exactly 4", len(with)))
	}
	if _, has := co["env"]; has {
		out = append(out, "the checkout step must not have env (the token goes to the script step only)")
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
		{cancelGoYml, "    permissions:\n      actions: write\n      contents: read\n    steps:\n      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1\n        with:\n          ref: " + cancelCheckoutRef + "\n          sparse-checkout: " + cancelScript, "    permissions:\n      actions: write\n      contents: read\n      packages: read\n    steps:\n      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1\n        with:\n          ref: " + cancelCheckoutRef + "\n          sparse-checkout: " + cancelScript},
		{cancelGoYml, "    permissions:\n      actions: write\n      contents: read\n    steps:\n      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1\n        with:\n          ref: " + cancelCheckoutRef + "\n          sparse-checkout: " + cancelScript, "    permissions:\n      actions: write\n      contents: write\n    steps:\n      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1\n        with:\n          ref: " + cancelCheckoutRef + "\n          sparse-checkout: " + cancelScript},
		{cancelGoYml, "        run: bash " + cancelScript + "\n", "        run: bash " + cancelScript + "\n      - name: Test\n        run: go test ./...\n"},
		{cancelGoYml, "    if: " + cancelIf + "\n", "    if: failure()\n"},
		{cancelGoYml, "    if: " + cancelIf + "\n", "    if: always() && github.event_name == 'merge_group'\n"},
		{cancelGoYml, "    continue-on-error: true\n    permissions:\n      actions: write", "    permissions:\n      actions: write"},
		{cancelGoYml, "    needs: [go-storage-integration]\n    if: " + cancelIf, "    needs: [go-storage-integration, go-container-smoke]\n    if: " + cancelIf},
		{cancelGoYml, "          persist-credentials: false\n          sparse-checkout-cone-mode: false\n", "          sparse-checkout-cone-mode: false\n"},
		{cancelGoYml, "          sparse-checkout: " + cancelScript + "\n", "          sparse-checkout: ci\n"},
		{cancelGoYml, "          sparse-checkout: " + cancelScript + "\n", "          sparse-checkout: " + cancelScript + " ci\n"},
		{cancelGoYml, "          sparse-checkout: " + cancelScript + "\n", "          sparse-checkout: |\n            " + cancelScript + "\n            ci\n"},
		{cancelGoYml, "          sparse-checkout-cone-mode: false\n", ""},
		{cancelGoYml, "          sparse-checkout-cone-mode: false\n", "          token: ${{ github.token }}\n"},
		{cancelGoYml, "          sparse-checkout-cone-mode: false\n", "          sparse-checkout-cone-mode: true\n"},
		{cancelGoYml, "          sparse-checkout-cone-mode: false\n", "          fetch-depth: 0\n"},
		{cancelGoYml, "        with:\n          ref: " + cancelCheckoutRef + "\n          sparse-checkout: " + cancelScript + "\n", "        with:\n          sparse-checkout: " + cancelScript + "\n"},
		{cancelGoYml, "          ref: " + cancelCheckoutRef + "\n", "          ref: ${{ github.sha }}\n"},
		{cancelGoYml, "          ref: " + cancelCheckoutRef + "\n", "          ref: ${{ github.event.merge_group.head_sha }}\n"},
		{cancelGoYml, "          ref: " + cancelCheckoutRef + "\n", "          ref: ${{ github.event.merge_group.base_sha }}\n"},
		{cancelGoYml, "          persist-credentials: false\n          sparse-checkout-cone-mode: false\n", "          persist-credentials: true\n          sparse-checkout-cone-mode: false\n"},
		{cancelGoYml, "          sparse-checkout-cone-mode: false\n      - name: Cancel the runs stacked", "          sparse-checkout-cone-mode: false\n          token: ${{ github.token }}\n      - name: Cancel the runs stacked"},
		{cancelGoYml, "        with:\n          ref: " + cancelCheckoutRef, "        env:\n          GH_TOKEN: ${{ github.token }}\n        with:\n          ref: " + cancelCheckoutRef},
		{cancelGoYml, "    permissions:\n      actions: write\n      contents: read\n    steps:", "    env:\n      GH_TOKEN: ${{ github.token }}\n    permissions:\n      actions: write\n      contents: read\n    steps:"},
		{cancelGoYml, "          HEAD_SHA: ${{ github.sha }}\n", "          HEAD_SHA: ${{ github.sha }}\n          SECRET: ${{ secrets.GITHUB_TOKEN }}\n"},
		{cancelGoYml, "      - go-auth-web-proxy-proof\n      - graphql-wire-parity\n    runs-on: ubuntu-latest\n    timeout-minutes: 5\n    steps:\n      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1\n\n      - name: Aggregate the heavy Go results", "      - go-auth-web-proxy-proof\n      - graphql-wire-parity\n      - " + cancelJobName + "\n    runs-on: ubuntu-latest\n    timeout-minutes: 5\n    steps:\n      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1\n\n      - name: Aggregate the heavy Go results"},
		{cancelGoYml, "    permissions:\n      actions: read\n    runs-on: ubuntu-latest\n    timeout-minutes: 5\n    env:\n      PLAN_RESULT", "    permissions:\n      actions: write\n    runs-on: ubuntu-latest\n    timeout-minutes: 5\n    env:\n      PLAN_RESULT"},
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
		{`gh api -X POST "repos/${repo}/actions/runs/${id}/cancel"`, `gh api -X POST "repos/${repo}/actions/runs/${id}/cancel" --raw-field body=x`},
		{`gh api -X POST "repos/${repo}/actions/runs/${id}/cancel"`, `gh api -X POST "repos/${repo}/actions/runs/${id}/cancel" --field body=x`},
		{`gh api -X POST "repos/${repo}/actions/runs/${id}/cancel"`, `gh api -X DELETE "repos/${repo}/actions/runs/${id}"`},
		{`gh api -X POST "repos/${repo}/actions/runs/${id}/cancel"`, `gh run rerun "${id}" -R "${repo}"`},
		{`cancelled=0
for id`, `cancelled=0
gh api -X POST "repos/${repo}/actions/workflows/go.yml/dispatches" -f ref=main
for id`},
		{`cancelled=0
for id`, `cancelled=0
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
		if strings.Contains(line, " -f ") || strings.Contains(line, " -F ") || strings.Contains(line, "--input") || strings.Contains(line, "--raw-field") || strings.Contains(line, "--field") {
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
	shaRed = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" // the red entry's head_sha
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
all="${args[*]}"
status=""
case "$all" in
  *status=queued*) status=queued ;;
  *status=in_progress*) status=in_progress ;;
  *) echo "unexpected gh call: $all" >&2; exit 9 ;;
esac
page=1
page_re='[&?]page=([0-9]+)'
if [[ "$all" =~ $page_re ]]; then page="${BASH_REMATCH[1]}"; fi
echo "$status $page" >> "$STUB_LOG.gets"
[ -n "${STUB_LIST_FAIL:-}" ] && exit 1
f="$STUB_DIR/$status.$page.json"
[ -e "$f" ] || f="$STUB_DIR/empty.json"
jq -r "$jqexpr" "$f"
`

type cancelRun struct {
	out  string
	rc   int
	ids  []string
	gets []string
}

func runCancelScriptWith(t *testing.T, scriptRoot string, files map[string]string, env ...string) cancelRun {
	t.Helper()
	dir := t.TempDir()
	all := map[string]string{"gh": ghStub, "empty.json": `{"workflow_runs":[]}`, "queued.1.json": fixtureQueued, "in_progress.1.json": fixtureInProgress}
	for k, v := range files {
		all[k] = v
	}
	for name, body := range all {
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
	r := cancelRun{out: string(out)}
	if err != nil {
		ee, ok := err.(*exec.ExitError)
		if !ok {
			t.Fatalf("running script: %v", err)
		}
		r.rc = ee.ExitCode()
	}
	if b, err := os.ReadFile(logf); err == nil {
		for _, l := range strings.Split(strings.TrimSpace(string(b)), "\n") {
			if l == "" {
				continue
			}
			m := regexp.MustCompile(`^api -X POST repos/o/r/actions/runs/([^/]+)/cancel$`).FindStringSubmatch(l)
			if m == nil {
				t.Fatalf("unexpected write call: %q", l)
			}
			r.ids = append(r.ids, m[1])
		}
	}
	sort.Strings(r.ids)
	if b, err := os.ReadFile(logf + ".gets"); err == nil {
		r.gets = strings.Split(strings.TrimSpace(string(b)), "\n")
	}
	return r
}

func runCancelScript(t *testing.T, scriptRoot string, env ...string) (string, []string, int) {
	t.Helper()
	r := runCancelScriptWith(t, scriptRoot, nil, env...)
	if r.rc != 0 {
		t.Fatalf("script must exit 0, got %d\n%s", r.rc, r.out)
	}
	return r.out, r.ids, len(r.gets)
}

// stalePages builds n pages of per queued stale runs (each a child of the red entry), ids from 1000.
func stalePages(n, per int) map[string]string {
	files := map[string]string{"in_progress.1.json": `{"workflow_runs":[]}`}
	id := 1000
	for p := 1; p <= n; p++ {
		var rows []string
		for i := 0; i < per; i++ {
			id++
			rows = append(rows, fmt.Sprintf(`{"id":%d,"name":"Go","head_branch":"gh-readonly-queue/main/pr-%d-%s","head_sha":"%s"}`, id, id, shaRed, shaB))
		}
		files[fmt.Sprintf("queued.%d.json", p)] = `{"workflow_runs":[` + strings.Join(rows, ",") + `]}`
	}
	return files
}

func TestStackedQueueCancelPagesAreRequestedInOrderAndStopAtAnEmptyOne(t *testing.T) {
	r := runCancelScriptWith(t, rootOf(t), nil)
	if got, want := strings.Join(r.gets, ","), "queued 1,queued 2,in_progress 1,in_progress 2"; got != want {
		t.Fatalf("list GETs %q, want %q (page 2 is empty, so each status stops there)", got, want)
	}
	r = runCancelScriptWith(t, rootOf(t), stalePages(3, 100))
	if r.rc != 0 || len(r.ids) != 300 || strings.Contains(r.out, "::warning::") {
		t.Fatalf("3 full pages then an empty one: rc=%d cancelled=%d, want rc 0, 300 cancelled, no warning", r.rc, len(r.ids))
	}
}

func TestStackedQueueCancelTruncatedScanIsLoudAndNonZero(t *testing.T) {
	r := runCancelScriptWith(t, rootOf(t), stalePages(5, 100))
	if r.rc != 2 {
		t.Fatalf("a full last page (5 x 100) must exit 2, got %d", r.rc)
	}
	if !strings.Contains(r.out, "::warning::stale-run scan truncated") {
		t.Fatalf("no ::warning:: line:\n%s", r.out)
	}
	if len(r.ids) != 500 {
		t.Fatalf("what was found must still be cancelled: %d of 500", len(r.ids))
	}
	if r = runCancelScriptWith(t, rootOf(t), stalePages(5, 100), "DRY_RUN=1"); r.rc != 2 || len(r.ids) != 0 {
		t.Fatalf("dry run: rc=%d writes=%d, want rc 2 and 0 writes", r.rc, len(r.ids))
	}
	files := stalePages(4, 100)
	files["queued.5.json"] = stalePages(1, 7)["queued.1.json"]
	if r = runCancelScriptWith(t, rootOf(t), files); r.rc != 0 {
		t.Fatalf("a partial page 5 is not truncated: rc=%d\n%s", r.rc, r.out)
	}
}

func TestStackedQueueCancelMissingInputsExit1(t *testing.T) {
	for _, v := range []string{"REPO", "HEAD_SHA", "RUN_ID"} {
		r := runCancelScriptWith(t, rootOf(t), nil, v+"=")
		if r.rc != 1 || len(r.ids) != 0 || !strings.Contains(r.out, v+" is required") {
			t.Errorf("%s empty: rc=%d writes=%d out=%q, want rc 1, 0 writes, %q", v, r.rc, len(r.ids), r.out, v+" is required")
		}
	}
}

func TestStackedQueueCancelSelectsOnlyTheChain(t *testing.T) {
	root := rootOf(t)
	_, ids, gets := runCancelScript(t, root)
	if gets != 4 {
		t.Fatalf("run-list GETs %d, want 4 (queued and in_progress, each stops at its empty page 2)", gets)
	}
	if got, want := strings.Join(ids, ","), "101,102,103,201"; got != want {
		t.Fatalf("cancelled runs %s, want %s (children, grandchildren; never the red run 105, an unrelated entry 104, a non-queue or look-alike branch 202-206)", got, want)
	}
}

func TestStackedQueueCancelDryRunWritesNothing(t *testing.T) {
	out, ids, _ := runCancelScript(t, rootOf(t), "DRY_RUN=1")
	if len(ids) != 0 {
		t.Fatalf("dry run wrote: %v", ids)
	}
	if !strings.Contains(out, "would cancel run 101") {
		t.Fatalf("dry run does not name run 101:\n%s", out)
	}
}

func TestStackedQueueCancelSurvivesFailures(t *testing.T) {
	root := rootOf(t)
	if _, ids, _ := runCancelScript(t, root, "STUB_POST_RC=1"); len(ids) != 4 {
		t.Fatalf("a failed cancel must not stop the others: attempted %v", ids)
	}
	if _, ids, _ := runCancelScript(t, root, "STUB_LIST_FAIL=1"); len(ids) != 0 {
		t.Fatalf("an unreadable run list must cancel nothing: %v", ids)
	}
	if _, ids, _ := runCancelScript(t, root, "HEAD_SHA=not-a-sha"); len(ids) != 0 {
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
		{cancelScript, `    [ -n "${frontier[${suffix}]:-}" ] || continue
`, ``},
		{cancelScript, `for status in queued in_progress; do`, `for status in queued; do`},
	} {
		tmp := plantTree(t, root, []string{cancelScript}, m)
		_, ids, gets := runCancelScript(t, tmp)
		if strings.Join(ids, ",") == "101,102,103,201" && gets == 4 {
			t.Errorf("planted selection fault was not caught: %q -> %q", m.old, m.new)
		}
	}
}

func TestStackedQueueCancelPlantedTruncationFaults(t *testing.T) {
	root := rootOf(t)
	for _, m := range []mutation{
		{cancelScript, `max_pages=5`, `max_pages=50`},
		{cancelScript, "page=${p}", "page=1"},
		{cancelScript, `exit 2`, allMarker + `exit 0`},
		{cancelScript, `::warning::stale-run scan truncated`, `stale-run scan truncated`},
		{cancelScript, `truncated=1`, `truncated=0`},
		{cancelScript, `-eq 100`, `-gt 100`},
		{cancelScript, `${REPO:?REPO is required}`, `${REPO:-o/r}`},
	} {
		tmp := plantTree(t, root, []string{cancelScript}, m)
		a := runCancelScriptWith(t, tmp, stalePages(5, 100))
		b := runCancelScriptWith(t, tmp, nil, "REPO=")
		ok := a.rc == 2 && strings.Contains(a.out, "::warning::stale-run scan truncated") && len(a.ids) == 500 && b.rc == 1
		if ok {
			t.Errorf("planted truncation fault was not caught: %q -> %q", m.old, m.new)
		}
	}
}

// A run whose id is not digits or whose head_sha is not 40 lowercase hex is skipped with a ::warning:: naming the run: it is
// never cancelled and never extends the chain. An empty run name is a valid run.
func invalidRunsFiles() map[string]string {
	queued := `{"workflow_runs":[
 {"id":101,"name":"` + shaB + `","head_branch":"gh-readonly-queue/main/pr-20-` + shaRed + `","head_sha":""},
 {"id":202,"name":"Other","head_branch":"gh-readonly-queue/main/pr-30-` + shaB + `","head_sha":"` + shaD + `"},
 {"id":203,"name":"","head_branch":"gh-readonly-queue/main/pr-40-` + shaRed + `","head_sha":"` + shaC + `"},
 {"id":204,"name":"Go","head_branch":"gh-readonly-queue/main/pr-50-` + shaC + `","head_sha":"` + shaD + `"},
 {"id":205,"name":"Go","head_branch":"gh-readonly-queue/main/pr-60-` + shaRed + `","head_sha":"not-a-sha"},
 {"id":206,"name":"Go","head_branch":"gh-readonly-queue/main/pr-70-not-a-sha","head_sha":"` + shaB + `"},
 {"name":"Go","head_branch":"gh-readonly-queue/main/pr-80-` + shaRed + `","head_sha":"` + shaMn + `"},
 {"id":"x7","name":"Go","head_branch":"gh-readonly-queue/main/pr-90-` + shaRed + `","head_sha":"` + shaMn + `"},
 {"id":208,"name":"Go","head_branch":"gh-readonly-queue/main/pr-91-` + shaMn + `","head_sha":null}
]}`
	return map[string]string{"queued.1.json": queued, "in_progress.1.json": `{"workflow_runs":[]}`}
}

func TestStackedQueueCancelInvalidRunsAreSkippedNeverChained(t *testing.T) {
	r := runCancelScriptWith(t, rootOf(t), invalidRunsFiles())
	if r.rc != 0 {
		t.Fatalf("rc %d\n%s", r.rc, r.out)
	}
	if got, want := strings.Join(r.ids, ","), "203,204"; got != want {
		t.Fatalf("cancelled %s, want %s (101, 205, 208 have an invalid head_sha and 7 has no id: skipped, never cancelled, never chained; 202 hangs on 101's name; 206 has no valid suffix; 203 has an empty name and is valid)\n%s", got, want, r.out)
	}
	for _, id := range []string{"101", "205", "208"} {
		if !strings.Contains(r.out, "::warning::") || !strings.Contains(r.out, "run "+id) {
			t.Errorf("no ::warning:: naming run %s:\n%s", id, r.out)
		}
	}
	if !strings.Contains(r.out, "scanned=9 candidates=2 cancelled=2 skipped_invalid=5 truncated=no\n") {
		t.Errorf("summary line is wrong:\n%s", r.out)
	}
}

// One summary line per run: scanned, candidates, cancelled, skipped_invalid, truncated. Zero candidates is exit 0 and visible.
func TestStackedQueueCancelPrintsOneSummaryLine(t *testing.T) {
	root := rootOf(t)
	count := func(out string) int { return strings.Count(out, "scanned=") }
	r := runCancelScriptWith(t, root, nil)
	if !strings.Contains(r.out, "scanned=11 candidates=4 cancelled=4 skipped_invalid=0 truncated=no\n") || count(r.out) != 1 {
		t.Errorf("default fixture summary:\n%s", r.out)
	}
	r = runCancelScriptWith(t, root, nil, "DRY_RUN=1")
	if !strings.Contains(r.out, "scanned=11 candidates=4 cancelled=0 skipped_invalid=0 truncated=no\n") || count(r.out) != 1 {
		t.Errorf("dry run summary:\n%s", r.out)
	}
	r = runCancelScriptWith(t, root, nil, "STUB_POST_RC=1")
	if !strings.Contains(r.out, "scanned=11 candidates=4 cancelled=0 skipped_invalid=0 truncated=no\n") {
		t.Errorf("failed cancels must not count as cancelled:\n%s", r.out)
	}
	r = runCancelScriptWith(t, root, nil, "HEAD_SHA="+shaD)
	if r.rc != 0 || len(r.ids) != 0 || !strings.Contains(r.out, "scanned=11 candidates=0 cancelled=0 skipped_invalid=0 truncated=no\n") {
		t.Errorf("nothing stale must be exit 0 with a visible zero:\nrc=%d\n%s", r.rc, r.out)
	}
	r = runCancelScriptWith(t, root, stalePages(5, 100))
	if r.rc != 2 || !strings.Contains(r.out, "scanned=500 candidates=500 cancelled=500 skipped_invalid=0 truncated=yes\n") {
		t.Errorf("truncated summary: rc=%d\n%s", r.rc, r.out[len(r.out)-200:])
	}
}

func TestStackedQueueCancelPlantedValidationFaults(t *testing.T) {
	root := rootOf(t)
	for _, m := range []mutation{
		{cancelScript, `.head_sha | test($sha_re)`, `.head_sha | type == "string"`},
		{cancelScript, `(.id | tostring) | test("^[0-9]+$")`, `(.id | tostring) | length > 0`},
		{cancelScript, `["I", `, `["C", `},
		{cancelScript, `skipped=$((skipped + 1))`, `skipped=$((skipped + 0))`},
	} {
		tmp := plantTree(t, root, []string{cancelScript}, m)
		r := runCancelScriptWith(t, tmp, invalidRunsFiles())
		if strings.Join(r.ids, ",") == "203,204" && strings.Contains(r.out, "skipped_invalid=5") {
			t.Errorf("planted validation fault was not caught: %q -> %q", m.old, m.new)
		}
	}
}
