package workflows_test

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// A workflow_dispatch of codeql-analysis.yml with input `pr` analyses that pull request's CURRENT merge commit and
// uploads it for refs/pull/<pr>/merge, so a PR that only went stale when main moved gets the CodeQL result the
// code_scanning rule wants without a full CI rerun. The refresh is the same `analyze` job (same analysis key, languages,
// queries and category as every other run), fed by a `refresh-target` job that refuses forks, closed, non-default-base,
// unmergeable and moved pull requests.

const codeqlWf = "codeql-analysis.yml"

var refreshResolveGuards = []string{
	`[[ "${PR}" =~ ^[1-9][0-9]{0,6}$ ]] || refuse`,
	`[ "${state}" = "open" ] || refuse`,
	`[ "${head_repo}" = "${REPO}" ] || refuse`,
	`[ "${base_ref}" = "${DEFAULT_BRANCH}" ] || refuse`,
	`[ "${mergeable}" = "true" ] || refuse`,
	`gh api "repos/${REPO}/git/ref/pull/${PR}/merge" --jq '.object.sha'`,
	`[ "${#parents[@]}" = "2" ] || refuse`,
	`[ "${parents[0]}" = "${base_sha}" ] || refuse`,
	`[ "${parents[1]}" = "${head_sha}" ] || refuse`,
	`echo "ref=refs/pull/${PR}/merge" >> "$GITHUB_OUTPUT"`,
	`echo "sha=${merge_sha}" >> "$GITHUB_OUTPUT"`,
}

func stepUsing(steps []yamlMap, prefix string) []yamlMap {
	var out []yamlMap
	for _, s := range steps {
		if strings.HasPrefix(asString(s["uses"]), prefix) {
			out = append(out, s)
		}
	}
	return out
}

func strMap(v any) map[string]string {
	out := map[string]string{}
	for k, x := range asMap(v) {
		out[k] = fmt.Sprint(x)
	}
	return out
}

func codeqlRefreshDefects(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	add := func(f string, a ...any) { out = append(out, fmt.Sprintf(f, a...)) }
	doc := loadWf(t, root, codeqlWf)

	pr := asMap(asMap(asMap(triggerEvents(doc)["workflow_dispatch"])["inputs"])["pr"])
	if pr == nil {
		add("workflow_dispatch has no input pr")
	} else if asString(pr["type"]) != "string" || pr["required"] != false || fmt.Sprint(pr["default"]) != "" {
		add("workflow_dispatch input pr is %v, want type string, required false, default empty (a plain dispatch must still analyse its ref)", pr)
	}

	jobs := jobsOf(doc)
	target := asMap(jobs["refresh-target"])
	if target == nil {
		add("job refresh-target is absent")
		return out
	}
	if got, want := asString(target["if"]), "github.event_name == 'workflow_dispatch' && inputs.pr != ''"; got != want {
		add("refresh-target if is %q, want %q", got, want)
	}
	if got, want := strMap(target["permissions"]), map[string]string{"contents": "read", "pull-requests": "read"}; !reflect.DeepEqual(got, want) {
		add("refresh-target permissions are %v, want exactly %v", got, want)
	}
	tsteps := stepsOf(target)
	if len(stepUsing(tsteps, "actions/checkout")) != 0 {
		add("refresh-target checks out code; it must run no pull request code")
	}
	var resolve yamlMap
	for _, s := range tsteps {
		if asString(s["id"]) == "resolve" {
			resolve = s
		}
	}
	if resolve == nil {
		add("refresh-target has no step with id resolve")
		return out
	}
	env := strMap(resolve["env"])
	for k, want := range map[string]string{"GH_TOKEN": "${{ github.token }}", "REPO": "${{ github.repository }}", "DEFAULT_BRANCH": "${{ github.event.repository.default_branch }}", "PR": "${{ inputs.pr }}"} {
		if env[k] != want {
			add("refresh-target resolve env %s is %q, want %q", k, env[k], want)
		}
	}
	run := shellCode(asString(resolve["run"]))
	if strings.Contains(run, "${{") {
		add("refresh-target resolve interpolates an expression into its script; pass it through env")
	}
	for _, g := range refreshResolveGuards {
		if !strings.Contains(run, g) {
			add("refresh-target resolve does not contain the guard %q", g)
		}
	}
	if got := strMap(target["outputs"]); got["ref"] != "${{ steps.resolve.outputs.ref }}" || got["sha"] != "${{ steps.resolve.outputs.sha }}" {
		add("refresh-target outputs are %v, want ref and sha from steps.resolve", got)
	}

	analyze := asMap(jobs["analyze"])
	if fmt.Sprint(pinNeeds(analyze)) != fmt.Sprint([]string{"changes", "refresh-target"}) {
		add("analyze needs %v, want [changes refresh-target]", pinNeeds(analyze))
	}
	if !strings.Contains(asString(analyze["if"]), "(needs.refresh-target.result == 'success' || !(github.event_name == 'workflow_dispatch' && inputs.pr != ''))") {
		add("analyze if does not require refresh-target success on a pr dispatch: a refused target would analyse and upload the dispatched ref instead")
	}
	if got, want := strMap(analyze["permissions"]), map[string]string{"actions": "read", "contents": "read", "security-events": "write"}; !reflect.DeepEqual(got, want) {
		add("analyze permissions are %v, want exactly %v", got, want)
	}
	if lang := fmt.Sprint(asMap(asMap(analyze["strategy"])["matrix"])["language"]); lang != "[go]" {
		add("analyze matrix language is %s, want [go]", lang)
	}
	steps := stepsOf(analyze)
	co := stepUsing(steps, "actions/checkout@")
	if len(co) != 1 {
		add("analyze has %d checkout steps, want 1", len(co))
	} else {
		w := asMap(co[0]["with"])
		if asString(w["ref"]) != "${{ needs.refresh-target.outputs.sha }}" {
			add("analyze checkout ref is %q, want the resolved merge sha: the upload's commit is the checked-out HEAD", asString(w["ref"]))
		}
		if w["persist-credentials"] != false {
			add("analyze checkout must set persist-credentials: false")
		}
	}
	initSteps := stepUsing(steps, "github/codeql-action/init@")
	if len(initSteps) != 1 || !reflect.DeepEqual(strMap(initSteps[0]["with"]), map[string]string{"languages": "${{ matrix.language }}", "queries": "security-extended,security-and-quality", "dependency-caching": "none", "trap-caching": "${{ " + mainOnlyGate + " }}"}) {
		add("analyze init is not the one CodeQL config (languages ${{ matrix.language }}, queries security-extended,security-and-quality, dependency-caching none, trap-caching the main-only gate)")
	}
	an := stepUsing(steps, "github/codeql-action/analyze@")
	if len(an) != 1 {
		add("analyze has %d codeql analyze steps, want 1", len(an))
	} else if got, want := strMap(an[0]["with"]), map[string]string{"category": "/language:${{matrix.language}}", "ref": "${{ needs.refresh-target.outputs.ref }}", "sha": "${{ needs.refresh-target.outputs.sha }}"}; !reflect.DeepEqual(got, want) {
		add("analyze step with is %v, want %v", got, want)
	}
	var moved yamlMap
	for _, s := range steps {
		if asString(s["name"]) == "Check the merge commit did not move" {
			moved = s
		}
	}
	if moved == nil {
		add("analyze has no step re-reading refs/pull/<pr>/merge after the upload: a run could end green for a sha that is no longer the merge commit")
	} else {
		menv := strMap(moved["env"])
		mrun := shellCode(asString(moved["run"]))
		if asString(moved["if"]) != "needs.refresh-target.outputs.sha != ''" || menv["ANALYSED_SHA"] != "${{ needs.refresh-target.outputs.sha }}" || menv["PR"] != "${{ inputs.pr }}" ||
			!strings.Contains(mrun, `now=$(gh api "repos/${REPO}/git/ref/pull/${PR}/merge" --jq '.object.sha')`) ||
			!strings.Contains(mrun, `if [ "${now}" != "${ANALYSED_SHA}" ]; then`) ||
			!strings.Contains(mrun, "stale merge sha, re-dispatch") || !strings.Contains(mrun, "exit 1") {
			add("analyze's merge-commit re-read is not the pr-dispatch check that fails with \"stale merge sha, re-dispatch\" when refs/pull/<pr>/merge moved")
		}
		if asString(steps[len(steps)-1]["name"]) != "Check the merge commit did not move" {
			add("analyze's merge-commit re-read is not its last step")
		}
	}
	// The analysis key is <workflow path>:<job id>; an override would make a configuration main does not have.
	if strings.Contains(readFile(t, root+"/.github/workflows/"+codeqlWf), "CODEQL_ACTION_ANALYSIS_KEY") {
		add("the workflow sets CODEQL_ACTION_ANALYSIS_KEY; the refresh must keep the key codeql-analysis.yml:analyze")
	}

	group, cancel := concurrencyOf(doc)
	ctx := func(c ctxMap, pr string) ctxMap {
		out := withInputsAction(c)
		out["github.event.inputs.pr"] = pr
		return out
	}
	for _, c := range []struct {
		label     string
		ctx       ctxMap
		wantGroup string
		cancel    bool
	}{
		{"pr dispatch from main", ctx(eventContext("workflow_dispatch", "refs/heads/main", "", "main", "", "mainsha1"), "7"), "W-pr-7", false},
		{"pr dispatch from a branch", ctx(dispatchCtx, "7"), "W-pr-7", false},
		{"plain dispatch", ctx(dispatchCtx, ""), "W-refs/heads/topic-branch", false},
		{"pull_request", ctx(prCtx, ""), "W-7", true},
	} {
		g, err := renderGroup(group, c.ctx)
		if err != nil {
			add("%s: %v", c.label, err)
		} else if g != c.wantGroup {
			add("%s: concurrency group is %q, want %q (a pr refresh is one group per pull request, apart from that PR's own runs)", c.label, g, c.wantGroup)
		}
		if got, err := cancels(cancel, c.ctx); err != nil || got != c.cancel {
			add("%s: cancel-in-progress is %v (%v), want %v", c.label, got, err, c.cancel)
		}
	}
	return out
}

func TestCodeQLRefreshWiring(t *testing.T) {
	expectNoViolations(t, codeqlRefreshDefects(t, repoRoot(t)))
}

func TestCodeQLRefreshPlantedDefects(t *testing.T) {
	root := repoRoot(t)
	const wf = ".github/workflows/" + codeqlWf
	files := []string{wf}
	for _, c := range []struct {
		name string
		m    mutation
		want string
	}{
		{"sha not passed to analyze", mutation{wf, "          sha: ${{ needs.refresh-target.outputs.sha }}\n", ""}, "analyze step with is"},
		{"fork guard dropped", mutation{wf, `[ "${head_repo}" = "${REPO}" ] || refuse`, `true || refuse`}, `guard "[ \"${head_repo}\" = \"${REPO}\" ] || refuse"`},
		{"closed guard dropped", mutation{wf, `[ "${state}" = "open" ] || refuse`, `true || refuse`}, `guard "[ \"${state}\" = \"open\" ] || refuse"`},
		{"mergeable guard dropped", mutation{wf, `[ "${mergeable}" = "true" ] || refuse`, `true || refuse`}, `guard "[ \"${mergeable}\" = \"true\" ] || refuse"`},
		{"parent1 guard dropped", mutation{wf, `[ "${parents[0]}" = "${base_sha}" ] || refuse`, `true || refuse`}, `guard "[ \"${parents[0]}\" = \"${base_sha}\" ] || refuse"`},
		{"parent2 guard dropped", mutation{wf, `[ "${parents[1]}" = "${head_sha}" ] || refuse`, `true || refuse`}, `guard "[ \"${parents[1]}\" = \"${head_sha}\" ] || refuse"`},
		{"ref without sha", mutation{wf, "          ref: ${{ needs.refresh-target.outputs.ref }}\n", ""}, "analyze step with is"},
		{"push and schedule get a target", mutation{wf, "    if: github.event_name == 'workflow_dispatch' && inputs.pr != ''\n", "    if: always()\n"}, "refresh-target if is"},
		{"end re-read dropped", mutation{wf, `          if [ "${now}" != "${ANALYSED_SHA}" ]; then`, `          if false; then`}, "stale merge sha, re-dispatch"},
		{"checkout of the dispatched ref", mutation{wf, "          ref: ${{ needs.refresh-target.outputs.sha }}\n          persist-credentials: false\n", "          persist-credentials: false\n"}, "analyze checkout ref"},
		{"refresh-target can write", mutation{wf, "      contents: read\n      pull-requests: read\n", "      contents: read\n      pull-requests: read\n      security-events: write\n"}, "refresh-target permissions"},
		{"analyze runs after a refused target", mutation{wf, " && (needs.refresh-target.result == 'success' || !(github.event_name == 'workflow_dispatch' && inputs.pr != ''))", ""}, "analyze if does not require"},
		{"pr dispatch shares the PR's own group", mutation{wf, "${{ github.event.inputs.pr && 'pr-' || '' }}", ""}, `pr dispatch from main: concurrency group is "W-7"`},
		{"analysis key overridden", mutation{wf, "      - name: Initialize CodeQL\n", "      - name: Initialize CodeQL\n        env:\n          CODEQL_ACTION_ANALYSIS_KEY: refresh\n"}, "CODEQL_ACTION_ANALYSIS_KEY"},
		{"other queries", mutation{wf, "queries: security-extended,security-and-quality", "queries: security-extended"}, "one CodeQL config"},
	} {
		t.Run(c.name, func(t *testing.T) {
			tree := plantTree(t, root, files, c.m)
			expectViolation(t, codeqlRefreshDefects(t, tree), c.want)
		})
	}
}
