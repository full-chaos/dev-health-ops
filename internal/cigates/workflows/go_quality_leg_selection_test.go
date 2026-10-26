package workflows_test

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// CHAOS-11418: a pull request without the ci:full label starts no race leg. go-quality-plan filters ci/go_quality_legs.json
// with ci/go_quality_legs.sh into the matrix of go-quality-leg; the fan-in names the policy and stays `unconditional`.

const (
	legsWorkflow = ".github/workflows/go-quality.yml"
	legsScript   = "ci/go_quality_legs.sh"
	legsFile     = "ci/go_quality_legs.json"
	racePolicy   = "race: not run on PR by policy (runs in merge_group, on the main push and with the ci:full label)"
)

func runLegsScript(t *testing.T, root string, args ...string) (stdout, stderr string, code int) {
	t.Helper()
	cmd := exec.Command("bash", append([]string{filepath.Join(root, legsScript)}, args...)...)
	cmd.Env = []string{"PATH=/usr/bin:/bin:/usr/local/bin"}
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		ee, ok := err.(*exec.ExitError)
		if !ok {
			t.Fatalf("running %s: %v", legsScript, err)
		}
		code = ee.ExitCode()
	}
	return out.String(), errb.String(), code
}

func legNames(t *testing.T, root string, matrixJSON string) []string {
	t.Helper()
	dir := t.TempDir()
	f := filepath.Join(dir, "m.json")
	if err := os.WriteFile(f, []byte(matrixJSON), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("jq", "-r", ".[].name", f)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("jq: %v", err)
	}
	return strings.Fields(string(out))
}

func TestLegScriptSelectsRaceLegsByEvent(t *testing.T) {
	root := repoRoot(t)
	all := []string{"static", "test-1-of-3", "test-2-of-3", "test-3-of-3", "race-1-of-4", "race-2-of-4", "race-3-of-4", "race-4-of-4"}
	noRace := all[:4]
	for _, c := range []struct {
		event, full string
		want        []string
		policy      string
	}{
		{"pull_request", "false", noRace, racePolicy},
		{"pull_request", "true", all, ""},
		{"merge_group", "false", all, ""},
		{"push", "false", all, ""},
		{"schedule", "false", all, ""},
		{"workflow_dispatch", "false", all, ""},
		{"an_event_nobody_knows", "false", all, ""},
	} {
		t.Run(c.event+"/ci_full="+c.full, func(t *testing.T) {
			out, errs, code := runLegsScript(t, root, c.event, c.full)
			if code != 0 {
				t.Fatalf("exit %d: %s", code, errs)
			}
			lines := strings.Split(out, "\n")
			if len(lines) < 2 {
				t.Fatalf("want a matrix line and a policy line, got %q", out)
			}
			if got := legNames(t, root, lines[0]); strings.Join(got, ",") != strings.Join(c.want, ",") {
				t.Fatalf("legs = %v, want %v", got, c.want)
			}
			if lines[1] != c.policy {
				t.Fatalf("policy line = %q, want %q", lines[1], c.policy)
			}
		})
	}
}

func TestLegScriptRefusesAnEmptyOrBrokenLegList(t *testing.T) {
	root := repoRoot(t)
	for _, c := range []struct {
		name, content string
		args          []string
		wantCode      int
	}{
		{"empty array", `[]`, []string{"merge_group", "false"}, 3},
		{"only heavy legs on a pull request", `[{"name":"race-1-of-4","args":"race 1 4","heavy":true}]`, []string{"pull_request", "false"}, 3},
		{"no static leg", `[{"name":"test-1-of-1","args":"test 1 1"}]`, []string{"merge_group", "false"}, 3},
		{"not an array", `{"name":"static"}`, []string{"merge_group", "false"}, 3},
		{"not json", `static`, []string{"merge_group", "false"}, 5},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := filepath.Join(t.TempDir(), "legs.json")
			if err := os.WriteFile(f, []byte(c.content), 0o644); err != nil {
				t.Fatal(err)
			}
			out, _, code := runLegsScript(t, root, append(c.args, f)...)
			if code == 0 || strings.Contains(out, "static") {
				t.Fatalf("exit %d, stdout %q: a broken leg list must fail the plan job", code, out)
			}
		})
	}
	if _, _, code := runLegsScript(t, root, "pull_request", "maybe"); code != 2 {
		t.Fatalf("a bad ci:full flag must exit 2, got %d", code)
	}
	if _, _, code := runLegsScript(t, root, "pull_request", "false", filepath.Join(t.TempDir(), "missing.json")); code == 0 {
		t.Fatal("a missing leg file must fail")
	}
}

// A missing or skipped leg job (an unusable legs output) and a failed plan job turn the required go-quality context red.
func TestFanInIsRedWhenTheLegsOutputIsUnusable(t *testing.T) {
	for _, c := range []struct{ name, leg, plan string }{
		{"leg job skipped", "skipped", "success"},
		{"plan job failed, legs skipped", "skipped", "failure"},
		{"leg job failed", "failure", "success"},
		{"leg job cancelled", "cancelled", "success"},
		{"plan job skipped", "success", "skipped"},
	} {
		t.Run(c.name, func(t *testing.T) {
			v := runGate(t, map[string]string{
				"GATE_NAME": "go-quality", "EVENT_NAME": "pull_request", "GATE_HAS_SELECTOR": "false",
				"GATED_JOB_1": "go-quality-leg|unconditional|" + c.leg, "GATED_JOB_2": "go-quality-plan|unconditional|" + c.plan,
			})
			if v.code == 0 {
				t.Fatalf("go-quality passed with leg=%s plan=%s: %s", c.leg, c.plan, v.stdout)
			}
		})
	}
	ok := runGate(t, map[string]string{
		"GATE_NAME": "go-quality", "EVENT_NAME": "pull_request", "GATE_HAS_SELECTOR": "false",
		"GATED_JOB_1": "go-quality-leg|unconditional|success", "GATED_JOB_2": "go-quality-plan|unconditional|success",
	})
	if ok.code != 0 {
		t.Fatalf("go-quality must pass when every started leg and the plan passed: %s %s", ok.stdout, ok.stderr)
	}
}

func legSelectionViolations(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	add := func(f string, a ...any) { out = append(out, sprintf(f, a...)) }
	doc := loadWf(t, root, "go-quality.yml")
	jobs := jobsOf(doc)
	leg, plan, fan := asMap(jobs["go-quality-leg"]), asMap(jobs["go-quality-plan"]), asMap(jobs["go-quality"])

	if got := asString(asMap(asMap(leg["strategy"])["matrix"])["include"]); got != "${{ fromJSON(needs.go-quality-plan.outputs.legs) }}" {
		add("go-quality-leg matrix.include must be ${{ fromJSON(needs.go-quality-plan.outputs.legs) }}, got %q: a static list starts every race leg on a pull request", got)
	}
	outs := asMap(plan["outputs"])
	if asString(outs["legs"]) != "${{ steps.legs.outputs.legs }}" || asString(outs["race_policy"]) != "${{ steps.legs.outputs.race_policy }}" {
		add("go-quality-plan must publish outputs legs and race_policy from step `legs`, got %v", outs)
	}
	legsAt, planAt := -1, -1
	for i, s := range stepsOf(plan) {
		switch asString(s["id"]) {
		case "legs":
			legsAt = i
			run := shellCode(asString(s["run"]))
			if !strings.Contains(run, "\n"+`bash ci/go_quality_legs.sh "${EVENT_NAME}" "${CI_FULL}" > "${out}"`+"\n") {
				add("step legs must run exactly `bash ci/go_quality_legs.sh \"${EVENT_NAME}\" \"${CI_FULL}\" > \"${out}\"`: a third argument would read another leg file")
			}
			if n := strings.Count(run, "go_quality_legs"); n != 1 {
				add("step legs names go_quality_legs %d times, want once (the script call): nothing else may produce or rewrite the output", n)
			}
			if n := strings.Count(run, "legs="); n != 1 || !strings.Contains(run, `printf 'legs=%s\n' "$(sed -n 1p "${out}")"`) {
				add("step legs must write legs= once, from line 1 of the script output")
			}
			env := asMap(s["env"])
			if asString(env["CI_FULL"]) != "${{ contains(github.event.pull_request.labels.*.name, 'ci:full') && 'true' || 'false' }}" || asString(env["EVENT_NAME"]) != "${{ github.event_name }}" {
				add("step legs must take the event name and the ci:full label from the event, got %v", env)
			}
		case "plan":
			planAt = i
		}
	}
	if legsAt < 0 {
		add("go-quality-plan has no step `legs`")
	} else if planAt >= 0 && legsAt > planAt {
		add("step `legs` must come before the fallible plan step")
	}

	// (c) no other step of any job writes a `legs=` output or reads the leg file
	for jn, jv := range jobs {
		for _, st := range stepsOf(jv) {
			if jn == "go-quality-plan" && asString(st["id"]) == "legs" {
				continue
			}
			if r := asString(st["run"]); strings.Contains(r, "legs=") || strings.Contains(r, "go_quality_legs") {
				add("job %s step %q writes a legs output or names the leg file: the legs step is the only producer", jn, asString(st["name"]))
			}
		}
	}
	for jn, jv := range jobs {
		if _, ok := asMap(asMap(jv)["outputs"])["legs"]; ok && jn != "go-quality-plan" {
			add("job %s publishes an output named legs: only go-quality-plan may", jn)
		}
	}

	// (b) the script reads exactly ci/go_quality_legs.json and merges nothing else into the list
	script := shellCode(readFile(t, filepath.Join(root, legsScript)))
	if !strings.Contains(script, `legs="${3:-${here}/go_quality_legs.json}"`) {
		add("%s must default its leg file to the literal ${here}/go_quality_legs.json", legsScript)
	}
	var jqLines []string
	for _, l := range strings.Split(script, "\n") {
		if strings.Contains(l, "jq ") {
			jqLines = append(jqLines, strings.TrimSpace(l))
		}
	}
	wantJQ := []string{
		`matrix="$(jq -ce '[.[] | select((.heavy // false) | not)]' "${legs}")"`,
		`matrix="$(jq -ce '.' "${legs}")"`,
		`if ! jq -e 'type == "array" and length > 0 and any(.[]; .name == "static")' <<<"${matrix}" >/dev/null; then`,
	}
	if !reflect.DeepEqual(jqLines, wantJQ) {
		add("%s runs jq as %q, want exactly %q: the matrix is the filtered leg file and nothing else", legsScript, jqLines, wantJQ)
	}
	if n := strings.Count(script, "go_quality_legs.json"); n != 1 {
		add("%s names go_quality_legs.json %d times, want once", legsScript, n)
	}

	var relevance map[string]any
	for _, s := range stepsOf(leg) {
		if asString(s["id"]) == "relevance" {
			relevance = s
		}
	}
	if relevance == nil {
		add("go-quality-leg has no step `relevance`")
	} else {
		run := shellCode(asString(relevance["run"]))
		i := strings.Index(run, `if [ "${HEAVY_ON_PR_WITHOUT_LABEL}" = "true" ]; then`)
		j := -1
		if i >= 0 {
			j = strings.Index(run[i:], "\nfi\n")
		}
		if i < 0 || j < 0 {
			add("the relevance step lost its HEAVY_ON_PR_WITHOUT_LABEL guard")
		} else if block := run[i : i+j]; !strings.Contains(block, "exit 1") || strings.Contains(block, "relevant=") || strings.Contains(block, "exit 0") {
			add("a race leg that starts on a pull request without ci:full must FAIL (exit 1), never report relevant=false or exit 0")
		}
	}

	var report map[string]any
	for _, s := range stepsOf(fan) {
		if asString(s["name"]) == "Report the race policy" {
			report = s
		}
	}
	switch {
	case report == nil:
		add("the go-quality fan-in has no step `Report the race policy`")
	default:
		if asString(asMap(report["env"])["RACE_POLICY"]) != "${{ needs.go-quality-plan.outputs.race_policy }}" || !strings.Contains(asString(report["run"]), `>> "${GITHUB_STEP_SUMMARY}"`) {
			add("the race policy step must print needs.go-quality-plan.outputs.race_policy to the log and the step summary")
		}
		if asString(report["if"]) != "always() && needs.go-quality-plan.outputs.race_policy != ''" {
			add("the race policy step must run with always() when a policy was set, got if: %q", asString(report["if"]))
		}
	}
	if asString(fan["if"]) != "always()" {
		add("the go-quality fan-in must stay if: always()")
	}
	return out
}

func sprintf(f string, a ...any) string { return fmt.Sprintf(f, a...) }

func TestGoQualityLegSelectionWiring(t *testing.T) {
	expectNoViolations(t, legSelectionViolations(t, repoRoot(t)))
}

func TestGoQualityLegSelectionPlantedDefects(t *testing.T) {
	root := repoRoot(t)
	files := []string{legsWorkflow, legsScript, legsFile}
	for _, c := range []struct {
		name string
		muts []mutation
		want []string
	}{
		{"a static matrix comes back", []mutation{{legsWorkflow, "include: ${{ fromJSON(needs.go-quality-plan.outputs.legs) }}", "include:\n          - name: static\n            args: static"}}, []string{"matrix.include must be"}},
		{"the matrix points at another output", []mutation{{legsWorkflow, "include: ${{ fromJSON(needs.go-quality-plan.outputs.legs) }}", "include: ${{ fromJSON(needs.go-quality-plan.outputs.race_policy) }}"}}, []string{"matrix.include must be"}},
		{"the producer is handed another file", []mutation{{legsWorkflow, `"${CI_FULL}" > "${out}"`, `"${CI_FULL}" ci/other_legs.json > "${out}"`}}, []string{"third argument"}},
		{"the script reads another file", []mutation{{legsScript, `${here}/go_quality_legs.json`, `${here}/other_legs.json`}}, []string{"literal ${here}/go_quality_legs.json"}},
		{"a second file is concatenated into the list", []mutation{{legsScript, `matrix="$(jq -ce '.' "${legs}")"`, `matrix="$(jq -ce -s 'add' "${legs}" "${here}/extra.json")"`}}, []string{"runs jq as"}},
		{"another step rewrites the legs output", []mutation{{legsWorkflow, "      - name: Configure self-hosted Go cache paths\n        if: vars.SELF_HOSTED_RUNNERS == 'enabled' && ((github.event_name == 'pull_request' && github.event.pull_request.head.repo.full_name == github.repository) || (github.event_name == 'push' && github.ref == 'refs/heads/main') || (github.event_name == 'workflow_dispatch' && github.event.inputs.ref == '') || github.event_name == 'release' || github.event_name == 'schedule')\n        run: |\n          echo \"GOMODCACHE=/mnt/go-cache/mod\" >> \"${GITHUB_ENV}\"\n          echo \"GOCACHE=/mnt/go-cache/build\" >> \"${GITHUB_ENV}\"\n\n      - name: Set up Go\n        uses: actions/setup-go@b7ad1dad31e06c5925ef5d2fc7ad053ef454303e # v7.0.0\n        with:\n          go-version-file: go.mod\n          cache-dependency-path", "      - name: Rewrite the legs\n        run: echo 'legs=[]' >> \"${GITHUB_OUTPUT}\"\n\n      - name: Set up Go\n        uses: actions/setup-go@b7ad1dad31e06c5925ef5d2fc7ad053ef454303e # v7.0.0\n        with:\n          go-version-file: go.mod\n          cache-dependency-path"}}, []string{"only producer"}},
		{"the plan stops publishing the legs", []mutation{{legsWorkflow, "      legs: ${{ steps.legs.outputs.legs }}\n", ""}}, []string{"must publish outputs legs"}},
		{"the legs step runs after the plan step", []mutation{{legsWorkflow, "        id: legs\n", "        id: legs2\n"}}, []string{"no step `legs`"}},
		{"the legs step ignores the label", []mutation{{legsWorkflow, "CI_FULL: ${{ contains(github.event.pull_request.labels.*.name, 'ci:full') && 'true' || 'false' }}", "CI_FULL: 'true'"}}, []string{"ci:full label from the event"}},
		{"a racing leg on a pull request reports green again", []mutation{{legsWorkflow, `echo "::error title=RACE LEG STARTED ON A PULL REQUEST WITHOUT ci:full::${LEG} must not be in this matrix (see the legs step of go-quality-plan)"
            exit 1`, `printf 'relevant=false\n' >> "${GITHUB_OUTPUT}"
            exit 0`}}, []string{"must FAIL"}},
		{"the race policy step is dropped", []mutation{{legsWorkflow, "name: Report the race policy", "name: Report something"}}, []string{"no step `Report the race policy`"}},
		{"the race policy never reaches the summary", []mutation{{legsWorkflow, `echo "${RACE_POLICY}" >> "${GITHUB_STEP_SUMMARY}"`, `echo "${RACE_POLICY}"`}}, []string{"step summary"}},
		{"the fan-in can skip", []mutation{{legsWorkflow, "\n    if: always()\n    needs: [go-quality-plan, go-quality-leg]", "\n    if: success()\n    needs: [go-quality-plan, go-quality-leg]"}}, []string{"must stay if: always()"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			tree := plantTree(t, root, files, c.muts...)
			expectViolation(t, legSelectionViolations(t, tree), c.want...)
		})
	}
}

// The script is the plan: a pull request that keeps race legs, or an event that loses them, is caught by the table above.
func TestLegScriptPlantedDefects(t *testing.T) {
	root := repoRoot(t)
	files := []string{legsScript, legsFile}
	for _, c := range []struct {
		name string
		mut  mutation
	}{
		{"race stays on a pull request", mutation{legsScript, `"${ci_full}" = false ]; then`, `"${ci_full}" = maybe ]; then`}},
		{"race leaves the merge queue", mutation{legsScript, `[ "${event}" = pull_request ]`, `[ "${event}" != push ]`}},
	} {
		t.Run(c.name, func(t *testing.T) {
			tree := plantTree(t, root, files, c.mut)
			if err := os.Chmod(filepath.Join(tree, legsScript), 0o755); err != nil {
				t.Fatal(err)
			}
			wrong := 0
			for _, e := range [][2]string{{"pull_request", "false"}, {"merge_group", "false"}} {
				out, _, code := runLegsScript(t, tree, e[0], e[1])
				hasRace := strings.Contains(out, "race-1-of-4")
				if code != 0 || hasRace != (e[0] == "merge_group") {
					wrong++
				}
			}
			if wrong == 0 {
				t.Fatal("the planted defect changed no leg selection: the table test would not catch it")
			}
		})
	}
}
