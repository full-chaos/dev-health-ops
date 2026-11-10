package workflows_test

import (
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"
)

// The ready flip runs a workflow again only when a job in it reads the draft state. A workflow that keeps
// ready_for_review carries a ready-reuse job, and every leg that can skip for reuse names a declared unit; its gate
// counts the skip only through that unit. A workflow without ready_for_review must read no draft state, so the
// checks of its last opened/synchronize run stand on the head sha. Each required check (ruleset "main") maps to one
// workflow here, so a new required check is classified before it can lose its run at the flip.

// requiredCheckWorkflow: the ruleset's required check names and the workflow whose job reports each one.
var requiredCheckWorkflow = map[string]string{
	"test":                   "test.yml",
	"go-quality":             "go-quality.yml",
	"metrics-executed-proof": "live-e2e.yml",
	"live-e2e":               "live-e2e.yml",
	"pr-title":               "pr-title.yml",
	"auth-gate":              "go-auth-service.yml",
	"go-queue-gate":          "go.yml",
	"image-queue-gate":       "docker-images.yml",
}

// readyWorkflows keep ready_for_review: a job in each reads the draft state.
var readyWorkflows = []string{
	"codeql-analysis.yml", "docker-images.yml", "go-auth-service.yml", "go-workers-compose-proof.yml", "go.yml",
	"security-scan.yml", "venue-oracles-changed.yml",
}

// noReuseDraftJobs: draft-gated jobs that run again at the flip on purpose, with the reason.
var noReuseDraftJobs = map[string]string{
	"go-auth-service.yml/auth-changes":      "the selector: the gate and every leg read its relevance outputs",
	"codeql-analysis.yml/analyze-docs-only": "uploads the empty CodeQL result for this run's merge commit, which mergegate reads",
}

const reuseJob = "ready-reuse"

var (
	reuseClauseRe = regexp.MustCompile(`!contains\(fromJSON\(needs\.ready-reuse\.outputs\.passed\), '([a-z0-9-]+)'\)`)
	draftReadRe   = regexp.MustCompile(`github\.event\.pull_request\.draft|github\.event\.action|PR_DRAFT`)
	gatedJobRe    = regexp.MustCompile(`^([^|]+)\|([^|]+)\|(.*)$`)
)

// topLevelOr reports a || outside every paren and quoted string.
func topLevelOr(e string) bool {
	depth, quoted := 0, false
	for i := 0; i < len(e); i++ {
		switch c := e[i]; {
		case c == '\'':
			quoted = !quoted
		case quoted:
		case c == '(':
			depth++
		case c == ')':
			depth--
		case depth == 0 && strings.HasPrefix(e[i:], "||"):
			return true
		}
	}
	return false
}

func readyReuseUnits(job yamlMap) (map[string]bool, string) {
	steps := stepsOf(job)
	if len(steps) != 2 {
		return nil, fmt.Sprintf("has %d steps, want 2 (checkout of the judge from the base commit, the judge)", len(steps))
	}
	units := map[string]bool{}
	for _, line := range strings.Split(asString(asMap(steps[1]["env"])["READY_REUSE_UNITS"]), "\n") {
		if f := strings.Fields(line); len(f) >= 5 {
			units[f[0]] = true
		}
	}
	return units, ""
}

func readyReuseJobDefects(job yamlMap) []string {
	var out []string
	if asString(job["name"]) != reuseJob {
		out = append(out, "name is not "+reuseJob)
	}
	if job["needs"] != nil {
		out = append(out, "needs nothing: every leg waits on it")
	}
	perms := asMap(job["permissions"])
	if perms["actions"] != "read" || perms["contents"] != "read" {
		out = append(out, fmt.Sprintf("permissions %v, want actions: read and contents: read", perms))
	}
	for k, v := range perms {
		if v != "read" {
			out = append(out, fmt.Sprintf("permission %s: %v; the judge only reads", k, v))
		}
	}
	if got := asString(asMap(job["outputs"])["passed"]); got != "${{ steps.reuse.outputs.passed || '[]' }}" {
		out = append(out, "outputs.passed is "+got+", want the step output with a [] fallback (a skipped or failed judge reuses nothing)")
	}
	steps := stepsOf(job)
	if len(steps) != 2 {
		return append(out, fmt.Sprintf("has %d steps, want 2", len(steps)))
	}
	co, judge := steps[0], steps[1]
	readyIf := "github.event_name == 'pull_request' && github.event.action == 'ready_for_review'"
	for i, s := range steps {
		if !strings.HasPrefix(asString(s["if"]), readyIf) {
			out = append(out, fmt.Sprintf("step %d runs off a ready flip (if %q)", i+1, asString(s["if"])))
		}
	}
	with := asMap(co["with"])
	if !strings.HasPrefix(asString(co["uses"]), "actions/checkout@") || with["ref"] != "${{ github.event.pull_request.base.sha }}" ||
		with["sparse-checkout"] != "ci/ready_reuse.sh" || with["persist-credentials"] != false {
		out = append(out, fmt.Sprintf("step 1 must check out only ci/ready_reuse.sh from the BASE commit without credentials (got uses %v with %v)", co["uses"], with))
	}
	run := asString(judge["run"])
	if asString(judge["id"]) != "reuse" || !strings.Contains(run, "[ ! -f ci/ready_reuse.sh ]") || !strings.Contains(run, "echo 'passed=[]'") ||
		!strings.HasSuffix(strings.TrimSpace(run), "bash ci/ready_reuse.sh") {
		out = append(out, "step 2 (id reuse) must pass [] when the base has no judge, else run bash ci/ready_reuse.sh")
	}
	env := asMap(judge["env"])
	for k, v := range map[string]string{
		"GH_TOKEN": "${{ github.token }}", "REPO": "${{ github.repository }}", "RUN_ID": "${{ github.run_id }}",
		"EVENT_NAME": "${{ github.event_name }}", "EVENT_ACTION": "${{ github.event.action }}",
		"HEAD_SHA": "${{ github.event.pull_request.head.sha }}", "MERGE_SHA": "${{ github.sha }}",
		"PR_NUMBER": "${{ github.event.pull_request.number }}",
	} {
		if env[k] != v {
			out = append(out, fmt.Sprintf("step 2 env %s is %v, want %s", k, env[k], v))
		}
	}
	return out
}

func readyReuseWiringViolations(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	add := func(f string, a ...any) { out = append(out, fmt.Sprintf(f, a...)) }
	for check, wf := range requiredCheckWorkflow {
		found := false
		for _, j := range jobsOf(loadWf(t, root, wf)) {
			if asString(asMap(j)["name"]) == check {
				found = true
			}
		}
		for id, j := range jobsOf(loadWf(t, root, wf)) {
			if id == check && asMap(j)["name"] == nil {
				found = true
			}
		}
		if !found {
			add("required check %s: no job of %s reports it", check, wf)
		}
	}
	for _, name := range workflowNames(t, root) {
		doc := loadWf(t, root, name)
		ev := triggerEvents(doc)
		if _, hasPR := ev["pull_request"]; !hasPR {
			continue
		}
		ready := slices.Contains(prTypes(ev), "ready_for_review")
		wantReady := slices.Contains(readyWorkflows, name)
		jobs := jobsOf(doc)
		if !ready {
			if wantReady {
				add("%s: lost ready_for_review, but its jobs read the draft state: a draft skip would stand on the head sha", name)
			}
			if _, has := jobs[reuseJob]; has {
				add("%s: has a %s job but no ready_for_review trigger", name, reuseJob)
			}
			for _, id := range sortedKeys(jobs) {
				if m := draftReadRe.FindString(fmt.Sprint(jobs[id])); m != "" {
					add("%s: job %s reads %s without a ready_for_review trigger: its draft verdict would stand at the flip", name, id, m)
				}
			}
			continue
		}
		if !wantReady {
			add("%s: triggers on ready_for_review; classify it (readyWorkflows) or drop the trigger", name)
		}
		rj, has := jobs[reuseJob]
		if !has {
			add("%s: triggers on ready_for_review but has no %s job: the flip reruns every leg", name, reuseJob)
			continue
		}
		for _, d := range readyReuseJobDefects(asMap(rj)) {
			add("%s: %s: %s", name, reuseJob, d)
		}
		units, bad := readyReuseUnits(asMap(rj))
		if bad != "" {
			add("%s: %s %s", name, reuseJob, bad)
			continue
		}
		used := map[string]bool{}
		jobUnit := map[string]string{}
		for _, id := range sortedKeys(jobs) {
			if id == reuseJob {
				continue
			}
			j := asMap(jobs[id])
			cond := asString(j["if"])
			ms := reuseClauseRe.FindAllStringSubmatch(cond, -1)
			needs := fmt.Sprint(j["needs"])
			if len(ms) == 0 {
				if strings.Contains(cond, "github.event.pull_request.draft") {
					if _, ok := noReuseDraftJobs[name+"/"+id]; !ok {
						add("%s: draft-gated job %s has no reuse clause: the flip reruns it even when it passed on this sha", name, id)
					}
				}
				continue
			}
			if len(ms) > 1 {
				add("%s: job %s names %d reuse units, want 1", name, id, len(ms))
			}
			u := ms[0][1]
			if !units[u] {
				add("%s: job %s skips for unit %s, which %s does not declare", name, id, u, reuseJob)
			}
			c := strings.TrimSpace(cond)
			if inner, wrapped := strings.CutPrefix(c, "${{"); wrapped {
				if strings.TrimSpace(strings.TrimSuffix(inner, "}}")) != ms[0][0] {
					add("%s: job %s must be <its condition> && <reuse clause>", name, id)
				}
			} else if rest, ok := strings.CutSuffix(c, " && "+ms[0][0]); !ok {
				add("%s: job %s must be <its condition> && <reuse clause>", name, id)
			} else if topLevelOr(rest) {
				add("%s: job %s: its condition has a top-level ||, so the appended reuse clause binds to its last operand only", name, id)
			}
			if !strings.Contains(" "+strings.Trim(needs, "[]")+" ", " "+reuseJob+" ") {
				add("%s: job %s reads the reuse output but does not need %s", name, id, reuseJob)
			}
			used[u] = true
			jobUnit[id] = u
		}
		for u := range units {
			if !used[u] {
				add("%s: unit %s is declared but no job skips for it", name, u)
			}
		}
		// A gate counts a reused skip only through the unit of that job (GATE_REUSE_n: <job>|<unit>).
		for _, id := range sortedKeys(jobs) {
			for _, s := range stepsOf(jobs[id]) {
				env := asMap(s["env"])
				gated, reuse := map[string]bool{}, map[string]string{}
				for _, k := range sortedKeys(env) {
					v := asString(env[k])
					if strings.HasPrefix(k, "GATED_JOB_") {
						if m := gatedJobRe.FindStringSubmatch(v); m != nil {
							gated[m[1]] = true
						}
					}
					if strings.HasPrefix(k, "GATE_REUSE_") {
						job, unit, _ := strings.Cut(v, "|")
						reuse[job] = unit
					}
				}
				for job := range gated {
					want := jobUnit[job]
					if job == "go-container-reproducibility" {
						want = jobUnit["go-container-reproducibility-self-hosted"]
					}
					if reuse[job] != want {
						add("%s: %s judges %s with reuse unit %q, want %q", name, id, job, reuse[job], want)
					}
				}
				for job := range reuse {
					if !gated[job] {
						add("%s: %s has a GATE_REUSE row for %s, which it does not judge", name, id, job)
					}
				}
				if len(reuse) > 0 && env["READY_REUSED"] != "${{ needs.ready-reuse.outputs.reused }}" {
					add("%s: %s judges reused legs without READY_REUSED", name, id)
				}
			}
		}
	}
	sort.Strings(out)
	return out
}

func TestReadyReuseWiring(t *testing.T) {
	expectNoViolations(t, readyReuseWiringViolations(t, repoRoot(t)))
}

func TestReadyReuseWiringPlantedDefects(t *testing.T) {
	root := repoRoot(t)
	var files []string
	for _, n := range workflowNames(t, root) {
		files = append(files, ".github/workflows/"+n)
	}
	const goYml = ".github/workflows/go.yml"
	cases := []struct {
		name string
		mut  mutation
		want []string
	}{
		{"a dropped workflow reads the draft state",
			mutation{".github/workflows/go-quality.yml", "    if: always()\n    needs: [go-quality-plan, go-quality-leg]", "    if: always() || github.event.pull_request.draft\n    needs: [go-quality-plan, go-quality-leg]"},
			[]string{"go-quality.yml", "without a ready_for_review trigger"}},
		{"a draft-gated workflow loses ready_for_review",
			mutation{".github/workflows/venue-oracles-changed.yml", "types: [opened, synchronize, reopened, ready_for_review]", "types: [opened, synchronize, reopened]"},
			[]string{"venue-oracles-changed.yml", "lost ready_for_review"}},
		{"a draft-gated leg without a reuse clause",
			mutation{goYml, " && !contains(fromJSON(needs.ready-reuse.outputs.passed), 'graphql-wire-parity')", ""},
			[]string{"go.yml", "draft-gated job graphql-wire-parity has no reuse clause"}},
		{"a leg skips for an undeclared unit",
			mutation{".github/workflows/security-scan.yml", "outputs.passed), 'semgrep')", "outputs.passed), 'semgre')"},
			[]string{"security-scan.yml", "unit semgre, which ready-reuse does not declare"}},
		{"the judge comes from the PR tree",
			mutation{goYml, "          ref: ${{ github.event.pull_request.base.sha }}\n          sparse-checkout: ci/ready_reuse.sh", "          sparse-checkout: ci/ready_reuse.sh"},
			[]string{"go.yml", "from the BASE commit"}},
		{"a gate entry without its unit",
			mutation{goYml, "          GATE_REUSE_6: graphql-wire-parity|graphql-wire-parity\n", ""},
			[]string{"go.yml", "go-queue-gate judges graphql-wire-parity with reuse unit \"\", want \"graphql-wire-parity\""}},
		{"the reuse clause binds to the last operand of a top-level ||",
			mutation{".github/workflows/go-auth-service.yml", "    if: needs.auth-changes.outputs.relevant == 'true' && !contains(fromJSON(needs.ready-reuse.outputs.passed), 'auth-test')", "    if: needs.auth-changes.outputs.relevant == 'true' || github.event_name == 'push' && !contains(fromJSON(needs.ready-reuse.outputs.passed), 'auth-test')"},
			[]string{"go-auth-service.yml", "auth-test: its condition has a top-level ||"}},
		{"a leg reads the reuse output without needing the judge",
			mutation{".github/workflows/go-workers-compose-proof.yml", "    needs: ready-reuse\n", ""},
			[]string{"go-workers-compose-proof.yml", "does not need ready-reuse"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			expectViolation(t, readyReuseWiringViolations(t, plantTree(t, root, files, c.mut)), c.want...)
		})
	}
}

var reuseSuffixRe = regexp.MustCompile(` && !contains\(fromJSON\(needs\.ready-reuse\.outputs\.passed\), '[a-z0-9-]+'\)$`)

// withoutReuse drops the ready-flip reuse clause from a job condition, so a guard of the job's own condition reads it as
// before; TestReadyReuseWiring guards the clause itself.
func withoutReuse(cond string) string {
	return reuseSuffixRe.ReplaceAllString(strings.TrimSpace(cond), "")
}

// needsWithoutReuse drops ready-reuse from a needs value (a list or a scalar), in the original order.
func needsWithoutReuse(v any) any {
	l, ok := v.([]any)
	if !ok {
		return v
	}
	var out []any
	for _, n := range l {
		if n != reuseJob {
			out = append(out, n)
		}
	}
	if len(out) == 1 {
		return out[0]
	}
	return out
}

// dropReuse drops ready-reuse from a needs list.
func dropReuse(needs []string) []string {
	var out []string
	for _, n := range needs {
		if n != reuseJob {
			out = append(out, n)
		}
	}
	return out
}

// reuseClause is the text a reusable leg appends to its condition for unit.
func reuseClause(unit string) string {
	return " && !contains(fromJSON(needs.ready-reuse.outputs.passed), '" + unit + "')"
}
