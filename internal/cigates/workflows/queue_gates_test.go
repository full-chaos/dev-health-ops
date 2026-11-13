package workflows_test

import (
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"
)

// The heavy jobs run in the merge queue, not on a pull request (unless ci:full), and a docs-only change skips them
// everywhere. Two required gates carry their verdict to the queue: go.yml's go-queue-gate and docker-images.yml's
// image-queue-gate. ci/aggregate_gate_results.sh judges a skip through two policies: `queue-heavy` and `docs-skippable`.

func TestQueueHeavyAndDocsSkippablePolicies(t *testing.T) {
	rows := []struct {
		id, event, changes, code, heavyOnPR, policy, result string
		want                                                bool
	}{
		{"docs: PR docs-only skip passes", "pull_request", "success", "false", "", "docs-skippable", "skipped", pass},
		{"docs: queue docs-only skip passes", "merge_group", "success", "false", "", "docs-skippable", "skipped", pass},
		{"docs: a push is never docs-only", "push", "success", "false", "", "docs-skippable", "skipped", fail},
		{"docs: a manual run is never docs-only", "workflow_dispatch", "success", "false", "", "docs-skippable", "skipped", fail},
		{"docs: PR code skip fails", "pull_request", "success", "true", "", "docs-skippable", "skipped", fail},
		{"docs: queue code skip fails", "merge_group", "success", "true", "", "docs-skippable", "skipped", fail},
		{"docs: an empty selector output is not a decision", "merge_group", "success", "", "", "docs-skippable", "skipped", fail},
		{"docs: a failure fails even when docs-only", "pull_request", "success", "false", "", "docs-skippable", "failure", fail},
		{"docs: a cancel fails even when docs-only", "merge_group", "success", "false", "", "docs-skippable", "cancelled", fail},
		{"docs: a failed selector fails the skip", "pull_request", "failure", "false", "", "docs-skippable", "skipped", fail},
		{"docs: success passes", "merge_group", "success", "true", "", "docs-skippable", "success", pass},
		{"heavy: PR without ci:full skip passes", "pull_request", "success", "true", "false", "queue-heavy", "skipped", pass},
		{"heavy: PR with ci:full skip fails", "pull_request", "success", "true", "true", "queue-heavy", "skipped", fail},
		{"heavy: PR with ci:full and nothing to build passes", "pull_request", "success", "false", "true", "queue-heavy", "skipped", pass},
		{"heavy: queue skip fails", "merge_group", "success", "true", "false", "queue-heavy", "skipped", fail},
		{"heavy: queue docs-only skip passes", "merge_group", "success", "false", "false", "queue-heavy", "skipped", pass},
		{"heavy: queue success passes", "merge_group", "success", "true", "false", "queue-heavy", "success", pass},
		{"heavy: queue failure fails", "merge_group", "success", "true", "false", "queue-heavy", "failure", fail},
		{"heavy: queue cancel fails", "merge_group", "success", "true", "false", "queue-heavy", "cancelled", fail},
		{"heavy: PR failure fails even without ci:full", "pull_request", "success", "true", "false", "queue-heavy", "failure", fail},
		{"heavy: a push skip fails", "push", "success", "true", "false", "queue-heavy", "skipped", fail},
		{"heavy: a failed selector fails", "merge_group", "cancelled", "false", "false", "queue-heavy", "skipped", fail},
		{"heavy: HEAVY_ON_PR unset is refused, even on success", "merge_group", "success", "true", "<unset>", "queue-heavy", "success", fail},
		{"heavy: HEAVY_ON_PR not literal is refused", "pull_request", "success", "true", "yes", "queue-heavy", "skipped", fail},
	}
	for _, r := range rows {
		t.Run(r.id, func(t *testing.T) {
			env := map[string]string{
				"GATE_NAME": "go-queue-gate", "EVENT_NAME": r.event, "GATE_HAS_SELECTOR": "true",
				"CHANGES_RESULT": r.changes, "CHANGES_CODE": r.code,
				"GATED_JOB_1": "heavy-job|" + r.policy + "|" + r.result,
			}
			if r.heavyOnPR != "<unset>" {
				env["HEAVY_ON_PR"] = r.heavyOnPR
			}
			if r.policy == "queue-heavy" && r.heavyOnPR == "" {
				t.Fatal("a queue-heavy row must state HEAVY_ON_PR")
			}
			assertVerdict(t, "go-queue-gate", runGate(t, env), r.want, r.id)
		})
	}
}

type queueGateSpec struct {
	file, job, condition string
	// name is the job's `name:` (the required context); empty means the job id itself.
	name  string
	needs []string
	env   map[string]string
	rows  []string
}

// live-e2e.yml has a `labeled` trigger for ci:full. The run of any other label (otherLabelRun) must post no check run
// under a real job name: a job skipped by `if:` posts a passing check, which would overwrite a red verdict of the same
// head. So every job renames itself there (otherLabelName) and the jobs that would post skip (labelFilter).
const (
	otherLabelRun = "github.event.action == 'labeled' && github.event.label.name != 'ci:full'"
	labelFilter   = "(github.event.action != 'labeled' || github.event.label.name == 'ci:full')"
)

func otherLabelName(job string) string {
	return "${{ " + otherLabelRun + " && '" + job + " (other label, not run)' || '" + job + "' }}"
}

var queueGates = []queueGateSpec{
	{
		file:      "live-e2e.yml",
		job:       "live-e2e",
		name:      otherLabelName("live-e2e"),
		condition: "always() && (github.event_name == 'pull_request' || github.event_name == 'merge_group') && " + labelFilter,
		needs:     []string{"changes", "live-e2e-run"},
		env: map[string]string{
			"GATE_NAME":         "live-e2e",
			"EVENT_NAME":        "${{ github.event_name }}",
			"GATE_HAS_SELECTOR": "true",
			"CHANGES_RESULT":    "${{ needs.changes.result }}",
			"CHANGES_CODE":      "${{ (needs.changes.outputs.docs_only == 'true' || (github.event_name == 'pull_request' && needs.changes.outputs.code == 'false')) && 'false' || 'true' }}",
			"HEAVY_ON_PR":       "${{ contains(github.event.pull_request.labels.*.name, 'ci:full') && 'true' || 'false' }}",
		},
		rows: []string{"live-e2e-run|queue-heavy|${{ needs.live-e2e-run.result }}"},
	},
	{
		file:      "live-e2e.yml",
		job:       "metrics-executed-proof",
		name:      otherLabelName("metrics-executed-proof"),
		condition: "always() && (github.event_name == 'pull_request' || github.event_name == 'merge_group') && " + labelFilter,
		needs:     []string{"changes", "metrics-executed-proof-run"},
		env: map[string]string{
			"GATE_NAME":         "metrics-executed-proof",
			"EVENT_NAME":        "${{ github.event_name }}",
			"GATE_HAS_SELECTOR": "true",
			"CHANGES_RESULT":    "${{ needs.changes.result }}",
			"CHANGES_CODE":      "${{ (needs.changes.outputs.docs_only == 'true' || (github.event_name == 'pull_request' && needs.changes.outputs.code == 'false')) && 'false' || 'true' }}",
			// a pull request touching a metrics_proof path selects the job as ci:full does, so its skip is not legitimate
			"HEAVY_ON_PR": "${{ (contains(github.event.pull_request.labels.*.name, 'ci:full') || needs.changes.outputs.metrics_proof == 'true') && 'true' || 'false' }}",
		},
		rows: []string{"metrics-executed-proof-run|queue-heavy|${{ needs.metrics-executed-proof-run.result }}"},
	},
	{
		file:      "go.yml",
		job:       "go-queue-gate",
		condition: "always() && (github.event_name == 'pull_request' || github.event_name == 'merge_group')",
		needs: []string{"go-storage-integration-plan", "go-storage-integration", "go-container-build", "go-container-smoke", "go-container-security",
			"go-container-reproducibility", "go-container-reproducibility-self-hosted", "go-auth-compose-proof", "go-auth-web-proxy-proof",
			"graphql-wire-parity"},
		env: map[string]string{
			"GATE_NAME":         "go-queue-gate",
			"EVENT_NAME":        "${{ github.event_name }}",
			"GATE_HAS_SELECTOR": "true",
			"CHANGES_RESULT":    "${{ needs.go-storage-integration-plan.result }}",
			"CHANGES_CODE":      "${{ needs.go-storage-integration-plan.outputs.docs_only == 'true' && 'false' || 'true' }}",
			"HEAVY_ON_PR":       "${{ contains(github.event.pull_request.labels.*.name, 'ci:full') && 'true' || 'false' }}",
		},
		rows: []string{
			"go-storage-integration|unconditional|${{ needs.go-storage-integration.result }}",
			"go-container-smoke|queue-heavy|${{ needs.go-container-smoke.result }}",
			"go-container-security|queue-heavy|${{ needs.go-container-security.result }}",
			"go-container-reproducibility|queue-heavy|${{ needs.go-container-reproducibility.result == 'skipped' && needs.go-container-reproducibility-self-hosted.result || needs.go-container-reproducibility.result }}",
			"go-auth-compose-proof|queue-heavy|${{ needs.go-auth-compose-proof.result }}",
			"go-auth-web-proxy-proof|queue-heavy|${{ needs.go-auth-web-proxy-proof.result }}",
			"graphql-wire-parity|queue-heavy|${{ needs.graphql-wire-parity.result }}",
			"go-container-build|queue-heavy|${{ needs.go-container-build.result }}",
		},
	},
	{
		file:      "docker-images.yml",
		job:       "image-queue-gate",
		condition: "always() && (github.event_name == 'pull_request' || github.event_name == 'merge_group') && (github.event.action != 'labeled' || github.event.label.name == 'hotfix-image')",
		needs:     []string{"changes", "resolve", "go-build", "go-api-tools-build", "go-build-dho-arm64"},
		env: map[string]string{
			"GATE_NAME":         "image-queue-gate",
			"EVENT_NAME":        "${{ github.event_name }}",
			"GATE_HAS_SELECTOR": "true",
			"CHANGES_RESULT":    "${{ needs.changes.result }}",
			"CHANGES_CODE":      "${{ (needs.changes.outputs.docs_only == 'true' || (github.event_name == 'pull_request' && needs.changes.outputs.code == 'false')) && 'false' || 'true' }}",
			"HEAVY_ON_PR":       "${{ (contains(github.event.pull_request.labels.*.name, 'ci:full') || (!github.event.pull_request.draft && contains(github.event.pull_request.labels.*.name, 'hotfix-image'))) && 'true' || 'false' }}",
		},
		rows: []string{
			"resolve|unconditional|${{ needs.resolve.result }}",
			"go-build|queue-heavy|${{ needs.go-build.result }}",
			"go-api-tools-build|queue-heavy|${{ needs.go-api-tools-build.result }}",
			"go-build-dho-arm64|queue-heavy|${{ needs.go-build-dho-arm64.result }}",
		},
	},
}

func queueGateViolations(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	add := func(f string, a ...any) { out = append(out, fmt.Sprintf(f, a...)) }
	judgedByFile := map[string]map[string]bool{}
	for _, g := range queueGates {
		j := asMap(jobsOf(loadWf(t, root, g.file))[g.job])
		if j == nil {
			add("%s: the required gate %s is missing", g.file, g.job)
			continue
		}
		wantName := g.name
		if wantName == "" {
			wantName = g.job
		}
		if asString(j["name"]) != wantName {
			add("%s:%s: the job name is the required context and must stay %q, got %q", g.file, g.job, wantName, asString(j["name"]))
		}
		if got := asString(j["if"]); got != g.condition {
			add("%s:%s: the if must be exactly %q (always(), so a cancelled run reports; pull_request and merge_group only), got %q", g.file, g.job, g.condition, got)
		}
		if got := needsList(j); !reflect.DeepEqual(got, g.needs) {
			add("%s:%s: needs %v, want %v", g.file, g.job, got, g.needs)
		}
		var env yamlMap
		for _, s := range stepsOf(j) {
			if strings.Contains(asString(s["run"]), "./ci/aggregate_gate_results.sh") {
				env = asMap(s["env"])
			}
		}
		if env == nil {
			add("%s:%s: no step runs ci/aggregate_gate_results.sh", g.file, g.job)
			continue
		}
		for k, want := range g.env {
			if got := asString(env[k]); got != want {
				add("%s:%s: %s = %q, want %q", g.file, g.job, k, got, want)
			}
		}
		var rows []string
		for i := 1; ; i++ {
			v, ok := env[fmt.Sprintf("GATED_JOB_%d", i)]
			if !ok {
				break
			}
			rows = append(rows, asString(v))
		}
		if !reflect.DeepEqual(rows, g.rows) {
			add("%s:%s: GATED_JOB rows %q, want %q", g.file, g.job, rows, g.rows)
		}
		if judgedByFile[g.file] == nil {
			judgedByFile[g.file] = map[string]bool{}
		}
		for _, r := range rows {
			judgedByFile[g.file][strings.SplitN(r, "|", 2)[0]] = true
		}
	}
	// Every heavy job, except the self-hosted dind preconditions, is judged by a required gate of its workflow.
	for _, file := range []string{"go.yml", "docker-images.yml", "live-e2e.yml"} {
		judged := judgedByFile[file]
		if judged == nil {
			judged = map[string]bool{}
		}
		judged["go-container-reproducibility-self-hosted"] = judged["go-container-reproducibility"]
		var unjudged []string
		for _, p := range sortedPairs(heavyGatedJobs) {
			if p[0] == file && p[1] != "dind-smoke-test" && !judged[p[1]] {
				unjudged = append(unjudged, p[1])
			}
		}
		sort.Strings(unjudged)
		if len(unjudged) > 0 {
			add("%s: no required gate judges the heavy job(s) %v: a red one would merge through the queue", file, unjudged)
		}
	}
	return out
}

// acp P1: a failed `changes` skips both live-e2e work jobs, and a skipped required check passes. The required contexts
// live-e2e and metrics-executed-proof are always() fan-ins whose selector is `changes`, so that run fails.
func TestLiveE2EFanInsFailWhenChangesFails(t *testing.T) {
	for _, gate := range []string{"live-e2e", "metrics-executed-proof"} {
		for _, event := range []string{"pull_request", "merge_group"} {
			for _, changes := range []string{"failure", "cancelled", "skipped"} {
				v := runGate(t, map[string]string{
					"GATE_NAME": gate, "EVENT_NAME": event, "GATE_HAS_SELECTOR": "true",
					"CHANGES_RESULT": changes, "CHANGES_CODE": "true", "HEAVY_ON_PR": "false",
					"GATED_JOB_1": gate + "-run|queue-heavy|skipped",
				})
				assertVerdict(t, gate, v, fail, gate+" "+event+" changes="+changes)
			}
		}
	}
	expectNoViolations(t, queueGateViolations(t, repoRoot(t)))
}

// live-e2e and metrics-executed-proof are required contexts, and a skipped job reports success. On merge_group the
// changes job's path filter sees only the entry's own PR (the event base is the parent entry), so the jobs must not read
// `code` there: every queue entry that is not docs-only runs them, even one whose own PR is docs-only behind a code entry.
// A pull request that touches a metrics_proof path runs metrics-executed-proof-run with no label (a metric change once
// merged with it skipped and turned main red); live-e2e-run still needs ci:full.
func TestLiveE2ERunsForEveryNonDocsQueueEntry(t *testing.T) {
	doc := loadWf(t, repoRoot(t), "live-e2e.yml")
	for _, c := range []struct {
		event, code, proof, docsOnly, labels string
		wantLive, wantProof                  bool
	}{
		{"merge_group", "false", "false", "false", "", true, true},
		{"merge_group", "true", "false", "false", "", true, true},
		{"merge_group", "false", "false", "true", "", false, false},
		{"merge_group", "true", "true", "true", "", false, false},
		{"pull_request", "true", "false", "false", "", false, false},
		{"pull_request", "true", "false", "false", "ci:full", true, true},
		{"pull_request", "false", "false", "false", "ci:full", false, false},
		{"pull_request", "true", "true", "false", "", false, true},
		{"pull_request", "true", "true", "false", "ci:full", true, true},
		{"pull_request", "false", "true", "false", "", false, false},
		{"pull_request", "true", "true", "true", "", false, false},
		{"push", "true", "false", "false", "", true, true},
		{"push", "false", "false", "false", "", false, false},
	} {
		for name, want := range map[string]bool{"live-e2e-run": c.wantLive, "metrics-executed-proof-run": c.wantProof} {
			condition := strings.TrimSpace(asString(pinJob(doc, name)["if"]))
			ctx := queueContext(c.event, "refs/heads/main")
			ctx["needs.changes.outputs.code"] = c.code
			ctx["needs.changes.outputs.metrics_proof"] = c.proof
			ctx["needs.changes.outputs.docs_only"] = c.docsOnly
			ctx["github.event.pull_request.labels.*.name"] = c.labels
			got, err := queueEval(condition, ctx)
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			if got.truthy() != want {
				t.Errorf("%s on %s (code=%s metrics_proof=%s docs_only=%s labels=%q) runs=%v, want %v", name, c.event, c.code, c.proof, c.docsOnly, c.labels, got.truthy(), want)
			}
		}
	}
}

// liveE2EJobs are every job of live-e2e.yml: each posts a check run under its name, and any of them may be a required
// context (the ruleset is not readable from the repo).
var liveE2EJobs = []string{"changes", "live-e2e-run", "metrics-executed-proof-run", "live-e2e", "metrics-executed-proof"}

// liveE2ELabelViolations: adding ci:full starts the run with no push; the run of any other label posts no check run
// under a real job name (a skipped job posts a passing check, which would overwrite a red verdict of the same head) and
// sits in its own concurrency group (so it cannot cancel the real run); unlabeled starts nothing.
func liveE2ELabelViolations(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	add := func(f string, a ...any) { out = append(out, fmt.Sprintf("live-e2e.yml: "+f, a...)) }
	doc := loadWf(t, root, "live-e2e.yml")
	types := map[string]bool{}
	for _, ty := range prTypes(asMap(doc["on"])) {
		types[ty] = true
	}
	if !types["labeled"] {
		add("pull_request types lack labeled: adding ci:full to an open PR starts nothing until the next push")
	}
	if types["unlabeled"] {
		add("pull_request types list unlabeled: a label removal would start a run")
	}
	pr := func(action, label, labels string) queueCtx {
		ctx := queueContext("pull_request", "refs/pull/7/merge")
		ctx["github.event.action"], ctx["github.event.label.name"] = action, label
		ctx["github.event.pull_request.labels.*.name"] = labels
		return ctx
	}
	contexts := []struct {
		id      string
		ctx     queueCtx
		renamed bool
	}{
		{"synchronize", pr("synchronize", "", ""), false},
		{"ci:full added", pr("labeled", "ci:full", "ci:full"), false},
		{"other label added", pr("labeled", "documentation", "documentation"), true},
		{"other label added to a ci:full PR", pr("labeled", "documentation", "ci:full,documentation"), true},
		{"merge_group", queueContext("merge_group", "refs/heads/gh-readonly-queue/main/pr-7-abcdef"), false},
		{"main push", queueContext("push", "refs/heads/main"), false},
	}
	realName := map[string]bool{}
	for _, jn := range liveE2EJobs {
		realName[jn] = true
	}
	eval := func(where, text string, ctx queueCtx) (queueValue, bool) {
		text = strings.TrimSpace(text)
		if m := queueBlockRE.FindStringSubmatch(text); m != nil && strings.HasPrefix(text, "${{") && strings.HasSuffix(text, "}}") {
			text = strings.TrimSpace(m[1])
		}
		v, err := queueEval(text, ctx)
		if err != nil {
			add("%s: %v", where, err)
			return v, false
		}
		return v, true
	}
	for _, jn := range liveE2EJobs {
		j := pinJob(doc, jn)
		if j == nil {
			add("no job %s", jn)
			continue
		}
		for _, c := range contexts {
			name := jn
			if raw := asString(j["name"]); raw != "" {
				v, ok := eval(jn+".name", raw, c.ctx)
				if !ok {
					continue
				}
				name = v.s
			}
			if c.renamed && realName[name] {
				add("%s: on the %s run the job posts its check under the real name %q: a skipped job posts a passing check and overwrites the head's verdict", jn, c.id, name)
			}
			if !c.renamed && name != jn {
				add("%s: on the %s run the check name is %q, want %q (the required context)", jn, c.id, name, jn)
			}
		}
	}
	// The jobs that would post on another label's run skip there; the work jobs skip with `changes` (no always()).
	for _, jn := range []string{"changes", "live-e2e", "metrics-executed-proof"} {
		j := pinJob(doc, jn)
		for _, c := range contexts {
			runs := true // no `if:` runs on every event
			if cond := asString(j["if"]); strings.TrimSpace(cond) != "" {
				v, ok := eval(jn+".if", cond, c.ctx)
				if !ok {
					continue
				}
				runs = v.truthy()
			}
			if c.renamed && runs {
				add("%s: runs on the %s run: it must skip (the last real run carries the verdict)", jn, c.id)
			}
			if !c.renamed && c.ctx["github.event_name"] != "push" && !runs {
				add("%s: skips on the %s run", jn, c.id)
			}
		}
	}
	for _, jn := range []string{"live-e2e-run", "metrics-executed-proof-run"} {
		j := pinJob(doc, jn)
		if strings.Contains(asString(j["if"]), "always()") || !slices.Contains(needsList(j), "changes") {
			add("%s: must need changes and carry no always(), so it skips when changes skips on another label's run", jn)
		}
	}
	conc := asMap(doc["concurrency"])
	group := asString(conc["group"])
	render := func(action, label string) string {
		c := withAction(prCtx, action)
		c["github.event.label.name"] = label
		g, err := renderGroup(group, c)
		if err != nil {
			add("concurrency group: %v", err)
		}
		return g
	}
	sync, full, other := render("synchronize", ""), render("labeled", "ci:full"), render("labeled", "documentation")
	if other == sync {
		add("another label's run shares the PR concurrency group %q and would cancel the real run", sync)
	}
	if full != sync {
		add("the ci:full run's group %q differs from the PR group %q: it would not supersede the run it recomputes", full, sync)
	}
	return out
}

func TestLiveE2ELabeledTriggerStartsOnlyOnCIFull(t *testing.T) {
	expectNoViolations(t, liveE2ELabelViolations(t, repoRoot(t)))
}

func TestLiveE2ELabeledTriggerPlantedDefects(t *testing.T) {
	root := repoRoot(t)
	const live = ".github/workflows/live-e2e.yml"
	cases := []struct {
		name string
		mut  mutation
		want []string
	}{
		{"no labeled type", mutation{live, "types: [opened, synchronize, reopened, labeled]", "types: [opened, synchronize, reopened]"}, []string{"lack labeled"}},
		{"unlabeled type", mutation{live, "types: [opened, synchronize, reopened, labeled]", "types: [opened, synchronize, reopened, labeled, unlabeled]"}, []string{"unlabeled"}},
		{"other label shares the group", mutation{live, "${{ github.event.action == 'labeled' && github.event.label.name != 'ci:full' && '-other-label' || '' }}", ""}, []string{"would cancel the real run"}},
		{"changes runs on another label", mutation{live, "    if: github.event.action != 'labeled' || github.event.label.name == 'ci:full'\n", ""}, []string{"changes: runs on the other label added run"}},
		{"metrics fan-in runs on another label", mutation{live, "github.event_name == 'merge_group') && (github.event.action != 'labeled' || github.event.label.name == 'ci:full')\n    needs: [changes, metrics-executed-proof-run]", "github.event_name == 'merge_group')\n    needs: [changes, metrics-executed-proof-run]"}, []string{"metrics-executed-proof: runs on the other label added run"}},
		{"proof run job keeps its real name", mutation{live, "    name: ${{ github.event.action == 'labeled' && github.event.label.name != 'ci:full' && 'metrics-executed-proof-run (other label, not run)' || 'metrics-executed-proof-run' }}\n", ""}, []string{"metrics-executed-proof-run", "under the real name"}},
		{"changes renames on ci:full", mutation{live, "&& 'changes (other label, not run)' || 'changes' }}", "&& 'changes (other label, not run)' || 'changes (label)' }}"}, []string{"changes: on the synchronize run the check name"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			expectViolation(t, liveE2ELabelViolations(t, plantTree(t, root, []string{live}, c.mut)), c.want...)
		})
	}
}

func TestQueueGatesJudgeEveryHeavyJob(t *testing.T) {
	expectNoViolations(t, queueGateViolations(t, repoRoot(t)))
}

func TestQueueGatesPlantedDefects(t *testing.T) {
	root := repoRoot(t)
	const goYml, dock, live = ".github/workflows/go.yml", ".github/workflows/docker-images.yml", ".github/workflows/live-e2e.yml"
	files := []string{goYml, dock, live}
	cases := []struct {
		name string
		muts []mutation
		want []string
	}{
		{"go-queue-gate can skip on a cancel", []mutation{{goYml, "    if: always() && (github.event_name == 'pull_request' || github.event_name == 'merge_group')\n    needs:\n      - go-storage-integration-plan", "    if: success() && (github.event_name == 'pull_request' || github.event_name == 'merge_group')\n    needs:\n      - go-storage-integration-plan"}}, []string{"go-queue-gate", "the if must be exactly"}},
		{"go-queue-gate drops a heavy row", []mutation{{goYml, "          GATED_JOB_3: go-container-security|queue-heavy|${{ needs.go-container-security.result }}\n", ""}}, []string{"go-container-security"}},
		{"go-queue-gate judges a heavy job unconditional", []mutation{{goYml, "go-auth-compose-proof|queue-heavy|", "go-auth-compose-proof|unconditional|"}}, []string{"GATED_JOB rows"}},
		{"go-queue-gate loses its docs selector", []mutation{{goYml, "CHANGES_CODE: ${{ needs.go-storage-integration-plan.outputs.docs_only == 'true' && 'false' || 'true' }}", "CHANGES_CODE: 'true'"}}, []string{"CHANGES_CODE"}},
		{"image-queue-gate stops needing go-build", []mutation{{dock, "needs: [changes, resolve, go-build, go-api-tools-build, go-build-dho-arm64]", "needs: [changes, resolve, go-api-tools-build, go-build-dho-arm64]"}}, []string{"image-queue-gate", "needs"}},
		{"image-queue-gate treats a hotfix label as ci:full on a draft", []mutation{{dock, "(!github.event.pull_request.draft && contains(github.event.pull_request.labels.*.name, 'hotfix-image'))", "contains(github.event.pull_request.labels.*.name, 'hotfix-image')"}}, []string{"HEAVY_ON_PR"}},
		{"image-queue-gate is renamed", []mutation{{dock, "    name: image-queue-gate\n", "    name: images\n"}}, []string{"required context"}},
		{"live-e2e fan-in loses always()", []mutation{{live, "    if: always() && (github.event_name == 'pull_request' || github.event_name == 'merge_group') && (github.event.action != 'labeled' || github.event.label.name == 'ci:full')\n    needs: [changes, live-e2e-run]", "    if: (github.event_name == 'pull_request' || github.event_name == 'merge_group') && (github.event.action != 'labeled' || github.event.label.name == 'ci:full')\n    needs: [changes, live-e2e-run]"}}, []string{"live-e2e.yml:live-e2e", "the if must be exactly"}},
		{"metrics-executed-proof fan-in drops changes as its selector", []mutation{{live, "    needs: [changes, metrics-executed-proof-run]\n", "    needs: [metrics-executed-proof-run]\n"}}, []string{"live-e2e.yml:metrics-executed-proof", "needs"}},
		{"metrics-executed-proof fan-in judges a metrics_proof skip as legitimate", []mutation{{live, "HEAVY_ON_PR: ${{ (contains(github.event.pull_request.labels.*.name, 'ci:full') || needs.changes.outputs.metrics_proof == 'true') && 'true' || 'false' }}", "HEAVY_ON_PR: ${{ contains(github.event.pull_request.labels.*.name, 'ci:full') && 'true' || 'false' }}"}}, []string{"live-e2e.yml:metrics-executed-proof", "HEAVY_ON_PR"}},
		{"live-e2e fan-in keeps its real name on another label's run", []mutation{{live, "    name: ${{ github.event.action == 'labeled' && github.event.label.name != 'ci:full' && 'live-e2e (other label, not run)' || 'live-e2e' }}\n", "    name: live-e2e\n"}}, []string{"live-e2e.yml:live-e2e", "required context"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			expectViolation(t, queueGateViolations(t, plantTree(t, root, files, c.muts...)), c.want...)
		})
	}
}
