package workflows_test

import (
	"fmt"
	"reflect"
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
	needs                []string
	env                  map[string]string
	rows                 []string
}

var queueGates = []queueGateSpec{
	{
		file:      "go.yml",
		job:       "go-queue-gate",
		condition: "always() && (github.event_name == 'pull_request' || github.event_name == 'merge_group')",
		needs: []string{"go-storage-integration-plan", "go-storage-integration", "go-container-smoke", "go-container-security",
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
	for _, g := range queueGates {
		j := asMap(jobsOf(loadWf(t, root, g.file))[g.job])
		if j == nil {
			add("%s: the required gate %s is missing", g.file, g.job)
			continue
		}
		if asString(j["name"]) != g.job {
			add("%s:%s: the job name is the required context and must stay %q, got %q", g.file, g.job, g.job, asString(j["name"]))
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
		// Every heavy job of this workflow, except the self-hosted dind precondition, is judged by its gate.
		judged := map[string]bool{}
		for _, r := range rows {
			judged[strings.SplitN(r, "|", 2)[0]] = true
		}
		judged["go-container-reproducibility-self-hosted"] = judged["go-container-reproducibility"]
		var unjudged []string
		for _, p := range sortedPairs(heavyGatedJobs) {
			if p[0] == g.file && p[1] != "dind-smoke-test" && !judged[p[1]] {
				unjudged = append(unjudged, p[1])
			}
		}
		sort.Strings(unjudged)
		if len(unjudged) > 0 {
			add("%s:%s does not judge the heavy job(s) %v: a red one would merge through the queue", g.file, g.job, unjudged)
		}
	}
	return out
}

func TestQueueGatesJudgeEveryHeavyJob(t *testing.T) {
	expectNoViolations(t, queueGateViolations(t, repoRoot(t)))
}

func TestQueueGatesPlantedDefects(t *testing.T) {
	root := repoRoot(t)
	const goYml, dock = ".github/workflows/go.yml", ".github/workflows/docker-images.yml"
	files := []string{goYml, dock}
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
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			expectViolation(t, queueGateViolations(t, plantTree(t, root, files, c.muts...)), c.want...)
		})
	}
}
