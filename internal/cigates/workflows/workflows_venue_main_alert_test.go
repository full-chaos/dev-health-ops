package workflows_test

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

// CHAOS-10177 (B2): Go port of the workflow-shape tests of tests/tooling/test_venue_main_alert.py (CHAOS-9597): the
// main-alert workflow listens to the venue workflow by its name, handles main push runs only, holds a least-privilege
// token, serialises alert jobs without ever cancelling one, and passes event data through env only. The script tests
// (a fake `gh` driving ci/venue-main-alert.sh) stay in Python until the venue-script port (CHAOS-10178).

const (
	mainAlertFile  = ".github/workflows/venue-oracles-main-alert.yml"
	mainAlertGroup = "main-red-alert-${{ github.event.workflow_run.name }}"
	mainAlertIf    = "github.event.workflow_run.head_branch == 'main' && github.event.workflow_run.event == 'push'"
)

var secretsWord = regexp.MustCompile(`\bsecrets\b`)

// stringLeafMatching returns the first string in a parsed YAML value that matches re (comments are not in the parsed value).
func stringLeafMatching(v any, re *regexp.Regexp) string {
	switch x := v.(type) {
	case string:
		if re.MatchString(x) {
			return x
		}
	case yamlMap:
		for _, k := range sortedKeys(x) {
			if re.MatchString(k) {
				return k
			}
			if got := stringLeafMatching(x[k], re); got != "" {
				return got
			}
		}
	case []any:
		for _, e := range x {
			if got := stringLeafMatching(e, re); got != "" {
				return got
			}
		}
	}
	return ""
}

// mainRedCovered reads ci/main-red-workflows.tsv and returns the covered workflow names, checking that each row's file
// exists and carries that exact name (a renamed workflow would otherwise stop alerting silently).
func mainRedCovered(t *testing.T, root string) ([]any, []string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, "ci", "main-red-workflows.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	var names []any
	var bad []string
	for _, line := range strings.Split(string(data), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		f := strings.Split(line, "\t")
		if len(f) != 5 {
			t.Fatalf("main-red-workflows.tsv row %q has %d fields, want 5", line, len(f))
		}
		if got := asString(loadWf(t, root, f[1])["name"]); got != f[0] {
			bad = append(bad, fmt.Sprintf("main-red-workflows.tsv: %s is named %q, the row says %q: a renamed workflow stops alerting", f[1], got, f[0]))
		}
		names = append(names, f[0])
	}
	return names, bad
}

func venueMainAlertViolations(t *testing.T, root string) []string {
	t.Helper()
	doc := loadWf(t, root, "venue-oracles-main-alert.yml")
	covered, out := mainRedCovered(t, root)

	events := triggerEvents(doc)
	cfg, ok := events["workflow_run"]
	switch run := asMap(cfg); {
	case !ok:
		out = append(out, "the main-alert workflow has no workflow_run trigger: it never hears the venue run complete")
	case run == nil:
		out = append(out, "the workflow_run trigger is bare: it names no workflow and no type")
	default:
		if keys := sortedKeys(run); !reflect.DeepEqual(keys, []string{"types", "workflows"}) {
			out = append(out, fmt.Sprintf("workflow_run declares %v, want only types and workflows: a branches or branches-ignore filter suppresses alerts for main runs", keys))
		}
		if got := asList(run["workflows"]); !reflect.DeepEqual(got, covered) {
			out = append(out, fmt.Sprintf("workflow_run.workflows is %v, want %v: the alert must listen to each covered workflow by its exact name, in ci/main-red-workflows.tsv order", got, covered))
		}
		if got := asList(run["types"]); !reflect.DeepEqual(got, []any{"completed"}) {
			out = append(out, fmt.Sprintf("workflow_run.types is %v, want [completed]", got))
		}
	}

	job := asMap(jobsOf(doc)["alert"])
	if got := asString(job["if"]); got != mainAlertIf {
		out = append(out, fmt.Sprintf("the alert job's if is %q, want %q: only main push runs may alert", got, mainAlertIf))
	}

	if top, ok := doc["permissions"].(yamlMap); !ok || len(top) != 0 {
		out = append(out, fmt.Sprintf("top-level permissions is %v, want {}: the token is granted at job level only", doc["permissions"]))
	}
	wantPerms := yamlMap{"contents": "read", "actions": "read", "issues": "write"}
	if got := asMap(job["permissions"]); !reflect.DeepEqual(got, wantPerms) {
		out = append(out, fmt.Sprintf("the alert job's permissions are %v, want %v: least privilege", got, wantPerms))
	}

	if _, has := doc["concurrency"]; has {
		out = append(out, "a workflow-level concurrency group is declared: it would take the group before the job's if and a non-main run could evict a queued main alert; put it on the job")
	}
	conc := asMap(job["concurrency"])
	if got := asString(conc["group"]); got != mainAlertGroup {
		out = append(out, fmt.Sprintf("the alert job's concurrency group is %q, want %q: one group per workflow, so two completions of one workflow cannot both open an issue and another workflow cannot evict a queued alert", got, mainAlertGroup))
	}
	if cancel, ok := conc["cancel-in-progress"].(bool); !ok || cancel {
		out = append(out, fmt.Sprintf("the alert job's cancel-in-progress is %v, want the literal false: a cancelled alert drops a red main run", conc["cancel-in-progress"]))
	}

	var runSteps []yamlMap
	for _, s := range stepsOf(job) {
		if _, has := s["run"]; has {
			runSteps = append(runSteps, s)
		}
	}
	if len(runSteps) != 1 {
		out = append(out, fmt.Sprintf("the alert job has %d run steps, want exactly 1", len(runSteps)))
	} else {
		if strings.Contains(asString(runSteps[0]["run"]), "${{") {
			out = append(out, "the alert step's run script holds a ${{ }} expression: event data reaches a shell only through env")
		}
		if code := venueShellCode(asString(runSteps[0]["run"])); len(code) != 1 || code[0].text != "bash ci/venue-main-alert.sh" {
			out = append(out, fmt.Sprintf("the alert step must run exactly `bash ci/venue-main-alert.sh` and nothing else, got %v: an echo, a comment or a trailing `|| true` sends no alert", code))
		}
		for _, key := range []string{"if", "continue-on-error"} {
			if v, has := runSteps[0][key]; has {
				out = append(out, fmt.Sprintf("the alert step declares %s: %v: the one step that sends the alert must always run and its failure must fail the job", key, v))
			}
		}
		env := asMap(runSteps[0]["env"])
		for key, want := range map[string]string{
			"RUN_WORKFLOW":   "${{ github.event.workflow_run.name }}",
			"RUN_ID":         "${{ github.event.workflow_run.id }}",
			"RUN_CONCLUSION": "${{ github.event.workflow_run.conclusion }}",
			"RUN_SHA":        "${{ github.event.workflow_run.head_sha }}",
		} {
			if got := asString(env[key]); got != want {
				out = append(out, fmt.Sprintf("the alert step's env %s is %q, want %q", key, got, want))
			}
		}
	}
	if v, has := job["continue-on-error"]; has {
		out = append(out, fmt.Sprintf("the alert job declares continue-on-error: %v: a failed alert must fail the job", v))
	}
	if leaf := stringLeafMatching(doc, secretsWord); leaf != "" {
		out = append(out, fmt.Sprintf("the main-alert workflow reads a secret (%q): it must use the job's own GITHUB_TOKEN only", leaf))
	}
	return out
}

func TestVenueMainAlertWorkflowShape(t *testing.T) {
	expectNoViolations(t, venueMainAlertViolations(t, repoRoot(t)))
}

func TestVenueMainAlertWorkflowPlantedDefects(t *testing.T) {
	t.Parallel()
	root := repoRoot(t)
	const f = mainAlertFile
	const tsv = "ci/main-red-workflows.tsv"
	files := []string{f, venueWorkflow, tsv}
	for _, w := range []string{"go", "go-quality", "go-auth-service", "go-workers-compose-proof", "live-e2e", "docker-images", "test", "codeql-analysis"} {
		files = append(files, ".github/workflows/"+w+".yml")
	}
	trigAll := "workflow_run:\n    workflows:\n      - Venue oracles\n      - Go\n      - Go quality\n      - Go auth service\n      - Go workers compose proof\n      - Live Backend E2E\n      - Build Docker images\n      - Tests\n      - CodeQL\n    types: [completed]\n"
	cases := []struct {
		name string
		muts []mutation
		want []string
	}{
		{"listens to a misspelt workflow", []mutation{{f, "      - Venue oracles\n", "      - Venue Oracles\n"}}, []string{"workflow_run.workflows"}},
		{"listens to the wrong event type", []mutation{{f, "types: [completed]", "types: [requested]"}}, []string{"workflow_run.types"}},
		{"bare workflow_run trigger", []mutation{{f, trigAll, "workflow_run:\n"}}, []string{"workflow_run trigger is bare"}},
		{"workflow_run replaced by push", []mutation{{f, trigAll, "push:\n"}}, []string{"no workflow_run trigger"}},
		{"scalar trigger", []mutation{{f, "on:\n  " + trigAll, "on: push\n"}}, []string{"no workflow_run trigger"}},
		{"a covered workflow is dropped from the listener", []mutation{{f, "      - Go quality\n", ""}}, []string{"workflow_run.workflows"}},
		{"a workflow is listened to but not in the table", []mutation{{f, "      - CodeQL\n", "      - CodeQL\n      - Security Scan\n"}}, []string{"workflow_run.workflows"}},
		{"a covered workflow is renamed", []mutation{{".github/workflows/go-quality.yml", "name: Go quality\n", "name: Go quality gate\n"}}, []string{"is named"}},
		{"the table row is dropped", []mutation{{tsv, "Tests\ttest.yml", "#Tests\ttest.yml"}}, []string{"workflow_run.workflows"}},
		{"the group ignores the workflow", []mutation{{f, "group: main-red-alert-${{ github.event.workflow_run.name }}", "group: main-red-alert"}}, []string{"concurrency group"}},
		{"the workflow name is not passed", []mutation{{f, "          RUN_WORKFLOW: ${{ github.event.workflow_run.name }}\n", ""}}, []string{"env RUN_WORKFLOW"}},
		{"workflow_run gains a branch filter", []mutation{{f, "types: [completed]", "types: [completed]\n    branches: [release]"}}, []string{"workflow_run declares [branches types workflows]"}},
		{"workflow_run gains a branches-ignore filter", []mutation{{f, "types: [completed]", "types: [completed]\n    branches-ignore: [main]"}}, []string{"workflow_run declares [branches-ignore types workflows]"}},
		{"the venue workflow is renamed", []mutation{{venueWorkflow, "name: Venue oracles\n", "name: Venue oracle suite\n"}}, []string{"is named"}},
		{"the push-only filter is dropped", []mutation{{f, " && github.event.workflow_run.event == 'push'", ""}}, []string{"only main push runs may alert"}},
		{"top-level permissions granted", []mutation{{f, "permissions: {}\n", "permissions:\n  contents: read\n"}}, []string{"top-level permissions"}},
		{"top-level permissions removed", []mutation{{f, "permissions: {}\n", ""}}, []string{"top-level permissions"}},
		{"job token loses least privilege", []mutation{{f, "      issues: write\n    steps:", "      issues: write\n      pull-requests: write\n    steps:"}}, []string{"least privilege"}},
		{"job token cannot write issues", []mutation{{f, "      issues: write\n    steps:", "      issues: read\n    steps:"}}, []string{"least privilege"}},
		{"workflow-level concurrency", []mutation{{f, "permissions: {}\n", "permissions: {}\nconcurrency:\n  group: x\n  cancel-in-progress: false\n"}}, []string{"workflow-level concurrency"}},
		{"alert group renamed", []mutation{{f, "group: main-red-alert-${{ github.event.workflow_run.name }}", "group: venue-alert"}}, []string{"concurrency group"}},
		{"alert jobs may be cancelled", []mutation{{f, "      cancel-in-progress: false", "      cancel-in-progress: true"}}, []string{"cancel-in-progress"}},
		{"cancel-in-progress becomes an expression", []mutation{{f, "      cancel-in-progress: false", "      cancel-in-progress: ${{ false }}"}}, []string{"cancel-in-progress"}},
		{"event data reaches the shell", []mutation{{f, "run: bash ci/venue-main-alert.sh", "run: bash ci/venue-main-alert.sh ${{ github.event.workflow_run.head_branch }}"}}, []string{"run script holds a ${{ }} expression"}},
		{"a second run step", []mutation{{f, "        run: bash ci/venue-main-alert.sh", "        run: bash ci/venue-main-alert.sh\n      - run: echo hi"}}, []string{"2 run steps"}},
		{"conclusion read from the wrong field", []mutation{{f, "RUN_CONCLUSION: ${{ github.event.workflow_run.conclusion }}", "RUN_CONCLUSION: ${{ github.event.workflow_run.head_branch }}"}}, []string{"env RUN_CONCLUSION"}},
		{"sha read from the wrong field", []mutation{{f, "RUN_SHA: ${{ github.event.workflow_run.head_sha }}", "RUN_SHA: ${{ github.sha }}"}}, []string{"env RUN_SHA"}},
		{"the alert step only echoes", []mutation{{f, "run: bash ci/venue-main-alert.sh", `run: echo "$RUN_ID"`}}, []string{"must run exactly `bash ci/venue-main-alert.sh`"}},
		{"the alert script is only in a comment", []mutation{{f, "run: bash ci/venue-main-alert.sh", `run: '# bash ci/venue-main-alert.sh'`}}, []string{"must run exactly `bash ci/venue-main-alert.sh`"}},
		{"the alert failure is swallowed", []mutation{{f, "run: bash ci/venue-main-alert.sh", "run: bash ci/venue-main-alert.sh || true"}}, []string{"must run exactly `bash ci/venue-main-alert.sh`"}},
		{"the alert script is skipped by an early exit", []mutation{{f, "run: bash ci/venue-main-alert.sh", "run: |\n          exit 0\n          bash ci/venue-main-alert.sh"}}, []string{"must run exactly `bash ci/venue-main-alert.sh`"}},
		{"the alert step is never run", []mutation{{f, "      - name: Open, update or close the main-red issue\n", "      - name: Open, update or close the main-red issue\n        if: false\n"}}, []string{"the alert step declares if"}},
		{"the alert step may fail silently", []mutation{{f, "      - name: Open, update or close the main-red issue\n", "      - name: Open, update or close the main-red issue\n        continue-on-error: true\n"}}, []string{"the alert step declares continue-on-error"}},
		{"the alert job may fail silently", []mutation{{f, "    timeout-minutes: 10\n", "    timeout-minutes: 10\n    continue-on-error: true\n"}}, []string{"the alert job declares continue-on-error"}},
		{"a secret is read through the index form", []mutation{{f, "          RUN_SHA: ${{ github.event.workflow_run.head_sha }}\n", "          RUN_SHA: ${{ github.event.workflow_run.head_sha }}\n          LINEAR: ${{ secrets['LINEAR_API_KEY'] }}\n"}}, []string{"reads a secret"}},
		{"all secrets are passed", []mutation{{f, "          RUN_SHA: ${{ github.event.workflow_run.head_sha }}\n", "          RUN_SHA: ${{ github.event.workflow_run.head_sha }}\n          ALL: ${{ toJSON(secrets) }}\n"}}, []string{"reads a secret"}},
		{"a secret is read", []mutation{{f, "          RUN_SHA: ${{ github.event.workflow_run.head_sha }}\n", "          RUN_SHA: ${{ github.event.workflow_run.head_sha }}\n          LINEAR: ${{ secrets.LINEAR_API_KEY }}\n"}}, []string{"reads a secret"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tree := plantTree(t, root, files, c.muts...)
			expectViolation(t, venueMainAlertViolations(t, tree), c.want...)
		})
	}
}
