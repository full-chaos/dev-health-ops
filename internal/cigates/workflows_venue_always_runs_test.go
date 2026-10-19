package cigates_test

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// CHAOS-10177 (B2): Go port of tests/tooling/test_venue_oracles_always_runs.py, the workflow-shape tests. The
// `venue-oracles` job runs on main and by hand, never as a pull request check, from exactly one workflow with no path
// filter; the relevance step wires github.event.before and answers relevant=true for workflow_dispatch before any diff
// is read. The two tests that EXECUTE the relevance step stay in Python until the venue-script port (CHAOS-10178).

const (
	venueJobID       = "venue-oracles"
	venueWorkflow    = ".github/workflows/venue-oracles.yml"
	venueGoWorkflow  = ".github/workflows/go.yml"
	venueFiltersKeys = "paths paths-ignore branches-ignore tags"
)

func workflowsDeclaringJob(t *testing.T, root, job string) []string {
	t.Helper()
	var names []string
	for _, name := range workflowNames(t, root) {
		if _, ok := jobsOf(loadWf(t, root, name))[job]; ok {
			names = append(names, name)
		}
	}
	return names
}

func venueRelevanceStep(job any) (yamlMap, int) {
	var found []yamlMap
	for _, s := range stepsOf(job) {
		if asString(s["id"]) == "relevance" {
			found = append(found, s)
		}
	}
	if len(found) != 1 {
		return nil, len(found)
	}
	return found[0], 1
}

func noSpaces(s string) string { return strings.ReplaceAll(s, " ", "") }

func venueAlwaysRunsViolations(t *testing.T, root string) []string {
	t.Helper()
	declaring := workflowsDeclaringJob(t, root, venueJobID)
	if len(declaring) == 0 {
		return []string{fmt.Sprintf("no workflow declares a job named %q: renamed or deleted; an empty result must not pass silently", venueJobID)}
	}
	var out []string
	if len(declaring) != 1 {
		out = append(out, fmt.Sprintf("job %q is declared in %d workflows %v: two workflows producing one context race to report it", venueJobID, len(declaring), declaring))
	}
	if len(declaring) == 1 && declaring[0] != "venue-oracles.yml" {
		out = append(out, fmt.Sprintf("%q now lives in %s, not venue-oracles.yml: a move must update this pin in the same change", venueJobID, declaring[0]))
	}
	if _, ok := jobsOf(loadWf(t, root, "go.yml"))[venueJobID]; ok {
		out = append(out, fmt.Sprintf("go.yml declares a job named %q again: go.yml is path-filtered by design, the job must live only in an always-triggering workflow", venueJobID))
	}
	if len(declaring) != 1 {
		return out
	}
	doc := loadWf(t, root, declaring[0])
	events := triggerEvents(doc)
	kinds := make([]string, 0, len(events))
	for k := range events {
		kinds = append(kinds, k)
	}
	sort.Strings(kinds)
	if !reflect.DeepEqual(kinds, []string{"push", "workflow_dispatch"}) {
		out = append(out, fmt.Sprintf("%s triggers on %v; it must trigger on exactly push and workflow_dispatch: a pull_request, pull_request_target or merge_group trigger makes it a PR check again, a missing push trigger means main is never checked", declaring[0], kinds))
	}
	if cfg, ok := events["push"]; ok {
		push := asMap(cfg)
		if push == nil {
			out = append(out, fmt.Sprintf("%s's push trigger is bare (%v): a bare push runs on every branch push, it must name branches: [main]", declaring[0], cfg))
		} else {
			if got := asList(push["branches"]); !reflect.DeepEqual(got, []any{"main"}) {
				out = append(out, fmt.Sprintf("%s's push trigger has branches %v; it must be exactly [main]", declaring[0], push["branches"]))
			}
			for _, key := range strings.Fields(venueFiltersKeys) {
				if _, has := push[key]; has {
					out = append(out, fmt.Sprintf("%s's push trigger declares a filter (%s): a path filter skips the run on main with no record, decide relevance inside the job", declaring[0], key))
				}
			}
		}
	}
	job := asMap(jobsOf(doc)[venueJobID])
	if cond, has := job["if"]; has {
		out = append(out, fmt.Sprintf("the %q job carries a job-level if: (%v): GitHub reports a conditionally skipped job as Success, indistinguishable from a real pass; put the condition on a step", venueJobID, cond))
	}
	step, n := venueRelevanceStep(job)
	if n != 1 {
		return append(out, fmt.Sprintf("expected exactly one step with id: relevance in %s's %q job, found %d", declaring[0], venueJobID, n))
	}
	env := asMap(step["env"])
	if got := noSpaces(asString(env["BASE_SHA"])); got != "${{github.event.before}}" {
		out = append(out, fmt.Sprintf("the relevance step's BASE_SHA expression (%q) does not fall back to github.event.before: a push then leaves BASE_SHA empty and the diff sees only the last commit of a multi-commit push", asString(env["BASE_SHA"])))
	}
	if got := noSpaces(asString(env["EVENT_NAME"])); got != "${{github.event_name}}" {
		out = append(out, fmt.Sprintf("the relevance step's EVENT_NAME is %q, not ${{ github.event_name }}", asString(env["EVENT_NAME"])))
	}
	script := asString(step["run"])
	const guard = `if [ "${EVENT_NAME}" = "workflow_dispatch" ]; then`
	at := strings.Index(script, guard)
	if at < 0 {
		return append(out, "the relevance step does not branch on workflow_dispatch before it reads the diff")
	}
	end := strings.Index(script[at:], "fi")
	branch := script[at:]
	if end >= 0 {
		branch = script[at : at+end]
	}
	if !strings.Contains(branch, `printf 'relevant=true\n' >> "${GITHUB_OUTPUT}"`) || !strings.Contains(branch, "exit 0") {
		out = append(out, "the workflow_dispatch branch of the relevance step must write relevant=true and stop, so the full suite runs")
	}
	if diff := strings.Index(script, "go_relevant_diff.sh"); diff >= 0 && at > diff {
		out = append(out, "the workflow_dispatch branch must come before the diff is read")
	}
	return out
}

func TestVenueJobAlwaysRuns(t *testing.T) {
	expectNoViolations(t, venueAlwaysRunsViolations(t, repoRoot(t)))
}

func venueAlwaysRunsFiles(t *testing.T, root string) []string {
	t.Helper()
	var files []string
	for _, n := range workflowNames(t, root) {
		files = append(files, ".github/workflows/"+n)
	}
	return files
}

func TestVenueJobAlwaysRunsPlantedDefects(t *testing.T) {
	root := repoRoot(t)
	const f = venueWorkflow
	const triggers = "on:\n  push:\n    branches: [main]\n  workflow_dispatch:\n"
	cases := []struct {
		name string
		muts []mutation
		want []string
	}{
		{"job renamed away", []mutation{{f, "jobs:\n  venue-oracles:\n", "jobs:\n  venue-oraclez:\n"}}, []string{"no workflow declares"}},
		{"go.yml regains the job", []mutation{{venueGoWorkflow, "\njobs:\n", "\njobs:\n  venue-oracles:\n    runs-on: ubuntu-latest\n    steps:\n      - run: 'true'\n"}}, []string{"go.yml declares a job named", "declared in 2 workflows"}},
		{"pull_request trigger added", []mutation{{f, triggers, triggers + "  pull_request:\n"}}, []string{"triggers on [pull_request push workflow_dispatch]"}},
		{"merge_group trigger added", []mutation{{f, triggers, triggers + "  merge_group:\n"}}, []string{"triggers on [merge_group push workflow_dispatch]"}},
		{"push trigger lost", []mutation{{f, triggers, "on:\n  workflow_dispatch:\n"}}, []string{"triggers on [workflow_dispatch]"}},
		{"bare push (every branch)", []mutation{{f, triggers, "on:\n  push:\n  workflow_dispatch:\n"}}, []string{"push trigger is bare", "every branch"}},
		{"bare push in the list form", []mutation{{f, triggers, "on: [push, workflow_dispatch]\n"}}, []string{"push trigger is bare"}},
		{"scalar trigger", []mutation{{f, triggers, "on: push\n"}}, []string{"triggers on [push]", "push trigger is bare"}},
		{"push branches widened", []mutation{{f, "branches: [main]", "branches: [main, dev]"}}, []string{"must be exactly [main]"}},
		{"push gains a path filter", []mutation{{f, "branches: [main]\n  workflow_dispatch:", "branches: [main]\n    paths: ['internal/**']\n  workflow_dispatch:"}}, []string{"declares a filter (paths)"}},
		{"push gains tags", []mutation{{f, "branches: [main]\n  workflow_dispatch:", "branches: [main]\n    tags: ['v*']\n  workflow_dispatch:"}}, []string{"declares a filter (tags)"}},
		{"job gains a job-level if", []mutation{{f, "jobs:\n  venue-oracles:\n", "jobs:\n  venue-oracles:\n    if: false\n"}}, []string{"job-level if:"}},
		{"relevance step loses its id", []mutation{{f, "id: relevance", "id: relevancy"}}, []string{"exactly one step with id: relevance", "found 0"}},
		{"BASE_SHA no longer the push before sha", []mutation{{f, "BASE_SHA: ${{ github.event.before }}", "BASE_SHA: ''"}}, []string{"BASE_SHA", "github.event.before"}},
		{"EVENT_NAME no longer the event", []mutation{{f, "EVENT_NAME: ${{ github.event_name }}", "EVENT_NAME: push"}}, []string{"EVENT_NAME"}},
		{"dispatch branch lost", []mutation{{f, `if [ "${EVENT_NAME}" = "workflow_dispatch" ]; then`, `if [ "${EVENT_NAME}" = "workflow_call" ]; then`}}, []string{"does not branch on workflow_dispatch"}},
		{"dispatch branch no longer writes relevant=true", []mutation{{f, `printf 'relevant=true\n' >> "${GITHUB_OUTPUT}"`, `printf 'relevant=false\n' >> "${GITHUB_OUTPUT}"`}}, []string{"must write relevant=true and stop"}},
		{"diff read before the dispatch branch", []mutation{{f, "set -euo pipefail\n          if [ \"${EVENT_NAME}\"", "set -euo pipefail\n          bash ci/go_relevant_diff.sh >/dev/null\n          if [ \"${EVENT_NAME}\""}}, []string{"must come before the diff is read"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tree := plantTree(t, root, venueAlwaysRunsFiles(t, root), c.muts...)
			expectViolation(t, venueAlwaysRunsViolations(t, tree), c.want...)
		})
	}

	t.Run("a second workflow declares the job", func(t *testing.T) {
		tree := plantTree(t, root, venueAlwaysRunsFiles(t, root))
		extra := "on:\n  push:\n    branches: [main]\njobs:\n  venue-oracles:\n    runs-on: ubuntu-latest\n    steps:\n      - run: 'true'\n"
		if err := os.WriteFile(filepath.Join(tree, ".github/workflows/second.yml"), []byte(extra), 0o644); err != nil {
			t.Fatal(err)
		}
		expectViolation(t, venueAlwaysRunsViolations(t, tree), "declared in 2 workflows", "second.yml")
	})
	t.Run("the job moves to another file", func(t *testing.T) {
		tree := plantTree(t, root, venueAlwaysRunsFiles(t, root))
		if err := os.Rename(filepath.Join(tree, venueWorkflow), filepath.Join(tree, ".github/workflows/moved.yml")); err != nil {
			t.Fatal(err)
		}
		expectViolation(t, venueAlwaysRunsViolations(t, tree), "now lives in moved.yml")
	})
}
