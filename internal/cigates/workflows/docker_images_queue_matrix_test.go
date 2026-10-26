package workflows_test

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// CHAOS-11516: go.yml's go-container-build already builds the same six Go targets on both platforms in every
// non-docs-only merge-queue entry, and nothing is pushed from the queue. docker-images.yml's go-build therefore keeps
// one real-metadata canary leg on a merge_group run and the full matrix on every other event.

const (
	dockerMatrixDock = ".github/workflows/docker-images.yml"
	dockerMatrixGo   = ".github/workflows/go.yml"
)

var dockerMatrixFromJSON = regexp.MustCompile(`(?s)^\$\{\{\s*fromJSON\((.*)\)\s*\}\}$`)

// dockerMatrixAxis resolves one matrix axis for the event: a literal list, or ${{ fromJSON(<expression>) }}.
func dockerMatrixAxis(value any, event string) ([]string, error) {
	if list, ok := value.([]any); ok {
		var out []string
		for _, v := range list {
			out = append(out, fmt.Sprint(v))
		}
		return out, nil
	}
	text, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("axis is neither a list nor an expression: %v", value)
	}
	m := dockerMatrixFromJSON.FindStringSubmatch(strings.TrimSpace(text))
	if m == nil {
		return nil, fmt.Errorf("axis expression %q is not fromJSON(...)", text)
	}
	got, err := queueEval(strings.TrimSpace(m[1]), queueContext(event, "refs/heads/main"))
	if err != nil {
		return nil, err
	}
	var out []string
	if err := json.Unmarshal([]byte(got.s), &out); err != nil {
		return nil, fmt.Errorf("axis %q is not a JSON list of strings: %v", got.s, err)
	}
	return out, nil
}

func dockerMatrixEntries(v any) []map[string]string {
	var out []map[string]string
	for _, e := range asList(v) {
		row := map[string]string{}
		for k, val := range asMap(e) {
			row[k] = fmt.Sprint(val)
		}
		out = append(out, row)
	}
	return out
}

func dockerMatrixCovers(combo, part map[string]string) bool {
	for k, v := range part {
		if cv, ok := combo[k]; !ok || cv != v {
			return false
		}
	}
	return true
}

// dockerMatrixLegs expands a job's strategy.matrix the way GitHub does: product, exclude, then include.
func dockerMatrixLegs(job yamlMap, event string) ([]map[string]string, error) {
	matrix := asMap(asMap(job["strategy"])["matrix"])
	var axes []string
	for k := range matrix {
		if k != "include" && k != "exclude" {
			axes = append(axes, k)
		}
	}
	sort.Strings(axes)
	combos := []map[string]string{{}}
	for _, axis := range axes {
		values, err := dockerMatrixAxis(matrix[axis], event)
		if err != nil {
			return nil, fmt.Errorf("axis %s: %w", axis, err)
		}
		var next []map[string]string
		for _, c := range combos {
			for _, v := range values {
				n := map[string]string{axis: v}
				for k, cv := range c {
					n[k] = cv
				}
				next = append(next, n)
			}
		}
		combos = next
	}
	var kept []map[string]string
	for _, c := range combos {
		drop := false
		for _, ex := range dockerMatrixEntries(matrix["exclude"]) {
			if dockerMatrixCovers(c, ex) {
				drop = true
			}
		}
		if !drop {
			kept = append(kept, c)
		}
	}
	original := kept
	for _, inc := range dockerMatrixEntries(matrix["include"]) {
		added := false
		for _, c := range original {
			clash := false
			for k, v := range inc {
				if cv, isOriginal := c[k]; isOriginal && cv != v && containsAxis(axes, k) {
					clash = true
				}
			}
			if !clash {
				for k, v := range inc {
					c[k] = v
				}
				added = true
			}
		}
		if !added {
			kept = append(kept, inc)
		}
	}
	return kept, nil
}

func containsAxis(axes []string, k string) bool {
	for _, a := range axes {
		if a == k {
			return true
		}
	}
	return false
}

func dockerMatrixNames(legs []map[string]string) []string {
	var out []string
	for _, l := range legs {
		out = append(out, l["target"]+"/"+l["platform"])
	}
	sort.Strings(out)
	return out
}

func dockerMatrixFull() []string {
	var out []string
	for _, target := range []string{"auth-migrate", "auth-service", "contractcheck", "dho", "migrate", "operator"} {
		for _, platform := range []string{"linux/amd64", "linux/arm64"} {
			if target == "dho" && platform == "linux/arm64" {
				continue
			}
			out = append(out, target+"/"+platform)
		}
	}
	sort.Strings(out)
	return out
}

func dockerMatrixViolations(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	add := func(format string, args ...any) { out = append(out, fmt.Sprintf(format, args...)) }

	dock := loadWf(t, root, "docker-images.yml")
	build := pinJob(dock, "go-build")
	if build == nil {
		return []string{"docker-images.yml has no go-build job"}
	}
	// T1: the queue builds exactly the canary; T2: every other event builds the whole publish set.
	queue, err := dockerMatrixLegs(build, "merge_group")
	if err != nil {
		add("go-build matrix on merge_group: %v", err)
	} else if got := dockerMatrixNames(queue); strings.Join(got, ",") != "auth-service/linux/amd64" {
		add("go-build expands to %d legs on merge_group, want exactly [auth-service/linux/amd64]: %v", len(got), got)
	}
	for _, event := range []string{"push", "release", "workflow_dispatch", "pull_request"} {
		legs, err := dockerMatrixLegs(build, event)
		if err != nil {
			add("go-build matrix on %s: %v", event, err)
			continue
		}
		if got, want := dockerMatrixNames(legs), dockerMatrixFull(); strings.Join(got, ",") != strings.Join(want, ",") {
			add("go-build expands to %v on %s, want the 11 publish legs %v", got, event, want)
		}
	}
	for _, event := range []string{"merge_group", "push"} {
		legs, _ := dockerMatrixLegs(build, event)
		for _, l := range legs {
			if l["target"] == "" || l["platform"] == "" {
				add("go-build has a leg with no target or platform on %s (an include entry created it): %v", event, l)
			}
		}
	}
	if strings.Contains(asString(build["runs-on"]), "matrix.runner") {
		add("go-build runs-on reads matrix.runner, which an include entry must supply: that include creates a bogus leg once the queue matrix is amd64 only")
	}

	// T3: image-queue-gate still needs every build job and judges each as queue-heavy.
	gate := pinJob(dock, "image-queue-gate")
	needs := strings.Join(pinNeeds(gate), ",")
	for _, job := range []string{"go-build", "go-api-tools-build", "go-build-dho-arm64"} {
		if !strings.Contains(","+needs+",", ","+job+",") {
			add("image-queue-gate no longer needs %s", job)
		}
		if !strings.Contains(readFile(t, root+"/"+dockerMatrixDock), job+"|queue-heavy|${{ needs."+job+".result }}") {
			add("image-queue-gate no longer judges %s as queue-heavy", job)
		}
	}
	for _, job := range []string{"go-build", "go-build-dho-arm64", "go-api-tools-build"} {
		cond := strings.TrimSpace(asString(pinJob(dock, job)["if"]))
		ctx := queueContext("merge_group", "refs/heads/gh-readonly-queue/main/pr-1-abc")
		if v, err := queueEval(cond, ctx); err != nil || !v.truthy() {
			add("%s does not run on a merge_group entry (err %v)", job, err)
		}
	}

	// Coverage: wherever go-build runs on the queue, go.yml's go-container-build runs too, and a required gate feeds on it.
	goDoc := loadWf(t, root, "go.yml")
	cb := pinJob(goDoc, "go-container-build")
	if cb == nil {
		return append(out, "go.yml has no go-container-build job: the queue would build the Go targets nowhere")
	}
	for _, docs := range []string{"false", "true"} {
		dctx := queueContext("merge_group", "refs/heads/gh-readonly-queue/main/pr-1-abc")
		dctx["needs.changes.outputs.docs_only"] = docs
		dv, derr := queueEval(strings.TrimSpace(asString(build["if"])), dctx)
		gctx := queueContext("merge_group", "refs/heads/gh-readonly-queue/main/pr-1-abc")
		gctx["needs.go-storage-integration-plan.outputs.docs_only"] = docs
		gv, gerr := queueEval(strings.TrimSpace(asString(cb["if"])), gctx)
		if derr != nil || gerr != nil {
			add("coverage docs_only=%s: eval errors docker %v go %v", docs, derr, gerr)
			continue
		}
		if dv.truthy() && !gv.truthy() {
			add("on a merge_group entry (docs_only=%s) docker-images go-build runs but go.yml go-container-build does not", docs)
		}
	}
	if strings.Contains(readFile(t, root+"/"+dockerMatrixGo), "merge_group:\n    paths") {
		add("go.yml merge_group has a paths filter: go-container-build can be absent from an entry that docker-images builds")
	}
	if !strings.Contains(readFile(t, root+"/"+dockerMatrixGo), "GATED_JOB_8: go-container-build|queue-heavy|${{ needs.go-container-build.result }}") {
		add("go-queue-gate does not judge go-container-build as queue-heavy")
	}
	gq := pinJob(goDoc, "go-queue-gate")
	if !strings.Contains(","+strings.Join(pinNeeds(gq), ",")+",", ",go-container-build,") {
		add("go-queue-gate does not need go-container-build")
	}
	return out
}

func TestDockerImagesGoBuildQueueMatrix(t *testing.T) {
	expectNoViolations(t, dockerMatrixViolations(t, repoRoot(t)))
}

func TestDockerImagesGoBuildQueueMatrixPlantedDefects(t *testing.T) {
	root := repoRoot(t)
	files := []string{dockerMatrixDock, dockerMatrixGo}
	cases := []struct {
		name string
		muts []mutation
		want []string
	}{
		{"queue builds the whole matrix again",
			[]mutation{{dockerMatrixDock, "fromJSON(github.event_name == 'merge_group' && '[\"auth-service\"]' ||", "fromJSON(false && '[\"auth-service\"]' ||"}},
			[]string{"legs on merge_group"}},
		{"queue canary is another target",
			[]mutation{{dockerMatrixDock, "'[\"auth-service\"]'", "'[\"dho\"]'"}},
			[]string{"want exactly [auth-service/linux/amd64]"}},
		{"an arm64 include entry makes a bogus leg on the queue",
			[]mutation{{dockerMatrixDock, "        exclude:\n          - target: dho\n", "        include:\n          - platform: linux/arm64\n            runner: ubuntu-latest\n        exclude:\n          - target: dho\n"}},
			[]string{"no target or platform"}},
		{"the publish set shrinks on a main push",
			[]mutation{{dockerMatrixDock, "github.event_name == 'merge_group' && '[\"linux/amd64\"]'", "github.event_name != 'release' && '[\"linux/amd64\"]'"}},
			[]string{"11 publish legs"}},
		{"image-queue-gate stops needing go-build",
			[]mutation{{dockerMatrixDock, "needs: [changes, resolve, go-build, go-api-tools-build, go-build-dho-arm64]", "needs: [changes, resolve, go-api-tools-build, go-build-dho-arm64]"}},
			[]string{"no longer needs go-build"}},
		{"go.yml go-container-build stops running on a queue entry",
			[]mutation{{dockerMatrixGo, "    if: (!(github.event_name == 'pull_request' && !contains(github.event.pull_request.labels.*.name, 'ci:full'))) && needs.go-storage-integration-plan.outputs.docs_only != 'true'\n    name: go-container-build", "    if: github.event_name == 'push'\n    name: go-container-build"}},
			[]string{"go-container-build does not"}},
		{"go-queue-gate stops judging go-container-build",
			[]mutation{{dockerMatrixGo, "GATED_JOB_8: go-container-build|queue-heavy|", "GATED_JOB_8: go-container-build|unconditional|"}},
			[]string{"go-queue-gate does not judge go-container-build"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			expectViolation(t, dockerMatrixViolations(t, plantTree(t, root, files, c.muts...)), c.want...)
		})
	}
}
