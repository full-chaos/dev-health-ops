package cigates_test

import (
	"fmt"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// CHAOS-10174 PR C: Go port of tests/tooling/test_go_quality_relevance_wiring.py. go-quality.yml's relevance step must
// supply the push-event BASE_SHA fallback and use the NUL-safe diff producer. The scripts' own tests (go_relevant_diff_test.go,
// internal/relevancedecider) hand BASE_SHA over directly; neither can see a regression in the WORKFLOW's wiring, such as an
// edit that moves github.event.before out of last position or reverts the producer to a plain `git diff --name-only`.

const (
	goQualityJob         = "go-quality-leg" // the matrix job; `go-quality` is the fan-in that keeps the required context name
	canonicalBaseSHAExpr = "${{ github.event.pull_request.base.sha || github.event.merge_group.base_sha || github.event.before }}"
)

func relevanceWiringDefects(workflow string) []string {
	var doc struct {
		Jobs map[string]struct {
			Steps []struct {
				ID  string            `yaml:"id"`
				Env map[string]string `yaml:"env"`
				Run string            `yaml:"run"`
			} `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal([]byte(workflow), &doc); err != nil {
		return []string{"go-quality.yml is not valid YAML: " + err.Error()}
	}
	job, ok := doc.Jobs[goQualityJob]
	if !ok {
		return []string{fmt.Sprintf("go-quality.yml has no job %q", goQualityJob)}
	}
	var found []int
	for i, s := range job.Steps {
		if s.ID == "relevance" {
			found = append(found, i)
		}
	}
	if len(found) != 1 {
		return []string{fmt.Sprintf("expected exactly one step with id: relevance in job %q, found %d", goQualityJob, len(found))}
	}
	step := job.Steps[found[0]]
	var d []string
	if got := step.Env["BASE_SHA"]; got != canonicalBaseSHAExpr {
		d = append(d, fmt.Sprintf("the relevance step's BASE_SHA expression is %q, want exactly %q: a push event must fall back to github.event.before as the LAST alternative", got, canonicalBaseSHAExpr))
	}
	if !strings.Contains(step.Run, "ci/go_relevant_diff.sh") {
		d = append(d, "the relevance step does not call ci/go_relevant_diff.sh: a plain git diff loses a renamed-out path and a non-ASCII path")
	}
	if strings.Contains(step.Run, "git diff --name-only") {
		d = append(d, "the relevance step still calls a plain `git diff --name-only`, the lossy shape ci/go_relevant_diff.sh replaces")
	}
	return d
}

func TestGoQualityRelevanceStepWiring(t *testing.T) {
	workflow := readText(t, scriptPath(t, ".github/workflows/go-quality.yml"))
	if d := relevanceWiringDefects(workflow); len(d) > 0 {
		t.Fatalf("go-quality.yml relevance wiring: %s", strings.Join(d, "; "))
	}
	plants := []struct{ name, old, repl, want string }{
		{"event.before moved out of last position",
			"github.event.pull_request.base.sha || github.event.merge_group.base_sha || github.event.before }}",
			"github.event.commits[0].id || github.event.before }}", "BASE_SHA expression"},
		{"push fallback dropped",
			"github.event.pull_request.base.sha || github.event.merge_group.base_sha || github.event.before }}",
			"github.event.pull_request.base.sha || github.event.merge_group.base_sha }}", "BASE_SHA expression"},
		{"diff producer replaced by a plain git diff", "bash ci/go_relevant_diff.sh > changed.txt", "git diff --name-only > changed.txt", "does not call ci/go_relevant_diff.sh"},
		{"relevance step id renamed", "id: relevance\n        env:", "id: decide\n        env:", "found 0"},
	}
	for _, pl := range plants {
		pl := pl
		t.Run("plant/"+pl.name, func(t *testing.T) {
			if n := strings.Count(workflow, pl.old); n < 1 {
				t.Fatalf("anchor %q is absent from go-quality.yml: the plant measured nothing", pl.old)
			}
			planted := strings.Replace(workflow, pl.old, pl.repl, 1)
			if joined := strings.Join(relevanceWiringDefects(planted), "; "); !strings.Contains(joined, pl.want) {
				t.Fatalf("planted defect %q not caught: %q, want one mentioning %q", pl.name, joined, pl.want)
			}
		})
	}
}
