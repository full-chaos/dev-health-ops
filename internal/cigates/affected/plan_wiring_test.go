package affected_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// The plan verdict is wired into the integration plan job (go.yml), the go-quality legs (go-quality.yml) and the
// venue-oracles relevance step; ci/go_relevance.sh is gone.

type wfStep struct {
	ID   string            `yaml:"id"`
	Name string            `yaml:"name"`
	If   string            `yaml:"if"`
	Env  map[string]string `yaml:"env"`
	Run  string            `yaml:"run"`
}

type wfJob struct {
	Needs   any               `yaml:"needs"`
	Outputs map[string]string `yaml:"outputs"`
	Steps   []wfStep          `yaml:"steps"`
}

func loadJobs(t *testing.T, rel string) map[string]wfJob {
	t.Helper()
	var wf struct {
		Jobs map[string]wfJob `yaml:"jobs"`
	}
	if err := yaml.Unmarshal([]byte(readText(t, scriptPath(t, rel))), &wf); err != nil {
		t.Fatalf("%s: %v", rel, err)
	}
	return wf.Jobs
}

func needsOf(j wfJob) []string {
	switch n := j.Needs.(type) {
	case string:
		return []string{n}
	case []any:
		var out []string
		for _, v := range n {
			if s, ok := v.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}

func stepRunning(j wfJob, needle string) (wfStep, bool) {
	for _, s := range j.Steps {
		if strings.Contains(s.Run, needle) {
			return s, true
		}
	}
	return wfStep{}, false
}

const eventExpr = "${{ github.event_name }}"

func TestIntegrationPlanJobRunsThePlanVerdict(t *testing.T) {
	jobs := loadJobs(t, ".github/workflows/go.yml")
	plan, ok := jobs["go-storage-integration-plan"]
	if !ok {
		t.Fatal("go.yml has no go-storage-integration-plan job")
	}
	s, ok := stepRunning(plan, "plan-verdict")
	if !ok {
		t.Fatal("go-storage-integration-plan runs no `cigate plan-verdict`")
	}
	for _, want := range []string{`--event "${EVENT_NAME}"`, `--base "${BASE_SHA}"`} {
		if !strings.Contains(s.Run, want) {
			t.Errorf("the integration plan step does not pass %s", want)
		}
	}
	if s.Env["EVENT_NAME"] != eventExpr {
		t.Errorf("the integration plan step's EVENT_NAME is %q, want %q", s.Env["EVENT_NAME"], eventExpr)
	}
	if strings.Contains(s.Run, "--go-files-full") {
		t.Error("the integration plan must not pass --go-files-full: a Go change selects its reverse closure")
	}
	if _, old := stepRunning(plan, "affected-integration"); old {
		t.Error("go.yml still runs `cigate affected-integration`")
	}
}

func TestGoQualityLegsRunThePlanVerdict(t *testing.T) {
	jobs := loadJobs(t, ".github/workflows/go-quality.yml")
	plan, ok := jobs["go-quality-plan"]
	if !ok {
		t.Fatal("go-quality.yml has no go-quality-plan job")
	}
	s, ok := stepRunning(plan, "plan-verdict")
	if !ok {
		t.Fatal("go-quality-plan runs no `cigate plan-verdict`")
	}
	for _, want := range []string{`--event "${EVENT_NAME}"`, "--go-files-full", "ci/go_relevant_diff.sh"} {
		if !strings.Contains(s.Run, want) {
			t.Errorf("the go-quality plan step does not contain %s", want)
		}
	}
	if s.Env["EVENT_NAME"] != eventExpr {
		t.Errorf("the go-quality plan step's EVENT_NAME is %q, want %q: a push to main, a merge group or a schedule must read FULL", s.Env["EVENT_NAME"], eventExpr)
	}
	if plan.Outputs["verdict"] == "" || plan.Outputs["selection"] == "" {
		t.Errorf("go-quality-plan must output verdict and selection, got %v", plan.Outputs)
	}
	leg := jobs["go-quality-leg"]
	if !strings.Contains(strings.Join(needsOf(leg), ","), "go-quality-plan") {
		t.Errorf("go-quality-leg must need go-quality-plan, needs %v", needsOf(leg))
	}
	var rel wfStep
	for _, st := range leg.Steps {
		if st.ID == "relevance" {
			rel = st
		}
	}
	if !strings.Contains(rel.Env["PLAN_VERDICT"], "needs.go-quality-plan.outputs.verdict") {
		t.Fatal("the legs' relevance step does not read needs.go-quality-plan.outputs.verdict")
	}
	if !strings.Contains(rel.Run, "GO_QUALITY_SELECTION_FILE") {
		t.Error("the legs' relevance step does not hand the selection to check_go.sh (GO_QUALITY_SELECTION_FILE)")
	}
	fan := jobs["go-quality"]
	if !strings.Contains(strings.Join(needsOf(fan), ","), "go-quality-plan") {
		t.Errorf("the go-quality fan-in must need go-quality-plan, needs %v", needsOf(fan))
	}
	agg, ok := stepRunning(fan, "aggregate_gate_results")
	found := false
	for k, v := range agg.Env {
		if ok && strings.HasPrefix(k, "GATED_JOB_") && strings.HasPrefix(v, "go-quality-plan|unconditional|") {
			found = true
		}
	}
	if !found {
		t.Error("the go-quality fan-in does not gate go-quality-plan as unconditional")
	}
}

func TestVenueOraclesRelevanceRunsThePlanVerdict(t *testing.T) {
	jobs := loadJobs(t, ".github/workflows/venue-oracles.yml")
	found := false
	for _, j := range jobs {
		for _, s := range j.Steps {
			if s.ID == "relevance" && strings.Contains(s.Run, "cigate plan-verdict") && strings.Contains(s.Run, "--event range") {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("venue-oracles.yml's relevance step does not run `cigate plan-verdict --event range`")
	}
}

func TestGoRelevanceDeciderIsGone(t *testing.T) {
	root := repoRoot(t)
	if _, err := os.Stat(filepath.Join(root, "ci", "go_relevance.sh")); err == nil {
		t.Error("ci/go_relevance.sh still exists: the plan verdict replaces it")
	}
	for _, wf := range []string{"go.yml", "go-quality.yml", "venue-oracles.yml"} {
		raw := readText(t, filepath.Join(root, ".github", "workflows", wf))
		if strings.Contains(raw, "bash ci/go_relevance.sh") {
			t.Errorf("%s still runs ci/go_relevance.sh", wf)
		}
	}
}

func TestIntegrationPlannerReadsNoneAsZeroLegs(t *testing.T) {
	script := scriptPath(t, "ci/check_go.sh")
	work := t.TempDir()
	sel := filepath.Join(work, "none.txt")
	if err := os.WriteFile(sel, []byte("NONE\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	res, out, err := plan(script, work, sel)
	if err != nil {
		t.Fatal(err)
	}
	if res.code != 0 || out["legs"] != "0" {
		t.Fatalf("a NONE selection must plan zero legs, got exit %d legs=%q: %s%s", res.code, out["legs"], res.stderr, res.stdout)
	}
}

// shardPlan runs `check_go.sh VERB SHARD COUNT` with a go-quality selection file and returns the package keys it lists.
func shardPlan(t *testing.T, verb, shard, count, selection string) ([]string, cmdResult) {
	t.Helper()
	root := repoRoot(t)
	sel := filepath.Join(t.TempDir(), "selection.txt")
	if err := os.WriteFile(sel, []byte(selection), 0o644); err != nil {
		t.Fatal(err)
	}
	env := inheritedEnv([]string{"GOFLAGS", "GO_QUALITY_SELECTION_FILE"}, "GOFLAGS=", "GO_QUALITY_SELECTION_FILE="+sel)
	res := cmdSpec{dir: root, env: env}.run("bash", filepath.Join(root, "ci", "check_go.sh"), verb, shard, count)
	var keys []string
	for _, line := range strings.Split(strings.TrimSpace(res.stdout), "\n") {
		if f := strings.Split(line, "\t"); len(f) == 3 {
			keys = append(keys, f[2])
		}
	}
	return keys, res
}

func TestQualityShardPlansRunOnlyTheSelection(t *testing.T) {
	const pkg = "internal/cigates/affected"
	for _, verb := range []string{"test-shard-plan", "race-shard-plan"} {
		var all []string
		for _, shard := range []string{"1", "2", "3"} {
			keys, res := shardPlan(t, verb, shard, "3", "AFFECTED 1\n"+pkg+"\n")
			if res.code != 0 {
				t.Fatalf("%s %s 3 with a one-package selection exited %d: %s", verb, shard, res.code, res.stderr)
			}
			all = append(all, keys...)
		}
		if len(all) != 1 || all[0] != pkg {
			shown := all
			if len(shown) > 3 {
				shown = shown[:3]
			}
			t.Errorf("%s over three shards listed %d package(s) (first %q), want exactly [%s]", verb, len(all), shown, pkg)
		}
	}
}

// The declared-read guard runs in the static leg, and the static leg runs on every change set: the guard walks every file,
// so it is enforced by the always-run leg, never by a selection.
func TestPlanReadsGuardRunsInTheAlwaysRunStaticLeg(t *testing.T) {
	script := readText(t, scriptPath(t, "ci/check_go.sh"))
	if !strings.Contains(script, "      ci_leg_stage plan-reads check_plan_reads\n") {
		t.Error("the static ci-leg does not run check_plan_reads")
	}
	if !strings.Contains(script, "go run -mod=readonly -trimpath ./cmd/cigate plan-reads --root \"${ROOT}\"") {
		t.Error("check_plan_reads does not run cmd/cigate plan-reads over the repository root")
	}
	jobs := loadJobs(t, ".github/workflows/go-quality.yml")
	var rel wfStep
	for _, st := range jobs["go-quality-leg"].Steps {
		if st.ID == "relevance" {
			rel = st
		}
	}
	_, block, ok := strings.Cut(rel.Run, "if [ \"${LEG}\" = \"static\" ]; then")
	block, _, _ = strings.Cut(block, "fi\n")
	if !ok || !strings.Contains(block, "relevant=true") || !strings.Contains(block, "exit 0") || strings.Contains(block, "relevant=false") {
		t.Errorf("the relevance step does not run the static leg on every change set:\n%s", rel.Run)
	}
}

// A pull request must not judge itself: both plan steps build cigate from a checkout of the merge base and run that
// binary on the PR tree, never `go run ./cmd/cigate` from the PR's own tree.
func TestPlanStepsBuildThePlannerFromTheMergeBase(t *testing.T) {
	for _, c := range []struct{ file, job string }{
		{".github/workflows/go.yml", "go-storage-integration-plan"},
		{".github/workflows/go-quality.yml", "go-quality-plan"},
	} {
		t.Run(c.job, func(t *testing.T) {
			s, ok := stepRunning(loadJobs(t, c.file)[c.job], "plan-verdict")
			if !ok {
				t.Fatalf("%s runs no plan-verdict", c.job)
			}
			for _, want := range []string{
				`mb="$(git merge-base "${BASE_SHA}" HEAD)"`,
				`git worktree add --detach "${RUNNER_TEMP}/planbase" "${mb}"`,
				`go -C "${RUNNER_TEMP}/planbase" build -trimpath -mod=readonly -o "${RUNNER_TEMP}/cigate-base" ./cmd/cigate`,
				`"${RUNNER_TEMP}/cigate-base" plan-verdict`,
				`--root "${GITHUB_WORKSPACE}"`,
			} {
				if !strings.Contains(s.Run, want) {
					t.Errorf("the %s plan step does not contain %s", c.job, want)
				}
			}
			if strings.Contains(s.Run, "./cmd/cigate plan-verdict") {
				t.Errorf("the %s plan step runs the planner from the PR's own tree", c.job)
			}
		})
	}
}
