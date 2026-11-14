package testshard_test

import (
	"strconv"
	"strings"
	"testing"
	"time"
)

// setLongLegBudget is the environment of a leg 44 minutes into a 45 minute cap: ci/check_go.sh refuses every guarded go test there.
func setLongLegBudget(t *testing.T) {
	t.Helper()
	t.Setenv("GITHUB_ACTIONS", "true")
	t.Setenv("CHECK_GO_JOB_CAP_MIN", "45")
	t.Setenv("CHECK_GO_JOB_START", strconv.FormatInt(time.Now().Add(-44*time.Minute).Unix(), 10))
	t.Setenv("CHECK_GO_NOW_EPOCH", "1")
	t.Setenv("CHECK_GO_NOW_EPOCH_FILE", "/nonexistent")
}

// Not parallel: t.Setenv. A test that runs check_go.sh must not inherit the leg's job budget.
func TestCheckGoChildrenOfTheTestshardHelpersDoNotInheritTheLegJobBudget(t *testing.T) {
	setLongLegBudget(t)
	for name, call := range map[string]func() (string, string, error){
		"run": func() (string, string, error) {
			return run(t, nil, "bash", "ci/check_go.sh", "job-timeout", "25")
		},
		"authTree.checkGo": func() (string, string, error) {
			return authTree{repoRoot(t)}.checkGo(t, nil, "job-timeout", "25")
		},
	} {
		out, errs, err := call()
		if err != nil || !strings.Contains(out, "1500s") {
			t.Errorf("%s: the child inherited the leg's job budget: err=%v stdout=%q stderr=%q", name, err, out, errs)
		}
	}
}

// A test that wants a deadline still gets it through its own env argument.
func TestCheckGoChildOfTheTestshardHelperStillTakesItsOwnJobBudget(t *testing.T) {
	setLongLegBudget(t)
	out, errs, err := run(t, []string{"CHECK_GO_JOB_CAP_MIN=20", "CHECK_GO_JOB_START=" + strconv.FormatInt(time.Now().Unix(), 10)}, "bash", "ci/check_go.sh", "job-timeout", "25")
	if err != nil || strings.Contains(out, "1500s") || !strings.HasSuffix(strings.TrimSpace(out), "s") {
		t.Errorf("the explicit budget was not applied: err=%v stdout=%q stderr=%q", err, out, errs)
	}
}
