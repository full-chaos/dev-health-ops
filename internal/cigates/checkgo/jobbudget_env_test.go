package checkgo_test

import (
	"strconv"
	"strings"
	"testing"
	"time"
)

// Not parallel: t.Setenv. soloExec must not pass the leg's job budget (44 minutes into a 45 minute cap) to check_go.sh.
func TestSoloExecChildDoesNotInheritTheLegJobBudget(t *testing.T) {
	t.Setenv("GITHUB_ACTIONS", "true")
	t.Setenv("CHECK_GO_JOB_CAP_MIN", "45")
	t.Setenv("CHECK_GO_JOB_START", strconv.FormatInt(time.Now().Add(-44*time.Minute).Unix(), 10))
	out, errs, err := soloExec(t, repoRoot(t), nil, "", "bash", "ci/check_go.sh", "job-timeout", "25")
	if err != nil || !strings.Contains(out, "1500s") {
		t.Errorf("the child inherited the leg's job budget: err=%v stdout=%q stderr=%q", err, out, errs)
	}
}
