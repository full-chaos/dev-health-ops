package testshard_test

import (
	"strings"
	"testing"
)

func TestJobBudgetFreeEnvironDropsEveryDeadlineName(t *testing.T) {
	for _, name := range []string{"GITHUB_ACTIONS", "CHECK_GO_JOB_CAP_MIN", "CHECK_GO_JOB_START", "CHECK_GO_NOW_EPOCH", "CHECK_GO_NOW_EPOCH_FILE"} {
		t.Setenv(name, "x")
	}
	t.Setenv("CHECK_GO_KEEP_ME", "1")
	kept := false
	for _, kv := range jobBudgetFreeEnviron() {
		name, _, _ := strings.Cut(kv, "=")
		switch name {
		case "GITHUB_ACTIONS", "CHECK_GO_JOB_CAP_MIN", "CHECK_GO_JOB_START", "CHECK_GO_NOW_EPOCH", "CHECK_GO_NOW_EPOCH_FILE":
			t.Errorf("%s is still in the environment", name)
		case "CHECK_GO_KEEP_ME":
			kept = true
		}
	}
	if !kept {
		t.Error("an unrelated variable was dropped")
	}
}
