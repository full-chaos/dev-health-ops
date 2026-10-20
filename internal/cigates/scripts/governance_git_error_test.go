package scripts_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cigates"
)

func TestGovernanceGitDiffErrorExitsTwo(t *testing.T) {
	var out bytes.Buffer
	if rc := cigates.Governance([]string{"--base-sha", "nonexistent1", "--head-sha", "nonexistent2"}, &out); rc != 2 || !strings.Contains(out.String(), "ERROR: Unable to compute changed files from git diff.") {
		t.Fatalf("rc=%d out=%q", rc, out.String())
	}
}
