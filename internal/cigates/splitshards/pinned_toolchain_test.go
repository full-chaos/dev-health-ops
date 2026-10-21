package splitshards_test

import (
	"path/filepath"
	"regexp"
	"testing"
)

// pinnedGoToolchain is the Go toolchain ci/check_go.sh pins (GO_TOOLCHAIN="go1.x.y"): the one source of truth, so a toolchain
// bump edits check_go.sh and no test.
func pinnedGoToolchain(t *testing.T) string {
	t.Helper()
	m := regexp.MustCompile(`(?m)^GO_TOOLCHAIN="(go[0-9][0-9.]*)"$`).FindStringSubmatch(readText(t, filepath.Join(rootOf(t), "ci", "check_go.sh")))
	if m == nil {
		t.Fatal(`ci/check_go.sh no longer declares GO_TOOLCHAIN="go1.x.y"`)
	}
	return m[1]
}
