//go:build !race

package affectedmodule_test

import (
	"fmt"
	"strings"
	"testing"
)

// This test runs real ci scripts and child processes; -race adds nothing to it and it sinks the package under the race timeout. ci/race_excluded_tests.tsv runs it without -race.

func TestGoAffectedIntegrationRealModule(t *testing.T) {
	t.Parallel()
	root := rootOf(t)
	t.Run("one_package_change_is_a_strict_subset", func(t *testing.T) {
		lines, total, _ := realSelection(t, root, []byte("internal/admincli/x.go\x00"))
		var count int
		if _, err := fmt.Sscanf(lines[0], "AFFECTED %d", &count); err != nil {
			t.Fatalf("first line %q: %v", lines[0], err)
		}
		found := false
		for _, l := range lines[1:] {
			found = found || l == "internal/admincli"
		}
		if !found || count < 1 || count >= total/2 {
			t.Fatalf("selection %q count=%d total=%d: want internal/admincli and a strict subset (< total/2)", lines, count, total)
		}
	})
	t.Run("shared_change_is_full", func(t *testing.T) {
		lines, _, _ := realSelection(t, root, []byte("ci/check_go.sh\x00"))
		if !strings.HasPrefix(lines[0], "FULL") {
			t.Fatalf("a shared change selected %q", lines)
		}
	})
}
