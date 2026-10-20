//go:build !race

package affected_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// These tests run real ci scripts and child processes; -race adds nothing to them and they sink the package under the race timeout. ci/race_excluded_tests.tsv runs each one without -race.

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

func TestGoAffectedIntegrationPlannerRejectsMalformedSelections(t *testing.T) {
	t.Parallel()
	script := scriptPath(t, "ci/check_go.sh")
	scenarios := affectedPlannerRejectScenarios()
	assertAllPass(t, script, scenarios)
	assertPlantsCaught(t, script, scenarios, []plant{
		pc("unplanned package accepted", `|| die "affected selection names '${line}', which is not a planned integration package"`, `|| true`,
			"planner_rejects_an_unplanned_package", "accepted"),
		pc("count mismatch accepted", `|| die "affected selection declares ${declared} package(s) but lists ${count}"`, `|| true`,
			"planner_rejects_a_count_mismatch", "accepted"),
		pc("duplicate key accepted", `|| die "affected selection lists '${line}' more than once"`, `|| true`,
			"planner_rejects_a_duplicate_key", "accepted"),
		pc("unknown first line accepted", `*) die "INTEGRATION_AFFECTED_FILE ${file} has an unknown first line '${first}'" ;;`, `*) return 0 ;;`,
			"planner_rejects_an_unknown_first_line", "accepted"),
	})
}

func TestGoAffectedIntegrationPlannerMatrixShrinksWithASelection(t *testing.T) {
	t.Parallel()
	root := rootOf(t)
	script := filepath.Join(root, "ci", "check_go.sh")
	work := t.TempDir()
	legs := func(selection string) (int, map[string]string) {
		t.Helper()
		sub := t.TempDir()
		res, outputs, err := plan(script, sub, selection)
		if err != nil || res.code != 0 {
			t.Fatalf("plan(%q): code=%d err=%v %s", selection, res.code, err, res.stderr)
		}
		var n int
		if _, err := fmt.Sscanf(outputs["legs"], "%d", &n); err != nil {
			t.Fatalf("legs output %q: %v", outputs["legs"], err)
		}
		return n, outputs
	}
	full, _ := legs("")
	_, _, selFile := realSelection(t, root, []byte("internal/admincli/x.go\x00"))
	few, fewOut := legs(selFile)
	if few < 1 || few >= full {
		t.Fatalf("a one-package selection planned %d legs, the full plan %d: the matrix did not shrink", few, full)
	}
	var m struct {
		Include []json.RawMessage `json:"include"`
	}
	if err := json.Unmarshal([]byte(fewOut["matrix"]), &m); err != nil || len(m.Include) == 0 {
		t.Fatalf("matrix %q: %v", fewOut["matrix"], err)
	}
	fullSel := filepath.Join(work, "full_sel.txt")
	if err := os.WriteFile(fullSel, []byte("FULL shared change: ci/x\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if same, _ := legs(fullSel); same != full {
		t.Fatalf("a FULL selection planned %d legs, want the full plan %d", same, full)
	}
	noneSel := filepath.Join(work, "none.txt")
	if err := os.WriteFile(noneSel, []byte("AFFECTED 0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	none, noneOut := legs(noneSel)
	if none != 0 || strings.ReplaceAll(noneOut["matrix"], " ", "") != `{"include":[]}` {
		t.Fatalf("AFFECTED 0 planned %d legs, matrix %q: want 0 and an empty include", none, noneOut["matrix"])
	}
}
