//go:build !race

package affectedmatrix_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// This test runs real ci scripts and child processes; -race adds nothing to it and it sinks the package under the race timeout. ci/race_excluded_tests.tsv runs it without -race.

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
	selFile := filepath.Join(work, "one_package.txt")
	if err := os.WriteFile(selFile, []byte("AFFECTED 1\ninternal/admincli\n"), 0o644); err != nil {
		t.Fatal(err)
	}
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
