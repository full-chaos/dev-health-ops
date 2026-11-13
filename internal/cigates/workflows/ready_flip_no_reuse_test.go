package workflows_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The ready-flip reuse judge is retired: on a draft->ready flip every workflow that reads the draft state runs its full
// job set again. Measured on the 12 flips after the judge shipped, the reusable legs saved about 15 s against 45-190 s of
// judge jobs per flip, so a flip never came out ahead.
var readyReuseMarkers = []string{"ready-reuse", "ready_reuse", "READY_REUSED", "GATE_REUSE_"}

const readyReuseScript = "ci/ready_reuse.sh"

func readyFlipDefects(t *testing.T, root string, files []string) []string {
	t.Helper()
	var bad []string
	if _, err := os.Stat(filepath.Join(root, readyReuseScript)); err == nil {
		bad = append(bad, readyReuseScript+" exists: a ready flip must not judge reuse")
	}
	for _, f := range files {
		text := readFile(t, filepath.Join(root, f))
		for _, m := range readyReuseMarkers {
			if strings.Contains(text, m) {
				bad = append(bad, fmt.Sprintf("%s names %q: a ready flip must run every leg with no reuse step", f, m))
			}
		}
		if !strings.HasPrefix(f, ".github/workflows/") {
			continue
		}
		doc := parseYAML(t, text, f)
		for name, raw := range asMap(doc["jobs"]) {
			job := asMap(raw)
			if strings.Contains(asString(job["if"]), "ready_for_review") {
				bad = append(bad, fmt.Sprintf("%s job %s: its condition reads ready_for_review, so a ready flip can skip it", f, name))
			}
		}
	}
	return bad
}

func readyFlipFiles(t *testing.T, root string) []string {
	return append(allWorkflowFiles(t, root), "ci/aggregate_gate_results.sh")
}

func TestReadyFlipRunsEveryLegWithNoReuseStep(t *testing.T) {
	root := rootOf(t)
	if d := readyFlipDefects(t, root, readyFlipFiles(t, root)); len(d) > 0 {
		t.Fatalf("%s", strings.Join(d, "\n"))
	}
}

func TestReadyFlipRefusesEachReuseShape(t *testing.T) {
	root := rootOf(t)
	files := readyFlipFiles(t, root)
	const goYml = ".github/workflows/go.yml"
	cases := []struct {
		name string
		mut  mutation
		want string
	}{
		{"a judge job comes back", mutation{goYml, "\njobs:\n", "\njobs:\n  ready-reuse:\n    runs-on: ubuntu-latest\n    steps:\n      - run: true\n"}, `names "ready-reuse"`},
		{"a leg skips on a ready flip", mutation{goYml, "\njobs:\n", "\njobs:\n  flip-skip:\n    if: github.event.action != 'ready_for_review'\n    runs-on: ubuntu-latest\n    steps:\n      - run: true\n"}, "job flip-skip: its condition reads ready_for_review"},
		{"a gate reads a reuse list", mutation{"ci/aggregate_gate_results.sh", "#!/usr/bin/env bash\n", "#!/usr/bin/env bash\n: \"${READY_REUSED:-}\"\n"}, `names "READY_REUSED"`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			planted := plantTree(t, root, files, c.mut)
			d := readyFlipDefects(t, planted, files)
			if !containsAll(d, c.want) {
				t.Fatalf("planted defect not refused; want %q in:\n%s", c.want, strings.Join(d, "\n"))
			}
		})
	}
	t.Run("the judge script comes back", func(t *testing.T) {
		planted := plantTree(t, root, files)
		if err := os.MkdirAll(filepath.Join(planted, "ci"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(planted, readyReuseScript), []byte("#!/usr/bin/env bash\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if d := readyFlipDefects(t, planted, files); !containsAll(d, readyReuseScript+" exists") {
			t.Fatalf("planted script not refused:\n%s", strings.Join(d, "\n"))
		}
	})
}
