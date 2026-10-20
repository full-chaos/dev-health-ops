package cigates_test

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// CHAOS-10300: the race weights are one file per package (ci/go_race_weights.d/<package with / as __>.tsv, one row each),
// so a pull request that adds a package adds a file and never edits a line another pull request edits. These tests run
// `ci/check_go.sh race-weights-rows` against a temp copy of the directory (GO_RACE_WEIGHTS_DIR) with one planted defect,
// and pin the go.yml path filters that make a new ci table need no go.yml edit.

const weightsDir = "ci/go_race_weights.d"

func copyWeights(t *testing.T, root string) string {
	t.Helper()
	dst := t.TempDir()
	entries, err := os.ReadDir(filepath.Join(root, weightsDir))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		b, err := os.ReadFile(filepath.Join(root, weightsDir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dst, e.Name()), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dst
}

func runWeightsCheck(t *testing.T, root, dir string) cmdResult {
	t.Helper()
	return cmdSpec{dir: root, env: inheritedEnv(nil, "GO_RACE_WEIGHTS_DIR="+dir)}.run("bash", "ci/check_go.sh", "race-weights-rows")
}

func TestRaceWeightsFiles(t *testing.T) {
	root := rootOf(t)
	victim := "internal/httpguard"
	victimFile := "internal__httpguard.tsv"
	victimRow, err := os.ReadFile(filepath.Join(root, weightsDir, victimFile))
	if err != nil {
		t.Fatalf("%s is the row this test plants against: %v", victimFile, err)
	}
	victimWeight := strings.Fields(string(victimRow))[1]

	write := func(dir, name, body string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	cases := []struct {
		name   string
		mutate func(dir string)
		fail   bool
		want   []string
	}{
		{"the real directory is accepted", func(string) {}, false, []string{"race-weights-rows:"}},
		{"a package with no file", func(d string) { _ = os.Remove(filepath.Join(d, victimFile)) }, true, []string{"NO row", victim}},
		{"a file of a package that is gone", func(d string) { write(d, "internal__zz-gone-package.tsv", "internal/zz-gone-package\t1\n") }, true, []string{"no longer exist", "internal/zz-gone-package"}},
		{"the same package in two files names both weights", func(d string) { write(d, "internal__httpguard__again.tsv", victim+"\t999\n") }, true, []string{"weighs a package twice", victim, victimWeight, "999"}},
		{"a file whose name is not its package", func(d string) { write(d, "internal__other.tsv", "internal/zz-other\t1\n") }, true, []string{"internal__other.tsv", "internal__zz-other.tsv"}},
		{"a file with two rows", func(d string) { write(d, victimFile, string(victimRow)+"cmd/dho\t5\n") }, true, []string{victimFile, "exactly one row"}},
		{"a row with no weight", func(d string) { write(d, victimFile, victim+"\n") }, true, []string{victimFile, "weight"}},
		{"a comment and a blank line in a file are not rows", func(d string) { write(d, victimFile, "# note\n\n"+string(victimRow)) }, false, []string{"race-weights-rows:"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := copyWeights(t, root)
			c.mutate(dir)
			got := runWeightsCheck(t, root, dir)
			if c.fail && got.code == 0 {
				t.Fatalf("accepted:\n%s%s", got.stdout, got.stderr)
			}
			if !c.fail && got.code != 0 {
				t.Fatalf("rejected (exit %d):\n%s%s", got.code, got.stdout, got.stderr)
			}
			for _, w := range c.want {
				if !strings.Contains(got.stdout+got.stderr, w) {
					t.Errorf("output lacks %q:\n%s%s", w, got.stdout, got.stderr)
				}
			}
		})
	}
}

// TestRaceShardPlanIgnoresFileOrder: the shard awk reads the rows as a lookup, so the same rows in any order give the
// same partition. This is what makes concatenating a directory safe.
func TestRaceShardPlanIgnoresFileOrder(t *testing.T) {
	root := rootOf(t)
	entries, err := os.ReadDir(filepath.Join(root, weightsDir))
	if err != nil {
		t.Fatal(err)
	}
	var rows []string
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".tsv") {
			continue
		}
		rows = append(rows, strings.TrimRight(readText(t, filepath.Join(root, weightsDir, e.Name())), "\n"))
	}
	if len(rows) < 300 {
		t.Fatalf("only %d weight files under %s", len(rows), weightsDir)
	}
	sort.Strings(rows)
	forward := filepath.Join(t.TempDir(), "forward.tsv")
	backward := filepath.Join(t.TempDir(), "backward.tsv")
	rev := make([]string, len(rows))
	for i, r := range rows {
		rev[len(rows)-1-i] = r
	}
	if err := os.WriteFile(forward, []byte(strings.Join(rows, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(backward, []byte(strings.Join(rev, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	list := cmdSpec{dir: root, env: inheritedEnv(nil)}.run("go", "list", "./...")
	if list.code != 0 {
		t.Fatalf("go list: %s", list.stderr)
	}
	for _, shard := range []string{"1", "2", "3"} {
		var out [2]string
		for i, w := range []string{forward, backward} {
			r := cmdSpec{dir: root, env: inheritedEnv(nil), stdin: []byte(list.stdout)}.run("awk", "-v", "mod=github.com/full-chaos/dev-health-ops", "-v", "shard="+shard, "-v", "count=3", "-v", "weights="+w, "-f", "ci/go_race_shard.awk")
			if r.code != 0 || r.stdout == "" {
				t.Fatalf("shard %s: exit %d %q %s", shard, r.code, r.stdout, r.stderr)
			}
			out[i] = r.stdout
		}
		if out[0] != out[1] {
			t.Errorf("shard %s differs when the weight rows come in reverse order", shard)
		}
	}
}

// TestGoYmlPathFiltersCoverCiTablesByGlob: both `paths` lists of go.yml (push, pull_request) hold the ci globs, and no
// enumerated name of a file a glob already covers, so a new ci table or weight file never edits go.yml.
func TestGoYmlPathFiltersCoverCiTablesByGlob(t *testing.T) {
	root := rootOf(t)
	doc := loadWf(t, root, "go.yml")
	globs := []string{"ci/*.tsv", "ci/*.awk", "ci/*.d/**"}
	for _, event := range []string{"push", "pull_request"} {
		paths := map[string]bool{}
		for _, p := range asList(asMap(asMap(doc["on"])[event])["paths"]) {
			paths[asString(p)] = true
		}
		for _, g := range globs {
			if !paths[g] {
				t.Errorf("go.yml %s.paths lacks %q", event, g)
			}
		}
		for p := range paths {
			covered := (strings.HasPrefix(p, "ci/") && !strings.Contains(strings.TrimPrefix(p, "ci/"), "/") && (strings.HasSuffix(p, ".tsv") || strings.HasSuffix(p, ".awk"))) ||
				(strings.HasPrefix(p, "ci/") && strings.Contains(p, ".d/") && !strings.HasSuffix(p, "**"))
			if covered && !strings.ContainsAny(p, "*") {
				t.Errorf("go.yml %s.paths names %q, which a ci glob already covers; list globs only", event, p)
			}
		}
	}
}
