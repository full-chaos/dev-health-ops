package checkgo_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// CHAOS-10300: the race weights are one file per package (ci/go_race_weights.d/<race_weights_file_name of the package>.tsv, one row each),
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
		{"a package with no file", func(d string) { _ = os.Remove(filepath.Join(d, victimFile)) }, true, []string{"NO row", victim}},
		{"a file of a package that is gone", func(d string) { write(d, "internal__zz-gone-package.tsv", "internal/zz-gone-package\t1\n") }, true, []string{"no longer exist", "internal/zz-gone-package"}},
		{"the same package in two files names both weights", func(d string) { write(d, "internal__httpguard__again.tsv", victim+"\t999\n") }, true, []string{"weighs a package twice", victim, victimWeight, "999"}},
		{"a file whose name is not its package", func(d string) { write(d, "internal__other.tsv", "internal/zz-other\t1\n") }, true, []string{"internal__other.tsv", "internal__zz-other.tsv"}},
		{"a file with two rows", func(d string) { write(d, victimFile, string(victimRow)+"cmd/dho\t5\n") }, true, []string{victimFile, "exactly one row"}},
		{"a row with no weight", func(d string) { write(d, victimFile, victim+"\n") }, true, []string{victimFile, "weight"}},
		{"an empty file", func(d string) { write(d, victimFile, "") }, true, []string{victimFile, "exactly one row"}},
		{"a file of comments and blank lines only", func(d string) { write(d, victimFile, "# note\n\n \t\n") }, true, []string{victimFile, "exactly one row"}},
		{"an empty weight", func(d string) { write(d, victimFile, victim+"\t\n") }, true, []string{victimFile, "the weight '' is not a whole number"}},
		{"a decimal weight", func(d string) { write(d, victimFile, victim+"\t3.5\n") }, true, []string{victimFile, "the weight '3.5' is not a whole number"}},
		{"a negative weight", func(d string) { write(d, victimFile, victim+"\t-3\n") }, true, []string{victimFile, "the weight '-3' is not a whole number"}},
		{"a weight in Arabic-Indic digits", func(d string) { write(d, victimFile, victim+"\t\u0663\n") }, true, []string{victimFile, "is not ASCII text"}},
		{"a weight with a no-break space", func(d string) { write(d, victimFile, victim+"\t3\u00a0\n") }, true, []string{victimFile, "is not ASCII text"}},
		{"a line of an em space only", func(d string) { write(d, victimFile, "\u2003\n"+string(victimRow)) }, true, []string{victimFile, "is not ASCII text"}},
		{"a line of a no-break space only", func(d string) { write(d, victimFile, "\u00a0\n"+string(victimRow)) }, true, []string{victimFile, "is not ASCII text"}},
		{"a line of an ideographic space only", func(d string) { write(d, victimFile, "\u3000\n"+string(victimRow)) }, true, []string{victimFile, "is not ASCII text"}},
		{"a comment that is not ASCII", func(d string) { write(d, victimFile, "# caf\u00e9\n"+string(victimRow)) }, true, []string{victimFile, "is not ASCII text"}},
		{"a row that ends in a carriage return", func(d string) { write(d, victimFile, victim+"\t3\r\n") }, true, []string{victimFile, "is not a whole number"}},
		{"a NUL byte", func(d string) { write(d, victimFile, victim+"\t3\x00\n") }, true, []string{victimFile, "is not ASCII text"}},
		{"a byte that is not UTF-8", func(d string) { write(d, victimFile, victim+"\t3\xff\n") }, true, []string{victimFile, "is not ASCII text"}},
		{"a UTF-8 sequence cut at the end of the file", func(d string) { write(d, victimFile, victim+"\t3\xc3") }, true, []string{victimFile, "is not ASCII text"}},
		{"an overlong UTF-8 sequence", func(d string) { write(d, victimFile, victim+"\t3\xc0\xaf\n") }, true, []string{victimFile, "is not ASCII text"}},
		{"the real directory with a comment and a blank line in a file is accepted", func(d string) { write(d, victimFile, "# note\n\n"+string(victimRow)) }, false, []string{"race-weights-rows:"}},
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

// mergedWeights writes every row of ci/go_race_weights.d to one temp file, the one weights stream the shard awk reads.
func mergedWeights(t *testing.T, root string) string {
	t.Helper()
	var b strings.Builder
	for _, f := range rowFiles(t, filepath.Join(root, weightsDir)) {
		b.WriteString(strings.TrimRight(readText(t, f), "\n") + "\n")
	}
	path := filepath.Join(t.TempDir(), "weights.tsv")
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

const weightsListStandInGo = `#!/usr/bin/env bash
if [ "$1" = "list" ] && [ "$2" = "-m" ]; then printf '%%s\n' "github.com/full-chaos/dev-health-ops"; exit 0; fi
if [ "$1" = "list" ]; then
  printf '%%s' "${FAKE_GO_PACKAGES}"
  exit 0
fi
exec "%s" "$@"
`

// runWeightsCheckForPackages runs race-weights-rows against a directory holding exactly the given files, with a stand-in
// `go list` that reports exactly the given packages (import paths relative to the module root; "." is the root package).
func runWeightsCheckForPackages(t *testing.T, packages []string, files map[string]string) cmdResult {
	t.Helper()
	root := rootOf(t)
	realGo, err := exec.LookPath("go")
	if err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "go"), []byte(fmt.Sprintf(weightsListStandInGo, realGo)), 0o755); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	solo := filepath.Join(t.TempDir(), "solo.tsv")
	if err := os.WriteFile(solo, []byte("# no solo packages\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var listed strings.Builder
	for _, p := range packages {
		if p == "." {
			listed.WriteString("github.com/full-chaos/dev-health-ops\n")
		} else {
			listed.WriteString("github.com/full-chaos/dev-health-ops/" + p + "\n")
		}
	}
	env := inheritedEnv(nil, "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"), "FAKE_GO_PACKAGES="+listed.String(), "GO_RACE_WEIGHTS_DIR="+dir, "GO_RACE_SOLO="+solo)
	return cmdSpec{dir: root, env: env}.run("bash", "ci/check_go.sh", "race-weights-rows")
}

// TestRaceWeightsFileNamesAreOneToOne: the file name of a package is injective, so two packages whose paths differ
// only in `_` versus `/` (zzreview__collision, zzreview/collision) each have their own file, and the root package has one
// that a `*.tsv` glob sees.
func TestRaceWeightsFileNamesAreOneToOne(t *testing.T) {
	cases := []struct {
		name     string
		packages []string
		files    map[string]string
		fail     bool
		want     string
	}{
		{"two paths that differ only in _ versus /", []string{"zzreview__collision", "zzreview/collision"},
			map[string]string{"zzreview_u_ucollision.tsv": "zzreview__collision\t1\n", "zzreview__collision.tsv": "zzreview/collision\t2\n"}, false, "2 row(s) match"},
		{"the root package", []string{"."}, map[string]string{"_root.tsv": ".\t1\n"}, false, "1 row(s) match"},
		{"the root package under a name a glob skips", []string{"."}, map[string]string{".tsv": ".\t1\n"}, true, "_root.tsv"},
		{"a package under the name of another encoding", []string{"zzreview/collision"}, map[string]string{"zzreview_collision.tsv": "zzreview/collision\t2\n"}, true, "zzreview__collision.tsv"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := runWeightsCheckForPackages(t, c.packages, c.files)
			out := got.stdout + got.stderr
			if c.fail && got.code == 0 {
				t.Fatalf("accepted:\n%s", out)
			}
			if !c.fail && got.code != 0 {
				t.Fatalf("rejected (exit %d):\n%s", got.code, out)
			}
			if !strings.Contains(out, c.want) {
				t.Errorf("output lacks %q:\n%s", c.want, out)
			}
		})
	}
}

// TestRaceWeightsRowHasExactlyTwoColumns: package<TAB>seconds, nothing after the weight.
func TestRaceWeightsRowHasExactlyTwoColumns(t *testing.T) {
	for _, body := range []string{"internal/zzcols\t3\textra\n", "internal/zzcols\t3\t\n"} {
		got := runWeightsCheckForPackages(t, []string{"internal/zzcols"}, map[string]string{"internal__zzcols.tsv": body})
		out := got.stdout + got.stderr
		if got.code == 0 {
			t.Fatalf("row %q accepted:\n%s", body, out)
		}
		if !strings.Contains(out, "exactly two") {
			t.Errorf("row %q: output lacks the two-column message:\n%s", body, out)
		}
	}
}

// A line of Unicode white space is refused with one message under every awk on PATH (gawk, mawk) and in the C, C.UTF-8 and
// en_US.UTF-8 locales: the check reads bytes, so no awk or locale decides what is blank. An awk that is not installed is logged
// and left out; at least one must run.
func TestRaceWeightsNonASCIIWhiteSpaceIsRefusedUnderEveryAwkAndLocale(t *testing.T) {
	root := rootOf(t)
	victimFile := "internal__httpguard.tsv"
	victimRow, err := os.ReadFile(filepath.Join(root, weightsDir, victimFile))
	if err != nil {
		t.Fatal(err)
	}
	const want = "ERROR: " + "internal__httpguard.tsv is not ASCII text: it holds a NUL byte or a byte above 0x7f (race weight files hold one package<TAB>seconds row, in ASCII)\n"
	ran := 0
	for _, awk := range []string{"gawk", "mawk"} {
		path, err := exec.LookPath(awk)
		if err != nil {
			t.Logf("%s is not installed: left out", awk)
			continue
		}
		bin := t.TempDir()
		if err := os.Symlink(path, filepath.Join(bin, "awk")); err != nil {
			t.Fatal(err)
		}
		for _, locale := range []string{"C", "C.UTF-8", "en_US.UTF-8"} {
			for _, line := range []string{" ", " ", "　"} {
				dir := copyWeights(t, root)
				if err := os.WriteFile(filepath.Join(dir, victimFile), append([]byte(line+"\n"), victimRow...), 0o644); err != nil {
					t.Fatal(err)
				}
				got := cmdSpec{dir: root, env: inheritedEnv([]string{"PATH", "LC_ALL"}, "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"),
					"LC_ALL="+locale, "GO_RACE_WEIGHTS_DIR="+dir)}.run("bash", "ci/check_go.sh", "race-weights-rows")
				ran++
				if got.code != 2 || got.stderr != want {
					t.Errorf("%s, LC_ALL=%s, a line of %q: exit %d, stderr %q; want exit 2 and %q", awk, locale, line, got.code, got.stderr, want)
				}
			}
		}
	}
	if ran == 0 {
		t.Fatal("neither gawk nor mawk is installed")
	}
}

// With no tr on PATH the census is refused by name before it reads a file (the ASCII check needs tr).
func TestRaceWeightsCensusNeedsTrOnPath(t *testing.T) {
	root := rootOf(t)
	bin := t.TempDir()
	for _, dir := range filepath.SplitList(os.Getenv("PATH")) {
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if e.Name() == "tr" {
				continue
			}
			if _, err := os.Lstat(filepath.Join(bin, e.Name())); err == nil {
				continue
			}
			_ = os.Symlink(filepath.Join(dir, e.Name()), filepath.Join(bin, e.Name()))
		}
	}
	if _, err := os.Lstat(filepath.Join(bin, "bash")); err != nil {
		t.Fatal("no bash on PATH")
	}
	got := cmdSpec{dir: root, env: inheritedEnv([]string{"PATH"}, "PATH="+bin)}.run(filepath.Join(bin, "bash"), "ci/check_go.sh", "race-weights-rows")
	const want = "ERROR: race-weights-rows needs tr on PATH (the ASCII check of ci/go_race_weights.d)\n"
	if got.code != 2 || got.stderr != want {
		t.Fatalf("exit %d, stderr %q; want exit 2 and %q", got.code, got.stderr, want)
	}
}
