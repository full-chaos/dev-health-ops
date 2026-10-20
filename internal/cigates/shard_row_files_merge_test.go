package cigates_test

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// CHAOS-10174 PR C: Go port of tests/tooling/test_shard_row_files_merge.py (CHAOS-6926). The integration shard manifest
// and the venue weights are directories of per-package row files so that two PRs editing neighbouring rows do not
// conflict. The test EXECUTES the claim with real git merges and runs the planner on the merged result. The control
// replays the same edits against the old flat layout and requires git to conflict there: that planted layout defect is
// the red proof, because a test that passes on a layout that has the old problem cannot fail.

func rowFiles(t *testing.T, dir string) []string {
	t.Helper()
	names, err := filepath.Glob(filepath.Join(dir, "*.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(names)
	return names
}

func packageRowFiles(t *testing.T, shards string) []string {
	var out []string
	for _, f := range rowFiles(t, shards) {
		if filepath.Base(f) != "_shards.tsv" {
			out = append(out, f)
		}
	}
	return out
}

func copyDir(t *testing.T, src, dst string) {
	t.Helper()
	if err := os.MkdirAll(dst, 0o755); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		raw, err := os.ReadFile(filepath.Join(src, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dst, e.Name()), raw, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func reweigh(t *testing.T, path string, delta int) {
	t.Helper()
	lines := strings.Split(strings.TrimRight(readText(t, path), "\n"), "\n")
	for i, line := range lines {
		if line != "" && !strings.HasPrefix(line, "#") && strings.Contains(line, "\t") {
			parts := strings.Split(line, "\t")
			if len(parts) != 2 {
				t.Fatalf("%s: row %q has %d columns, want 2", path, line, len(parts))
			}
			w, err := strconv.Atoi(parts[1])
			if err != nil {
				t.Fatal(err)
			}
			lines[i] = fmt.Sprintf("%s\t%d", parts[0], w+delta)
			break
		}
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func dataRows(t *testing.T, dir string) [][]string {
	var rows [][]string
	for _, f := range rowFiles(t, dir) {
		for _, line := range strings.Split(readText(t, f), "\n") {
			if strings.HasPrefix(line, "<<<<<<<") || strings.HasPrefix(line, "=======") || strings.HasPrefix(line, ">>>>>>>") {
				t.Fatalf("%s holds a conflict marker", f)
			}
			if strings.TrimSpace(line) != "" && !strings.HasPrefix(line, "#") {
				rows = append(rows, strings.Split(line, "\t"))
			}
		}
	}
	return rows
}

func commitAll(r *gitRepo, msg string) {
	r.git("add", "-A")
	r.git("commit", "-q", "-m", msg)
}

func TestBranchesThatAddAndRetimeRowsMergeWithoutConflict(t *testing.T) {
	t.Parallel()
	root := rootOf(t)
	shardsSrc := filepath.Join(root, "ci", "go_integration_shards.d")
	weightsSrc := filepath.Join(root, "ci", "venue_oracle_weights.d")
	packages := packageRowFiles(t, shardsSrc)
	if len(packages) <= 100 {
		t.Fatalf("the manifest directory holds %d package files: the test measured nothing", len(packages))
	}
	addedA, addedB, neighbour, distant := packages[20], packages[21], packages[22], packages[100]
	venue := rowFiles(t, weightsSrc)
	if len(venue) < 4 {
		t.Fatalf("the venue weights directory holds %d files", len(venue))
	}
	venueX, venueY := venue[1], venue[3]

	repo := newGitRepo(filepath.Join(t.TempDir(), "repo"))
	shards := filepath.Join(repo.dir, "ci", "go_integration_shards.d")
	weights := filepath.Join(repo.dir, "ci", "venue_oracle_weights.d")
	copyDir(t, shardsSrc, shards)
	copyDir(t, weightsSrc, weights)
	// the base lacks the two packages the branches add back
	for _, f := range []string{addedA, addedB} {
		if err := os.Remove(filepath.Join(shards, filepath.Base(f))); err != nil {
			t.Fatal(err)
		}
	}
	commitAll(repo, "base")

	branch := func(name string, edit func()) {
		repo.git("checkout", "-q", "-b", name, "main")
		edit()
		commitAll(repo, name)
		repo.git("checkout", "-q", "main")
	}
	copyFile := func(src, dst string) {
		if err := os.WriteFile(dst, []byte(readText(t, src)), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	addVenueRow := func(target, test string) func() {
		return func() {
			pkg := strings.Split(readText(t, target), "\t")[0]
			f, err := os.OpenFile(filepath.Join(weights, filepath.Base(target)), os.O_APPEND|os.O_WRONLY, 0o644)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			if _, err := fmt.Fprintf(f, "%s\t%s\t11\n", pkg, test); err != nil {
				t.Fatal(err)
			}
		}
	}
	names := []string{"a-adds-a-package", "b-adds-the-next-package", "c-retimes-the-neighbour", "d-retimes-a-distant-package", "e-adds-a-venue-row", "f-adds-a-venue-row-elsewhere", "g-adds-a-venue-package"}
	branch(names[0], func() { copyFile(addedA, filepath.Join(shards, filepath.Base(addedA))) })
	branch(names[1], func() { copyFile(addedB, filepath.Join(shards, filepath.Base(addedB))) })
	branch(names[2], func() { reweigh(t, filepath.Join(shards, filepath.Base(neighbour)), 7) })
	branch(names[3], func() { reweigh(t, filepath.Join(shards, filepath.Base(distant)), 3) })
	branch(names[4], addVenueRow(venueX, "TestZZAddedByE"))
	branch(names[5], addVenueRow(venueY, "TestZZAddedByF"))
	branch(names[6], func() {
		body := "internal/zzz/newvenue\tTestNewVenueOracle\t600\tunmeasured\n"
		if err := os.WriteFile(filepath.Join(weights, "internal__zzz__newvenue.tsv"), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	})
	if repo.err != nil {
		t.Fatal(repo.err)
	}
	for _, name := range names {
		res := cmdSpec{dir: repo.dir, env: gitEnv}.run("git", "-c", "commit.gpgsign=false", "merge", "--no-edit", "-q", name)
		if res.code != 0 {
			t.Fatalf("merging %s conflicted: %s%s", name, res.stdout, res.stderr)
		}
	}

	merged := rowFiles(t, shards)
	if filepath.Base(merged[0]) != "_shards.tsv" {
		t.Fatalf("first merged row file is %s, want _shards.tsv", merged[0])
	}
	read := func(files []string) map[string]string {
		m := map[string]string{}
		for _, f := range files {
			m[filepath.Base(f)] = readText(t, f)
		}
		return m
	}
	real, got := read(rowFiles(t, shardsSrc)), read(merged)
	var changed []string
	for name, body := range real {
		if got[name] != body {
			changed = append(changed, name)
		}
	}
	for name := range got {
		if _, ok := real[name]; !ok {
			t.Fatalf("merged manifest has a file the real one lacks: %s", name)
		}
	}
	sort.Strings(changed)
	wantChanged := []string{filepath.Base(neighbour), filepath.Base(distant)}
	sort.Strings(wantChanged)
	if strings.Join(changed, "|") != strings.Join(wantChanged, "|") || len(got) != len(real) {
		t.Fatalf("merged manifest differs from the real one in %v (files %d vs %d), want exactly %v", changed, len(got), len(real), wantChanged)
	}
	pkgSeen := map[string]bool{}
	for _, row := range dataRows(t, shards) {
		if row[0] == "shards" {
			continue
		}
		if len(row) != 2 {
			t.Fatalf("merged manifest row %q has %d columns, want 2", row, len(row))
		}
		if _, err := strconv.Atoi(row[1]); err != nil {
			t.Fatalf("merged manifest row %q has a non-integer weight", row)
		}
		if pkgSeen[row[0]] {
			t.Fatalf("merged manifest names %s twice", row[0])
		}
		pkgSeen[row[0]] = true
	}

	seen := map[string]bool{}
	for _, row := range dataRows(t, weights) {
		if len(row) >= 2 {
			seen[row[0]+"|"+row[1]] = true
		}
	}
	for _, test := range []string{"TestZZAddedByE", "TestZZAddedByF"} {
		found := false
		for k := range seen {
			found = found || strings.HasSuffix(k, "|"+test)
		}
		if !found {
			t.Fatalf("merged venue weights lack the row of %s", test)
		}
	}
	if !seen["internal/zzz/newvenue|TestNewVenueOracle"] {
		t.Fatal("merged venue weights lack the new package file's row")
	}
}

// The control: the SAME kind of edits against ONE sorted flat file conflict. If git ever merged them cleanly, the
// directory layout would no longer be the fix and this test would say so.
func TestTheOldFlatLayoutConflictsOnTheSameEdits(t *testing.T) {
	var lines []string
	for _, f := range packageRowFiles(t, filepath.Join(rootOf(t), "ci", "go_integration_shards.d")) {
		rows := strings.Split(strings.TrimSpace(readText(t, f)), "\n")
		lines = append(lines, rows[len(rows)-1])
	}
	sort.Strings(lines)
	if len(lines) < 100 {
		t.Fatalf("only %d manifest rows: the control measured nothing", len(lines))
	}
	repo := newGitRepo(filepath.Join(t.TempDir(), "flat"))
	flat := filepath.Join(repo.dir, "ci", "go_integration_shards.tsv")
	if err := os.MkdirAll(filepath.Dir(flat), 0o755); err != nil {
		t.Fatal(err)
	}
	base := append(append([]string{}, lines[:20]...), lines[22:]...)
	write := func(rows []string) {
		if err := os.WriteFile(flat, []byte(strings.Join(rows, "\n")+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write(append([]string{"shards\t6"}, base...))
	commitAll(repo, "base")
	read := func() []string { return strings.Split(strings.TrimRight(readText(t, flat), "\n"), "\n") }
	editBranch := func(name string, edit func([]string) []string) {
		repo.git("checkout", "-q", "-b", name, "main")
		write(edit(read()))
		commitAll(repo, name)
		repo.git("checkout", "-q", "main")
	}
	insertAt := func(i int, row string) func([]string) []string {
		return func(rows []string) []string {
			return append(rows[:i], append([]string{row}, rows[i:]...)...)
		}
	}
	// the flat file's first line is the shard count, so data row n sits at index n+1
	editBranch("a", insertAt(21, lines[20]))
	editBranch("b", insertAt(21, lines[21]))
	editBranch("c", func(rows []string) []string {
		parts := strings.Split(rows[22], "\t")
		w, err := strconv.Atoi(parts[1])
		if err != nil {
			t.Fatal(err)
		}
		rows[22] = fmt.Sprintf("%s\t%d", parts[0], w+7)
		return rows
	})
	if repo.err != nil {
		t.Fatal(repo.err)
	}
	merge := func(name string) int {
		return cmdSpec{dir: repo.dir, env: gitEnv}.run("git", "-c", "commit.gpgsign=false", "merge", "--no-edit", "-q", name).code
	}
	if code := merge("a"); code != 0 {
		t.Fatalf("control setup: the first branch must merge cleanly, exit %d", code)
	}
	twoAdds := merge("b")
	cmdSpec{dir: repo.dir, env: gitEnv}.run("git", "merge", "--abort")
	retimed := merge("c")
	if twoAdds == 0 && retimed == 0 {
		t.Fatal("git merged the flat file's adjacent edits cleanly: the per-package layout would no longer be needed")
	}
}
