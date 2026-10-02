// Package liveoracleentries pins ci/live_python_oracles.d: one file per
// live-Python oracle, holding its command AND the proof markers that prove it ran.
//
// CHAOS-7656. Every freeze PR used to delete its blocks from the same lines of
// ci/check_go.sh, so each merge made the next freeze PR conflict. The entries are
// small files read in sorted order; a freeze PR deletes its file and never touches
// a shared line. A proof cannot be dropped without its command. These tests pin the
// reader (`check_go.sh live-python-oracles --list`): the resolved list changes by
// exactly the entry removed, an empty or missing directory fails loudly, and a
// malformed entry fails instead of being skipped.
package liveoracleentries

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "ci", "check_go.sh")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("no ci/check_go.sh above the test directory")
		}
		dir = parent
	}
}

type result struct {
	stdout, stderr string
	err            error
}

// list runs `check_go.sh live-python-oracles --list`, over directory when given.
func list(t *testing.T, root, directory string) result {
	t.Helper()
	command := exec.Command("bash", filepath.Join(root, "ci", "check_go.sh"), "live-python-oracles", "--list")
	command.Dir = root
	command.Env = os.Environ()
	if directory != "" {
		command.Env = append(command.Env, "LIVE_PYTHON_ORACLES_DIR="+directory)
	}
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	err := command.Run()
	return result{stdout.String(), stderr.String(), err}
}

func resolved(r result) []string {
	var out []string
	for _, line := range strings.Split(r.stdout, "\n") {
		if strings.HasPrefix(line, "RUN ") || strings.HasPrefix(line, "PROOF ") {
			out = append(out, line)
		}
	}
	return out
}

type entry struct {
	path   string
	fields [][2]string
}

func (e entry) get(key string) string {
	for _, field := range e.fields {
		if field[0] == key {
			return field[1]
		}
	}
	return ""
}

func (e entry) proofs() []string {
	var names []string
	for _, field := range e.fields {
		if field[0] == "proof" {
			names = append(names, strings.SplitN(field[1], "|", 2)[0])
		}
	}
	return names
}

func entries(t *testing.T, directory string) []entry {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(directory, "*.run"))
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(paths)
	var out []entry
	for _, path := range paths {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		e := entry{path: path}
		for _, line := range strings.Split(string(raw), "\n") {
			if line == "" {
				continue
			}
			key, value, _ := strings.Cut(line, "=")
			e.fields = append(e.fields, [2]string{key, value})
		}
		out = append(out, e)
	}
	return out
}

func copyEntries(t *testing.T, root string) string {
	t.Helper()
	target := t.TempDir()
	for _, e := range entries(t, filepath.Join(root, "ci", "live_python_oracles.d")) {
		raw, err := os.ReadFile(e.path)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(target, filepath.Base(e.path)), raw, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return target
}

func TestTheResolvedListHasEveryEntryInFileNameOrder(t *testing.T) {
	root := repoRoot(t)
	r := list(t, root, "")
	if r.err != nil {
		t.Fatalf("--list failed: %v\n%s", r.err, r.stderr)
	}
	var packages, proofs, gotPackages, gotProofs []string
	for _, e := range entries(t, filepath.Join(root, "ci", "live_python_oracles.d")) {
		packages = append(packages, e.get("package"))
		proofs = append(proofs, e.proofs()...)
	}
	if len(packages) == 0 {
		t.Fatal("no entry: the list would be vacuous")
	}
	for _, line := range resolved(r) {
		if strings.HasPrefix(line, "RUN ") {
			gotPackages = append(gotPackages, strings.SplitN(line, " package=", 2)[1])
		} else {
			gotProofs = append(gotProofs, strings.TrimPrefix(strings.Fields(line)[1], "name="))
		}
	}
	if strings.Join(gotPackages, ",") != strings.Join(packages, ",") || strings.Join(gotProofs, ",") != strings.Join(proofs, ",") {
		t.Fatalf("the resolved list is not the entries in file-name order:\n%v\n%v", gotPackages, packages)
	}
}

func TestEveryRunEntryNamesAPackageThatExistsAndAProof(t *testing.T) {
	root := repoRoot(t)
	seen := map[string]string{}
	for _, e := range entries(t, filepath.Join(root, "ci", "live_python_oracles.d")) {
		pkg := strings.TrimSuffix(strings.TrimPrefix(e.get("package"), "./"), "/...")
		if info, err := os.Stat(filepath.Join(root, pkg)); err != nil || !info.IsDir() {
			t.Errorf("%s runs %s, which does not exist: a stale entry runs nothing and its proof can never be written", filepath.Base(e.path), pkg)
		}
		if len(e.proofs()) == 0 {
			t.Errorf("%s declares no proof", filepath.Base(e.path))
		}
		for _, name := range e.proofs() {
			if other, dup := seen[name]; dup {
				t.Errorf("proof %s is declared by %s and %s", name, other, filepath.Base(e.path))
			}
			seen[name] = filepath.Base(e.path)
		}
	}
}

func TestRemovingOneEntryChangesTheListByExactlyThatEntryAndItsProofs(t *testing.T) {
	root := repoRoot(t)
	before := resolved(list(t, root, ""))
	copyDir := copyEntries(t, root)
	all := entries(t, copyDir)
	victim := all[3]
	if err := os.Remove(victim.path); err != nil {
		t.Fatal(err)
	}
	r := list(t, root, copyDir)
	if r.err != nil {
		t.Fatalf("--list failed: %v\n%s", r.err, r.stderr)
	}
	after := resolved(r)
	index := map[string]bool{}
	for _, line := range after {
		index[line] = true
	}
	var removed []string
	for _, line := range before {
		if !index[line] {
			removed = append(removed, line)
		}
	}
	want := 1 + len(victim.proofs())
	if len(removed) != want || len(before)-len(after) != want {
		t.Fatalf("removed %d lines, want %d: %v", len(removed), want, removed)
	}
	if !strings.Contains(removed[0], " package="+victim.get("package")) {
		t.Fatalf("the removed command is %q, want package %s", removed[0], victim.get("package"))
	}
	for position, name := range victim.proofs() {
		if !strings.HasPrefix(removed[1+position], "PROOF name="+name+" ") {
			t.Fatalf("removed proof %d is %q, want %s", position, removed[1+position], name)
		}
	}
}

func TestARunWithoutAProofFailsLoudly(t *testing.T) {
	root := repoRoot(t)
	copyDir := copyEntries(t, root)
	victim := entries(t, copyDir)[5]
	var kept []string
	for _, field := range victim.fields {
		if !strings.HasPrefix(field[0], "proof") {
			kept = append(kept, field[0]+"="+field[1])
		}
	}
	if err := os.WriteFile(victim.path, []byte(strings.Join(kept, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	r := list(t, root, copyDir)
	if r.err == nil || !strings.Contains(r.stderr, "declares no proof") {
		t.Fatalf("a run without a proof was accepted: %v\n%s", r.err, r.stderr)
	}
}

func TestAnEmptyOrMissingDirectoryFailsLoudly(t *testing.T) {
	root := repoRoot(t)
	r := list(t, root, t.TempDir())
	if r.err == nil || !strings.Contains(r.stderr, "holds no .run entry") {
		t.Fatalf("an empty directory was accepted: %v\n%s", r.err, r.stderr)
	}
	r = list(t, root, filepath.Join(t.TempDir(), "does-not-exist"))
	if r.err == nil || !strings.Contains(r.stderr, "missing or unreadable") {
		t.Fatalf("a missing directory was accepted: %v\n%s", r.err, r.stderr)
	}
}

func TestAMalformedEntryFailsInsteadOfBeingSkipped(t *testing.T) {
	root := repoRoot(t)
	cases := []struct{ text, message string }{
		{"pakage=./internal/x\nproof=a|b\n", "unknown key"},
		{"label=nothing to run\nproof=a|b\n", "needs a package"},
		{"package=./internal/x\nproof=a|b\nproof=a|c\n", "listed twice"},
		{"package=./internal/x\nproof_match=zzz|^x$\n", "no earlier proof line"},
	}
	for number, c := range cases {
		t.Run(fmt.Sprintf("case%d", number), func(t *testing.T) {
			copyDir := copyEntries(t, root)
			victim := entries(t, copyDir)[0]
			if err := os.WriteFile(victim.path, []byte(c.text), 0o644); err != nil {
				t.Fatal(err)
			}
			r := list(t, root, copyDir)
			if r.err == nil || !strings.Contains(r.stderr, c.message) {
				t.Fatalf("%q was accepted or failed for another reason: %v\n%s", c.text, r.err, r.stderr)
			}
		})
	}
}
