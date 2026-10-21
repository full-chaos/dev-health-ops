package docstools_test

import (
	"bytes"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cigates/docstools"
)

// caseSetups are per-verb hooks a test file adds (init) to arm a recorded case: a fake HTTP client, a fake
// command runner, a git repository. They run after the case inputs are in the work directory and before the verb.
var caseSetups []func(t *testing.T, caseDir, work string, sub map[string]string)

// caseScaffold holds the files a setup hook wrote for the current case that are neither inputs nor outputs.
var caseScaffold map[string]bool

func runDocs(args ...string) (int, string, string) {
	var stdout, stderr bytes.Buffer
	code := docstools.Run(args, &stdout, &stderr)
	return code, stdout.String(), stderr.String()
}

// TestCLIOutputParityRecordedPair replays each case under testdata/parity (recorded from the Python scripts, see
// testdata/README.md): the same arguments on the same inputs give the same exit code, stdout, stderr and written
// files, byte for byte, and leave no other file behind.
func TestCLIOutputParityRecordedPair(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("testdata", "parity"))
	if err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) == 0 {
		t.Fatalf("no recorded cases under %s: %v", root, err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			dir := filepath.Join(root, entry.Name())
			t.Run(entry.Name(), func(t *testing.T) { replayCase(t, dir) })
		}
	}
}

func replayCase(t *testing.T, dir string) { replayCaseWith(t, dir, runDocs) }

// replayCaseWith replays a case with run (in process, or the built cigate binary) as the verb.
func replayCaseWith(t *testing.T, dir string, run func(args ...string) (int, string, string)) {
	work := t.TempDir()
	expected := map[string]bool{}
	placeInputs(t, dir, work, expected)
	seedSymlinks(t, filepath.Join(dir, "symlinks"), work)
	sub := map[string]string{"{WORK}": work}
	t.Chdir(work)
	caseScaffold = map[string]bool{}
	if raw, err := os.ReadFile(filepath.Join(dir, "cwd")); err == nil {
		// the case runs from a directory below the work directory (the former scripts found their root from their own path)
		sub := filepath.Join(work, strings.TrimSpace(string(raw)))
		if err := os.MkdirAll(sub, 0o755); err != nil {
			t.Fatal(err)
		}
		t.Chdir(sub)
	}
	for _, setup := range caseSetups {
		setup(t, dir, work, sub)
	}
	subst := func(s string) string {
		for k, v := range sub {
			s = strings.ReplaceAll(s, k, v)
		}
		return s
	}
	var args []string
	if raw := mustRead(t, filepath.Join(dir, "args")); raw != "" {
		for _, a := range strings.Split(strings.TrimSuffix(raw, "\n"), "\n") {
			args = append(args, subst(a))
		}
	}
	if raw, err := os.ReadFile(filepath.Join(dir, "env")); err == nil {
		for _, line := range strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n") {
			k, v, _ := strings.Cut(line, "=")
			t.Setenv(k, subst(v))
		}
	}

	code, stdout, stderr := run(args...)
	if want := strings.TrimSpace(mustRead(t, filepath.Join(dir, "rc"))); want != strconv.Itoa(code) {
		t.Errorf("exit %d, recorded %s", code, want)
	}
	if want := subst(mustRead(t, filepath.Join(dir, "stdout"))); stdout != want {
		t.Errorf("stdout differs from the recording\ngot:\n%q\nrecorded:\n%q", stdout, want)
	}
	if line, err := os.ReadFile(filepath.Join(dir, "stderr.traceback")); err == nil {
		// Python stopped on an uncaught exception: the verb prints exactly the exception text on stderr instead. It is
		// one line, or, when the case keeps the whole recorded traceback (stderr.python), the text that ends it.
		want := subst(string(line))
		if full, err := os.ReadFile(filepath.Join(dir, "stderr.python")); err == nil {
			if !strings.HasPrefix(string(full), "Traceback (most recent call last):\n") || !strings.HasSuffix(string(full), "\n"+string(line)) {
				t.Errorf("stderr.traceback is not the exception text that ends the recorded traceback stderr.python")
			}
		} else if strings.Count(want, "\n") != 1 {
			t.Errorf("stderr.traceback without stderr.python must be one line")
		}
		if stderr != want {
			t.Errorf("stderr is not the exception text that replaces the Python traceback\ngot:\n%q\nwant:\n%q", stderr, want)
		}
	} else if want := subst(mustRead(t, filepath.Join(dir, "stderr"))); stderr != want {
		t.Errorf("stderr differs from the recording\ngot:\n%q\nrecorded:\n%q", stderr, want)
	}
	out := filepath.Join(dir, "out")
	_ = filepath.Walk(out, func(path string, info os.FileInfo, err error) error {
		if err == nil && info.Mode().IsRegular() {
			rel, _ := filepath.Rel(out, path)
			rel = caseRel(strings.TrimSuffix(rel, ".fixture"))
			expected[rel] = true
			if got, want := mustRead(t, filepath.Join(work, rel)), mustRead(t, path); got != want {
				t.Errorf("%s differs from the recording\ngot:\n%q\nrecorded:\n%q", rel, got, want)
			}
		}
		return nil
	})
	var left []string
	_ = filepath.Walk(work, func(path string, info os.FileInfo, err error) error {
		if err == nil && info.Mode().IsRegular() {
			rel, _ := filepath.Rel(work, path)
			if !expected[rel] && !caseScaffold[rel] {
				left = append(left, rel)
			}
		}
		return nil
	})
	sort.Strings(left)
	if len(left) > 0 {
		t.Errorf("the verb left files the Python script did not: %v", left)
	}
}

// placeInputs writes a case's inputs into work: first the inputs of the case named in its base file (if any), then
// its own in/ over them, then it removes the paths its drop file lists (one per line).
func placeInputs(t *testing.T, dir, work string, expected map[string]bool) {
	t.Helper()
	if raw, err := os.ReadFile(filepath.Join(dir, "base")); err == nil {
		placeInputs(t, filepath.Join(filepath.Dir(dir), strings.TrimSpace(string(raw))), work, expected)
	}
	in := filepath.Join(dir, "in")
	_ = filepath.Walk(in, func(path string, info os.FileInfo, err error) error {
		if err == nil && info.Mode().IsRegular() {
			rel, _ := filepath.Rel(in, path)
			rel = caseRel(strings.TrimSuffix(rel, ".fixture"))
			expected[rel] = true
			writeTree(t, filepath.Join(work, rel), mustRead(t, path), info.Mode().Perm())
		} else if err == nil && info.Mode()&os.ModeSymlink != 0 {
			rel, _ := filepath.Rel(in, path)
			copySymlink(t, path, filepath.Join(work, rel))
		}
		return nil
	})
	if raw, err := os.ReadFile(filepath.Join(dir, "drop")); err == nil {
		for _, rel := range strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n") {
			if err := os.Remove(filepath.Join(work, rel)); err != nil {
				t.Fatalf("%s: drop %s: %v", dir, rel, err)
			}
			delete(expected, rel)
		}
	}
}

// caseRel is the work-directory path of a stored case file: tokens stand for what a tracked path cannot hold well,
// {E9} for e-acute, {DOT} for a leading dot an ignore rule would match (.build), {FF} and {FE} for those raw bytes.
func caseRel(rel string) string {
	return strings.NewReplacer("{E9}", "é", "{DOT}", ".", "{FC}", "ü", "{FF}", "\xff", "{FE}", "\xfe").Replace(rel)
}

func mustRead(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(raw)
}

func writeTree(t *testing.T, path, content string, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
}

// copySymlink makes dst a symlink with the same target as src (a dangling target stays dangling).
func copySymlink(t *testing.T, src, dst string) {
	t.Helper()
	target, err := os.Readlink(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, dst); err != nil {
		t.Fatal(err)
	}
}

// seedSymlinks creates the symlinks a case lists in its "symlinks" file, one "<link> -> <target>" per line (git
// keeps no dangling or looping link well, so the case records them as text). No file, no links.
func seedSymlinks(t *testing.T, list, work string) {
	t.Helper()
	raw, err := os.ReadFile(list)
	if err != nil {
		return
	}
	for _, line := range strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n") {
		link, target, ok := strings.Cut(line, " -> ")
		if !ok {
			t.Fatalf("%s: bad line %q", list, line)
		}
		if err := os.MkdirAll(filepath.Dir(filepath.Join(work, link)), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(strings.ReplaceAll(target, "{WORK}", work), filepath.Join(work, link)); err != nil {
			t.Fatal(err)
		}
	}
}
