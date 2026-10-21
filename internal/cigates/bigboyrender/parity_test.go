package bigboyrender_test

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// TestCLIOutputParityRecordedPair replays each case under testdata/parity (recorded from the Python scripts, see
// testdata/README.md) against the BUILT cigate binary: the same arguments on the same inputs give the same exit code,
// stdout, process stderr and written files, byte for byte, and leave no other file behind. The binary (not an
// in-process call) is run so a stray write on the real process stderr, such as a library WARN, fails the case.
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
		dir := filepath.Join(root, entry.Name())
		if !entry.IsDir() {
			continue
		}
		t.Run(entry.Name(), func(t *testing.T) { replay(t, dir) })
	}
}

var fileArg = regexp.MustCompile(`^\{FILE:(.+)\}$`)

func replay(t *testing.T, dir string) {
	work := t.TempDir()
	inputs := map[string]bool{}
	in := filepath.Join(dir, "in")
	_ = filepath.Walk(in, func(path string, info os.FileInfo, err error) error {
		if err == nil && info.Mode().IsRegular() {
			rel, _ := filepath.Rel(in, path)
			rel = strings.TrimSuffix(rel, ".fixture")
			inputs[rel] = true
			writeFile(t, filepath.Join(work, rel), readFile(t, path))
		}
		return nil
	})
	sub := map[string]string{"{WORK}": work}
	if inputs["old.go"] {
		// a posture case: the repository holds old.go then new.go as the role's posture file
		args := strings.Split(strings.TrimSpace(readFile(t, filepath.Join(dir, "args"))), "\n")
		gofile := map[string]string{"api": "api_authorization.go", "domain": "domain_authorization.go"}[args[1]]
		repo, shas := gitRepo(t, "internal/storage/postgres/"+gofile, readFile(t, filepath.Join(in, "old.go.fixture")), readFile(t, filepath.Join(in, "new.go.fixture")))
		sub["{REPO}"], sub["{OLD}"], sub["{NEW}"] = repo, shas[0], shas[1]
		delete(inputs, "old.go")
		delete(inputs, "new.go")
		for _, name := range []string{"old.go", "new.go"} {
			_ = os.Remove(filepath.Join(work, name))
		}
	}
	subst := func(s string) string {
		for k, v := range sub {
			s = strings.ReplaceAll(s, k, v)
		}
		return s
	}
	var args []string
	for _, a := range strings.Split(strings.TrimSuffix(readFile(t, filepath.Join(dir, "args")), "\n"), "\n") {
		if m := fileArg.FindStringSubmatch(a); m != nil {
			name := m[1]
			if strings.HasSuffix(name, ".go") {
				name += ".fixture"
			}
			a = readFile(t, filepath.Join(in, name))
		}
		args = append(args, subst(a))
	}
	env := []string{}
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "API_CH_PASSWORD=") && !strings.HasPrefix(kv, "BIGBOY_OPS_REPO=") {
			env = append(env, kv)
		}
	}
	env = append(env, "API_CH_PASSWORD=", "BIGBOY_OPS_REPO=")
	if raw, err := os.ReadFile(filepath.Join(dir, "env")); err == nil {
		for _, line := range strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n") {
			k, v, _ := strings.Cut(line, "=")
			env = append(env, k+"="+subst(v))
		}
	}
	before := snapshotFiles(work)

	code, stdout, stderr := runBinary(t, work, env, args)
	if want := strings.TrimSpace(readFile(t, filepath.Join(dir, "rc"))); want != itoa(code) {
		t.Errorf("exit %d, recorded %s", code, want)
	}
	if want := subst(readFile(t, filepath.Join(dir, "stdout"))); stdout != want {
		t.Errorf("stdout differs from the recording\ngot:\n%q\nrecorded:\n%q", stdout, want)
	}
	if problem := stderrProblem(filepath.Base(dir), stderr, recordedStderrOf(t, dir), subst); problem != "" {
		t.Error(problem)
	}
	expected := map[string]bool{}
	for name := range inputs {
		expected[name] = true
	}
	out := filepath.Join(dir, "out")
	_ = filepath.Walk(out, func(path string, info os.FileInfo, err error) error {
		if err == nil && info.Mode().IsRegular() {
			rel, _ := filepath.Rel(out, path)
			expected[rel] = true
			if got := readFile(t, filepath.Join(work, rel)); got != readFile(t, path) {
				t.Errorf("%s differs from the recording\ngot:\n%q\nrecorded:\n%q", rel, got, readFile(t, path))
			}
			if rel == "out.xml" {
				if info, err := os.Stat(filepath.Join(work, rel)); err != nil || info.Mode().Perm() != 0o644 {
					t.Errorf("out.xml mode is %v, the Python script wrote 0644 (err %v)", info.Mode().Perm(), err)
				}
			}
		}
		return nil
	})
	recorded := map[string]bool{}
	_ = filepath.Walk(out, func(path string, info os.FileInfo, err error) error {
		if err == nil && info.Mode().IsRegular() {
			rel, _ := filepath.Rel(out, path)
			recorded[rel] = true
		}
		return nil
	})
	if changed := changedInputs(before, snapshotFiles(work), recorded); len(changed) > 0 {
		t.Errorf("the verb changed or removed input files the Python script left alone (not in out/): %v", changed)
	}
	var left []string
	_ = filepath.Walk(work, func(path string, info os.FileInfo, err error) error {
		if err == nil && info.Mode().IsRegular() {
			rel, _ := filepath.Rel(work, path)
			if !expected[rel] {
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

func itoa(n int) string { return strconv.Itoa(n) }

// snapshotFiles is path (relative to root) -> content of every regular file under root.
func snapshotFiles(root string) map[string]string {
	files := map[string]string{}
	_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err == nil && info.Mode().IsRegular() {
			rel, _ := filepath.Rel(root, path)
			if b, err := os.ReadFile(path); err == nil {
				files[rel] = string(b)
			}
		}
		return nil
	})
	return files
}

// changedInputs lists the files of before that are gone or different in after and are not in the case's
// recorded out/ (those are compared with the recording instead).
func changedInputs(before, after map[string]string, recorded map[string]bool) []string {
	var changed []string
	for rel, content := range before {
		if got, ok := after[rel]; (!ok || got != content) && !recorded[rel] {
			changed = append(changed, rel)
		}
	}
	sort.Strings(changed)
	return changed
}

// TestParityInputGuardCatchesAModifiedInput is the control of the guard in replay: a verb that rewrote or removed an
// input the Python script left alone must be reported, and a recorded out/ file must not be.
func TestParityInputGuardCatchesAModifiedInput(t *testing.T) {
	work := t.TempDir()
	writeFile(t, filepath.Join(work, "a.txt"), "one")
	writeFile(t, filepath.Join(work, "b.txt"), "two")
	writeFile(t, filepath.Join(work, "c.txt"), "three")
	before := snapshotFiles(work)
	if got := changedInputs(before, snapshotFiles(work), nil); len(got) != 0 {
		t.Fatalf("nothing changed, guard reports %v", got)
	}
	writeFile(t, filepath.Join(work, "a.txt"), "ONE")
	writeFile(t, filepath.Join(work, "b.txt"), "TWO")
	if err := os.Remove(filepath.Join(work, "c.txt")); err != nil {
		t.Fatal(err)
	}
	got := changedInputs(before, snapshotFiles(work), map[string]bool{"b.txt": true})
	if strings.Join(got, ",") != "a.txt,c.txt" {
		t.Fatalf("guard reports %v, want [a.txt c.txt] (b.txt is recorded in out/)", got)
	}
}

// recordedStderr is what a case directory holds about stderr: stderr (Python's own, compared exactly), stderr.python
// (Python's recorded stderr of a case that has no exact file), stderr.traceback (the one line the verb prints where
// Python printed a traceback) and stderr.usage (the declared exception, see usageExceptions).
type recordedStderr struct {
	exact, python, traceback, goLine         string
	hasExact, hasPython, hasTraceback, hasGo bool
}

func recordedStderrOf(t *testing.T, dir string) recordedStderr {
	t.Helper()
	var r recordedStderr
	read := func(name string) (string, bool) {
		b, err := os.ReadFile(filepath.Join(dir, name))
		return string(b), err == nil
	}
	r.exact, r.hasExact = read("stderr")
	r.python, r.hasPython = read("stderr.python")
	r.traceback, r.hasTraceback = read("stderr.traceback")
	r.goLine, r.hasGo = read("stderr.usage")
	return r
}

const tracebackStart = "Traceback (most recent call last):"

// usageExceptions are the cases where the verb's stderr differs from Python's on purpose although Python did NOT
// print a traceback: case -> reason. Each has stderr.usage (the verb's line) and stderr.python (Python's own line).
var usageExceptions = map[string]string{
	"render-missing-args": "the usage line names the verb (cigate bigboy render-ch-users), not render-dho-api-ch-users.py; Python's rc 2 and empty stdout are compared as everywhere",
}

// stderrProblem is "" when the process stderr is what the recording allows: the exact Python stderr; or, only where the
// recorded Python stderr really starts with a traceback, the verb's one pinned error line; or, for a case named in
// usageExceptions, its declared line. Anything else, including a case with no recorded Python stderr, is a problem.
func stderrProblem(name, got string, rec recordedStderr, subst func(string) string) string {
	switch {
	case rec.hasExact:
		if rec.hasTraceback || rec.hasGo {
			return "stderr is compared exactly, a traceback or go-line file next to it is not allowed"
		}
		if want := subst(rec.exact); got != want {
			return "stderr differs from the recording\ngot:\n" + strconvQuote(got) + "\nrecorded:\n" + strconvQuote(want)
		}
	case rec.hasTraceback:
		if !rec.hasPython || !strings.HasPrefix(rec.python, tracebackStart) {
			return "stderr.traceback is allowed only where the recorded Python stderr (stderr.python) is a traceback"
		}
		if want := subst(rec.traceback); got != want || strings.Count(got, "\n") != 1 {
			return "stderr is not the one error line that replaces the Python traceback\ngot:\n" + strconvQuote(got) + "\nwant:\n" + strconvQuote(want)
		}
	case rec.hasGo:
		if _, listed := usageExceptions[name]; !listed {
			return "stderr.usage is allowed only for a case listed in usageExceptions"
		}
		if !rec.hasPython || strings.HasPrefix(rec.python, tracebackStart) {
			return "a usageExceptions case needs the recorded Python stderr, and it is not a traceback"
		}
		if want := subst(rec.goLine); got != want {
			return "stderr differs from the declared exception line\ngot:\n" + strconvQuote(got) + "\nwant:\n" + strconvQuote(want)
		}
	default:
		return "no recorded stderr (stderr, stderr.traceback or stderr.usage) for this case"
	}
	return ""
}

func strconvQuote(s string) string { return strconv.Quote(s) }

// TestStderrExemptionsAreOnlyWhereThePythonRecordsAllowThem is the planted red rows of stderrProblem.
func TestStderrExemptionsAreOnlyWhereThePythonRecordsAllowThem(t *testing.T) {
	id := func(s string) string { return s }
	tb := tracebackStart + "\n  File x\nValueError\n"
	plain := "usage: render-dho-api-ch-users.py <authorization.go> <out-file>\n"
	line := "boom\n"
	for name, tc := range map[string]struct {
		rec  recordedStderr
		got  string
		fail bool
	}{
		"traceback file, Python recorded a traceback":                    {recordedStderr{traceback: line, hasTraceback: true, python: tb, hasPython: true}, line, false},
		"traceback file, no Python stderr recorded":                      {recordedStderr{traceback: line, hasTraceback: true}, line, true},
		"traceback file, Python stderr is a plain line":                  {recordedStderr{traceback: line, hasTraceback: true, python: plain, hasPython: true}, line, true},
		"traceback file, verb printed two lines":                         {recordedStderr{traceback: line, hasTraceback: true, python: tb, hasPython: true}, line + line, true},
		"exact file, same text":                                          {recordedStderr{exact: plain, hasExact: true}, plain, false},
		"exact file, other text":                                         {recordedStderr{exact: plain, hasExact: true}, line, true},
		"exact file with a traceback file beside it":                     {recordedStderr{exact: plain, hasExact: true, traceback: line, hasTraceback: true}, plain, true},
		"nothing recorded":                                               {recordedStderr{}, "", true},
		"go line for a case that is not listed":                          {recordedStderr{goLine: line, hasGo: true, python: plain, hasPython: true}, line, true},
		"go line for a listed case, Python plain":                        {recordedStderr{goLine: line, hasGo: true, python: plain, hasPython: true}, line, false},
		"go line for a listed case, Python recorded a traceback":         {recordedStderr{goLine: line, hasGo: true, python: tb, hasPython: true}, line, true},
		"go line for a listed case, verb printed something else instead": {recordedStderr{goLine: line, hasGo: true, python: plain, hasPython: true}, plain, true},
	} {
		name := name
		listed := strings.HasPrefix(name, "go line for a listed")
		caseName := "render-missing-args"
		if !listed {
			caseName = "bump-ok"
		}
		if got := stderrProblem(caseName, tc.got, tc.rec, id) != ""; got != tc.fail {
			t.Errorf("%s: problem=%v, want %v", name, got, tc.fail)
		}
	}
}

// TestEveryUsageExceptionHasItsFiles keeps the exception list honest: a listed case has stderr.usage and a plain
// stderr.python, and no case outside the list has stderr.usage.
func TestEveryUsageExceptionHasItsFiles(t *testing.T) {
	root := filepath.Join("testdata", "parity")
	for name := range usageExceptions {
		rec := recordedStderrOf(t, filepath.Join(root, name))
		if !rec.hasGo || !rec.hasPython || strings.HasPrefix(rec.python, tracebackStart) {
			t.Errorf("%s is listed in usageExceptions but lacks stderr.usage or a plain stderr.python", name)
		}
	}
	entries, _ := os.ReadDir(root)
	for _, e := range entries {
		if _, err := os.Stat(filepath.Join(root, e.Name(), "stderr.usage")); err == nil {
			if _, listed := usageExceptions[e.Name()]; !listed {
				t.Errorf("%s has stderr.usage but is not in usageExceptions", e.Name())
			}
		}
	}
}

var (
	cigateOnce sync.Once
	cigatePath string
	cigateErr  error
)

// runBinary runs `cigate bigboy <args>` (built once from ./cmd/cigate) in dir with env and returns the exit code,
// stdout and the process stderr.
func runBinary(t *testing.T, dir string, env, args []string) (int, string, string) {
	t.Helper()
	cigateOnce.Do(func() {
		tmp, err := os.MkdirTemp("", "cigate-bin-")
		if err != nil {
			cigateErr = err
			return
		}
		cigateBinDir = tmp
		cigatePath = filepath.Join(tmp, "cigate")
		build := exec.Command("go", "build", "-trimpath", "-o", cigatePath, "./cmd/cigate")
		build.Dir = repoRoot(t)
		if out, err := build.CombinedOutput(); err != nil {
			cigateErr = fmt.Errorf("go build ./cmd/cigate: %v\n%s", err, out)
		}
	})
	if cigateErr != nil {
		t.Fatal(cigateErr)
	}
	cmd := exec.Command(cigatePath, append([]string{"bigboy"}, args...)...)
	cmd.Dir, cmd.Env = dir, env
	var so, se bytes.Buffer
	cmd.Stdout, cmd.Stderr = &so, &se
	err := cmd.Run()
	code := 0
	if ee, ok := err.(*exec.ExitError); ok {
		code = ee.ExitCode()
	} else if err != nil {
		t.Fatal(err)
	}
	return code, so.String(), se.String()
}

var cigateBinDir string

func TestMain(m *testing.M) {
	code := m.Run()
	if cigateBinDir != "" {
		_ = os.RemoveAll(cigateBinDir)
	}
	os.Exit(code)
}
