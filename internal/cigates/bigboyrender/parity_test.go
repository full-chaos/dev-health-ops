package bigboyrender_test

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

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
	t.Setenv("API_CH_PASSWORD", "")
	t.Setenv("BIGBOY_OPS_REPO", "")
	if raw, err := os.ReadFile(filepath.Join(dir, "env")); err == nil {
		for _, line := range strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n") {
			k, v, _ := strings.Cut(line, "=")
			t.Setenv(k, subst(v))
		}
	}
	t.Chdir(work)

	code, stdout, stderr := run(args...)
	if want := strings.TrimSpace(readFile(t, filepath.Join(dir, "rc"))); want != itoa(code) {
		t.Errorf("exit %d, recorded %s", code, want)
	}
	if want := subst(readFile(t, filepath.Join(dir, "stdout"))); stdout != want {
		t.Errorf("stdout differs from the recording\ngot:\n%q\nrecorded:\n%q", stdout, want)
	}
	if _, err := os.Stat(filepath.Join(dir, "stderr.skip")); err != nil {
		if want := subst(readFile(t, filepath.Join(dir, "stderr"))); stderr != want {
			t.Errorf("stderr differs from the recording\ngot:\n%q\nrecorded:\n%q", stderr, want)
		}
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
