package scripts_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// ci/auth_queue_relevance.sh: the auth gates' relevance for a merge queue entry (acp 5a). It judges the WHOLE group
// (merge-base(target tip, entry head)..entry head), never the entry's own PR, and it fails closed: any error or non-zero
// exit of the relevance check prints relevant=true.

type authRelevanceRepo struct {
	t   *testing.T
	dir string
}

func (r authRelevanceRepo) git(args ...string) string {
	r.t.Helper()
	cmd := exec.Command("git", append([]string{"-c", "user.name=t", "-c", "user.email=t@example.com", "-c", "commit.gpgsign=false"}, args...)...)
	cmd.Dir = r.dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		r.t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

func (r authRelevanceRepo) commit(path string) string {
	r.t.Helper()
	full := filepath.Join(r.dir, path)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		r.t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(path+"\n"), 0o644); err != nil {
		r.t.Fatal(err)
	}
	r.git("add", "-A")
	r.git("commit", "-q", "-m", path)
	return r.git("rev-parse", "HEAD")
}

// newAuthRelevanceRepo copies the real script into a fresh repo; checkGo is the ci/check_go.sh it runs ("" = the real one,
// with ci/lib).
func newAuthRelevanceRepo(t *testing.T, checkGo string) authRelevanceRepo {
	t.Helper()
	root := repoRoot(t)
	r := authRelevanceRepo{t: t, dir: t.TempDir()}
	r.git("init", "-q", "-b", "main")
	copyFile := func(src, dst string) {
		raw, err := os.ReadFile(src)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(dst, raw, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	copyFile(filepath.Join(root, "ci", "auth_queue_relevance.sh"), filepath.Join(r.dir, "ci", "auth_queue_relevance.sh"))
	if checkGo == "" {
		copyFile(filepath.Join(root, "ci", "check_go.sh"), filepath.Join(r.dir, "ci", "check_go.sh"))
		entries, err := os.ReadDir(filepath.Join(root, "ci", "lib"))
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			if !e.IsDir() {
				copyFile(filepath.Join(root, "ci", "lib", e.Name()), filepath.Join(r.dir, "ci", "lib", e.Name()))
			}
		}
	} else if err := os.WriteFile(filepath.Join(r.dir, "ci", "check_go.sh"), []byte(checkGo), 0o755); err != nil {
		t.Fatal(err)
	}
	return r
}

func runAuthRelevance(t *testing.T, r authRelevanceRepo, extraPath string, args ...string) cmdResult {
	t.Helper()
	path := "PATH=" + withShims("/usr/bin:/bin:/usr/local/bin")
	if extraPath != "" {
		path = "PATH=" + withShims(extraPath+string(os.PathListSeparator)+"/usr/bin:/bin:/usr/local/bin")
	}
	res := cmdSpec{dir: r.dir, env: []string{path, "HOME=" + t.TempDir(), "GOCACHE=" + t.TempDir()}}.run("bash", append([]string{filepath.Join(r.dir, "ci", "auth_queue_relevance.sh")}, args...)...)
	if res.code != 0 {
		t.Fatalf("the script must always exit 0, got %d: stdout=%q stderr=%q", res.code, res.stdout, res.stderr)
	}
	return res
}

// The planted failing `go list` (acp 5a ii): the REAL ci/check_go.sh, a go that passes the toolchain checks and fails
// every `go list`.
func TestAuthQueueRelevanceAFailingGoListRunsTheGates(t *testing.T) {
	r := newAuthRelevanceRepo(t, "")
	if err := os.WriteFile(filepath.Join(r.dir, "go.mod"), []byte("module example.com/authrelevance\n\ngo 1.27\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	base := r.commit("README.md")
	head := r.commit("docs/a.md")
	bin, goroot := t.TempDir(), t.TempDir()
	if err := os.MkdirAll(filepath.Join(goroot, "bin"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(goroot, "bin", "gofmt"), []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	stub := "#!/bin/sh\ncase \"$1\" in\n" +
		"  version) echo \"go version go1.27.3 linux/amd64\" ;;\n" +
		"  env) if [ \"$2\" = GOROOT ]; then echo \"" + goroot + "\"; fi ;;\n" +
		"  list) echo \"go: planted go list failure\" >&2; echo planted > \"" + filepath.Join(bin, "list-called") + "\"; exit 1 ;;\n" +
		"  *) echo \"go stub: unexpected $*\" >&2; exit 1 ;;\nesac\n"
	if err := os.WriteFile(filepath.Join(bin, "go"), []byte(stub), 0o755); err != nil {
		t.Fatal(err)
	}
	res := runAuthRelevance(t, r, bin, base, head)
	if strings.TrimSpace(res.stdout) != "relevant=true" {
		t.Fatalf("a failing go list printed %q, want relevant=true (fail closed)\nstderr: %s", res.stdout, res.stderr)
	}
	if _, err := os.Stat(filepath.Join(bin, "list-called")); err != nil {
		t.Fatalf("the relevance check never reached go list, so the test proved nothing: %s", res.stderr)
	}
	if !strings.Contains(res.stderr, "exited non-zero") {
		t.Errorf("stderr does not name the failed relevance check: %s", res.stderr)
	}
}

const recordingCheckGo = "#!/usr/bin/env bash\nprintf '%s\\n' \"$*\" > \"$(dirname \"$0\")/../args\"\n"

func TestAuthQueueRelevanceVerdicts(t *testing.T) {
	cases := []struct {
		name, checkGo, want string
	}{
		{"the rule says false", recordingCheckGo + "echo relevant=false\n", "relevant=false"},
		{"the rule says true", recordingCheckGo + "echo 'auth relevance: x' >&2\necho relevant=true\n", "relevant=true"},
		{"a verdict printed before a non-zero exit is not trusted", recordingCheckGo + "echo relevant=false\nexit 1\n", "relevant=true"},
		{"no verdict", recordingCheckGo + "echo done\n", "relevant=true"},
		{"two verdicts", recordingCheckGo + "echo relevant=false\necho relevant=false\n", "relevant=true"},
		{"a verdict with trailing text", recordingCheckGo + "echo 'relevant=false '\n", "relevant=true"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := newAuthRelevanceRepo(t, c.checkGo)
			tip := r.commit("seed.txt")
			head := r.commit("docs/a.md")
			if got := strings.TrimSpace(runAuthRelevance(t, r, "", tip, head).stdout); got != c.want {
				t.Errorf("printed %q, want %q", got, c.want)
			}
		})
	}
}

// The whole group: the target branch tip is the main commit, entry 1 changes auth code, entry 2 (the judged entry, on
// top of entry 1) changes docs only. The rule must be handed merge-base(tip, entry 2)..entry 2, never entry 1..entry 2.
func TestAuthQueueRelevanceJudgesTheWholeGroup(t *testing.T) {
	r := newAuthRelevanceRepo(t, recordingCheckGo+"echo relevant=false\n")
	tip := r.commit("seed.txt")
	r.commit("internal/auth/x.go")
	entry2 := r.commit("docs/a.md")
	runAuthRelevance(t, r, "", tip, entry2)
	args, err := os.ReadFile(filepath.Join(r.dir, "args"))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := strings.TrimSpace(string(args)), "auth relevant "+tip+" "+entry2; got != want {
		t.Errorf("the rule was called with %q, want %q (the whole group from the merge base with the target tip)", got, want)
	}
}

func TestAuthQueueRelevanceFailsClosedOnBadInput(t *testing.T) {
	r := newAuthRelevanceRepo(t, recordingCheckGo+"echo relevant=false\n")
	tip := r.commit("seed.txt")
	head := r.commit("docs/a.md")
	for name, args := range map[string][]string{
		"no arguments":          {},
		"one argument":          {tip},
		"three arguments":       {tip, head, head},
		"a ref name":            {"main", head},
		"a short sha":           {tip[:12], head},
		"an unknown commit":     {strings.Repeat("0", 40), head},
		"an unrelated tip":      {strings.Repeat("a", 40), head},
		"an option as argument": {"--help", head},
	} {
		if got := strings.TrimSpace(runAuthRelevance(t, r, "", args...).stdout); got != "relevant=true" {
			t.Errorf("%s: printed %q, want relevant=true", name, got)
		}
	}
	if got := strings.TrimSpace(runAuthRelevance(t, r, "", tip, head).stdout); got != "relevant=false" {
		t.Errorf("control: valid input printed %q, want relevant=false", got)
	}
}

// A planted merge-base failure: the target branch tip is a real commit of an unrelated history, so git merge-base exits
// non-zero. The script must print relevant=true and never call the rule.
func TestAuthQueueRelevanceFailsClosedOnAMergeBaseFailure(t *testing.T) {
	r := newAuthRelevanceRepo(t, recordingCheckGo+"echo relevant=false\n")
	r.commit("seed.txt")
	head := r.commit("docs/a.md")
	r.git("checkout", "-q", "--orphan", "unrelated")
	unrelated := r.commit("other.txt")
	if out, err := exec.Command("git", "-C", r.dir, "merge-base", unrelated, head).CombinedOutput(); err == nil {
		t.Fatalf("control: git merge-base found %q for unrelated histories; the plant did not land", out)
	}
	res := runAuthRelevance(t, r, "", unrelated, head)
	if got := strings.TrimSpace(res.stdout); got != "relevant=true" {
		t.Errorf("a merge-base failure printed %q, want relevant=true", got)
	}
	if !strings.Contains(res.stderr, "no merge base") {
		t.Errorf("stderr does not name the merge-base failure: %s", res.stderr)
	}
	if _, err := os.Stat(filepath.Join(r.dir, "args")); err == nil {
		t.Error("the rule was called although no merge base exists")
	}
}
