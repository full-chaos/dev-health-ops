package workflows_test

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

// CHAOS-11670: the merge queue runs the external auth corpus when a queue entry touches ci/auth_corpus_paths.txt (the
// floor) OR a repo package of the LIVE `go list -deps` closure of the auth route packages (the relevance script runs go
// list itself). These tests pin the roots and fail when a floor row matches no tracked file.

const corpusModule = "github.com/full-chaos/dev-health-ops"

// The roots ruled for CHAOS-11670 (acp): the go-api auth route packages, the apiservice top level, both binaries.
var corpusClosureRoots = []string{
	"./internal/api/session", "./internal/api/sso", "./internal/api/policy", "./internal/apiservice", "./cmd/dho", "./cmd/auth-service",
	"./cmd/dev-health-worker-migrate", "./cmd/auth-migrate",
}

// The package the corpus runs `go test -tags integration` on.
const corpusTestPackage = "./internal/apiservice/authflowvenue"

const corpusSetFile = "ci/auth_corpus_set.sh"

func corpusRun(t *testing.T, root, name string, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = root
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("%s %v: %v\n%s", name, args, err, stderr.String())
	}
	return stdout.String()
}

// corpusSetValue reads `NAME=value` or `NAME=(v1 v2 ...)` from ci/auth_corpus_set.sh, the one source of the roots and of
// the tested package for both ci/auth_corpus_external.sh and ci/auth_corpus_queue_relevance.sh.
func corpusSetValue(t *testing.T, root, name string) []string {
	t.Helper()
	for _, line := range strings.Split(readFile(t, filepath.Join(root, corpusSetFile)), "\n") {
		rest, ok := strings.CutPrefix(strings.TrimSpace(line), name+"=")
		if !ok {
			continue
		}
		rest = strings.Trim(rest, "()")
		var out []string
		for _, f := range strings.Fields(rest) {
			out = append(out, strings.Trim(f, `"`))
		}
		return out
	}
	t.Fatalf("%s has no %s=", corpusSetFile, name)
	return nil
}

// The roots of the live closure exist in the tree, and the shared set names exactly them.
func TestAuthCorpusClosureRootsExistAndTheSetNamesThem(t *testing.T) {
	root := authProofRoot(t)
	got := corpusSetValue(t, root, "AUTH_CORPUS_ROOTS")
	if strings.Join(got, " ") != strings.Join(corpusClosureRoots, " ") {
		t.Fatalf("the set's roots %v differ from the ruled roots %v", got, corpusClosureRoots)
	}
	for _, r := range corpusClosureRoots {
		out := corpusRun(t, root, "go", "list", "-f", "{{.ImportPath}}", r)
		if !strings.HasPrefix(strings.TrimSpace(out), corpusModule+"/"+strings.TrimPrefix(r, "./")) {
			t.Errorf("root %s: go list printed %q", r, out)
		}
	}
}

// Every command the corpus images build (the go build loop of docker/go-worker.Dockerfile) is a root.
func TestAuthCorpusClosureRootsCoverTheImageBuildList(t *testing.T) {
	root := authProofRoot(t)
	text := readFile(t, filepath.Join(root, "docker", "go-worker.Dockerfile"))
	i := strings.Index(text, "for command in")
	j := strings.Index(text[i:], "; do")
	if i < 0 || j < 0 {
		t.Fatal("docker/go-worker.Dockerfile has no `for command in ... ; do` build loop")
	}
	roots := map[string]bool{}
	for _, r := range corpusSetValue(t, root, "AUTH_CORPUS_ROOTS") {
		roots[r] = true
	}
	n := 0
	for _, f := range strings.Fields(strings.ReplaceAll(text[i+len("for command in"):i+j], "\\", " ")) {
		n++
		if !roots["./cmd/"+f] {
			t.Errorf("the corpus images build cmd/%s but it is not in AUTH_CORPUS_ROOTS", f)
		}
	}
	if n < 4 {
		t.Errorf("the build loop lists %d commands, want at least 4", n)
	}
}

// ONE source: both scripts source ci/auth_corpus_set.sh; the tested package is the one the test command runs and the one
// the relevance script lists with its test dependencies.
func TestAuthCorpusSetIsTheOneSourceOfTheTestedPackage(t *testing.T) {
	root := authProofRoot(t)
	if got := corpusSetValue(t, root, "AUTH_CORPUS_TEST_PACKAGE"); len(got) != 1 || got[0] != corpusTestPackage {
		t.Fatalf("AUTH_CORPUS_TEST_PACKAGE = %v, want [%s]", got, corpusTestPackage)
	}
	for _, name := range []string{"ci/auth_corpus_external.sh", "ci/auth_corpus_queue_relevance.sh"} {
		text := readFile(t, filepath.Join(root, name))
		if !strings.Contains(text, "auth_corpus_set.sh") {
			t.Errorf("%s does not source ci/auth_corpus_set.sh", name)
		}
		if !strings.Contains(text, "${AUTH_CORPUS_TEST_PACKAGE}") {
			t.Errorf("%s does not use ${AUTH_CORPUS_TEST_PACKAGE}", name)
		}
	}
	ext := readFile(t, filepath.Join(root, "ci/auth_corpus_external.sh"))
	if strings.Contains(ext, "./internal/apiservice/authflowvenue") {
		t.Error("ci/auth_corpus_external.sh still names the tested package literally")
	}
	rel := readFile(t, filepath.Join(root, "ci/auth_corpus_queue_relevance.sh"))
	if !strings.Contains(rel, "-test -tags integration") {
		t.Error("the relevance script does not list the tested package with -test -tags integration")
	}
	out := corpusRun(t, root, "go", "list", "-deps", "-test", "-tags", "integration", "-f", "{{.ImportPath}}", corpusTestPackage)
	for _, must := range []string{"/internal/testsupport/venueoracle", "/internal/testsupport/proofprincipals"} {
		if !strings.Contains(out, corpusModule+must) {
			t.Errorf("go list -deps -test -tags integration %s does not reach %s", corpusTestPackage, must)
		}
	}
}

// CHAOS-11670 ruled the closure live: the checked-in table and its generator do not exist, and nothing names them.
func TestAuthCorpusClosureHasNoTableAndNoGenerator(t *testing.T) {
	root := authProofRoot(t)
	for _, gone := range []string{"ci/auth_corpus_closure.txt", "ci/auth_corpus_closure_gen.sh"} {
		if _, err := os.Stat(filepath.Join(root, gone)); err == nil {
			t.Errorf("%s still exists", gone)
		}
	}
	cmd := exec.Command("git", "grep", "-l", "-e", "auth_corpus_closure", "--", ".", ":(exclude)internal/cigates")
	cmd.Dir = root
	out, err := cmd.Output() // exit status 1 = no match = what we want
	if exit, ok := err.(*exec.ExitError); err != nil && !(ok && exit.ExitCode() == 1) {
		t.Fatalf("git grep: %v", err)
	}
	if strings.TrimSpace(string(out)) != "" {
		t.Errorf("tracked files outside internal/cigates still name the table or the generator:\n%s", out)
	}
}

// corpusDeadRows runs the script's own `case` matching (bash, no extglob, `=` flat-row rule) over files and returns the
// rows that match none of them.
func corpusDeadRows(t *testing.T, rows, files []string) []string {
	t.Helper()
	const prog = `
rows=(); files=()
while IFS= read -r -d '' x; do rows+=("$x"); done < "$1"
while IFS= read -r -d '' x; do files+=("$x"); done < "$2"
for glob in "${rows[@]}"; do
  flat=0; pat=$glob
  if [[ $glob == =* ]]; then flat=1; pat=${glob#=}; fi
  hit=0
  for file in "${files[@]}"; do
    # shellcheck disable=SC2254
    case "$file" in
      ${pat})
        if [ "$flat" = 1 ] && [ "${file%/*}" != "${pat%/*}" ]; then continue; fi
        hit=1; break ;;
    esac
  done
  [ "$hit" = 1 ] || printf '%s\0' "$glob"
done
`
	dir := t.TempDir()
	write := func(name string, items []string) string {
		p := filepath.Join(dir, name)
		if err := writeNUL(p, items); err != nil {
			t.Fatal(err)
		}
		return p
	}
	out := corpusRun(t, dir, "bash", "-c", prog, "bash", write("rows", rows), write("files", files))
	var dead []string
	for _, r := range strings.Split(out, "\x00") {
		if r != "" {
			dead = append(dead, r)
		}
	}
	return dead
}

func TestAuthCorpusPathsRowsMatchATrackedFile(t *testing.T) {
	root := authProofRoot(t)
	files := strings.Split(strings.TrimSuffix(corpusRun(t, root, "git", "-c", "core.quotePath=false", "ls-files", "-z"), "\x00"), "\x00")
	if dead := corpusDeadRows(t, corpusRows(t, root), files); len(dead) > 0 {
		t.Fatalf("rows of %s that match no `git ls-files` path: %q", corpusQueuePaths, dead)
	}
}

func TestAuthCorpusDeadRowCheckSeesEveryDeadForm(t *testing.T) {
	files := []string{"go.mod", "internal/auth/a.go", "internal/audit/b.go", "docker/x.Dockerfile", "-x"}
	live := []string{"go.mod", "internal/auth/*", "=internal/auth/*.go", "docker/*", "internal/audit/b.go"}
	dead := []string{".", "..", "./internal/auth/*", "/internal/auth/*", "internal//auth/*", "internal/auth", "internal/auth/*  # the auth tree",
		"\vgo.mod", "\fgo.mod", "\u00a0go.mod", "\ufeffgo.mod", "internal/{auth,audit}/*", "!(internal)/auth/*", "=internal/*.go", "go.mod/"}
	got := corpusDeadRows(t, append(append([]string{}, live...), dead...), files)
	sort.Strings(got)
	want := append([]string{}, dead...)
	sort.Strings(want)
	if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("dead rows = %q, want %q", got, want)
	}
}

func writeNUL(path string, items []string) error {
	var b bytes.Buffer
	for _, it := range items {
		b.WriteString(it)
		b.WriteByte(0)
	}
	return os.WriteFile(path, b.Bytes(), 0o644)
}
