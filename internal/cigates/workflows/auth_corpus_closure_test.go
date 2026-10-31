package workflows_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
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
	"./internal/testsupport/providerstub/cmd/providerstub", "./internal/testsupport/smtpcapture/cmd/smtpsink",
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

// CHAOS-11735: every `go build` of every image the corpus compose project builds names a command that is a root.
// The corpus images are the Dockerfiles that compose.yml and ci/auth_corpus_external.override.yml name in a build block.
var corpusImageDockerfiles = map[string]int{
	"docker/go-worker.Dockerfile":    4, // the for-loop: dev-health-worker-migrate auth-service auth-migrate dho
	"docker/providerstub.Dockerfile": 1,
	"docker/smtpsink.Dockerfile":     1,
}

// corpusComposeDockerfiles lists every build.dockerfile of the services in the given compose files.
func corpusComposeDockerfiles(t *testing.T, root string, files ...string) []string {
	t.Helper()
	seen := map[string]bool{}
	for _, f := range files {
		doc := parseYAML(t, readFile(t, filepath.Join(root, f)), f)
		services, ok := doc["services"].(yamlMap)
		if !ok {
			t.Fatalf("%s has no services map", f)
		}
		for name, raw := range services {
			svc, ok := raw.(yamlMap)
			if !ok {
				t.Fatalf("%s: service %s is not a map", f, name)
			}
			build, has := svc["build"]
			if !has {
				continue
			}
			b, ok := build.(yamlMap)
			if !ok {
				t.Fatalf("%s: service %s has a build that is not a map; cannot tell its Dockerfile", f, name)
			}
			df, ok := b["dockerfile"].(string)
			if !ok || df == "" {
				t.Fatalf("%s: service %s has a build block without a dockerfile", f, name)
			}
			seen[df] = true
		}
	}
	var out []string
	for d := range seen {
		out = append(out, d)
	}
	sort.Strings(out)
	return out
}

func TestAuthCorpusClosureRootsCoverTheImageBuildList(t *testing.T) {
	root := authProofRoot(t)
	got := corpusComposeDockerfiles(t, root, "compose.yml", "ci/auth_corpus_external.override.yml")
	var want []string
	for d := range corpusImageDockerfiles {
		want = append(want, d)
	}
	sort.Strings(want)
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("the corpus compose files build %v, the test pins %v: a new corpus image must be parsed here", got, want)
	}
	roots := map[string]bool{}
	for _, r := range corpusSetValue(t, root, "AUTH_CORPUS_ROOTS") {
		roots[r] = true
	}
	for _, df := range want {
		builds, err := parseDockerfileBuilds(readFile(t, filepath.Join(root, df)))
		if err != nil {
			t.Fatalf("%s: %v", df, err)
		}
		if len(builds) != corpusImageDockerfiles[df] {
			t.Fatalf("%s: parsed %d go build targets %v, want %d", df, len(builds), builds, corpusImageDockerfiles[df])
		}
		for _, b := range builds {
			if !roots[b.pkg] {
				t.Errorf("%s:%d builds %s but it is not in AUTH_CORPUS_ROOTS", df, b.line, b.pkg)
			}
		}
	}
}

func TestAuthCorpusDockerfileParserTable(t *testing.T) {
	ok := []struct {
		name string
		text string
		want []string
	}{
		{"a loop over a variable, multi-line RUN", "FROM x\nRUN --mount=type=cache,target=/a \\\n    set -eu; \\\n    for command in \\\n        a \\\n        b; do \\\n      GOOS=\"${T}\" go build -trimpath -ldflags=\"-s -w \\\n -X p.V=${V}\" -o \"/out/${command}\" \\\n        \"./cmd/${command}\"; \\\n    done; \\\n    mkdir -p /x\n", []string{"./cmd/a", "./cmd/b"}},
		{"an && chain", "RUN go build -o /out/a ./cmd/a && go build -o /out/b ./cmd/b\n", []string{"./cmd/a", "./cmd/b"}},
		{"a second loop later in the file is read too", "RUN for c in a; do go build ./cmd/$c; done\nRUN for c in b d; do go build -o /o/$c ./cmd/${c}; done\n", []string{"./cmd/a", "./cmd/b", "./cmd/d"}},
		{"a literal target, no loop", "RUN go build -buildvcs=false -o /out/x ./internal/x/cmd/x\n", []string{"./internal/x/cmd/x"}},
		{"a value flag before the target", "RUN go build -tags integration -p 4 -o /out/x ./cmd/x\n", []string{"./cmd/x"}},
		{"comment lines and other instructions", "# go build ./cmd/no\nENV A=go\nRUN echo hi\nRUN echo go build ./cmd/no\n", nil},
		{"no go build at all", "FROM x\nRUN echo hi\n", nil},
		{"go mod download is not a build", "RUN go mod download\n", nil},
	}
	for _, c := range ok {
		t.Run(c.name, func(t *testing.T) {
			got, err := parseDockerfileBuilds(c.text)
			if err != nil {
				t.Fatal(err)
			}
			var pkgs []string
			for _, b := range got {
				pkgs = append(pkgs, b.pkg)
			}
			if strings.Join(pkgs, " ") != strings.Join(c.want, " ") {
				t.Fatalf("got %v, want %v", pkgs, c.want)
			}
		})
	}
	bad := map[string]string{
		"an unset variable":                "RUN go build ./cmd/${other}\n",
		"a build-arg variable":             "ARG C=a\nRUN go build ./cmd/${C}\n",
		"a command substitution":           "RUN go build $(echo ./cmd/a)\n",
		"a backtick":                       "RUN go build `echo ./cmd/a`\n",
		"a heredoc":                        "RUN <<EOF\ngo build ./cmd/a\nEOF\n",
		"the all-packages pattern":         "RUN go build ./...\n",
		"a target without ./":              "RUN go build cmd/a\n",
		"a build without a target":         "RUN go build -o /out/a\n",
		"an unterminated quote":            "RUN go build \"./cmd/a\n",
		"a loop that never ends":           "RUN for c in a; do go build ./cmd/$c\n",
		"a done without a loop":            "RUN go build ./cmd/a; done\n",
		"a loop over a substitution":       "RUN for c in $(ls); do go build ./cmd/$c; done\n",
		"an if around a build":             "RUN if true; then go build ./cmd/a; fi\n",
		"a while around a build":           "RUN while true; do go build ./cmd/a; done\n",
		"a shell -c wrapper":               "RUN sh -c \"go build ./cmd/a\"\n",
		"an exec-form RUN with go":         "RUN [\"go\", \"build\", \"./cmd/a\"]\n",
		"go run":                           "RUN go run ./cmd/a\n",
		"a go test":                        "RUN go test ./cmd/a\n",
		"a shell comment after the target": "RUN go build ./cmd/y # tail\n",
		"a go build with a stray word":     "RUN go build ./cmd/a b\n",
		"a variable inside a target":       "RUN go build ./cmd/a${V}\n",
		"a dot-dot target":                 "RUN go build ../x\n",
	}
	for name, text := range bad {
		t.Run("fails closed: "+name, func(t *testing.T) {
			if got, err := parseDockerfileBuilds(text); err == nil {
				t.Fatalf("want an error, got %v", got)
			}
		})
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
	cmd := exec.Command("git", "-c", "maintenance.auto=false", "-c", "gc.auto=0", "grep", "-l", "-e", "auth_corpus_closure", "--", ".", ":(exclude)internal/cigates")
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

type dockerBuild struct {
	line int
	pkg  string
}

// parseDockerfileBuilds returns every `go build`/`go install` target of every RUN of a Dockerfile text. It fails closed:
// a RUN it cannot read to the end (a heredoc, an exec form that names go, a command substitution, a backtick, an
// unterminated quote, a shell comment, a control form other than `for NAME in WORDS; do ... done`, a go run or go test,
// a target that is not a plain ./path after the loop variable is expanded) is an error, never a silent skip.
func parseDockerfileBuilds(text string) ([]dockerBuild, error) {
	var out []dockerBuild
	lines := strings.Split(text, "\n")
	for i := 0; i < len(lines); i++ {
		first := i + 1
		trimmed := strings.TrimSpace(lines[i])
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		logical := trimmed
		for strings.HasSuffix(logical, "\\") {
			logical = strings.TrimSpace(strings.TrimSuffix(logical, "\\"))
			i++
			if i >= len(lines) {
				break
			}
			next := strings.TrimSpace(lines[i])
			if strings.HasPrefix(next, "#") {
				next = ""
			}
			logical += " " + next
			logical = strings.TrimSpace(logical)
		}
		word, rest, _ := strings.Cut(logical, " ")
		if !strings.EqualFold(word, "RUN") {
			continue
		}
		builds, err := parseRunBuilds(rest, first)
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", first, err)
		}
		out = append(out, builds...)
	}
	return out, nil
}

// goCommandWord: a RUN with no go build/install/run/test/generate word has nothing to parse (a bounded retry of go mod download, mkdir).
var goCommandWord = regexp.MustCompile(`(^|[^A-Za-z0-9_./-])go\s+(build|install|run|test|generate)(\s|$)`)

type shellLoop struct {
	name   string
	words  []string
	active bool
}

func parseRunBuilds(body string, line int) ([]dockerBuild, error) {
	body = strings.TrimSpace(body)
	for strings.HasPrefix(body, "--") {
		_, rest, _ := strings.Cut(body, " ")
		body = strings.TrimSpace(rest)
	}
	if strings.HasPrefix(body, "[") {
		if strings.Contains(body, "go") {
			return nil, fmt.Errorf("an exec-form RUN that names go cannot be parsed: %s", body)
		}
		return nil, nil
	}
	if strings.Contains(body, "<<") {
		return nil, fmt.Errorf("a heredoc RUN cannot be parsed")
	}
	if !goCommandWord.MatchString(body) {
		return nil, nil
	}
	stmts, err := shellStatements(body)
	if err != nil {
		return nil, err
	}
	var loops []shellLoop
	var out []dockerBuild
	for _, s := range stmts {
		switch s[0] {
		case "if", "then", "elif", "else", "fi", "while", "until", "case", "esac", "select", "function", "{", "}", "(", "!":
			return nil, fmt.Errorf("the control form %q is not parsed", s[0])
		case "for":
			if len(s) < 4 || s[2] != "in" || strings.ContainsAny(s[1], "$=") {
				return nil, fmt.Errorf("a for loop that is not `for NAME in WORDS`: %v", s)
			}
			for _, w := range s[3:] {
				if w == "" || strings.ContainsAny(w, "$*?[{~") {
					return nil, fmt.Errorf("a for loop over a word that is not plain: %q", w)
				}
			}
			loops = append(loops, shellLoop{name: s[1], words: s[3:]})
			continue
		case "do":
			if len(loops) == 0 || loops[len(loops)-1].active {
				return nil, fmt.Errorf("a do without a for")
			}
			loops[len(loops)-1].active = true
			s = s[1:]
			if len(s) == 0 {
				continue
			}
		case "done":
			if len(loops) == 0 || !loops[len(loops)-1].active || len(s) != 1 {
				return nil, fmt.Errorf("a done without a loop")
			}
			loops = loops[:len(loops)-1]
			continue
		}
		for len(s) > 0 && strings.Contains(s[0], "=") && strings.Index(s[0], "=") > 0 && !strings.ContainsAny(s[0][:strings.Index(s[0], "=")], "/$-") {
			s = s[1:]
		}
		if len(s) == 0 {
			continue
		}
		if filepath.Base(s[0]) != "go" {
			for _, w := range s[1:] {
				if strings.Contains(w, "go build") || strings.Contains(w, "go install") || strings.Contains(w, "go run") {
					return nil, fmt.Errorf("a go command inside the argument of %q cannot be parsed", s[0])
				}
			}
			continue
		}
		if len(s) < 2 {
			continue
		}
		switch s[1] {
		case "build", "install":
		case "run", "test", "generate":
			return nil, fmt.Errorf("go %s in a RUN is not parsed", s[1])
		default:
			continue
		}
		var pkgs []string
		valueFlags := map[string]bool{"-o": true, "-ldflags": true, "-tags": true, "-gcflags": true, "-asmflags": true, "-p": true, "-mod": true, "-modfile": true, "-C": true, "-toolexec": true, "-overlay": true, "-pkgdir": true, "-compiler": true, "-gccgoflags": true, "-installsuffix": true}
		for k := 2; k < len(s); k++ {
			a := s[k]
			if strings.HasPrefix(a, "-") {
				if valueFlags[a] {
					k++
				}
				continue
			}
			pkgs = append(pkgs, a)
		}
		if len(pkgs) != 1 {
			return nil, fmt.Errorf("go %s with %d package words %v, want exactly one", s[1], len(pkgs), pkgs)
		}
		expanded, err := expandLoopWord(pkgs[0], loops)
		if err != nil {
			return nil, err
		}
		for _, p := range expanded {
			if !strings.HasPrefix(p, "./") || strings.ContainsAny(p, "$*") || strings.Contains(p, "..") || strings.HasSuffix(p, "/") {
				return nil, fmt.Errorf("go build target %q is not a plain ./path", p)
			}
			out = append(out, dockerBuild{line: line, pkg: p})
		}
	}
	if len(loops) > 0 {
		return nil, fmt.Errorf("a for loop without its done")
	}
	return out, nil
}

// expandLoopWord replaces ${NAME} / $NAME of the innermost active loop by each of its words; any other $ is an error.
func expandLoopWord(word string, loops []shellLoop) ([]string, error) {
	if !strings.Contains(word, "$") {
		return []string{word}, nil
	}
	for k := len(loops) - 1; k >= 0; k-- {
		l := loops[k]
		if !l.active {
			continue
		}
		var out []string
		for _, w := range l.words {
			r := strings.NewReplacer("${"+l.name+"}", w, "$"+l.name, w).Replace(word)
			out = append(out, r)
		}
		if len(out) > 0 && !strings.Contains(out[0], "$") {
			return out, nil
		}
	}
	return nil, fmt.Errorf("go build target %q uses a variable that is not a loop variable of the enclosing for", word)
}

// shellStatements splits a RUN body into statements of words (quotes removed), on ; && || | & and newlines outside quotes.
func shellStatements(body string) ([][]string, error) {
	var stmts [][]string
	var cur []string
	var word strings.Builder
	inWord := false
	flushWord := func() {
		if inWord {
			cur = append(cur, word.String())
			word.Reset()
			inWord = false
		}
	}
	flushStmt := func() {
		flushWord()
		if len(cur) > 0 {
			stmts = append(stmts, cur)
			cur = nil
		}
	}
	rs := []rune(body)
	for i := 0; i < len(rs); i++ {
		c := rs[i]
		switch {
		case c == '`':
			return nil, fmt.Errorf("a backtick substitution cannot be parsed")
		case c == '$' && i+1 < len(rs) && rs[i+1] == '(':
			return nil, fmt.Errorf("a $( substitution cannot be parsed")
		case c == '\'':
			j := i + 1
			for j < len(rs) && rs[j] != '\'' {
				j++
			}
			if j >= len(rs) {
				return nil, fmt.Errorf("an unterminated single quote")
			}
			inWord = true
			word.WriteString(string(rs[i+1 : j]))
			i = j
		case c == '"':
			j := i + 1
			inWord = true
			for j < len(rs) && rs[j] != '"' {
				switch {
				case rs[j] == '`' || (rs[j] == '$' && j+1 < len(rs) && rs[j+1] == '('):
					return nil, fmt.Errorf("a substitution inside double quotes cannot be parsed")
				case rs[j] == '\\' && j+1 < len(rs):
					j++
				}
				word.WriteRune(rs[j])
				j++
			}
			if j >= len(rs) {
				return nil, fmt.Errorf("an unterminated double quote")
			}
			i = j
		case c == '\\':
			if i+1 >= len(rs) {
				return nil, fmt.Errorf("a trailing backslash")
			}
			i++
			inWord = true
			word.WriteRune(rs[i])
		case c == '#' && !inWord:
			return nil, fmt.Errorf("a shell comment in a RUN cannot be parsed")
		case c == ' ' || c == '\t':
			flushWord()
		case c == ';' || c == '&' || c == '|' || c == '\n':
			flushStmt()
		default:
			inWord = true
			word.WriteRune(c)
		}
	}
	flushStmt()
	return stmts, nil
}
