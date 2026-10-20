package testshard_test

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// CHAOS-8296: every go build/test/vet/run/install call of the CI scripts, workflows and Dockerfiles carries -trimpath, and
// ci/check_go.sh calls also name -mod=readonly. Go port of section 5 of tests/tooling/test_go_quality_legs.py. The scanners
// take text so the planted tests can feed them mutated copies; no test edits a real tree file.

var trimpathGoCall = regexp.MustCompile("(?:^|[\\s;&(|`])go (?:build|test|vet|run|install)(?:\\s|$)")

// Places a `go <verb>` appears that are not a call this repo makes with the module's flags, each with the reason.
// A new entry is a decision, not a convenience.
var trimpathExceptions = []struct{ file, prefix, reason string }{
	{".github/workflows/arc-runner-image.yml", "go run /tmp/cgo_probe.go",
		"one probe file outside the module, run to check cgo on the runner image"},
	{"ci/check_migration_matrix.sh", `go run ./cmd/dev-health-migration-matrix -render -root ."`,
		"the text of the re-verify hint a human reads (a multi-line message), not a call"},
}

type trimpathCall struct{ file, command string }

func trimpathIsExcepted(file, command string) bool {
	for _, e := range trimpathExceptions {
		if file == e.file && strings.HasPrefix(command, e.prefix) {
			return true
		}
	}
	return false
}

// trimpathFiles is the set the Python walked: ci/**/*.sh, workflows, docker Dockerfiles, scripts/**/*.sh, plus ci/check_go.sh.
func trimpathFiles(t *testing.T) []string {
	t.Helper()
	root := repoRoot(t)
	set := map[string]bool{"ci/check_go.sh": true}
	walk := func(dir string, match func(name string) bool) {
		_ = filepath.WalkDir(filepath.Join(root, dir), func(p string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || !match(d.Name()) {
				return nil
			}
			rel, _ := filepath.Rel(root, p)
			set[filepath.ToSlash(rel)] = true
			return nil
		})
	}
	sh := func(n string) bool { return strings.HasSuffix(n, ".sh") }
	walk("ci", sh)
	walk("scripts", sh)
	walk("docker", func(n string) bool { return strings.HasPrefix(n, "Dockerfile") || strings.Contains(n, "Dockerfile") })
	if m, _ := filepath.Glob(filepath.Join(root, ".github", "workflows", "*.yml")); m != nil {
		for _, p := range m {
			rel, _ := filepath.Rel(root, p)
			set[filepath.ToSlash(rel)] = true
		}
	}
	var out []string
	for f := range set {
		out = append(out, f)
	}
	sort.Strings(out)
	return out
}

// trimpathUnquoteSubstitutions drops the double quotes around each `"$( ... )"` of a line (closing paren matched by depth).
func trimpathUnquoteSubstitutions(line string) string {
	var out strings.Builder
	i := 0
	for i < len(line) {
		if strings.HasPrefix(line[i:], `"$(`) {
			depth := 0
			j := i + 1
			for j < len(line) {
				if strings.HasPrefix(line[j:], "$(") {
					depth++
					j += 2
					continue
				}
				if line[j] == '(' {
					depth++
				} else if line[j] == ')' {
					depth--
					if depth == 0 {
						break
					}
				}
				j++
			}
			if depth == 0 && j+1 < len(line) && line[j+1] == '"' {
				out.WriteString(line[i+1 : j+1])
				i = j + 2
				continue
			}
		}
		out.WriteByte(line[i])
		i++
	}
	return out.String()
}

var (
	trimpathUsage    = regexp.MustCompile(`(?ms)^usage\(\) \{.*?^\}\n`)
	trimpathComment  = regexp.MustCompile(`(?m)^\s*#.*$`)
	trimpathYamlText = regexp.MustCompile(`(?m)^\s*-?\s*(?:name|description|summary):.*$`)
	trimpathContinue = regexp.MustCompile(`\\\n\s*`)
	trimpathQuoted   = regexp.MustCompile(`"(?:[^"\\]|\\.)*"|'[^']*'`)
	trimpathCmdEnd   = regexp.MustCompile(`&&|\|\||;|\||\)`)
)

// trimpathCallsInText derives every go verb call of one file's text: comments, YAML names, usage text and quoted strings
// are removed first, a backslash continuation joins lines. Same named limits as the Python (quoted run: scalars,
// bash -c strings, go through a path or variable, files outside the file set).
func trimpathCallsInText(file, text string) []trimpathCall {
	text = trimpathUsage.ReplaceAllString(text, "")
	text = trimpathComment.ReplaceAllString(text, "")
	text = trimpathYamlText.ReplaceAllString(text, "")
	text = trimpathContinue.ReplaceAllString(text, " ")
	var calls []trimpathCall
	for _, line := range strings.Split(text, "\n") {
		line = trimpathUnquoteSubstitutions(line)
		unquoted := trimpathQuoted.ReplaceAllString(line, `""`)
		for _, m := range trimpathGoCall.FindAllStringIndex(unquoted, -1) {
			rest := unquoted[m[0]:]
			if loc := trimpathCmdEnd.FindStringIndex(rest); loc != nil {
				rest = rest[:loc[0]]
			}
			calls = append(calls, trimpathCall{file, strings.TrimSpace(rest)})
		}
	}
	return calls
}

func trimpathRealCalls(t *testing.T) []trimpathCall {
	t.Helper()
	root := repoRoot(t)
	var calls []trimpathCall
	for _, f := range trimpathFiles(t) {
		b, err := os.ReadFile(filepath.Join(root, f))
		if err != nil {
			t.Fatal(err)
		}
		calls = append(calls, trimpathCallsInText(f, string(b))...)
	}
	return calls
}

// trimpathMissing lists the calls without -trimpath that are not excepted.
func trimpathMissing(calls []trimpathCall) []string {
	var missing []string
	for _, c := range calls {
		if !strings.Contains(c.command, "-trimpath") && !trimpathIsExcepted(c.file, c.command) {
			missing = append(missing, fmt.Sprintf("%s: %s", c.file, trimpathHead(c.command, 90)))
		}
	}
	return missing
}

// trimpathStale lists exception entries no call matches any more.
func trimpathStale(calls []trimpathCall) []string {
	var stale []string
	for _, e := range trimpathExceptions {
		found := false
		for _, c := range calls {
			if c.file == e.file && strings.HasPrefix(c.command, e.prefix) {
				found = true
			}
		}
		if !found {
			stale = append(stale, e.file+": "+e.prefix)
		}
	}
	return stale
}

func trimpathHead(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

// trimpathCheckGoProblems: every derived check_go.sh call names -mod=readonly and -trimpath, and no line that holds a go verb
// and -mod=readonly is one the derivation missed.
func trimpathCheckGoProblems(text string) []string {
	var problems []string
	var derived []string
	for _, c := range trimpathCallsInText("ci/check_go.sh", text) {
		derived = append(derived, c.command)
		if !strings.Contains(c.command, "-mod=readonly") {
			problems = append(problems, "check_go.sh call without -mod=readonly: "+trimpathHead(c.command, 90))
		}
		if !strings.Contains(c.command, "-trimpath") {
			problems = append(problems, "check_go.sh call without -trimpath: "+trimpathHead(c.command, 90))
		}
	}
	if len(derived) < 15 {
		problems = append(problems, fmt.Sprintf("only %d calls derived from check_go.sh", len(derived)))
	}
	for i, raw := range strings.Split(text, "\n") {
		line := strings.TrimSpace(raw)
		loc := trimpathGoCall.FindStringIndex(line)
		if strings.HasPrefix(line, "#") || loc == nil || !strings.Contains(line, "-mod=readonly") {
			continue
		}
		if strings.Contains(line, "printf") || strings.Contains(line, "die ") {
			continue
		}
		head := trimpathHead(strings.TrimSpace(line[loc[0]:]), 25)
		seen := false
		for _, d := range derived {
			if strings.Contains(d, head) {
				seen = true
			}
		}
		if !seen {
			problems = append(problems, fmt.Sprintf("check_go.sh line %d holds a go call the derivation missed: %s", i+1, trimpathHead(line, 90)))
		}
	}
	return problems
}

// trimpathRaceProblems checks the recorded stand-in calls all carry -trimpath.
func trimpathRaceProblems(calls []string) []string {
	if len(calls) == 0 {
		return []string{"no go test call was recorded"}
	}
	var bad []string
	for _, c := range calls {
		if !strings.Contains(" "+c+" ", " -trimpath ") {
			bad = append(bad, c)
		}
	}
	return bad
}

func TestEveryGoBuildTestVetRunCallCarriesTrimpath(t *testing.T) {
	t.Parallel()
	calls := trimpathRealCalls(t)
	if len(calls) < 35 {
		t.Fatalf("the derivation found only %d go calls", len(calls))
	}
	if m := trimpathMissing(calls); len(m) > 0 {
		t.Fatalf("go call(s) without -trimpath:\n%s", strings.Join(m, "\n"))
	}
	if s := trimpathStale(calls); len(s) > 0 {
		t.Fatalf("exception entries no call matches any more: %v", s)
	}
}

func TestEveryGoBuildTestVetRunCallCarriesTrimpathPlanted(t *testing.T) {
	t.Parallel()
	good := "go build -trimpath ./...\ngo test -trimpath -race ./..."
	if m := trimpathMissing(trimpathCallsInText("ci/x.sh", good)); len(m) != 0 {
		t.Fatalf("clean text flagged: %v", m)
	}
	for name, bad := range map[string]string{
		"plain":        "go build ./...",
		"in-subshell":  `out="$(cd x && go vet ./...)"`,
		"continuation": "go test \\\n  -race ./...",
		"yaml-run":     "      - run: go run ./cmd/x",
	} {
		m := trimpathMissing(trimpathCallsInText("ci/x.sh", bad))
		if len(m) != 1 {
			t.Errorf("%s: want 1 missing -trimpath, got %v", name, m)
		}
	}
	// A quoted message and a comment are not calls.
	if c := trimpathCallsInText("ci/x.sh", "# go build ./...\necho \"run go test ./...\""); len(c) != 0 {
		t.Errorf("comment/message read as calls: %v", c)
	}
	// An exception only covers its own file and prefix; a stale entry is reported.
	e := trimpathExceptions[0]
	if m := trimpathMissing([]trimpathCall{{e.file, e.prefix}}); len(m) != 0 {
		t.Errorf("excepted call flagged: %v", m)
	}
	if m := trimpathMissing([]trimpathCall{{"ci/other.sh", e.prefix}}); len(m) != 1 {
		t.Errorf("exception leaked to another file: %v", m)
	}
	if s := trimpathStale(nil); len(s) != len(trimpathExceptions) {
		t.Errorf("stale exceptions not reported: %v", s)
	}
}

func TestEveryGoCallOfCheckGoNamesModReadonlyAndTrimpath(t *testing.T) {
	t.Parallel()
	b, err := os.ReadFile(filepath.Join(repoRoot(t), "ci", "check_go.sh"))
	if err != nil {
		t.Fatal(err)
	}
	if p := trimpathCheckGoProblems(string(b)); len(p) > 0 {
		t.Fatal(strings.Join(p, "\n"))
	}
}

func TestEveryGoCallOfCheckGoNamesModReadonlyAndTrimpathPlanted(t *testing.T) {
	t.Parallel()
	b, err := os.ReadFile(filepath.Join(repoRoot(t), "ci", "check_go.sh"))
	if err != nil {
		t.Fatal(err)
	}
	real := string(b)
	// Plant one call per flag loss, then a go line the derivation cannot see.
	noMod := strings.Replace(real, "-mod=readonly", "", 1)
	noTrim := strings.Replace(real, "-trimpath", "", 1)
	if noMod == real || noTrim == real {
		t.Fatal("check_go.sh has no flag to mutate")
	}
	if p := trimpathCheckGoProblems(noMod); len(p) == 0 {
		t.Error("a check_go.sh call that lost -mod=readonly was not caught")
	}
	if p := trimpathCheckGoProblems(noTrim); len(p) == 0 {
		t.Error("a check_go.sh call that lost -trimpath was not caught")
	}
	hidden := real + "\necho \"a go vet -mod=readonly ./hidden\"\n"
	if p := trimpathCheckGoProblems(hidden); len(p) == 0 {
		t.Error("a go call hidden from the derivation was not caught")
	}
	if p := trimpathCheckGoProblems("go vet -mod=readonly -trimpath ./...\n"); len(p) == 0 {
		t.Error("a file with too few derived calls was not caught")
	}
}

func TestTheStandInSeesTrimpathOnEveryRecordedRaceCall(t *testing.T) {
	t.Parallel()
	r := runCheckGo(t, nil, false, "ci-leg", "race", "1", "3")
	if r.Err != nil {
		t.Fatalf("%v\n%s", r.Err, trimpathHead(r.Stderr, 1500))
	}
	if bad := trimpathRaceProblems(r.Calls); len(bad) > 0 {
		t.Fatalf("race call(s) without -trimpath or none recorded: %v", bad)
	}
}

func TestTheStandInSeesTrimpathOnEveryRecordedRaceCallPlanted(t *testing.T) {
	t.Parallel()
	if p := trimpathRaceProblems([]string{"-mod=readonly -trimpath -race ./a", "-race -trimpath ./b"}); len(p) != 0 {
		t.Errorf("clean calls flagged: %v", p)
	}
	if p := trimpathRaceProblems([]string{"-mod=readonly -trimpath -race ./a", "-mod=readonly -race ./b"}); len(p) != 1 {
		t.Errorf("a race call lacking -trimpath was not caught: %v", p)
	}
	if p := trimpathRaceProblems([]string{"-race -trimpathx ./b"}); len(p) != 1 {
		t.Errorf("a lookalike flag passed: %v", p)
	}
	if p := trimpathRaceProblems(nil); len(p) == 0 {
		t.Error("no recorded call was not caught")
	}
}
