package docmaketargets_test

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// archivedDocs is the archive tree: kept for the record, never published or followed.
const archivedDocs = ".github/docs-legacy/"

var (
	makefileTarget = regexp.MustCompile(`^([A-Za-z0-9_.\\:-]+):([^=]|$)`)
	fencedMake     = regexp.MustCompile(`^\s*\$?\s*make\s+([A-Za-z][A-Za-z0-9_:.-]*)`)
	inlineCode     = regexp.MustCompile("`([^`]+)`")
)

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above the test directory")
		}
		dir = parent
	}
}

func trackedMarkdown(t *testing.T, root string) []string {
	t.Helper()
	cmd := exec.Command("git", "-C", root, "ls-files", "-z", "--", "*.md")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("git ls-files in %s: %v: %s", root, err, stderr.String())
	}
	var files []string
	for _, rel := range strings.Split(string(out), "\x00") {
		if rel == "" || strings.HasPrefix(rel, archivedDocs) {
			continue
		}
		if _, err := os.Stat(filepath.Join(root, rel)); err == nil {
			files = append(files, rel)
		}
	}
	return files
}

// makefileTargets is every target the Makefile defines, with its escaped colons unescaped.
func makefileTargets(makefile string) map[string]bool {
	targets := map[string]bool{}
	for _, line := range strings.Split(makefile, "\n") {
		if m := makefileTarget.FindStringSubmatch(line); m != nil {
			targets[strings.ReplaceAll(m[1], `\`, "")] = true
		}
	}
	return targets
}

// missingMakeTargets lists every `make <target>` a document gives as a command, in a fenced code block or in inline
// code, whose target the Makefile does not define.
func missingMakeTargets(targets map[string]bool, docs map[string]string) []string {
	var out []string
	for rel, body := range docs {
		fenced := false
		for number, line := range strings.Split(body, "\n") {
			if strings.HasPrefix(strings.TrimSpace(line), "```") {
				fenced = !fenced
				continue
			}
			var named []string
			if fenced {
				if m := fencedMake.FindStringSubmatch(line); m != nil {
					named = append(named, m[1])
				}
			}
			for _, code := range inlineCode.FindAllStringSubmatch(line, -1) {
				if m := fencedMake.FindStringSubmatch(code[1]); m != nil {
					named = append(named, m[1])
				}
			}
			for _, target := range named {
				if !targets[target] {
					out = append(out, rel+":"+strconv.Itoa(number+1)+": make "+target)
				}
			}
		}
	}
	sort.Strings(out)
	return out
}

// Every `make <target>` a published document gives as a command names a target the Makefile defines.
func TestDocsNameOnlyMakeTargetsTheMakefileDefines(t *testing.T) {
	root := repoRoot(t)
	makefile, err := os.ReadFile(filepath.Join(root, "Makefile"))
	if err != nil {
		t.Fatal(err)
	}
	targets := makefileTargets(string(makefile))
	if len(targets) == 0 {
		t.Fatal("no Makefile target parsed: the scan is broken")
	}
	files := trackedMarkdown(t, root)
	if len(files) == 0 {
		t.Fatal("no tracked Markdown file outside the archive: the scan is broken")
	}
	docs := map[string]string{}
	for _, rel := range files {
		raw, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			t.Fatal(err)
		}
		docs[rel] = string(raw)
	}
	for _, missing := range missingMakeTargets(targets, docs) {
		t.Errorf("%s: the Makefile defines no such target", missing)
	}
}

func TestMissingMakeTargetsCatchesPlantedCommands(t *testing.T) {
	targets := makefileTargets("go\\:check:\n\t@./ci/check_go.sh all\n\ntest\\:live-e2e:\n\t@./ci/run_tests.sh live-e2e\nPYTHON ?= python3\n")
	if !targets["go:check"] || !targets["test:live-e2e"] || targets["PYTHON ?= python3"] {
		t.Fatalf("Makefile targets parsed wrong: %v", targets)
	}
	docs := map[string]string{
		"fenced.md": "Run:\n\n```bash\nmake test:unit\n$ make go:check\n```\n",
		"inline.md": "Use `make test:ci` or `make test:live-e2e`.\n",
		"prose.md":  "Make sure you make a habit of it; make test:unit outside code is prose.\n",
	}
	got := missingMakeTargets(targets, docs)
	want := []string{"fenced.md:4: make test:unit", "inline.md:1: make test:ci"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("planted commands: got %v, want %v", got, want)
	}
}
