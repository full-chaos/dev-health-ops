package workflows_test

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// Inputs of the Go guards in internal/goapicli/routing (docs_guards_test.go): the SDL and the digest pin, the history doc the
// digest must be a row of, the runbook whose enable commands are checked, and the verb source whose flags define the vocabulary.
// A pull request that changes only one of them must run the Go tests, so each one must match go.yml's own push and pull_request
// path filters. The doc and the runbook are Markdown, which the path-filter scan of Go string literals does not read.
var routingGuardInputs = []string{
	"contracts/graphql/v1/schema.graphql",
	"contracts/graphql/v1/schema-digest.json",
	"docs/contribute/architecture/go-api-wave-0-proof-infrastructure.md",
	"docs/operate/runbooks/query-api-bootstrap.md",
	"internal/goapicli/routing/enable.go",
}

func filterMisses(t *testing.T, files, patterns []string) []string {
	t.Helper()
	var missed []string
	for _, f := range files {
		if !matchesAny(t, f, patterns) {
			missed = append(missed, f)
		}
	}
	return missed
}

func TestRoutingGuardInputsAreInGoYmlPathFilters(t *testing.T) {
	root := repoRoot(t)
	for _, event := range []string{"push", "pull_request"} {
		patterns := goPaths(t, root, event)
		if len(patterns) == 0 {
			t.Fatalf("go.yml declares no %s paths", event)
		}
		if missed := filterMisses(t, routingGuardInputs, patterns); len(missed) != 0 {
			t.Errorf("go.yml's %s path filter does not select inputs of the routing docs guards: %v", event, missed)
		}
	}
}

// Planted defect: a filter without the history doc must be reported, and one that covers every input must not be.
func TestRoutingGuardInputsPlantedDefect(t *testing.T) {
	without := []string{"**/*.go", "contracts/**", "docs/operate/runbooks/query-api-bootstrap.md"}
	missed := filterMisses(t, routingGuardInputs, without)
	if len(missed) != 1 || missed[0] != "docs/contribute/architecture/go-api-wave-0-proof-infrastructure.md" {
		t.Fatalf("a filter without the history doc reported %v, want exactly the history doc", missed)
	}
	with := append([]string{"docs/contribute/architecture/go-api-wave-0-proof-infrastructure.md"}, without...)
	if missed := filterMisses(t, routingGuardInputs, with); len(missed) != 0 {
		t.Fatalf("a filter naming every input still reported %v", missed)
	}
}

// The bare-verb scans in internal/goapicli/routing read every file with a scanned extension under a scanned root. Roots and
// extensions are read from the declarations the scan itself uses (bareScanRoots, bareScanExtensions in docs_guards_test.go),
// not from a list kept here, so a root or extension added to the scan without a path filter fails this test.
// docs/ is named parity: the Python test it replaces ran for docs/** edits only through its own tests/ and ci/ paths, never
// on a docs-only change, and go.yml selects only the docs files Go tests name.
var scanDeclRoots = regexp.MustCompile(`(?s)var bareScanRoots = \[\]string\{([^}]*)\}`)
var scanDeclExts = regexp.MustCompile(`(?s)var bareScanExtensions = map\[string\]bool\{([^}]*)\}`)
var quoted = regexp.MustCompile(`"([^"]+)"`)

func scanInputs(t *testing.T, root string) (roots, exts []string) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, "internal", "goapicli", "routing", "docs_guards_test.go"))
	if err != nil {
		t.Fatal(err)
	}
	r, e := scanDeclRoots.FindSubmatch(raw), scanDeclExts.FindSubmatch(raw)
	if r == nil || e == nil {
		t.Fatal("the scan declarations (bareScanRoots, bareScanExtensions) were not found: this guard would derive nothing")
	}
	for _, m := range quoted.FindAllSubmatch(r[1], -1) {
		roots = append(roots, string(m[1]))
	}
	for _, m := range quoted.FindAllSubmatch(e[1], -1) {
		exts = append(exts, string(m[1]))
	}
	sort.Strings(roots)
	sort.Strings(exts)
	if len(roots) == 0 || len(exts) == 0 {
		t.Fatalf("empty scan declarations: roots %v, extensions %v", roots, exts)
	}
	return roots, exts
}

// scanFilterMisses returns every root/extension pair, as a file directly under the root and as a file in a nested directory,
// that the patterns do not select. `.go` is covered by **/*.go and docs/ is named parity.
func scanFilterMisses(t *testing.T, roots, exts, patterns []string) []string {
	t.Helper()
	var missed []string
	for _, r := range roots {
		if r == "docs" {
			continue
		}
		for _, e := range exts {
			if e == ".go" {
				continue
			}
			for _, p := range []string{r + "/zz" + e, r + "/aa/bb/zz" + e} {
				if !matchesAny(t, p, patterns) {
					missed = append(missed, p)
				}
			}
		}
	}
	return missed
}

func TestBareVerbScanInputsAreInGoYmlPathFilters(t *testing.T) {
	root := repoRoot(t)
	roots, exts := scanInputs(t, root)
	if !strings.Contains(strings.Join(roots, ","), "ci") || !strings.Contains(strings.Join(exts, ","), ".sh") {
		t.Fatalf("the derived scan inputs look wrong: roots %v, extensions %v", roots, exts)
	}
	for _, event := range []string{"push", "pull_request"} {
		if missed := scanFilterMisses(t, roots, exts, goPaths(t, root, event)); len(missed) != 0 {
			t.Errorf("go.yml's %s path filter does not select files the bare-verb scans read: %v", event, missed)
		}
	}
}

func TestBareVerbScanInputsPlantedDefect(t *testing.T) {
	roots, exts := []string{"ci", "cmd", "docs"}, []string{".go", ".md", ".sh"}
	full := []string{"**/*.go", "ci/**/*.md", "ci/**/*.sh", "cmd/**/*.md", "cmd/**/*.sh"}
	if missed := scanFilterMisses(t, roots, exts, full); len(missed) != 0 {
		t.Fatalf("a filter naming every root and extension reported %v", missed)
	}
	lacking := []string{"**/*.go", "ci/**/*.md", "cmd/**/*.md", "cmd/**/*.sh"}
	missed := scanFilterMisses(t, roots, exts, lacking)
	if len(missed) != 2 || missed[0] != "ci/zz.sh" || missed[1] != "ci/aa/bb/zz.sh" {
		t.Fatalf("a filter without ci/**/*.sh reported %v, want the two ci .sh probes", missed)
	}
}
