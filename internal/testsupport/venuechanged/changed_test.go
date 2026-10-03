// Package venuechanged pins the pull_request form of the venue oracles
// (CHAOS-7653): `ci/check_go.sh venue-oracles --changed FILE --list` selects the
// registry rows of the packages the changed paths lie in, on whole path segments,
// and the workflow runs the heavy steps only when something is selected.
package venuechanged

import (
	"bytes"
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

// selected runs the verb over the given changed paths (joined by sep) and returns the
// SELECT rows as "package\ttest".
func selected(t *testing.T, root, sep string, paths ...string) ([]string, error, string) {
	t.Helper()
	file := filepath.Join(t.TempDir(), "changed.txt")
	if err := os.WriteFile(file, []byte(strings.Join(paths, sep)+sep), 0o644); err != nil {
		t.Fatal(err)
	}
	return selectedFrom(t, root, file)
}

func selectedFrom(t *testing.T, root, file string) ([]string, error, string) {
	t.Helper()
	command := exec.Command("bash", filepath.Join(root, "ci", "check_go.sh"), "venue-oracles", "--changed", file, "--list")
	command.Dir = root
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	err := command.Run()
	var rows []string
	for _, line := range strings.Split(stdout.String(), "\n") {
		if rest, ok := strings.CutPrefix(line, "SELECT\t"); ok {
			rows = append(rows, rest)
		}
	}
	return rows, err, stderr.String()
}

// registryRows are the run rows of one package's registry file.
func registryRows(t *testing.T, root, pkg string) []string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, "ci", "venue_oracle_registry.d", strings.ReplaceAll(pkg, "/", "__")+".tsv"))
	if err != nil {
		t.Fatal(err)
	}
	var rows []string
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Split(line, "\t")
		if len(fields) == 3 && fields[2] == "run" {
			rows = append(rows, fields[0]+"\t"+fields[1])
		}
	}
	sort.Strings(rows)
	return rows
}

func TestAChangeInAVenuePackageSelectsThatPackagesTestsByName(t *testing.T) {
	root := repoRoot(t)
	const pkg = "internal/apiservice/billingvenue"
	want := registryRows(t, root, pkg)
	if len(want) == 0 {
		t.Fatalf("%s has no registered run row: the test would be vacuous", pkg)
	}
	for _, sep := range []string{"\x00", "\n"} {
		rows, err, stderr := selected(t, root, sep, pkg+"/billing_edge_venue_oracle_integration_test.go", "README.md")
		if err != nil {
			t.Fatalf("--list failed: %v\n%s", err, stderr)
		}
		sort.Strings(rows)
		if strings.Join(rows, "|") != strings.Join(want, "|") {
			t.Fatalf("separator %q: selected\n%v\nwant\n%v", sep, rows, want)
		}
	}
}

func TestAChangeInASubPackageDoesNotSelectItsParentAndAGoldenSelectsItsPackage(t *testing.T) {
	root := repoRoot(t)
	rows, err, stderr := selected(t, root, "\x00", "internal/apiservice/billingvenue/x.go")
	if err != nil {
		t.Fatalf("--list failed: %v\n%s", err, stderr)
	}
	for _, row := range rows {
		if strings.HasPrefix(row, "internal/apiservice\t") {
			t.Fatalf("a change in the billingvenue sub-package selected a row of its parent package: %s", row)
		}
	}
	golden, err, stderr := selected(t, root, "\x00", "internal/apiservice/billingvenue/testdata/golden/billing-edge.json")
	if err != nil || len(golden) != len(rows) || len(golden) == 0 {
		t.Fatalf("a golden under testdata selected %d rows, the package's own file %d (err %v)\n%s", len(golden), len(rows), err, stderr)
	}
	parentFile, err, stderr := selected(t, root, "\x00", "internal/apiservice/x.go")
	if err != nil || len(parentFile) == 0 {
		t.Fatalf("a file directly in internal/apiservice selected nothing (err %v)\n%s", err, stderr)
	}
}

func TestAnUnrelatedChangeSelectsNothing(t *testing.T) {
	root := repoRoot(t)
	rows, err, stderr := selected(t, root, "\x00", "README.md", "docs/index.md", "internal/not-a-venue-package/x.go", "ci/check_go.sh")
	if err != nil || len(rows) != 0 {
		t.Fatalf("an unrelated change selected %v (err %v)\n%s", rows, err, stderr)
	}
}

func TestSelectionIsOnWholePathSegments(t *testing.T) {
	root := repoRoot(t)
	if len(registryRows(t, root, "internal/apiservice/admin")) == 0 {
		t.Fatal("internal/apiservice/admin has no registered row: the boundary check would be vacuous")
	}
	rows, err, stderr := selected(t, root, "\x00", "internal/apiservice/adminllmvenue/x.go")
	if err != nil {
		t.Fatalf("--list failed: %v\n%s", err, stderr)
	}
	for _, row := range rows {
		if strings.HasPrefix(row, "internal/apiservice/admin\t") {
			t.Fatalf("a change in adminllmvenue selected a row of the admin package: %s", row)
		}
	}
	if len(rows) == 0 {
		t.Fatal("a change in adminllmvenue selected nothing: the package is registered, so its own rows must be selected")
	}
}

func TestAMissingChangedFileFailsLoudly(t *testing.T) {
	root := repoRoot(t)
	_, err, stderr := selectedFrom(t, root, filepath.Join(t.TempDir(), "nope.txt"))
	if err == nil || !strings.Contains(stderr, "missing or unreadable") {
		t.Fatalf("a missing changed-file list was accepted: %v\n%s", err, stderr)
	}
}

func TestTheWorkflowRunsTheHeavyStepsOnlyWhenSomethingIsSelected(t *testing.T) {
	root := repoRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, ".github", "workflows", "venue-oracles-changed.yml"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	if !strings.Contains(text, "pull_request:") || strings.Contains(text, "push:") || strings.Contains(text, "workflow_dispatch:") {
		t.Fatal("the changed-packages workflow must trigger on pull_request only (the full suite stays in venue-oracles.yml, main and by hand)")
	}
	if !strings.Contains(text, "ci/check_go.sh venue-oracles --changed changed.txt --list") || !strings.Contains(text, "ci/check_go.sh venue-oracles --changed changed.txt 2>&1") {
		t.Fatal("the workflow does not select and then run through the verb")
	}
	const condition = "if: steps.select.outputs.run == 'true'"
	// Go, registry login, image pre-pull, the run, and the upload (no Python is installed).
	if got := strings.Count(text, condition); got != 5 {
		t.Fatalf("%d heavy steps carry the selection condition, want 5 (setup-go, login, pre-pull, run, upload)", got)
	}
	if !strings.Contains(text, "if: steps.select.outputs.run == 'true' && always()") {
		t.Fatal("the receipt upload is not gated on the selection")
	}
	if strings.Contains(text, "required") && strings.Contains(strings.ToLower(text), "required check: yes") {
		t.Fatal("the job is informational (R430)")
	}
}
