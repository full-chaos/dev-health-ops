// Package venuechanged pins the pull_request form of the venue oracles
// (CHAOS-7653): `ci/check_go.sh venue-oracles --changed FILE --list` selects the
// registry rows of the packages the changed paths lie in, on whole path segments,
// and the workflow runs the heavy steps only when something is selected.
package venuechanged

import (
	"bytes"
	"fmt"
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

// TestAChangeInAPackageTheVenueServerLinksSelectsTheTestsThatStartIt is the acceptance of CHAOS-9935:
// internal/api/policy is linked by the production api server the browse oracles of internal/apiservice
// start, and no hand-written trigger row names it for them (PR 4186 changed it and the pull request job
// selected only TestVenueOracleBillingLedger; main went red after the merge). The dependency closure
// selects them.
func TestAChangeInAPackageTheVenueServerLinksSelectsTheTestsThatStartIt(t *testing.T) {
	root := repoRoot(t)
	rows, err, stderr := selected(t, root, "\x00", "internal/api/policy/clienterror.go")
	if err != nil {
		t.Fatalf("--list failed: %v\n%s", err, stderr)
	}
	have := map[string]bool{}
	for _, row := range rows {
		have[row] = true
	}
	for _, want := range []string{
		"internal/apiservice\tTestVenueOracleCredentialGitHubRepos",
		"internal/apiservice\tTestVenueOracleCredentialRepos",
	} {
		if !have[want] {
			t.Errorf("a change in internal/api/policy did not select %q (selected %d row(s))", want, len(rows))
		}
	}
	// A package with no hand-written row at all: internal/auth/signedtoken is linked by the auth flow
	// oracle's server and the pull request job used to select nothing for it.
	signed, err, stderr := selected(t, root, "\x00", "internal/auth/signedtoken/x.go")
	if err != nil {
		t.Fatalf("--list failed: %v\n%s", err, stderr)
	}
	foundAuthFlow := false
	for _, row := range signed {
		foundAuthFlow = foundAuthFlow || row == "internal/apiservice/authflowvenue\tTestAuthFlowVenueOracle"
	}
	if !foundAuthFlow {
		t.Errorf("a change in internal/auth/signedtoken did not select TestAuthFlowVenueOracle (selected %d row(s))", len(signed))
	}
	// A golden under the testdata of a linked package selects as well; a package nothing links does not.
	golden, err, stderr := selected(t, root, "\x00", "internal/api/policy/testdata/x.json")
	if err != nil || len(golden) != len(rows) {
		t.Errorf("a file under internal/api/policy/testdata selected %d row(s), a Go file there %d (err %v)\n%s", len(golden), len(rows), err, stderr)
	}
	none, err, stderr := selected(t, root, "\x00", "internal/api/not-linked-by-any-venue-test/x.go")
	if err != nil || len(none) != 0 {
		t.Errorf("a package no venue test links selected %v (err %v)\n%s", none, err, stderr)
	}
}

// TestAMatchIsNotLostOnALargeChangeSet holds that selection does not depend on the size of the change
// set: a pipeline of `printf | grep -q` under pipefail (it needs more distinct directories than fit a 64 KB pipe, and the matching directory sorting before them) reports failure when grep stops reading at its
// first match and the writer dies of SIGPIPE, so a large pull request selected nothing and the job read
// a green no-op (round 1 of CHAOS-9935).
func TestAMatchIsNotLostOnALargeChangeSet(t *testing.T) {
	root := repoRoot(t)
	paths := make([]string, 0, 20001)
	for i := 0; i < 20000; i++ {
		paths = append(paths, fmt.Sprintf("zdocs/generated-%05d/page.md", i))
	}
	paths = append([]string{"internal/api/policy/clienterror.go"}, paths...)
	rows, err, stderr := selected(t, root, "\x00", paths...)
	if err != nil {
		t.Fatalf("--list failed: %v\n%s", err, stderr)
	}
	for _, row := range rows {
		if row == "internal/apiservice\tTestVenueOracleCredentialRepos" {
			return
		}
	}
	t.Fatalf("a change in internal/api/policy among 20000 other paths selected %d row(s) and not TestVenueOracleCredentialRepos\n%s", len(rows), stderr)
}

// TestAChangeToAFileALinkedPackageEmbedsSelectsTheTestsThatLinkIt holds the embedded-resource half of the
// closure (round 2 of CHAOS-9935): internal/auth/passwordpolicy embeds data/common_passwords.txt and the
// admin venue server links it, but the file is in no package directory, so the package rule alone selected
// nothing and the job read green.
func TestAChangeToAFileALinkedPackageEmbedsSelectsTheTestsThatLinkIt(t *testing.T) {
	root := repoRoot(t)
	for _, changed := range []string{
		"internal/auth/passwordpolicy/data/common_passwords.txt",
		"internal/pgmigrate/sql/0139_refunds_write_first_idempotency_key.sql",
	} {
		rows, err, stderr := selected(t, root, "\x00", changed)
		if err != nil {
			t.Fatalf("--list failed: %v\n%s", err, stderr)
		}
		admin := false
		for _, row := range rows {
			admin = admin || strings.HasPrefix(row, "internal/apiservice/admin\t")
		}
		if !admin {
			t.Errorf("a change to the embedded file %s did not select a test of internal/apiservice/admin (selected %d row(s))", changed, len(rows))
		}
	}
}
