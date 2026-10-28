package scripts_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The advisory PR check for frozen-Python golden drift (ci/check_golden_drift.sh, ci/golden_drift_registry.tsv). The five
// culprit shapes are the file sets of the pull requests that failed in the merge queue; a PR file list is abbreviated
// to the paths that decide the selection.

func nulList(paths ...string) string { return strings.Join(paths, "\x00") + "\x00" }

func goldenDriftSelect(t *testing.T, list string) (verdict string, pkgs []string, code int) {
	t.Helper()
	root := repoRoot(t)
	f := filepath.Join(t.TempDir(), "changed.txt")
	if err := os.WriteFile(f, []byte(list), 0o600); err != nil {
		t.Fatal(err)
	}
	res := cmdSpec{dir: t.TempDir(), env: []string{shimBasePath()}}.run("bash", filepath.Join(root, "ci", "check_golden_drift.sh"), "select", f)
	for _, line := range strings.Split(strings.TrimSpace(res.stdout), "\n") {
		switch {
		case strings.HasPrefix(line, "VERDICT "):
			verdict = strings.Fields(line)[1]
		case strings.HasPrefix(line, "ROW\t"):
			pkgs = append(pkgs, strings.Split(line, "\t")[1])
		}
	}
	return verdict, pkgs, res.code
}

var allGoldenPkgs = []string{"internal/adminops", "internal/fixturescli", "internal/auth/usersbackfill", "internal/api/session"}

func TestGoldenDriftSelect(t *testing.T) {
	rows := []struct {
		name    string
		list    string
		verdict string
		pkgs    []string
	}{
		{"5083 users column: migration", nulList("internal/pgmigrate/sql/0160_add_users_saml_nameid_parts.sql", "internal/auth/usersbackfill/backfill.go", "internal/api/sso/saml.go"), "select", allGoldenPkgs},
		{"5092 finalize column: migration", nulList("internal/pgmigrate/sql/0161_add_sync_run_post_dispatches_recorded_at.sql", "internal/syncreconciler/materializer.go"), "select", allGoldenPkgs},
		{"5131 org_activity: migration and chain manifest", nulList("internal/pgmigrate/chain_manifest.tsv", "internal/orgactivity/orgactivity.go"), "select", allGoldenPkgs},
		{"chain manifest alone", nulList("internal/pgmigrate/chain_manifest.tsv"), "select", allGoldenPkgs},
		{"5088 no migration: usersbackfill Go change", nulList("internal/auth/usersbackfill/oauthsso.go", "internal/auth/authruntime/usersguard.go"), "select", []string{"internal/auth/usersbackfill"}},
		{"5100 no migration: api/session writer guard", nulList("internal/api/session/social.go", "internal/auth/identity/providerid.go", "internal/api/sso/oauth.go"), "select", []string{"internal/api/session"}},
		{"planted Go-only change in usersbackfill", nulList("internal/auth/usersbackfill/planted.go"), "select", []string{"internal/auth/usersbackfill"}},
		{"planted Go-only change in api/session", nulList("internal/api/session/planted.go"), "select", []string{"internal/api/session"}},
		{"test file in a golden package", nulList("internal/adminops/users_integration_test.go"), "select", []string{"internal/adminops"}},
		{"nested path under a golden package", nulList("internal/fixturescli/testdata/golden/finalize_synthetic.json"), "select", []string{"internal/fixturescli"}},
		{"docs only", nulList("docs/reference/cli/index.md", "README.md"), "none", nil},
		{"unrelated Go package", nulList("internal/orgactivity/orgactivity.go"), "none", nil},
		{"lookalike dirs", nulList("internal/adminops2/x.go", "internal/api/sessions/x.go", "internal/pgmigrate/sqlx/a.sql", "internal/pgmigrate/chain_manifest.tsv.bak"), "none", nil},
		{"down migration is a migration path", nulList("internal/pgmigrate/sql/down/0160_x.sql"), "select", allGoldenPkgs},
		{"the check itself runs every row", nulList("ci/check_golden_drift.sh"), "select", allGoldenPkgs},
		{"the registry runs every row", nulList("ci/golden_drift_registry.tsv"), "select", allGoldenPkgs},
		{"bad input: empty list", "", "all", allGoldenPkgs},
		{"bad input: nul only", "\x00", "all", allGoldenPkgs},
		{"bad input: unterminated", "docs/a.md\x00internal/foo/a.go", "all", allGoldenPkgs},
		{"bad input: empty record", "docs/a.md\x00\x00", "all", allGoldenPkgs},
	}
	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			verdict, pkgs, code := goldenDriftSelect(t, r.list)
			if code != 0 {
				t.Fatalf("exit %d, want 0 (a bad input must fail open, never fail)", code)
			}
			if verdict != r.verdict {
				t.Fatalf("verdict %q, want %q (rows %v)", verdict, r.verdict, pkgs)
			}
			if strings.Join(pkgs, ",") != strings.Join(r.pkgs, ",") {
				t.Fatalf("rows %v, want %v", pkgs, r.pkgs)
			}
		})
	}
}

func TestGoldenDriftMissingListFailsOpen(t *testing.T) {
	root := repoRoot(t)
	res := cmdSpec{dir: t.TempDir(), env: []string{shimBasePath()}}.run("bash", filepath.Join(root, "ci", "check_golden_drift.sh"), "select", filepath.Join(t.TempDir(), "absent.txt"))
	if res.code != 0 || !strings.HasPrefix(res.stdout, "VERDICT all ") || strings.Count(res.stdout, "ROW\t") != len(allGoldenPkgs) {
		t.Fatalf("missing list: exit %d stdout %q, want every row", res.code, res.stdout)
	}
}

// Every registry row names a package that exists and a -run regex that matches at least one integration test there; the
// culprit tests stay named.
func TestGoldenDriftRegistryMatchesRealTests(t *testing.T) {
	root := repoRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, "ci", "golden_drift_registry.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	must := map[string]bool{
		"TestUsersMatchTheFrozenPythonOutput":                     false,
		"TestOrgsDeleteMatchesTheFrozenPythonOutput":              false,
		"TestFinalizeSyntheticWritesTheFrozenPythonRows":          false,
		"TestOrgsBackfillInsertsThenRerunChangesNothing":          false,
		"TestReconcileAfterBackfillIsZeroDiffAndSeesPlantedDrift": false,
		"TestSessionRoutesMatchGolden":                            false,
	}
	funcRe := regexp.MustCompile(`(?m)^func (Test\w+)\(t \*testing\.T\)`)
	rows := 0
	for _, line := range strings.Split(string(raw), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		f := strings.Split(line, "\t")
		if len(f) != 3 || f[0] == "" || f[1] == "" || f[2] == "" {
			t.Fatalf("registry row needs 3 tab separated fields: %q", line)
		}
		rows++
		re, err := regexp.Compile(f[1])
		if err != nil {
			t.Fatalf("%s: regex: %v", f[0], err)
		}
		entries, err := os.ReadDir(filepath.Join(root, f[0]))
		if err != nil {
			t.Fatal(err)
		}
		matched := 0
		for _, e := range entries {
			if !strings.HasSuffix(e.Name(), "_test.go") {
				continue
			}
			b, err := os.ReadFile(filepath.Join(root, f[0], e.Name()))
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(b), "//go:build integration") {
				continue
			}
			for _, m := range funcRe.FindAllStringSubmatch(string(b), -1) {
				if re.MatchString(m[1]) {
					matched++
					if _, ok := must[m[1]]; ok {
						must[m[1]] = true
					}
				}
			}
		}
		if matched == 0 {
			t.Errorf("%s: regex %s matches no integration test", f[0], f[1])
		}
	}
	if rows < 4 {
		t.Errorf("registry has %d rows, want at least the 4 culprit packages", rows)
	}
	for name, seen := range must {
		if !seen {
			t.Errorf("culprit test %s is not matched by any registry row", name)
		}
	}
}

const fakeGo = `#!/bin/bash
echo "go $*" >> "${FAKE_GO_LOG}"
last="${@: -1}"
case "${FAKE_GO_MODE:-pass}:${last}" in
  fail:*"${FAKE_GO_FAIL_PKG:-nomatch}") echo "--- FAIL: TestX (0.00s)"; exit 1 ;;
  nomatch:*) echo "testing: warning: no tests to run"; echo "ok  ${last}"; exit 0 ;;
  *) echo "--- PASS: TestX (0.00s)"; exit 0 ;;
esac
`

func runGoldenDrift(t *testing.T, rows string, mode, failPkg string) (cmdResult, string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "go"), []byte(fakeGo), 0o755); err != nil {
		t.Fatal(err)
	}
	rf := filepath.Join(t.TempDir(), "rows.tsv")
	if err := os.WriteFile(rf, []byte(rows), 0o600); err != nil {
		t.Fatal(err)
	}
	logf := filepath.Join(t.TempDir(), "go.log")
	res := cmdSpec{dir: t.TempDir(), env: []string{"PATH=" + dir + ":/usr/bin:/bin", "FAKE_GO_LOG=" + logf, "FAKE_GO_MODE=" + mode, "FAKE_GO_FAIL_PKG=" + failPkg}}.run("bash", filepath.Join(repoRoot(t), "ci", "check_golden_drift.sh"), "run", rf)
	b, _ := os.ReadFile(logf)
	return res, string(b)
}

func TestGoldenDriftRun(t *testing.T) {
	rows := "VERDICT select 2 of 4\nROW\tinternal/adminops\t^TestA$\nROW\tinternal/api/session\t^TestB$\n"
	t.Run("pass runs one go test per row with the integration tag and -run", func(t *testing.T) {
		res, log := runGoldenDrift(t, rows, "pass", "")
		if res.code != 0 {
			t.Fatalf("exit %d: %s %s", res.code, res.stdout, res.stderr)
		}
		for _, want := range []string{"-tags=integration", "-count=1", "-run ^TestA$ ./internal/adminops", "-run ^TestB$ ./internal/api/session"} {
			if !strings.Contains(log, want) {
				t.Errorf("go call log lacks %q:\n%s", want, log)
			}
		}
	})
	t.Run("a failing row exits 1, still runs the next row, and prints the fix path", func(t *testing.T) {
		res, log := runGoldenDrift(t, rows, "fail", "./internal/adminops")
		if res.code != 1 {
			t.Fatalf("exit %d, want 1", res.code)
		}
		if !strings.Contains(log, "./internal/api/session") {
			t.Errorf("the second row did not run:\n%s", log)
		}
		for _, want := range []string{"FROZEN PYTHON GOLDEN DRIFT", "internal/adminops", "no update command", "Go-only", "users_integration_test.go:185"} {
			if !strings.Contains(res.stderr, want) {
				t.Errorf("failure message lacks %q:\n%s", want, res.stderr)
			}
		}
	})
	t.Run("a regex that matches no test fails", func(t *testing.T) {
		res, _ := runGoldenDrift(t, rows, "nomatch", "")
		if res.code != 1 || !strings.Contains(res.stderr, "matched no passing test") {
			t.Fatalf("exit %d stderr %q, want 1 and the no-match refusal", res.code, res.stderr)
		}
	})
	t.Run("a malformed row is a usage error", func(t *testing.T) {
		res, _ := runGoldenDrift(t, "ROW\tinternal/adminops\n", "pass", "")
		if res.code != 2 {
			t.Fatalf("exit %d, want 2", res.code)
		}
	})
}
