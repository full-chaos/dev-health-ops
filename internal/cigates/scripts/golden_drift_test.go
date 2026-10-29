package scripts_test

import (
	"fmt"
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
  fail:*"${FAKE_GO_FAIL_PKG:-nomatch}") echo "--- PASS: TestSibling (0.00s)"; echo "--- FAIL: TestX (0.00s)"; exit 1 ;;
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
		for _, line := range strings.Split(strings.TrimSpace(log), "\n") {
			if !strings.Contains(line, " -count=1 ") {
				t.Errorf("every go test the check runs carries an explicit -count=1 (a cached pass proves nothing): %q", line)
			}
		}
		for _, want := range []string{"-tags=integration", "-run ^TestA$ ./internal/adminops", "-run ^TestB$ ./internal/api/session"} {
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

// A registry error is a usage error (exit 2), never a verdict: the workflow step must stop on it.
func TestGoldenDriftSelectRegistryErrorExitsTwo(t *testing.T) {
	root := repoRoot(t)
	list := filepath.Join(t.TempDir(), "changed.txt")
	if err := os.WriteFile(list, []byte(nulList("docs/a.md")), 0o600); err != nil {
		t.Fatal(err)
	}
	res := cmdSpec{dir: t.TempDir(), env: []string{shimBasePath(), "GOLDEN_DRIFT_REGISTRY=" + filepath.Join(t.TempDir(), "absent.tsv")}}.run("bash", filepath.Join(root, "ci", "check_golden_drift.sh"), "select", list)
	if res.code != 2 || res.stdout != "" {
		t.Fatalf("exit %d stdout %q, want exit 2 and no verdict", res.code, res.stdout)
	}
}

// A malformed registry row is a registry error (exit 2, no verdict). Bash `read` with a tab IFS merges adjacent tabs, so
// an empty field must be rejected by an explicit split, never read as the next field.
func TestGoldenDriftMalformedRegistryExitsTwo(t *testing.T) {
	root := repoRoot(t)
	rows := map[string]string{
		"empty trigger then a fourth field": "internal/adminops\t^TestUsersMatchTheFrozenPythonOutput$\t\tinternal/adminops/*\n",
		"two fields":                        "internal/adminops\t^TestX$\n",
		"empty package":                     "\t^TestX$\t@MIG\n",
		"empty regex":                       "internal/adminops\t\t@MIG\n",
		"empty trigger":                     "internal/adminops\t^TestX$\t\n",
		"trailing empty fourth field":       "internal/adminops\t^TestX$\t@MIG\t\n",
		"four fields":                       "internal/adminops\t^TestX$\t@MIG\textra\n",
	}
	list := filepath.Join(t.TempDir(), "changed.txt")
	if err := os.WriteFile(list, []byte(nulList("internal/pgmigrate/sql/0160_add_users_saml_nameid_parts.sql")), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, row := range rows {
		t.Run(name, func(t *testing.T) {
			reg := filepath.Join(t.TempDir(), "registry.tsv")
			if err := os.WriteFile(reg, []byte(row), 0o600); err != nil {
				t.Fatal(err)
			}
			res := cmdSpec{dir: t.TempDir(), env: []string{shimBasePath(), "GOLDEN_DRIFT_REGISTRY=" + reg}}.run("bash", filepath.Join(root, "ci", "check_golden_drift.sh"), "select", list)
			if res.code != 2 || res.stdout != "" {
				t.Fatalf("exit %d stdout %q, want exit 2 and no verdict", res.code, res.stdout)
			}
		})
	}
}

func TestGoldenDriftRunRefusesAMalformedRowLine(t *testing.T) {
	for _, rows := range []string{"ROW\tinternal/adminops\t\n", "ROW\t\t^TestA$\n", "ROW\tinternal/adminops\t^TestA$\textra\n"} {
		res, _ := runGoldenDrift(t, rows, "pass", "")
		if res.code != 2 {
			t.Fatalf("rows %q: exit %d, want 2", rows, res.code)
		}
	}
}

// run must never turn a rows file that names rows into a green no-op: a line that starts with ROW but does not parse, or a
// select/all verdict without a single ROW line, is a usage error (exit 2); only an explicit "VERDICT none" runs nothing.
func TestGoldenDriftRunNeverPassesWithoutRunningANamedRow(t *testing.T) {
	bad := map[string]string{
		"bare ROW marker":                  "ROW\n",
		"ROW with one field":               "ROW\tinternal/adminops\n",
		"ROW glued to a word":              "ROWX\tinternal/adminops\t^TestA$\n",
		"ROW then a space":                 "ROW internal/adminops ^TestA$\n",
		"select verdict and no row":        "VERDICT select 1 of 4 rows for 2 changed paths\n",
		"all verdict and no row":           "VERDICT all bad input\n",
		"empty rows file":                  "",
		"verdict none but a malformed row": "VERDICT none x\nROW\n",
	}
	for name, rows := range bad {
		t.Run(name, func(t *testing.T) {
			res, log := runGoldenDrift(t, rows, "pass", "")
			if res.code != 2 {
				t.Fatalf("exit %d, want 2 (stdout %q stderr %q)", res.code, res.stdout, res.stderr)
			}
			if strings.TrimSpace(log) != "" {
				t.Fatalf("no go test may run for an unparseable rows file: %q", log)
			}
		})
	}
	t.Run("an explicit none verdict runs nothing and passes", func(t *testing.T) {
		res, log := runGoldenDrift(t, "VERDICT none 1 changed paths reach no golden row\n", "pass", "")
		if res.code != 0 || strings.TrimSpace(log) != "" {
			t.Fatalf("exit %d log %q, want exit 0 and no go test", res.code, log)
		}
	})
}

// Any non-empty line of a rows file must be a VERDICT line or a well-formed ROW: anything else is a usage error that runs
// nothing, and a "VERDICT none" never sits beside a ROW.
func TestGoldenDriftRunRefusesAnyOtherLine(t *testing.T) {
	good := "ROW\tinternal/adminops\t^TestA$\n"
	for name, rows := range map[string]string{
		"a stray word before a good row": "hello\n" + good,
		"a stray word after a good row":  good + "hello\n",
		"a leading space on a ROW":       " " + good,
		"a lowercase row line":           "row\tinternal/adminops\t^TestA$\n",
		"none verdict beside a good row": "VERDICT none x\n" + good,
		"a verdict-like word":            "VERDICTS select\n" + good,
	} {
		t.Run(name, func(t *testing.T) {
			res, log := runGoldenDrift(t, rows, "pass", "")
			if res.code != 2 || strings.TrimSpace(log) != "" {
				t.Fatalf("exit %d, go calls %q: want exit 2 and no go test", res.code, log)
			}
		})
	}
}

// The workflow decides "rows != 0" with a ROW-prefix count (grep -c '^ROW'). That count and the number of rows `run`
// actually runs must be one rule: for every rows file, `run` either fails without running anything or runs exactly as many
// go tests as the workflow's prefix count.
func TestGoldenDriftWorkflowRowCountEqualsRowsRun(t *testing.T) {
	root := repoRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, ".github", "workflows", "go.yml"))
	if err != nil {
		t.Fatal(err)
	}
	job := string(raw)
	if i := strings.Index(job, "  go-golden-drift:\n"); i >= 0 {
		job = job[i:]
	} else {
		t.Fatal("go.yml lost the go-golden-drift job")
	}
	if j := strings.Index(job, "\n  # Preserve the existing required-check contract"); j >= 0 {
		job = job[:j]
	}
	m := regexp.MustCompile(`grep -c '\^(ROW[^']*)'`).FindStringSubmatch(job)
	if m == nil || m[1] != "ROW" {
		t.Fatalf("the select step must count rows with grep -c '^ROW' (the script's own prefix); got %v", m)
	}
	files := map[string]string{
		"one row":            "VERDICT select 1 of 4\nROW\tinternal/adminops\t^TestA$\n",
		"four rows":          "VERDICT all x\nROW\ta\t^A$\nROW\tb\t^B$\nROW\tc\t^C$\nROW\td\t^D$\n",
		"none":               "VERDICT none x\n",
		"bare ROW":           "VERDICT select x\nROW\n",
		"ROW glued":          "VERDICT select x\nROWX\ta\t^A$\n",
		"ROW one field":      "VERDICT select x\nROW\ta\n",
		"ROW then space":     "VERDICT select x\nROW a ^A$\n",
		"good then bare ROW": "VERDICT select x\nROW\ta\t^A$\nROW\n",
	}
	for name, rows := range files {
		t.Run(name, func(t *testing.T) {
			prefixed := 0
			for _, l := range strings.Split(rows, "\n") {
				if strings.HasPrefix(l, "ROW") {
					prefixed++
				}
			}
			res, log := runGoldenDrift(t, rows, "pass", "")
			calls := 0
			if s := strings.TrimSpace(log); s != "" {
				calls = len(strings.Split(s, "\n"))
			}
			if res.code == 0 && calls != prefixed {
				t.Fatalf("run exited 0 after %d go test(s) but the workflow counts %d ROW-prefixed line(s)", calls, prefixed)
			}
			if res.code != 0 && calls != 0 {
				t.Fatalf("a refused rows file must run nothing, ran %d", calls)
			}
		})
	}
}

// run accepts exactly the line set `select` emits: one well-formed ROW per row and exactly one VERDICT line
// (select|none|all, with a reason). select never emits an empty line, so an empty line is refused too. Everything else
// exits 2 and runs no go test.
func TestGoldenDriftRunAcceptsExactlyTheSelectOutput(t *testing.T) {
	v := "VERDICT select 1 of 4 rows for 1 changed paths\n"
	row := "ROW\tinternal/adminops\t^TestA$\n"
	bad := map[string]string{
		"leading-space ROW":                v + " " + row,
		"lower-case row":                   v + "row\tinternal/adminops\t^TestA$\n",
		"a garbage line":                   v + row + "garbage\n",
		"a garbage line first":             "garbage\n" + v + row,
		"two VERDICT lines":                v + v + row,
		"none verdict with a ROW":          "VERDICT none x\n" + row,
		"ROW lines and no VERDICT":         row,
		"an empty line between":            v + "\n" + row,
		"a trailing empty line":            v + row + "\n",
		"VERDICT without a reason":         "VERDICT select\n" + row,
		"unknown verdict kind":             "VERDICT maybe x\n" + row,
		"select verdict and no ROW":        v,
		"two none verdicts and no ROW":     "VERDICT none a\nVERDICT none b\n",
		"a space-indented VERDICT":         " " + v + row,
		"VERDICT glued to a longer word":   "VERDICTS select x\n" + row,
		"a carriage return ends the ROW":   v + "ROW\tinternal/adminops\t^TestA$\r\n",
		"a fifth ROW field":                v + row[:len(row)-1] + "\textra\n",
		"a ROW with an empty regex":        v + "ROW\tinternal/adminops\t\n",
		"a ROW with an empty package":      v + "ROW\t\t^TestA$\n",
		"a comment line":                   "# note\n" + v + row,
		"a tab-led line":                   "\t" + v + row,
		"two ROWs glued on one line":       v + "ROW\ta\t^A$ROW\tb\t^B$\n",
		"VERDICT after the rows (kept ok)": "",
	}
	delete(bad, "VERDICT after the rows (kept ok)")
	for name, rows := range bad {
		t.Run(name, func(t *testing.T) {
			res, log := runGoldenDrift(t, rows, "pass", "")
			if res.code != 2 || strings.TrimSpace(log) != "" {
				t.Fatalf("exit %d, go calls %q: want exit 2 and no go test (stderr %q)", res.code, log, res.stderr)
			}
		})
	}
	for name, rows := range map[string]string{
		"exactly what select emits":              v + row,
		"all verdict, two rows":                  "VERDICT all bad input: change list missing\n" + row + "ROW\ta\t^A$\n",
		"the verdict after the rows is still ok": row + v,
	} {
		t.Run("accepted: "+name, func(t *testing.T) {
			res, log := runGoldenDrift(t, rows, "pass", "")
			if res.code != 0 || strings.TrimSpace(log) == "" {
				t.Fatalf("exit %d, go calls %q: want exit 0 and the rows run (stderr %q)", res.code, log, res.stderr)
			}
		})
	}
}

// Hostile rows files: bytes the shell would silently drop or reinterpret must not reach go test.
func TestGoldenDriftRunRefusesHostileRowsFiles(t *testing.T) {
	v := "VERDICT select 1 of 4 rows for 1 changed paths\n"
	row := "ROW\tinternal/adminops\t^TestA$\n"
	bad := map[string]string{
		"a NUL byte in a ROW regex":                  v + "ROW\tinternal/adminops\t^Test\x00X$\n",
		"a NUL byte in a ROW package":                v + "ROW\tinternal/admin\x00ops\t^TestA$\n",
		"a NUL byte in the VERDICT line":             "VERDICT select 1 of\x00 4\n" + row,
		"a NUL byte on its own line":                 v + "\x00\n" + row,
		"no final newline after a ROW":               v + strings.TrimSuffix(row, "\n"),
		"no final newline after VERDICT none":        "VERDICT none x",
		"no final newline after a VERDICT with rows": row + "VERDICT select x",
		"a space-only reason":                        "VERDICT all  \n" + row,
		"a tab-only reason":                          "VERDICT select \t\n" + row,
		"a space-only none reason":                   "VERDICT none   \n",
		"a package ..":                               v + "ROW\t..\t^TestA$\n",
		"a package ../":                              v + "ROW\t../\t^TestA$\n",
		"a package with an inner ..":                 v + "ROW\tinternal/../etc\t^TestA$\n",
		"a package ending in ..":                     v + "ROW\tinternal/adminops/..\t^TestA$\n",
		"an absolute package":                        v + "ROW\t/etc\t^TestA$\n",
		"VERDICT nonesuch":                           "VERDICT nonesuch x\n" + row,
		"VERDICT selected":                           "VERDICT selected x\n" + row,
		"VERDICT allx":                               "VERDICT allx\n" + row,
		"VERDICT nonesuch alone":                     "VERDICT nonesuch x\n",
		"VERDICT none without a reason alone":        "VERDICT none\n",
		"VERDICT none glued to a word alone":         "VERDICT nonex y\n",
		"VERDICT selected alone":                     "VERDICT selected x\n",
	}
	for name, rows := range bad {
		t.Run(name, func(t *testing.T) {
			res, log := runGoldenDrift(t, rows, "pass", "")
			if res.code != 2 || strings.TrimSpace(log) != "" {
				t.Fatalf("exit %d, go calls %q: want exit 2 and no go test (stderr %q)", res.code, log, res.stderr)
			}
		})
	}
	for name, rows := range map[string]string{
		"a tab inside the reason":         "VERDICT select a\tb\n" + row,
		"a regex that starts with a dash": v + "ROW\tinternal/adminops\t-x\n",
		"a dotted package name":           v + "ROW\tinternal/a..b\t^TestA$\n",
	} {
		t.Run("accepted: "+name, func(t *testing.T) {
			res, log := runGoldenDrift(t, rows, "pass", "")
			if res.code != 0 || strings.TrimSpace(log) == "" {
				t.Fatalf("exit %d, go calls %q: want exit 0 and the row runs (stderr %q)", res.code, log, res.stderr)
			}
		})
	}
}

// select must not emit a package the run side refuses: the registry applies the same package rule.
func TestGoldenDriftRegistryRefusesAnUnsafePackage(t *testing.T) {
	root := repoRoot(t)
	list := filepath.Join(t.TempDir(), "changed.txt")
	if err := os.WriteFile(list, []byte(nulList("internal/pgmigrate/sql/0160_x.sql")), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, pkg := range map[string]string{"dotdot": "..", "dotdot slash": "../x", "inner dotdot": "internal/../x", "trailing dotdot": "internal/..", "absolute": "/etc"} {
		t.Run(name, func(t *testing.T) {
			reg := filepath.Join(t.TempDir(), "registry.tsv")
			if err := os.WriteFile(reg, []byte(pkg+"\t^TestX$\t@MIG\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			res := cmdSpec{dir: t.TempDir(), env: []string{shimBasePath(), "GOLDEN_DRIFT_REGISTRY=" + reg}}.run("bash", filepath.Join(root, "ci", "check_golden_drift.sh"), "select", list)
			if res.code != 2 || res.stdout != "" {
				t.Fatalf("exit %d stdout %q, want exit 2 and no verdict", res.code, res.stdout)
			}
		})
	}
}

// The golden rows run under the job deadline: -timeout = min(22 min, job cap - elapsed - 3 min), and no budget left runs nothing.
func TestGoldenDriftRunFollowsTheJobBudget(t *testing.T) {
	const t0 = 1700000000
	rows := "VERDICT select 1 of 1\nROW\tinternal/adminops\t^TestA$\n"
	run := func(nowOffset int) (cmdResult, string) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "go"), []byte(fakeGo), 0o755); err != nil {
			t.Fatal(err)
		}
		rf := filepath.Join(t.TempDir(), "rows.tsv")
		if err := os.WriteFile(rf, []byte(rows), 0o600); err != nil {
			t.Fatal(err)
		}
		logf := filepath.Join(t.TempDir(), "go.log")
		env := []string{"PATH=" + dir + ":/usr/bin:/bin", "FAKE_GO_LOG=" + logf, "FAKE_GO_MODE=pass",
			"CHECK_GO_JOB_CAP_MIN=25", fmt.Sprintf("CHECK_GO_JOB_START=%d", t0), fmt.Sprintf("CHECK_GO_NOW_EPOCH=%d", t0+nowOffset)}
		res := cmdSpec{dir: t.TempDir(), env: env}.run("bash", filepath.Join(repoRoot(t), "ci", "check_golden_drift.sh"), "run", rf)
		b, _ := os.ReadFile(logf)
		return res, string(b)
	}
	res, log := run(600)
	if res.code != 0 || !strings.Contains(log, " -timeout=720s ") {
		t.Errorf("exit %d; the go call must carry -timeout=720s (cap 25 min - 600 s - 3 min):\n%s\n%s", res.code, log, res.stderr)
	}
	res, log = run(1400)
	if res.code != 2 || strings.Contains(log, "go test") || !strings.Contains(res.stderr, "job time budget exhausted before") {
		t.Errorf("no budget left: exit %d (want 2), go log %q, stderr %s", res.code, log, res.stderr)
	}
}
