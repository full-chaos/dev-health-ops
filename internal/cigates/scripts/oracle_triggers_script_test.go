//go:build !race

package scripts_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The pull request venue-oracle selection also follows declared paths. `ci/check_go.sh venue-oracles --changed FILE`
// selects a venue test by the paths of its own package, by ci/venue_oracle_triggers.d/ rows (a venue test compares what code
// in other packages does) and by the dependency closure in ci/venue_oracle_closure.d/. A trigger row that could go stale
// without a sign fails the static check, and a broken file stops the selection instead of reading as "nothing selected".

type selectedRow = [2]string

func (o *otree) selected(nul bool, changed ...string) []selectedRow {
	o.t.Helper()
	sep := "\n"
	if nul {
		sep = "\x00"
	}
	o.write("changed.txt", strings.Join(changed, sep)+sep)
	r := o.venueOracles(nil, "--changed", filepath.Join(o.dir, "changed.txt"), "--list")
	if r.res.code != 0 {
		o.t.Fatalf("--list for %v: %s", changed, r.res.stderr)
	}
	return parseSelected(r.res.stdout)
}

func parseSelected(stdout string) []selectedRow {
	var rows []selectedRow
	for _, l := range strings.Split(stdout, "\n") {
		if f := strings.Split(l, "\t"); f[0] == "SELECT" && len(f) == 3 {
			rows = append(rows, selectedRow{f[1], f[2]})
		}
	}
	return rows
}

func sameSelected(a, b []selectedRow) bool { return rowsEqual(a, b) }

func (o *otree) triggers(pkg string, rows ...[2]string) string {
	o.t.Helper()
	var b strings.Builder
	for _, r := range rows {
		fmt.Fprintf(&b, "%s\t%s\t%s\n", pkg, r[0], r[1])
	}
	rel := "ci/venue_oracle_triggers.d/" + strings.ReplaceAll(pkg, "/", "__") + ".tsv"
	o.write(rel, b.String())
	return rel
}

func (o *otree) staticCheck() cmdResult {
	return freeSpec{dir: o.dir, env: inheritedEnv(nil)}.run("bash", filepath.Join(o.dir, "ci", "check_venue_oracle_registry.sh"))
}

func triggerTree(t *testing.T) *otree {
	t.Helper()
	o := scratchOTree(t, []pkgTests{
		{"internal/api", []string{"TestVenueOracleRoutes", "TestVenueOracleSettings"}},
		{"internal/jobs", []string{"TestVenueOracleJobs"}},
	})
	o.write("internal/store/store.go", "package store\n")
	o.write("internal/store/sub/sub.go", "package sub\n")
	o.write("internal/storefront/x.go", "package storefront\n")
	return o
}

func selectedOf(t *testing.T, o *otree, changed ...string) []selectedRow {
	t.Helper()
	return o.selected(false, changed...)
}

func TestOracleTriggersRealTree(t *testing.T) {
	t.Parallel()
	selected := func(changed string) []selectedRow {
		listing := filepath.Join(t.TempDir(), "changed.txt")
		if err := os.WriteFile(listing, []byte(changed+"\x00"), 0o644); err != nil {
			t.Fatal(err)
		}
		r := freeSpec{dir: rootOf(t), env: inheritedEnv(nil)}.run("bash", "ci/check_go.sh", "venue-oracles", "--changed", listing, "--list")
		if r.code != 0 {
			t.Fatalf("--list %s: %s", changed, r.stderr)
		}
		return parseSelected(r.stdout)
	}
	protected := selectedRow{"internal/apiservice", "TestVenueOracleProtectedRoutes"}
	for _, changed := range []string{"internal/api/teamsidentity/store.go", "internal/api/teamsidentity/drift_review.go", "internal/chmigrate/sql/115_teams_deleted_at.sql"} {
		found := false
		for _, r := range selected(changed) {
			if r == protected {
				found = true
			}
		}
		if !found {
			t.Errorf("a pull request that changes only %s does not select the protected-routes venue test: it reads green while main goes red", changed)
		}
	}
	for _, far := range []string{"internal/scheduler/fixed/x.go", "docs/index.md"} {
		if got := selected(far); len(got) != 0 {
			t.Errorf("a change to %s selects %v, want nothing", far, got)
		}
	}
	r := freeSpec{dir: rootOf(t), env: inheritedEnv(nil)}.run("bash", "ci/check_venue_oracle_registry.sh")
	if r.code != 0 {
		t.Fatalf("the static check rejects the real trigger rows: %s", r.stderr)
	}
	files, _ := filepath.Glob(filepath.Join(rootOf(t), "ci", "venue_oracle_triggers.d", "*.tsv"))
	declared := 0
	for _, f := range files {
		for _, l := range strings.Split(readText(t, f), "\n") {
			if strings.TrimSpace(l) != "" {
				declared++
			}
		}
	}
	if !strings.Contains(r.stdout, fmt.Sprintf("venue-oracle-triggers: %d declared path(s)", declared)) {
		t.Errorf("stdout lacks the declared count %d: %s", declared, r.stdout)
	}
}

func TestOracleTriggerSelectionRule(t *testing.T) {
	t.Parallel()
	routes := selectedRow{"internal/api", "TestVenueOracleRoutes"}
	settings := selectedRow{"internal/api", "TestVenueOracleSettings"}
	jobs := selectedRow{"internal/jobs", "TestVenueOracleJobs"}
	check := func(t *testing.T, got []selectedRow, want ...selectedRow) {
		t.Helper()
		if !sameSelected(got, want) {
			t.Errorf("selected %v, want %v", got, want)
		}
	}
	t.Run("without a trigger a dependency change selects nothing", func(t *testing.T) {
		t.Parallel()
		o := triggerTree(t)
		check(t, selectedOf(t, o, "internal/store/store.go"))
	})
	t.Run("a declared directory selects exactly its test", func(t *testing.T) {
		t.Parallel()
		o := triggerTree(t)
		o.triggers("internal/api", [2]string{"TestVenueOracleRoutes", "internal/store"})
		check(t, selectedOf(t, o, "internal/store/store.go"), routes)
		check(t, selectedOf(t, o, "internal/store/sub/sub.go"), routes)
		check(t, o.selected(true, "internal/store/store.go"), routes)
	})
	t.Run("a declared file selects only for that file", func(t *testing.T) {
		t.Parallel()
		o := triggerTree(t)
		o.triggers("internal/api", [2]string{"TestVenueOracleRoutes", "internal/store/store.go"})
		check(t, selectedOf(t, o, "internal/store/store.go"), routes)
		check(t, selectedOf(t, o, "internal/store/sub/sub.go"))
		check(t, selectedOf(t, o, "internal/store/store.go.orig"))
	})
	t.Run("a declared path matches whole segments only", func(t *testing.T) {
		t.Parallel()
		o := triggerTree(t)
		o.triggers("internal/api", [2]string{"TestVenueOracleRoutes", "internal/store"})
		check(t, selectedOf(t, o, "internal/storefront/x.go"))
		check(t, selectedOf(t, o, "x/internal/store/store.go"))
	})
	t.Run("the package rule is unchanged beside a trigger", func(t *testing.T) {
		t.Parallel()
		o := triggerTree(t)
		o.triggers("internal/api", [2]string{"TestVenueOracleRoutes", "internal/store"})
		check(t, selectedOf(t, o, "internal/api/x_test.go"), routes, settings)
		check(t, selectedOf(t, o, "internal/jobs/x_test.go"), jobs)
	})
	t.Run("two tests can declare the same path", func(t *testing.T) {
		t.Parallel()
		o := triggerTree(t)
		o.triggers("internal/api", [2]string{"TestVenueOracleRoutes", "internal/store"})
		o.triggers("internal/jobs", [2]string{"TestVenueOracleJobs", "internal/store"})
		check(t, selectedOf(t, o, "internal/store/store.go"), routes, jobs)
	})
	t.Run("a trigger selects the test of its own package only", func(t *testing.T) {
		t.Parallel()
		o := scratchOTree(t, []pkgTests{{"internal/api", []string{"TestVenueOracleSame"}}, {"internal/jobs", []string{"TestVenueOracleSame"}}})
		o.mkdir("internal/store")
		o.triggers("internal/jobs", [2]string{"TestVenueOracleSame", "internal/store"})
		check(t, selectedOf(t, o, "internal/store/store.go"), selectedRow{"internal/jobs", "TestVenueOracleSame"})
	})
	t.Run("the run runs the test a declared path selects", func(t *testing.T) {
		t.Parallel()
		o := triggerTree(t)
		o.triggers("internal/api", [2]string{"TestVenueOracleRoutes", "internal/store"})
		o.write("changed.txt", "internal/store/store.go\n")
		r := o.venueOracles(nil, "--changed", filepath.Join(o.dir, "changed.txt"))
		if r.res.code != 0 || !rowsEqual(r.ran, []selectedRow{routes}) {
			t.Fatalf("exit %d ran %v: %s%s", r.res.code, r.ran, r.res.stdout, r.res.stderr)
		}
	})
	t.Run("the run without a trigger is the loud no-op", func(t *testing.T) {
		t.Parallel()
		o := triggerTree(t)
		o.write("changed.txt", "internal/store/store.go\n")
		r := o.venueOracles(nil, "--changed", filepath.Join(o.dir, "changed.txt"))
		if r.res.code != 0 || len(r.ran) != 0 || !strings.Contains(r.res.stdout, "nothing to run for this change set") {
			t.Fatalf("exit %d ran %v: %s%s", r.res.code, r.ran, r.res.stdout, r.res.stderr)
		}
	})
}

func TestOracleTriggerStaticCheck(t *testing.T) {
	t.Parallel()
	failsWith := func(t *testing.T, r cmdResult, needle string) {
		t.Helper()
		if r.code == 0 || !strings.Contains(r.stderr, needle) {
			t.Fatalf("want a failure containing %q, got exit %d:\n%s%s", needle, r.code, r.stdout, r.stderr)
		}
	}
	t.Run("a good trigger row passes", func(t *testing.T) {
		t.Parallel()
		o := triggerTree(t)
		o.triggers("internal/api", [2]string{"TestVenueOracleRoutes", "internal/store"})
		r := o.staticCheck()
		if r.code != 0 || !strings.Contains(r.stdout, "venue-oracle-triggers: 1 declared path(s)") {
			t.Fatalf("exit %d: %s%s", r.code, r.stdout, r.stderr)
		}
	})
	for _, c := range []struct{ name, test, target, needle string }{
		{"a row for a test that is not a run row", "TestVenueOracleGone", "internal/store", "is not a run row"},
		{"a target that does not exist", "TestVenueOracleRoutes", "internal/gone", "does not exist"},
		{"a target inside the package of the test", "TestVenueOracleRoutes", "internal/api/x_test.go", "inside the package of the test"},
		{"a leading slash", "TestVenueOracleRoutes", "/internal/store", "is not <package>"},
		{"a trailing slash", "TestVenueOracleRoutes", "internal/store/", "is not <package>"},
		{"a dot dot segment", "TestVenueOracleRoutes", "internal/../internal/store", "is not <package>"},
	} {
		t.Run("a trigger row that selects nothing fails: "+c.name, func(t *testing.T) {
			t.Parallel()
			o := triggerTree(t)
			o.triggers("internal/api", [2]string{c.test, c.target})
			failsWith(t, o.staticCheck(), c.needle)
		})
	}
	t.Run("a trigger for a local only test fails", func(t *testing.T) {
		t.Parallel()
		o := scratchOTree(t, []pkgTests{{"internal/api", []string{"TestVenueOracleLocal", "TestVenueOracleRoutes"}}}, "TestVenueOracleLocal")
		o.mkdir("internal/store")
		o.triggers("internal/api", [2]string{"TestVenueOracleLocal", "internal/store"})
		failsWith(t, o.staticCheck(), "is not a run row")
	})
	t.Run("unsorted or duplicate trigger rows fail", func(t *testing.T) {
		t.Parallel()
		o := triggerTree(t)
		o.mkdir("internal/alpha")
		o.triggers("internal/api", [2]string{"TestVenueOracleRoutes", "internal/store"}, [2]string{"TestVenueOracleRoutes", "internal/alpha"})
		failsWith(t, o.staticCheck(), "must be sorted")
		o.triggers("internal/api", [2]string{"TestVenueOracleRoutes", "internal/store"}, [2]string{"TestVenueOracleRoutes", "internal/store"})
		failsWith(t, o.staticCheck(), "must be sorted")
	})
	t.Run("a row in the wrong package's file fails", func(t *testing.T) {
		t.Parallel()
		o := triggerTree(t)
		rel := o.triggers("internal/api", [2]string{"TestVenueOracleRoutes", "internal/store"})
		if err := os.Rename(filepath.Join(o.dir, rel), filepath.Join(o.dir, "ci", "venue_oracle_triggers.d", "internal__jobs.tsv")); err != nil {
			t.Fatal(err)
		}
		failsWith(t, o.staticCheck(), "belongs in internal__api.tsv")
	})
	t.Run("a stray or empty trigger file fails", func(t *testing.T) {
		t.Parallel()
		o := triggerTree(t)
		o.write("ci/venue_oracle_triggers.d/notes.txt", "x\n")
		failsWith(t, o.staticCheck(), "stray entry notes.txt")
		if err := os.Remove(filepath.Join(o.dir, "ci", "venue_oracle_triggers.d", "notes.txt")); err != nil {
			t.Fatal(err)
		}
		o.write("ci/venue_oracle_triggers.d/internal__api.tsv", "# nothing\n")
		failsWith(t, o.staticCheck(), "holds no rows")
	})
	t.Run("a broken trigger file stops the selection", func(t *testing.T) {
		t.Parallel()
		o := triggerTree(t)
		o.write("ci/venue_oracle_triggers.d/internal__api.tsv", "internal/api\tonly-two\n")
		o.write("changed.txt", "internal/store/store.go\n")
		r := o.venueOracles(nil, "--changed", filepath.Join(o.dir, "changed.txt"), "--list")
		if r.res.code == 0 || !strings.Contains(r.res.stderr, "could not be read") {
			t.Fatalf("exit %d: %s%s", r.res.code, r.res.stdout, r.res.stderr)
		}
	})
}

// The pull request job's own selection step (venue-oracles-changed.yml, step id select), run on a commit that changes
// internal/store.
func runSelectStep(t *testing.T, o *otree) (cmdResult, string) {
	t.Helper()
	job := oracleWorkflowJob(t, "venue-oracles-changed.yml", "venue-oracles-changed")
	var script string
	n := 0
	steps, _ := job["steps"].([]any)
	for _, s := range steps {
		m, _ := s.(map[string]any)
		if m["id"] == "select" {
			script, _ = m["run"].(string)
			n++
		}
	}
	if n != 1 {
		t.Fatalf("expected exactly one select step, found %d", n)
	}
	gitDo := func(args ...string) string {
		res := freeSpec{dir: o.dir, env: gitEnv}.run("git", append([]string{"-c", "commit.gpgsign=false", "-c", "maintenance.auto=false"}, args...)...)
		if res.code != 0 {
			t.Fatalf("git %v: %s%s", args, res.stderr, res.stdout)
		}
		return strings.TrimSpace(res.stdout)
	}
	gitDo("init", "-q", "-b", "main")
	gitDo("add", "-A")
	gitDo("commit", "-q", "-m", "base")
	base := gitDo("rev-parse", "HEAD")
	o.write("internal/store/store.go", "package store\n\n// changed\n")
	gitDo("commit", "-q", "-am", "head")
	head := gitDo("rev-parse", "HEAD")
	out := filepath.Join(o.dir, "github_output.txt")
	o.write("github_output.txt", "")
	cmd := exec.Command("bash", "-c", script)
	cmd.Dir = o.dir
	cmd.Env = inheritedEnv(nil, "BASE_SHA="+base, "HEAD_SHA="+head, "GITHUB_OUTPUT="+out, "VENUE_LEG=1", "VENUE_LEGS=1")
	stdout, err := cmd.Output()
	res := cmdResult{stdout: string(stdout)}
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			res.code, res.stderr = ee.ExitCode(), string(ee.Stderr)
		} else {
			t.Fatal(err)
		}
	}
	return res, readText(t, out)
}

func TestOracleChangedJobSelectStep(t *testing.T) {
	t.Parallel()
	t.Run("the job step runs the tests of a declared path", func(t *testing.T) {
		t.Parallel()
		o := triggerTree(t)
		o.triggers("internal/api", [2]string{"TestVenueOracleRoutes", "internal/store"})
		r, out := runSelectStep(t, o)
		if r.code != 0 || out != "run=true\n" || !strings.Contains(r.stdout, "SELECT\tinternal/api\tTestVenueOracleRoutes") {
			t.Fatalf("exit %d output %q: %s%s", r.code, out, r.stdout, r.stderr)
		}
	})
	t.Run("the job step says not run when nothing is selected", func(t *testing.T) {
		t.Parallel()
		o := triggerTree(t)
		r, out := runSelectStep(t, o)
		if r.code != 0 || out != "run=false\n" || !strings.Contains(r.stdout, "VENUE ORACLES NOT RUN") {
			t.Fatalf("exit %d output %q: %s%s", r.code, out, r.stdout, r.stderr)
		}
	})
	t.Run("the job step fails on a trigger file it cannot read", func(t *testing.T) {
		t.Parallel()
		o := triggerTree(t)
		o.write("ci/venue_oracle_triggers.d/internal__api.tsv", "internal/api\tonly-two\n")
		r, out := runSelectStep(t, o)
		if r.code == 0 || out != "" {
			t.Fatalf("a trigger file that cannot be read was reported as 'nothing selected' (exit %d, output %q): %s", r.code, out, r.stdout)
		}
	})
}
