//go:build !race

package scripts_test

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// Every venue oracle test is in `ci/venue_oracle_registry.d/`, and the registry is what runs. ci/venue_oracle_discovery.awk
// finds venue tests by STRUCTURE (a call that reaches internal/testsupport/venueoracle); a test it cannot reach was dropped
// from the hosted venue-oracles job while the job read green. ci/check_venue_oracle_registry.sh fails when a venue test
// exists (found by structure OR by a `Test*VenueOracle*` name) that the registry does not name, or the registry names one
// that does not exist. Each guard is proved against a planted defect in a scratch tree: the baseline passes, the defect
// fails and names the culprit.

var venueRegistryCIFiles = []string{
	"ci/check_go.sh", "ci/check_venue_oracle_registry.sh", "ci/venue_oracle_discovery.awk", "ci/venue_oracle_names.awk",
	"ci/venue_oracle_proof.awk", "ci/lib/venue_oracle_registry.sh",
}

const venueHarness = `"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"`

type vgo struct {
	pkg     string
	funcs   []string
	harness bool
	marker  string
	proof   string
	noProof bool
}

func (g vgo) src() string {
	proof := g.proof
	if proof == "" && !g.noProof {
		proof = "\tvenueoracle.Diff(t)\n"
	}
	body := "package " + g.pkg + "\n\n"
	if g.harness {
		body += "import (\n\t" + venueHarness + "\n)\n\n"
	}
	for _, name := range g.funcs {
		if g.marker == name {
			body += "//venueoracle:local-only needs a key CI does not hold\n"
		}
		body += fmt.Sprintf("func %s(t *testing.T) {\n%s}\n\n", name, proof)
	}
	return body
}

type vtree struct {
	t    *testing.T
	dir  string
	root string
}

func (v *vtree) write(rel, text string) {
	v.t.Helper()
	p := filepath.Join(v.dir, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		v.t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(text), 0o644); err != nil {
		v.t.Fatal(err)
	}
}

func (v *vtree) read(rel string) string {
	v.t.Helper()
	return readText(v.t, filepath.Join(v.dir, filepath.FromSlash(rel)))
}

func (v *vtree) registry(rows []string, sorted bool) {
	v.t.Helper()
	dir := filepath.Join(v.dir, "ci", "venue_oracle_registry.d")
	if err := os.RemoveAll(dir); err != nil {
		v.t.Fatal(err)
	}
	groups := map[string][]string{}
	var order []string
	for _, line := range rows {
		if strings.TrimSpace(line) == "" {
			continue
		}
		pkg := strings.SplitN(line, "\t", 2)[0]
		if _, ok := groups[pkg]; !ok {
			order = append(order, pkg)
		}
		groups[pkg] = append(groups[pkg], line)
	}
	for _, pkg := range order {
		g := groups[pkg]
		if sorted {
			sort.Strings(g)
		}
		v.write("ci/venue_oracle_registry.d/"+strings.ReplaceAll(pkg, "/", "__")+".tsv", "# test tree registry\n"+strings.Join(g, "\n")+"\n")
	}
	if len(order) == 0 {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			v.t.Fatal(err)
		}
	}
}

func (v *vtree) rows() []string {
	v.t.Helper()
	files, _ := filepath.Glob(filepath.Join(v.dir, "ci", "venue_oracle_registry.d", "*.tsv"))
	sort.Strings(files)
	var out []string
	for _, f := range files {
		for _, l := range strings.Split(readText(v.t, f), "\n") {
			if l != "" && !strings.HasPrefix(l, "#") {
				out = append(out, l)
			}
		}
	}
	return out
}

func newVTree(t *testing.T) *vtree {
	t.Helper()
	v := &vtree{t: t, dir: t.TempDir(), root: rootOf(t)}
	for _, f := range venueRegistryCIFiles {
		v.write(f, readText(t, filepath.Join(v.root, filepath.FromSlash(f))))
	}
	v.write("go.mod", "module example.com/x\n")
	v.write("internal/a/a_test.go", vgo{pkg: "a", funcs: []string{"TestAlpha", "TestBeta"}, harness: true}.src())
	v.write("internal/b/b_test.go", vgo{pkg: "b", funcs: []string{"TestVenueOracleGamma"}}.src())
	v.write("internal/c/c_test.go", vgo{pkg: "c", funcs: []string{"TestDelta", "TestEpsilonVenueOracle"}, harness: true, marker: "TestEpsilonVenueOracle"}.src())
	v.registry([]string{
		"internal/a\tTestAlpha\trun", "internal/a\tTestBeta\trun", "internal/b\tTestVenueOracleGamma\trun",
		"internal/c\tTestDelta\trun", "internal/c\tTestEpsilonVenueOracle\tlocal",
	}, true)
	return v
}

func (v *vtree) run(script string, env []string, verb ...string) cmdResult {
	v.t.Helper()
	tmp := filepath.Join(v.dir, "tmp")
	if err := os.MkdirAll(tmp, 0o755); err != nil {
		v.t.Fatal(err)
	}
	e := inheritedEnv(nil, append([]string{"TMPDIR=" + tmp, "DEV_HEALTH_GO_CACHE=" + filepath.Join(tmp, "gocache")}, env...)...)
	return cmdSpec{dir: v.dir, env: e}.run("bash", append([]string{filepath.Join(v.dir, "ci", script)}, verb...)...)
}

func (v *vtree) check() cmdResult { return v.run("check_venue_oracle_registry.sh", nil) }

func venueFails(t *testing.T, r cmdResult, needles ...string) {
	t.Helper()
	if r.code == 0 {
		t.Fatalf("accepted:\n%s%s", r.stdout, r.stderr)
	}
	for _, n := range needles {
		if !strings.Contains(r.stderr, n) {
			t.Errorf("stderr lacks %q:\n%s", n, r.stderr)
		}
	}
}

func venuePasses(t *testing.T, r cmdResult, stdout ...string) {
	t.Helper()
	if r.code != 0 {
		t.Fatalf("rejected (exit %d):\n%s%s", r.code, r.stdout, r.stderr)
	}
	for _, n := range stdout {
		if !strings.Contains(r.stdout, n) {
			t.Errorf("stdout lacks %q:\n%s", n, r.stdout)
		}
	}
}

func TestRegistryGuardRealTree(t *testing.T) {
	root := rootOf(t)
	r := cmdSpec{dir: root, env: inheritedEnv(nil)}.run("bash", "ci/check_venue_oracle_registry.sh")
	if r.code != 0 {
		t.Fatalf("exit %d:\n%s%s", r.code, r.stderr, r.stdout)
	}
	v := &vtree{t: t, dir: root}
	registered := v.rows()
	if len(registered) < 45 {
		t.Fatalf("only %d registry rows: an empty or truncated registry cannot read as covered", len(registered))
	}
	if !strings.Contains(r.stdout, fmt.Sprintf("%d registered test(s)", len(registered))) {
		t.Errorf("stdout does not report %d registered tests:\n%s", len(registered), r.stdout)
	}
}

func TestRegistryGuardPlantedDefects(t *testing.T) {
	t.Run("the scratch baseline passes", func(t *testing.T) {
		venuePasses(t, newVTree(t).check(), "5 registered test(s)")
	})
	t.Run("a structure found test left out of the registry fails", func(t *testing.T) {
		v := newVTree(t)
		v.write("internal/a/extra_test.go", vgo{pkg: "a", funcs: []string{"TestZeta"}, harness: true}.src())
		venueFails(t, v.check(), "internal/a TestZeta", "NOT in ci/venue_oracle_registry.d/", "internal__a.tsv")
	})
	t.Run("a name found test in a non harness package left out fails", func(t *testing.T) {
		v := newVTree(t)
		v.write("internal/d/d_test.go", vgo{pkg: "d", funcs: []string{"TestVenueOracleOmega"}}.src())
		venueFails(t, v.check(), "internal/d TestVenueOracleOmega")
	})
	t.Run("a registry row with no test fails", func(t *testing.T) {
		v := newVTree(t)
		v.registry(append(v.rows(), "internal/a\tTestGhost\trun"), true)
		venueFails(t, v.check(), "internal/a TestGhost", "found 0")
	})
	t.Run("a registry row with no package fails", func(t *testing.T) {
		v := newVTree(t)
		v.registry(append(v.rows(), "internal/zzz\tTestNowhere\trun"), true)
		venueFails(t, v.check(), "package dir internal/zzz does not exist")
	})
	t.Run("a test declared twice in one package fails", func(t *testing.T) {
		v := newVTree(t)
		v.write("internal/a/a_x_test.go", vgo{pkg: "a_test", funcs: []string{"TestAlpha"}, harness: true}.src())
		venueFails(t, v.check(), "internal/a TestAlpha", "found 2")
	})
	t.Run("a mode contradicting the local only marker fails", func(t *testing.T) {
		v := newVTree(t)
		var rows []string
		for _, r := range v.rows() {
			rows = append(rows, strings.Replace(r, "TestEpsilonVenueOracle\tlocal", "TestEpsilonVenueOracle\trun", 1))
		}
		v.registry(rows, true)
		venueFails(t, v.check(), "TestEpsilonVenueOracle is registered as run")
	})
	for _, c := range []struct{ name, raw, needle string }{
		{"empty", "", "holds no rows"},
		{"two columns", "internal/a\tTestAlpha\n", "is not <package>"},
		{"unknown mode", "internal/a\tTestAlpha\tmaybe\n", "is not <package>"},
	} {
		t.Run("a malformed registry fails: "+c.name, func(t *testing.T) {
			v := newVTree(t)
			v.registry(strings.Split(strings.TrimRight(c.raw, "\n"), "\n"), true)
			venueFails(t, v.check(), c.needle)
		})
	}
	t.Run("an unsorted registry fails", func(t *testing.T) {
		v := newVTree(t)
		rows := v.rows()
		for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
			rows[i], rows[j] = rows[j], rows[i]
		}
		v.registry(rows, false)
		venueFails(t, v.check(), "must be sorted and unique within a file")
	})
	t.Run("a duplicate registry row fails", func(t *testing.T) {
		v := newVTree(t)
		rows := v.rows()
		rows = append([]string{rows[0], rows[0]}, rows[1:]...)
		v.registry(rows, false)
		venueFails(t, v.check(), "must be sorted and unique within a file")
	})
	t.Run("a missing registry directory fails", func(t *testing.T) {
		v := newVTree(t)
		if err := os.RemoveAll(filepath.Join(v.dir, "ci", "venue_oracle_registry.d")); err != nil {
			t.Fatal(err)
		}
		venueFails(t, v.check(), "does not exist")
	})
	t.Run("a row left in the retired single file fails", func(t *testing.T) {
		v := newVTree(t)
		v.write("ci/venue_oracle_registry.tsv", "internal/a\tTestLegacyRow\trun\n")
		r := v.check()
		venueFails(t, r, "must not exist any more")
		if strings.Contains(r.stdout, "TestLegacyRow") {
			t.Error("the retired file's row was read")
		}
	})
	t.Run("a row in the wrong package's file fails", func(t *testing.T) {
		v := newVTree(t)
		v.write("ci/venue_oracle_registry.d/internal__b.tsv", v.read("ci/venue_oracle_registry.d/internal__b.tsv")+"internal/a\tTestStrayRow\trun\n")
		venueFails(t, v.check(), "package internal/a belongs in internal__a.tsv")
	})
	t.Run("a stray file in the registry directory fails", func(t *testing.T) {
		v := newVTree(t)
		v.write("ci/venue_oracle_registry.d/internal__a.tsv.orig", "internal/a\tTestAlpha\trun\n")
		venueFails(t, v.check(), "stray entry internal__a.tsv.orig")
	})
	t.Run("an empty package file fails", func(t *testing.T) {
		v := newVTree(t)
		v.write("ci/venue_oracle_registry.d/internal__e.tsv", "# nothing\n")
		venueFails(t, v.check(), "internal__e.tsv holds no rows")
	})
	t.Run("two prs adding tests in different packages touch different files", func(t *testing.T) {
		v := newVTree(t)
		v.write("internal/f/f_test.go", vgo{pkg: "f", funcs: []string{"TestVenueOracleF"}, harness: true}.src())
		v.write("ci/venue_oracle_registry.d/internal__f.tsv", "internal/f\tTestVenueOracleF\trun\n")
		venuePasses(t, v.check(), "6 registered test(s)")
	})
}

const venueFakeGo = `#!/usr/bin/env bash
case "$1" in
  version) echo "go version go1.27.2 linux/amd64"; exit 0 ;;
  env) echo "${FAKE_GOROOT}"; exit 0 ;;
esac
pattern="" ; pkg=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    -run) pattern="$2"; shift ;;
    ./*) pkg="$1" ;;
  esac
  shift
done
printf '%s %s\n' "${pkg}" "${pattern}" >> "${FAKE_GO_LOG}"
names="${pattern#^(}"; names="${names%)\$}"
IFS='|' read -ra list <<< "${names}"
for n in "${list[@]}"; do
  case "${FAKE_GO_SKIP_PROOF:-}" in "${n}") continue ;; esac
  printf 'executed' > "${DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR}/${n}"
done
exit 0
`

func (v *vtree) verb(skipProof string) (cmdResult, []string) {
	v.t.Helper()
	bin := filepath.Join(v.dir, "bin")
	goroot := filepath.Join(v.dir, "fakeroot")
	log := filepath.Join(v.dir, "fake_go.log")
	v.write("bin/go", venueFakeGo)
	v.write("fakeroot/bin/gofmt", "#!/bin/sh\n")
	v.write("fake_go.log", "")
	for _, p := range []string{filepath.Join(bin, "go"), filepath.Join(goroot, "bin", "gofmt")} {
		if err := os.Chmod(p, 0o755); err != nil {
			v.t.Fatal(err)
		}
	}
	r := v.run("check_go.sh", []string{"PATH=" + bin + string(os.PathListSeparator) + os.Getenv("PATH"), "FAKE_GO_LOG=" + log, "FAKE_GOROOT=" + goroot, "FAKE_GO_SKIP_PROOF=" + skipProof}, "venue-oracles")
	var calls []string
	for _, l := range strings.Split(strings.TrimRight(readText(v.t, log), "\n"), "\n") {
		if l != "" {
			calls = append(calls, l)
		}
	}
	sort.Strings(calls)
	return r, calls
}

func TestRegistryVerbRunsTheRegistry(t *testing.T) {
	t.Run("the verb runs exactly the registry", func(t *testing.T) {
		r, calls := newVTree(t).verb("")
		venuePasses(t, r, "4 registered test(s) across 3 package(s)", "local-only: TestEpsilonVenueOracle")
		want := []string{"./internal/a ^(TestAlpha|TestBeta)$", "./internal/b ^(TestVenueOracleGamma)$", "./internal/c ^(TestDelta)$"}
		if strings.Join(calls, "|") != strings.Join(want, "|") {
			t.Errorf("go test calls %v, want %v", calls, want)
		}
		if strings.Contains(r.stdout, "5 registered test(s)") {
			t.Error("the local-only row was counted as runnable")
		}
	})
	t.Run("the verb runs a registry only test discovery cannot reach", func(t *testing.T) {
		v := newVTree(t)
		v.write("internal/e/e_test.go", vgo{pkg: "e", funcs: []string{"TestReachesHarnessSomeOtherWay"}}.src())
		v.registry(append(v.rows(), "internal/e\tTestReachesHarnessSomeOtherWay\trun"), true)
		r, calls := v.verb("")
		venuePasses(t, r)
		if !contains(calls, "./internal/e ^(TestReachesHarnessSomeOtherWay)$") {
			t.Errorf("calls %v lack the registry-only test", calls)
		}
	})
	t.Run("the verb fails before running anything on an unregistered test", func(t *testing.T) {
		v := newVTree(t)
		v.write("internal/d/d_test.go", vgo{pkg: "d", funcs: []string{"TestVenueOracleOmega"}}.src())
		r, calls := v.verb("")
		venueFails(t, r, "internal/d TestVenueOracleOmega")
		if len(calls) != 0 {
			t.Errorf("the verb ran go test before validating the registry: %v", calls)
		}
	})
	t.Run("the verb fails when a registered test writes no proof", func(t *testing.T) {
		r, _ := newVTree(t).verb("TestBeta")
		venueFails(t, r, "venue oracle TestBeta did not run a real comparison")
	})
	t.Run("a package registered only local only fails", func(t *testing.T) {
		v := newVTree(t)
		v.write("internal/f/f_test.go", vgo{pkg: "f", funcs: []string{"TestOnlyVenueOracle"}, harness: true, marker: "TestOnlyVenueOracle"}.src())
		v.registry(append(v.rows(), "internal/f\tTestOnlyVenueOracle\tlocal"), true)
		r, _ := v.verb("")
		venueFails(t, r, "internal/f is registered but no test there is run")
	})
}

func contains(l []string, s string) bool {
	for _, x := range l {
		if x == s {
			return true
		}
	}
	return false
}

func TestRegistryRunRowsCanLeaveAProof(t *testing.T) {
	proofless := func(v *vtree, name, proof string) {
		v.write("internal/a/a_test.go", vgo{pkg: "a", funcs: []string{"TestAlpha"}, harness: true}.src()+vgo{pkg: "a", funcs: []string{name}, harness: true, proof: proof, noProof: proof == ""}.src())
	}
	t.Run("a run row that cannot write a proof fails at pr time", func(t *testing.T) {
		v := newVTree(t)
		venuePasses(t, v.check())
		proofless(v, "TestBeta", "")
		r := v.check()
		venueFails(t, r, "internal/a TestBeta is a run row")
		if strings.Contains(r.stderr, "TestAlpha is a run row") {
			t.Error("TestAlpha, which writes a proof, was reported")
		}
	})
	t.Run("the go only proof and the exported proof both count", func(t *testing.T) {
		v := newVTree(t)
		proofless(v, "TestBeta", "\tvenueoracle.WriteGoOnlyProof(t, \"checks the log lines\")\n")
		venuePasses(t, v.check())
		proofless(v, "TestBeta", "\tvenueoracle.WriteProof(t)\n")
		venuePasses(t, v.check())
	})
	t.Run("a proof written by a helper the test calls counts", func(t *testing.T) {
		v := newVTree(t)
		proofless(v, "TestBeta", "\thelper(t)\n")
		helper := "package a\n\nfunc helper(t *testing.T) {\n\tinner(t)\n}\n\nfunc inner(t *testing.T) {\n\tvenueoracle.Diff(t)\n}\n"
		v.write("internal/a/helper_test.go", helper)
		venuePasses(t, v.check())
		v.write("internal/a/helper_test.go", strings.Replace(helper, "venueoracle.Diff(t)", "", 1))
		venueFails(t, v.check(), "internal/a TestBeta is a run row")
	})
	t.Run("a proof call named only in a comment does not count", func(t *testing.T) {
		v := newVTree(t)
		proofless(v, "TestBeta", "\t// venueoracle.Diff(t) is called by the harness\n")
		venueFails(t, v.check(), "internal/a TestBeta is a run row")
	})
	t.Run("a proof through a harness import alias counts", func(t *testing.T) {
		v := newVTree(t)
		text := vgo{pkg: "a", funcs: []string{"TestAlpha"}, harness: true, proof: "\tvo.Diff(t)\n"}.src() + "func TestBeta(t *testing.T) {\n\tvo.WriteProof(t)\n}\n"
		v.write("internal/a/a_test.go", strings.Replace(text, venueHarness, "vo "+venueHarness, 1))
		venuePasses(t, v.check())
	})
	t.Run("a local only row needs no proof", func(t *testing.T) {
		v := newVTree(t)
		text := vgo{pkg: "c", funcs: []string{"TestDelta", "TestEpsilonVenueOracle"}, harness: true, marker: "TestEpsilonVenueOracle", noProof: true}.src()
		v.write("internal/c/c_test.go", strings.Replace(text, "func TestDelta(t *testing.T) {\n}", "func TestDelta(t *testing.T) {\n\tvenueoracle.Diff(t)\n}", 1))
		venuePasses(t, v.check())
	})
}
