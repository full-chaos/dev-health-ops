package testshard_test

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
)

// Go port of sections "weights census" (CHAOS-4233), 5 and 6 (CHAOS-8135) of tests/tooling/test_go_quality_legs.py: the
// race-weights census, the missing-row awk, the race leg's refusal to start, and the race-excluded test list. Every guard has a
// ...Planted... companion that feeds the same check a planted defect.

var (
	censusListOnce sync.Once
	censusMod      string
	censusPkgs     []string
	censusListErr  string

	censusFilesOnce  sync.Once
	censusPlain      map[string]censusPkg
	censusRaced      map[string]censusPkg
	censusFilesError string
)

type censusPkg struct {
	Dir  string
	Test []string
}

func censusPackages(t *testing.T) (string, []string) {
	t.Helper()
	censusListOnce.Do(func() {
		env := append(os.Environ(), "GOWORK=off", "GOFLAGS=-mod=readonly")
		for i, args := range [][]string{{"list", "-m"}, {"list", "./..."}} {
			cmd := exec.Command("go", args...)
			cmd.Dir = repoRoot(t)
			cmd.Env = env
			var out, errb bytes.Buffer
			cmd.Stdout, cmd.Stderr = &out, &errb
			if err := cmd.Run(); err != nil {
				censusListErr = "go " + strings.Join(args, " ") + ": " + err.Error() + "\n" + errb.String()
				return
			}
			if i == 0 {
				censusMod = strings.TrimSpace(out.String())
			} else {
				censusPkgs = strings.Fields(out.String())
			}
		}
	})
	if censusListErr != "" {
		t.Fatal(censusListErr)
	}
	return censusMod, censusPkgs
}

func censusGoListFiles(t *testing.T, tags string) map[string]censusPkg {
	t.Helper()
	args := []string{"list", "-json=ImportPath,Dir,TestGoFiles,XTestGoFiles"}
	if tags != "" {
		args = append(args, "-tags", tags)
	}
	cmd := exec.Command("go", append(args, "./...")...)
	cmd.Dir = repoRoot(t)
	cmd.Env = append(os.Environ(), "GOWORK=off", "GOFLAGS=-mod=readonly")
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		censusFilesError = err.Error() + "\n" + errb.String()
		return nil
	}
	files := map[string]censusPkg{}
	dec := json.NewDecoder(&out)
	for dec.More() {
		var p struct {
			ImportPath, Dir           string
			TestGoFiles, XTestGoFiles []string
		}
		if err := dec.Decode(&p); err != nil {
			censusFilesError = err.Error()
			return nil
		}
		files[p.ImportPath] = censusPkg{Dir: p.Dir, Test: append(append([]string{}, p.TestGoFiles...), p.XTestGoFiles...)}
	}
	return files
}

func censusTestFiles(t *testing.T) (map[string]censusPkg, map[string]censusPkg) {
	t.Helper()
	censusFilesOnce.Do(func() {
		censusPlain = censusGoListFiles(t, "")
		if censusFilesError == "" {
			censusRaced = censusGoListFiles(t, "race")
		}
	})
	if censusFilesError != "" {
		t.Fatal(censusFilesError)
	}
	return censusPlain, censusRaced
}

func censusRead(t *testing.T, rel string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(repoRoot(t), rel))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// censusRowKeys returns the first column of every non-comment, non-blank row, in file order.
func censusRowKeys(text string) []string {
	var keys []string
	for _, line := range strings.Split(text, "\n") {
		if strings.TrimSpace(line) == "" || strings.HasPrefix(line, "#") {
			continue
		}
		keys = append(keys, strings.Split(line, "\t")[0])
	}
	return keys
}

func censusWithout(text, victim string) string {
	var b strings.Builder
	for _, line := range strings.Split(strings.TrimRight(text, "\n"), "\n") {
		if strings.HasPrefix(line, victim+"\t") {
			continue
		}
		b.WriteString(line + "\n")
	}
	return b.String()
}

func censusWriteTemp(t *testing.T, name, text string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, []byte(text), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func censusRowsRun(t *testing.T, text string) (string, string, error) {
	t.Helper()
	return checkGo(t, []string{"GO_RACE_WEIGHTS=" + censusWriteTemp(t, "weights.tsv", text)}, "race-weights-rows")
}

const censusVictim = "internal/httpguard"

func TestTheGoSideWeightsCensusAcceptsTheTable(t *testing.T) {
	text := censusRead(t, "ci/go_race_weights.tsv")
	out, errs, err := censusRowsRun(t, text)
	if err != nil {
		t.Fatalf("census refused the real table: %v\n%s", err, errs)
	}
	if !strings.Contains(out, "race-weights-rows:") {
		t.Fatalf("no census summary line: %q", out)
	}
}

func TestTheGoSideWeightsCensusPlantedDefectsEachName(t *testing.T) {
	text := censusRead(t, "ci/go_race_weights.tsv")
	if !strings.Contains(text, censusVictim+"\t") {
		t.Fatalf("%s has no weights row", censusVictim)
	}
	cases := []struct {
		name, text, stderrHas string
	}{
		{"row removed", censusWithout(text, censusVictim), censusVictim},
		{"stale row", text + "internal/zz-gone-package\t1\n", "internal/zz-gone-package"},
		{"indented comment", text + "  # an indented comment\n", ""},
		{"package weighed twice", text + "cmd/auth-migrate\t1\n", "twice"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, errs, err := censusRowsRun(t, c.text)
			if err == nil {
				t.Fatalf("census accepted a planted defect (%s)", c.name)
			}
			if c.stderrHas != "" && !strings.Contains(errs, c.stderrHas) {
				t.Fatalf("stderr does not name %q: %s", c.stderrHas, errs)
			}
		})
	}
	// The removed-row and stale-row diagnostics are distinct guards.
	_, errs, _ := censusRowsRun(t, censusWithout(text, censusVictim))
	if !strings.Contains(errs, "NO row in ci/go_race_weights.tsv") {
		t.Fatalf("missing-row message changed: %s", errs)
	}
	_, errs, _ = censusRowsRun(t, text+"internal/zz-gone-package\t1\n")
	if !strings.Contains(errs, "no longer exist") {
		t.Fatalf("stale-row message changed: %s", errs)
	}
}

// censusVerbRunsCensus reports whether the case arm of verb in source calls check_race_weights_rows.
func censusVerbRunsCensus(source, verb string) bool {
	re := regexp.MustCompile(`(?ms)^  ` + regexp.QuoteMeta(verb) + `\)\n(.*?)^    ;;`)
	if verb == "static" {
		re = regexp.MustCompile(`(?ms)^    static\)\n(.*?)^      ;;`)
	}
	m := re.FindStringSubmatch(source)
	return m != nil && strings.Contains(m[1], "check_race_weights_rows")
}

func TestFastCiAllAndTheStaticLegRunTheWeightsCensus(t *testing.T) {
	source := censusRead(t, "ci/check_go.sh")
	for _, verb := range []string{"fast", "ci", "all", "static"} {
		if !censusVerbRunsCensus(source, verb) {
			t.Errorf("%s does not run check_race_weights_rows", verb)
		}
	}
}

func TestFastCiAllAndTheStaticLegPlantedCensusRemoval(t *testing.T) {
	source := censusRead(t, "ci/check_go.sh")
	for _, verb := range []string{"fast", "ci", "all"} {
		re := regexp.MustCompile(`(?ms)(^  ` + verb + `\)\n.*?)    check_race_weights_rows\n(.*?^    ;;)`)
		planted := re.ReplaceAllString(source, "$1$2")
		if planted == source {
			t.Fatalf("could not plant a census removal in %s", verb)
		}
		if censusVerbRunsCensus(planted, verb) {
			t.Errorf("guard accepted %s without the census", verb)
		}
	}
	planted := strings.Replace(source, "      ci_leg_stage race-weights-rows check_race_weights_rows\n", "", 1)
	if planted == source || censusVerbRunsCensus(planted, "static") {
		t.Error("guard accepted the static leg without the census")
	}
}

func censusMissing(mod string, pkgs []string, weights map[string]bool) []string {
	var missing []string
	for _, p := range pkgs {
		if k := keyOf(mod, p); !weights[k] {
			missing = append(missing, k)
		}
	}
	sort.Strings(missing)
	return missing
}

func censusWeightSet(t *testing.T) map[string]bool {
	t.Helper()
	set := map[string]bool{}
	for _, k := range censusRowKeys(censusRead(t, "ci/go_race_weights.tsv")) {
		set[k] = true
	}
	return set
}

func TestEveryPackageOfTheRootModuleHasAWeightsRow(t *testing.T) {
	mod, pkgs := censusPackages(t)
	if missing := censusMissing(mod, pkgs, censusWeightSet(t)); len(missing) != 0 {
		t.Fatalf("packages with no row in ci/go_race_weights.tsv: %v", missing)
	}
}

func TestEveryPackageOfTheRootModuleHasAWeightsRowPlanted(t *testing.T) {
	mod, pkgs := censusPackages(t)
	set := censusWeightSet(t)
	delete(set, censusVictim)
	if missing := censusMissing(mod, pkgs, set); len(missing) != 1 || missing[0] != censusVictim {
		t.Fatalf("a removed row was not reported: %v", missing)
	}
	if missing := censusMissing(mod, append(append([]string{}, pkgs...), mod+"/internal/zz-new"), censusWeightSet(t)); len(missing) != 1 || missing[0] != "internal/zz-new" {
		t.Fatalf("a new package with no row was not reported: %v", missing)
	}
}

func censusAwk(t *testing.T, mod, weights, packages string) (string, string, error) {
	t.Helper()
	cmd := exec.Command("awk", "-v", "mod="+mod, "-v", "weights="+weights, "-f", filepath.Join(repoRoot(t), "ci", "go_race_missing_rows.awk"))
	cmd.Stdin = strings.NewReader(packages)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	err := cmd.Run()
	return out.String(), errb.String(), err
}

func TestTheMissingRowAwkAcceptsTheRealTable(t *testing.T) {
	mod, pkgs := censusPackages(t)
	out, errs, err := censusAwk(t, mod, filepath.Join(repoRoot(t), "ci", "go_race_weights.tsv"), strings.Join(pkgs, "\n")+"\n")
	if err != nil || out != "" {
		t.Fatalf("awk on the real table: err=%v stdout=%q stderr=%q", err, out, errs)
	}
}

func TestTheMissingRowAwkPlantedRemovedRowAndEmptyInputs(t *testing.T) {
	mod, pkgs := censusPackages(t)
	text := strings.Join(pkgs, "\n") + "\n"
	real := filepath.Join(repoRoot(t), "ci", "go_race_weights.tsv")
	trimmed := censusWriteTemp(t, "trimmed.tsv", censusWithout(censusRead(t, "ci/go_race_weights.tsv"), censusVictim))
	out, _, _ := censusAwk(t, mod, trimmed, text)
	if got := strings.Fields(out); len(got) != 1 || got[0] != censusVictim {
		t.Fatalf("awk did not name exactly the removed row: %q", out)
	}
	empty := censusWriteTemp(t, "empty.tsv", "# no rows\n")
	if _, _, err := censusAwk(t, mod, empty, text); err == nil {
		t.Error("an empty weights file must fail loudly")
	}
	if _, _, err := censusAwk(t, mod, real, ""); err == nil {
		t.Error("an empty package list must fail loudly")
	}
	if _, _, err := censusAwk(t, mod, "", text); err == nil {
		t.Error("an absent weights argument must fail loudly")
	}
}

func TestARaceLegWithAPackageThatHasNoRowFailsBeforeAnyTestRunsPlanted(t *testing.T) {
	trimmed := censusWriteTemp(t, "trimmed.tsv", censusWithout(censusRead(t, "ci/go_race_weights.tsv"), censusVictim))
	r := runCheckGo(t, []string{"GO_RACE_WEIGHTS=" + trimmed}, false, "ci-leg", "race", "1", "3")
	if r.Err == nil {
		t.Fatalf("race leg passed with a missing row: %s", r.Stdout)
	}
	if !strings.Contains(r.Stderr, "NO row in ci/go_race_weights.tsv") || !strings.Contains(r.Stderr, censusVictim) {
		t.Fatalf("stderr does not name the missing row: %s", r.Stderr)
	}
	if len(r.Calls) != 0 {
		t.Fatalf("go test ran with a package that has no row: %v", r.Calls)
	}
	if strings.Contains(r.Stdout, "ci-leg race: OK") {
		t.Fatal("leg reported OK")
	}
}

func TestARaceLegWithAnEmptyWeightsFileFailsBeforeAnyTestRunsPlanted(t *testing.T) {
	empty := censusWriteTemp(t, "empty.tsv", "# no rows\n")
	r := runCheckGo(t, []string{"GO_RACE_WEIGHTS=" + empty}, false, "ci-leg", "race", "1", "3")
	if r.Err == nil {
		t.Fatalf("race leg passed with an empty weights file: %s", r.Stdout)
	}
	if !strings.Contains(r.Stderr, "go_race_missing_rows.awk failed") {
		t.Fatalf("stderr does not name the awk failure: %s", r.Stderr)
	}
	if len(r.Calls) != 0 {
		t.Fatalf("go test ran with no weights: %v", r.Calls)
	}
}

type censusExcludedRow struct{ pkg, name string }

var censusTestFunc = regexp.MustCompile(`(?m)^func (Test\w+)\(`)

// censusExcludedFound derives, from the plain and race-tagged listings, the (package, Test) pairs compiled without -race only.
func censusExcludedFound(mod string, plain, raced map[string]censusPkg, read func(string) (string, error)) (map[censusExcludedRow]bool, error) {
	found := map[censusExcludedRow]bool{}
	for pkg, p := range plain {
		in := map[string]bool{}
		for _, n := range raced[pkg].Test {
			in[n] = true
		}
		for _, n := range p.Test {
			if in[n] {
				continue
			}
			text, err := read(filepath.Join(p.Dir, n))
			if err != nil {
				return nil, err
			}
			for _, m := range censusTestFunc.FindAllStringSubmatch(text, -1) {
				found[censusExcludedRow{keyOf(mod, pkg), m[1]}] = true
			}
		}
	}
	return found, nil
}

func censusExcludedListed(t *testing.T) map[censusExcludedRow]bool {
	t.Helper()
	listed := map[censusExcludedRow]bool{}
	for _, line := range strings.Split(censusRead(t, "ci/race_excluded_tests.tsv"), "\n") {
		if strings.TrimSpace(line) == "" || strings.HasPrefix(line, "#") {
			continue
		}
		f := strings.Split(line, "\t")
		if len(f) != 2 {
			t.Fatalf("bad race_excluded_tests.tsv row %q", line)
		}
		listed[censusExcludedRow{f[0], f[1]}] = true
	}
	if len(listed) == 0 {
		t.Fatal("ci/race_excluded_tests.tsv lists no test")
	}
	return listed
}

func censusDiff(a, b map[censusExcludedRow]bool) []censusExcludedRow {
	var d []censusExcludedRow
	for r := range a {
		if !b[r] {
			d = append(d, r)
		}
	}
	return d
}

func censusReadFile(p string) (string, error) {
	b, err := os.ReadFile(p)
	return string(b), err
}

func TestEveryRaceExcludedTestIsListed(t *testing.T) {
	mod, _ := censusPackages(t)
	plain, raced := censusTestFiles(t)
	if len(plain) == 0 {
		t.Fatal("go list found no package")
	}
	found, err := censusExcludedFound(mod, plain, raced, censusReadFile)
	if err != nil {
		t.Fatal(err)
	}
	listed := censusExcludedListed(t)
	if u, l := censusDiff(found, listed), censusDiff(listed, found); len(u) != 0 || len(l) != 0 {
		t.Fatalf("tests compiled without -race only and ci/race_excluded_tests.tsv differ: unlisted %v, listed but compiled in the race leg %v", u, l)
	}
}

func TestEveryRaceExcludedTestIsListedPlanted(t *testing.T) {
	mod, _ := censusPackages(t)
	plain, raced := censusTestFiles(t)
	listed := censusExcludedListed(t)
	found, err := censusExcludedFound(mod, plain, raced, censusReadFile)
	if err != nil {
		t.Fatal(err)
	}
	// A listed row that is not excluded: the diff reports it as "listed but compiled in the race leg".
	extra := map[censusExcludedRow]bool{{"internal/zz-none", "TestZz"}: true}
	for r := range listed {
		extra[r] = true
	}
	if l := censusDiff(extra, found); len(l) == 0 {
		t.Error("a planted extra row was not reported")
	}
	// A row dropped from the list: the real exclusion is reported as unlisted.
	var drop censusExcludedRow
	for r := range listed {
		drop = r
		break
	}
	short := map[censusExcludedRow]bool{}
	for r := range listed {
		if r != drop {
			short[r] = true
		}
	}
	if u := censusDiff(found, short); len(u) != 1 || u[0] != drop {
		t.Errorf("a dropped row was not reported: %v", u)
	}
	// A planted !race-only test file in a package is derived as excluded.
	plantedPlain := map[string]censusPkg{"m/pkg": {Dir: "/d", Test: []string{"a_test.go", "b_test.go"}}}
	plantedRaced := map[string]censusPkg{"m/pkg": {Dir: "/d", Test: []string{"a_test.go"}}}
	got, err := censusExcludedFound("m", plantedPlain, plantedRaced, func(p string) (string, error) {
		if strings.HasSuffix(p, "b_test.go") {
			return "//go:build !race\n\npackage pkg\n\nfunc TestPlantedOnly(t *testing.T) {}\n", nil
		}
		return "func TestShared(t *testing.T) {}\n", nil
	})
	if err != nil || len(got) != 1 || !got[censusExcludedRow{"pkg", "TestPlantedOnly"}] {
		t.Errorf("planted race-excluded file not derived: %v %v", got, err)
	}
}
