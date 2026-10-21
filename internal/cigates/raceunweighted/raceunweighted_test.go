package raceunweighted_test

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// CHAOS-10481: the providersync race leg runs go test -json and fails, naming the test, when a top-level test took >= 30 s and
// has no row in ci/go_providersync_race_weights.tsv. These tests drive the real ci/check_go.sh with a stand-in go that replays a
// canned -json stream; nothing here starts a container or a real test run.

const standInGo = `#!/usr/bin/env bash
if [ "$1" != "test" ]; then exec "%[1]s" "$@"; fi
case " $* " in *" ./internal/providersync "*) ;; *) exit 0 ;; esac
case " $* " in
  *" -list "*) printf 'TestAlpha\nTestBeta\nTestGamma\nTestDelta\nTestWeighted\nok  \tpkg\t0.01s\n'; exit 0 ;;
esac
printf '%%s\n' "$*" >> "${FAKE_GO_LOG}"
cat "${FAKE_GO_JSON}"
exit "${FAKE_GO_RC:-0}"
`

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

func ev(action, test string, elapsed float64) string {
	if action == "pass" || action == "fail" {
		return fmt.Sprintf(`{"Time":"2026-01-01T00:00:00Z","Action":%q,"Package":"p","Test":%q,"Elapsed":%v}`, action, test, elapsed)
	}
	return fmt.Sprintf(`{"Time":"2026-01-01T00:00:00Z","Action":%q,"Package":"p","Test":%q}`, action, test)
}

func out(test, text string) string {
	return fmt.Sprintf(`{"Time":"2026-01-01T00:00:00Z","Action":"output","Package":"p","Test":%q,"Output":%q}`, test, text)
}

type run struct {
	Stdout, Stderr string
	Err            error
	Code           int
	JSONDir        string
	Calls          []string
}

// leg runs `check_go.sh ci-leg race SHARD 3` with the stand-in go replaying stream; weights is the TSV text ("" = the real table).
func leg(t *testing.T, shard string, stream []string, rc int, weights string, env ...string) run {
	t.Helper()
	realGo, err := exec.LookPath("go")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	for _, d := range []string{bin, filepath.Join(dir, "tmp"), filepath.Join(dir, "json")} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	write := func(p, s string) string {
		if err := os.WriteFile(p, []byte(s), 0o755); err != nil {
			t.Fatal(err)
		}
		return p
	}
	write(filepath.Join(bin, "go"), fmt.Sprintf(standInGo, realGo))
	jsonFile := write(filepath.Join(dir, "stream.json"), strings.Join(stream, "\n")+"\n")
	log := write(filepath.Join(dir, "calls.log"), "")
	full := append(os.Environ(), "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"),
		"FAKE_GO_JSON="+jsonFile, "FAKE_GO_LOG="+log, fmt.Sprintf("FAKE_GO_RC=%d", rc),
		"TMPDIR="+filepath.Join(dir, "tmp"), "GO_TEST_JSON_DIR="+filepath.Join(dir, "json"))
	if weights != "" {
		full = append(full, "GO_PROVIDERSYNC_RACE_WEIGHTS="+write(filepath.Join(dir, "w.tsv"), weights))
	}
	full = append(full, env...)
	root := repoRoot(t)
	cmd := exec.Command("bash", filepath.Join(root, "ci", "check_go.sh"), "ci-leg", "race", shard, "3")
	cmd.Dir, cmd.Env = root, full
	var so, se bytes.Buffer
	cmd.Stdout, cmd.Stderr = &so, &se
	runErr := cmd.Run()
	code := 0
	if ee, ok := runErr.(*exec.ExitError); ok {
		code = ee.ExitCode()
	}
	b, _ := os.ReadFile(log)
	var calls []string
	for _, l := range strings.Split(string(b), "\n") {
		if l != "" {
			calls = append(calls, l)
		}
	}
	return run{so.String(), se.String(), runErr, code, filepath.Join(dir, "json"), calls}
}

const rows = "# header\nTestSlowWeighted\t40000\nTestWeighted\t1500\n"

func TestUnweightedTopLevelTestOf31SecondsFailsTheLegAndNamesItAndItsSeconds(t *testing.T) {
	t.Parallel()
	r := leg(t, "1", []string{ev("run", "TestSlowNew", 0), ev("pass", "TestSlowNew", 31.25)}, 0, rows)
	if r.Err == nil {
		t.Fatalf("leg passed with an unweighted 31 s test: %s", r.Stdout)
	}
	for _, want := range []string{"unweighted-slow-test: TestSlowNew", "31.25s", "add a row to ci/go_providersync_race_weights.tsv (median ms of 3 race runs)"} {
		if !strings.Contains(r.Stderr, want) {
			t.Errorf("stderr misses %q: %s", want, r.Stderr)
		}
	}
	if strings.Contains(r.Stdout, "ci-leg race: OK") {
		t.Errorf("leg reported success: %s", r.Stdout)
	}
}

func TestPlantedWeightedSlowTestPassesTheSameLeg(t *testing.T) {
	t.Parallel()
	r := leg(t, "1", []string{ev("run", "TestSlowWeighted", 0), ev("pass", "TestSlowWeighted", 31.25)}, 0, rows)
	if r.Err != nil {
		t.Fatalf("leg failed: %v\n%s%s", r.Err, r.Stderr, r.Stdout)
	}
	if len(r.Calls) != 1 || !strings.Contains(r.Calls[0], "-json") || !strings.HasSuffix(r.Calls[0], "./internal/providersync") {
		t.Errorf("providersync run call: %v", r.Calls)
	}
}

func TestPlantedRealWeightsRowCoversItsTest(t *testing.T) {
	t.Parallel()
	r := leg(t, "1", []string{ev("pass", "TestCompactionPlannerEqualsThe9556Planner", 31)}, 0, "")
	if r.Err != nil {
		t.Fatalf("a test with a row in the real table failed the leg: %v\n%s", r.Err, r.Stderr)
	}
}

func TestThresholdIsThirtySecondsInclusive(t *testing.T) {
	t.Parallel()
	if r := leg(t, "1", []string{ev("pass", "TestNew", 29.99)}, 0, rows); r.Err != nil {
		t.Errorf("29.99 s failed: %s", r.Stderr)
	}
	r := leg(t, "1", []string{ev("pass", "TestNew", 30)}, 0, rows)
	if r.Err == nil || !strings.Contains(r.Stderr, "unweighted-slow-test: TestNew") {
		t.Errorf("exactly 30 s passed: %v %s", r.Err, r.Stderr)
	}
}

func TestThresholdEnvOverrideApplies(t *testing.T) {
	t.Parallel()
	r := leg(t, "1", []string{ev("pass", "TestNew", 12)}, 0, rows, "DEV_HEALTH_GO_RACE_UNWEIGHTED_SLOW_SECONDS=10")
	if r.Err == nil {
		t.Fatal("12 s passed under a 10 s threshold")
	}
}

func TestSubtestOver30SecondsUnderAWeightedParentPasses(t *testing.T) {
	t.Parallel()
	r := leg(t, "1", []string{ev("run", "TestSlowWeighted", 0), ev("run", "TestSlowWeighted/sub", 0),
		ev("pass", "TestSlowWeighted/sub", 40), ev("pass", "TestSlowWeighted", 41)}, 0, rows)
	if r.Err != nil {
		t.Fatalf("subtest under a weighted parent failed the leg: %s", r.Stderr)
	}
}

func TestUnweightedParentIsNamedNotItsSubtest(t *testing.T) {
	t.Parallel()
	r := leg(t, "1", []string{ev("pass", "TestNew/sub", 40), ev("pass", "TestNew/other", 3), ev("pass", "TestNew", 43)}, 0, rows)
	if r.Err == nil || !strings.Contains(r.Stderr, "unweighted-slow-test: TestNew took 43s") {
		t.Fatalf("parent not named: %v %s", r.Err, r.Stderr)
	}
	if strings.Contains(r.Stderr, "TestNew/") {
		t.Errorf("a subtest is named: %s", r.Stderr)
	}
}

func TestWeightLookupIsExactNameOnly(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"TestSlowWeightedX", "TestSlowWeight", "testslowweighted", "TestSlowWeighted "} {
		r := leg(t, "1", []string{ev("pass", name, 35)}, 0, rows)
		if r.Err == nil || !strings.Contains(r.Stderr, "unweighted-slow-test: "+name+" took") {
			t.Errorf("near-miss %q counted as weighted: %v %s", name, r.Err, r.Stderr)
		}
	}
	if r := leg(t, "1", []string{ev("pass", "TestSlowWeighted", 35)}, 0, rows); r.Err != nil {
		t.Errorf("exact name failed: %s", r.Stderr)
	}
}

func TestTestThatNeverEndsIsNamedWhenGoTestTimesOut(t *testing.T) {
	t.Parallel()
	r := leg(t, "1", []string{ev("run", "TestHangs", 0), ev("run", "TestDone", 0), ev("pass", "TestDone", 1)}, 2, rows)
	if r.Err == nil || !strings.Contains(r.Stderr, "unfinished-test: TestHangs") {
		t.Fatalf("unfinished test not named: %v %s", r.Err, r.Stderr)
	}
}

func TestStreamWithNoTopLevelResultFailsTheGuard(t *testing.T) {
	t.Parallel()
	for name, stream := range map[string][]string{"empty": {""}, "output only": {out("", "FAIL\n")}, "subtests only": {ev("pass", "TestA/x", 1)}} {
		r := leg(t, "1", stream, 0, rows)
		if r.Err == nil || !strings.Contains(r.Stderr, "holds no top-level test result") {
			t.Errorf("%s: guard passed on nothing: %v %s", name, r.Err, r.Stderr)
		}
	}
}

func TestLegExitCodeIsTheLargerOfGoTestAndGuard(t *testing.T) {
	t.Parallel()
	clean, slow := []string{ev("pass", "TestWeighted", 1)}, []string{ev("pass", "TestNew", 31)}
	if r := leg(t, "1", clean, 1, rows); r.Err == nil {
		t.Error("a failing go test with a clean stream passed the leg")
	}
	if r := leg(t, "1", slow, 0, rows); r.Err == nil {
		t.Error("a passing go test with a slow unweighted test passed the leg")
	}
	both := leg(t, "1", slow, 1, rows)
	one := leg(t, "1", clean, 1, rows)
	if both.Err == nil || both.Code < one.Code || !strings.Contains(both.Stderr, "unweighted-slow-test: TestNew") {
		t.Errorf("a failing run hid the guard or lowered the code: %d vs %d %s", both.Code, one.Code, both.Stderr)
	}
	if r := leg(t, "1", clean, 0, rows); r.Err != nil {
		t.Errorf("clean run failed: %s", r.Stderr)
	}
}

func TestPlainLogStaysReadable(t *testing.T) {
	t.Parallel()
	r := leg(t, "1", []string{ev("run", "TestWeighted", 0), out("TestWeighted", "=== RUN   TestWeighted\nhello from the test\n"), ev("pass", "TestWeighted", 1)}, 0, rows)
	if r.Err != nil {
		t.Fatalf("leg failed: %s", r.Stderr)
	}
	if !strings.Contains(r.Stdout, "=== RUN   TestWeighted\nhello from the test\n") || strings.Contains(r.Stdout, `"Action"`) {
		t.Errorf("log is not the plain go test text: %q", r.Stdout)
	}
}

func TestJSONFileNameCarriesTheLegNumber(t *testing.T) {
	t.Parallel()
	for _, shard := range []string{"1", "2", "3"} {
		r := leg(t, shard, []string{ev("pass", "TestWeighted", 1+0*float64(len(shard)))}, 0, rows)
		if r.Err != nil {
			t.Fatalf("shard %s: %s", shard, r.Stderr)
		}
		entries, _ := os.ReadDir(r.JSONDir)
		if len(entries) != 1 || entries[0].Name() != "providersync-race-"+shard+".json" {
			t.Errorf("shard %s json files: %v", shard, entries)
		}
	}
}
