package scripts_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// The pre-push gate declares its stage ids up front and verify_stage_manifest fails the gate when the executed set
// differs, even if every stage that ran passed. These tests run the REAL run_declared_stages, run_stage,
// verify_stage_manifest and fail_fast of ci/local_validate.sh, with ch_provision replaced by a stub that runs the
// stages a scenario names. No container, ClickHouse or Python is reachable: docker, clickhouse, python and curl on
// PATH are shims that exit 127 and leave a marker, and the test fails if a marker appears.

var lvManifestFuncs = []string{
	"array_contains", "join_commas", "record", "die", "fail_fast", "run_stage", "banner",
	"print_summary", "verify_stage_manifest", "run_declared_stages",
}

const lvManifestHarness = `set -u
RESULTS=()
FAILED=0
DECLARED_STAGE_IDS=()
EXECUTED_STAGE_IDS=()
c_red() { printf '%s' "$1"; }
c_green() { printf '%s' "$1"; }
c_yellow() { printf '%s' "$1"; }
hr() { printf -- '---\n'; }
ch_provision() {
  case "${SCENARIO}" in
  pass)
    run_stage "probe" ch_probe true
    run_stage "scratch" ch_scratch_create true
    run_stage "migrate" ch_migrate true
    ;;
  silent_skip)
    run_stage "probe" ch_probe true
    run_stage "scratch" ch_scratch_create true
    ;;
  undeclared)
    run_stage "probe" ch_probe true
    run_stage "scratch" ch_scratch_create true
    run_stage "migrate" ch_migrate true
    run_stage "ghost" ghost true
    ;;
  stage_fails)
    run_stage "probe" ch_probe true
    run_stage "scratch" ch_scratch_create false
    ;;
  esac
}
run_declared_stages
`

type lvManifestRun struct {
	out string
	rc  int
}

func lvManifestScenario(t *testing.T, script, scenario string) lvManifestRun {
	t.Helper()
	raw, err := os.ReadFile(script)
	if err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	for _, name := range lvManifestFuncs {
		body, ok := lvFunc(string(raw), name)
		if !ok {
			t.Fatalf("%s() not found in the gate", name)
		}
		b.WriteString(body + "\n")
	}
	b.WriteString(lvManifestHarness)

	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(dir, "forbidden-called")
	for _, name := range []string{"docker", "clickhouse", "clickhouse-client", "python", "python3", "curl"} {
		shim := "#!/bin/sh\necho " + name + " >> '" + marker + "'\nexit 127\n"
		if err := os.WriteFile(filepath.Join(bin, name), []byte(shim), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	cmd := exec.Command("bash", "-c", b.String())
	cmd.Env = []string{"PATH=" + bin + ":/usr/bin:/bin", "SCENARIO=" + scenario, "HOME=" + dir}
	out, err := cmd.CombinedOutput()
	rc := 0
	if ee, ok := err.(*exec.ExitError); ok {
		rc = ee.ExitCode()
	} else if err != nil {
		t.Fatal(err)
	}
	if called, readErr := os.ReadFile(marker); readErr == nil {
		t.Fatalf("scenario %s reached a forbidden command: %s", scenario, called)
	}
	return lvManifestRun{string(out), rc}
}

// lvManifestViolations runs every scenario on the script at path and returns one message per wrong outcome.
func lvManifestViolations(t *testing.T, script string) []string {
	t.Helper()
	var out []string
	check := func(scenario string, wantRC int, wants ...string) {
		r := lvManifestScenario(t, script, scenario)
		if r.rc != wantRC {
			out = append(out, scenario+": exit "+strconv.Itoa(r.rc)+", want "+strconv.Itoa(wantRC))
		}
		for _, w := range wants {
			if !strings.Contains(r.out, w) {
				out = append(out, scenario+": output lacks "+w)
			}
		}
	}
	check("pass", 0,
		"GATE_STAGE_MANIFEST result=PASSED declared=3 executed=3 declared_ids=ch_probe,ch_scratch_create,ch_migrate executed_ids=ch_probe,ch_scratch_create,ch_migrate",
		"GATE PASSED. [3/3: ch_probe,ch_scratch_create,ch_migrate] safe to push.",
		"stage-manifest self-check (3/3 declared stages executed)")
	check("silent_skip", 1,
		"declared-but-not-executed=[ch_migrate] executed-but-not-declared=[none]",
		"GATE_STAGE_MANIFEST result=FAILED declared=3 executed=2",
		"failed_at=stage-manifest self-check")
	check("undeclared", 1,
		"declared-but-not-executed=[none] executed-but-not-declared=[ghost]",
		"GATE_STAGE_MANIFEST result=FAILED declared=3 executed=4",
		"failed_at=stage-manifest self-check")
	check("stage_fails", 1,
		"GATE_STAGE_MANIFEST result=FAILED declared=3 executed=2",
		"failed_at=scratch")
	if r := lvManifestScenario(t, script, "silent_skip"); strings.Contains(r.out, "GATE PASSED") {
		out = append(out, "silent_skip printed GATE PASSED")
	}
	out = append(out, lvEntryViolations(t, script)...)
	return out
}

// lvEntryRun runs the REAL script, entry point included, from a temp tree: main() takes the lock, runs preflight and
// dispatches run_declared_stages. The only reachable "docker" is a shim that exits 127, so the first stage fails at
// the probe: a gate whose entry skips stage dispatch exits 0 with no manifest line instead.
func lvEntryRun(t *testing.T, script string) lvManifestRun {
	t.Helper()
	raw, err := os.ReadFile(script)
	if err != nil {
		t.Fatal(err)
	}
	tree := t.TempDir()
	if err := os.MkdirAll(filepath.Join(tree, "lock"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(tree, "ci"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tree, "ci", "local_validate.sh"), raw, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tree, "ci", "run_tests.sh"), []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(tree, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	forbidden := filepath.Join(tree, "forbidden-called")
	for _, name := range []string{"clickhouse", "clickhouse-client", "python", "python3", "curl"} {
		shim := "#!/bin/sh\necho " + name + " >> '" + forbidden + "'\nexit 127\n"
		if err := os.WriteFile(filepath.Join(bin, name), []byte(shim), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	dockerMarker := filepath.Join(tree, "docker-shim-called")
	if err := os.WriteFile(filepath.Join(bin, "docker"), []byte("#!/bin/sh\necho docker >> '"+dockerMarker+"'\nexit 127\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("bash", filepath.Join(tree, "ci", "local_validate.sh"))
	cmd.Dir = tree
	cmd.Env = []string{
		"PATH=" + bin + ":/usr/bin:/bin", "HOME=" + tree, "TMPDIR=" + tree,
		"DEV_HEALTH_LOCK_ROOT=" + filepath.Join(tree, "lock"), "LOCK_WAIT_SECS=0",
	}
	out, err := cmd.CombinedOutput()
	rc := 0
	if ee, ok := err.(*exec.ExitError); ok {
		rc = ee.ExitCode()
	} else if err != nil {
		t.Fatal(err)
	}
	if called, readErr := os.ReadFile(forbidden); readErr == nil {
		t.Fatalf("the entry run reached a forbidden command: %s", called)
	}
	return lvManifestRun{string(out), rc}
}

func lvEntryViolations(t *testing.T, script string) []string {
	t.Helper()
	r := lvEntryRun(t, script)
	var out []string
	if r.rc != 1 {
		out = append(out, "entry: exit "+strconv.Itoa(r.rc)+", want 1 (the docker probe fails behind the shim)")
	}
	for _, w := range []string{
		"GATE_STAGE_MANIFEST result=FAILED declared=3 executed=1 declared_ids=ch_probe,ch_scratch_create,ch_migrate executed_ids=ch_probe failed_at=clickhouse: docker probe",
		"docker ps probe FAILED (exit 127)",
	} {
		if !strings.Contains(r.out, w) {
			out = append(out, "entry: output lacks "+w)
		}
	}
	if strings.Contains(r.out, "GATE PASSED") {
		out = append(out, "entry printed GATE PASSED")
	}
	return out
}

func TestLocalValidateStageManifestHoldsOnTheRealScript(t *testing.T) {
	script := filepath.Join(repoRoot(t), "ci", "local_validate.sh")
	if got := lvManifestViolations(t, script); len(got) != 0 {
		t.Fatalf("stage manifest: %s", strings.Join(got, "; "))
	}
}

func TestLocalValidateStageManifestFiresPerPlantedDefect(t *testing.T) {
	real := filepath.Join(repoRoot(t), "ci", "local_validate.sh")
	raw, err := os.ReadFile(real)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ label, from, to, want string }{
		{"declared-but-not-executed check removed", `missing="${missing:+${missing} }${id}"`, `:`, "silent_skip"},
		{"executed-but-not-declared check removed", `extra="${extra:+${extra} }${id}"`, `:`, "undeclared"},
		{"declared set shrinks", "DECLARED_STAGE_IDS=(ch_probe ch_scratch_create ch_migrate)", "DECLARED_STAGE_IDS=(ch_probe ch_scratch_create)", "pass"},
		{"a failing stage no longer stops the gate", `"$(c_red 'GATE FAILED.')" "${name}"` + "\n  exit 1", `"$(c_red 'GATE FAILED.')" "${name}"` + "\n  exit 0", "stage_fails"},
		{"main skips stage dispatch", "  preflight\n  run_declared_stages\n}", "  preflight\n  :\n}", "entry"},
		{"the verdict loses its stage count", `"$(c_green 'GATE PASSED.')" "${#EXECUTED_STAGE_IDS[@]}" "${#DECLARED_STAGE_IDS[@]}"`, `"$(c_green 'GATE PASSED.')" "0" "0"`, "pass"},
	} {
		t.Run(tc.label, func(t *testing.T) {
			if !strings.Contains(string(raw), tc.from) {
				t.Fatalf("the gate no longer contains %q: the plant measures nothing", tc.from)
			}
			planted := filepath.Join(t.TempDir(), "local_validate.sh")
			if err := os.WriteFile(planted, []byte(strings.Replace(string(raw), tc.from, tc.to, 1)), 0o755); err != nil {
				t.Fatal(err)
			}
			got := strings.Join(lvManifestViolations(t, planted), "; ")
			if !strings.Contains(got, tc.want+":") && !strings.Contains(got, tc.want+" ") {
				t.Fatalf("planted %q: violations %q do not name scenario %s", tc.label, got, tc.want)
			}
		})
	}
}

func TestLocalValidateStageManifestShimsCatchAForbiddenCommand(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "forbidden-called")
	shim := filepath.Join(dir, "docker")
	if err := os.WriteFile(shim, []byte("#!/bin/sh\necho docker >> '"+marker+"'\nexit 127\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("bash", "-c", "docker ps")
	cmd.Env = []string{"PATH=" + dir + ":/usr/bin:/bin"}
	if err := cmd.Run(); err == nil {
		t.Fatal("the docker shim did not fail")
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("the docker shim left no marker: %v", err)
	}
}
