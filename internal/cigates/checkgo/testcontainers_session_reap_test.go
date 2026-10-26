package checkgo_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestIntegrationGoTestRunsThroughTheSessionReapHelper(t *testing.T) {
	text, err := os.ReadFile(scriptPath(t, "ci/check_go.sh"))
	if err != nil {
		t.Fatal(err)
	}
	s := string(text)
	if got := strings.Count(s, `run_integration_go_test "${module_dir}" "${run_pkgs[@]}"`); got != 2 {
		t.Errorf("integration go test call sites through run_integration_go_test = %d; want 2", got)
	}
	start := strings.Index(s, "run_integration_go_test() {")
	if start < 0 {
		t.Fatal("run_integration_go_test is absent")
	}
	body := s[start:]
	body = body[:strings.Index(body, "\n}\n")]
	for _, want := range []string{
		`TESTCONTAINERS_SESSION_ID="${sid}"`,
		`INT TERM`,
		`reap-testcontainers-session.sh`,
		`trap - INT TERM`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("run_integration_go_test lacks %q", want)
		}
	}
}

func TestReapTestcontainersSessionRemovesOnlyThatSessionByID(t *testing.T) {
	script := scriptPath(t, "ci/reap-testcontainers-session.sh")
	if st, err := os.Stat(script); err != nil || st.Mode()&0o111 == 0 {
		t.Fatalf("%s must exist and be executable: %v", script, err)
	}
	if text, _ := os.ReadFile(script); strings.Contains(string(text), "docker system prune") || strings.Contains(string(text), "container prune") {
		t.Error("the reap script must never prune")
	}
	dir := t.TempDir()
	log := filepath.Join(dir, "docker.log")
	fake := "#!/bin/sh\necho \"$*\" >> " + log + "\n" +
		"if [ \"$1\" = ps ]; then printf 'aaa111\\nbbb222\\n'; fi\n"
	if err := os.WriteFile(filepath.Join(dir, "docker"), []byte(fake), 0o755); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("bash", script, "sess-12345678")
	cmd.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"))
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("reap failed: %v\n%s", err, out)
	}
	raw, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	want := "ps -aq --filter label=org.testcontainers.sessionId=sess-12345678\nrm -f aaa111\nrm -f bbb222\n"
	if string(raw) != want {
		t.Errorf("docker calls:\n%s\nwant:\n%s", raw, want)
	}

	bad := exec.Command("bash", script, "x;rm -rf")
	bad.Env = cmd.Env
	if err := bad.Run(); err == nil {
		t.Error("an unsafe session id must be refused")
	}
}

// helperHarness extracts tc_trap and run_integration_go_test from ci/check_go.sh and runs them against a stand-in
// `go` that records its start and a stand-in reaper that records the session id it is given.
func helperHarness(t *testing.T, sid string, signal, noSetsid bool) (exit int, marker, goStarted, childAlive, sentinelAlive bool, reaped string) {
	t.Helper()
	text, err := os.ReadFile(scriptPath(t, "ci/check_go.sh"))
	if err != nil {
		t.Fatal(err)
	}
	s := string(text)
	end := strings.Index(s, "run_integration_go_test() {")
	start := strings.Index(s, "tc_trap() {")
	if start < 0 {
		start = end
	}
	end += strings.Index(s[end:], "\n}\n") + 3
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	started := filepath.Join(dir, "go-started")
	reapLog := filepath.Join(dir, "reaped")
	markerPath := filepath.Join(dir, "marker")
	childPID := filepath.Join(dir, "child-pid")
	fakeGo := "#!/bin/sh\ntouch " + started + "\nsleep 30 &\necho $! > " + childPID + "\nwait\n"
	if err := os.WriteFile(filepath.Join(bin, "go"), []byte(fakeGo), 0o755); err != nil {
		t.Fatal(err)
	}
	fakeReap := "#!/bin/sh\necho \"$1\" >> " + reapLog + "\n"
	if err := os.WriteFile(filepath.Join(dir, "reap-testcontainers-session.sh"), []byte(fakeReap), 0o755); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(dir, "sentinel-pid")
	pre := "sleep 30 &\necho $! > " + sentinel + "\n"
	if noSetsid {
		pre += "command() { if [ \"${1:-}\" = -v ] && [ \"${2:-}\" = setsid ]; then return 1; fi; builtin command \"$@\"; }\n"
	}
	script := "set -uo pipefail\n" + pre +
		"SCRIPT_DIR=" + dir + "\nROOT=" + dir + "\nGO_ENV_OFF=(env)\nINTEGRATION_TEST_ENV=()\n" +
		"die() { echo \"$*\" >&2; exit 2; }\n" + s[start:end] +
		"run_integration_go_test . ./x &\nHP=$!\n" +
		"if [ \"" + map[bool]string{true: "1", false: "0"}[signal] + "\" = 1 ]; then sleep 1; kill -TERM $HP; fi\n" +
		"wait $HP\n"
	// The helper must run in the foreground shell to own its trap: call it directly when no signal is sent.
	if !signal {
		script = "set -uo pipefail\n" + pre + "SCRIPT_DIR=" + dir + "\nROOT=" + dir + "\nGO_ENV_OFF=(env)\nINTEGRATION_TEST_ENV=()\n" +
			"die() { echo \"$*\" >&2; exit 2; }\n" + s[start:end] + "run_integration_go_test . ./x\n"
	}
	cmd := exec.Command("bash", "-c", script)
	cmd.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"),
		"TESTCONTAINERS_SESSION_ID="+sid, "PROOF_MARKER="+markerPath)
	err = cmd.Run()
	exit = 0
	if ee, ok := err.(*exec.ExitError); ok {
		exit = ee.ExitCode()
	} else if err != nil {
		t.Fatal(err)
	}
	_, e1 := os.Stat(markerPath)
	_, e2 := os.Stat(started)
	raw, _ := os.ReadFile(reapLog)
	if cp, err := os.ReadFile(childPID); err == nil {
		if pid, err := strconv.Atoi(strings.TrimSpace(string(cp))); err == nil {
			// an orphaned child stays a zombie until init reaps it: poll for its exit, bounded
			for i := 0; i < 250; i++ {
				if childAlive = syscall.Kill(pid, 0) == nil; !childAlive {
					break
				}
				time.Sleep(20 * time.Millisecond)
			}
			if childAlive {
				_ = syscall.Kill(pid, syscall.SIGKILL)
			}
		}
	}
	if sp, err := os.ReadFile(sentinel); err == nil {
		if pid, err := strconv.Atoi(strings.TrimSpace(string(sp))); err == nil {
			sentinelAlive = syscall.Kill(pid, 0) == nil
			if sentinelAlive {
				_ = syscall.Kill(pid, syscall.SIGKILL)
			}
		}
	}
	return exit, e1 == nil, e2 == nil, childAlive, sentinelAlive, strings.TrimSpace(string(raw))
}

func TestIntegrationHelperRefusesAnUnsafeSessionIDBeforeGoTestStarts(t *testing.T) {
	for _, sid := range []string{
		`$(touch "$PROOF_MARKER")`,
		"has space1234",
		"line\nbreak12345",
		"short",
		strings.Repeat("a", 65),
	} {
		exit, marker, goStarted, _, _, _ := helperHarness(t, sid, true, false)
		if exit == 0 || marker || goStarted {
			t.Errorf("sid %q: exit=%d marker=%v goStarted=%v; want a refusal before go test, no marker", sid, exit, marker, goStarted)
		}
	}
}

func TestIntegrationHelperTrapReapsTheSessionOnTERM(t *testing.T) {
	exit, marker, goStarted, childAlive, sentinelAlive, reaped := helperHarness(t, "sess-abcdef12", true, false)
	if exit != 143 || marker || !goStarted || childAlive || !sentinelAlive || reaped != "sess-abcdef12" {
		t.Errorf("exit=%d marker=%v goStarted=%v childAlive=%v sentinelAlive=%v reaped=%q; want 143, no marker, go started, the go test's child gone, a bystander in the helper's own group alive, reaped sess-abcdef12", exit, marker, goStarted, childAlive, sentinelAlive, reaped)
	}
}

func TestIntegrationHelperWithoutSetsidStopsOnlyTheRecordedPID(t *testing.T) {
	exit, _, goStarted, _, sentinelAlive, reaped := helperHarness(t, "sess-abcdef12", true, true)
	if exit != 143 || !goStarted || !sentinelAlive || reaped != "sess-abcdef12" {
		t.Errorf("exit=%d goStarted=%v sentinelAlive=%v reaped=%q; want 143, go started, a bystander in the helper's group alive (no group kill), reaped", exit, goStarted, sentinelAlive, reaped)
	}
}

func TestTcTrapNeverKillsTheHelpersOwnProcessGroup(t *testing.T) {
	text, err := os.ReadFile(scriptPath(t, "ci/check_go.sh"))
	if err != nil {
		t.Fatal(err)
	}
	s := string(text)
	start := strings.Index(s, "tc_trap() {")
	end := start + strings.Index(s[start:], "\n}\n") + 3
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "reap-testcontainers-session.sh"), []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	pids := filepath.Join(dir, "pids")
	script := "set -uo pipefail\nSCRIPT_DIR=" + dir + "\n" + s[start:end] +
		"sleep 30 &\nBYSTANDER=$!\nsleep 30 &\nTARGET=$!\n" +
		"TC_TRAP_PID=$TARGET TC_TRAP_SID=sess-abcdef12 TC_TRAP_PGID=$(ps -o pgid= -p $$ | tr -d ' ')\n" +
		"( tc_trap ) 2>/dev/null\nrc=$?\n" +
		"b=dead; kill -0 $BYSTANDER 2>/dev/null && b=alive\n" +
		// the subshell above cannot reap the target, so it is a zombie until this shell handles SIGCHLD: poll for its exit, bounded
		"for i in $(seq 1 200); do kill -0 $TARGET 2>/dev/null || break; sleep 0.05; done\n" +
		"t=dead; kill -0 $TARGET 2>/dev/null && t=alive\n" +
		"echo \"$rc $b $t\" > " + pids + "\nkill -KILL $BYSTANDER $TARGET 2>/dev/null\n"
	if err := exec.Command("setsid", "bash", "-c", script).Run(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(pids)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(string(raw)); got != "143 alive dead" {
		t.Errorf("rc/bystander/target = %q; want %q (pgid equal to the helper's own group: stop the recorded pid only)", got, "143 alive dead")
	}
}
