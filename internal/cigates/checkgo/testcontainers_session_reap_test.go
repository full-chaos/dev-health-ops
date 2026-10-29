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
	// One call site: each go test group of run_integration_packages, which the shard and integration verbs both call.
	if got := strings.Count(s, `run_integration_go_test "${module_dir}" "${run_pkgs[@]}"`); got != 1 {
		t.Errorf("integration go test call sites through run_integration_go_test = %d; want 1", got)
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
func helperHarness(t *testing.T, sid string, signal, noSetsid, slowPS bool) (exit int, marker, goStarted, childAlive, sentinelAlive bool, reaped string) {
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
	if slowPS {
		// Forced slow setup: the first three ps calls of the helper take 2 s each, so a TERM lands before TC_TRAP_PGID is set.
		realPS, err := exec.LookPath("ps")
		if err != nil {
			t.Fatal(err)
		}
		cnt := filepath.Join(dir, "ps-count")
		shim := "#!/bin/sh\nn=$(cat " + cnt + " 2>/dev/null || echo 0); n=$((n+1)); echo $n > " + cnt + "\n" +
			"if [ $n -le 3 ]; then sleep 2; fi\nexec " + realPS + " \"$@\"\n"
		if err := os.WriteFile(filepath.Join(bin, "ps"), []byte(shim), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	sentinel := filepath.Join(dir, "sentinel-pid")
	pre := "sleep 30 &\necho $! > " + sentinel + "\n"
	if noSetsid {
		pre += "command() { if [ \"${1:-}\" = -v ] && [ \"${2:-}\" = setsid ]; then return 1; fi; builtin command \"$@\"; }\n"
	}
	script := "set -uo pipefail\n" + pre +
		"SCRIPT_DIR=" + dir + "\nROOT=" + dir + "\nGO_ENV_OFF=(env)\nINTEGRATION_TEST_ENV=()\n" +
		"die() { echo \"$*\" >&2; exit 2; }\n" + jobBudgetSnippet(t) + "\n" + s[start:end] +
		"run_integration_go_test . ./x &\nHP=$!\n" +
		"if [ \"" + map[bool]string{true: "1", false: "0"}[signal] + "\" = 1 ]; then\n" +
		// wait for the fake go's child (it starts after exec setsid), bounded: the 30 s ceiling only bounds a hang
		"for i in $(seq 1 600); do [ -s " + childPID + " ] && break; kill -0 $HP 2>/dev/null || break; sleep 0.05; done\nkill -TERM $HP\nfi\n" +
		"wait $HP\n"
	// The helper must run in the foreground shell to own its trap: call it directly when no signal is sent.
	if !signal {
		script = "set -uo pipefail\n" + pre + "SCRIPT_DIR=" + dir + "\nROOT=" + dir + "\nGO_ENV_OFF=(env)\nINTEGRATION_TEST_ENV=()\n" +
			"die() { echo \"$*\" >&2; exit 2; }\n" + jobBudgetSnippet(t) + "\n" + s[start:end] + "run_integration_go_test . ./x\n"
	}
	cmd := exec.Command("bash", "-c", script)
	cmd.Env = inheritedEnv(deadlineEnvNames, "PATH="+bin+":"+os.Getenv("PATH"),
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
		exit, marker, goStarted, _, _, _ := helperHarness(t, sid, true, false, false)
		if exit == 0 || marker || goStarted {
			t.Errorf("sid %q: exit=%d marker=%v goStarted=%v; want a refusal before go test, no marker", sid, exit, marker, goStarted)
		}
	}
}

func TestIntegrationHelperTrapReapsTheSessionOnTERM(t *testing.T) {
	exit, marker, goStarted, childAlive, sentinelAlive, reaped := helperHarness(t, "sess-abcdef12", true, false, false)
	if exit != 143 || marker || !goStarted || childAlive || !sentinelAlive || reaped != "sess-abcdef12" {
		t.Errorf("exit=%d marker=%v goStarted=%v childAlive=%v sentinelAlive=%v reaped=%q; want 143, no marker, go started, the go test's child gone, a bystander in the helper's own group alive, reaped sess-abcdef12", exit, marker, goStarted, childAlive, sentinelAlive, reaped)
	}
}

func TestIntegrationHelperTrapReapsTheSessionWhenTERMArrivesDuringSlowGroupSetup(t *testing.T) {
	exit, marker, goStarted, childAlive, sentinelAlive, reaped := helperHarness(t, "sess-abcdef12", true, false, true)
	if exit != 143 || marker || !goStarted || childAlive || !sentinelAlive || reaped != "sess-abcdef12" {
		t.Errorf("exit=%d marker=%v goStarted=%v childAlive=%v sentinelAlive=%v reaped=%q; want 143, no marker, go started, the go test's child gone (the group resolved at trap time), a bystander in the helper's own group alive, reaped sess-abcdef12", exit, marker, goStarted, childAlive, sentinelAlive, reaped)
	}
}

func TestIntegrationHelperWithoutSetsidStopsOnlyTheRecordedPID(t *testing.T) {
	exit, _, goStarted, _, sentinelAlive, reaped := helperHarness(t, "sess-abcdef12", true, true, false)
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

func TestTcTrapResolvingAtTrapTimeNeverKillsAGroupItsPIDDoesNotLead(t *testing.T) {
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
	res := filepath.Join(dir, "res")
	tf, pf := filepath.Join(dir, "target-pid"), filepath.Join(dir, "peer-pid")
	// A foreign group (leader L, from setsid) holds the target T and a peer P, neither of them the leader: T has not done its own setsid.
	script := "set -uo pipefail\nSCRIPT_DIR=" + dir + "\n" + s[start:end] +
		"sleep 30 &\nBYSTANDER=$!\n" +
		"setsid sh -c 'sleep 30 & echo $! > " + tf + "; sleep 30 & echo $! > " + pf + "; wait' &\n" +
		"for i in $(seq 1 200); do [ -s " + tf + " ] && [ -s " + pf + " ] && break; sleep 0.05; done\n" +
		"TARGET=$(cat " + tf + "); PEER=$(cat " + pf + ")\n" +
		"TC_TRAP_PID=$TARGET TC_TRAP_SID=sess-abcdef12 TC_TRAP_PGID=\n" +
		"( tc_trap ) 2>/dev/null\nrc=$?\n" +
		"for i in $(seq 1 200); do kill -0 $TARGET 2>/dev/null || break; sleep 0.05; done\n" +
		"t=dead; kill -0 $TARGET 2>/dev/null && t=alive\n" +
		"p=dead; kill -0 $PEER 2>/dev/null && p=alive\n" +
		"b=dead; kill -0 $BYSTANDER 2>/dev/null && b=alive\n" +
		"echo \"$rc $t $p $b\" > " + res + "\nkill -KILL $BYSTANDER $TARGET $PEER 2>/dev/null\n"
	if err := exec.Command("setsid", "bash", "-c", script).Run(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(res)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(string(raw)); got != "143 dead alive alive" {
		t.Errorf("rc/target/peer/bystander = %q; want %q (the target's group is not led by it: stop the recorded pid only)", got, "143 dead alive alive")
	}
}

// CHAOS-11794 step 1b: the providersync shard pipeline (`go test -json | tee | jq`) runs in its own process group under a
// hard step bound. A child that holds stdout open must not hang the step; the group gets a QUIT dump, a ps table
// (pid, pgid, ppid, etimes, args: never env) and a KILL; leftover group members are reaped after go test exits.

const providersyncFakeGo = `#!/bin/sh
[ -z "${GOTMPDIR+x}" ] || echo "$GOTMPDIR" >> "$FAKE_DIR/gotmpdir"
case "$1" in test) ;; *) exec "$FAKE_REAL_GO" "$@" ;; esac
case "$*" in *-tags=integration*-json*internal/providersync*) ;; *) exec "$FAKE_REAL_GO" "$@" ;; esac
: > "$FAKE_DIR/go-started"
echo "$*" >> "$FAKE_DIR/test-args"
printf '{"Action":"output","Output":"fake-go-started\\n"}\n'
case "$FAKE_MODE" in
  hold)
    sleep 300 &
    echo $! > "$FAKE_DIR/sleeper-pid"
    exit 0 ;;
  leftover)
    sleep 300 >/dev/null 2>&1 &
    echo $! > "$FAKE_DIR/sleeper-pid"
    exit 0 ;;
  exit3)
    exit 3 ;;
esac
`

// providersyncQuitFake is a Go program that stands in for a hung go test binary. The Go runtime prints every goroutine on
// SIGQUIT and exits 2 whatever signal disposition the process inherited (an sh script cannot trap a signal that was
// ignored on entry), as the real test binary does. It runs `sleep 300` holding the pipes, writes one line to the piped
// stdout when the QUIT arrives, then lets the runtime dump. It records ready/quit-seen markers and its signal masks.
const providersyncQuitFake = `package main

import (
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
)

func main() {
	args := os.Args[1:]
	if len(args) == 0 || args[0] != "test" || !strings.Contains(strings.Join(args, " "), "internal/providersync") {
		_ = syscall.Exec(os.Getenv("FAKE_REAL_GO"), append([]string{"go"}, args...), os.Environ())
		os.Exit(127)
	}
	dir := os.Getenv("FAKE_DIR")
	if st, err := os.ReadFile("/proc/self/status"); err == nil {
		for _, l := range strings.Split(string(st), "\n") {
			if strings.HasPrefix(l, "SigIgn") || strings.HasPrefix(l, "SigBlk") {
				_ = os.WriteFile(dir+"/diag-"+strings.Fields(l)[0], []byte(l+"\n"), 0o644)
			}
		}
	}
	fmt.Printf("{\"Action\":\"output\",\"Output\":\"fake-go-started\\n\"}\n")
	sleeper := exec.Command("sleep", "300")
	sleeper.Stdout, sleeper.Stderr = os.Stdout, os.Stderr
	if err := sleeper.Start(); err != nil {
		os.Exit(1)
	}
	_ = os.WriteFile(dir+"/sleeper-pid", []byte(fmt.Sprint(sleeper.Process.Pid)), 0o644)
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGQUIT)
	_ = os.WriteFile(dir+"/ready", nil, 0o644)
	<-quit
	_ = os.WriteFile(dir+"/quit-seen", nil, 0o644)
	fmt.Printf("{\"Action\":\"output\",\"Output\":\"GOROUTINE-DUMP-PIPED\\n\"}\n")
	signal.Reset(syscall.SIGQUIT)
	_ = syscall.Kill(os.Getpid(), syscall.SIGQUIT)
	select {}
}
`

func buildQuitFake(t *testing.T, realGo, work, bin string) {
	t.Helper()
	src := filepath.Join(work, "quitfake")
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "main.go"), []byte(providersyncQuitFake), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(realGo, "build", "-o", filepath.Join(bin, "go"), "main.go")
	cmd.Dir = src
	cmd.Env = append(os.Environ(), "GOWORK=off", "GOFLAGS=")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("building the fake go: %v\n%s", err, out)
	}
}

type providersyncStepRun struct {
	code           int
	stdout, stderr string
	sleeperPID     int
	sleeperAlive   bool
	timedOut       bool
	diag           string
	work           string
	goStarted      bool
	testArgs       string
}

// runProvidersyncStep runs the shard step against a fake go. opts: "nosetsid" makes `command -v setsid` fail inside check_go.sh.
func runProvidersyncStep(t *testing.T, mode, bound string, wrap func(script, work string) []string, opts ...string) providersyncStepRun {
	t.Helper()
	script := scriptPath(t, "ci/check_go.sh")
	repo := filepath.Dir(filepath.Dir(script))
	work := t.TempDir()
	bin := filepath.Join(work, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	realGo, err := exec.LookPath("go")
	if err != nil {
		t.Fatal(err)
	}
	if mode == "quitdump" {
		buildQuitFake(t, realGo, work, bin)
	} else if err := os.WriteFile(filepath.Join(bin, "go"), []byte(providersyncFakeGo), 0o755); err != nil {
		t.Fatal(err)
	}
	argv := []string{"bash", script, "integration-shard", "providersync", "1"}
	if wrap != nil {
		argv = wrap(script, work)
	}
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Dir = repo
	goEnv, err := exec.Command(realGo, "env", "GOMODCACHE", "GOPATH", "GOCACHE").Output()
	if err != nil {
		t.Fatal(err)
	}
	dirs := strings.Fields(string(goEnv))
	if len(dirs) != 3 {
		t.Fatalf("go env GOMODCACHE GOPATH GOCACHE printed %q", goEnv)
	}
	cmd.Env = []string{"PATH=" + bin + ":" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"), "TMPDIR=" + work, "FAKE_REAL_GO=" + realGo,
		"GOMODCACHE=" + dirs[0], "GOPATH=" + dirs[1], "DEV_HEALTH_GO_CACHE=" + dirs[2],
		"FAKE_MODE=" + mode, "FAKE_DIR=" + work, "GO_TEST_JSON_DIR=" + filepath.Join(work, "json"), "GITHUB_ACTIONS=true", "CI=true", "GITHUB_RUN_ID=1",
		"RUNNER_TEMP=" + work, "CHECK_GO_JOB_CAP_MIN=25", "DEV_HEALTH_GO_STEP_BOUND_SECS=" + bound, "SECRET_CANARY=canary-11794-do-not-print"}
	if v, ok := os.LookupEnv("GOTMPDIR"); ok {
		cmd.Env = append(cmd.Env, "GOTMPDIR="+v)
	}
	for _, o := range opts {
		if o == "local" {
			var kept []string
			for _, e := range cmd.Env {
				if !strings.HasPrefix(e, "GITHUB_ACTIONS=") && !strings.HasPrefix(e, "CI=") && !strings.HasPrefix(e, "GITHUB_RUN_ID=") {
					kept = append(kept, e)
				}
			}
			cmd.Env = kept
		}
		if v, ok := strings.CutPrefix(o, "env:"); ok {
			cmd.Env = append(cmd.Env, v)
		}
		if o == "nosetsid" {
			shim := filepath.Join(work, "nosetsid.sh")
			body := "command() { if [ \"${1:-}\" = -v ] && [ \"${2:-}\" = setsid ]; then return 1; fi; builtin command \"$@\"; }\n"
			if err := os.WriteFile(shim, []byte(body), 0o644); err != nil {
				t.Fatal(err)
			}
			cmd.Env = append(cmd.Env, "BASH_ENV="+shim)
		}
	}
	var stdout, stderr strings.Builder
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	res := providersyncStepRun{}
	// Failure-only hang ceiling (D6418): the pass path asserts no wall time.
	select {
	case err := <-done:
		if ee, ok := err.(*exec.ExitError); ok {
			res.code = ee.ExitCode()
		}
	case <-time.After(240 * time.Second):
		res.timedOut = true
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		<-done
	}
	res.stdout, res.stderr = stdout.String(), stderr.String()
	res.work = work
	if raw, err := os.ReadFile(filepath.Join(work, "test-args")); err == nil {
		res.testArgs = string(raw)
	}
	_, statErr := os.Stat(filepath.Join(work, "go-started"))
	res.goStarted = statErr == nil
	for _, n := range []string{"ready", "quit-seen", "diag-SigIgn", "diag-SigBlk"} {
		if raw, err := os.ReadFile(filepath.Join(work, n)); err == nil {
			res.diag += n + "=" + strings.TrimSpace(string(raw)) + "; "
		}
	}
	if raw, err := os.ReadFile(filepath.Join(work, "sleeper-pid")); err == nil {
		if pid, err := strconv.Atoi(strings.TrimSpace(string(raw))); err == nil {
			res.sleeperPID = pid
			for i := 0; i < 100; i++ {
				if res.sleeperAlive = syscall.Kill(pid, 0) == nil; !res.sleeperAlive {
					break
				}
				time.Sleep(20 * time.Millisecond)
			}
			if res.sleeperAlive {
				_ = syscall.Kill(pid, syscall.SIGKILL)
			}
		}
	}
	return res
}

func TestProvidersyncStepBoundsAStdoutHoldingChild(t *testing.T) {
	t.Parallel()
	r := runProvidersyncStep(t, "hold", "10", nil)
	if r.timedOut {
		t.Fatal("the step hung: a child holding stdout open kept `tee` alive and no step bound stopped it")
	}
	if r.code != 124 {
		t.Errorf("exit code %d, want 124 (step bound hit); stderr: %s", r.code, tail(r.stderr, 1500))
	}
	if !strings.Contains(r.stderr, "step bound") {
		t.Errorf("stderr does not name the step bound: %s", tail(r.stderr, 1500))
	}
	if r.sleeperPID == 0 || !stderrListsPID(r.stderr, r.sleeperPID) {
		t.Errorf("the ps table does not list the leftover child pid %d: %s", r.sleeperPID, tail(r.stderr, 1500))
	}
	if r.sleeperAlive {
		t.Error("the stdout-holding child survived the step")
	}
	if strings.Contains(r.stdout+r.stderr, "canary-11794-do-not-print") {
		t.Error("the step printed an environment value (D6331)")
	}
}

func TestProvidersyncStepDumpReachesTheLogOnBound(t *testing.T) {
	t.Parallel()
	r := runProvidersyncStep(t, "quitdump", "15", nil)
	if r.timedOut {
		t.Fatal("the step hung: no step bound stopped a go test that never exits")
	}
	if !strings.Contains(r.stderr, "SIGQUIT: quit") || !strings.Contains(r.stderr, "goroutine 1") {
		t.Errorf("the QUIT goroutine dump on the test binary's stderr did not reach the step stderr (fake saw: %s): %s", r.diag, tail(r.stderr, 1500))
	}
	if !strings.Contains(r.stdout, "GOROUTINE-DUMP-PIPED") {
		t.Errorf("tee/jq died before the line written to the piped stdout arrived (fake saw: %s): stdout %s", r.diag, tail(r.stdout, 1500))
	}
	if r.code != 124 {
		t.Errorf("exit code %d, want 124", r.code)
	}
}

func TestProvidersyncStepReapsLeftoverChildrenAfterGoTestExits(t *testing.T) {
	t.Parallel()
	r := runProvidersyncStep(t, "leftover", "600", nil)
	if r.timedOut {
		t.Fatal("the step hung")
	}
	if r.code != 0 {
		t.Errorf("exit code %d, want 0 (go test passed); stderr: %s", r.code, tail(r.stderr, 1500))
	}
	if r.sleeperAlive {
		t.Error("a child left in go test's process group survived the step (not reaped)")
	}
	if r.sleeperPID == 0 || !stderrListsPID(r.stderr, r.sleeperPID) {
		t.Errorf("the leftover ps table does not list pid %d: %s", r.sleeperPID, tail(r.stderr, 1500))
	}
}

func TestProvidersyncStepKeepsGoTestExitCode(t *testing.T) {
	t.Parallel()
	r := runProvidersyncStep(t, "exit3", "600", nil)
	if r.timedOut {
		t.Fatal("the step hung")
	}
	if r.code != 3 {
		t.Errorf("exit code %d, want go test's 3", r.code)
	}
	if strings.Contains(r.stderr, "step bound") {
		t.Error("the step bound fired on a go test that exited by itself")
	}
}

// check_go.sh itself must never be exec-replaced: the bounded runner returns and the next line runs.
func TestProvidersyncStepRunnerReturnsToItsCaller(t *testing.T) {
	t.Parallel()
	text, err := os.ReadFile(scriptPath(t, "ci/check_go.sh"))
	if err != nil {
		t.Fatal(err)
	}
	s := string(text)
	start := strings.Index(s, "PROVIDERSYNC_STEP_BOUND_SECS=")
	fn := strings.Index(s, "run_providersync_bounded() {")
	if start < 0 || fn < start {
		t.Fatal("PROVIDERSYNC_STEP_BOUND_SECS / run_providersync_bounded not found in ci/check_go.sh")
	}
	end := fn + strings.Index(s[fn:], "\n}\n") + 3
	snippet := s[start:end]
	r := runProvidersyncStep(t, "exit3", "600", func(script, work string) []string {
		harness := "set -uo pipefail\nROOT=" + filepath.Dir(filepath.Dir(script)) + "\nSCRIPT_DIR=" + work + "\nGO_ENV_OFF=(env)\n" +
			"die() { echo \"$*\" >&2; exit 2; }\n" + jobBudgetSnippet(t) + "\n" + snippet +
			"\nrun_providersync_bounded '^TestX$' " + filepath.Join(work, "out.json") + "\necho \"NEXT-LINE-RAN rc=$?\"\n"
		return []string{"bash", "-c", harness}
	})
	if r.timedOut {
		t.Fatal("the runner hung")
	}
	if !strings.Contains(r.stdout, "NEXT-LINE-RAN rc=3") {
		t.Errorf("the line after the runner did not run with go test's rc 3 (the script was exec-replaced?): stdout %s stderr %s", tail(r.stdout, 800), tail(r.stderr, 800))
	}
}

// stderrListsPID reports whether a ps row of the text starts with the pid.
func stderrListsPID(text string, pid int) bool {
	for _, line := range strings.Split(text, "\n") {
		if f := strings.Fields(line); len(f) > 0 && f[0] == strconv.Itoa(pid) {
			return true
		}
	}
	return false
}

// Without setsid there is no process group to stop, so the bounded step must refuse before it starts go test.
func TestProvidersyncStepRefusesWithoutSetsid(t *testing.T) {
	t.Parallel()
	r := runProvidersyncStep(t, "hold", "3", nil, "nosetsid")
	if r.timedOut {
		t.Fatal("the step hung")
	}
	if r.code != 2 {
		t.Errorf("exit code %d, want 2 (setsid missing); stderr: %s", r.code, tail(r.stderr, 1500))
	}
	const want = "check_go.sh: the bounded providersync step needs setsid (util-linux) on PATH; it is missing, so the step does not start. Install a setsid on PATH (Linux: util-linux) or run this shard on Linux."
	if !strings.Contains(r.stderr, want) {
		t.Errorf("stderr does not carry the refusal line %q: %s", want, tail(r.stderr, 1500))
	}
	if r.goStarted {
		t.Error("go test was started although the step cannot stop its process group")
	}
}

// The helper's explicit environment must carry a configured GOTMPDIR to the go calls of check_go.sh.
func TestProvidersyncStepHelperPassesGOTMPDIRThrough(t *testing.T) {
	want := t.TempDir()
	t.Setenv("GOTMPDIR", want)
	r := runProvidersyncStep(t, "exit3", "600", nil)
	raw, err := os.ReadFile(filepath.Join(r.work, "gotmpdir"))
	if err != nil {
		t.Fatalf("no go call saw a GOTMPDIR: the helper dropped it (%v)", err)
	}
	for _, line := range strings.Fields(string(raw)) {
		if line != want {
			t.Errorf("a go call saw GOTMPDIR=%q, want %q", line, want)
		}
	}
}

// Only the providersync shard step needs setsid: another shard still runs when `command -v setsid` fails.
func TestOtherShardsStillRunWithoutSetsid(t *testing.T) {
	t.Parallel()
	r := runProvidersyncStep(t, "exit3", "600", func(script, work string) []string {
		return []string{"bash", script, "integration-shard", "daily", "1", "--dry-run"}
	}, "nosetsid")
	if r.timedOut {
		t.Fatal("the dry run hung")
	}
	if r.code != 0 || !strings.Contains(r.stdout, "DRY RUN") {
		t.Errorf("a non-providersync shard did not run without setsid: rc %d stdout %s stderr %s", r.code, tail(r.stdout, 600), tail(r.stderr, 600))
	}
	if strings.Contains(r.stderr, "needs setsid") {
		t.Errorf("a non-providersync shard printed the setsid refusal: %s", tail(r.stderr, 600))
	}
}

// ---- the job deadline: every integration go test call gets min(its constant, job cap - elapsed - 3 min) ----

const jobDeadlineT0 = 1700000000

// jobBudgetSnippet returns job_budget_init and job_go_test_timeout from ci/check_go.sh.
func jobBudgetSnippet(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile(scriptPath(t, "ci/check_go.sh"))
	if err != nil {
		t.Fatal(err)
	}
	s := string(raw)
	start := strings.Index(s, "job_budget_init() {")
	fn := strings.Index(s, "job_go_test_timeout() {")
	if start < 0 || fn < start {
		t.Fatal("job_budget_init / job_go_test_timeout not found in ci/check_go.sh")
	}
	return s[start : fn+strings.Index(s[fn:], "\n}\n")+3]
}

func runJobFn(t *testing.T, env []string, body string) (code int, stdout, stderr string) {
	t.Helper()
	script := "set -uo pipefail\ndie() { printf 'ERROR: %s\\n' \"$1\" >&2; exit 2; }\n" + jobBudgetSnippet(t) + "\n" + body + "\n"
	cmd := exec.Command("bash", "-c", script)
	cmd.Env = append([]string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME")}, env...)
	var so, se strings.Builder
	cmd.Stdout, cmd.Stderr = &so, &se
	if err := cmd.Run(); err != nil {
		ee, ok := err.(*exec.ExitError)
		if !ok {
			t.Fatal(err)
		}
		code = ee.ExitCode()
	}
	return code, strings.TrimSpace(so.String()), se.String()
}

func TestJobGoTestTimeoutCells(t *testing.T) {
	t.Parallel()
	t0 := strconv.Itoa(jobDeadlineT0)
	at := func(sec int) string { return "CHECK_GO_NOW_EPOCH=" + strconv.Itoa(jobDeadlineT0+sec) }
	for _, c := range []struct {
		name     string
		env      []string
		arg      string
		wantCode int
		wantOut  string
		wantErr  string
	}{
		{"remaining above the constant keeps the constant", []string{"CHECK_GO_JOB_CAP_MIN=60", "CHECK_GO_JOB_START=" + t0, at(60)}, "22", 0, "1320s", ""},
		{"remaining below the constant wins", []string{"CHECK_GO_JOB_CAP_MIN=25", "CHECK_GO_JOB_START=" + t0, at(600)}, "22", 0, "720s", ""},
		{"no budget left refuses", []string{"CHECK_GO_JOB_CAP_MIN=25", "CHECK_GO_JOB_START=" + t0, at(1320)}, "22", 2, "", "job time budget exhausted before"},
		{"a cap missing in CI refuses", []string{"GITHUB_ACTIONS=true"}, "22", 2, "", "CHECK_GO_JOB_CAP_MIN is not set for a guarded step"},
		{"a cap missing outside CI keeps the constant", nil, "22", 0, "1320s", ""},
		{"the fake clock is ignored in CI", []string{"GITHUB_ACTIONS=true", "CHECK_GO_JOB_CAP_MIN=25", "CHECK_GO_JOB_START=" + strconv.FormatInt(time.Now().Unix(), 10), "CHECK_GO_NOW_EPOCH=" + strconv.FormatInt(time.Now().Unix()+9999, 10)}, "1", 0, "60s", ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			code, out, errText := runJobFn(t, c.env, "job_go_test_timeout "+c.arg)
			if code != c.wantCode || out != c.wantOut || !strings.Contains(errText, c.wantErr) {
				t.Fatalf("rc %d (want %d) stdout %q (want %q) stderr %q (want it to contain %q)", code, c.wantCode, out, c.wantOut, errText, c.wantErr)
			}
		})
	}
}

func TestJobStartIsTakenOnceAndExportedToLaterSteps(t *testing.T) {
	t.Parallel()
	ghEnv := filepath.Join(t.TempDir(), "github_env")
	if err := os.WriteFile(ghEnv, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	env := []string{"CHECK_GO_JOB_CAP_MIN=25", "CHECK_GO_NOW_EPOCH=" + strconv.Itoa(jobDeadlineT0), "GITHUB_ENV=" + ghEnv}
	code, out, errText := runJobFn(t, env, "job_budget_init; echo \"${CHECK_GO_JOB_START:-unset}\"")
	if code != 0 || out != strconv.Itoa(jobDeadlineT0) {
		t.Fatalf("first call: rc %d stdout %q stderr %q", code, out, errText)
	}
	raw, _ := os.ReadFile(ghEnv)
	if strings.TrimSpace(string(raw)) != "CHECK_GO_JOB_START="+strconv.Itoa(jobDeadlineT0) {
		t.Fatalf("GITHUB_ENV holds %q", raw)
	}
	env2 := append(env, "CHECK_GO_JOB_START="+strconv.Itoa(jobDeadlineT0-500), "CHECK_GO_NOW_EPOCH="+strconv.Itoa(jobDeadlineT0+100))
	code, out, _ = runJobFn(t, env2, "job_budget_init; echo \"${CHECK_GO_JOB_START}\"")
	if code != 0 || out != strconv.Itoa(jobDeadlineT0-500) {
		t.Fatalf("a later call re-took the start: rc %d stdout %q", code, out)
	}
	raw, _ = os.ReadFile(ghEnv)
	if strings.Count(string(raw), "CHECK_GO_JOB_START=") != 1 {
		t.Fatalf("the later call appended to GITHUB_ENV again: %q", raw)
	}
}

// runCheckGoWithFakeGo runs ci/check_go.sh with a fake go whose test calls are logged to $FAKE_DIR/test-args.
func runCheckGoWithFakeGo(t *testing.T, args []string, env []string, fake string) (code int, stderr, testArgs string) {
	t.Helper()
	script := scriptPath(t, "ci/check_go.sh")
	repo := filepath.Dir(filepath.Dir(script))
	work := t.TempDir()
	bin := filepath.Join(work, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	realGo, err := exec.LookPath("go")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bin, "go"), []byte(fake), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(work, "clock"), []byte(strconv.Itoa(jobDeadlineT0)), 0o644); err != nil {
		t.Fatal(err)
	}
	goEnv, err := exec.Command(realGo, "env", "GOMODCACHE", "GOPATH", "GOCACHE").Output()
	if err != nil {
		t.Fatal(err)
	}
	dirs := strings.Fields(string(goEnv))
	cmd := exec.Command("bash", append([]string{script}, args...)...)
	cmd.Dir = repo
	cmd.Env = append([]string{"PATH=" + bin + ":" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"), "TMPDIR=" + work, "FAKE_REAL_GO=" + realGo,
		"GOMODCACHE=" + dirs[0], "GOPATH=" + dirs[1], "DEV_HEALTH_GO_CACHE=" + dirs[2], "FAKE_DIR=" + work,
		"CHECK_GO_NOW_EPOCH_FILE=" + filepath.Join(work, "clock")}, env...)
	var se strings.Builder
	cmd.Stderr = &se
	if err := cmd.Run(); err != nil {
		ee, ok := err.(*exec.ExitError)
		if !ok {
			t.Fatal(err)
		}
		code = ee.ExitCode()
	}
	raw, _ := os.ReadFile(filepath.Join(work, "test-args"))
	return code, se.String(), string(raw)
}

// The fake go of the loop cells: it logs a test call, moves the fake clock by FAKE_ADVANCE seconds and prints the race marker of every TestRace of the package.
const jobDeadlineFakeGo = `#!/bin/sh
case "$1" in test) ;; *) exec "$FAKE_REAL_GO" "$@" ;; esac
echo "$*" >> "$FAKE_DIR/test-args"
n=$(cat "$CHECK_GO_NOW_EPOCH_FILE"); echo $((n + ${FAKE_ADVANCE:-0})) > "$CHECK_GO_NOW_EPOCH_FILE"
for last; do :; done
grep -hoE '^func TestRace[A-Za-z0-9_]+' "${last#./}"/*_test.go 2>/dev/null | sed 's/^func //' | while read -r name; do echo "AUTH_RACE_EXECUTED $name"; done
exit 0
`

func timeoutArgs(testArgs string) []string {
	var out []string
	for _, line := range strings.Split(strings.TrimSpace(testArgs), "\n") {
		for _, f := range strings.Fields(line) {
			if v, ok := strings.CutPrefix(f, "-timeout="); ok {
				out = append(out, v)
			}
		}
	}
	return out
}

func TestAuthRaceIntegrationLoopShrinksTheNextTimeout(t *testing.T) {
	t.Parallel()
	code, errText, testArgs := runCheckGoWithFakeGo(t, []string{"auth", "race-integration"}, []string{"CHECK_GO_JOB_CAP_MIN=30", "FAKE_ADVANCE=1200"}, jobDeadlineFakeGo)
	got := timeoutArgs(testArgs)
	if len(got) != 2 || got[0] != "1620s" || got[1] != "420s" {
		t.Errorf("-timeout of the calls = %v, want [1620s 420s] (the third package has no budget left and is not started); stderr: %s", got, tail(errText, 800))
	}
	if code != 2 || !strings.Contains(errText, "job time budget exhausted before") {
		t.Errorf("rc %d, want 2 with the exhausted line; stderr: %s", code, tail(errText, 800))
	}
}

func TestMultiReplicaCallFollowsTheJobBudget(t *testing.T) {
	t.Parallel()
	_, errText, testArgs := runCheckGoWithFakeGo(t, []string{"multi-replica-workers"}, []string{"CHECK_GO_JOB_CAP_MIN=30", "CHECK_GO_JOB_START=" + strconv.Itoa(jobDeadlineT0-1500)}, jobDeadlineFakeGo)
	if got := timeoutArgs(testArgs); len(got) != 1 || got[0] != "120s" {
		t.Errorf("-timeout of the call = %v, want [120s]; stderr: %s", got, tail(errText, 800))
	}
}

func TestProvidersyncTimeoutFollowsTheJobBudget(t *testing.T) {
	t.Parallel()
	local := []string{"local", "env:CHECK_GO_JOB_START=" + strconv.Itoa(jobDeadlineT0)}
	r := runProvidersyncStep(t, "exit3", "600", nil, append(local, "env:CHECK_GO_NOW_EPOCH="+strconv.Itoa(jobDeadlineT0+1000))...)
	if r.code != 3 || !strings.Contains(r.testArgs, "-timeout=320s") {
		t.Errorf("rc %d (want 3); the go test call was %q, want -timeout=320s (cap 25 min - 1000 s - 3 min)", r.code, r.testArgs)
	}
	r = runProvidersyncStep(t, "exit3", "600", nil, append(local, "env:CHECK_GO_NOW_EPOCH="+strconv.Itoa(jobDeadlineT0+1400))...)
	if r.code != 2 || r.goStarted || !strings.Contains(r.stderr, "job time budget exhausted before") {
		t.Errorf("exhausted budget: rc %d (want 2), go started %v, stderr %s", r.code, r.goStarted, tail(r.stderr, 800))
	}
}

// The packages loop (run_integration_go_test, one call per package group): a call that starts late gets a smaller -timeout and a call with no budget left is not started.
func TestIntegrationPackagesLoopShrinksTheNextTimeout(t *testing.T) {
	t.Parallel()
	text, err := os.ReadFile(scriptPath(t, "ci/check_go.sh"))
	if err != nil {
		t.Fatal(err)
	}
	s := string(text)
	start := strings.Index(s, `TC_TRAP_PID="" TC_TRAP_SID="" TC_TRAP_PGID=""`)
	fn := strings.Index(s, "run_integration_go_test() {")
	if start < 0 || fn < start {
		t.Fatal("run_integration_go_test not found in ci/check_go.sh")
	}
	end := fn + strings.Index(s[fn:], "\n}\n") + 3
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(dir, "test-args")
	if err := os.WriteFile(filepath.Join(bin, "go"), []byte("#!/bin/sh\necho \"$*\" >> \""+logPath+"\"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "reap-testcontainers-session.sh"), []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	at := func(sec int) string { return strconv.Itoa(jobDeadlineT0 + sec) }
	script := "set -uo pipefail\nSCRIPT_DIR=" + dir + "\nROOT=" + dir + "\nGO_ENV_OFF=(env)\nINTEGRATION_TEST_ENV=()\n" +
		"die() { printf 'ERROR: %s\\n' \"$1\" >&2; exit 2; }\n" + jobBudgetSnippet(t) + "\n" + s[start:end] + "\n" +
		"CHECK_GO_NOW_EPOCH=" + at(0) + " job_budget_init; CHECK_GO_NOW_EPOCH=" + at(0) + " run_integration_go_test . ./a; echo \"rc1=$?\"\n" +
		"CHECK_GO_NOW_EPOCH=" + at(1200) + " run_integration_go_test . ./b; echo \"rc2=$?\"\n" +
		"CHECK_GO_NOW_EPOCH=" + at(1400) + " run_integration_go_test . ./c; echo \"rc3=$?\"\n"
	cmd := exec.Command("bash", "-c", script)
	cmd.Env = []string{"PATH=" + bin + ":" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"), "TESTCONTAINERS_SESSION_ID=sessionid11794",
		"CHECK_GO_JOB_CAP_MIN=25"}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("harness: %v\n%s", err, out)
	}
	raw, _ := os.ReadFile(logPath)
	if got := timeoutArgs(string(raw)); len(got) != 2 || got[0] != "1320s" || got[1] != "120s" {
		t.Errorf("-timeout of the calls = %v, want [1320s 120s] (the third call has no budget left); output:\n%s", got, out)
	}
	if !strings.Contains(string(out), "rc3=2") || !strings.Contains(string(out), "job time budget exhausted before") {
		t.Errorf("the third call must return 2 with the exhausted line; output:\n%s", out)
	}
}

// deadlineEnvNames are pinned to "unset" by the helpers that run ci/check_go.sh with a stand-in go: a hosted run inherits
// GITHUB_ACTIONS=true, and the job deadline refuses a guarded call without CHECK_GO_JOB_CAP_MIN.
var deadlineEnvNames = []string{"GITHUB_ACTIONS", "CHECK_GO_JOB_CAP_MIN", "CHECK_GO_JOB_START", "CHECK_GO_NOW_EPOCH", "CHECK_GO_NOW_EPOCH_FILE"}

// The process-group bound is min(its constant, the job budget left): with 10 s left it must not wait 1380 s (or the test override).
func TestProvidersyncBoundShrinksWithTheJobBudget(t *testing.T) {
	t.Parallel()
	local := []string{"local", "env:CHECK_GO_JOB_START=" + strconv.Itoa(jobDeadlineT0), "env:CHECK_GO_NOW_EPOCH=" + strconv.Itoa(jobDeadlineT0+1310)}
	r := runProvidersyncStep(t, "hold", "600", nil, local...)
	if r.timedOut {
		t.Fatal("the step hung")
	}
	if r.code != 124 || !strings.Contains(r.stderr, "step bound (10 s) hit") {
		t.Errorf("rc %d (want 124); stderr must name a 10 s bound (cap 25 min - 1310 s - 3 min = 10 s): %s", r.code, tail(r.stderr, 1000))
	}
}
