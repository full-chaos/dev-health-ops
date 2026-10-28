package bigboy_test

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func composeHangScript(t *testing.T, body string) (dir, script string) {
	t.Helper()
	dir = t.TempDir()
	script = filepath.Join(dir, "hang.sh")
	if err := os.WriteFile(script, []byte("#!/usr/bin/env bash\n"+body), 0o755); err != nil {
		t.Fatal(err)
	}
	return dir, script
}

var composeHangEnv = []string{"PATH=/usr/bin:/bin"}

// composeExecWithin runs the script under composeExec and returns its result. The select is a hang ceiling only: without
// the kill path these runs would block for ever; each ceiling is at least 5x the clean time of its test (stated at the call).
func composeExecWithin(t *testing.T, ceiling time.Duration, dir string, timeout time.Duration, script string, extraEnv ...string) (scriptRun, error) {
	t.Helper()
	type result struct {
		r   scriptRun
		err error
	}
	done := make(chan result, 1)
	go func() {
		r, err := composeExec(dir, append(append([]string{}, extraEnv...), composeHangEnv...), timeout, "bash", script)
		done <- result{r, err}
	}()
	select {
	case res := <-done:
		return res.r, res.err
	case <-time.After(ceiling):
		t.Fatalf("composeExec did not return: the kill path left the run blocked")
		return scriptRun{}, nil
	}
}

// A step that never returns is killed at the ceiling, and the failure names the
// last step the script reported, with only the tail of the output and without
// the credential.
func TestComposeExecKillNamesTheLastStepAndHidesTheCredential(t *testing.T) {
	dir, script := composeHangScript(t,
		"for i in $(seq 1 2000); do echo \"noise line $i\"; done\n"+
			"echo \"STEP planted-a rc=0 00:00:00\"\n"+
			"echo \"STEP planted-b rc=0 00:00:01\"\n"+
			"echo \"leak="+composeCHSentinel+"\" >&2\n"+
			"exec sleep 30\n")
	// Clean time about 1.1 s; ceiling 20 s.
	_, err := composeExecWithin(t, 20*time.Second, dir, time.Second, script)
	if err == nil {
		t.Fatal("a step that never returns gave no error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "STEP planted-b rc=0 00:00:01") {
		t.Errorf("the failure does not name the last step: %.300s", msg)
	}
	if strings.Contains(msg, composeCHSentinel) {
		t.Error("the credential is in the failure text")
	}
	if !strings.Contains(msg, "[redacted]") {
		t.Error("the credential was not replaced")
	}
	if strings.Contains(msg, "noise line 1\n") || len(msg) > 9000 {
		t.Errorf("the failure holds more than the tail of the output: %d bytes", len(msg))
	}
}

// The kill reaches the children of the script: a child of the script is dead
// when the run returns, and it did not hold the run until the wait delay.
func TestComposeExecKillReachesTheProcessGroup(t *testing.T) {
	dir, script := composeHangScript(t, "echo \"STEP group rc=0 00:00:00\"\nsleep 20 &\necho \"CHILD $!\"\nwait\n")
	// Clean time about 1.1 s; ceiling 30 s.
	r, err := composeExecWithin(t, 30*time.Second, dir, time.Second, script)
	if err == nil {
		t.Fatal("a step that never returns gave no error")
	}
	pid := composeChildPID(t, r.stdout)
	t.Cleanup(func() { _ = syscall.Kill(pid, syscall.SIGKILL) })
	for i := 0; i < 100 && syscall.Kill(pid, 0) == nil; i++ {
		time.Sleep(50 * time.Millisecond)
	}
	if syscall.Kill(pid, 0) == nil {
		t.Errorf("a child of the script (pid %d) is still alive after the kill", pid)
	}
}

func composeChildPID(t *testing.T, stdout string) int {
	t.Helper()
	_, rest, ok := strings.Cut(stdout, "CHILD ")
	if !ok {
		t.Fatalf("the script did not report its child: %q", stdout)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(strings.SplitN(rest, "\n", 2)[0]))
	if err != nil || pid <= 1 {
		t.Fatalf("child pid in %q: %v", rest, err)
	}
	return pid
}

// A child that left the process group still cannot hold the run: the wait delay ends it. The child sleeps far longer
// than the ceiling, so a run that waits for it never returns.
func TestComposeExecKillIsBoundedForAChildOutsideTheGroup(t *testing.T) {
	dir, script := composeHangScript(t, "echo \"STEP outside rc=0 00:00:00\"\nsetsid sleep 120 &\necho \"CHILD $!\"\nwait\n")
	// Clean time about 6.1 s (1 s ceiling of the script + the 5 s wait delay); ceiling 40 s.
	r, err := composeExecWithin(t, 40*time.Second, dir, time.Second, script)
	if err == nil {
		t.Fatal("a step that never returns gave no error")
	}
	pid := composeChildPID(t, r.stdout)
	t.Cleanup(func() { _ = syscall.Kill(pid, syscall.SIGKILL) })
}

// On a kill the failure holds a snapshot of the process group taken BEFORE the
// kill: the planted step is in it by its command name, with its state, and no
// argument or environment value is printed.
func TestComposeExecKillPrintsTheProcessGroupSnapshotWithoutArguments(t *testing.T) {
	dir, script := composeHangScript(t,
		"cp /bin/sleep \"$PWD/planted-step\"\n"+
			"echo \"STEP snapshot rc=0 00:00:00\"\n"+
			"exec \"$PWD/planted-step\" 31337\n")
	// Clean time about 1.1 s; ceiling 20 s.
	_, err := composeExecWithin(t, 20*time.Second, dir, time.Second, script, "HANG_SECRET="+composeCHSentinel)
	if err == nil {
		t.Fatal("a step that never returns gave no error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "process group at the kill") {
		t.Fatalf("the failure has no process group snapshot: %.300s", msg)
	}
	_, snapshot, _ := strings.Cut(msg, "process group at the kill")
	snapshot, _, _ = strings.Cut(snapshot, "the last output")
	if !strings.Contains(snapshot, "planted-step") {
		t.Errorf("the planted step is not in the snapshot: %q", snapshot)
	}
	if !strings.Contains(snapshot, "STAT") || !strings.Contains(snapshot, "WCHAN") {
		t.Errorf("the snapshot has no state columns: %q", snapshot)
	}
	_, rows, _ := strings.Cut(snapshot, "COMMAND")
	groups := map[string]bool{}
	for _, row := range strings.Split(rows, "\n") {
		if f := strings.Fields(row); len(f) >= 7 {
			groups[f[2]] = true
		}
	}
	if len(groups) != 1 {
		t.Errorf("the snapshot lists %d process groups, want only the group of the script: %q", len(groups), snapshot)
	}
	if strings.Contains(snapshot, "31337") || strings.Contains(msg, composeCHSentinel) {
		t.Errorf("the snapshot prints an argument or an environment value: %q", snapshot)
	}
}
