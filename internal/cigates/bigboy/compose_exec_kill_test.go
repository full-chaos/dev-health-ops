package bigboy_test

import (
	"os"
	"path/filepath"
	"strings"
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
	started := time.Now()
	_, err := composeExec(dir, composeHangEnv, time.Second, "bash", script)
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
	if time.Since(started) > 10*time.Second {
		t.Errorf("the kill took %s", time.Since(started))
	}
}

// The kill reaches the children of the script: a child that holds the output
// pipes does not keep the run alive.
func TestComposeExecKillReachesTheProcessGroup(t *testing.T) {
	dir, script := composeHangScript(t, "echo \"STEP group rc=0 00:00:00\"\nsleep 20 &\nwait\n")
	started := time.Now()
	_, err := composeExec(dir, composeHangEnv, time.Second, "bash", script)
	if err == nil {
		t.Fatal("a step that never returns gave no error")
	}
	if elapsed := time.Since(started); elapsed > 8*time.Second {
		t.Errorf("a child of the script held the run for %s after the kill", elapsed)
	}
}

// A child that left the process group still cannot hold the run past the wait
// delay.
func TestComposeExecKillIsBoundedForAChildOutsideTheGroup(t *testing.T) {
	dir, script := composeHangScript(t, "echo \"STEP outside rc=0 00:00:00\"\nsetsid sleep 20 &\nwait\n")
	started := time.Now()
	_, err := composeExec(dir, composeHangEnv, time.Second, "bash", script)
	if err == nil {
		t.Fatal("a step that never returns gave no error")
	}
	if elapsed := time.Since(started); elapsed > 12*time.Second {
		t.Errorf("a child outside the group held the run for %s after the kill", elapsed)
	}
}
