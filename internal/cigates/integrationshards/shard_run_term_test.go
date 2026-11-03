//go:build !race

package integrationshards_test

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

// A TERM sent to the check_go.sh process alone (a step timeout or a runner cancel signals the main process, not its
// group) must stop the go test group and reap the testcontainers session by id, then exit 143 (CHAOS-10036). The trap
// lives in the shell that runs run_integration_go_test, so that function must run in the main shell of a shard run.
func TestATermToTheShardRunScriptReapsTheSessionAndStopsGoTest(t *testing.T) {
	t.Parallel()
	root := rootOf(t)
	shard, _, _, _ := driftShard(t)
	realGo, err := exec.LookPath("go")
	if err != nil {
		t.Fatal(err)
	}
	bin, state := t.TempDir(), t.TempDir()
	pidFile, dockerLog := filepath.Join(state, "gotest.pid"), filepath.Join(state, "docker.log")
	goScript := "#!/usr/bin/env bash\nset -euo pipefail\nif [ \"${1-}\" = test ]; then\n  echo $$ > \"${FAKE_PIDFILE}\"\n  exec sleep 600\nfi\nexec '" + realGo + "' \"$@\"\n"
	dockerScript := "#!/bin/sh\necho \"$*\" >> \"${FAKE_DOCKER_LOG}\"\nexit 0\n"
	for name, text := range map[string]string{"go": goScript, "docker": dockerScript} {
		if err := os.WriteFile(filepath.Join(bin, name), []byte(text), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	const session = "sess-termtest01"
	env := inheritedEnv(deadlineEnvNames,
		"PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"), "FAKE_PIDFILE="+pidFile, "FAKE_DOCKER_LOG="+dockerLog,
		"TESTCONTAINERS_SESSION_ID="+session, "TMPDIR="+t.TempDir(),
		"DEV_HEALTH_GO_INTEGRATION_SHARD_MANIFEST="+shardManifestDir(root),
		"DEV_HEALTH_GO_PROVIDER_TEST_SHARD_MANIFEST="+filepath.Join(root, "ci", "go_providersync_test_shards.tsv"))
	cmd := exec.Command("bash", "ci/check_go.sh", "integration-shard", "packages", strconv.Itoa(shard))
	cmd.Dir, cmd.Env = root, env
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	var goPID int
	t.Cleanup(func() {
		if goPID > 0 {
			_ = syscall.Kill(goPID, syscall.SIGKILL)
		}
	})
	deadline := time.Now().Add(120 * time.Second)
	for goPID == 0 {
		if raw, err := os.ReadFile(pidFile); err == nil {
			goPID, _ = strconv.Atoi(strings.TrimSpace(string(raw)))
		}
		if goPID == 0 {
			select {
			case err := <-done:
				t.Fatalf("the shard run ended before go test started: %v", err)
			case <-time.After(100 * time.Millisecond):
			}
			if time.Now().After(deadline) {
				_ = cmd.Process.Kill()
				t.Fatal("the fake go test never started")
			}
		}
	}
	time.Sleep(1500 * time.Millisecond)
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	var runErr error
	select {
	case runErr = <-done:
	case <-time.After(60 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("the shard run did not end within 60 s of the TERM")
	}
	code := -1
	if cmd.ProcessState != nil {
		code = cmd.ProcessState.ExitCode()
	}
	if code != 143 {
		t.Errorf("exit %d (%v), want 143", code, runErr)
	}
	log, _ := os.ReadFile(dockerLog)
	if !strings.Contains(string(log), "label=org.testcontainers.sessionId="+session) {
		t.Errorf("the session %s was not reaped by id after the TERM; docker calls: %q", session, log)
	}
	if err := syscall.Kill(goPID, 0); err == nil {
		t.Errorf("go test process %d is still running after the TERM", goPID)
	}
}
