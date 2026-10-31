//go:build !race

package integrationshards_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// The shard run copies go test's stdout through a tee for the weight drift check. A process that go test leaves behind
// and that still holds that stdout keeps the tee alive; the run must not wait for it past a bound (CHAOS-11794 is the
// same class on the providersync shard): the go test status stays, the drift check reads what the tee had written, and
// one line says the log was partial.
func TestAShardRunDoesNotWaitForAProcessHoldingGoTestStdout(t *testing.T) {
	t.Parallel()
	root := rootOf(t)
	shard, weights, heavy, _ := driftShard(t)
	realGo, err := exec.LookPath("go")
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		rc          int
		wait        string
		secs        int
		wantWarning bool
	}{
		{0, "2", 2, true}, {1, "2", 2, true},
		// Zero-padded values are decimal, not octal: 08 and 09 are no arithmetic error, 010 is ten seconds, not eight.
		{0, "08", 8, true}, {0, "09", 9, true}, {0, "010", 10, true}, {0, "0", 0, false}, {0, "00", 0, false},
	} {
		rc := c.rc
		t.Run("go test exits "+strconv.Itoa(rc)+", wait "+c.wait, func(t *testing.T) {
			t.Parallel()
			bin, state := t.TempDir(), t.TempDir()
			holderPID := filepath.Join(state, "holder.pid")
			var okLines []string
			for p, w := range weights {
				if p == heavy {
					w *= 2
				}
				okLines = append(okLines, fmt.Sprintf("printf 'ok  \\tgithub.com/full-chaos/dev-health-ops/%s\\t%ds\\n'", p, w))
			}
			goScript := "#!/usr/bin/env bash\nset -euo pipefail\nif [ \"${1-}\" = test ]; then\n" +
				"  sleep 600 &\n  echo $! > \"${FAKE_HOLDER_PID}\"\n  " + strings.Join(okLines, "\n  ") + "\n  exit \"${FAKE_RC}\"\nfi\n" +
				"exec '" + realGo + "' \"$@\"\n"
			for name, text := range map[string]string{"go": goScript, "docker": "#!/bin/sh\nexit 0\n"} {
				if err := os.WriteFile(filepath.Join(bin, name), []byte(text), 0o755); err != nil {
					t.Fatal(err)
				}
			}
			t.Cleanup(func() {
				if raw, err := os.ReadFile(holderPID); err == nil {
					if pid, _ := strconv.Atoi(strings.TrimSpace(string(raw))); pid > 0 {
						_ = syscall.Kill(pid, syscall.SIGKILL)
					}
				}
			})
			out, err := os.Create(filepath.Join(state, "out.txt"))
			if err != nil {
				t.Fatal(err)
			}
			defer out.Close()
			cmd := exec.Command("bash", "ci/check_go.sh", "integration-shard", "packages", strconv.Itoa(shard))
			cmd.Dir = root
			cmd.Env = inheritedEnv(deadlineEnvNames,
				"PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"), "FAKE_HOLDER_PID="+holderPID, "FAKE_RC="+strconv.Itoa(rc),
				"DEV_HEALTH_GO_TEE_WAIT_SECS="+c.wait, "TMPDIR="+t.TempDir(),
				"DEV_HEALTH_GO_INTEGRATION_SHARD_MANIFEST="+shardManifestDir(root),
				"DEV_HEALTH_GO_PROVIDER_TEST_SHARD_MANIFEST="+filepath.Join(root, "ci", "go_providersync_test_shards.tsv"))
			cmd.Stdout, cmd.Stderr = out, out
			cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- cmd.Wait() }()
			select {
			case <-done:
			case <-time.After(150 * time.Second):
				_ = cmd.Process.Kill()
				<-done
				t.Fatal("the shard run still waits for the process holding go test's stdout 150 s after go test ended")
			}
			text, _ := os.ReadFile(out.Name())
			if code := cmd.ProcessState.ExitCode(); code != rc {
				t.Errorf("exit %d, want go test's %d", code, rc)
			}
			want := fmt.Sprintf("%s%s measured %ds, weight %ds (+100%%)", driftWarning, heavy, weights[heavy]*2, weights[heavy])
			if c.wantWarning && (strings.Count(string(text), driftWarning) != 1 || !strings.Contains(string(text), want+"\n")) {
				t.Errorf("want exactly the warning %q from the lines already read", want)
			}
			if n := strings.Count(string(text), "partial log"); n != 1 {
				t.Errorf("%d partial-log line(s), want exactly one", n)
			}
			if !strings.Contains(string(text), fmt.Sprintf("stdout %ds after go test ended", c.secs)) {
				t.Errorf("the partial-log line does not name %d decimal seconds", c.secs)
			}
			if strings.Contains(string(text), "too great") || strings.Contains(string(text), "syntax error") {
				t.Errorf("an arithmetic error reached the output for wait %s", c.wait)
			}
		})
	}
}
