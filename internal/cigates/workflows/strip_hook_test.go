package workflows_test

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func stripHookRun(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), "lefthook.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var cfg struct {
		CommitMsg struct {
			Commands map[string]struct {
				Run string `yaml:"run"`
			} `yaml:"commands"`
		} `yaml:"commit-msg"`
	}
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		t.Fatal(err)
	}
	run := cfg.CommitMsg.Commands["strip-agent-attribution"].Run
	if !strings.Contains(run, "{1}") {
		t.Fatalf("commit-msg strip-agent-attribution has no {1} argument: %q", run)
	}
	return run
}

func runStripHook(t *testing.T, arg string) (int, string) {
	t.Helper()
	cmd := exec.Command("bash", "-c", strings.ReplaceAll(stripHookRun(t), "{1}", arg))
	cmd.Dir = repoRoot(t)
	out, err := cmd.CombinedOutput()
	if err == nil {
		return 0, string(out)
	}
	var ee *exec.ExitError
	if !errors.As(err, &ee) {
		t.Fatal(err)
	}
	return ee.ExitCode(), string(out)
}

func TestStripHookPreservesTheToolsExitStatuses(t *testing.T) {
	if code, out := runStripHook(t, ""); code != 2 || !strings.Contains(out, "usage:") {
		t.Fatalf("hook with no file: exit %d, want 2 and a usage line; output %q", code, out)
	}
	if code, _ := runStripHook(t, filepath.Join(t.TempDir(), "missing")); code != 1 {
		t.Fatalf("hook with an unreadable file: exit %d, want 1", code)
	}
}

func TestStripHookStripsAnAgentTrailerThroughTheConfiguredCommand(t *testing.T) {
	msg := filepath.Join(t.TempDir(), "COMMIT_EDITMSG")
	body := "fix: a thing\n\nCo-authored-by: Sisyphus <clio-agent@sisyphuslabs.ai>\nCo-authored-by: Real Person <real@example.com>\n"
	if err := os.WriteFile(msg, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	if code, out := runStripHook(t, msg); code != 0 {
		t.Fatalf("hook exit %d: %s", code, out)
	}
	got, err := os.ReadFile(msg)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(got), "Sisyphus") || !strings.Contains(string(got), "Real Person") {
		t.Fatalf("hook rewrote the message wrongly: %q", got)
	}
}
