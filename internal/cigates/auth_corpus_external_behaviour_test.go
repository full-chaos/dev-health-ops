package cigates_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Behaviour of ci/auth_corpus_external.sh and ci/auth_corpus_set_env.sh against a fake docker: it records every
// call, answers the few queries the scripts make, and blocks on `up` when asked. No docker runs.

type fakeDocker struct {
	dir, record string
}

func newFakeDocker(t *testing.T) fakeDocker {
	t.Helper()
	dir := t.TempDir()
	record := filepath.Join(dir, "calls")
	docker := `#!/bin/sh
printf '%s\n' "$*" >> "` + record + `"
case "$*" in
  *" up "*)
    if [ -n "$FAKE_BLOCK_UP" ]; then sleep "$FAKE_BLOCK_UP"; fi ;;
  *" ps -q go-api"*) echo cid-go-api ;;
  *"inspect --format"*) echo "${FAKE_HEALTH:-healthy}" ;;
esac
exit 0
`
	goShim := "#!/bin/sh\nexit 0\n"
	for name, body := range map[string]string{"docker": docker, "go": goShim} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return fakeDocker{dir: dir, record: record}
}

func (f fakeDocker) calls(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile(f.record)
	if err != nil {
		return ""
	}
	return string(raw)
}

func (f fakeDocker) env(extra ...string) []string {
	return append([]string{"PATH=" + f.dir + ":" + os.Getenv("PATH"), "HOME=" + f.dir}, extra...)
}

func TestAuthCorpusExternalCleansUpWhenTerminated(t *testing.T) {
	fake := newFakeDocker(t)
	cmd := exec.Command("bash", filepath.Join(authProofRoot(t), "ci", "auth_corpus_external.sh"), "go-api")
	cmd.Env = fake.env("FAKE_BLOCK_UP=3")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _ = cmd.Wait(); close(done) }()
	deadline := time.Now().Add(20 * time.Second)
	for !strings.Contains(fake.calls(t), " up ") {
		if time.Now().After(deadline) {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			t.Fatalf("the script never reached `compose up`: %q", fake.calls(t))
		}
		time.Sleep(100 * time.Millisecond)
	}
	// The script itself is terminated (a wrapper such as timeout signals only its child) while compose is still
	// running: bash then dies at once unless it traps the signal, and an EXIT trap alone does not run.
	if err := syscall.Kill(cmd.Process.Pid, syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		t.Fatal("the script did not exit after SIGTERM and the end of compose")
	}
	if !strings.Contains(fake.calls(t), " down --volumes") {
		t.Errorf("SIGTERM left the compose project behind; calls:\n%s", fake.calls(t))
	}
}

func setEnvFixture(t *testing.T, fake fakeDocker) (state string, env []string) {
	t.Helper()
	state = filepath.Join(t.TempDir(), "state.env")
	if err := os.WriteFile(state, []byte("export JWT_SECRET_KEY=key\nexport AUTH_AUTO_CREATE_ORG_ON_REGISTER=old\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return state, fake.env("AUTH_CORPUS_PROJECT=authcorpus1a2b3c", "AUTH_CORPUS_STATE="+state)
}

func TestAuthCorpusSetEnvRecreatesGoAPIAndKeepsTheState(t *testing.T) {
	fake := newFakeDocker(t)
	state, env := setEnvFixture(t, fake)
	if out, code := runScript(t, "auth_corpus_set_env.sh", env, "AUTH_AUTO_CREATE_ORG_ON_REGISTER", " OFF "); code != 0 {
		t.Fatalf("set: exit %d: %s", code, out)
	}
	calls := fake.calls(t)
	if !strings.Contains(calls, "compose -p authcorpus1a2b3c") || !strings.Contains(calls, "up --detach --no-deps --force-recreate --no-build go-api") {
		t.Errorf("go-api was not recreated through compose; calls:\n%s", calls)
	}
	raw, _ := os.ReadFile(state)
	text := string(raw)
	if !strings.Contains(text, "export JWT_SECRET_KEY=key") || strings.Contains(text, "=old") || !strings.Contains(text, "AUTH_AUTO_CREATE_ORG_ON_REGISTER=") {
		t.Errorf("state after the set:\n%s", text)
	}
	if out, code := runScript(t, "auth_corpus_set_env.sh", env, "AUTH_AUTO_CREATE_ORG_ON_REGISTER", ""); code != 0 {
		t.Fatalf("unset: exit %d: %s", code, out)
	}
	raw, _ = os.ReadFile(state)
	if strings.Contains(string(raw), "AUTH_AUTO_CREATE_ORG_ON_REGISTER") || !strings.Contains(string(raw), "JWT_SECRET_KEY") {
		t.Errorf("state after the unset:\n%s", raw)
	}
}

func TestAuthCorpusSetEnvGivesUpWhenGoAPIStaysUnhealthy(t *testing.T) {
	fake := newFakeDocker(t)
	_, env := setEnvFixture(t, fake)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "bash", filepath.Join(authProofRoot(t), "ci", "auth_corpus_set_env.sh"), "X_NAME", "v")
	cmd.Env = append(env, "FAKE_HEALTH=starting", "AUTH_CORPUS_WAIT_SECONDS=3")
	out, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("the wait is not bounded by AUTH_CORPUS_WAIT_SECONDS: still running after 15 s; output:\n%s", out)
	}
	if err == nil || !strings.Contains(string(out), "healthy") {
		t.Errorf("an unhealthy go-api was reported as ready: err=%v output=%s", err, out)
	}
}
