package workflows_test

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
  *" down "*)
    if [ -n "$FAKE_BLOCK_DOWN" ]; then exec sleep "$FAKE_BLOCK_DOWN"; fi ;;
  *" up "*)
    if [ -n "$FAKE_BLOCK_UP" ]; then exec sleep "$FAKE_BLOCK_UP"; fi
    if [ -n "$FAKE_FAIL_UP" ]; then exit 1; fi ;;
  *" port "*) echo 127.0.0.1:12345 ;;
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
	if out, code := runSetEnv(t, env, "AUTH_AUTO_CREATE_ORG_ON_REGISTER", " OFF "); code != 0 {
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
	if out, code := runSetEnv(t, env, "AUTH_AUTO_CREATE_ORG_ON_REGISTER", ""); code != 0 {
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

func TestAuthCorpusExternalCleanupIsBounded(t *testing.T) {
	fake := newFakeDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "bash", filepath.Join(authProofRoot(t), "ci", "auth_corpus_external.sh"), "go-api")
	cmd.Env = fake.env("FAKE_FAIL_UP=1", "FAKE_BLOCK_DOWN=40", "AUTH_CORPUS_DOWN_SECONDS=2")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	out, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("a hung `compose down` held the cleanup past AUTH_CORPUS_DOWN_SECONDS (still running after 12 s); output:\n%s", out)
	}
	if err == nil {
		t.Errorf("the script reported success although compose up failed:\n%s", out)
	}
}

// compose.yml's migrate service refuses a database that holds objects but no alembic_version table, and
// auth-migrate creates the auth schema: starting them together races. Leg acp starts go-api (and so migrate) first
// and auth-service after it.
func TestAuthCorpusExternalAcpLegStartsAuthServiceAfterGoAPI(t *testing.T) {
	fake := newFakeDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "bash", filepath.Join(authProofRoot(t), "ci", "auth_corpus_external.sh"), "acp")
	cmd.Env = fake.env()
	_, _ = cmd.CombinedOutput()
	var ups []string
	for _, call := range strings.Split(fake.calls(t), "\n") {
		if strings.Contains(call, " up ") {
			ups = append(ups, call[strings.Index(call, " up ")+4:])
		}
	}
	if len(ups) != 2 {
		t.Fatalf("want two `compose up` calls (go-api, then auth-service), got %d: %q", len(ups), ups)
	}
	if !strings.HasSuffix(ups[0], " go-api") || strings.Contains(ups[0], "auth-service") {
		t.Errorf("the first up must start go-api alone: %q", ups[0])
	}
	if !strings.HasSuffix(ups[1], " auth-service") || strings.Contains(ups[1], "go-api") {
		t.Errorf("the second up must start auth-service alone: %q", ups[1])
	}
}

// `compose up --wait-timeout` bounds the wait for health, not a build or a hung compose command: every up carries
// its own bound.
func TestAuthCorpusExternalUpIsBounded(t *testing.T) {
	fake := newFakeDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "bash", filepath.Join(authProofRoot(t), "ci", "auth_corpus_external.sh"), "go-api")
	cmd.Env = fake.env("FAKE_BLOCK_UP=60", "AUTH_CORPUS_UP_SECONDS=2", "AUTH_CORPUS_DOWN_SECONDS=2")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	out, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("a hung `compose up` was not bounded by AUTH_CORPUS_UP_SECONDS (still running after 12 s); output:\n%s", out)
	}
	if err == nil {
		t.Errorf("the script reported success although compose up never finished:\n%s", out)
	}
}

func TestAuthCorpusSetEnvRecreateIsBounded(t *testing.T) {
	fake := newFakeDocker(t)
	_, env := setEnvFixture(t, fake)
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "bash", filepath.Join(authProofRoot(t), "ci", "auth_corpus_set_env.sh"), "X_NAME", "v")
	cmd.Env = append(env, "FAKE_BLOCK_UP=60", "AUTH_CORPUS_WAIT_SECONDS=2")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	out, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("a hung recreate was not bounded (still running after 12 s); output:\n%s", out)
	}
	if err == nil {
		t.Errorf("the hook reported success although the recreate never finished:\n%s", out)
	}
}

// fakeCurl answers the acp leg's two kinds of curl call: OPTIONS to auth-service's API port prints 405 for a path in
// FAKE_SERVED (comma-separated paths) and 404 for any other; the /metrics read prints FAKE_METRICS.
func (f fakeDocker) withCurl(t *testing.T) {
	t.Helper()
	curl := `#!/bin/bash
for arg in "$@"; do last="$arg"; done
if [[ " $* " == *" -X OPTIONS "* ]]; then
  path="/${last#http://*/}"
  if [[ ",${FAKE_SERVED}," == *",${path},"* ]]; then printf 405; else printf 404; fi
  exit 0
fi
printf '%s\n' "${FAKE_METRICS:-}"
`
	if err := os.WriteFile(filepath.Join(f.dir, "curl"), []byte(curl), 0o755); err != nil {
		t.Fatal(err)
	}
}

func runAcpLeg(t *testing.T, extra ...string) (fakeDocker, string, error) {
	t.Helper()
	fake := newFakeDocker(t)
	fake.withCurl(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "bash", filepath.Join(authProofRoot(t), "ci", "auth_corpus_external.sh"), "acp")
	cmd.Env = fake.env(extra...)
	out, err := cmd.CombinedOutput()
	return fake, string(out), err
}

func proxyFlagRecreates(calls string) []string {
	var out []string
	for _, line := range strings.Split(calls, "\n") {
		if strings.Contains(line, " up ") && strings.Contains(line, "--force-recreate") {
			out = append(out, line)
		}
	}
	return out
}

// Leg acp forwards an aggregate only when the running auth-service serves every route of it, and the sessions
// aggregate gates the others: a flag on for a route auth-service does not serve would compare auth-service's 404
// with go-api's answer.
func TestAuthCorpusAcpLegForwardsOnlyWhatAuthServiceServes(t *testing.T) {
	sessions := "/api/v1/auth/refresh,/api/v1/auth/logout"
	forwarded := func(aggregate string) string {
		return `devhealth_api_auth_proxy_requests_total{aggregate="` + aggregate + `",outcome="forwarded"} 3`
	}
	for name, tc := range map[string]struct {
		served, metrics string
		wantRecreates   int
		wantForwards    string
		wantProbes      []string
		wantError       string
	}{
		"nothing served": {wantForwards: "leg acp forwards: none (", wantProbes: []string{
			"probe sessions: not served (2 routes)", "probe local_login: not served (1 routes)", "probe credentials: not served (6 routes)",
			"probe social: not served (1 routes)", "probe oidc: not served (2 routes)", "probe saml: not served (3 routes)"}},
		"login without sessions": {served: "/api/v1/auth/login", wantForwards: "leg acp forwards: none (",
			wantProbes: []string{"probe sessions: not served (2 routes)", "probe local_login: served (1 routes)"}},
		"sessions":           {served: sessions, metrics: forwarded("sessions"), wantRecreates: 1, wantForwards: "leg acp forwards: sessions\n"},
		"sessions and login": {served: sessions + ",/api/v1/auth/login", metrics: forwarded("sessions") + "\n" + forwarded("local_login"), wantRecreates: 2, wantForwards: "leg acp forwards: sessions local_login\n"},
		"half an aggregate": {served: "/api/v1/auth/refresh", wantError: "auth-service serves 1 of the 2 routes of aggregate sessions",
			wantProbes: []string{"probe sessions: partial (1 of 2 routes served)"}},
		"half a non-session aggregate": {served: sessions + ",/api/v1/auth/oidc/p-probe/authorize",
			wantError:  "auth-service serves 1 of the 2 routes of aggregate oidc",
			wantProbes: []string{"probe sessions: served (2 routes)", "probe oidc: partial (1 of 2 routes served)"}},
		"half a non-session aggregate without sessions": {served: "/api/v1/auth/saml/p-probe/metadata",
			wantError: "auth-service serves 1 of the 3 routes of aggregate saml"},
		"served but not forwarded": {served: sessions, wantRecreates: 1, wantError: "no forwarded request for aggregate sessions"},
		"forwarded while off":      {metrics: forwarded("saml"), wantError: "go-api forwarded aggregate saml, whose flag is off"},
	} {
		t.Run(name, func(t *testing.T) {
			fake, out, err := runAcpLeg(t, "FAKE_SERVED="+tc.served, "FAKE_METRICS="+tc.metrics)
			if tc.wantError != "" {
				if err == nil || !strings.Contains(out, tc.wantError) {
					t.Fatalf("want a failure naming %q, got err=%v:\n%s", tc.wantError, err, out)
				}
			} else if err != nil || !strings.Contains(out, "AUTH CORPUS EXTERNAL (acp) PASSED") {
				t.Fatalf("want a pass, got err=%v:\n%s", err, out)
			}
			if tc.wantForwards != "" && !strings.Contains(out, tc.wantForwards) {
				t.Errorf("want %q in the output:\n%s", tc.wantForwards, out)
			}
			for _, line := range tc.wantProbes {
				if !strings.Contains(out, line+"\n") {
					t.Errorf("want the probe line %q in the output:\n%s", line, out)
				}
			}
			if got := proxyFlagRecreates(fake.calls(t)); len(got) != tc.wantRecreates {
				t.Errorf("go-api recreated %d times for proxy flags, want %d: %q", len(got), tc.wantRecreates, got)
			}
		})
	}
}

// Leg acp stores go-api's session issue credential and applies the sessions-owned grants before go-api first
// restarts with the sessions flag, and no call line carries the token.
func TestAuthCorpusAcpLegStoresTheIssueCredentialAndGrantsBeforeTheSessionsFlag(t *testing.T) {
	fake, out, err := runAcpLeg(t, "FAKE_SERVED=/api/v1/auth/refresh,/api/v1/auth/logout",
		"FAKE_METRICS=devhealth_api_auth_proxy_requests_total{aggregate=\"sessions\",outcome=\"forwarded\"} 3")
	if err != nil {
		t.Fatalf("leg acp failed: %v\n%s", err, out)
	}
	calls := fake.calls(t)
	if strings.Contains(calls, "svc_authsess_") || strings.Contains(out, "svc_authsess_") {
		t.Fatalf("the session issue token reached a docker argument or the output:\n%s\n%s", calls, out)
	}
	lines := strings.Split(calls, "\n")
	index := func(part string) int {
		for i, line := range lines {
			if strings.Contains(line, part) {
				return i
			}
		}
		return -1
	}
	store, grants, flag := index("exec -T postgres psql"), index("run --rm --no-deps -e AUTH_PROXY_SESSIONS=true go-river-migrate"), index("--force-recreate")
	if store < 0 || grants < 0 || flag < 0 || !(store < flag && grants < flag) {
		t.Fatalf("want the credential store (%d) and the grants (%d) before the first go-api recreate (%d):\n%s", store, grants, flag, calls)
	}
}
