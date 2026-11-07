package workflows_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

// Behaviour of ci/auth_corpus_external.sh and ci/auth_corpus_set_env.sh against a fake docker: it records every
// call, answers the few queries the scripts make, and blocks on `up` when asked. No docker runs.

// fakeSigningPEM is what the fake docker's `cp` of auth-service's signing key writes: a marker no output may show.
const fakeSigningPEM = "FAKE-SIGNING-KEY-MARKER-7f3e9c21"

// noSessionsRefusal is the corpus script's refusal of a leg whose go-api does not forward sessions (CHAOS-13154).
const noSessionsRefusal = "sessions are not forwarded"

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
  *" cp auth-service:"*)
    # docker compose cp writes the key file itself, with the container file's mode.
    for arg in "$@"; do last="$arg"; done
    printf '%s\n' "$FAKE_SIGNING_PEM" > "$last" && chmod 0400 "$last" ;;
  *users-backfill*)
    if [ -n "$FAKE_FAIL_BACKFILL" ]; then exit 1; fi ;;
  *" down "*)
    if [ -n "$FAKE_BLOCK_DOWN" ]; then exec sleep "$FAKE_BLOCK_DOWN"; fi ;;
  *" up "*)
    case "$*" in
      *--force-recreate*" auth-service") : > "` + record + `.owned" ;;
      *--force-recreate*" go-api") printf '%s %s\n' "${AUTH_PROXY_CREDENTIALS:-}" "${AUTH_USERS_OWNED:-}" >> "` + record + `.goapi"; printf '%s\n' "${AUTH_PROXY_SOCIAL:-}" >> "` + record + `.goapisocial" ;;
    esac
    printf '%s %s %s\n' "${AUTH_SESSION_SUCCESSOR_KEY:-}" "${AUTH_LOGIN_LOCKOUT_KEY:-}" "${AUTH_JWT_SECRET_KEY:+jwt}" >> "` + record + `.keys"
    for arg in "$@"; do last="$arg"; done
    printf '%s bootstrap=%s subject=%s credential=%s token=%s\n' "$last" "${AUTH_WORKLOAD_BOOTSTRAP:-}" "${AUTH_SESSION_ISSUE_WORKLOAD_SUBJECT:-}" "${AUTH_SESSION_ISSUE_CREDENTIAL:-}" "${AUTH_SESSION_ISSUE_TOKEN:+set}" >> "` + record + `.workload"
    printf '%s %s\n' "$last" "${AUTH_TOKEN_ISSUER_URL:-unset}" >> "` + record + `.upissuer"
    if [ -n "$FAKE_BLOCK_UP" ]; then exec sleep "$FAKE_BLOCK_UP"; fi
    if [ -n "$FAKE_FAIL_UP" ]; then exit 1; fi ;;
  *" logs "*" auth-service"*)
    if [ -z "${FAKE_AUTH_LOGS+x}" ]; then echo 'level=INFO msg="listener bound"'; else printf '%s\n' "$FAKE_AUTH_LOGS"; fi ;;
  *" port "*) echo 127.0.0.1:12345 ;;
  *" config --format json"*)
    if [ -n "${FAKE_COMPOSE_CONFIG:-}" ]; then printf '%s\n' "$FAKE_COMPOSE_CONFIG"; else echo '{"services":{"go-api":{"environment":{"AUTH_PROXY_SESSIONS":"true","AUTH_PROXY_LOCAL_LOGIN":"true","AUTH_PROXY_CREDENTIALS":"true","AUTH_PROXY_SOCIAL":"true"}},"auth-service":{"environment":{"AUTH_TOKEN_ISSUER_URL":"https://auth.corpus.example","AUTH_SIGNING_KEY_ID":"auth-signing-corpus"}}}}'; fi ;;
  *" ps -q go-api"*) echo cid-go-api ;;
  *"exec -T postgres psql"*"-At -f -"*) echo prn_AAAAAAAAAAAAAAAAAAAAAA ;;
  *"inspect --format"*)
    # go-api refuses AUTH_USERS_OWNED=true without AUTH_PROXY_CREDENTIALS (apiservice/usersowned.go)
    if [ -z "$FAKE_HEALTH" ] && [ -e "` + record + `.goapi" ] && [ "$(tail -n 1 "` + record + `.goapi")" = " true" ]; then echo unhealthy; else echo "${FAKE_HEALTH:-healthy}"; fi ;;
esac
exit 0
`
	goShim := `#!/bin/sh
case "$*" in
  *" certs -out "*)
    for arg in "$@"; do last="$arg"; done
    mkdir -p "$last" && : > "$last/ca.pem" && : > "$last/server.pem" && : > "$last/server.key" ;;
esac
case "$1" in
  test)
    case "$*" in
      *" -c "*) ;;
      *) printf 'SMTPADMIN %s\n' "$DEVHEALTH_AUTH_ORACLE_SMTP_ADMIN" >> "` + record + `"
         printf '%s\n' "${DEVHEALTH_AUTH_ORACLE_FORWARDED-unset}" >> "` + record + `.forwarded"
         printf '%s\n' "${DEVHEALTH_AUTH_ORACLE_SESSION_ISSUER-unset}" >> "` + record + `.issuer"
         printf '%s\n' "${DEVHEALTH_AUTH_ORACLE_USERS_OWNED-unset}" >> "` + record + `.usersowned"
         key="${DEVHEALTH_AUTH_ORACLE_SIGNING_KEY_FILE-unset}"
         printf '%s %s %s %s\n' "$key" "${DEVHEALTH_AUTH_ORACLE_SIGNING_KEY_ID-unset}" "$(stat -c %a "$key" 2>/dev/null)" "$(stat -c %a "$(dirname "$key")" 2>/dev/null)" >> "` + record + `.signing"
         if [ -f "$key" ]; then cat "$key" >> "` + record + `.signingcontent"; fi ;;
    esac
    case "$*" in
      *" -c "*)
        # go test -c ... -o BIN: BIN is the fake test binary, its pid is recorded where the test can read it.
        for arg in "$@"; do if [ "$prev" = "-o" ]; then out="$arg"; fi; prev="$arg"; done
        printf '%s\n' "$*" >> "` + record + `.compile"
        cat > "$out" <<'BIN'
#!/bin/sh
printf 'SMTPADMIN %s\n' "$DEVHEALTH_AUTH_ORACLE_SMTP_ADMIN" >> "$FAKE_RECORD"
printf 'TESTENV forwarded=%s owned=%s\n' "${DEVHEALTH_AUTH_ORACLE_FORWARDED-unset}" "${DEVHEALTH_AUTH_ORACLE_USERS_OWNED-unset}" >> "$FAKE_RECORD"
if [ -n "$FAKE_GO_TEST_EXITS" ]; then exit 0; fi
printf '%s\n' "$$" > "$FAKE_TEST_PID_FILE"
printf 'SEEDED\n' >> "$FAKE_RECORD"
: > "$DEVHEALTH_AUTH_ORACLE_SEEDED_FILE"
i=0
while [ ! -e "$DEVHEALTH_AUTH_ORACLE_FLIPPED_FILE" ] && [ "$i" -lt 300 ]; do sleep 0.1; i=$((i+1)); done
[ -e "$DEVHEALTH_AUTH_ORACLE_FLIPPED_FILE" ] || exit 1
printf 'FLIPPED %s\n' "$(cat "$DEVHEALTH_AUTH_ORACLE_FLIPPED_FILE")" >> "$FAKE_RECORD"
BIN
        chmod 0755 "$out" ;;
    esac ;;
esac
exit 0
`
	chmodShim := `#!/bin/sh
if [ "$1" = 0600 ]; then stat -c %a "$2" >> "` + record + `.premode"; fi
exec /bin/chmod "$@"
`
	for name, body := range map[string]string{"docker": docker, "go": goShim, "chmod": chmodShim} {
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
	return append([]string{"PATH=" + f.dir + ":" + os.Getenv("PATH"), "HOME=" + f.dir,
		"FAKE_RECORD=" + f.record, "FAKE_TEST_PID_FILE=" + f.record + ".testpid", "FAKE_SIGNING_PEM=" + fakeSigningPEM}, extra...)
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
if [[ " $* " == *"/mails"* ]] && [ -n "${FAKE_SINK_DOWN:-}" ]; then exit 22; fi
if [[ " $* " == *" -X OPTIONS "* ]]; then
  path="/${last#http://*/}"
  served="${FAKE_SERVED}"
  if [ -e "` + f.record + `.owned" ] && [ -n "${FAKE_SERVED_OWNED+x}" ]; then served="${FAKE_SERVED_OWNED}"; fi
  if [[ ",${served}," == *",${path},"* ]]; then printf 405; else printf 404; fi
  exit 0
fi
printf '%s\n' "${FAKE_METRICS:-}"
`
	if err := os.WriteFile(filepath.Join(f.dir, "curl"), []byte(curl), 0o755); err != nil {
		t.Fatal(err)
	}
}

// runAcpLeg runs leg acp with users NOT owned (AUTH_CORPUS_USERS_UNOWNED): the probe tests below are about the first probe.
func runAcpLeg(t *testing.T, extra ...string) (fakeDocker, string, error) {
	t.Helper()
	return runAcpLegWith(t, append([]string{"AUTH_CORPUS_USERS_UNOWNED=1"}, extra...)...)
}

// runAcpOwnedLeg runs leg acp as the script does by default: users owned by auth-service.
func runAcpOwnedLeg(t *testing.T, extra ...string) (fakeDocker, string, error) {
	t.Helper()
	return runAcpLegWith(t, extra...)
}

func runAcpLegWith(t *testing.T, extra ...string) (fakeDocker, string, error) {
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
			if strings.HasSuffix(line, " auth-service") {
				break // step 1e (users owned) follows the proxy flags
			}
			out = append(out, line)
		}
	}
	return out
}

// Leg acp forwards an aggregate only when the running auth-service serves every route of it, and the sessions
// aggregate gates the others: a flag on for a route auth-service does not serve would compare auth-service's 404
// with go-api's answer.
func TestAuthCorpusAcpLegForwardsOnlyWhatAuthServiceServes(t *testing.T) {
	sessions := "/api/v1/auth/refresh,/api/v1/auth/logout,/api/v1/auth/validate,/api/v1/auth/switch-org"
	required := sessions + ",/api/v1/auth/login,/api/v1/auth/social-login"
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
		"nothing served": {wantError: "required aggregate sessions is not served by auth-service", wantProbes: []string{
			"probe sessions: not served (4 routes)", "probe local_login: not served (1 routes)", "probe credentials: not served (6 routes)",
			"probe social: not served (1 routes)", "probe oidc: not served (2 routes)", "probe saml: not served (3 routes)"}},
		"login without sessions": {served: "/api/v1/auth/login", wantError: "required aggregate sessions is not served by auth-service",
			wantProbes: []string{"probe sessions: not served (4 routes)", "probe local_login: served (1 routes)"}},
		"sessions only": {served: sessions, wantError: "required aggregate local_login is not served by auth-service"},
		"sessions and login": {served: sessions + ",/api/v1/auth/login", metrics: forwarded("sessions") + "\n" + forwarded("local_login"),
			wantRecreates: 2, wantForwards: "leg acp forwards: sessions local_login\n"},
		"social served stays on go-api": {served: required, metrics: forwarded("sessions") + "\n" + forwarded("local_login"),
			wantRecreates: 2, wantForwards: "leg acp forwards: sessions local_login\n"},
		"social forwarded although kept on go-api": {served: required, wantRecreates: 2,
			metrics:   forwarded("sessions") + "\n" + forwarded("local_login") + "\n" + forwarded("social"),
			wantError: "go-api forwarded aggregate social, whose flag is off"},
		"half an aggregate": {served: "/api/v1/auth/refresh", wantError: "auth-service serves 1 of the 4 routes of aggregate sessions",
			wantProbes: []string{"probe sessions: partial (1 of 4 routes served)"}},
		"half a non-session aggregate": {served: sessions + ",/api/v1/auth/oidc/p-probe/authorize",
			wantError:  "auth-service serves 1 of the 2 routes of aggregate oidc",
			wantProbes: []string{"probe sessions: served (4 routes)", "probe oidc: partial (1 of 2 routes served)"}},
		"half a non-session aggregate without sessions": {served: "/api/v1/auth/saml/p-probe/metadata",
			wantError: "auth-service serves 1 of the 3 routes of aggregate saml"},
		"served but not forwarded": {served: required, wantRecreates: 2, wantError: "no forwarded request for aggregate sessions"},
		"forwarded while off":      {served: required, wantRecreates: 2, metrics: forwarded("sessions") + "\n" + forwarded("local_login") + "\n" + forwarded("saml"), wantError: "go-api forwarded aggregate saml, whose flag is off"},
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

// Leg acp signs go-api in with its workload token, never a svc_authsess_ credential: the bootstrap credential is written
// and the api principal read before auth-service starts, auth-service starts with the bootstrap exchange and that
// subject, the sessions-owned grants run before go-api first restarts with the sessions flag, and that restart selects
// the workload credential with no AUTH_SESSION_ISSUE_TOKEN in its environment.
func TestAuthCorpusAcpLegSignsInWithTheWorkloadTokenAndGrantsBeforeTheSessionsFlag(t *testing.T) {
	fake, out, err := runAcpLeg(t, "FAKE_SERVED=/api/v1/auth/refresh,/api/v1/auth/logout,/api/v1/auth/validate,/api/v1/auth/switch-org,/api/v1/auth/login,/api/v1/auth/social-login",
		"FAKE_METRICS="+metric("sessions")+"\n"+metric("local_login"))
	if err != nil {
		t.Fatalf("leg acp failed: %v\n%s", err, out)
	}
	calls := fake.calls(t)
	if strings.Contains(calls, "svc_authsess_") || strings.Contains(out, "svc_authsess_") {
		t.Fatalf("a session issue token reached a docker argument or the output:\n%s\n%s", calls, out)
	}
	lines := strings.Split(calls, "\n")
	index := func(match func(string) bool) int {
		for i, line := range lines {
			if match(line) {
				return i
			}
		}
		return -1
	}
	has := func(part string) func(string) bool {
		return func(line string) bool { return strings.Contains(line, part) }
	}
	bootstrap := index(has("run --build --rm auth-workload-bootstrap"))
	subject := index(has("exec -T postgres psql"))
	authUp := index(func(line string) bool {
		return strings.Contains(line, " up ") && strings.HasSuffix(line, " auth-service")
	})
	grants := index(has("run --rm --no-deps -e AUTH_PROXY_SESSIONS=true go-river-migrate"))
	flag := index(has("--force-recreate"))
	if bootstrap < 0 || subject < 0 || authUp < 0 || !(bootstrap < subject && subject < authUp) {
		t.Fatalf("want the bootstrap run (%d), then the subject read (%d), then auth-service up (%d):\n%s", bootstrap, subject, authUp, calls)
	}
	if grants < 0 || flag < 0 || !(grants < flag) {
		t.Fatalf("want the grants (%d) before the first go-api recreate (%d):\n%s", grants, flag, calls)
	}
	raw, err := os.ReadFile(fake.record + ".workload")
	if err != nil {
		t.Fatal(err)
	}
	const subjectEnv = "subject=prn_AAAAAAAAAAAAAAAAAAAAAA"
	var authService, goAPISessions bool
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if strings.Contains(line, "token=set") {
			t.Errorf("a compose up carried AUTH_SESSION_ISSUE_TOKEN: %q", line)
		}
		switch {
		case strings.HasPrefix(line, "auth-service "):
			authService = authService || (strings.Contains(line, "bootstrap=true") && strings.Contains(line, subjectEnv))
		case strings.HasPrefix(line, "go-api "):
			goAPISessions = goAPISessions || strings.Contains(line, "credential=workload")
		}
	}
	if !authService {
		t.Errorf("auth-service never started with AUTH_WORKLOAD_BOOTSTRAP=true and the api subject:\n%s", raw)
	}
	if !goAPISessions {
		t.Errorf("go-api never started with AUTH_SESSION_ISSUE_CREDENTIAL=workload:\n%s", raw)
	}
}

// CHAOS-13126: go-api verifies the venue's EdDSA tokens with its issuer's key set (authverify's JWKS client dials
// <issuer>/.well-known/openid-configuration and jwks.json), so leg acp starts go-api and auth-service with the issuer
// compose's auth-issuer serves (compose.yml's AUTH_TOKEN_ISSUER_URL default), not the workload lib's default, which
// no host serves: there every venue token answered 503.
func TestAuthCorpusAcpLegUsesTheIssuerAuthIssuerServes(t *testing.T) {
	compose, err := os.ReadFile(filepath.Join(authProofRoot(t), "compose.yml"))
	if err != nil {
		t.Fatal(err)
	}
	served := regexp.MustCompile(`AUTH_ACCESS_TOKEN_ISSUER: \$\{AUTH_TOKEN_ISSUER_URL:-(https://[^}]+)\}`).FindSubmatch(compose)
	if served == nil {
		t.Fatal("compose.yml names no AUTH_TOKEN_ISSUER_URL default for go-api's AUTH_ACCESS_TOKEN_ISSUER")
	}
	issuer := string(served[1])
	fake, out, err := runAcpLeg(t, "FAKE_SERVED="+requiredServed, "FAKE_METRICS="+metric("sessions")+"\n"+metric("local_login"))
	if err != nil {
		t.Fatalf("leg acp failed: %v\n%s", err, out)
	}
	raw, err := os.ReadFile(fake.record + ".upissuer")
	if err != nil {
		t.Fatal(err)
	}
	ups := map[string]int{}
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		service, got, _ := strings.Cut(line, " ")
		if service != "go-api" && service != "auth-service" {
			continue
		}
		ups[service]++
		if got != issuer {
			t.Errorf("%s started with AUTH_TOKEN_ISSUER_URL=%q, want the issuer auth-issuer serves %q", service, got, issuer)
		}
	}
	if ups["go-api"] == 0 || ups["auth-service"] == 0 {
		t.Fatalf("want go-api and auth-service started at least once each, got %v:\n%s", ups, raw)
	}
}

func metric(aggregate string) string {
	return `devhealth_api_auth_proxy_requests_total{aggregate="` + aggregate + `",outcome="forwarded"} 3`
}

// upKeys returns, per `compose up`, the successor key, the lockout key and "jwt" when the access-token secret was set
// in the environment the script gave compose.
func upKeys(t *testing.T, fake fakeDocker) [][3]string {
	t.Helper()
	raw, err := os.ReadFile(fake.record + ".keys")
	if err != nil {
		t.Fatal(err)
	}
	var out [][3]string
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		var row [3]string
		copy(row[:], strings.Fields(line))
		out = append(out, row)
	}
	return out
}

const requiredServed = "/api/v1/auth/refresh,/api/v1/auth/logout,/api/v1/auth/validate,/api/v1/auth/switch-org,/api/v1/auth/login,/api/v1/auth/social-login"

// Leg acp gives auth-service freshly made session keys through the compose environment only: never an argument, never
// the output (CI masks them first), and none at all when AUTH_CORPUS_OMIT_AUTH_KEYS is set.
func TestAuthCorpusAcpLegKeysStayOutOfArgumentsAndOutput(t *testing.T) {
	metrics := metric("sessions") + "\n" + metric("local_login")
	fake, out, err := runAcpLeg(t, "FAKE_SERVED="+requiredServed, "FAKE_METRICS="+metrics)
	if err != nil {
		t.Fatalf("leg acp failed: %v\n%s", err, out)
	}
	rows := upKeys(t, fake)
	if len(rows) == 0 {
		t.Fatal("no compose up recorded")
	}
	calls := fake.calls(t)
	for _, row := range rows {
		if len(row[0]) != 43 || len(row[1]) != 43 || row[0] == row[1] || row[2] != "jwt" {
			t.Fatalf("a compose up did not get two distinct 43-character keys and the access-token secret: %v", row)
		}
		for _, key := range row[:2] {
			if strings.Contains(out, key) || strings.Contains(calls, key) {
				t.Fatalf("a session key reached a docker argument or the output:\n%s\n%s", calls, out)
			}
		}
	}
}

func TestAuthCorpusAcpLegMasksTheKeysBeforeAnythingElse(t *testing.T) {
	metrics := metric("sessions") + "\n" + metric("local_login")
	fake, out, err := runAcpLeg(t, "GITHUB_ACTIONS=true", "CHECK_GO_JOB_CAP_MIN=30", "FAKE_SERVED="+requiredServed, "FAKE_METRICS="+metrics)
	if err != nil {
		t.Fatalf("leg acp failed: %v\n%s", err, out)
	}
	row := upKeys(t, fake)[0]
	for _, key := range row[:2] {
		if got := strings.Count(out, key); got != 1 || !strings.Contains(out, "::add-mask::"+key+"\n") {
			t.Errorf("a key appears %d times in the output, want once as the mask command:\n%s", got, out)
		}
		if strings.Index(out, "::add-mask::"+key) > strings.Index(out, "step 1:") {
			t.Error("a key is masked after the first step started")
		}
	}
}

func TestAuthCorpusAcpLegOmitsEveryKeyWhenAsked(t *testing.T) {
	fake, out, err := runAcpLeg(t, "AUTH_CORPUS_OMIT_AUTH_KEYS=1", "FAKE_SERVED=")
	if err == nil || !strings.Contains(out, "required aggregate sessions is not served by auth-service") {
		t.Fatalf("want a failure on the required aggregate sessions, got err=%v:\n%s", err, out)
	}
	for _, row := range upKeys(t, fake) {
		if row != ([3]string{}) {
			t.Fatalf("a compose up got a key although they were omitted: %v", row)
		}
	}
}

// The state file holds the session keys: it is created under umask 077, so it is 0600 from its first write, even
// when the caller's umask would make it world-readable.
func TestAuthCorpusStateFileIsPrivateFromItsFirstWrite(t *testing.T) {
	fake := newFakeDocker(t)
	fake.withCurl(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "bash", "-c", `umask 000; exec bash "$1" acp`, "bash", filepath.Join(authProofRoot(t), "ci", "auth_corpus_external.sh"))
	cmd.Env = fake.env("FAKE_SERVED=")
	_, _ = cmd.CombinedOutput()
	raw, err := os.ReadFile(fake.record + ".premode")
	if err != nil {
		t.Fatalf("the script never made the state file private: %v", err)
	}
	if got := strings.TrimSpace(string(raw)); got != "600" {
		t.Errorf("the state file mode before its chmod is %q, want 600", got)
	}
}

// A failed run dumps the logs of auth-service (a whole corpus run: it is the side whose answers are compared with
// go-api's) and of the provider stub, next to go-api's own.
func TestAuthCorpusFailureDumpIncludesAuthServiceAndTheStub(t *testing.T) {
	fake, out, err := runAcpLeg(t, "FAKE_SERVED=")
	if err == nil {
		t.Fatalf("want a failing run:\n%s", out)
	}
	var authLogs, stubLogs bool
	for _, call := range strings.Split(fake.calls(t), "\n") {
		if !strings.Contains(call, " logs ") {
			continue
		}
		authLogs = authLogs || (strings.HasSuffix(call, " auth-service") && strings.Contains(call, "--tail=3000"))
		stubLogs = stubLogs || (strings.HasSuffix(call, " providerstub") && strings.Contains(call, "--tail=200"))
	}
	if !authLogs || !stubLogs {
		t.Errorf("want `compose logs --tail=3000 auth-service` and `--tail=200 providerstub` at failure (auth %v, stub %v):\n%s", authLogs, stubLogs, fake.calls(t))
	}
}

// Leg acp reads auth-service's own log: the corpus test runs in another process than auth-service, so a body written
// with the wrong writer for its route is visible only as the policy's ERROR line.
func TestAuthCorpusAcpLegFailsOnAuthServiceWriterViolations(t *testing.T) {
	served := "/api/v1/auth/refresh,/api/v1/auth/logout,/api/v1/auth/validate,/api/v1/auth/switch-org,/api/v1/auth/login"
	metrics := `devhealth_api_auth_proxy_requests_total{aggregate="sessions",outcome="forwarded"} 3` + "\n" +
		`devhealth_api_auth_proxy_requests_total{aggregate="local_login",outcome="forwarded"} 3`
	known := `level=INFO msg="listener bound" component=auth-api-http address=127.0.0.1:8095`
	violation := `level=ERROR msg="policy: wrong body writer for the route; FastAPI writes this body the other way" route="POST /api/v1/auth/login"`
	for name, tc := range map[string]struct {
		logs, wantError string
	}{
		"clean log":        {logs: known},
		"empty log":        {wantError: "auth-service logs empty or unrecognised"},
		"unrecognised log": {logs: `level=INFO msg="something else"`, wantError: "auth-service logs empty or unrecognised"},
		"one violation":    {logs: known + "\n" + violation, wantError: "auth-service logged 1 body-writer violations"},
		"two violations":   {logs: known + "\n" + violation + "\n" + violation, wantError: "auth-service logged 2 body-writer violations"},
		"other error line": {logs: known + "\n" + `level=ERROR msg="api response: body could not be serialized"`},
	} {
		t.Run(name, func(t *testing.T) {
			_, out, err := runAcpLeg(t, "FAKE_SERVED="+served, "FAKE_METRICS="+metrics, "FAKE_AUTH_LOGS="+tc.logs)
			if tc.wantError != "" {
				if err == nil || !strings.Contains(out, tc.wantError) {
					t.Fatalf("want a failure naming %q, got err=%v:\n%s", tc.wantError, err, out)
				}
				return
			}
			if err != nil || !strings.Contains(out, "AUTH CORPUS EXTERNAL (acp) PASSED") {
				t.Fatalf("want a pass, got err=%v:\n%s", err, out)
			}
		})
	}
}

const credentialsRoutes = "/api/v1/auth/register,/api/v1/auth/verify,/api/v1/auth/resend-verification,/api/v1/auth/forgot-password,/api/v1/auth/reset-password,/api/v1/auth/change-password"

// Leg acp copies public.users into the auth schema before AUTH_USERS_OWNED turns on (the backfill refuses once it is
// true), then applies the grants, restarts auth-service and go-api with it; AUTH_CORPUS_USERS_UNOWNED skips all of it.
func TestAuthCorpusAcpLegBackfillsUsersBeforeTheOwnedFlag(t *testing.T) {
	metrics := metric("sessions") + "\n" + metric("local_login") + "\n" + metric("credentials") + "\n" + metric("social")
	fake, out, err := runAcpOwnedLeg(t, "FAKE_SERVED="+requiredServed, "FAKE_SERVED_OWNED="+requiredServed+","+credentialsRoutes, "FAKE_METRICS="+metrics)
	if err != nil || !strings.Contains(out, "AUTH CORPUS EXTERNAL (acp) PASSED") {
		t.Fatalf("want a pass, got err=%v:\n%s", err, out)
	}
	calls := fake.calls(t)
	backfill := strings.Index(calls, "users-backfill")
	grants := strings.Index(calls, "auth-grants")
	restart := strings.Index(calls, "--force-recreate --no-build --wait --wait-timeout 300 auth-service")
	if backfill < 0 || grants < backfill || restart < grants {
		t.Fatalf("want users-backfill, then auth-grants, then the auth-service restart; got positions %d %d %d:\n%s", backfill, grants, restart, calls)
	}
	if strings.Contains(calls, "-e AUTH_USERS_OWNED") {
		t.Errorf("the backfill must run with the flag off:\n%s", calls)
	}

	fake, out, err = runAcpLeg(t, "FAKE_SERVED="+requiredServed, "FAKE_METRICS="+metric("sessions")+"\n"+metric("local_login"))
	if err != nil || !strings.Contains(out, "AUTH CORPUS EXTERNAL (acp) PASSED") {
		t.Fatalf("want a pass, got err=%v:\n%s", err, out)
	}
	if calls := fake.calls(t); strings.Contains(calls, "users-backfill") || strings.Contains(calls, "auth-grants") {
		t.Errorf("AUTH_CORPUS_USERS_UNOWNED must skip step 1e:\n%s", calls)
	}
}

func goAPIFlagRecreates(t *testing.T, fake fakeDocker) []string {
	t.Helper()
	raw, err := os.ReadFile(fake.record + ".goapi")
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
}

// go-api refuses the owned flag without the credentials flag: the script turns credentials on first, and the fake
// docker's go-api turns unhealthy for the reverse order, so a script that swapped them fails here.
func TestAuthCorpusAcpOwnedLegTurnsCredentialsOnBeforeTheOwnedFlag(t *testing.T) {
	metrics := metric("sessions") + "\n" + metric("local_login") + "\n" + metric("credentials") + "\n" + metric("social")
	fake, out, err := runAcpOwnedLeg(t, "FAKE_SERVED="+requiredServed, "FAKE_SERVED_OWNED="+requiredServed+","+credentialsRoutes, "FAKE_METRICS="+metrics)
	if err != nil || !strings.Contains(out, "AUTH CORPUS EXTERNAL (acp) PASSED") {
		t.Fatalf("want a pass, got err=%v:\n%s", err, out)
	}
	got := goAPIFlagRecreates(t, fake)
	want := []string{" ", " ", "true ", "true ", "true true"}
	if len(got) != len(want) {
		t.Fatalf("go-api recreates (credentials owned) = %q, want %q", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("go-api recreate %d = %q, want %q (all %q)", i, got[i], want[i], got)
		}
	}
	if !strings.Contains(out, "leg acp forwards (users owned): sessions local_login credentials social\n") {
		t.Errorf("want the re-probed forwarded set in the output:\n%s", out)
	}
}

// The owned leg fails by name when credentials is still not served after step 1e: the gate refuses a mounted-or-not credentials
// aggregate once the users are owned.
func TestAuthCorpusAcpOwnedLegFailsWhenCredentialsStaysNotServed(t *testing.T) {
	_, out, err := runAcpOwnedLeg(t, "FAKE_SERVED="+requiredServed, "FAKE_SERVED_OWNED="+requiredServed,
		"FAKE_METRICS="+metric("sessions")+"\n"+metric("local_login"))
	if err == nil || !strings.Contains(out, "aggregate credentials is not served by auth-service, which owns the users") {
		t.Fatalf("want a failure naming credentials, got err=%v:\n%s", err, out)
	}
}

func forwardedLine(t *testing.T, out, prefix string) []string {
	t.Helper()
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, prefix) {
			return strings.Fields(strings.TrimPrefix(line, prefix))
		}
	}
	t.Fatalf("no %q line:\n%s", prefix, out)
	return nil
}

// The owned and the unowned legs forward the same aggregates but credentials and social.
func TestAuthCorpusAcpOwnedAndUnownedForwardedSetsDifferByCredentialsAndSocialOnly(t *testing.T) {
	served := "FAKE_SERVED=" + requiredServed
	both := "FAKE_SERVED_OWNED=" + requiredServed + "," + credentialsRoutes
	_, owned, err := runAcpOwnedLeg(t, served, both, "FAKE_METRICS="+metric("sessions")+"\n"+metric("local_login")+"\n"+metric("credentials")+"\n"+metric("social"))
	if err != nil {
		t.Fatalf("owned: %v\n%s", err, owned)
	}
	_, unowned, err := runAcpLeg(t, served, both, "FAKE_METRICS="+metric("sessions")+"\n"+metric("local_login"))
	if err != nil {
		t.Fatalf("unowned: %v\n%s", err, unowned)
	}
	o, u := forwardedLine(t, owned, "leg acp forwards (users owned): "), forwardedLine(t, unowned, "leg acp forwards: ")
	if strings.Join(u, ",") != "sessions,local_login" || strings.Join(o, ",") != "sessions,local_login,credentials,social" {
		t.Fatalf("owned %v, unowned %v: they must differ by credentials and social only", o, u)
	}
}

// The corpus seeds its users before the backfill, as a real switch has users first: the owned leg runs the test in the
// background, which seeds, signals, and waits for the script to finish step 1e and 1f before it drives go-api.
func TestAuthCorpusAcpOwnedLegSeedsBeforeTheBackfillAndHandsTheForwardedSetToTheTest(t *testing.T) {
	metrics := metric("sessions") + "\n" + metric("local_login") + "\n" + metric("credentials") + "\n" + metric("social")
	fake, out, err := runAcpOwnedLeg(t, "FAKE_SERVED="+requiredServed, "FAKE_SERVED_OWNED="+requiredServed+","+credentialsRoutes, "FAKE_METRICS="+metrics)
	if err != nil || !strings.Contains(out, "AUTH CORPUS EXTERNAL (acp) PASSED") {
		t.Fatalf("want a pass, got err=%v:\n%s", err, out)
	}
	calls := fake.calls(t)
	seeded, backfill := strings.Index(calls, "SEEDED\n"), strings.Index(calls, "users-backfill")
	if seeded < 0 || backfill < seeded {
		t.Fatalf("want the test's seed before users-backfill, got positions %d %d:\n%s", seeded, backfill, calls)
	}
	if !strings.Contains(calls, "FLIPPED sessions,local_login,credentials,social\n") {
		t.Errorf("want the test to be handed the forwarded set after step 1f:\n%s", calls)
	}
	if strings.Index(calls, "FLIPPED") < backfill {
		t.Errorf("the test must be released only after the backfill:\n%s", calls)
	}
}

// The unowned legs keep their order: the test runs in the foreground, with no seed hand-off.
func TestAuthCorpusUnownedLegHasNoSeedHandOff(t *testing.T) {
	fake, out, err := runAcpLeg(t, "FAKE_SERVED="+requiredServed, "FAKE_METRICS="+metric("sessions")+"\n"+metric("local_login"))
	if err != nil || !strings.Contains(out, "AUTH CORPUS EXTERNAL (acp) PASSED") {
		t.Fatalf("want a pass, got err=%v:\n%s", err, out)
	}
	if calls := fake.calls(t); strings.Contains(calls, "SEEDED") || strings.Contains(calls, "FLIPPED") {
		t.Errorf("an unowned leg must not hand off:\n%s", calls)
	}
}

// The test fails the leg when it ends before it seeds.
func TestAuthCorpusAcpOwnedLegFailsWhenTheTestEndsBeforeSeeding(t *testing.T) {
	_, out, err := runAcpOwnedLeg(t, "FAKE_SERVED="+requiredServed, "FAKE_SERVED_OWNED="+requiredServed+","+credentialsRoutes,
		"FAKE_METRICS="+metric("sessions")+"\n"+metric("local_login")+"\n"+metric("credentials")+"\n"+metric("social"), "FAKE_GO_TEST_EXITS=1")
	if err == nil || !strings.Contains(out, "the corpus test ended before it seeded") {
		t.Fatalf("want a failure naming the early end, got err=%v:\n%s", err, out)
	}
}

type overrideDoc struct {
	Networks map[string]any
	Services map[string]struct {
		Environment []string
		Ports       []string
		DependsOn   map[string]any `yaml:"depends_on"`
		Networks    any
	}
}

func parseOverride(t *testing.T, text string) overrideDoc {
	t.Helper()
	var doc overrideDoc
	if err := yaml.Unmarshal([]byte(text), &doc); err != nil {
		t.Fatal(err)
	}
	return doc
}

func containsKey(m map[string]any, key string) bool { _, ok := m[key]; return ok }

func hasEnv(env []string, want string) bool {
	for _, entry := range env {
		if entry == want {
			return true
		}
	}
	return false
}

// go-api and auth-service both send their mail to the corpus mail sink by its service name, and the sink is a service of
// the leg's own project on the networks both reach: no address, no host gateway. CHAOS-13107: auth-service sends the
// credentials mail on both legs (compose.yml forwards credentials on leg go-api too), so its mail settings and the
// sink's two networks are the main override's.
func TestAuthCorpusPlanesSendMailToTheSinkServiceByName(t *testing.T) {
	mainText, acpText := readRepoFile(t, "ci", "auth_corpus_external.override.yml"), readRepoFile(t, "ci", "auth_corpus_external.acp.override.yml")
	main := parseOverride(t, mainText)
	if !hasEnv(main.Services["go-api"].Environment, "SMTP_HOST=smtpsink") || !hasEnv(main.Services["go-api"].Environment, "SMTP_PORT=2525") {
		t.Errorf("go-api must send mail to smtpsink:2525: %v", main.Services["go-api"].Environment)
	}
	for _, want := range []string{"EMAIL_PROVIDER=smtp", "EMAIL_FROM_ADDRESS", "SMTP_HOST=smtpsink", "SMTP_PORT=2525", "APP_BASE_URL=https://app.example.test/"} {
		if !hasEnv(main.Services["auth-service"].Environment, want) {
			t.Errorf("auth-service environment lacks %q: %v", want, main.Services["auth-service"].Environment)
		}
	}
	if _, ok := main.Services["smtpsink"]; !ok {
		t.Fatal("the main override has no smtpsink service")
	}
	for _, name := range []string{"default", "auth_plane"} {
		if networks, _ := main.Services["smtpsink"].Networks.(map[string]any); networks == nil || !containsKey(networks, name) {
			t.Errorf("smtpsink is not on the %s network: %v", name, main.Services["smtpsink"].Networks)
		}
	}
	for _, service := range []struct {
		doc  overrideDoc
		name string
	}{{main, "go-api"}, {main, "auth-service"}} {
		if _, ok := service.doc.Services[service.name].DependsOn["smtpsink"]; !ok {
			t.Errorf("%s does not wait for smtpsink", service.name)
		}
	}
	for name, text := range map[string]string{"auth_corpus_external.override.yml": mainText, "auth_corpus_external.acp.override.yml": acpText} {
		for _, line := range strings.Split(text, "\n") {
			if strings.Contains(line, "SMTP_HOST=") && !strings.HasSuffix(strings.TrimSpace(line), "SMTP_HOST=smtpsink") {
				t.Errorf("%s: SMTP_HOST must be the sink's service name: %q", name, line)
			}
		}
	}
}

// The sink's read API is published on loopback only, on an ephemeral host port.
func TestAuthCorpusSinkReadAPIIsPublishedOnLoopbackOnly(t *testing.T) {
	ports := parseOverride(t, readRepoFile(t, "ci", "auth_corpus_external.override.yml")).Services["smtpsink"].Ports
	if len(ports) != 1 || ports[0] != "127.0.0.1::9090" {
		t.Fatalf("smtpsink ports = %q, want exactly [127.0.0.1::9090]", ports)
	}
}

// The script reads the sink's published port, fails the leg when the read API does not answer, and hands the test its URL
// on every leg.
func TestAuthCorpusScriptHandsTheTestTheSinkReadAPI(t *testing.T) {
	metrics := metric("sessions") + "\n" + metric("local_login") + "\n" + metric("credentials") + "\n" + metric("social")
	fake, out, err := runAcpOwnedLeg(t, "FAKE_SERVED="+requiredServed, "FAKE_SERVED_OWNED="+requiredServed+","+credentialsRoutes, "FAKE_METRICS="+metrics)
	if err != nil || !strings.Contains(out, "AUTH CORPUS EXTERNAL (acp) PASSED") {
		t.Fatalf("want a pass, got err=%v:\n%s", err, out)
	}
	calls := fake.calls(t)
	if !strings.Contains(calls, " port smtpsink 9090") || !strings.Contains(calls, "SMTPADMIN http://127.0.0.1:12345\n") {
		t.Errorf("want the sink's port read and exported to the test:\n%s", calls)
	}
	fake, out, err = runAcpLeg(t, "FAKE_SERVED="+requiredServed, "FAKE_METRICS="+metric("sessions")+"\n"+metric("local_login"))
	if err != nil {
		t.Fatalf("unowned leg: %v\n%s", err, out)
	}
	if calls := fake.calls(t); !strings.Contains(calls, "SMTPADMIN http://127.0.0.1:12345\n") {
		t.Errorf("the unowned leg must read the sink too:\n%s", calls)
	}
	_, out, err = runAcpOwnedLeg(t, "FAKE_SERVED="+requiredServed, "FAKE_SERVED_OWNED="+requiredServed+","+credentialsRoutes, "FAKE_METRICS="+metrics,
		"FAKE_SINK_DOWN=1", "AUTH_CORPUS_SINK_TRIES=2")
	if err == nil || !strings.Contains(out, "the corpus mail sink's read API does not answer") {
		t.Fatalf("want the leg to fail closed on a silent sink, got err=%v:\n%s", err, out)
	}
}

// The owned leg runs the test as a compiled binary and, when the script fails while the test waits, cleanup kills
// that pid: no test process outlives the script (it would hold the mail sink's port).
func TestAuthCorpusAcpOwnedLegKillsTheTestItself(t *testing.T) {
	fake, out, err := runAcpOwnedLeg(t, "FAKE_SERVED="+requiredServed, "FAKE_SERVED_OWNED="+requiredServed+","+credentialsRoutes,
		"FAKE_METRICS="+metric("sessions")+"\n"+metric("local_login")+"\n"+metric("credentials")+"\n"+metric("social"), "FAKE_FAIL_BACKFILL=1")
	if err == nil || !strings.Contains(out, "users-backfill failed") {
		t.Fatalf("want the backfill failure, got err=%v:\n%s", err, out)
	}
	compile, readErr := os.ReadFile(fake.record + ".compile")
	if readErr != nil || !strings.Contains(string(compile), "-o ") {
		t.Fatalf("want the test compiled to a binary, got %q (%v)", compile, readErr)
	}
	raw, readErr := os.ReadFile(fake.record + ".testpid")
	if readErr != nil {
		t.Fatalf("the test binary never ran: %v\n%s", readErr, out)
	}
	pid, convErr := strconv.Atoi(strings.TrimSpace(string(raw)))
	if convErr != nil {
		t.Fatal(convErr)
	}
	deadline := time.Now().Add(5 * time.Second)
	for syscall.Kill(pid, 0) == nil {
		if time.Now().After(deadline) {
			_ = syscall.Kill(pid, syscall.SIGKILL)
			t.Fatalf("the test process %d outlived the script", pid)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// auth-service reads AUTH_AUTO_CREATE_ORG_ON_REGISTER on every request, as go-api does, so a corpus case that sets it
// must reach both: on leg acp the set recreates auth-service too (with the state, so users stay owned), and no other
// variable, and no other leg, recreates it.
func TestAuthCorpusSetEnvOfTheAutoOrgVariableAlsoRecreatesAuthServiceOnLegAcp(t *testing.T) {
	fake := newFakeDocker(t)
	_, env := setEnvFixture(t, fake)
	if out, code := runSetEnv(t, append(env, "AUTH_CORPUS_LEG=acp"), "AUTH_AUTO_CREATE_ORG_ON_REGISTER", " OFF "); code != 0 {
		t.Fatalf("set on leg acp: exit %d: %s", code, out)
	}
	calls := fake.calls(t)
	if !strings.Contains(calls, "up --detach --no-deps --force-recreate --no-build go-api") {
		t.Errorf("go-api was not recreated; calls:\n%s", calls)
	}
	if !strings.Contains(calls, "--force-recreate --no-build --wait --wait-timeout 120 auth-service") {
		t.Errorf("auth-service was not recreated and awaited; calls:\n%s", calls)
	}
}

// CHAOS-13107: compose.yml forwards sign-up (AUTH_PROXY_CREDENTIALS) to auth-service on leg go-api too, so the auto-org
// variable recreates auth-service on both legs; any other variable recreates go-api only.
func TestAuthCorpusSetEnvRecreatesAuthServiceOnlyForTheAutoOrgVariable(t *testing.T) {
	for name, c := range map[string]struct {
		leg, variable string
		touched       bool
	}{
		"other variable on acp":      {"acp", "X_NAME", false},
		"other variable on go-api":   {"go-api", "X_NAME", false},
		"auto-org on the go-api leg": {"go-api", "AUTH_AUTO_CREATE_ORG_ON_REGISTER", true},
		"auto-org on the acp leg":    {"acp", "AUTH_AUTO_CREATE_ORG_ON_REGISTER", true},
	} {
		fake := newFakeDocker(t)
		_, env := setEnvFixture(t, fake)
		if out, code := runSetEnv(t, append(env, "AUTH_CORPUS_LEG="+c.leg), c.variable, "v"); code != 0 {
			t.Fatalf("%s: exit %d: %s", name, code, out)
		}
		if calls := fake.calls(t); strings.Contains(calls, "auth-service") != c.touched {
			t.Errorf("%s: auth-service touched = %v, want %v; calls:\n%s", name, !c.touched, c.touched, calls)
		}
	}
}

// The two images the corpus builds for itself carry the project in their tag, and the trap that removes the project removes
// exactly those two tags by name after `down`: no prune, no other image (the dev stack's :local images are not the run's).
func TestAuthCorpusCleanupRemovesExactlyTheProjectsOwnBuiltImagesAfterDown(t *testing.T) {
	for _, leg := range []string{"go-api", "acp"} {
		fake := newFakeDocker(t)
		cmd := exec.Command("bash", filepath.Join(authProofRoot(t), "ci", "auth_corpus_external.sh"), leg)
		cmd.Env = fake.env("FAKE_FAIL_UP=1")
		if out, err := cmd.CombinedOutput(); err == nil {
			t.Fatalf("%s: the failing up must fail the script: %s", leg, out)
		}
		calls := fake.calls(t)
		fields := strings.Fields(calls[strings.Index(calls, "compose -p "):])
		project := fields[2]
		if !strings.HasPrefix(project, "authcorpus") {
			t.Fatalf("%s: project %q", leg, project)
		}
		down := strings.Index(calls, " down --volumes")
		var removals []string
		for _, line := range strings.Split(strings.TrimSpace(calls), "\n") {
			if strings.HasPrefix(line, "image ") || strings.HasPrefix(line, "rmi ") || strings.Contains(line, "prune") || strings.HasPrefix(line, "system ") {
				removals = append(removals, line)
			}
		}
		want := []string{"image rm authcorpus-providerstub:" + project, "image rm authcorpus-smtpsink:" + project}
		if strings.Join(removals, "\n") != strings.Join(want, "\n") {
			t.Errorf("%s: image commands:\n%s\nwant:\n%s", leg, strings.Join(removals, "\n"), strings.Join(want, "\n"))
		}
		if down < 0 || strings.Index(calls, want[0]) < down || strings.Index(calls, want[1]) < down {
			t.Errorf("%s: the images are not removed after `down`:\n%s", leg, calls)
		}
	}
}

// credentials is mounted by auth-service as a 503 stub before it owns the users: go-api must keep forwarding nothing for
// it until an owned step sets USERS_OWNED, however many of its routes answer OPTIONS.
func TestAuthCorpusAcpLegDoesNotForwardMountedCredentialsBeforeUsersAreOwned(t *testing.T) {
	credentials := "/api/v1/auth/register,/api/v1/auth/verify,/api/v1/auth/resend-verification,/api/v1/auth/forgot-password," +
		"/api/v1/auth/reset-password,/api/v1/auth/change-password"
	served := "/api/v1/auth/refresh,/api/v1/auth/logout,/api/v1/auth/validate,/api/v1/auth/switch-org,/api/v1/auth/login,/api/v1/auth/social-login," + credentials
	metrics := `devhealth_api_auth_proxy_requests_total{aggregate="sessions",outcome="forwarded"} 3` + "\n" +
		`devhealth_api_auth_proxy_requests_total{aggregate="local_login",outcome="forwarded"} 3`
	for name, extra := range map[string][]string{"no caller env": nil, "caller env USERS_OWNED=1": {"USERS_OWNED=1"}} {
		t.Run(name, func(t *testing.T) {
			fake, out, err := runAcpLeg(t, append([]string{"FAKE_SERVED=" + served, "FAKE_METRICS=" + metrics}, extra...)...)
			if err != nil || !strings.Contains(out, "leg acp forwards: sessions local_login\n") {
				t.Fatalf("want credentials kept on go-api, got err=%v:\n%s", err, out)
			}
			if got := proxyFlagRecreates(fake.calls(t)); len(got) != 2 {
				t.Errorf("go-api recreated %d times for proxy flags, want 2: %q", len(got), got)
			}
		})
	}
}

// gateOwned runs the script's gate_owned function alone with USERS_OWNED set inside the shell, never by the caller env.
func gateOwned(t *testing.T, owned, probed string) (string, error) {
	t.Helper()
	script := filepath.Join(authProofRoot(t), "ci", "auth_corpus_external.sh")
	prog := `die() { printf 'ERROR: %s\n' "$1"; exit 1; }
eval "$(sed -n '/^gate_owned() {$/,/^}$/p' "$1")"
USERS_OWNED="$2"; PROBED="$3"
gate_owned "$4"
printf 'PROBED=%s\n' "${PROBED}"`
	out, err := exec.Command("env", "-u", "USERS_OWNED", "bash", "-c", prog, "x", script, owned, probed, "credentials").CombinedOutput()
	return string(out), err
}

func TestAuthCorpusCredentialsGateNeedsMountAndOwnedUsers(t *testing.T) {
	for name, tc := range map[string]struct {
		owned, probed, want string
		fails               bool
	}{
		"not owned and mounted stays on go-api": {owned: "0", probed: "served", want: "PROBED=not owned"},
		"owned and mounted is forwarded":        {owned: "1", probed: "served", want: "PROBED=served"},
		"owned and unmounted fails the leg":     {owned: "1", probed: "not served", want: "aggregate credentials is not served", fails: true},
	} {
		t.Run(name, func(t *testing.T) {
			out, err := gateOwned(t, tc.owned, tc.probed)
			if (err != nil) != tc.fails || !strings.Contains(out, tc.want) {
				t.Fatalf("want %q (fails=%v), got err=%v:\n%s", tc.want, tc.fails, err, out)
			}
		})
	}
}

// A partly mounted credentials aggregate fails in probe, owned or not, before the gate runs.
func TestAuthCorpusPartlyMountedCredentialsFailTheLeg(t *testing.T) {
	_, out, err := runAcpLeg(t, "FAKE_SERVED=/api/v1/auth/refresh,/api/v1/auth/logout,/api/v1/auth/validate,/api/v1/auth/switch-org,/api/v1/auth/login,/api/v1/auth/register")
	if err == nil || !strings.Contains(out, "auth-service serves 1 of the 6 routes of aggregate credentials") {
		t.Fatalf("want a partial-mount failure, got err=%v:\n%s", err, out)
	}
}

// USERS_OWNED is the script's own variable: it starts at 0, never reads the caller's environment, and only step 1e (the
// owned_flip function) sets it to 1, once, before it probes the aggregates again.
func TestAuthCorpusOnlyStep1eSetsUsersOwned(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(authProofRoot(t), "ci", "auth_corpus_external.sh"))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(string(raw), "\n")
	var zero, ones, flipStart, flipEnd, setter, reprobe int
	for number, line := range lines {
		trim := strings.TrimSpace(line)
		switch {
		case trim == "owned_flip() {":
			flipStart = number + 1
		case flipStart > 0 && flipEnd == 0 && line == "  }":
			flipEnd = number + 1
		}
		switch {
		case strings.HasPrefix(trim, "#"):
		case trim == "USERS_OWNED=0":
			zero++
		case trim == "USERS_OWNED=1":
			ones++
			setter = number + 1
		case strings.Contains(trim, "USERS_OWNED=1"):
			t.Errorf("USERS_OWNED=1 is set inside another statement: %s", trim)
		case strings.Contains(trim, "USERS_OWNED:-"):
			t.Errorf("USERS_OWNED must not be read from the caller env: %s", trim)
		}
		if flipStart > 0 && flipEnd == 0 && trim == "classify" {
			reprobe = number + 1
		}
	}
	if zero != 1 {
		t.Errorf("want exactly one unconditional USERS_OWNED=0, got %d", zero)
	}
	if ones != 1 || flipStart == 0 || flipEnd == 0 || setter <= flipStart || setter >= flipEnd {
		t.Fatalf("want exactly one USERS_OWNED=1 and it inside owned_flip (lines %d-%d), got %d at line %d", flipStart, flipEnd, ones, setter)
	}
	if reprobe == 0 || reprobe < setter {
		t.Errorf("owned_flip must set USERS_OWNED=1 before it probes again (setter line %d, classify line %d)", setter, reprobe)
	}
}

// local_login keeps today's rule: mounted means served, and it is required. Its mount is only real when auth-service
// has the lockout key, so the corpus env must keep setting it.
func TestAuthCorpusLocalLoginStaysRequiredAndKeyed(t *testing.T) {
	_, out, err := runAcpLeg(t, "FAKE_SERVED=/api/v1/auth/refresh,/api/v1/auth/logout,/api/v1/auth/validate,/api/v1/auth/switch-org")
	if err == nil || !strings.Contains(out, "required aggregate local_login is not served by auth-service") {
		t.Fatalf("want local_login required, got err=%v:\n%s", err, out)
	}
	raw, err := os.ReadFile(filepath.Join(authProofRoot(t), "ci", "auth_corpus_external.sh"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `printf 'export AUTH_LOGIN_LOCKOUT_KEY=%q\n' "${LOGIN_LOCKOUT_KEY}"`) {
		t.Error("the corpus env no longer sets AUTH_LOGIN_LOCKOUT_KEY")
	}
}

// gateSessions runs the script's gate_owned function alone for the sessions aggregate with SESSIONS_KEYED set inside the
// shell, never by the caller env.
func gateSessions(t *testing.T, keyed, probed string, callerEnv ...string) (string, error) {
	t.Helper()
	script := filepath.Join(authProofRoot(t), "ci", "auth_corpus_external.sh")
	prog := `die() { printf 'ERROR: %s\n' "$1"; exit 1; }
eval "$(sed -n '/^gate_owned() {$/,/^}$/p' "$1")"
USERS_OWNED=0; SESSIONS_KEYED="$2"; PROBED="$3"
gate_owned sessions
printf 'PROBED=%s\n' "${PROBED}"`
	cmd := exec.Command("env", append([]string{"-u", "SESSIONS_KEYED"}, "bash", "-c", prog, "x", script, keyed, probed)...)
	cmd.Env = append(os.Environ(), callerEnv...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// Without the session keys auth-service mounts refresh and logout on a 503 stub, which answers OPTIONS 405 like the
// real routes: mounted is not served until the keys are set.
func TestAuthCorpusSessionsGateNeedsMountAndKeys(t *testing.T) {
	for name, tc := range map[string]struct{ keyed, probed, want string }{
		"keyed and mounted is served":          {keyed: "1", probed: "served", want: "PROBED=served"},
		"unkeyed and mounted is a stub":        {keyed: "0", probed: "served", want: "PROBED=not keyed"},
		"unkeyed and unmounted stays unserved": {keyed: "0", probed: "not served", want: "PROBED=not served"},
	} {
		t.Run(name, func(t *testing.T) {
			out, err := gateSessions(t, tc.keyed, tc.probed)
			if err != nil || !strings.Contains(out, tc.want) {
				t.Fatalf("want %q, got err=%v:\n%s", tc.want, err, out)
			}
		})
	}
}

// The no-keys leg meets the refresh and logout stubs: it must still fail by name on sessions, not on local_login.
func TestAuthCorpusNoKeysLegFailsOnSessionsWhenTheStubsAreMounted(t *testing.T) {
	_, out, err := runAcpLeg(t, "AUTH_CORPUS_OMIT_AUTH_KEYS=1", "FAKE_SERVED=/api/v1/auth/refresh,/api/v1/auth/logout,/api/v1/auth/validate,/api/v1/auth/switch-org")
	if err == nil || !strings.Contains(out, "required aggregate sessions is not served by auth-service") {
		t.Fatalf("want the leg to fail on sessions, got err=%v:\n%s", err, out)
	}
	if !strings.Contains(out, "probe sessions: mounted, not keyed") {
		t.Errorf("want the stub named as not keyed:\n%s", out)
	}
}

// SESSIONS_KEYED follows the script's own key choice and is never read from the caller env.
func TestAuthCorpusSessionsKeyedIsScriptInternal(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(authProofRoot(t), "ci", "auth_corpus_external.sh"))
	if err != nil {
		t.Fatal(err)
	}
	var one, zero int
	for _, line := range strings.Split(string(raw), "\n") {
		trim := strings.TrimSpace(line)
		switch {
		case trim == "SESSIONS_KEYED=1":
			one++
		case trim == `[ -z "${AUTH_CORPUS_OMIT_AUTH_KEYS:-}" ] || SESSIONS_KEYED=0`:
			zero++
		case strings.Contains(trim, "SESSIONS_KEYED:-"):
			t.Errorf("SESSIONS_KEYED must not be read from the caller env: %s", trim)
		case strings.Contains(trim, "SESSIONS_KEYED=") && !strings.HasPrefix(trim, "#") && !strings.HasPrefix(trim, "local "):
			t.Errorf("unexpected SESSIONS_KEYED setter: %s", trim)
		}
	}
	if one != 1 || zero != 1 {
		t.Errorf("want exactly one unconditional SESSIONS_KEYED=1 and one key-choice SESSIONS_KEYED=0, got %d and %d", one, zero)
	}
}

func goAPISocialRecreates(t *testing.T, fake fakeDocker) []string {
	t.Helper()
	raw, err := os.ReadFile(fake.record + ".goapisocial")
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
}

// Social joins the owned gate (CHAOS-11350): auth-service refuses a new social sign-in until it owns users, so go-api keeps
// the aggregate until step 1e, and step 1f forwards it with the credentials, BEFORE the owned flag reaches go-api.
func TestAuthCorpusAcpOwnedLegForwardsSocialAfterTheFlipAndBeforeTheOwnedFlag(t *testing.T) {
	fake, out, err := runAcpOwnedLeg(t, "FAKE_SERVED="+requiredServed, "FAKE_SERVED_OWNED="+requiredServed+","+credentialsRoutes,
		"FAKE_METRICS="+metric("sessions")+"\n"+metric("local_login")+"\n"+metric("credentials")+"\n"+metric("social"))
	if err != nil || !strings.Contains(out, "AUTH CORPUS EXTERNAL (acp) PASSED") {
		t.Fatalf("want a pass, got err=%v:\n%s", err, out)
	}
	if !strings.Contains(out, "probe social: mounted, not owned yet: stays on go-api\n") || !strings.Contains(out, "leg acp forwards: sessions local_login\n") {
		t.Errorf("before step 1e social must stay on go-api, like credentials:\n%s", out)
	}
	owned, social := goAPIFlagRecreates(t, fake), goAPISocialRecreates(t, fake)
	if len(owned) != len(social) || len(owned) != 5 {
		t.Fatalf("go-api recreates: %q (flags) and %q (social), want 5 each", owned, social)
	}
	first := -1
	for i, value := range social {
		if value == "true" {
			first = i
			break
		}
	}
	if first < 0 {
		t.Fatalf("AUTH_PROXY_SOCIAL never reached go-api: %q", social)
	}
	if strings.HasSuffix(owned[first], " true") {
		t.Errorf("the social flag must reach go-api before AUTH_USERS_OWNED (recreate %d: %q, social %q)", first, owned[first], social[first])
	}
	if first < 2 {
		t.Errorf("social turned on at recreate %d, before step 1f (recreates 0 and 1 are sessions and local_login)", first)
	}
	if !strings.HasSuffix(owned[len(owned)-1], " true") || social[len(social)-1] != "true" {
		t.Errorf("the last recreate must carry both the owned flag and the social flag: %q %q", owned[len(owned)-1], social[len(social)-1])
	}
}

// Without the posture social stays on go-api, as before; and a leg where social is served but the metrics show it
// forwarded fails by name.
func TestAuthCorpusUnownedLegKeepsSocialOnGoAPIThroughTheGate(t *testing.T) {
	fake, out, err := runAcpLeg(t, "FAKE_SERVED="+requiredServed, "FAKE_METRICS="+metric("sessions")+"\n"+metric("local_login"))
	if err != nil || !strings.Contains(out, "probe social: mounted, not owned yet: stays on go-api\n") || !strings.Contains(out, "leg acp forwards: sessions local_login\n") {
		t.Fatalf("want social kept on go-api, got err=%v:\n%s", err, out)
	}
	for _, value := range goAPISocialRecreates(t, fake) {
		if value != "" {
			t.Errorf("AUTH_PROXY_SOCIAL reached go-api on the not-owned leg: %q", value)
		}
	}
}

// The non-vacuity run (CHAOS-11350): AUTH_CORPUS_FORCE_SOCIAL forwards social although users are not owned, so the new-user
// row must fail (auth-service refuses it 403 signup_unavailable). Only on the not-owned leg; elsewhere the script refuses it.
func TestAuthCorpusForceSocialForwardsSocialOnlyOnTheNotOwnedLeg(t *testing.T) {
	fake, out, err := runAcpLeg(t, "AUTH_CORPUS_FORCE_SOCIAL=1", "FAKE_SERVED="+requiredServed,
		"FAKE_METRICS="+metric("sessions")+"\n"+metric("local_login")+"\n"+metric("social"))
	if err != nil || !strings.Contains(out, "leg acp forwards: sessions local_login social\n") {
		t.Fatalf("want social forwarded without the posture, got err=%v:\n%s", err, out)
	}
	if got := goAPISocialRecreates(t, fake); got[len(got)-1] != "true" {
		t.Errorf("AUTH_PROXY_SOCIAL never reached go-api: %q", got)
	}
	_, out, err = runAcpOwnedLeg(t, "AUTH_CORPUS_FORCE_SOCIAL=1", "FAKE_SERVED="+requiredServed)
	if err == nil || !strings.Contains(out, "AUTH_CORPUS_FORCE_SOCIAL needs AUTH_CORPUS_USERS_UNOWNED=1") {
		t.Errorf("FORCE_SOCIAL on the owned leg must be refused by name, got err=%v:\n%s", err, out)
	}
	_, out, err = runLegArg(t, "go-api", "AUTH_CORPUS_FORCE_SOCIAL=1")
	if err == nil || !strings.Contains(out, "AUTH_CORPUS_FORCE_SOCIAL needs AUTH_CORPUS_USERS_UNOWNED=1") {
		t.Errorf("FORCE_SOCIAL on leg go-api must be refused by name, got err=%v:\n%s", err, out)
	}
}

func runLegArg(t *testing.T, leg string, extra ...string) (fakeDocker, string, error) {
	t.Helper()
	fake := newFakeDocker(t)
	fake.withCurl(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "bash", filepath.Join(authProofRoot(t), "ci", "auth_corpus_external.sh"), leg)
	cmd.Env = fake.env(extra...)
	out, err := cmd.CombinedOutput()
	return fake, string(out), err
}

func gateOwnedFor(t *testing.T, aggregate, owned, probed, force string) (string, error) {
	t.Helper()
	script := filepath.Join(authProofRoot(t), "ci", "auth_corpus_external.sh")
	prog := `die() { printf 'ERROR: %s\n' "$1"; exit 1; }
eval "$(sed -n '/^gate_owned() {$/,/^}$/p' "$1")"
USERS_OWNED="$2"; PROBED="$3"; FORCE_SOCIAL="$5"
gate_owned "$4"
printf 'PROBED=%s\n' "${PROBED}"`
	out, err := exec.Command("env", "-u", "USERS_OWNED", "bash", "-c", prog, "x", script, owned, probed, aggregate, force).CombinedOutput()
	return string(out), err
}

// Social is gated like credentials: mounted and not owned stays on go-api, owned must be served, and only the forced
// non-vacuity run forwards it without the posture. No other aggregate is affected by the force.
func TestAuthCorpusSocialGateNeedsMountAndOwnedUsersUnlessForced(t *testing.T) {
	for name, tc := range map[string]struct {
		aggregate, owned, probed, force, want string
		fails                                 bool
	}{
		"not owned and mounted stays on go-api":   {"social", "0", "served", "", "PROBED=not owned", false},
		"owned and mounted is forwarded":          {"social", "1", "served", "", "PROBED=served", false},
		"owned and unmounted fails the leg":       {"social", "1", "not served", "", "aggregate social is not served", true},
		"forced and mounted is forwarded unowned": {"social", "0", "served", "1", "PROBED=served", false},
		"the force does not reach credentials":    {"credentials", "0", "served", "1", "PROBED=not owned", false},
		"not owned and unmounted stays unmounted": {"social", "0", "not served", "", "PROBED=not served", false},
		"forced and unmounted stays not served":   {"social", "0", "not served", "1", "PROBED=not served", false},
		"another aggregate is never owned-gated":  {"oidc", "0", "served", "", "PROBED=served", false},
	} {
		t.Run(name, func(t *testing.T) {
			out, err := gateOwnedFor(t, tc.aggregate, tc.owned, tc.probed, tc.force)
			if (err != nil) != tc.fails || !strings.Contains(out, tc.want) {
				t.Fatalf("want %q (fails=%v), got err=%v:\n%s", tc.want, tc.fails, err, out)
			}
		})
	}
}

// AUTH_CORPUS_FORCE_SOCIAL is a corpus-harness name: it lives in ci/ scripts and test files only, never in a production Go
// file or a chart (a leaked switch would forward social without the posture where it is read).
func TestAuthCorpusForceSocialNameStaysOutOfProductionCodeAndCharts(t *testing.T) {
	root := authProofRoot(t)
	var leaks []string
	for _, base := range []string{"internal", "cmd", "deploy", "compose.yml"} {
		_ = filepath.WalkDir(filepath.Join(root, base), func(path string, entry os.DirEntry, err error) error {
			if err != nil || entry.IsDir() {
				return nil
			}
			if strings.HasSuffix(path, "_test.go") {
				return nil
			}
			ext := filepath.Ext(path)
			if ext != ".go" && ext != ".yaml" && ext != ".yml" && ext != ".tpl" && ext != ".json" && ext != ".sh" {
				return nil
			}
			raw, readErr := os.ReadFile(path)
			if readErr == nil && strings.Contains(string(raw), "AUTH_CORPUS_FORCE_SOCIAL") {
				leaks = append(leaks, strings.TrimPrefix(path, root+"/"))
			}
			return nil
		})
	}
	if len(leaks) > 0 {
		t.Errorf("AUTH_CORPUS_FORCE_SOCIAL appears outside ci/ scripts and test files: %v", leaks)
	}
}

// A refused AUTH_CORPUS_FORCE_SOCIAL invocation is refused before the script makes any temporary directory: the cleanup
// trap is installed later, so a directory made first would be left behind by every refusal.
func TestAuthCorpusRefusedForceSocialLeavesNoTemporaryDirectory(t *testing.T) {
	for name, tc := range map[string]struct {
		leg   string
		extra []string
	}{
		"owned acp leg": {"acp", []string{"AUTH_CORPUS_FORCE_SOCIAL=1"}},
		"go-api leg":    {"go-api", []string{"AUTH_CORPUS_FORCE_SOCIAL=1"}},
	} {
		t.Run(name, func(t *testing.T) {
			tmp := t.TempDir()
			_, out, err := runLegArg(t, tc.leg, append([]string{"TMPDIR=" + tmp}, tc.extra...)...)
			if err == nil || !strings.Contains(out, "AUTH_CORPUS_FORCE_SOCIAL needs AUTH_CORPUS_USERS_UNOWNED=1") {
				t.Fatalf("want the refusal by name, got err=%v:\n%s", err, out)
			}
			entries, readErr := os.ReadDir(tmp)
			if readErr != nil || len(entries) != 0 {
				t.Errorf("the refused run left %d entries in its TMPDIR (%v)", len(entries), readErr)
			}
		})
	}
}

// CHAOS-13107: compose.yml routes go-api's auth aggregates through auth-service, as production does (sessions, local
// login, credentials and social; OIDC and SAML off). On leg go-api the script reads each of go-api's AUTH_PROXY_* keys,
// one by one, as compose renders them for the leg and hands the corpus exactly the aggregates that are "true", so it
// classifies auth-service's answers (authrows) instead of comparing them as go-api's own. go-api's own start-up rules
// are the script's too: an aggregate without sessions, and OIDC or SAML (no corpus rows), fail the leg by name.
func TestAuthCorpusGoAPILegForwardsSessionsWhenComposeRoutesThem(t *testing.T) {
	env := func(pairs string) string {
		return `{"services":{"go-api":{"environment":{` + pairs + `}},"auth-service":{"environment":{"AUTH_TOKEN_ISSUER_URL":"https://auth.corpus.example","AUTH_SIGNING_KEY_ID":"auth-signing-corpus"}}}}`
	}
	const prodSet = `"AUTH_PROXY_SESSIONS":"true","AUTH_PROXY_LOCAL_LOGIN":"true","AUTH_PROXY_CREDENTIALS":"true","AUTH_PROXY_SOCIAL":"true"`
	for _, tc := range []struct {
		name, config, want, refusal string
	}{
		{"the prod set", env(prodSet), "sessions,local_login,credentials,social", ""},
		{"the prod set, OIDC and SAML off", env(prodSet + `,"AUTH_PROXY_OIDC":"false","AUTH_PROXY_SAML":null`), "sessions,local_login,credentials,social", ""},
		{"sessions only", env(`"AUTH_PROXY_SESSIONS":"true"`), "sessions", ""},
		{"sessions and local login", env(`"AUTH_PROXY_SESSIONS":"true","AUTH_PROXY_LOCAL_LOGIN":"true","AUTH_PROXY_CREDENTIALS":"false"`), "sessions,local_login", ""},
		{"social without credentials", env(`"AUTH_PROXY_SESSIONS":"true","AUTH_PROXY_SOCIAL":"true"`), "sessions,social", ""},
		// CHAOS-13154: go-api trusts auth-service's tokens only, so a leg without sessions forwarded (go-api's local login
		// mints the retired HS256 class) is refused before the corpus runs.
		{"sessions off", `{"services":{"go-api":{"environment":{"AUTH_PROXY_SESSIONS":"false"}}}}`, "", noSessionsRefusal},
		{"sessions unset", `{"services":{"go-api":{"environment":{"AUTH_PROXY_SESSIONS":null}}}}`, "", noSessionsRefusal},
		{"no environment", `{"services":{"go-api":{}}}`, "", noSessionsRefusal},
		{"local login without sessions", env(`"AUTH_PROXY_LOCAL_LOGIN":"true"`), "", "AUTH_PROXY_LOCAL_LOGIN=true without AUTH_PROXY_SESSIONS"},
		{"OIDC on", env(prodSet + `,"AUTH_PROXY_OIDC":"true"`), "", "AUTH_PROXY_OIDC=true"},
		{"SAML on", env(prodSet + `,"AUTH_PROXY_SAML":"true"`), "", "AUTH_PROXY_SAML=true"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := newFakeDocker(t)
			fake.withCurl(t)
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, "bash", filepath.Join(authProofRoot(t), "ci", "auth_corpus_external.sh"), "go-api")
			cmd.Env = fake.env("FAKE_COMPOSE_CONFIG=" + tc.config)
			out, err := cmd.CombinedOutput()
			if tc.refusal != "" {
				if err == nil || !strings.Contains(string(out), tc.refusal) {
					t.Fatalf("want the refusal naming %q, got err=%v:\n%s", tc.refusal, err, out)
				}
				if _, statErr := os.Stat(fake.record + ".forwarded"); statErr == nil {
					t.Error("the corpus test ran")
				}
				return
			}
			if err != nil {
				t.Fatalf("leg go-api failed: %v\n%s", err, out)
			}
			if !strings.Contains(fake.calls(t), " config --format json") {
				t.Errorf("leg go-api never read go-api's environment from the rendered compose config; calls:\n%s", fake.calls(t))
			}
			raw, err := os.ReadFile(fake.record + ".forwarded")
			if err != nil {
				t.Fatalf("the corpus test never ran: %v\n%s", err, out)
			}
			if got := strings.TrimSpace(string(raw)); got != tc.want {
				t.Errorf("leg go-api hands the corpus DEVHEALTH_AUTH_ORACLE_FORWARDED=%q, want %q", got, tc.want)
			}
			if owned, _ := os.ReadFile(fake.record + ".usersowned"); strings.TrimSpace(string(owned)) != "false" {
				t.Errorf("leg go-api with users not owned hands the corpus DEVHEALTH_AUTH_ORACLE_USERS_OWNED=%q, want false", owned)
			}
		})
	}
}

// CHAOS-13107: compose.yml has auth-service own users (AUTH_USERS_OWNED, as production's phase 4). The corpus seeds its
// users into public.users, so on leg go-api with users owned the script runs the owned hand-off of leg acp: the test
// seeds and waits, the script copies the seeded users (users-backfill, refused while the flag is true, so run with it
// false for that one command), recreates auth-service, and hands the test the forwarded set; the test is told users
// are owned. go-api's rule is the script's too: users owned needs sessions, local login and credentials forwarded.
func TestAuthCorpusGoAPILegRunsTheOwnedHandOffWhenComposeOwnsUsers(t *testing.T) {
	const prodSet = `"AUTH_PROXY_SESSIONS":"true","AUTH_PROXY_LOCAL_LOGIN":"true","AUTH_PROXY_CREDENTIALS":"true","AUTH_PROXY_SOCIAL":"true"`
	env := func(pairs string) string {
		return `{"services":{"go-api":{"environment":{` + pairs + `}},"auth-service":{"environment":{"AUTH_TOKEN_ISSUER_URL":"https://auth.corpus.example","AUTH_SIGNING_KEY_ID":"auth-signing-corpus"}}}}`
	}
	t.Run("owned", func(t *testing.T) {
		fake := newFakeDocker(t)
		fake.withCurl(t)
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, "bash", filepath.Join(authProofRoot(t), "ci", "auth_corpus_external.sh"), "go-api")
		cmd.Env = fake.env("FAKE_COMPOSE_CONFIG=" + env(prodSet+`,"AUTH_USERS_OWNED":"true"`))
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("leg go-api failed: %v\n%s", err, out)
		}
		calls := fake.calls(t)
		for _, want := range []string{
			"TESTENV forwarded=sessions,local_login,credentials,social owned=true",
			"SEEDED",
			"FLIPPED sessions,local_login,credentials,social",
		} {
			if !strings.Contains(calls, want) {
				t.Errorf("no %q in the calls:\n%s", want, calls)
			}
		}
		seeded, backfill := strings.Index(calls, "SEEDED"), strings.Index(calls, "run --rm --no-deps auth-users-backfill")
		flipped := strings.Index(calls, "FLIPPED")
		if backfill < 0 || seeded > backfill || backfill > flipped {
			t.Errorf("users-backfill must run after the seed and before the hand-off; calls:\n%s", calls)
		}
		// CHAOS-13107: the copy is compose.yml's own one-shot (AUTH_USERS_OWNED=false for that run only), not an
		// ad-hoc run of migrate with the flag on the command line.
		if !strings.Contains(calls, "run --rm --no-deps auth-users-backfill") || strings.Contains(calls, "migrate auth users-backfill") {
			t.Errorf("the seeded users were not copied by the auth-users-backfill one-shot; calls:\n%s", calls)
		}
		if _, statErr := os.Stat(fake.record + ".owned"); statErr != nil {
			t.Error("auth-service was not recreated after the backfill")
		}
	})
	t.Run("owned without credentials", func(t *testing.T) {
		fake := newFakeDocker(t)
		fake.withCurl(t)
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, "bash", filepath.Join(authProofRoot(t), "ci", "auth_corpus_external.sh"), "go-api")
		cmd.Env = fake.env("FAKE_COMPOSE_CONFIG=" + env(`"AUTH_PROXY_SESSIONS":"true","AUTH_PROXY_LOCAL_LOGIN":"true","AUTH_USERS_OWNED":"true"`))
		out, err := cmd.CombinedOutput()
		if err == nil || !strings.Contains(string(out), "AUTH_USERS_OWNED=true without AUTH_PROXY_CREDENTIALS") {
			t.Fatalf("want the refusal by name, got err=%v:\n%s", err, out)
		}
	})
}

// CHAOS-13107: with sessions forwarded the corpus checks each answer's session_access_token against auth-service's issuer
// (authrows.SessionAccessTokenErr): the script hands it the issuer as compose renders it, and refuses a leg that names none.
// CHAOS-13154: a leg without sessions forwarded is refused before the corpus runs (the venue mints under that issuer).
func TestAuthCorpusHandsTheCorpusAuthServicesIssuerWhenSessionsAreForwarded(t *testing.T) {
	const issuer = "https://auth.corpus.example"
	for _, tc := range []struct {
		name, config, want string
		refusal            string
	}{
		{"sessions on", `{"services":{"go-api":{"environment":{"AUTH_PROXY_SESSIONS":"true"}},"auth-service":{"environment":{"AUTH_TOKEN_ISSUER_URL":"` + issuer + `","AUTH_SIGNING_KEY_ID":"auth-signing-corpus"}}}}`, issuer, ""},
		// CHAOS-13154: without sessions forwarded the leg is refused before the corpus runs (noSessionsRefusal).
		{"sessions off", `{"services":{"go-api":{"environment":{"AUTH_PROXY_SESSIONS":"false"}},"auth-service":{"environment":{"AUTH_TOKEN_ISSUER_URL":"` + issuer + `"}}}}`, "", noSessionsRefusal},
		{"sessions on, no issuer", `{"services":{"go-api":{"environment":{"AUTH_PROXY_SESSIONS":"true"}},"auth-service":{"environment":{}}}}`, "", "auth-service names no AUTH_TOKEN_ISSUER_URL"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := newFakeDocker(t)
			fake.withCurl(t)
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, "bash", filepath.Join(authProofRoot(t), "ci", "auth_corpus_external.sh"), "go-api")
			cmd.Env = fake.env("FAKE_COMPOSE_CONFIG=" + tc.config)
			out, err := cmd.CombinedOutput()
			if tc.refusal != "" {
				if err == nil || !strings.Contains(string(out), tc.refusal) {
					t.Fatalf("want the refusal by name, got err=%v:\n%s", err, out)
				}
				if _, statErr := os.Stat(fake.record + ".issuer"); statErr == nil {
					t.Error("the corpus test ran without the issuer")
				}
				return
			}
			if err != nil {
				t.Fatalf("leg go-api failed: %v\n%s", err, out)
			}
			raw, err := os.ReadFile(fake.record + ".issuer")
			if err != nil {
				t.Fatalf("the corpus test never ran: %v\n%s", err, out)
			}
			if got := strings.TrimSpace(string(raw)); got != tc.want {
				t.Errorf("leg go-api hands the corpus DEVHEALTH_AUTH_ORACLE_SESSION_ISSUER=%q, want %q", got, tc.want)
			}
		})
	}
}

// CHAOS-13154: the venue mints its seed tokens under auth-service's signing key. The script copies the key out of
// auth-service into a mode 0600 file of a mode 0700 directory in the run's private directory, hands the test only its
// path and auth-service's key id, never shows the key, and removes the file on exit.
func TestAuthCorpusVenueSigningKeyIsAPrivateFileOfTheRun(t *testing.T) {
	acp := []string{"AUTH_CORPUS_USERS_UNOWNED=1",
		"FAKE_SERVED=/api/v1/auth/refresh,/api/v1/auth/logout,/api/v1/auth/validate,/api/v1/auth/switch-org,/api/v1/auth/login,/api/v1/auth/social-login",
		"FAKE_METRICS=" + metric("sessions") + "\n" + metric("local_login")}
	for leg, extra := range map[string][]string{"go-api": nil, "acp": acp} {
		t.Run(leg, func(t *testing.T) {
			fake := newFakeDocker(t)
			fake.withCurl(t)
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, "bash", filepath.Join(authProofRoot(t), "ci", "auth_corpus_external.sh"), leg)
			cmd.Env = fake.env(extra...)
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("leg %s failed: %v\n%s", leg, err, out)
			}
			raw, err := os.ReadFile(fake.record + ".signing")
			if err != nil {
				t.Fatalf("the corpus test never ran: %v\n%s", err, out)
			}
			fields := strings.Fields(strings.TrimSpace(string(raw)))
			if len(fields) != 4 || fields[1] != "auth-signing-corpus" || fields[2] != "600" || fields[3] != "700" ||
				filepath.Base(fields[0]) != "signing.pem" {
				t.Fatalf("the test was handed %q, want <dir>/signing.pem, key id auth-signing-corpus, file 600, dir 700", raw)
			}
			if content, _ := os.ReadFile(fake.record + ".signingcontent"); strings.TrimSpace(string(content)) != fakeSigningPEM {
				t.Errorf("the key file the test read holds %q, want auth-service's key", content)
			}
			if _, statErr := os.Stat(fields[0]); !os.IsNotExist(statErr) {
				t.Errorf("the key file %s is left after the run: %v", fields[0], statErr)
			}
			if _, statErr := os.Stat(filepath.Dir(fields[0])); !os.IsNotExist(statErr) {
				t.Errorf("the key directory is left after the run: %v", statErr)
			}
			calls := fake.calls(t)
			if !strings.Contains(calls, " cp auth-service:/run/auth/signing/envelope-private-key.pem "+fields[0]) {
				t.Errorf("the key was not copied from auth-service's key file to the handed path; calls:\n%s", calls)
			}
			for name, text := range map[string]string{"output": string(out), "docker calls": calls} {
				if strings.Contains(text, fakeSigningPEM) {
					t.Errorf("the key appears in the %s", name)
				}
			}
		})
	}
}
