package workflows_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/providerstub"
)

// The web live e2e logs in through auth-service the way prod does (go-api session issue with the workload
// credential). ci/live_e2e_auth_stack.sh builds the environment of each process; ci/live_e2e_auth_flags.tsv is the
// one place that names the flags, with the prod line each one copies. Nothing here starts a process.

// The web workload principal the stack lets call POST /v1/authorize (22 base64url characters after prn_).
const webSubject = "prn_AAAAAAAAAAAAAAAAAAAAAA"

type flagRow struct{ scope, name, value, source, delta string }

func liveFlagRows(t *testing.T) []flagRow {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), "ci", "live_e2e_auth_flags.tsv"))
	if err != nil {
		t.Fatalf("FAIL: the live e2e auth flag table is missing: %v", err)
	}
	var rows []flagRow
	for _, line := range strings.Split(string(raw), "\n") {
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "scope\t") {
			continue
		}
		c := strings.Split(line, "\t")
		if len(c) != 5 {
			t.Fatalf("FAIL: flag row has %d columns, want 5: %q", len(c), line)
		}
		rows = append(rows, flagRow{c[0], c[1], c[2], c[3], c[4]})
	}
	return rows
}

func liveStackEnv(t *testing.T, side string) map[string]string {
	t.Helper()
	cmd := exec.Command("bash", filepath.Join(repoRoot(t), "ci", "live_e2e_auth_stack.sh"), "with-env", side, "/state", "--", "env")
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"JWT_SECRET_KEY=0123456789abcdef0123456789abcdef0123",                    // gitleaks:allow -- placeholder value fed to the environment printer, not a credential
		"AUTH_SESSION_SUCCESSOR_KEY=a2tra2tra2tra2tra2tra2tra2tra2tra2tra2tra2s", // gitleaks:allow -- placeholder value fed to the environment printer, not a credential
		"AUTH_LOGIN_LOCKOUT_KEY=a2tra2tra2tra2tra2tra2tra2tra2tra2tra2tra2s",     // gitleaks:allow -- placeholder value fed to the environment printer, not a credential
		"AUTH_RUNTIME_DATABASE_PASSWORD=runtime-password-for-test",               // gitleaks:allow -- placeholder value fed to the environment printer, not a credential
		"AUTH_SESSION_ISSUE_WORKLOAD_SUBJECT=prn_hh9WycdvSHSM26P15yRmgA",
		"AUTH_AUTHORIZE_CALLER_SUBJECTS=" + webSubject,
	}
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		t.Fatalf("FAIL: with-env %s: %v\n%s", side, err, errb.String())
	}
	env := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(out.String()), "\n") {
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			t.Fatalf("FAIL: with-env %s printed a line that is not NAME=VALUE: %q", side, line)
		}
		env[k] = v
	}
	return env
}

func TestLiveE2EAuthFlagTableDeltasAreTheNamedOnes(t *testing.T) {
	// The only flags that differ from prod, each with its reason (the delta column must say it). Any other delta fails.
	want := map[string]string{
		"auth-service AUTH_WORKLOAD_BOOTSTRAP": "bootstrap subject, no Kubernetes",
	}
	got := map[string]string{}
	for _, r := range liveFlagRows(t) {
		if r.source == "" || r.source == "-" {
			t.Errorf("FAIL: %s %s cites no prod source", r.scope, r.name)
		}
		if r.delta != "-" {
			got[r.scope+" "+r.name] = r.delta
		}
	}
	for name, reason := range want {
		if got[name] != reason {
			t.Errorf("FAIL: delta %s has reason %q, want %q", name, got[name], reason)
		}
	}
	for name := range got {
		if _, ok := want[name]; !ok {
			t.Errorf("FAIL: %s differs from prod and is not a named delta", name)
		}
	}
}

func TestLiveE2EGoAPIEnvIsProdsFlagSet(t *testing.T) {
	env := liveStackEnv(t, "go-api")
	lookup := func(k string) (string, bool) { v, ok := env[k]; return v, ok }
	cfg, err := apiservice.ParseAuthProxyConfig(lookup)
	if err != nil {
		t.Fatalf("FAIL: go-api refuses the proxy flags: %v", err)
	}
	for agg, want := range map[apiservice.AuthAggregate]bool{
		apiservice.AuthSessions: true, apiservice.AuthLocalLogin: true, apiservice.AuthSocial: true,
		apiservice.AuthCredentials: true, apiservice.AuthOIDC: false, apiservice.AuthSAML: false,
	} {
		if cfg.Enabled(agg) != want {
			t.Errorf("FAIL: go-api %s enabled=%v, prod %v", agg, cfg.Enabled(agg), want)
		}
	}
	if cfg.OrgProvisioningOn() {
		t.Error("FAIL: AUTH_PROXY_ORG_PROVISIONING is on; prod has it off")
	}
	// CHAOS-13356: users are owned, as prod (phase 4, usersOwnedStage owned): go-api forwards the credentials routes.
	if env["AUTH_USERS_OWNED"] != "true" {
		t.Errorf("FAIL: go-api AUTH_USERS_OWNED=%q, prod true", env["AUTH_USERS_OWNED"])
	}
	for _, name := range []string{"AUTH_SESSION_ISSUE_TOKEN", "AUTH_WORKLOAD_SUBJECT_FILE"} {
		if _, ok := env[name]; ok {
			t.Errorf("FAIL: go-api env carries %s", name)
		}
	}
	for name, want := range map[string]string{"AUTH_SESSION_ISSUE_CREDENTIAL": "workload", "AUTH_PROVISIONING_CREDENTIAL": "workload"} {
		if env[name] != want {
			t.Errorf("FAIL: %s=%q, prod %q", name, env[name], want)
		}
	}
	if env["AUTH_WORKLOAD_BOOTSTRAP_FILE"] == "" || env["AUTH_WORKLOAD_ISSUER"] != env["AUTH_ACCESS_TOKEN_ISSUER"] {
		t.Errorf("FAIL: the workload inputs are incomplete or the workload issuer differs from the access token issuer")
	}
}

func TestLiveE2EAuthServiceEnvLoadsAndMintsEdDSA(t *testing.T) {
	env := liveStackEnv(t, "auth-service")
	cfg, err := authconfig.Load(authconfig.Spec{LookupEnv: func(k string) (string, bool) { v, ok := env[k]; return v, ok }})
	if err != nil {
		t.Fatalf("FAIL: auth-service refuses the live e2e environment: %v", err)
	}
	if cfg.Session.AccessClass != authconfig.AccessClassEdDSA || !cfg.Session.Enabled || !cfg.Session.LoginEnabled {
		t.Errorf("FAIL: class %q, session routes %v, login %v", cfg.Session.AccessClass, cfg.Session.Enabled, cfg.Session.LoginEnabled)
	}
	if cfg.SessionIssueWorkloadSubject != "prn_hh9WycdvSHSM26P15yRmgA" || !strings.HasPrefix(cfg.TokenIssuerURL, "https://") {
		t.Errorf("FAIL: workload subject %q, issuer %q", cfg.SessionIssueWorkloadSubject, cfg.TokenIssuerURL)
	}
}

// CHAOS-13356: with users owned go-api forwards register to auth-service, whose register budget is 3 per hour per
// client address unless AUTH_REGISTER_LIMIT says otherwise; every live e2e request comes from one address. The script
// gives auth-service go-api's venue value (1000/hour, the form go-api parses) and no other side of the script sets it;
// prod sets it nowhere (deploy/helm/dev-health/auth_register_limit_test.go).
func TestLiveE2EAuthStackRegisterLimitIsAuthServiceOnly(t *testing.T) {
	env := liveStackEnv(t, "auth-service")
	if env["AUTH_REGISTER_LIMIT"] != "1000/hour" {
		t.Fatalf("FAIL: auth-service AUTH_REGISTER_LIMIT=%q, want 1000/hour", env["AUTH_REGISTER_LIMIT"])
	}
	limit, err := httpapi.ParseLimit("auth_svc_register_ip", env["AUTH_REGISTER_LIMIT"])
	if err != nil || limit.Count != 1000 || limit.Window.Hours() != 1 {
		t.Fatalf("FAIL: AUTH_REGISTER_LIMIT is not go-api's form: %+v %v", limit, err)
	}
	for _, side := range []string{"go-api", "query-api", "web"} {
		if value, ok := liveStackEnv(t, side)["AUTH_REGISTER_LIMIT"]; ok {
			t.Errorf("FAIL: the script sets AUTH_REGISTER_LIMIT=%q for %s; it is auth-service's only", value, side)
		}
	}
}

func TestLiveE2EQueryAPIEnvIsProdsFlagSet(t *testing.T) {
	env := liveStackEnv(t, "query-api")
	if env["AUTH_PROXY_SESSIONS"] != "true" || env["AUTH_ACCESS_TOKEN_ISSUER"] == "" {
		t.Fatalf("FAIL: query-api env %v lacks the sessions flag or the access token issuer", env)
	}
}

// CHAOS-13356: prod runs platform roles owned (deploy values.prod.yaml:1830), and the chart renders
// AUTH_PLATFORM_ROLES_OWNED=true on auth-service, go-api and query-api, no other Deployment
// (deploy/helm/dev-health/live_e2e_auth_flags_test.go pins the flag table rows to that render). go-api's impersonation
// refusal of a superuser target (internal/apiservice/admin/impersonation.go) reads is_superuser through
// policy.PlatformRoleRead: the venue must read it from auth.platform_role_assignments, the table grant-platform-admin
// writes, as prod does, not from public.users.is_superuser. web reads no platform role and gets no switch.
func TestLiveE2EPlatformRolesOwnedOnEveryProdSide(t *testing.T) {
	for _, side := range []string{"go-api", "query-api", "auth-service"} {
		env := liveStackEnv(t, side)
		owned, err := rule.OwnedFromLookup(func(k string) (string, bool) { v, ok := env[k]; return v, ok })
		if err != nil || !owned {
			t.Errorf("FAIL: %s %s=%q (owned %v, %v); prod renders true", side, rule.OwnedEnv, env[rule.OwnedEnv], owned, err)
		}
	}
	if value, ok := liveStackEnv(t, "web")[rule.OwnedEnv]; ok {
		t.Errorf("FAIL: web carries %s=%q; prod renders it on no web workload", rule.OwnedEnv, value)
	}
	env := liveStackEnv(t, "go-api")
	owned, err := rule.OwnedFromLookup(func(k string) (string, bool) { v, ok := env[k]; return v, ok })
	if err != nil {
		t.Fatalf("FAIL: go-api refuses %s=%q: %v", rule.OwnedEnv, env[rule.OwnedEnv], err)
	}
	expr, err := policy.PlatformRoleRead{Owned: owned, AuthSchema: env["AUTH_DATABASE_SCHEMA"]}.Superuser("is_superuser", "users.id")
	if err != nil {
		t.Fatalf("FAIL: go-api's is_superuser read: %v", err)
	}
	if want := rule.HoldsSQL(`"auth"`, "users.id", "'"+rule.PlatformAdmin+"'"); expr != want {
		t.Errorf("FAIL: the venue's go-api reads an impersonation target's is_superuser as %q, not the auth platform role grant-platform-admin writes", expr)
	}
}

func liveE2EUpFakes(t *testing.T, state string) string {
	t.Helper()
	fake := t.TempDir()
	write := func(name, body string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(fake, name), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	write("go", "#!/bin/sh\nwhile [ \"$1\" != -o ]; do shift; done\nprintf '#!/bin/sh\\necho started\\nprintf %%s \"$AUTH_SESSION_ISSUE_WORKLOAD_SUBJECT\" > \"'"+state+"'/subject\"\\n' > \"$2\"\nchmod 755 \"$2\"\n")
	write("psql", "#!/bin/sh\ncase \"$*\" in *-At*) echo 3b1e2c4d-5f60-4a71-8b92-0c1d2e3f4a5b ;; esac\n")
	write("sleep", "#!/bin/sh\nexit 0\n")
	write("curl", "#!/bin/sh\ngrep -q '^started$' '"+state+"/logs/auth-service.log'\n")
	// dho records each call with the AUTH_USERS_OWNED it ran under (the seed tool runs owned, CHAOS-13356).
	write("dho", "#!/bin/sh\nprintf '%s %s\\n' \"${AUTH_USERS_OWNED:-unset}\" \"$*\" >> '"+state+"/dho.calls'\nexit 0\n")
	return fake
}

// liveE2ENoXxdPath is a PATH of every host tool except xxd, with fakes in front.
func liveE2ENoXxdPath(t *testing.T, fake string) string {
	t.Helper()
	bin := t.TempDir()
	for _, dir := range []string{"/usr/local/bin", "/usr/bin", "/bin"} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if e.Name() == "xxd" {
				continue
			}
			_ = os.Symlink(filepath.Join(dir, e.Name()), filepath.Join(bin, e.Name()))
		}
	}
	return fake + ":" + bin
}

func runLiveE2EUp(t *testing.T, path, fake, state string) string {
	t.Helper()
	cmd := exec.Command("bash", "ci/live_e2e_auth_stack.sh", "up", state)
	cmd.Dir = repoRoot(t)
	cmd.Env = []string{
		"PATH=" + path,
		"PGUSER=postgres",
		"PGPASSWORD=placeholder-for-fake-psql",                // gitleaks:allow -- placeholder value for the fake psql, not a credential
		"JWT_SECRET_KEY=0123456789abcdef0123456789abcdef0123", // gitleaks:allow -- placeholder value fed to the fakes, not a credential
		"DHO_BIN=" + filepath.Join(fake, "dho"),
	}
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Run(); err != nil {
		logged, _ := os.ReadFile(filepath.Join(state, "logs", "auth-service.log"))
		t.Fatalf("FAIL: up did not start auth-service: %v\n%s\nauth-service.log:\n%s", err, out.String(), logged)
	}
	return out.String()
}

// The web workflow runs the script by a path relative to the ops checkout. `up` changes directory into the state
// directory before it starts auth-service through `with-env`, so that re-exec has to name the script by an absolute path.
func TestLiveE2EAuthStackUpStartsAuthServiceWhenRunByARelativePath(t *testing.T) {
	state := filepath.Join(t.TempDir(), "state")
	fake := liveE2EUpFakes(t, state)
	out := runLiveE2EUp(t, fake+":"+os.Getenv("PATH"), fake, state)
	assertUpWiredTheWebCaller(t, state, out)
}

// Hosted race legs have no xxd. The workload subject is the uuid of the fake psql row as prn_ and unpadded base64url.
func TestLiveE2EAuthStackUpNeedsNoXxdAndPinsTheWorkloadSubject(t *testing.T) {
	state := filepath.Join(t.TempDir(), "state")
	fake := liveE2EUpFakes(t, state)
	runLiveE2EUp(t, liveE2ENoXxdPath(t, fake), fake, state)
	got, err := os.ReadFile(filepath.Join(state, "subject"))
	if err != nil {
		t.Fatal(err)
	}
	if want := "prn_Ox4sTV9gSnGLkgwdLj9KWw"; string(got) != want {
		t.Fatalf("FAIL: workload subject %q, want %q", got, want)
	}
}

// up binds the web workload, writes the web subject and the reviewer token, and saves the auth-service environment
// (mode 0600) that start-auth reuses.
func assertUpWiredTheWebCaller(t *testing.T, state, out string) {
	t.Helper()
	saved, err := os.ReadFile(filepath.Join(state, "auth-service.env"))
	if err != nil {
		t.Fatalf("FAIL: up saved no auth-service environment: %v", err)
	}
	if !strings.Contains(string(saved), "\nAUTH_AUTHORIZE_CALLER_SUBJECTS=prn_") && !strings.HasPrefix(string(saved), "AUTH_AUTHORIZE_CALLER_SUBJECTS=prn_") {
		t.Errorf("FAIL: the saved environment names no authorize caller")
	}
	if info, err := os.Stat(filepath.Join(state, "auth-service.env")); err != nil || info.Mode().Perm() != 0o600 {
		t.Errorf("FAIL: auth-service.env mode %v err %v, want 0600", info, err)
	}
	if !strings.Contains(out, "step 5b") || !strings.Contains(out, "step 6b") {
		t.Errorf("FAIL: up skipped the web binding or the fake TokenReview step:\n%s", out)
	}
	// CHAOS-13356: users are owned. The seed tool copies the users the caller seeded before auth-service starts, with
	// users owned, and auth-service has a mail setting for the credentials routes it now serves.
	calls, err := os.ReadFile(filepath.Join(state, "dho.calls"))
	if err != nil || !strings.Contains(string(calls), "true auth users-backfill -insert-only\n") {
		t.Errorf("FAIL: up did not run the seed tool owned (dho calls %q, err %v)", calls, err)
	}
	seed, start := strings.Index(out, "step 4b"), strings.Index(out, "step 7")
	if seed < 0 || start < 0 || seed > start {
		t.Errorf("FAIL: the seed step (%d) must come before auth-service starts (%d)", seed, start)
	}
	for _, want := range []string{"AUTH_USERS_OWNED=true\n", "EMAIL_PROVIDER=console\n", "AUTH_MAIL_ALLOW_CONSOLE=true\n", "EMAIL_FROM_ADDRESS=", "APP_BASE_URL=", "AUTH_REGISTER_LIMIT=1000/hour\n"} {
		if !strings.Contains(string(saved), want) {
			t.Errorf("FAIL: the saved auth-service environment lacks %q", strings.TrimSpace(want))
		}
	}
}

// stopStartFixture fills the state dir with a saved environment, a fake auth-service and a fake readyz. run calls a verb of
// the script with the fakes first on PATH; down runs at cleanup.
func stopStartFixture(t *testing.T, fake, state string, extraEnv ...string) (run func(verb string)) {
	t.Helper()
	root := repoRoot(t)
	for _, d := range []string{"bin", "logs"} {
		if err := os.Mkdir(filepath.Join(state, d), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	// auth-service records its start, then writes its pid to auth-service.ready: the real readyz answers only after the
	// process is up with its environment, not when its parent forked it.
	writeExec(t, filepath.Join(state, "bin", "auth-service"), "#!/bin/sh\necho \"$MARK\" >> '"+state+"/logs/starts'\n"+
		"echo $$ > '"+state+"/auth-service.ready'\nexec sleep 60\n", 0o755)
	writeExec(t, filepath.Join(state, "auth-service.env"), "MARK=started-with-saved-env\n", 0o600)
	// readyz answers while the pid of the pid file is alive and is the pid that wrote auth-service.ready.
	writeExec(t, filepath.Join(fake, "curl"), "#!/bin/sh\npid=\"$(cat '"+state+"/auth-service.pid')\"\n"+
		"[ \"$(cat '"+state+"/auth-service.ready' 2>/dev/null)\" = \"$pid\" ] && kill -0 \"$pid\" 2>/dev/null\n", 0o755)
	run = func(verb string) {
		t.Helper()
		cmd := exec.Command("bash", "ci/live_e2e_auth_stack.sh", verb, state)
		cmd.Dir = root
		cmd.Env = append([]string{"PATH=" + fake + ":" + os.Getenv("PATH")}, extraEnv...)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("FAIL: %s: %v\n%s", verb, err, out)
		}
	}
	t.Cleanup(func() { run("down") })
	return run
}

func writeExec(t *testing.T, path, body string, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), mode); err != nil {
		t.Fatal(err)
	}
}

func startCount(t *testing.T, state string) (int, string) {
	t.Helper()
	starts, _ := os.ReadFile(filepath.Join(state, "logs", "starts"))
	return strings.Count(string(starts), "started-with-saved-env"), string(starts)
}

// stop-auth ends auth-service by pid (the outage cell); start-auth brings it back with the saved environment.
func TestLiveE2EStopAndStartAuthService(t *testing.T) {
	state := t.TempDir()
	run := stopStartFixture(t, t.TempDir(), state)
	run("start-auth")
	run("stop-auth")
	run("start-auth")
	if n, starts := startCount(t, state); n != 2 {
		t.Errorf("FAIL: auth-service started with the saved environment %d times, want 2:\n%s", n, starts)
	}
}

// start-auth answers only after auth-service is up with the saved environment. The fake auth-service here holds before
// it records its start until the ready loop of start-auth polls again (its sleep is a fake that opens the gate), so a
// readyz that answers before the start is recorded returns from start-auth with no start recorded. The ready file names
// a live pid that is not auth-service's: a readyz that trusts any ready file or any live pid in it answers too early.
func TestLiveE2EStartAuthAnswersOnlyAfterAuthServiceRecordedItsStart(t *testing.T) {
	realSleep, err := exec.LookPath("sleep")
	if err != nil {
		t.Fatal(err)
	}
	fake, state := t.TempDir(), t.TempDir()
	run := stopStartFixture(t, fake, state)
	for _, f := range []string{"gate", "recorded"} {
		if err := syscall.Mkfifo(filepath.Join(state, f), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	writeExec(t, filepath.Join(state, "auth-service.ready"), strconv.Itoa(os.Getpid())+"\n", 0o600)
	writeExec(t, filepath.Join(state, "bin", "auth-service"), "#!/bin/sh\nread _ < '"+state+"/gate'\n"+
		"echo \"$MARK\" >> '"+state+"/logs/starts'\necho $$ > '"+state+"/auth-service.ready'\n"+
		"echo recorded > '"+state+"/recorded'\nexec '"+realSleep+"' 60\n", 0o755)
	writeExec(t, filepath.Join(fake, "sleep"), "#!/bin/sh\necho go > '"+state+"/gate'\nread _ < '"+state+"/recorded'\n", 0o755)
	run("start-auth")
	if n, starts := startCount(t, state); n != 1 {
		t.Errorf("FAIL: start-auth answered with %d recorded starts, want 1: readyz answered before auth-service was up:\n%s", n, starts)
	}
}

const (
	sentinelJWT       = "jwt-key-sentinel"
	sentinelSuccessor = "successor-key-sentinel"
	sentinelLockout   = "lockout-key-sentinel"
	sentinelDBPass    = "db-password-sentinel"
)

func sentinelEnv() []string {
	return []string{
		"PATH=" + os.Getenv("PATH"),
		"JWT_SECRET_KEY=" + sentinelJWT,                    // gitleaks:allow -- sentinel for the redaction cell, not a credential
		"AUTH_SESSION_SUCCESSOR_KEY=" + sentinelSuccessor,  // gitleaks:allow -- sentinel for the redaction cell, not a credential
		"AUTH_LOGIN_LOCKOUT_KEY=" + sentinelLockout,        // gitleaks:allow -- sentinel for the redaction cell, not a credential
		"AUTH_RUNTIME_DATABASE_PASSWORD=" + sentinelDBPass, // gitleaks:allow -- sentinel for the redaction cell, not a credential
		"AUTH_SESSION_ISSUE_WORKLOAD_SUBJECT=prn_subject-sentinel",
		"AUTH_AUTHORIZE_CALLER_SUBJECTS=prn_caller-sentinel",
	}
}

// print-env is a diagnostic printer: no secret value may travel through its output.
func TestLiveE2EPrintEnvAuthServiceRedactsEverySecret(t *testing.T) {
	cmd := exec.Command("bash", filepath.Join(repoRoot(t), "ci", "live_e2e_auth_stack.sh"), "print-env", "auth-service", "/state")
	cmd.Env = sentinelEnv()
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		t.Fatalf("FAIL: print-env auth-service: %v\n%s", err, errb.String())
	}
	for name, sentinel := range map[string]string{"JWT_SECRET_KEY": sentinelJWT, "AUTH_SESSION_SUCCESSOR_KEY": sentinelSuccessor, "AUTH_LOGIN_LOCKOUT_KEY": sentinelLockout, "database password": sentinelDBPass} {
		if strings.Contains(out.String()+errb.String(), sentinel) {
			t.Errorf("FAIL: print-env auth-service prints the %s value", name)
		}
	}
	if !strings.Contains(out.String(), "AUTH_DATABASE_URI=") {
		t.Errorf("FAIL: print-env auth-service dropped the AUTH_DATABASE_URI line")
	}
}

// with-env still hands the real values to the child; the child prints only yes or no.
func TestLiveE2EWithEnvAuthServicePassesTheRealSecrets(t *testing.T) {
	script := `[ "$AUTH_JWT_SECRET_KEY" = ` + sentinelJWT + ` ] && [ "$AUTH_SESSION_SUCCESSOR_KEY" = ` + sentinelSuccessor +
		` ] && [ "$AUTH_LOGIN_LOCKOUT_KEY" = ` + sentinelLockout + ` ] && case "$AUTH_DATABASE_URI" in *"password='` + sentinelDBPass + `'"*) true ;; *) false ;; esac && echo yes || echo no`
	cmd := exec.Command("bash", filepath.Join(repoRoot(t), "ci", "live_e2e_auth_stack.sh"), "with-env", "auth-service", "/state", "--", "bash", "-c", script)
	cmd.Env = sentinelEnv()
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("FAIL: with-env auth-service: %v", err)
	}
	if strings.TrimSpace(string(out)) != "yes" {
		t.Errorf("FAIL: with-env auth-service did not pass the real secret values to the child")
	}
}

// A side whose required variable is unset is refused: with-env runs no command. A process substitution would hide the
// refusal of raw_env and run the command with half the environment.
func TestLiveE2EWithEnvRefusesAHalfConfiguredSide(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "ran")
	cmd := exec.Command("bash", filepath.Join(repoRoot(t), "ci", "live_e2e_auth_stack.sh"), "with-env", "auth-service", "/state", "--", "touch", marker)
	cmd.Env = sentinelEnv()[:5:5] // no AUTH_SESSION_ISSUE_WORKLOAD_SUBJECT
	var errb bytes.Buffer
	cmd.Stderr = &errb
	err := cmd.Run()
	if _, statErr := os.Stat(marker); statErr == nil {
		t.Errorf("FAIL: with-env ran the command on a half-configured auth-service side")
	}
	if err == nil {
		t.Errorf("FAIL: with-env answered success for an auth-service side with a required variable unset")
	}
	if !strings.Contains(errb.String(), "AUTH_SESSION_ISSUE_WORKLOAD_SUBJECT") {
		t.Errorf("FAIL: with-env does not name the unset variable: %q", errb.String())
	}
}

// assert-audit reads one row: total workload service_credential. Every delegated issue row must carry the workload
// marker and no service credential marker.
func TestLiveE2EAssertAuditCells(t *testing.T) {
	fake := t.TempDir()
	shim := "#!/bin/sh\ncase \"$AUDIT_CASE\" in\n" +
		"one_valid_issue) echo '1 1 0' ;;\n" +
		"valid_plus_missing_workload_marker) echo '2 1 0' ;;\n" +
		"valid_plus_service_credential) echo '2 1 1' ;;\n" +
		"no_workload_marker) echo '1 0 0' ;;\n" +
		"no_issue) echo '0 0 0' ;;\n" +
		"two_valid_issues) echo '2 2 0' ;;\n" +
		"*) exit 2 ;;\nesac\n"
	if err := os.WriteFile(filepath.Join(fake, "psql"), []byte(shim), 0o755); err != nil {
		t.Fatal(err)
	}
	for cell, wantOK := range map[string]bool{
		"one_valid_issue": true, "two_valid_issues": true,
		"valid_plus_missing_workload_marker": false, "valid_plus_service_credential": false,
		"no_workload_marker": false, "no_issue": false,
	} {
		t.Run(cell, func(t *testing.T) {
			cmd := exec.Command("bash", filepath.Join(repoRoot(t), "ci", "live_e2e_auth_stack.sh"), "assert-audit", t.TempDir())
			cmd.Env = []string{"PATH=" + fake + ":" + os.Getenv("PATH"), "AUDIT_CASE=" + cell}
			err := cmd.Run()
			if wantOK && err != nil {
				t.Errorf("FAIL: %s refused: %v", cell, err)
			}
			if !wantOK && err == nil {
				t.Errorf("FAIL: %s accepted", cell)
			}
		})
	}
}

// liveE2ESetupVerb runs a test-setup verb of the stack script (verify-user, grant-platform-admin) against a fake psql. The fake
// records every SQL text it gets (one per line, newlines folded) and answers by its STACK_CASE: the write answers nothing or
// fails, the read-back answers the case's count.
func liveE2ESetupVerb(t *testing.T, verb, email, stackCase string) (string, string, error) {
	t.Helper()
	fake := t.TempDir()
	record := filepath.Join(fake, "sql.log")
	shim := "#!/bin/sh\nfor a in \"$@\"; do last=\"$a\"; done\nprintf '%s\\n' \"$last\" | tr '\\n' ' ' >> '" + record + "'; echo >> '" + record + "'\n" +
		"case \"$last\" in\n" +
		"  *INSERT\\ INTO*) [ \"$STACK_CASE\" = write_fails ] && exit 1; exit 0 ;;\n" +
		"esac\n" +
		"case \"$STACK_CASE\" in\n" +
		"  verified) echo t; echo '1 1' ;;\n  verified_none) echo t; echo '0 0' ;;\n  verified_auth_only) echo t; echo '1 0' ;;\n" +
		"  granted) echo 1 ;;\n  granted_none|write_fails) echo 0 ;;\n  granted_two) echo 2 ;;\n  *) exit 2 ;;\nesac\n"
	if err := os.WriteFile(filepath.Join(fake, "psql"), []byte(shim), 0o755); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	cmd := exec.Command("bash", filepath.Join(repoRoot(t), "ci", "live_e2e_auth_stack.sh"), verb, t.TempDir(), email)
	cmd.Env = []string{"PATH=" + fake + ":" + os.Getenv("PATH"), "STACK_CASE=" + stackCase}
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	sql, _ := os.ReadFile(record)
	return out.String(), string(sql), err
}

// CHAOS-13356: verify-user is test setup. It writes auth.users (the authority while users are owned) through the audit
// guard and the public.users projection in one transaction, and fails loud unless the read-back is one row of each.
func TestLiveE2EVerifyUserCells(t *testing.T) {
	for cell, wantOK := range map[string]bool{"verified": true, "verified_none": false, "verified_auth_only": false} {
		t.Run(cell, func(t *testing.T) {
			out, sql, err := liveE2ESetupVerb(t, "verify-user", "a@example.test", cell)
			if wantOK != (err == nil) {
				t.Fatalf("FAIL: %s: exit err %v, want ok=%v\n%s", cell, err, wantOK, out)
			}
			if !wantOK && !strings.Contains(out, "verify-user: want one auth.users and one public.users row") {
				t.Errorf("FAIL: %s failed without naming the read-back:\n%s", cell, out)
			}
			if !strings.Contains(sql, "pg_current_xact_id()::text, true)") || !strings.Contains(sql, "UPDATE auth.users") {
				t.Errorf("FAIL: the write is not one guarded transaction on auth.users: %q", sql)
			}
		})
	}
	if _, _, err := liveE2ESetupVerb(t, "verify-user", "not-an-address", "verified"); err == nil {
		t.Error("FAIL: verify-user accepted a value that is not an e-mail address")
	}
}

// CHAOS-13356: grant-platform-admin is test setup. It writes the user's platform_admin row directly into
// auth.platform_role_assignments (the guard's audit transaction first, one psql string), then fails loud unless the read-back
// of the user's active rows is exactly 1.
func TestLiveE2EGrantPlatformAdminCells(t *testing.T) {
	for cell, wantOK := range map[string]bool{"granted": true, "granted_none": false, "granted_two": false, "write_fails": false} {
		t.Run(cell, func(t *testing.T) {
			out, sql, err := liveE2ESetupVerb(t, "grant-platform-admin", "admin@example.test", cell)
			if wantOK != (err == nil) {
				t.Fatalf("FAIL: %s: exit err %v, want ok=%v\n%s", cell, err, wantOK, out)
			}
			if cell == "granted_none" || cell == "granted_two" {
				if !strings.Contains(out, "grant-platform-admin: want one active platform_admin row") {
					t.Errorf("FAIL: %s failed without naming the read-back:\n%s", cell, out)
				}
			}
			lines := strings.Split(strings.TrimSpace(sql), "\n")
			write := lines[0]
			if !strings.Contains(write, "pg_current_xact_id()::text, true)") ||
				!strings.Contains(write, "INSERT INTO auth.platform_role_assignments") || !strings.Contains(write, "'platform_admin'") ||
				strings.Index(write, "set_config") > strings.Index(write, "INSERT INTO") {
				t.Errorf("FAIL: the write is not one guarded transaction that inserts the platform_admin row: %q", write)
			}
			if cell != "write_fails" && (len(lines) < 2 || !strings.Contains(lines[1], "disabled_at IS NULL") || !strings.Contains(lines[1], "expires_at")) {
				t.Errorf("FAIL: the read-back does not count active rows by the store's rule: %q", sql)
			}
		})
	}
	if _, _, err := liveE2ESetupVerb(t, "grant-platform-admin", "x' OR '1'='1", "granted"); err == nil {
		t.Error("FAIL: grant-platform-admin accepted a value that is not an e-mail address")
	}
}

// The PDP refuses every caller unless AUTH_AUTHORIZE_CALLER_SUBJECTS names it, and the web workload gets its token through
// the exchange against a job-local fake of the Kubernetes API (the product trusts its issuer by configuration).
func TestLiveE2EAuthServiceEnvLetsTheWebWorkloadCallThePDP(t *testing.T) {
	env := liveStackEnv(t, "auth-service")
	cfg, err := authconfig.Load(authconfig.Spec{LookupEnv: func(k string) (string, bool) { v, ok := env[k]; return v, ok }})
	if err != nil {
		t.Fatalf("FAIL: auth-service refuses the live e2e environment: %v", err)
	}
	if len(cfg.AuthorizeCallerSubjects) != 1 || cfg.AuthorizeCallerSubjects[0] != webSubject {
		t.Errorf("FAIL: authorize callers %v, want only the web workload %s", cfg.AuthorizeCallerSubjects, webSubject)
	}
	ex := cfg.WorkloadExchange
	if !ex.Enabled || !ex.Bootstrap {
		t.Errorf("FAIL: exchange enabled %v, bootstrap %v: go-api keeps the bootstrap subject, web needs the TokenReview one", ex.Enabled, ex.Bootstrap)
	}
	if !strings.HasPrefix(ex.KubeAPIURL, "https://127.0.0.1:") || ex.ClusterIssuer == "" || ex.TrustDomain == "" {
		t.Errorf("FAIL: kube API %q, cluster issuer %q, trust domain %q", ex.KubeAPIURL, ex.ClusterIssuer, ex.TrustDomain)
	}
	if !strings.HasSuffix(ex.KubeCAFile, "/tls/ca.pem") || !strings.HasSuffix(ex.ReviewerTokenFile, "/workload/reviewer.token") {
		t.Errorf("FAIL: CA file %q, reviewer token file %q", ex.KubeCAFile, ex.ReviewerTokenFile)
	}
}

func TestLiveE2EAuthServiceEnvNeedsTheCallerSubjects(t *testing.T) {
	cmd := exec.Command("bash", filepath.Join(repoRoot(t), "ci", "live_e2e_auth_stack.sh"), "with-env", "auth-service", "/state", "--", "env")
	cmd.Env = append(sentinelEnv()[:5:5], "AUTH_SESSION_ISSUE_WORKLOAD_SUBJECT=prn_subject-sentinel")
	if err := cmd.Run(); err == nil {
		t.Fatal("FAIL: auth-service env builds without AUTH_AUTHORIZE_CALLER_SUBJECTS: the PDP would refuse web")
	}
}

func TestLiveE2EWebEnvHasTheWorkloadInputs(t *testing.T) {
	env := liveStackEnv(t, "web")
	if env["AUTH_INTERNAL_URL"] != "http://127.0.0.1:8095" || env["AUTH_WORKLOAD_SUBJECT_FILE"] != "/state/workload/web.subject" {
		t.Errorf("FAIL: web workload inputs %q %q", env["AUTH_INTERNAL_URL"], env["AUTH_WORKLOAD_SUBJECT_FILE"])
	}
	if env["AUTH_WORKLOAD_ISSUER"] == "" || env["AUTH_WORKLOAD_ISSUER"] != env["AUTH_ACCESS_TOKEN_ISSUER"] {
		t.Errorf("FAIL: web workload issuer %q, access token issuer %q", env["AUTH_WORKLOAD_ISSUER"], env["AUTH_ACCESS_TOKEN_ISSUER"])
	}
}

// web reads the session token with the class auth-service issues; unset, web reads hs256_compat and refuses an EdDSA token.
func TestLiveE2EWebReadsTheTokenClassAuthServiceIssues(t *testing.T) {
	web, issuer := liveStackEnv(t, "web"), liveStackEnv(t, "auth-service")
	if issuer["AUTH_ACCESS_TOKEN_CLASS"] == "" || web["AUTH_ACCESS_TOKEN_CLASS"] != issuer["AUTH_ACCESS_TOKEN_CLASS"] {
		t.Errorf("FAIL: web token class %q, auth-service token class %q", web["AUTH_ACCESS_TOKEN_CLASS"], issuer["AUTH_ACCESS_TOKEN_CLASS"])
	}
}

func TestLiveE2EEnvOverlays(t *testing.T) {
	for _, side := range []string{"go-api", "query-api", "web"} {
		t.Run(side, func(t *testing.T) {
			state := t.TempDir()
			if err := os.Mkdir(filepath.Join(state, "extra"), 0o700); err != nil {
				t.Fatal(err)
			}
			writeExec(t, filepath.Join(state, "extra", side+".env"), "AUTH_ACCESS_TOKEN_ISSUER=overlay-issuer\nOVERLAY_VALUE=literal=$(false)=value", 0o600)
			for _, verb := range []string{"with-env", "print-env"} {
				args := []string{filepath.Join(repoRoot(t), "ci", "live_e2e_auth_stack.sh"), verb, side, state}
				if verb == "with-env" {
					args = append(args, "--", "env")
				}
				cmd := exec.Command("bash", args...)
				cmd.Env = sentinelEnv()
				out, err := cmd.CombinedOutput()
				if err != nil {
					t.Fatalf("%s: %v\n%s", verb, err, out)
				}
				for _, want := range []string{"AUTH_ACCESS_TOKEN_ISSUER=overlay-issuer\n", "OVERLAY_VALUE=literal=$(false)=value\n"} {
					if !strings.Contains(string(out), want) {
						t.Errorf("%s lacks %q: %s", verb, want, out)
					}
				}
				if verb == "with-env" && strings.Contains(string(out), "AUTH_ACCESS_TOKEN_ISSUER=https://") {
					t.Error("the overlay did not win over the side's own lines")
				}
			}
			for _, invalid := range []string{"bad=value", "# comment", "", "NO_VALUE", "export KEY=value", "1KEY=value"} {
				writeExec(t, filepath.Join(state, "extra", side+".env"), "SAFE=value\n"+invalid+"\n", 0o600)
				for _, verb := range []string{"with-env", "print-env"} {
					marker := filepath.Join(state, "ran")
					args := []string{filepath.Join(repoRoot(t), "ci", "live_e2e_auth_stack.sh"), verb, side, state}
					if verb == "with-env" {
						args = append(args, "--", "touch", marker)
					}
					cmd := exec.Command("bash", args...)
					out, err := cmd.CombinedOutput()
					if err == nil || !strings.Contains(string(out), "invalid overlay line") {
						t.Fatalf("%s accepted %q or failed without a reason: %v %s", verb, invalid, err, out)
					}
					if _, err := os.Stat(marker); !os.IsNotExist(err) {
						t.Fatal("with-env ran a command with an invalid overlay")
					}
				}
			}
		})
	}
	state := t.TempDir()
	if err := os.Mkdir(filepath.Join(state, "extra"), 0o700); err != nil {
		t.Fatal(err)
	}
	writeExec(t, filepath.Join(state, "extra", "auth-service.env"), "invalid overlay\n", 0o600)
	cmd := exec.Command("bash", filepath.Join(repoRoot(t), "ci", "live_e2e_auth_stack.sh"), "with-env", "auth-service", state, "--", "true")
	cmd.Env = sentinelEnv()
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("auth-service read an overlay: %v %s", err, out)
	}
}

func liveE2EStubFakes(t *testing.T) (string, string, func(...string) (string, error)) {
	t.Helper()
	state, fake := t.TempDir(), t.TempDir()
	stopStartFixture(t, fake, state, "FAKE_STATE="+state, "FAKE_BIN="+fake)
	if err := os.Mkdir(filepath.Join(state, "tls"), 0o700); err != nil {
		t.Fatal(err)
	}
	writeExec(t, filepath.Join(state, "tls", "ca.pem"), "auth-ca\n", 0o600)
	writeExec(t, filepath.Join(state, "hosts"), "127.0.0.1 localhost\n", 0o600)
	writeExec(t, filepath.Join(state, "bin", "auth-service"), `#!/bin/sh
printf '%s\n' "$MARK" >> "$FAKE_STATE/logs/starts"
env > "$FAKE_STATE/auth-service.loaded"
echo $$ > "$FAKE_STATE/auth-service.ready"
exec sleep 60
`, 0o755)
	writeExec(t, filepath.Join(fake, "go"), `#!/bin/sh
printf '%s %s\n' "$PWD" "$*" >> "$FAKE_STATE/builds"
while [ "$1" != -o ]; do shift; done
cp "$FAKE_BIN/stub-template" "$2"
chmod 755 "$2"
`, 0o755)
	writeExec(t, filepath.Join(fake, "stub-template"), `#!/bin/sh
name=$(basename "$0")
printf '%s %s\n' "$name" "$*" >> "$FAKE_STATE/stub.calls"
case "$1" in
  certs)
    [ "$2" = -out ] || exit 2
    mkdir -p "$3"
    printf '%s-ca\n' "$name" > "$3/ca.pem"
    touch "$3/server.pem" "$3/server.key"
    ;;
  serve)
    echo "$name serve started"
    echo $$ > "$FAKE_STATE/$name.ready"
    sleep 60 & child=$!
    trap 'kill "$child"; exit 0' TERM
    wait "$child"
    ;;
  *) exit 2 ;;
esac
`, 0o755)
	writeExec(t, filepath.Join(fake, "sudo"), `#!/bin/sh
printf '%s\n' "$*" >> "$FAKE_STATE/sudo.calls"
case "$1" in
  tee) [ "$2" = -a ] && [ "$3" = /etc/hosts ] || exit 2; exec tee -a "$FAKE_STATE/hosts" ;;
  sed) [ "$2" = -i ] && [ "$3" = '/# live-e2e-social/d' ] && [ "$4" = /etc/hosts ] || exit 2
    exec sed -i "$3" "$FAKE_STATE/hosts" ;;
  kill) exec /bin/kill "$2" ;;
  bash) exec "$@" ;;
  *) exit 2 ;;
esac
`, 0o755)
	grep, err := exec.LookPath("grep")
	if err != nil {
		t.Fatal(err)
	}
	writeExec(t, filepath.Join(fake, "grep"), "#!/bin/bash\nargs=()\nfor arg; do\n"+
		"if [ \"$arg\" = /etc/hosts ]; then arg=\"$FAKE_STATE/hosts\"; fi\nargs+=(\"$arg\")\ndone\nexec '"+grep+"' \"${args[@]}\"\n", 0o755)
	writeExec(t, filepath.Join(fake, "curl"), `#!/bin/sh
printf '%s\n' "$*" >> "$FAKE_STATE/curl.calls"
case "$*" in
  *https://api.github.com/*) name=providerstub ;;
  *https://localhost:9443/*) name=idpstub ;;
  *) name=auth-service ;;
esac
[ "$name" = auth-service ] || [ ! -f "$FAKE_STATE/fail-stub" ] || exit 1
pid=$(cat "$FAKE_STATE/$name.pid" 2>/dev/null)
[ "$(cat "$FAKE_STATE/$name.ready" 2>/dev/null)" = "$pid" ] && kill -0 "$pid" 2>/dev/null
`, 0o755)
	sleep, err := exec.LookPath("sleep")
	if err != nil {
		t.Fatal(err)
	}
	writeExec(t, filepath.Join(fake, "sleep"), "#!/bin/sh\n[ \"$1\" != 1 ] || set -- 0.01\nexec '"+sleep+"' \"$@\"\n", 0o755)
	writeExec(t, filepath.Join(fake, "openssl"), "#!/bin/sh\n[ \"$*\" = 'rand -base64 32' ] || exit 2\nprintf 'new-state-key+test/==\\n'\n", 0o755)
	env := []string{"PATH=" + fake + ":" + os.Getenv("PATH"), "FAKE_STATE=" + state, "FAKE_BIN=" + fake, "GITHUB_ACTIONS=true"}
	run := func(args ...string) (string, error) {
		t.Helper()
		cmd := exec.Command("bash", append([]string{filepath.Join(repoRoot(t), "ci", "live_e2e_auth_stack.sh")}, args...)...)
		cmd.Env = env
		out, err := cmd.CombinedOutput()
		return string(out), err
	}
	t.Cleanup(func() {
		if out, err := run("down", state); err != nil {
			t.Errorf("stub cleanup: %v %s", err, out)
		}
	})
	return state, fake, run
}

func liveE2ERead(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func TestLiveE2EEnableSocialAndSSO(t *testing.T) {
	for _, order := range [][]string{{"enable-social", "enable-sso"}, {"enable-sso", "enable-social"}} {
		t.Run(strings.Join(order, "-"), func(t *testing.T) {
			state, _, run := liveE2EStubFakes(t)
			if order[0] == "enable-sso" {
				writeExec(t, filepath.Join(state, "hosts"), "127.0.0.1 localhost\n127.0.0.1 api.github.com github.com # live-e2e-social\n", 0o600)
			}
			mustRun := func(args ...string) string {
				t.Helper()
				out, err := run(args...)
				if err != nil {
					t.Fatalf("%s: %v\n%s", args[0], err, out)
				}
				if strings.Contains(out, "gh-secret") || strings.Contains(out, "new-state-key") {
					t.Fatal("a secret appeared in the command output")
				}
				return out
			}
			mustRun("start-auth", state)
			for i, verb := range order {
				mustRun(verb, state)
				if n, starts := startCount(t, state); n != i+2 {
					t.Fatalf("%s did not restart with the saved environment: %s", verb, starts)
				}
			}
			saved := liveE2ERead(t, filepath.Join(state, "auth-service.env"))
			loaded := liveE2ERead(t, filepath.Join(state, "auth-service.loaded"))
			for _, want := range []string{"MARK=started-with-saved-env", "SOCIAL_GITHUB_CLIENT_ID=gh-client", "SOCIAL_GITHUB_CLIENT_SECRET=gh-secret",
				"SSL_CERT_FILE=" + state + "/ca-bundle.pem", "AUTH_SSO_STATE_KEY=new-state-key-test_", "SETTINGS_ENCRYPTION_KEY=live-e2e-test-settings-key-do-not-use", "APP_BASE_URL=http://127.0.0.1:3002"} {
				if !strings.Contains(saved, want+"\n") || !strings.Contains(loaded, want+"\n") {
					t.Errorf("saved or loaded auth-service environment lacks %s", strings.Split(want, "=")[0])
				}
			}
			info, err := os.Stat(filepath.Join(state, "auth-service.env"))
			if err != nil || info.Mode().Perm() != 0o600 {
				t.Fatalf("saved environment mode: %v %v", info, err)
			}
			if got := liveE2ERead(t, filepath.Join(state, "ca-bundle.pem")); got != "auth-ca\nproviderstub-ca\nidpstub-ca\n" {
				t.Errorf("CA bundle: %q", got)
			}
			for _, side := range []string{"go-api", "web"} {
				var out string
				wants := []string{"AUTH_PROXY_OIDC=true\n", "AUTH_PROXY_SAML=true\n"}
				if side == "web" {
					out, err = run("with-env", side, state, "--", "bash", "-c", `[ "$AUTH_GITHUB_SECRET" = gh-secret ] && env -u AUTH_GITHUB_SECRET`)
					if err != nil {
						t.Fatalf("web's secret was not exported: %v", err)
					}
					wants = []string{"AUTH_GITHUB_ID=gh-client\n", "AUTH_SSO_ENABLED=true\n", "NODE_EXTRA_CA_CERTS=" + state + "/ca-bundle.pem\n"}
				} else {
					out = mustRun("with-env", side, state, "--", "env")
				}
				for _, want := range wants {
					if !strings.Contains(out, want) {
						t.Errorf("%s lacks %q", side, want)
					}
				}
			}
			printed := mustRun("print-env", "web", state)
			if !strings.Contains(printed, "AUTH_GITHUB_SECRET=<redacted>\n") {
				t.Fatal("print-env did not redact the overlay secret")
			}
			mustRun("start-idp", state, "-redirect-uri", "http://127.0.0.1:3002/callback", "-acs-url", "http://127.0.0.1:3002/acs", "-user", "live,live@example.test,Live User")
			calls := liveE2ERead(t, filepath.Join(state, "stub.calls"))
			for _, want := range []string{
				"providerstub certs -out " + state + "/social",
				"idpstub certs -out " + state + "/idp -host localhost",
				"providerstub serve -fixtures " + repoRoot(t) + "/ci/live_e2e_stub -cert " + state + "/social/server.pem -key " + state + "/social/server.key -listen 127.0.0.1:443 -admin 127.0.0.1:9091",
				"idpstub serve -cert " + state + "/idp/server.pem -key " + state + "/idp/server.key -listen 127.0.0.1:9443 -base-url https://localhost:9443 -redirect-uri http://127.0.0.1:3002/callback -acs-url http://127.0.0.1:3002/acs -user live,live@example.test,Live User",
			} {
				if !strings.Contains(calls, want+"\n") {
					t.Errorf("missing stub call %q: %s", want, calls)
				}
			}
			builds := liveE2ERead(t, filepath.Join(state, "builds"))
			for _, name := range []string{"providerstub", "idpstub"} {
				want := repoRoot(t) + " build -o " + state + "/bin/" + name + " ./internal/testsupport/" + name + "/cmd/" + name + "\n"
				if !strings.Contains(builds, want) {
					t.Errorf("missing build %q", want)
				}
				curl := liveE2ERead(t, filepath.Join(state, "curl.calls"))
				ca, url := "social", "https://api.github.com/"
				if name == "idpstub" {
					ca, url = "idp", "https://localhost:9443/"
				}
				if !strings.Contains(curl, "-sS --max-time 1 -o /dev/null --cacert "+state+"/"+ca+"/ca.pem "+url) {
					t.Errorf("%s readiness did not trust its CA or would refuse a non-2xx response", name)
				}
			}
			hosts := liveE2ERead(t, filepath.Join(state, "hosts"))
			if strings.Count(hosts, "127.0.0.1 api.github.com github.com # live-e2e-social\n") != 1 {
				t.Fatalf("social hosts entry was not idempotent: %q", hosts)
			}
			providerPID := strings.TrimSpace(liveE2ERead(t, filepath.Join(state, "providerstub.pid")))
			if ready := strings.TrimSpace(liveE2ERead(t, filepath.Join(state, "providerstub.ready"))); ready != providerPID {
				t.Fatalf("the pid file holds %q, not the stub's own pid %q", providerPID, ready)
			}
			pids := []string{providerPID}
			for _, name := range []string{"auth-service", "idpstub"} {
				pids = append(pids, strings.TrimSpace(liveE2ERead(t, filepath.Join(state, name+".pid"))))
			}
			mustRun("down", state)
			for _, pid := range pids {
				deadline := time.Now().Add(time.Second)
				for {
					raw, _ := os.ReadFile(filepath.Join("/proc", pid, "cmdline"))
					if len(raw) == 0 {
						break
					}
					if time.Now().After(deadline) {
						t.Errorf("down left process %s running", pid)
						break
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
			if got := liveE2ERead(t, filepath.Join(state, "hosts")); got != "127.0.0.1 localhost\n" {
				t.Errorf("down did not remove only the social hosts entry: %q", got)
			}
			if !strings.Contains(liveE2ERead(t, filepath.Join(state, "sudo.calls")), "kill "+providerPID+"\n") {
				t.Error("down did not stop providerstub through sudo")
			}
		})
	}
}

func TestLiveE2EStubReadinessFailure(t *testing.T) {
	for _, verb := range []string{"enable-social", "start-idp"} {
		t.Run(verb, func(t *testing.T) {
			state, _, run := liveE2EStubFakes(t)
			if out, err := run("enable-sso", state); err != nil {
				t.Fatalf("enable-sso: %v %s", err, out)
			}
			writeExec(t, filepath.Join(state, "fail-stub"), "", 0o600)
			out, err := run(verb, state)
			name := "providerstub"
			if verb == "start-idp" {
				name = "idpstub"
			}
			if err == nil || !strings.Contains(out, name+" did not become ready") || !strings.Contains(out, name+" serve") {
				t.Fatalf("readiness failure did not report the stub and its log: %v %s", err, out)
			}
		})
	}
}

func TestLiveE2ESocialRequiresSudo(t *testing.T) {
	bin := t.TempDir()
	for _, name := range []string{"dirname", "basename"} {
		path, err := exec.LookPath(name)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(path, filepath.Join(bin, name)); err != nil {
			t.Fatal(err)
		}
	}
	cmd := exec.Command("bash", filepath.Join(repoRoot(t), "ci", "live_e2e_auth_stack.sh"), "enable-social", t.TempDir())
	cmd.Env = []string{"PATH=" + bin, "GITHUB_ACTIONS=true"}
	out, err := cmd.CombinedOutput()
	if err == nil || !strings.Contains(string(out), "enable-social requires sudo") {
		t.Fatalf("missing sudo was not explained: %v %s", err, out)
	}
}

func TestLiveE2ESocialRefusesOutsideAGitHubRunnerBeforeAnyWrite(t *testing.T) {
	for _, value := range []string{"", "1", "True", "false", "true "} {
		t.Run("GITHUB_ACTIONS="+value, func(t *testing.T) {
			state, fake, _ := liveE2EStubFakes(t)
			env := []string{"PATH=" + fake + ":" + os.Getenv("PATH"), "FAKE_STATE=" + state, "FAKE_BIN=" + fake}
			if value != "" {
				env = append(env, "GITHUB_ACTIONS="+value)
			}
			hostsBefore := liveE2ERead(t, filepath.Join(state, "hosts"))
			cmd := exec.Command("bash", filepath.Join(repoRoot(t), "ci", "live_e2e_auth_stack.sh"), "enable-social", state)
			cmd.Env = env
			out, err := cmd.CombinedOutput()
			if err == nil || !strings.Contains(string(out), "runs only on a GitHub Actions runner") {
				t.Fatalf("enable-social was not refused: %v %s", err, out)
			}
			if got := liveE2ERead(t, filepath.Join(state, "hosts")); got != hostsBefore {
				t.Errorf("the hosts file changed: %q", got)
			}
			for _, name := range []string{"sudo.calls", "builds", "stub.calls", "providerstub.pid", "extra", "ca-bundle.pem", "bin/providerstub", "social"} {
				if _, statErr := os.Stat(filepath.Join(state, name)); statErr == nil {
					t.Errorf("%s exists: the refusal came after a write", name)
				}
			}
		})
	}
}

func TestLiveE2EProviderStopRefusesUnrelatedOrInvalidPID(t *testing.T) {
	state, _, run := liveE2EStubFakes(t)
	for _, pid := range []string{strconv.Itoa(os.Getpid()), "not-a-pid", "0"} {
		writeExec(t, filepath.Join(state, "providerstub.pid"), pid+"\n", 0o600)
		out, err := run("down", state)
		if (pid == "not-a-pid") != (err != nil) {
			t.Fatalf("down pid %q: %v %s", pid, err, out)
		}
		if raw, _ := os.ReadFile(filepath.Join(state, "sudo.calls")); strings.Contains(string(raw), "kill ") {
			t.Fatal("down tried to kill an unrelated process")
		}
	}
}

func TestLiveE2ESSOFlagsStayOffWithoutAnOverlay(t *testing.T) {
	for _, name := range []string{"AUTH_PROXY_OIDC", "AUTH_PROXY_SAML"} {
		found := false
		for _, row := range liveFlagRows(t) {
			if row.scope == "go-api" && row.name == name {
				found = true
				if row.value != "false" {
					t.Errorf("prod flag table enables %s", name)
				}
			}
		}
		if !found || liveStackEnv(t, "go-api")[name] != "false" {
			t.Errorf("%s is not off without an overlay", name)
		}
	}
}

func TestLiveE2ESocialFixtures(t *testing.T) {
	stub, err := providerstub.LoadDir(filepath.Join(repoRoot(t), "ci", "live_e2e_stub"))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(stub)
	defer server.Close()
	for _, cell := range []struct {
		method, host, path, auth, body, want string
		status                               int
	}{
		{"POST", "github.com", "/login/oauth/access_token", "", "code=test&client_id=gh-client&client_secret=gh-secret", `{"access_token":"gh-live","token_type":"bearer","scope":"read:user,user:email"}`, 200},
		{"GET", "api.github.com", "/user", "Bearer gh-live", "", `{"id":920001,"login":"live-social","name":"Live Social","email":"live-social@example.test"}`, 200},
		{"POST", "api.github.com", "/applications/gh-client/token", "Basic Z2gtY2xpZW50OmdoLXNlY3JldA==", `{"access_token":"gh-live"}`, `{"app":{"client_id":"gh-client"},"user":{"id":920001}}`, 200},
		{"GET", "api.github.com", "/unknown", "", "", "", 599},
		{"GET", "api.github.com", "/user/emails", "Bearer gh-live", "", "", 599},
		{"GET", "api.github.com", "/user", "Bearer wrong", "", "", 599},
		{"POST", "api.github.com", "/applications/gh-client/token", "Basic wrong", `{"access_token":"gh-live"}`, "", 599},
		{"POST", "api.github.com", "/applications/gh-client/token", "Basic Z2gtY2xpZW50OmdoLXNlY3JldA==", `{"access_token":"wrong"}`, "", 599},
	} {
		t.Run(cell.method+cell.path+strconv.Itoa(cell.status), func(t *testing.T) {
			req, err := http.NewRequest(cell.method, server.URL+cell.path, strings.NewReader(cell.body))
			if err != nil {
				t.Fatal(err)
			}
			req.Host = cell.host
			if cell.auth != "" {
				req.Header.Set("Authorization", cell.auth)
			}
			resp, err := server.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != cell.status {
				t.Fatalf("got %d, want %d: %s", resp.StatusCode, cell.status, body)
			}
			if cell.want != "" {
				var got, want any
				if err := json.Unmarshal(body, &got); err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal([]byte(cell.want), &want); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("got %s, want %s", body, cell.want)
				}
			}
		})
	}
}

func TestLiveE2ESSOUsesCallerSettings(t *testing.T) {
	state, fake, _ := liveE2EStubFakes(t)
	cmd := exec.Command("bash", filepath.Join(repoRoot(t), "ci", "live_e2e_auth_stack.sh"), "enable-sso", state)
	cmd.Env = []string{"PATH=" + fake + ":" + os.Getenv("PATH"), "FAKE_STATE=" + state, "FAKE_BIN=" + fake,
		"LIVE_E2E_SETTINGS_ENCRYPTION_KEY=caller-settings-key", "LIVE_E2E_WEB_ORIGIN=http://localhost:3005"}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("enable-sso: %v %s", err, out)
	}
	if strings.Contains(string(out), "caller-settings-key") {
		t.Fatal("enable-sso logged the encryption key")
	}
	loaded := liveE2ERead(t, filepath.Join(state, "auth-service.loaded"))
	for _, want := range []string{"SETTINGS_ENCRYPTION_KEY=caller-settings-key\n", "APP_BASE_URL=http://localhost:3005\n"} {
		if !strings.Contains(loaded, want) {
			t.Errorf("auth-service did not load the caller's %s", strings.Split(want, "=")[0])
		}
	}
}
