package workflows_test

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
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
		apiservice.AuthCredentials: false, apiservice.AuthOIDC: false, apiservice.AuthSAML: false,
	} {
		if cfg.Enabled(agg) != want {
			t.Errorf("FAIL: go-api %s enabled=%v, prod %v", agg, cfg.Enabled(agg), want)
		}
	}
	if cfg.OrgProvisioningOn() {
		t.Error("FAIL: AUTH_PROXY_ORG_PROVISIONING is on; prod has it off")
	}
	for _, name := range []string{"AUTH_USERS_OWNED", "AUTH_SESSION_ISSUE_TOKEN", "AUTH_WORKLOAD_SUBJECT_FILE"} {
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

func TestLiveE2EQueryAPIEnvIsProdsFlagSet(t *testing.T) {
	env := liveStackEnv(t, "query-api")
	if env["AUTH_PROXY_SESSIONS"] != "true" || env["AUTH_ACCESS_TOKEN_ISSUER"] == "" {
		t.Fatalf("FAIL: query-api env %v lacks the sessions flag or the access token issuer", env)
	}
}

// The web workflow runs the script by a path relative to the ops checkout. `up` changes directory into the state
// directory before it starts auth-service through `with-env`, so that re-exec has to name the script by an absolute path.
func TestLiveE2EAuthStackUpStartsAuthServiceWhenRunByARelativePath(t *testing.T) {
	root := repoRoot(t)
	fake := t.TempDir()
	write := func(path, body string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	write(filepath.Join(fake, "go"), "#!/bin/sh\nwhile [ \"$1\" != -o ]; do shift; done\nprintf '#!/bin/sh\\necho started\\n' > \"$2\"\nchmod 755 \"$2\"\n")
	write(filepath.Join(fake, "psql"), "#!/bin/sh\ncase \"$*\" in *-At*) echo 3b1e2c4d-5f60-4a71-8b92-0c1d2e3f4a5b ;; esac\n")
	write(filepath.Join(fake, "sleep"), "#!/bin/sh\nexit 0\n")
	state := filepath.Join(t.TempDir(), "state")
	write(filepath.Join(fake, "curl"), "#!/bin/sh\ngrep -q '^started$' '"+state+"/logs/auth-service.log'\n")
	write(filepath.Join(fake, "dho"), "#!/bin/sh\nexit 0\n")

	cmd := exec.Command("bash", "ci/live_e2e_auth_stack.sh", "up", state)
	cmd.Dir = root
	cmd.Env = []string{
		"PATH=" + fake + ":" + os.Getenv("PATH"),
		"PGUSER=postgres",
		"PGPASSWORD=placeholder-for-fake-psql",                // gitleaks:allow -- placeholder value for the fake psql, not a credential
		"JWT_SECRET_KEY=0123456789abcdef0123456789abcdef0123", // gitleaks:allow -- placeholder value fed to the fakes, not a credential
		"DHO_BIN=" + filepath.Join(fake, "dho"),
	}
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Run(); err != nil {
		logged, _ := os.ReadFile(filepath.Join(state, "logs", "auth-service.log"))
		t.Fatalf("FAIL: up by a relative path did not start auth-service: %v\n%s\nauth-service.log:\n%s", err, out.String(), logged)
	}
	// up binds the web workload, writes the web subject and the reviewer token, and saves the auth-service environment
	// (mode 0600) that start-auth reuses.
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
	if !strings.Contains(out.String(), "step 5b") || !strings.Contains(out.String(), "step 6b") {
		t.Errorf("FAIL: up skipped the web binding or the fake TokenReview step:\n%s", out.String())
	}
}

// stop-auth ends auth-service by pid (the outage cell); start-auth brings it back with the saved environment.
func TestLiveE2EStopAndStartAuthService(t *testing.T) {
	root := repoRoot(t)
	fake := t.TempDir()
	state := t.TempDir()
	for _, d := range []string{"bin", "logs"} {
		if err := os.Mkdir(filepath.Join(state, d), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	write := func(path, body string, mode os.FileMode) {
		t.Helper()
		if err := os.WriteFile(path, []byte(body), mode); err != nil {
			t.Fatal(err)
		}
	}
	write(filepath.Join(state, "bin", "auth-service"), "#!/bin/sh\necho \"$MARK\" >> \""+state+"/logs/starts\"\nexec sleep 60\n", 0o755)
	write(filepath.Join(state, "auth-service.env"), "MARK=started-with-saved-env\n", 0o600)
	// readyz answers while the pid of the pid file is alive.
	write(filepath.Join(fake, "curl"), "#!/bin/sh\nkill -0 \"$(cat '"+state+"/auth-service.pid')\" 2>/dev/null\n", 0o755)
	run := func(verb string) {
		t.Helper()
		cmd := exec.Command("bash", "ci/live_e2e_auth_stack.sh", verb, state)
		cmd.Dir = root
		cmd.Env = []string{"PATH=" + fake + ":" + os.Getenv("PATH")}
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("FAIL: %s: %v\n%s", verb, err, out)
		}
	}
	t.Cleanup(func() { run("down") })
	run("start-auth")
	run("stop-auth")
	run("start-auth")
	starts, _ := os.ReadFile(filepath.Join(state, "logs", "starts"))
	if strings.Count(string(starts), "started-with-saved-env") != 2 {
		t.Errorf("FAIL: auth-service started with the saved environment %d times, want 2:\n%s", strings.Count(string(starts), "started-with-saved-env"), starts)
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
