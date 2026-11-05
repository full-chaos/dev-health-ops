package scripts_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// ci/auth_flipback_sso_k3s.sh is run by the lead on bigboy k3s, never in CI. These cells hold what can be checked without a
// cluster: the refusals before any change, the --print-plan render of every chart step, the order of the steps, and the
// shape of the script (secrets off the command line, a namespace-scoped kubeconfig only, nothing that makes or deletes a
// namespace, no JIT switch outside the JIT values layer).

const fbScript = "ci/auth_flipback_sso_k3s.sh"

var (
	fbDho      = "registry.example/dev-health-go-dho@sha256:" + strings.Repeat("a", 64)
	fbService  = "registry.example/auth-service@sha256:" + strings.Repeat("b", 64)
	fbMigrate  = "registry.example/auth-migrate@sha256:" + strings.Repeat("c", 64)
	fbIdp      = "idpstub-fb/idpstub:run0123456789abcd"
	fbNS       = "authfb-0a1b2c3d"
	fbToken    = "0123456789abcdef"
	fbPodCIDR  = "10.42.0.0/24"
	fbGoodEnvs = []string{
		"AUTH_FB_IMAGE_DHO=" + fbDho, "AUTH_FB_IMAGE_SERVICE=" + fbService, "AUTH_FB_IMAGE_MIGRATE=" + fbMigrate,
		"AUTH_FB_IMAGE_IDPSTUB=" + fbIdp, "AUTH_FB_NAMESPACE=" + fbNS, "AUTH_FB_RUN_TOKEN=" + fbToken,
		"AUTH_FB_POD_CIDR=" + fbPodCIDR,
	}
)

// without returns env minus the entry named name.
func without(env []string, name string) []string {
	var out []string
	for _, e := range env {
		if !strings.HasPrefix(e, name+"=") {
			out = append(out, e)
		}
	}
	return out
}

// with returns env with the entry named by the prefix of kv replaced by kv.
func with(env []string, kv string) []string {
	name := kv[:strings.Index(kv, "=")]
	return append(without(env, name), kv)
}

const fbStubKubectl = `#!/usr/bin/env bash
echo "kubectl $*" >>"$STUB_LOG"
case "$*" in
  *"auth can-i"*) printf '%s\n' "${STUB_CAN_I:-no}" ;;
  *"version -o json"*) printf '{"serverVersion":{"gitVersion":"%s"}}\n' "${STUB_GIT_VERSION:-v1.36.4+k3s1}" ;;
  *"get configmap fb-run"*) [ -n "${STUB_TOKEN:-}" ] && printf '%s' "$STUB_TOKEN" || exit 1 ;;
  *) [ -t 0 ] || cat >/dev/null; [ "${STUB_PERMISSIVE:-0}" = 1 ] && exit 0 || exit 1 ;;
esac
`

// fbRun runs the script with a kubectl and a helm stub in front of PATH: a defect of the script, or a red run of an unfixed
// one, cannot reach the cluster of a host that has /etc/rancher/k3s/k3s.yaml. The stubs log every call.
func fbRun(t *testing.T, env []string, args ...string) (cmdResult, string) {
	t.Helper()
	for _, tool := range []string{"jq", "openssl", "od"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s is not installed", tool)
		}
	}
	root := rootOf(t)
	dir := t.TempDir()
	for name, body := range map[string]string{"kubectl": fbStubKubectl, "helm": "#!/usr/bin/env bash\necho \"helm $*\" >>\"$STUB_LOG\"\nexit 1\n"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	kubeconfig := filepath.Join(dir, "sa.yaml")
	if err := os.WriteFile(kubeconfig, []byte("apiVersion: v1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	log := filepath.Join(dir, "calls.log")
	if err := os.WriteFile(log, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	work := filepath.Join(dir, "work")
	path := strings.Replace(shimBasePath(), "PATH=", "PATH="+dir+":", 1)
	for _, a := range args {
		if a == "--print-plan" { // the plan renders the chart with the real helm and touches no cluster
			path = shimBasePathWith(t, "", "helm")
		}
	}
	base := []string{path, "STUB_LOG=" + log, "AUTH_FB_KUBECONFIG=" + kubeconfig, "AUTH_FB_WORK=" + work}
	res := cmdSpec{dir: root, env: append(base, env...)}.run("bash", append([]string{filepath.Join(root, fbScript)}, args...)...)
	return res, readText(t, log)
}

func TestFlipbackSSOK3sRefusesBadInputsBeforeAnyClusterCall(t *testing.T) {
	cases := map[string]struct {
		env  []string
		want string
	}{
		"no images": {without(without(without(fbGoodEnvs, "AUTH_FB_IMAGE_DHO"), "AUTH_FB_IMAGE_SERVICE"), "AUTH_FB_IMAGE_MIGRATE"), "is not a registry digest reference"},
		"a tag":     {with(fbGoodEnvs, "AUTH_FB_IMAGE_DHO=registry.example/dho:ci"), "is not a registry digest reference"},
		"short":     {with(fbGoodEnvs, "AUTH_FB_IMAGE_SERVICE=registry.example/auth-service@sha256:abc"), "is not a registry digest reference"},
		"double slash": {with(fbGoodEnvs, "AUTH_FB_IMAGE_MIGRATE=registry.example//m@sha256:"+strings.Repeat("c", 64)),
			"is not a registry digest reference"},
		"idpstub from a registry": {with(fbGoodEnvs, "AUTH_FB_IMAGE_IDPSTUB=registry.example/idpstub:ci"), "AUTH_FB_IMAGE_IDPSTUB must be a local import ref"},
		"idpstub latest":          {with(fbGoodEnvs, "AUTH_FB_IMAGE_IDPSTUB=idpstub-fb/idpstub:latest"), "AUTH_FB_IMAGE_IDPSTUB must be a local import ref"},
		"namespace dev-health":    {with(fbGoodEnvs, "AUTH_FB_NAMESPACE=dev-health"), "AUTH_FB_NAMESPACE must be authfb-<8 hex>"},
		"namespace default":       {with(fbGoodEnvs, "AUTH_FB_NAMESPACE=default"), "AUTH_FB_NAMESPACE must be authfb-<8 hex>"},
		"token short":             {with(fbGoodEnvs, "AUTH_FB_RUN_TOKEN=abc"), "AUTH_FB_RUN_TOKEN must be 16 lowercase hex"},
		"pod cidr wide":           {with(fbGoodEnvs, "AUTH_FB_POD_CIDR=10.0.0.0/8"), "AUTH_FB_POD_CIDR must be an IPv4 /24"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			res, calls := fbRun(t, c.env)
			if res.code == 0 || !strings.Contains(res.stderr, c.want) {
				t.Fatalf("exit %d, stderr %q; want a refusal naming %q", res.code, res.stderr, c.want)
			}
			if strings.TrimSpace(calls) != "" {
				t.Errorf("a refusal was preceded by a cluster call:\n%s", calls)
			}
		})
	}
}

func TestFlipbackSSOK3sRefusesTheHostDefaultKubeconfigAndAMissingOne(t *testing.T) {
	res, calls := fbRun(t, with(fbGoodEnvs, "AUTH_FB_KUBECONFIG=/etc/rancher/k3s/k3s.yaml"))
	if res.code == 0 || !strings.Contains(res.stderr, "refusing the host default kubeconfig") || strings.TrimSpace(calls) != "" {
		t.Errorf("host default: exit %d, stderr %q, calls %q", res.code, res.stderr, calls)
	}
	res, _ = fbRun(t, with(fbGoodEnvs, "AUTH_FB_KUBECONFIG=/nonexistent/sa.yaml"))
	if res.code == 0 || !strings.Contains(res.stderr, "is not readable") {
		t.Errorf("missing kubeconfig: exit %d, stderr %q", res.code, res.stderr)
	}
	res, _ = fbRun(t, without(fbGoodEnvs, "AUTH_FB_NAMESPACE"))
	if res.code == 0 || !strings.Contains(res.stderr, "AUTH_FB_NAMESPACE must be authfb-<8 hex>") {
		t.Errorf("unset namespace: exit %d, stderr %q", res.code, res.stderr)
	}
}

func TestFlipbackSSOK3sRefusesAKubeconfigThatIsNotNamespaceScopedOrNotThisRuns(t *testing.T) {
	res, calls := fbRun(t, append(append([]string{}, fbGoodEnvs...), "STUB_CAN_I=yes", "STUB_TOKEN="+fbToken))
	if res.code == 0 || !strings.Contains(res.stderr, "kubeconfig is not namespace scoped") || strings.Contains(calls, "helm ") {
		t.Errorf("an admin-capable kubeconfig: exit %d, stderr %q, calls:\n%s", res.code, res.stderr, calls)
	}
	res, calls = fbRun(t, append(append([]string{}, fbGoodEnvs...), "STUB_TOKEN=ffffffffffffffff"))
	if res.code == 0 || !strings.Contains(res.stderr, "does not carry this run's token") || strings.Contains(calls, "helm ") {
		t.Errorf("another run's token: exit %d, stderr %q, calls:\n%s", res.code, res.stderr, calls)
	}
	// The next call is the one that passes every check of the script: a namespace scoped kubeconfig (can-i answers no) that carries
	// this run's token. STUB_PERMISSIVE lets the stub kubectl answer the install steps, so the install must be reached and the
	// script must not name the token in its refusal text. The token is a fixture, 16 hex digits, written into the stub kubeconfig
	// only; the .gitleaksignore rows for this file name the fixture const and this call by line.
	res, calls = fbRun(t, append(append([]string{}, fbGoodEnvs...), "STUB_TOKEN="+fbToken, "STUB_PERMISSIVE=1"))
	if strings.Contains(res.stderr, "token") || !strings.Contains(calls, "helm upgrade --install") {
		t.Errorf("a scoped kubeconfig with this run's token never reached the install: stderr %q, calls:\n%s", res.stderr, calls)
	}
	for _, line := range strings.Split(calls, "\n") {
		if strings.HasPrefix(line, "kubectl") && !strings.Contains(line, "--kubeconfig") {
			t.Errorf("a kubectl call without the kubeconfig flag: %s", line)
		}
		if strings.HasPrefix(line, "helm upgrade") && !strings.Contains(line, "--kubeconfig") {
			t.Errorf("a helm call without the kubeconfig flag: %s", line)
		}
	}
}

// stepNames are the plan lines of the script, in the order the rehearsal runs them.
var fbSteps = []string{
	"S0 install at phase 3",
	"S1 admin and organization",
	"S2 the SAML provider row, one INSERT in the rehearsal namespace",
	"S3 SSO sign-in at phase 3 creates the SAML user",
	"S4 flip 3 to 4: U1 go-api forwards, barrier up-go-api",
	"S5 users-backfill and users-reconcile",
	"S6 barrier up-owned, U3 owned",
	"S7 SSO sign-in under owned finds the same accounts",
	"S8 flip back 4 to 3: D1, barrier down-auth, D2",
	"S9 SSO sign-in through go-api finds the same accounts",
}

func TestFlipbackSSOK3sPrintPlanListsTheStepsInOrderAndRendersEveryChartStep(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}
	res, _ := fbRun(t, nil, "--print-plan")
	if res.code != 0 {
		t.Fatalf("--print-plan exit %d\n%s\n%s", res.code, res.stdout, res.stderr)
	}
	at := 0
	for _, step := range fbSteps {
		i := strings.Index(res.stdout[at:], "plan: "+step)
		if i < 0 {
			t.Fatalf("--print-plan lacks %q after offset %d\n%s", step, at, res.stdout)
		}
		at += i + 1
	}
	for _, want := range []string{
		"PASS: plan: phase 3 renders go-api forwarding login and auth-service not owned",
		"PASS: plan: U1 renders go-api owned and forwarding credentials, auth-service not owned",
		"PASS: plan: U3 renders auth-service owned",
		"PASS: plan: D1 renders stage goApi with the narrowing deferred",
		"PASS: plan: D2 renders phase 3 with neither go-api nor auth-service owned",
		"PASS: plan: no step renders AUTH_SSO_JIT",
		"PASS: plan: the JIT layer renders AUTH_SSO_JIT on auth-service only, with both users switches on",
	} {
		if !strings.Contains(res.stdout, want) {
			t.Errorf("--print-plan lacks %q\n%s", want, res.stdout)
		}
	}
}

func TestFlipbackSSOK3sPrintPlanNeedsNoClusterInput(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}
	res, calls := fbRun(t, []string{"AUTH_FB_KUBECONFIG=/nonexistent/sa.yaml"}, "--print-plan")
	if res.code != 0 || strings.Contains(res.stderr, "is not readable") || strings.Contains(calls, "kubectl") {
		t.Errorf("--print-plan touched the kubeconfig or kubectl: stderr %q, calls:\n%s", res.stderr, calls)
	}
}

// The live phase 4 step is read-only first (pm 2026-11-02): count the prod providers and the SSO-linked users, create nothing.
func TestFlipbackSSOK3sLiveCountModeIsReadOnlyAndNeedsNoCluster(t *testing.T) {
	res, calls := fbRun(t, nil, "--print-live-count-sql")
	if res.code != 0 || strings.TrimSpace(calls) != "" {
		t.Fatalf("exit %d, calls %q, stderr %q", res.code, calls, res.stderr)
	}
	for _, want := range []string{"FROM public.sso_providers", "auth_provider IN ('oidc', 'saml')", "sso_provider_id IS NOT NULL", "BEGIN READ ONLY", "ROLLBACK"} {
		if !strings.Contains(res.stdout, want) {
			t.Errorf("the count SQL lacks %q\n%s", want, res.stdout)
		}
	}
	for _, banned := range []string{"INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER"} {
		if strings.Contains(strings.ToUpper(res.stdout), banned) {
			t.Errorf("the count SQL holds %s", banned)
		}
	}
}

func TestFlipbackSSOK3sHasNoKeepFlag(t *testing.T) {
	if res, _ := fbRun(t, fbGoodEnvs, "--keep"); res.code == 0 || !strings.Contains(res.stderr, "usage:") {
		t.Errorf("--keep: exit %d, stderr %q; want it refused as an unknown argument", res.code, res.stderr)
	}
}

func TestFlipbackSSOK3sShape(t *testing.T) {
	text := readText(t, filepath.Join(rootOf(t), fbScript))
	for _, want := range []string{"--from-file=\"$2=/dev/stdin\"", "auth-users-owned-barrier.sh", "up-go-api", "up-owned", "down-auth", "users-backfill", "users-reconcile",
		"INSERT INTO sso_providers"} {
		if !strings.Contains(text, want) {
			t.Errorf("the script lacks %q", want)
		}
	}
	for _, banned := range []string{"kind create", "kind load", "docker ", "ctr ", "sudo ", "--from-literal", "--password ", "name: AUTH_SSO_JIT",
		"create namespace", "delete namespace", "pkill", "killall", "nohup", "/api/v1/auth/sso/providers", "UPDATE public", "DELETE FROM", "externalIPs", "IDP_IP", "/oidc/", "oidc_config"} {
		if strings.Contains(text, banned) {
			t.Errorf("the script contains %q", banned)
		}
	}
	if n := strings.Count(text, "authService.ssoJit=true"); n != 1 {
		t.Errorf("the script sets authService.ssoJit=true %d times; want only the JIT values layer", n)
	}
	if n := strings.Count(text, "INSERT INTO"); n != 1 {
		t.Errorf("the script holds %d INSERT statements; want exactly the one sso_providers row of the rehearsal namespace", n)
	}
	// the one time the host default path may appear is the refusal
	if n := strings.Count(text, "/etc/rancher/k3s/k3s.yaml"); n != 1 {
		t.Errorf("the host default kubeconfig path appears %d times; want 1 (the refusal)", n)
	}
	var clusterCalls int
	for n, line := range strings.Split(text, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#") {
			continue
		}
		for _, sub := range []string{"upgrade", "install", "get", "status", "uninstall", "list", "rollback", "history"} {
			if strings.Contains(trimmed, "helm "+sub+" ") {
				clusterCalls++
				if !strings.Contains(trimmed, `--kubeconfig "${KUBECONFIG_FILE}"`) {
					t.Errorf("line %d: a helm %s call without --kubeconfig \"${KUBECONFIG_FILE}\": %s", n+1, sub, trimmed)
				}
			}
		}
		if regexp.MustCompile(`\bkubectl\s`).MatchString(trimmed) && !strings.Contains(trimmed, "--kubeconfig") && !strings.Contains(trimmed, "command -v") &&
			!strings.Contains(trimmed, "printf") && !strings.Contains(trimmed, "die ") && !strings.HasPrefix(trimmed, "for tool in") {
			t.Errorf("line %d: a kubectl call without the kubeconfig flag: %s", n+1, trimmed)
		}
	}
	if clusterCalls < 1 {
		t.Errorf("found %d cluster helm calls; the cell reads nothing", clusterCalls)
	}
}

func TestFlipbackSSORunbookCarriesTheStepLinesAndTheJITScope(t *testing.T) {
	text := readText(t, filepath.Join(rootOf(t), "docs/operate/runbooks/auth-users-owned-flip-and-rollback.md"))
	for _, want := range []string{
		"ci/auth_flipback_sso_k3s.sh",
		"SAML user created through go-api at phase 3",
		"flip back 4 to 3",
		"sign in through go-api",
		"JIT values layer",
		"OIDC leg stays in the integration tests",
		"count the SSO providers and the SSO-linked users",
		"not applicable at 11505",
		"creates nothing",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("the runbook lacks %q", want)
		}
	}
}

// The port-forward is a long-lived child. Started through the kc shell function it is a subshell: kill of $! leaves the kubectl
// behind, and that kubectl holds fd 9 of the heavy slot lock. So it is a direct kubectl command with fd 9 closed, and the stop
// waits for it.
func TestFlipbackSSOK3sPortForwardIsADirectKubectlWithoutTheSlotFd(t *testing.T) {
	text := readText(t, filepath.Join(rootOf(t), fbScript))
	var found int
	for n, line := range strings.Split(text, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#") || !strings.Contains(trimmed, "port-forward") || strings.HasPrefix(trimmed, "die ") || strings.Contains(trimmed, "did not start") {
			continue
		}
		found++
		if !strings.HasPrefix(trimmed, `kubectl --kubeconfig "${KUBECONFIG_FILE}"`) || !strings.Contains(trimmed, "9>&-") || !strings.HasSuffix(trimmed, "&") {
			t.Errorf("line %d: the port-forward must be a direct kubectl command with 9>&- run in the background: %s", n+1, trimmed)
		}
	}
	if found != 1 {
		t.Errorf("found %d port-forward start lines; want 1", found)
	}
	if !strings.Contains(text, `wait "${pid}"`) {
		t.Errorf("pf_stop does not wait for the killed port-forward")
	}
}

func TestFlipbackSSOK3sLiveStepsKeepTheFixesOfTheFirstRuns(t *testing.T) {
	text := readText(t, filepath.Join(rootOf(t), fbScript))
	for _, want := range []string{
		// go-api's session issue token needs an auth-session-issuer row of internal_service_credentials, held in its own Secret
		"service-credentials create --service auth-session-issuer --scope sessions:issue",
		`K wait --for=jsonpath='{.status.phase}'=Succeeded pod/fb-issue-cred`,
		"secretName: session-issue",
		"put_secret session-issue AUTH_SESSION_ISSUE_TOKEN",
		// the verb reads the owner DSN, not the API login
		`name: "MIGRATION_DATABASE_URI"`,
		// RELEASE is readonly: the barrier gets it through env, after the superseded pods are gone
		"barrier() { no_terminating_pods; env RELEASE=",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("the script lacks %q", want)
		}
	}
	if strings.Contains(text, "AUTH_SESSION_ISSUE_TOKEN: \"%s\"") {
		t.Errorf("the secrets values still carry a random AUTH_SESSION_ISSUE_TOKEN go-api cannot present")
	}
}

func TestFlipbackSSOK3sRefusesAServerThatIsNotK3sBeforeAnyWrite(t *testing.T) {
	res, calls := fbRun(t, append(append([]string{}, fbGoodEnvs...), "STUB_TOKEN="+fbToken, "STUB_PERMISSIVE=1", "STUB_GIT_VERSION=v1.30.2")) // gitleaks:allow
	if res.code == 0 || !strings.Contains(res.stderr, "the cluster is not k3s") || strings.Contains(calls, "helm ") || strings.Contains(calls, "create secret") {
		t.Errorf("a kind-like server: exit %d, stderr %q, calls:\n%s", res.code, res.stderr, calls)
	}
}

func TestFlipbackSSOK3sSignInsMustAnswerTheSavedSSOUser(t *testing.T) {
	text := readText(t, filepath.Join(rootOf(t), fbScript))
	for _, want := range []string{`die "S3: the sign-in answered user_id`, `die "S7: the sign-in under owned answered user_id`, `die "S9: the sign-in after the flip back answered user_id`,
		`die "S1: the admin user verb failed"`, `die "S1: the admin organization verb failed"`} {
		if !strings.Contains(text, want) {
			t.Errorf("the script lacks %q", want)
		}
	}
}
