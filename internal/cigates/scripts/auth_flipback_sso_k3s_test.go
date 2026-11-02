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
// namespace, no JIT switch).

const fbScript = "ci/auth_flipback_sso_k3s.sh"

var (
	fbDho      = "registry.example/dev-health-go-dho@sha256:" + strings.Repeat("a", 64)
	fbService  = "registry.example/auth-service@sha256:" + strings.Repeat("b", 64)
	fbMigrate  = "registry.example/auth-migrate@sha256:" + strings.Repeat("c", 64)
	fbIdp      = "idpstub-fb/idpstub:run0123456789abcd"
	fbNS       = "authfb-0a1b2c3d"
	fbToken    = "0123456789abcdef"
	fbPodCIDR  = "10.42.0.0/24"
	fbIdpIP    = "11.0.0.1"
	fbGoodEnvs = []string{
		"AUTH_FB_IMAGE_DHO=" + fbDho, "AUTH_FB_IMAGE_SERVICE=" + fbService, "AUTH_FB_IMAGE_MIGRATE=" + fbMigrate,
		"AUTH_FB_IMAGE_IDPSTUB=" + fbIdp, "AUTH_FB_NAMESPACE=" + fbNS, "AUTH_FB_RUN_TOKEN=" + fbToken,
		"AUTH_FB_POD_CIDR=" + fbPodCIDR, "AUTH_FB_IDP_IP=" + fbIdpIP,
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
  *"get configmap fb-run"*) [ -n "${STUB_TOKEN:-}" ] && printf '%s' "$STUB_TOKEN" || exit 1 ;;
  *) [ "${STUB_PERMISSIVE:-0}" = 1 ] && exit 0 || exit 1 ;;
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
			path = shimBasePath()
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
		"idp ip private":          {with(fbGoodEnvs, "AUTH_FB_IDP_IP=10.43.0.9"), "AUTH_FB_IDP_IP must be a global IPv4"},
		"idp ip loopback":         {with(fbGoodEnvs, "AUTH_FB_IDP_IP=127.0.0.1"), "AUTH_FB_IDP_IP must be a global IPv4"},
		"idp ip shared space":     {with(fbGoodEnvs, "AUTH_FB_IDP_IP=100.64.0.5"), "AUTH_FB_IDP_IP must be a global IPv4"},
		"idp ip docs range":       {with(fbGoodEnvs, "AUTH_FB_IDP_IP=203.0.113.7"), "AUTH_FB_IDP_IP must be a global IPv4"},
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
	"S2 provider rows through the go-api admin route",
	"S3 SSO sign-in at phase 3 creates the OIDC and SAML users",
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

func TestFlipbackSSOK3sHasNoKeepFlag(t *testing.T) {
	if res, _ := fbRun(t, fbGoodEnvs, "--keep"); res.code == 0 || !strings.Contains(res.stderr, "usage:") {
		t.Errorf("--keep: exit %d, stderr %q; want it refused as an unknown argument", res.code, res.stderr)
	}
}

func TestFlipbackSSOK3sShape(t *testing.T) {
	text := readText(t, filepath.Join(rootOf(t), fbScript))
	for _, want := range []string{"--from-file=\"$2=/dev/stdin\"", "auth-users-owned-barrier.sh", "up-go-api", "up-owned", "down-auth", "users-backfill", "users-reconcile",
		"/api/v1/auth/sso/providers"} {
		if !strings.Contains(text, want) {
			t.Errorf("the script lacks %q", want)
		}
	}
	for _, banned := range []string{"kind create", "kind load", "docker ", "ctr ", "sudo ", "--from-literal", "--password ", "authService.ssoJit", "ssoJit=", "name: AUTH_SSO_JIT",
		"create namespace", "delete namespace", "pkill", "killall", "nohup", "INSERT INTO", "UPDATE public", "DELETE FROM"} {
		if strings.Contains(text, banned) {
			t.Errorf("the script contains %q", banned)
		}
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
		"OIDC and SAML users created through go-api at phase 3",
		"flip back 4 to 3",
		"sign in through go-api",
		"CHAOS-10998",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("the runbook lacks %q", want)
		}
	}
}
