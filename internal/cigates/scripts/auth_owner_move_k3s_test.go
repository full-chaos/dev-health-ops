package scripts_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// ci/auth_owner_move_k3s.sh is run by the lead on bigboy k3s, never in CI. These cells hold what can be checked without
// a cluster: the refusals before any cluster call, the --print-plan render of the chart, and the shape of the script
// (secrets on stdin only, no kind, the two SQL files it feeds psql).

const k3sScript = "ci/auth_owner_move_k3s.sh"

var digestA = "registry.example/auth-migrate@sha256:" + strings.Repeat("a", 64)
var digestB = "registry.example/auth-migrate@sha256:" + strings.Repeat("b", 64)
var digestS = "registry.example/auth-service@sha256:" + strings.Repeat("c", 64)

// k3sRun runs the script with a kubectl that refuses every call and a kubeconfig that does not exist, so that no defect of the
// script, and no red run of an unfixed one, can reach the cluster of a host that has /etc/rancher/k3s/k3s.yaml.
func k3sRun(t *testing.T, env []string, args ...string) cmdResult {
	t.Helper()
	root := rootOf(t)
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "kubectl"), []byte("#!/usr/bin/env bash\necho 'kubectl is denied in this cell' >&2\nexit 99\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	base := []string{strings.Replace(shimBasePath(), "PATH=", "PATH="+dir+":", 1), "AUTH_MOVE_KUBECONFIG=" + filepath.Join(dir, "no-such-k3s.yaml")}
	return cmdSpec{dir: root, env: append(base, env...)}.run("bash", append([]string{filepath.Join(root, k3sScript)}, args...)...)
}

func TestAuthOwnerMoveK3sPrintPlanRendersThePinOnlyWhenOwnerRoleIsSet(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}
	res := k3sRun(t, nil, "--print-plan")
	if res.code != 0 {
		t.Fatalf("--print-plan exit %d\n%s\n%s", res.code, res.stdout, res.stderr)
	}
	for _, want := range []string{
		"PASS: plan: the chart renders the pin on the migrate Job when ownerRole is set",
		"PASS: plan: the chart renders no pin when ownerRole is empty (the base state of leg 2)",
		"leg 1 FRESH", "leg 2 UPGRADE", "leg 3 ROLLBACK",
	} {
		if !strings.Contains(res.stdout, want) {
			t.Errorf("--print-plan lacks %q\n%s", want, res.stdout)
		}
	}
}

func TestAuthOwnerMoveK3sRefusesImagesThatAreNotRegistryDigests(t *testing.T) {
	for name, env := range map[string][]string{
		"no images":          nil,
		"a tag":              {"AUTH_MOVE_IMAGE_MIGRATE=registry.example/auth-migrate:ci", "AUTH_MOVE_IMAGE_MIGRATE_PRIOR=" + digestB, "AUTH_MOVE_IMAGE_SERVICE=" + digestS},
		"a short digest":     {"AUTH_MOVE_IMAGE_MIGRATE=registry.example/auth-migrate@sha256:abc", "AUTH_MOVE_IMAGE_MIGRATE_PRIOR=" + digestB, "AUTH_MOVE_IMAGE_SERVICE=" + digestS},
		"a local image name": {"AUTH_MOVE_IMAGE_MIGRATE=auth-proof/auth-migrate:ci", "AUTH_MOVE_IMAGE_MIGRATE_PRIOR=" + digestB, "AUTH_MOVE_IMAGE_SERVICE=" + digestS},
	} {
		t.Run(name, func(t *testing.T) {
			res := k3sRun(t, env)
			if res.code == 0 || !strings.Contains(res.stderr, "is not a registry digest reference") {
				t.Fatalf("exit %d, stderr %q; want a refusal naming the digest rule", res.code, res.stderr)
			}
		})
	}
}

func TestAuthOwnerMoveK3sRefusesAPriorImageEqualToTheTreesAndAKindKubeconfig(t *testing.T) {
	same := []string{"AUTH_MOVE_IMAGE_MIGRATE=" + digestA, "AUTH_MOVE_IMAGE_MIGRATE_PRIOR=" + digestA, "AUTH_MOVE_IMAGE_SERVICE=" + digestS}
	if res := k3sRun(t, same); res.code == 0 || !strings.Contains(res.stderr, "prior auth-migrate image equals the tree's") {
		t.Errorf("equal prior image: exit %d, stderr %q", res.code, res.stderr)
	}
	kind := []string{"AUTH_MOVE_IMAGE_MIGRATE=" + digestA, "AUTH_MOVE_IMAGE_MIGRATE_PRIOR=" + digestB, "AUTH_MOVE_IMAGE_SERVICE=" + digestS, "AUTH_MOVE_KUBECONFIG=/dev/null"}
	if res := k3sRun(t, kind); res.code == 0 || !strings.Contains(res.stderr, "must name a k3s kubeconfig") {
		t.Errorf("non-k3s kubeconfig: exit %d, stderr %q", res.code, res.stderr)
	}
}

func TestAuthOwnerMoveK3sKeepsSecretsOffTheCommandLineAndUsesNoKind(t *testing.T) {
	text := readText(t, filepath.Join(rootOf(t), k3sScript))
	for _, want := range []string{"--from-file=\"$2=/dev/stdin\"", "--from-file=password=/dev/stdin", "kc delete namespace"} {
		if !strings.Contains(text, want) {
			t.Errorf("the script lacks %q", want)
		}
	}
	for _, banned := range []string{"kind create", "kind load", "docker build", "ctr images", "sudo ", "--from-literal", "--password"} {
		if strings.Contains(text, banned) {
			t.Errorf("the script contains %q", banned)
		}
	}
	for _, file := range []string{"ci/auth_k8s/acl_snapshot.sql", "ci/auth_k8s/owner_count.sql"} {
		if !strings.Contains(text, strings.TrimPrefix(file, "ci/")) {
			t.Errorf("the script does not use %s", file)
		}
		if got := readText(t, filepath.Join(rootOf(t), file)); strings.TrimSpace(got) == "" {
			t.Errorf("%s is empty", file)
		}
	}
}

// kubectl got the kubeconfig and helm did not, so the first helm call of a lead run on bigboy reached localhost:8080.
// Every helm call that talks to the cluster names the kubeconfig; `helm template` does not talk to it.
func TestAuthOwnerMoveK3sGivesEveryClusterHelmCallTheKubeconfig(t *testing.T) {
	text := readText(t, filepath.Join(rootOf(t), k3sScript))
	var clusterCalls int
	for n, line := range strings.Split(text, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#") {
			continue
		}
		for _, sub := range []string{"upgrade", "install", "get", "status", "uninstall", "list", "rollback", "history"} {
			if !strings.Contains(trimmed, "helm "+sub+" ") {
				continue
			}
			clusterCalls++
			if !strings.Contains(trimmed, `--kubeconfig "${KUBECONFIG_FILE}"`) {
				t.Errorf("line %d: a helm %s call without --kubeconfig \"${KUBECONFIG_FILE}\": %s", n+1, sub, trimmed)
			}
		}
	}
	if clusterCalls < 2 {
		t.Errorf("found %d cluster helm calls; the cell reads nothing", clusterCalls)
	}
}

// A stub kubectl and helm stand in for the cluster: they log every call and answer from the environment. No cluster is
// contacted; what the script does before and after its first namespace call is what these cells hold.
const k3sStubKubectl = `#!/usr/bin/env bash
echo "$*" >>"$STUB_LOG"
ns_file="$STUB_STATE/ns"
make_ns() {
  # the namespace exists after this call; STUB_CREATE_FOREIGN=1 makes it carry another run's token
  if [ "${STUB_CREATE_FOREIGN:-0}" = 1 ]; then printf 'authmove-run: foreign0token\n' >"$ns_file"; else
    { cat 2>/dev/null; printf '\n'; } >"$ns_file"
  fi
  [ "${STUB_KILL_PARENT_AFTER_CREATE:-0}" = 1 ] && kill -TERM "$PPID"
  exit 0
}
case "$*" in
  *"get nodes"*) printf '%s' "${STUB_NODE_VERSION:-}" ;;
  *"get namespace"*jsonpath*) [ -s "$ns_file" ] || exit 1; sed -n 's/.*authmove-run: *//p' "$ns_file" ;;
  *"get namespace"*) { [ "${STUB_NS_EXISTS:-0}" = 1 ] || [ -s "$ns_file" ]; } && exit 0 || exit 1 ;;
  *"create namespace"* | *"create -f"*) [ "${STUB_CREATE_RC:-0}" = 0 ] || exit "$STUB_CREATE_RC"; make_ns ;;
  *"delete namespace"*) rm -f "$ns_file"; exit 0 ;;
  *) exit 1 ;;
esac
`

func k3sStubRun(t *testing.T, env []string, args ...string) (cmdResult, string) {
	t.Helper()
	for _, tool := range []string{"jq", "openssl", "envsubst", "od"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s is not installed", tool)
		}
	}
	root := rootOf(t)
	dir := t.TempDir()
	for name, body := range map[string]string{"kubectl": k3sStubKubectl, "helm": "#!/usr/bin/env bash\nexit 1\n"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	kubeconfig := filepath.Join(dir, "k3s-test.yaml")
	if err := os.WriteFile(kubeconfig, []byte("apiVersion: v1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "state"), 0o700); err != nil {
		t.Fatal(err)
	}
	log := filepath.Join(dir, "calls.log")
	if err := os.WriteFile(log, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	base := []string{
		strings.Replace(shimBasePath(), "PATH=", "PATH="+dir+":", 1), "STUB_LOG=" + log, "STUB_STATE=" + filepath.Join(dir, "state"), "AUTH_MOVE_KUBECONFIG=" + kubeconfig,
		"AUTH_MOVE_IMAGE_MIGRATE=" + digestA, "AUTH_MOVE_IMAGE_MIGRATE_PRIOR=" + digestB, "AUTH_MOVE_IMAGE_SERVICE=" + digestS,
	}
	res := cmdSpec{dir: root, env: append(base, env...)}.run("bash", append([]string{filepath.Join(root, k3sScript)}, args...)...)
	return res, readText(t, log)
}

func TestAuthOwnerMoveK3sRefusesAClusterThatIsNotK3sBeforeAnyNamespaceCall(t *testing.T) {
	res, calls := k3sStubRun(t, []string{"STUB_NODE_VERSION=v1.30.2"})
	if res.code == 0 || !strings.Contains(res.stderr, "is not a k3s cluster") {
		t.Errorf("a kind-like kubelet version behind a file named k3s: exit %d, stderr %q", res.code, res.stderr)
	}
	if strings.Contains(calls, "create namespace") || strings.Contains(calls, "delete") {
		t.Errorf("the script changed the cluster before refusing:\n%s", calls)
	}
	res, calls = k3sStubRun(t, []string{"STUB_NODE_VERSION=v1.30.2+k3s1"})
	if strings.Contains(res.stderr, "is not a k3s cluster") || !strings.Contains(calls, "create namespace authmove-") {
		t.Errorf("a k3s kubelet version was refused or never reached the namespace: stderr %q, calls:\n%s", res.stderr, calls)
	}
}

func TestAuthOwnerMoveK3sDeletesOnlyTheNamespaceItCreated(t *testing.T) {
	k3s := "STUB_NODE_VERSION=v1.30.2+k3s1"
	res, calls := k3sStubRun(t, []string{k3s, "STUB_NS_EXISTS=1"})
	if res.code == 0 || !strings.Contains(res.stderr, "exists") || strings.Contains(calls, "delete namespace") {
		t.Errorf("a namespace that exists: exit %d, stderr %q, calls:\n%s", res.code, res.stderr, calls)
	}
	_, calls = k3sStubRun(t, []string{k3s, "STUB_CREATE_RC=1"})
	if strings.Contains(calls, "delete namespace") {
		t.Errorf("a failed create was followed by a delete:\n%s", calls)
	}
	_, calls = k3sStubRun(t, []string{k3s})
	if !strings.Contains(calls, "create namespace authmove-") || !strings.Contains(calls, "delete namespace authmove-") {
		t.Errorf("a namespace the run created was not deleted on failure:\n%s", calls)
	}
}

// The namespace can exist before the script has recorded that it created it (a signal between the API call and the
// next statement). The run owns a namespace by a token it put on it at creation, never by a variable set afterwards.
func TestAuthOwnerMoveK3sDeletesItsNamespaceWhenTerminatedRightAfterCreate(t *testing.T) {
	k3s := "STUB_NODE_VERSION=v1.30.2+k3s1"
	res, calls := k3sStubRun(t, []string{k3s, "STUB_KILL_PARENT_AFTER_CREATE=1"})
	if res.code != 143 {
		t.Errorf("exit %d, want 143 (the termination); stderr %q", res.code, res.stderr)
	}
	if !strings.Contains(calls, "delete namespace authmove-") {
		t.Errorf("terminated after the namespace was created, and it was not deleted:\n%s", calls)
	}
}

func TestAuthOwnerMoveK3sNeverDeletesANamespaceThatCarriesAnotherRunsToken(t *testing.T) {
	_, calls := k3sStubRun(t, []string{"STUB_NODE_VERSION=v1.30.2+k3s1", "STUB_CREATE_FOREIGN=1"})
	if !strings.Contains(calls, "authmove-") {
		t.Fatalf("the script never reached the namespace:\n%s", calls)
	}
	if strings.Contains(calls, "delete namespace") {
		t.Errorf("deleted a namespace whose token is not this run's:\n%s", calls)
	}
}

func TestAuthOwnerMoveK3sRefusesMalformedImagePathsAndTheSameDigestUnderAMirror(t *testing.T) {
	good := "registry.example/auth-migrate@sha256:" + strings.Repeat("a", 64)
	mirror := "mirror.example/other/auth-migrate@sha256:" + strings.Repeat("a", 64)
	slash := "registry.example//auth-migrate@sha256:" + strings.Repeat("a", 64)
	for name, env := range map[string][]string{
		"double slash, migrate": {"AUTH_MOVE_IMAGE_MIGRATE=" + slash, "AUTH_MOVE_IMAGE_MIGRATE_PRIOR=" + digestB, "AUTH_MOVE_IMAGE_SERVICE=" + digestS},
		"double slash, prior":   {"AUTH_MOVE_IMAGE_MIGRATE=" + digestA, "AUTH_MOVE_IMAGE_MIGRATE_PRIOR=" + slash, "AUTH_MOVE_IMAGE_SERVICE=" + digestS},
		"double slash, service": {"AUTH_MOVE_IMAGE_MIGRATE=" + digestA, "AUTH_MOVE_IMAGE_MIGRATE_PRIOR=" + digestB, "AUTH_MOVE_IMAGE_SERVICE=" + slash},
		"trailing slash":        {"AUTH_MOVE_IMAGE_MIGRATE=registry.example/@sha256:" + strings.Repeat("a", 64), "AUTH_MOVE_IMAGE_MIGRATE_PRIOR=" + digestB, "AUTH_MOVE_IMAGE_SERVICE=" + digestS},
	} {
		t.Run(name, func(t *testing.T) {
			res := k3sRun(t, env)
			if res.code == 0 || !strings.Contains(res.stderr, "is not a registry digest reference") {
				t.Fatalf("exit %d, stderr %q; want a refusal naming the digest rule", res.code, res.stderr)
			}
		})
	}
	res := k3sRun(t, []string{"AUTH_MOVE_IMAGE_MIGRATE=" + good, "AUTH_MOVE_IMAGE_MIGRATE_PRIOR=" + mirror, "AUTH_MOVE_IMAGE_SERVICE=" + digestS})
	if res.code == 0 || !strings.Contains(res.stderr, "prior auth-migrate image equals the tree's") {
		t.Errorf("the same digest under another repository: exit %d, stderr %q", res.code, res.stderr)
	}
}

func TestAuthOwnerMoveK3sHasNoKeepFlag(t *testing.T) {
	if res := k3sRun(t, nil, "--keep"); res.code == 0 || !strings.Contains(res.stderr, "usage:") {
		t.Errorf("--keep: exit %d, stderr %q; want it refused as an unknown argument", res.code, res.stderr)
	}
}
