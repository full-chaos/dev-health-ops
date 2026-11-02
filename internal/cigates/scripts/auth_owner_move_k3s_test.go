package scripts_test

import (
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

func k3sRun(t *testing.T, env []string, args ...string) cmdResult {
	t.Helper()
	root := rootOf(t)
	return cmdSpec{dir: root, env: append([]string{shimBasePath()}, env...)}.run("bash", append([]string{filepath.Join(root, k3sScript)}, args...)...)
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
