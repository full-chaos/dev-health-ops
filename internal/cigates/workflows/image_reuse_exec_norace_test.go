//go:build !race

package workflows_test

import (
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// These tests run the real ci/check_go_containers.sh against a stand-in docker that only logs its arguments and
// answers `image inspect --format {{.Id}} <tag>` with a fixed id per tag. ci/race_excluded_tests.tsv runs each one
// without -race. Nothing here starts a container or builds an image.

const imageReuseStubDocker = `#!/bin/sh
echo "$@" >>"$STUB_LOG"
if [ "$1 $2" = "image inspect" ]; then
  for last; do :; done
  case " $STUB_MISSING " in *" $last "*) exit 1 ;; esac
  id="$(printf '%s' "$last" | sha256sum | cut -d' ' -f1)"
  printf 'sha256:%s\n' "$id"
fi
exit 0
`

func stubImageID(tag string) string { return fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(tag))) }

func expectedImageIDs(targets []string, override map[string]string) string {
	var pairs []string
	for _, target := range targets {
		id := stubImageID("dev-health-go-" + target + ":ci")
		if v, ok := override[target]; ok {
			id = v
		}
		pairs = append(pairs, target+"="+id)
	}
	return strings.Join(pairs, ",")
}

func runGoContainersScript(t *testing.T, env []string, args ...string) (string, string, error) {
	t.Helper()
	bin := t.TempDir()
	log := filepath.Join(bin, "docker.log")
	if err := os.WriteFile(filepath.Join(bin, "docker"), []byte(imageReuseStubDocker), 0o755); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("bash", append([]string{filepath.Join(repoRoot(t), "ci", "check_go_containers.sh")}, args...)...)
	cmd.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "STUB_LOG="+log)
	cmd.Env = append(cmd.Env, env...)
	out, err := cmd.CombinedOutput()
	calls, _ := os.ReadFile(log)
	return string(out), string(calls), err
}

func TestVerifyImagesFailsClosedOnAnyDifferentOrMissingId(t *testing.T) {
	wrong := stubImageID("another image")
	cases := []struct {
		name     string
		expected string
		missing  string
		args     []string
		wantFail string
	}{
		{"every id matches", expectedImageIDs(imageReuseTargets, nil), "", nil, ""},
		{"a consumer verifies only the targets it uses", expectedImageIDs(imageReuseTargets, nil), "", []string{"dho", "auth-service"}, ""},
		{"one id differs", expectedImageIDs(imageReuseTargets, map[string]string{"dho": wrong}), "", nil, "dho"},
		{"the differing target is the one asked for", expectedImageIDs(imageReuseTargets, map[string]string{"dho": wrong}), "", []string{"dho"}, wrong},
		{"no expected ids at all", "", "", nil, "no expected image id"},
		{"the expected ids omit a target", expectedImageIDs(imageReuseTargets[:5], nil), "", nil, "auth-migrate"},
		{"an expected id is not a sha256 id", expectedImageIDs(imageReuseTargets, map[string]string{"migrate": "latest"}), "", nil, "not a sha256 id"},
		{"an image was not loaded", expectedImageIDs(imageReuseTargets, nil), "dev-health-go-operator:ci", nil, "not loaded"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out, _, err := runGoContainersScript(t, []string{"GO_IMAGES_EXPECTED=" + c.expected, "STUB_MISSING=" + c.missing}, append([]string{"verify-images"}, c.args...)...)
			if c.wantFail == "" {
				if err != nil {
					t.Fatalf("verify-images failed although every id matches: %v\n%s", err, out)
				}
				if !strings.Contains(out, "image-receipt consumer=verify target=") {
					t.Fatalf("verify-images printed no receipt line:\n%s", out)
				}
				return
			}
			if err == nil {
				t.Fatalf("verify-images passed with a wrong or missing id:\n%s", out)
			}
			if !strings.Contains(out, c.wantFail) {
				t.Fatalf("verify-images failed without naming %q:\n%s", c.wantFail, out)
			}
		})
	}
}

// A prebuilt smoke must refuse a swapped image before it builds or runs anything.
func TestPrebuiltSmokeRefusesADifferentImageBeforeRunningIt(t *testing.T) {
	env := []string{
		"GO_IMAGES_PREBUILT=1", "GO_IMAGES_CONSUMER=smoke",
		"GO_IMAGES_EXPECTED=" + expectedImageIDs(imageReuseTargets, map[string]string{"dho": stubImageID("a different dho")}),
	}
	out, calls, err := runGoContainersScript(t, env, "smoke")
	if err == nil {
		t.Fatalf("smoke passed on an image whose id is not the reported one:\n%s", out)
	}
	if !strings.Contains(out, "image-receipt consumer=smoke target=dho") {
		t.Fatalf("no smoke receipt for the differing image:\n%s", out)
	}
	for _, line := range strings.Split(calls, "\n") {
		if strings.HasPrefix(line, "build ") || strings.HasPrefix(line, "run ") {
			t.Fatalf("smoke built or ran an image before the id check failed: %q", line)
		}
	}
}

func TestBuildSharedBuildsEachTargetOnceAndReportsTheIds(t *testing.T) {
	dir := t.TempDir()
	out, calls, err := runGoContainersScript(t, nil, "build-shared", dir)
	if err != nil {
		t.Fatalf("build-shared failed: %v\n%s", err, out)
	}
	builds := 0
	for _, line := range strings.Split(calls, "\n") {
		if strings.HasPrefix(line, "build ") {
			builds++
			if !strings.Contains(line, "--provenance=false") || !strings.Contains(line, "VERSION=phase1-ci") {
				t.Errorf("a shared build does not use the fixed metadata and --provenance=false: %q", line)
			}
		}
	}
	if builds != len(imageReuseTargets) {
		t.Fatalf("build-shared ran %d builds, want one per target (%d)", builds, len(imageReuseTargets))
	}
	ids, err := os.ReadFile(filepath.Join(dir, "ids.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(ids) != expectedImageIDs(imageReuseTargets, nil) {
		t.Fatalf("ids.txt = %q, want %q", ids, expectedImageIDs(imageReuseTargets, nil))
	}
	if !strings.Contains(calls, "save --output "+filepath.Join(dir, "images.tar")) {
		t.Fatalf("build-shared did not docker save into the directory:\n%s", calls)
	}
	// What the build reports is exactly what verify-images accepts.
	if out, _, err := runGoContainersScript(t, []string{"GO_IMAGES_EXPECTED=" + string(ids)}, "verify-images"); err != nil {
		t.Fatalf("verify-images rejected the ids build-shared wrote: %v\n%s", err, out)
	}
}

// Without the opt-in, smoke keeps building its own images, so a local run of the script is unchanged.
func TestSmokeBuildsByDefault(t *testing.T) {
	_, calls, _ := runGoContainersScript(t, nil, "smoke")
	if !strings.Contains(calls, "build --file ") {
		t.Fatalf("smoke without GO_IMAGES_PREBUILT never built an image:\n%s", calls)
	}
}
