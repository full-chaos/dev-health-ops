package workflows_test

import (
	"bytes"
	"fmt"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// CHAOS-10366 (part A): the two base images every Go image build starts from (the golang build image and the debian runtime
// image of docker/go-api-tools.Dockerfile) are mirrored to ghcr.io/full-chaos by mirror-test-images.yml, from ci/base_images.tsv,
// by digest. Today the Dockerfiles pull them from mirror.gcr.io, which answers 429 under load. This gate holds: the tsv names
// exactly go-build and tools-runtime and parses through the one reader (ci/builder_images.sh); every Dockerfile pin is that
// same ref (the registry prefix aside); the mirror workflow resolves the tsv in BOTH of its resolve steps, runs the anonymous
// digest check after the publish, and starts on a change of either file.

const baseImagesOwner = "full-chaos"

var baseImageDockerfiles = []string{
	"docker/go-worker.Dockerfile", "docker/smtpsink.Dockerfile", "docker/providerstub.Dockerfile", "docker/go-api-tools.Dockerfile",
}

var baseImageFiles = append([]string{
	"ci/builder_images.sh", "ci/builder_images.tsv", "ci/base_images.tsv", "ci/verify_base_images.sh", mirrorWf,
}, baseImageDockerfiles...)

var baseImageArg = regexp.MustCompile(`(?m)^ARG (GO_BUILD_IMAGE|TOOLS_RUNTIME_IMAGE)="([^"]+)"$`)

func baseImagesDeclared(t *testing.T, root string) (map[string]string, string, int) {
	t.Helper()
	cmd := exec.Command("bash", filepath.Join(root, "ci", "builder_images.sh"), baseImagesOwner, "ci/base_images.tsv")
	cmd.Dir = root
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	code := 0
	if err := cmd.Run(); err != nil {
		ee, ok := err.(*exec.ExitError)
		if !ok {
			t.Fatal(err)
		}
		code = ee.ExitCode()
	}
	declared := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(out.String()), "\n") {
		f := strings.Split(line, "\t")
		if len(f) == 3 && f[0] == "upstream" {
			declared[f[1]] = f[2]
		}
	}
	return declared, errb.String(), code
}

// stripRegistry removes the registry part of a Dockerfile pin: the old mirror host or the new owner-qualified ghcr prefix.
func stripRegistry(ref string) (string, bool) {
	for _, p := range []string{"mirror.gcr.io/", "ghcr.io/" + baseImagesOwner + "/"} {
		if rest, ok := strings.CutPrefix(ref, p); ok {
			return rest, true
		}
	}
	return ref, false
}

func baseImagesViolations(t *testing.T, root string) []string {
	t.Helper()
	declared, stderr, code := baseImagesDeclared(t, root)
	if code != 0 {
		return []string{"ci/builder_images.sh ci/base_images.tsv failed: " + stderr}
	}
	var out []string
	if len(declared) != 2 || declared["go-build"] == "" || declared["tools-runtime"] == "" {
		out = append(out, fmt.Sprintf("ci/base_images.tsv must name exactly go-build and tools-runtime, got %v", declared))
	}
	want := map[string]string{"GO_BUILD_IMAGE": declared["go-build"], "TOOLS_RUNTIME_IMAGE": declared["tools-runtime"]}
	seen := map[string]int{}
	for _, f := range baseImageDockerfiles {
		for _, m := range baseImageArg.FindAllStringSubmatch(readFile(t, filepath.Join(root, f)), -1) {
			seen[m[1]]++
			rest, ok := stripRegistry(m[2])
			if !ok {
				out = append(out, fmt.Sprintf("%s: ARG %s=%q names a registry other than mirror.gcr.io or ghcr.io/%s", f, m[1], m[2], baseImagesOwner))
				continue
			}
			if rest != want[m[1]] {
				out = append(out, fmt.Sprintf("%s: ARG %s pins %q, but ci/base_images.tsv declares %q (the digest and tag must match)", f, m[1], rest, want[m[1]]))
			}
		}
	}
	if seen["GO_BUILD_IMAGE"] != 4 || seen["TOOLS_RUNTIME_IMAGE"] != 1 {
		out = append(out, fmt.Sprintf("the Dockerfile walk found GO_BUILD_IMAGE x%d and TOOLS_RUNTIME_IMAGE x%d, want x4 and x1: the walk found nothing to hold", seen["GO_BUILD_IMAGE"], seen["TOOLS_RUNTIME_IMAGE"]))
	}

	doc := loadWf(t, root, "mirror-test-images.yml")
	resolve := `ci/builder_images.sh "${OWNER}" ci/base_images.tsv`
	for _, job := range []string{"precheck", "mirror"} {
		found := false
		for _, s := range stepsOf(jobsOf(doc)[job]) {
			if asString(s["id"]) == "images" && strings.Contains(asString(s["run"]), resolve) {
				found = true
			}
		}
		if !found {
			out = append(out, fmt.Sprintf("mirror-test-images.yml: job %s: the `images` resolve step does not read ci/base_images.tsv through `%s`", job, resolve))
		}
	}
	verifyAt, digestAt := -1, -1
	for i, s := range stepsOf(jobsOf(doc)["mirror"]) {
		if strings.Contains(asString(s["run"]), `bash ci/verify_base_images.sh "${OWNER}"`) {
			verifyAt = i
			if t := fmt.Sprint(s); strings.Contains(t, "secrets.") || strings.Contains(t, "GITHUB_TOKEN") {
				out = append(out, "mirror-test-images.yml: the anonymous base-image check must use no secret and no token")
			}
		}
		if asString(s["name"]) == "Verify every mirrored digest matches upstream" {
			digestAt = i
		}
	}
	if verifyAt < 0 {
		out = append(out, "mirror-test-images.yml: job mirror has no step running `bash ci/verify_base_images.sh \"${OWNER}\"` (the published digest is never compared to the pin anonymously)")
	} else if digestAt >= 0 && verifyAt < digestAt {
		out = append(out, "mirror-test-images.yml: the anonymous base-image check must run AFTER the publish and its digest verification")
	}
	on := asMap(doc["on"])
	for _, event := range []string{"push", "pull_request"} {
		paths := map[string]bool{}
		for _, p := range asList(asMap(on[event])["paths"]) {
			paths[asString(p)] = true
		}
		for _, want := range []string{"ci/base_images.tsv", "ci/verify_base_images.sh"} {
			if !paths[want] {
				out = append(out, fmt.Sprintf("mirror-test-images.yml: %s paths lack %s (a change of it would not start the mirror)", event, want))
			}
		}
	}
	return out
}

func TestBaseImagesMirrored(t *testing.T) {
	expectNoViolations(t, baseImagesViolations(t, repoRoot(t)))
}

func TestBaseImagesPlantedDefects(t *testing.T) {
	root := repoRoot(t)
	cases := []struct {
		name string
		muts []mutation
		want []string
	}{
		{"a tsv ref loses its digest pin", []mutation{{"ci/base_images.tsv", "library/golang:1.27.3-alpine@sha256:", "library/golang:1.27.3-alpine@sha1:"}}, []string{"ci/builder_images.sh ci/base_images.tsv failed", "@sha256"}},
		{"a third image is added to the tsv", []mutation{{"ci/base_images.tsv", "tools-runtime\t", "extra\tlibrary/alpine:3@sha256:" + strings.Repeat("a", 64) + "\ntools-runtime\t"}}, []string{"must name exactly go-build and tools-runtime"}},
		{"a Dockerfile digest drifts from the tsv", []mutation{{"docker/smtpsink.Dockerfile", "@sha256:f2c872bcee949b7be9c490efd02b48a8a2fbf947cdf533f1c58525ec9d8170fd", "@sha256:" + strings.Repeat("b", 64)}}, []string{"docker/smtpsink.Dockerfile", "GO_BUILD_IMAGE", "digest and tag must match"}},
		{"the runtime pin drifts", []mutation{{"docker/go-api-tools.Dockerfile", "@sha256:88200866dfff7ea7f5cbcb6ec7c8a701889efe6fe859fe64d6990e4b07ea4171", "@sha256:" + strings.Repeat("c", 64)}}, []string{"TOOLS_RUNTIME_IMAGE"}},
		{"a Dockerfile pin names another registry", []mutation{{"docker/providerstub.Dockerfile", "ARG GO_BUILD_IMAGE=\"mirror.gcr.io/", "ARG GO_BUILD_IMAGE=\"docker.io/"}}, []string{"names a registry other than"}},
		{"the precheck resolve step stops reading the tsv", []mutation{{mirrorWf, `bash ci/builder_images.sh "${OWNER}" ci/base_images.tsv`, `bash ci/builder_images.sh "${OWNER}"`}}, []string{"job precheck", "does not read ci/base_images.tsv"}},
		{"the anonymous check is gone", []mutation{{mirrorWf, `bash ci/verify_base_images.sh "${OWNER}"`, `true "${OWNER}"`}}, []string{"has no step running"}},
		{"the push filter lacks the tsv", []mutation{{mirrorWf, "      - ci/base_images.tsv\n", ""}}, []string{"paths lack ci/base_images.tsv"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tree := plantTree(t, root, baseImageFiles, c.muts...)
			expectViolation(t, baseImagesViolations(t, tree), c.want...)
		})
	}
}
