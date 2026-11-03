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
	"docker/go-worker.Dockerfile", "docker/smtpsink.Dockerfile", "docker/providerstub.Dockerfile", "docker/idpstub.Dockerfile", "docker/go-api-tools.Dockerfile",
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
	if seen["GO_BUILD_IMAGE"] != 5 || seen["TOOLS_RUNTIME_IMAGE"] != 1 {
		out = append(out, fmt.Sprintf("the Dockerfile walk found GO_BUILD_IMAGE x%d and TOOLS_RUNTIME_IMAGE x%d, want x5 and x1: the walk found nothing to hold", seen["GO_BUILD_IMAGE"], seen["TOOLS_RUNTIME_IMAGE"]))
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
		{"a tsv ref has no digest at all", []mutation{{"ci/base_images.tsv", "library/golang:1.27.3-alpine@sha256:f2c872bcee949b7be9c490efd02b48a8a2fbf947cdf533f1c58525ec9d8170fd", "library/golang:1.27.3-alpine"}}, []string{"ci/builder_images.sh ci/base_images.tsv failed"}},
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

// CHAOS-10366 (part A, safety): the mirror workflow must never publish from a pull_request run. A PR run used to reach the
// mirror job (when the precheck found an image missing) with a workflow-wide packages: write token, so a PR pushed new
// packages into ghcr.io/full-chaos before any review. The shape held here:
//   - workflow-level permissions grant no write; the ONE job that publishes (mirror) holds packages: write;
//   - the mirror job runs only on a push, a workflow_dispatch or the weekly schedule, and only from refs/heads/main;
//   - on a pull_request the job `dry-run` runs with packages: read, no login, no secret and no step that can push, and
//     verifies each pinned base-image digest at its source.

const mirrorPublishIf = "github.ref == 'refs/heads/main' && (github.event_name == 'push' || github.event_name == 'workflow_dispatch' || github.event_name == 'schedule')"

func permissionsOf(v any) map[string]string {
	out := map[string]string{}
	for k, val := range asMap(v) {
		out[k] = asString(val)
	}
	return out
}

func permsEqual(got map[string]string, want map[string]string) bool {
	if len(got) != len(want) {
		return false
	}
	for k, v := range want {
		if got[k] != v {
			return false
		}
	}
	return true
}

var pushingCommand = regexp.MustCompile(`imagetools\s+create|docker\s+push|buildx\s+build[^\n]*--push|\bcrane\s+(push|copy|cp)\b|\bskopeo\s+copy\b`)

func mirrorPushGuardViolations(t *testing.T, root string) []string {
	t.Helper()
	doc := loadWf(t, root, "mirror-test-images.yml")
	var out []string
	for k, v := range permissionsOf(doc["permissions"]) {
		if v == "write" {
			out = append(out, fmt.Sprintf("mirror-test-images.yml: workflow-level permissions must grant no write (got %s: write): a job without its own permissions would inherit it, pull_request runs included", k))
		}
	}
	jobs := jobsOf(doc)
	for _, name := range sortedKeys(jobs) {
		perms := permissionsOf(asMap(jobs[name])["permissions"])
		for k, v := range perms {
			if v == "write" && !(name == "mirror" && k == "packages") {
				out = append(out, fmt.Sprintf("mirror-test-images.yml: job %s holds %s: write; only the mirror job may hold packages: write", name, k))
			}
		}
		if name != "mirror" {
			for _, s := range stepsOf(jobs[name]) {
				if pushingCommand.MatchString(asString(s["run"])) {
					out = append(out, fmt.Sprintf("mirror-test-images.yml: job %s runs a command that pushes to a registry; only the mirror job may", name))
				}
			}
		}
	}
	mirror := asMap(jobs["mirror"])
	if mirror == nil {
		return append(out, "mirror-test-images.yml: job mirror is missing")
	}
	if got := strings.Join(strings.Fields(asString(mirror["if"])), " "); got != mirrorPublishIf {
		out = append(out, "mirror-test-images.yml: the mirror if must be the ref guard `"+mirrorPublishIf+"` (publish only from main, never on a pull_request), got: "+got)
	}
	if !permsEqual(permissionsOf(mirror["permissions"]), map[string]string{"contents": "read", "packages": "write"}) {
		out = append(out, "mirror-test-images.yml: job mirror permissions must be exactly contents: read, packages: write")
	}
	dry := asMap(jobs["dry-run"])
	if dry == nil {
		return append(out, "mirror-test-images.yml: job dry-run is missing (a pull_request run must verify the pinned digests at the source without publishing)")
	}
	if got := strings.Join(strings.Fields(asString(dry["if"])), " "); got != "github.event_name == 'pull_request'" {
		out = append(out, "mirror-test-images.yml: job dry-run must run on a pull_request only, got if: "+got)
	}
	if !permsEqual(permissionsOf(dry["permissions"]), map[string]string{"contents": "read", "packages": "read"}) {
		out = append(out, "mirror-test-images.yml: job dry-run permissions must be exactly contents: read, packages: read")
	}
	ran := false
	for _, s := range stepsOf(dry) {
		if u := strings.ToLower(asString(s["uses"])); strings.Contains(u, "login") {
			out = append(out, "mirror-test-images.yml: job dry-run must use no login action")
		}
		if txt := fmt.Sprint(s); strings.Contains(txt, "secrets.") || strings.Contains(txt, "GITHUB_TOKEN") {
			out = append(out, "mirror-test-images.yml: job dry-run must use no secret and no token")
		}
		if strings.Contains(asString(s["run"]), `bash ci/verify_base_sources.sh "${OWNER}"`) {
			ran = true
		}
	}
	if !ran {
		out = append(out, "mirror-test-images.yml: job dry-run does not run `bash ci/verify_base_sources.sh \"${OWNER}\"`")
	}
	return out
}

func TestMirrorPublishesOnlyFromMain(t *testing.T) {
	expectNoViolations(t, mirrorPushGuardViolations(t, repoRoot(t)))
}

func TestMirrorPublishGuardPlantedDefects(t *testing.T) {
	root := repoRoot(t)
	const dryHead = "  dry-run:\n    if: github.event_name == 'pull_request'\n    runs-on: ubuntu-latest\n    timeout-minutes: 10\n    permissions:\n      contents: read\n      packages: read\n"
	cases := []struct {
		name string
		muts []mutation
		want []string
	}{
		{"write on pull_request: the workflow level grants packages: write again", []mutation{{mirrorWf, "permissions:\n  contents: read\n\nconcurrency:", "permissions:\n  contents: read\n  packages: write\n\nconcurrency:"}}, []string{"workflow-level permissions must grant no write"}},
		{"missing ref guard: the mirror if loses the main-only guard", []mutation{{mirrorWf, "github.ref == 'refs/heads/main' &&", ""}}, []string{"the mirror if must be the ref guard"}},
		{"the mirror publishes on a pull_request again", []mutation{{mirrorWf, "github.event_name == 'push' ||", "github.event_name == 'pull_request' ||"}}, []string{"the mirror if must be the ref guard"}},
		{"the mirror job loses its own write", []mutation{{mirrorWf, "    permissions:\n      contents: read\n      packages: write\n", "    permissions:\n      contents: read\n"}}, []string{"job mirror permissions must be exactly"}},
		{"the dry-run job gains write", []mutation{{mirrorWf, dryHead, strings.Replace(dryHead, "packages: read", "packages: write", 1)}}, []string{"job dry-run holds packages: write"}},
		{"the dry-run job logs in", []mutation{{mirrorWf, "      - name: Verify each pinned base image digest at its source\n", "      - uses: docker/login-action@v3\n      - name: Verify each pinned base image digest at its source\n"}}, []string{"job dry-run must use no login action"}},
		{"the dry-run job pushes", []mutation{{mirrorWf, `bash ci/verify_base_sources.sh "${OWNER}"`, `docker buildx imagetools create --tag ghcr.io/x/y:z docker.io/library/z`}}, []string{"job dry-run runs a command that pushes"}},
		{"the dry-run job is gone from pull_request", []mutation{{mirrorWf, "    if: github.event_name == 'pull_request'\n", "    if: github.event_name == 'push'\n"}}, []string{"job dry-run must run on a pull_request only"}},
		{"another job gains write", []mutation{{mirrorWf, "  ratelimit-probe:\n    if: github.event_name == 'workflow_dispatch'\n", "  ratelimit-probe:\n    if: github.event_name == 'workflow_dispatch'\n    permissions:\n      contents: read\n      packages: write\n"}}, []string{"job ratelimit-probe holds packages: write"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tree := plantTree(t, root, []string{mirrorWf}, c.muts...)
			expectViolation(t, mirrorPushGuardViolations(t, tree), c.want...)
		})
	}
}
