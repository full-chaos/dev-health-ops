package workflows_test

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

// The Go images of go.yml are built once per platform by go-container-build; smoke, the scans and the two compose
// proofs load that build and compare each image id with the id the build job reported as a job output.
// go-container-reproducibility is the one consumer that must NOT reuse it: it builds twice from scratch and compares.

var imageReuseConsumers = []string{"go-container-smoke", "go-container-security", "go-auth-compose-proof", "go-auth-web-proxy-proof"}

var imageReuseTargets = []string{"operator", "contractcheck", "migrate", "dho", "auth-service", "auth-migrate"}

const imageReuseArtifact = "go-images-"

func imageReuseSteps(job yamlMap) (uses, runs []string) {
	for _, s := range stepsOf(job) {
		if u := asString(s["uses"]); u != "" {
			uses = append(uses, u)
		}
		if r := asString(s["run"]); r != "" {
			runs = append(runs, r)
		}
	}
	return uses, runs
}

func imageReuseHasNeed(job yamlMap, want string) bool {
	for _, n := range pinNeeds(job) {
		if n == want {
			return true
		}
	}
	return false
}

func imageReuseViolations(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	add := func(format string, args ...any) { out = append(out, fmt.Sprintf(format, args...)) }

	goDoc := loadWf(t, root, "go.yml")
	build := pinJob(goDoc, "go-container-build")
	if build == nil {
		return []string{"go.yml has no go-container-build job: the Go images are built once per consumer"}
	}
	smoke := pinJob(goDoc, "go-container-smoke")
	if strings.TrimSpace(asString(build["if"])) != strings.TrimSpace(asString(smoke["if"])) {
		add("go-container-build's if must be exactly go-container-smoke's (heavy gate + docs-only skip)")
	}
	outputs := asMap(build["outputs"])
	for _, k := range []string{"ids_amd64", "ids_arm64"} {
		if !strings.Contains(asString(outputs[k]), "steps.build.outputs."+k) {
			add("go-container-build must expose %s from the build step as a job output", k)
		}
	}
	buildUses, buildRuns := imageReuseSteps(build)
	if !strings.Contains(strings.Join(buildRuns, "\n"), "ci/check_go_containers.sh build-shared") {
		add("go-container-build does not run ci/check_go_containers.sh build-shared")
	}
	var uploadName string
	for _, s := range stepsOf(build) {
		if strings.HasPrefix(asString(s["uses"]), "actions/upload-artifact@") {
			with := asMap(s["with"])
			uploadName = asString(with["name"])
			if fmt.Sprint(with["retention-days"]) != "1" {
				add("go-container-build's image artifact must keep retention-days: 1, got %v", with["retention-days"])
			}
			if asString(with["if-no-files-found"]) != "error" {
				add("go-container-build's image artifact must fail when the tar is missing (if-no-files-found: error)")
			}
		}
	}
	if len(buildUses) == 0 || uploadName == "" {
		add("go-container-build uploads no image artifact")
	}
	for _, part := range []string{"github.run_id", "github.run_attempt", "matrix.arch"} {
		if !strings.Contains(uploadName, part) {
			add("the image artifact name %q must carry %s", uploadName, part)
		}
	}

	for _, name := range imageReuseConsumers {
		job := pinJob(goDoc, name)
		if job == nil {
			add("go.yml has no %s job", name)
			continue
		}
		if !imageReuseHasNeed(job, "go-container-build") {
			add("%s does not need go-container-build", name)
		}
		_, runs := imageReuseSteps(job)
		code := shellCode(strings.Join(runs, "\n"))
		for _, banned := range []string{"docker build", "docker buildx build", "--build "} {
			if strings.Contains(code, banned) {
				add("%s builds an image itself (%q); it must use the shared build", name, banned)
			}
		}
		if !strings.Contains(code, "docker load") || !strings.Contains(code, "ci/check_go_containers.sh verify-images") {
			add("%s does not load the shared images and run verify-images", name)
		}
		var download, expected bool
		for _, s := range stepsOf(job) {
			if strings.HasPrefix(asString(s["uses"]), "actions/download-artifact@") {
				download = true
				n := asString(asMap(s["with"])["name"])
				if !strings.Contains(n, imageReuseArtifact) || !strings.Contains(n, "github.run_id") || !strings.Contains(n, "github.run_attempt") {
					add("%s downloads artifact %q, which does not carry the run id and attempt", name, n)
				}
			}
			if e := asString(asMap(s["env"])["GO_IMAGES_EXPECTED"]); e != "" {
				expected = true
				if !strings.Contains(e, "needs.go-container-build.outputs.ids_") {
					add("%s takes its expected image ids from %q, not from the build job's outputs", name, e)
				}
			}
		}
		if !download {
			add("%s downloads no shared image artifact", name)
		}
		if !expected {
			add("%s sets no GO_IMAGES_EXPECTED from the build job's outputs", name)
		}
	}

	if !strings.Contains(shellCode(strings.Join(imageReuseRuns(smoke), "\n")), "ci/check_go_containers.sh smoke") ||
		!imageReuseEnvHas(smoke, "GO_IMAGES_PREBUILT", "1") {
		add("go-container-smoke must run smoke with GO_IMAGES_PREBUILT=1")
	}
	for _, name := range []string{"go-auth-compose-proof", "go-auth-web-proxy-proof"} {
		if !imageReuseEnvHas(pinJob(goDoc, name), "AUTH_PROOF_PREBUILT", "1") {
			add("%s does not run its compose proof with AUTH_PROOF_PREBUILT=1 (up --no-build)", name)
		}
		for _, v := range []string{"DEV_HEALTH_GO_OPERATOR_IMAGE", "DEV_HEALTH_GO_DHO_IMAGE", "DEV_HEALTH_GO_CONTRACTCHECK_IMAGE", "DEV_HEALTH_GO_AUTH_MIGRATE_IMAGE", "DEV_HEALTH_GO_AUTH_SERVICE_IMAGE"} {
			if !imageReuseEnvHas(pinJob(goDoc, name), v, "") {
				add("%s does not pin %s to a loaded image", name, v)
			}
		}
	}

	security := pinJob(goDoc, "go-container-security")
	if security != nil {
		if security["strategy"] != nil {
			add("go-container-security is a matrix again; the six scans belong in one job over the shared images")
		}
		var sbom, trivy = map[string]bool{}, map[string]bool{}
		for _, s := range stepsOf(security) {
			with := asMap(s["with"])
			if strings.HasPrefix(asString(s["uses"]), "anchore/sbom-action@") {
				sbom[asString(with["image"])] = true
			}
			if strings.HasPrefix(asString(s["uses"]), "aquasecurity/trivy-action@") {
				trivy[asString(with["image-ref"])] = true
				if asString(with["severity"]) != "CRITICAL" || fmt.Sprint(with["exit-code"]) != "1" {
					add("the Trivy scan of %s no longer fails on CRITICAL", asString(with["image-ref"]))
				}
			}
		}
		for _, target := range imageReuseTargets {
			ref := "dev-health-go-" + target + ":ci"
			if !sbom[ref] {
				add("go-container-security has no SBOM step for %s", ref)
			}
			if !trivy[ref] {
				add("go-container-security has no Trivy scan for %s", ref)
			}
		}
	}

	for _, name := range []string{"go-container-reproducibility", "go-container-reproducibility-self-hosted"} {
		job := pinJob(goDoc, name)
		if job == nil {
			add("go.yml has no %s job", name)
			continue
		}
		if imageReuseHasNeed(job, "go-container-build") {
			add("%s needs go-container-build; it must build twice from scratch, independent of the shared build", name)
		}
		if !strings.Contains(shellCode(strings.Join(imageReuseRuns(job), "\n")), "ci/check_go_containers.sh reproducible") {
			add("%s no longer runs ci/check_go_containers.sh reproducible", name)
		}
	}
	script := readFile(t, filepath.Join(root, "ci", "check_go_containers.sh"))
	if i := strings.Index(script, "\nreproducible() {"); i < 0 {
		add("ci/check_go_containers.sh has no reproducible() function")
	} else {
		body := script[i:]
		if j := strings.Index(body, "\ncase "); j > 0 {
			body = body[:j]
		}
		if strings.Count(shellCode(body), "--no-cache") < 2 {
			add("reproducible() no longer builds with --no-cache")
		}
		for _, banned := range []string{"GO_IMAGES_PREBUILT", "verify_loaded_image", "docker load"} {
			if strings.Contains(shellCode(body), banned) {
				add("reproducible() reads the shared images (%s); it must build independently", banned)
			}
		}
	}

	gate := pinJob(goDoc, "go-queue-gate")
	if !imageReuseHasNeed(gate, "go-container-build") {
		add("go-queue-gate does not need go-container-build")
	}
	if !strings.Contains(fmt.Sprint(stepsOf(gate)), "go-container-build|queue-heavy|${{ needs.go-container-build.result }}") {
		add("go-queue-gate has no queue-heavy row for go-container-build")
	}

	docker := readFile(t, filepath.Join(root, ".github", "workflows", "docker-images.yml"))
	if strings.Contains(docker, "go-container-build") && !strings.Contains(docker, "CHAOS-11415") {
		add("docker-images.yml refers to go-container-build without saying why it builds on its own")
	}
	for _, want := range []string{"COMMIT=${{ steps.versioning.outputs.commit }}", "VERSION=${{ steps.versioning.outputs.version }}"} {
		if !strings.Contains(docker, want) {
			add("docker-images.yml no longer builds the Go images with the real commit metadata (%s)", want)
		}
	}
	for _, banned := range []string{"download-artifact", "docker load"} {
		start := strings.Index(docker, "\n  go-build:")
		end := strings.Index(docker, "\n  dind-smoke-test:")
		if start >= 0 && end > start && strings.Contains(docker[start:end], banned+" ") && strings.Contains(docker[start:end], "go-images-") {
			add("docker-images.yml go-build reuses go.yml's shared images (%s)", banned)
		}
	}

	return out
}

func imageReuseRuns(job yamlMap) []string {
	_, runs := imageReuseSteps(job)
	return runs
}

func imageReuseEnvHas(job yamlMap, key, want string) bool {
	for _, s := range stepsOf(job) {
		if v, ok := asMap(s["env"])[key]; ok {
			return want == "" || fmt.Sprint(v) == want
		}
	}
	return false
}

func TestGoImagesAreBuiltOnceAndReusedByDigest(t *testing.T) {
	expectNoViolations(t, imageReuseViolations(t, repoRoot(t)))
}

func TestGoImageReusePlantedDefects(t *testing.T) {
	root := repoRoot(t)
	const goYml, dock, script = ".github/workflows/go.yml", ".github/workflows/docker-images.yml", "ci/check_go_containers.sh"
	files := []string{goYml, dock, script}
	cases := []struct {
		name string
		muts []mutation
		want []string
	}{
		{"smoke stops needing the shared build", []mutation{{goYml, "    needs: [go-storage-integration-plan, go-container-build]\n    if: (!(github.event_name == 'pull_request' && !contains(github.event.pull_request.labels.*.name, 'ci:full'))) && needs.go-storage-integration-plan.outputs.docs_only != 'true'\n    # Every Go image", "    needs: go-storage-integration-plan\n    if: (!(github.event_name == 'pull_request' && !contains(github.event.pull_request.labels.*.name, 'ci:full'))) && needs.go-storage-integration-plan.outputs.docs_only != 'true'\n    # Every Go image"}}, []string{"go-container-smoke does not need go-container-build"}},
		{"a consumer builds its own image again", []mutation{{goYml, "          bash ci/check_go_containers.sh verify-images\n          bash ci/check_go_containers.sh smoke", "          docker build --target dho .\n          bash ci/check_go_containers.sh smoke"}}, []string{"go-container-smoke", "builds an image itself"}},
		{"smoke compares no ids", []mutation{{goYml, "          GO_IMAGES_EXPECTED: ${{ matrix.arch == 'amd64' && needs.go-container-build.outputs.ids_amd64 || needs.go-container-build.outputs.ids_arm64 }}", "          GO_IMAGES_EXPECTED: ''"}}, []string{"go-container-smoke sets no GO_IMAGES_EXPECTED"}},
		{"smoke takes its expected ids from a file", []mutation{{goYml, "          GO_IMAGES_EXPECTED: ${{ matrix.arch == 'amd64' && needs.go-container-build.outputs.ids_amd64 || needs.go-container-build.outputs.ids_arm64 }}", "          GO_IMAGES_EXPECTED: ${{ hashFiles('x') }}"}}, []string{"go-container-smoke", "not from the build job's outputs"}},
		{"the artifact name drops the attempt", []mutation{{goYml, "          name: go-images-${{ matrix.arch }}-${{ github.run_id }}-${{ github.run_attempt }}\n          path: ${{ runner.temp }}/go-images/images.tar", "          name: go-images-${{ matrix.arch }}-${{ github.run_id }}\n          path: ${{ runner.temp }}/go-images/images.tar"}}, []string{"must carry github.run_attempt"}},
		{"the artifact lives longer than a day", []mutation{{goYml, "          retention-days: 1\n          compression-level: 1", "          retention-days: 7\n          compression-level: 1"}}, []string{"retention-days: 1"}},
		{"reproducibility reuses the shared build", []mutation{{goYml, "  go-container-reproducibility:\n    name: go-container-reproducibility\n    # Heavy (merge queue, main push, ci:full PR); a docs-only change skips it. go-queue-gate judges the pair as one.\n    needs: go-storage-integration-plan\n", "  go-container-reproducibility:\n    name: go-container-reproducibility\n    # Heavy (merge queue, main push, ci:full PR); a docs-only change skips it. go-queue-gate judges the pair as one.\n    needs: [go-storage-integration-plan, go-container-build]\n"}}, []string{"go-container-reproducibility needs go-container-build"}},
		{"reproducibility builds from cache", []mutation{{script, "--no-cache --provenance=false\n    for target", "--provenance=false\n    for target"}}, []string{"reproducible() no longer builds with --no-cache"}},
		{"go-queue-gate drops the build row", []mutation{{goYml, "          GATED_JOB_8: go-container-build|queue-heavy|${{ needs.go-container-build.result }}\n", ""}}, []string{"queue-heavy row for go-container-build"}},
		{"the security scan of one image is dropped", []mutation{{goYml, "          image-ref: dev-health-go-dho:ci\n", "          image-ref: dev-health-go-dho:other\n"}}, []string{"no Trivy scan for dev-health-go-dho:ci"}},
		{"the security job is a matrix again", []mutation{{goYml, "  go-container-security:\n", "  go-container-security:\n    strategy:\n      matrix:\n        target: [operator]\n"}}, []string{"matrix again"}},
		{"a compose proof builds again", []mutation{{goYml, "          AUTH_PROOF_PREBUILT: \"1\"\n          DEV_HEALTH_GO_OPERATOR_IMAGE: dev-health-go-operator:ci\n          DEV_HEALTH_GO_DHO_IMAGE: dev-health-go-dho:ci\n          DEV_HEALTH_GO_CONTRACTCHECK_IMAGE: dev-health-go-contractcheck:ci\n          DEV_HEALTH_GO_AUTH_MIGRATE_IMAGE: dev-health-go-auth-migrate:ci\n          DEV_HEALTH_GO_AUTH_SERVICE_IMAGE: dev-health-go-auth-service:ci\n        run: bash ci/check_auth_compose.sh", "          X: y\n        run: bash ci/check_auth_compose.sh"}}, []string{"go-auth-compose-proof", "AUTH_PROOF_PREBUILT"}},
		{"docker-images.yml loses the real commit metadata", []mutation{{dock, "COMMIT=${{ steps.versioning.outputs.commit }}", allMarker + "COMMIT=0000000000000000000000000000000000000000"}}, []string{"real commit metadata"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			expectViolation(t, imageReuseViolations(t, plantTree(t, root, files, c.muts...)), c.want...)
		})
	}
}
