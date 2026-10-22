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

// CHAOS-10177: Go port of tests/tooling/test_builder_images_pinned.py (CHAOS-9064). Every docker/setup-buildx and
// docker/setup-qemu step must pull the ghcr.io mirror at exactly the digest in ci/builder_images.tsv, never the
// anonymous Docker Hub default that fails once the shared runner IP is rate limited.

const builderOwner = "full-chaos"

var builderFiles = []string{"ci/builder_images.sh", "ci/builder_images.tsv", "docker/go-worker.Dockerfile", "docker/go-api-tools.Dockerfile"}

func builderDeclared(t *testing.T, root string) (map[string]string, string, int) {
	t.Helper()
	cmd := exec.Command("bash", filepath.Join(root, "ci", "builder_images.sh"), builderOwner)
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
		if len(f) == 3 && f[0] == "ghcr" {
			declared[f[1]] = f[2]
		}
	}
	return declared, errb.String(), code
}

var setupAction = regexp.MustCompile(`^docker/(setup-buildx|setup-qemu)-action@`)

type setupStep struct {
	file, job, action string
	with              yamlMap
}

func builderSetupSteps(t *testing.T, root string) []setupStep {
	t.Helper()
	var found []setupStep
	for _, name := range workflowNames(t, root) {
		doc := loadWf(t, root, name)
		jobs := jobsOf(doc)
		for _, jn := range sortedKeys(jobs) {
			for _, s := range stepsOf(jobs[jn]) {
				if m := setupAction.FindStringSubmatch(asString(s["uses"])); m != nil {
					found = append(found, setupStep{name, jn, m[1], asMap(s["with"])})
				}
			}
		}
	}
	return found
}

func builderViolations(t *testing.T, root string) []string {
	t.Helper()
	declared, stderr, code := builderDeclared(t, root)
	if code != 0 {
		return []string{"ci/builder_images.sh failed: " + stderr}
	}
	var out []string
	if len(declared) != 3 || declared["buildkit"] == "" || declared["binfmt"] == "" || declared["golang"] == "" {
		out = append(out, fmt.Sprintf("ci/builder_images.tsv must name exactly buildkit, binfmt and golang, got %v", declared))
	}
	for _, df := range []string{"docker/go-worker.Dockerfile", "docker/go-api-tools.Dockerfile"} {
		want := "ARG GO_BUILD_IMAGE=\"" + declared["golang"] + "\""
		if !strings.Contains(readFile(t, filepath.Join(root, df)), want) {
			out = append(out, fmt.Sprintf("%s: the Go builder base must be `%s` (the ghcr.io mirror row in ci/builder_images.tsv), not a mirror.gcr.io or Docker Hub ref", df, want))
		}
	}
	steps := builderSetupSteps(t, root)
	if len(steps) < 12 {
		out = append(out, fmt.Sprintf("only %d setup-buildx/setup-qemu steps found, want >= 12: the walk found nothing to hold", len(steps)))
	}
	for _, s := range steps {
		if s.action == "setup-buildx" {
			want := declared["buildkit"]
			var images []string
			for _, line := range strings.Split(asString(s.with["driver-opts"]), "\n") {
				if l := strings.TrimSpace(line); strings.HasPrefix(l, "image=") {
					images = append(images, strings.TrimPrefix(l, "image="))
				}
			}
			if len(images) != 1 || images[0] != want {
				out = append(out, fmt.Sprintf("%s:%s: docker/setup-buildx-action must set `driver-opts: image=%s` (the default pulls moby/buildkit from Docker Hub anonymously); got %q", s.file, s.job, want, asString(s.with["driver-opts"])))
			}
		} else if got := asString(s.with["image"]); got != declared["binfmt"] {
			out = append(out, fmt.Sprintf("%s:%s: docker/setup-qemu-action must set `image: %s` (the default pulls tonistiigi/binfmt from Docker Hub anonymously); got %q", s.file, s.job, declared["binfmt"], got))
		}
	}
	return out
}

func TestBuilderImagesPinned(t *testing.T) {
	expectNoViolations(t, builderViolations(t, repoRoot(t)))
}

func TestBuilderImagesPlantedDefects(t *testing.T) {
	root := repoRoot(t)
	// the planted trees need every workflow, so copy them all
	files := append([]string{}, builderFiles...)
	for _, n := range workflowNames(t, root) {
		files = append(files, ".github/workflows/"+n)
	}
	buildx := regexp.MustCompile(`image=ghcr\.io/\S+`)
	var victim string
	for _, s := range builderSetupSteps(t, root) {
		if s.action == "setup-buildx" {
			victim = ".github/workflows/" + s.file
			break
		}
	}
	victimText := readFile(t, filepath.Join(root, victim))
	line := buildx.FindString(victimText)
	if line == "" {
		t.Fatalf("no buildx image= line in %s", victim)
	}
	var qemuFile, qemuLine string
	for _, s := range builderSetupSteps(t, root) {
		if s.action == "setup-qemu" {
			qemuFile = ".github/workflows/" + s.file
			qemuLine = "image: " + asString(s.with["image"])
			break
		}
	}
	cases := []struct {
		name string
		muts []mutation
		want []string
	}{
		{"buildx falls back to the Docker Hub default", []mutation{{victim, line, strings.Replace(line, "image=ghcr.io", "image=docker.io", 1)}}, []string{"docker/setup-buildx-action must set"}},
		{"qemu points at an unpinned image", []mutation{{qemuFile, qemuLine, "image: tonistiigi/binfmt:latest"}}, []string{"docker/setup-qemu-action must set"}},
		{"go-worker builder base back on mirror.gcr.io", []mutation{{"docker/go-worker.Dockerfile", "ghcr.io/full-chaos/library/golang@", "mirror.gcr.io/library/golang:1.27.3-alpine@"}}, []string{"docker/go-worker.Dockerfile: the Go builder base must be"}},
		{"go-api-tools builder digest drifts from the tsv", []mutation{{"docker/go-api-tools.Dockerfile", "golang@sha256:f2c8", "golang@sha256:0000"}}, []string{"docker/go-api-tools.Dockerfile: the Go builder base must be"}},
		{"tsv ref loses its digest pin", []mutation{{"ci/builder_images.tsv", "moby/buildkit:buildx-stable-1@sha256:", "moby/buildkit:buildx-stable-1@sha1:"}}, []string{"builder_images.sh failed", "@sha256"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tree := plantTree(t, root, files, c.muts...)
			expectViolation(t, builderViolations(t, tree), c.want...)
		})
	}
}
