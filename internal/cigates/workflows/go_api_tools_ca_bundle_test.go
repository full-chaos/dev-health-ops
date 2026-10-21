package workflows_test

import (
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The go-api-tools runtime image carries a CA bundle (debian:bookworm-slim has none, so outbound TLS from the one-off
// tools pod failed certificate verification), and the docker-images workflow checks the bundle in the built image.

func runtimeStage(t *testing.T, dockerfile string) string {
	t.Helper()
	parts := regexp.MustCompile(`(?m)^FROM .* AS runtime`+pyS+`*$`).Split(dockerfile, -1)
	if len(parts) != 2 {
		t.Fatalf("expected exactly one runtime stage, found %d", len(parts)-1)
	}
	return parts[1]
}

var installsCABundle = regexp.MustCompile(`apt-get install[^\n]*(\\\n[^\n]*)*ca-certificates`)

func TestGoAPIToolsRuntimeStageInstallsCACertificates(t *testing.T) {
	root := rootOf(t)
	if !installsCABundle.MatchString(runtimeStage(t, readFile(t, filepath.Join(root, "docker", "go-api-tools.Dockerfile")))) {
		t.Fatal("the runtime stage of docker/go-api-tools.Dockerfile does not apt-get install ca-certificates")
	}
	planted := strings.Replace(readFile(t, filepath.Join(root, "docker", "go-api-tools.Dockerfile")), "ca-certificates", "curl", -1)
	if installsCABundle.MatchString(runtimeStage(t, planted)) {
		t.Fatal("a runtime stage without ca-certificates was accepted")
	}
}

func TestDockerImagesWorkflowChecksTheCABundleInTheBuiltImage(t *testing.T) {
	root := rootOf(t)
	if !strings.Contains(readFile(t, filepath.Join(root, ".github", "workflows", "docker-images.yml")), "/etc/ssl/certs/ca-certificates.crt") {
		t.Fatal("docker-images.yml no longer checks /etc/ssl/certs/ca-certificates.crt in the built image")
	}
}
