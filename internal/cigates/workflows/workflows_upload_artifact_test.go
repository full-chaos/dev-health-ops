package workflows_test

import (
	"path/filepath"
	"strings"
	"testing"
)

// A diagnostic artifact upload must not fail a required shard: every job of these workflows feeds a required
// fan-in (go-queue-gate, image-queue-gate, go-quality, live-e2e, metrics-executed-proof), so an upload step
// either sets continue-on-error: true or is listed below with the reason it must keep failing.
var gatedUploadWorkflows = []string{"go.yml", "go-quality.yml", "live-e2e.yml", "docker-images.yml"}

var essentialUploads = map[string]string{
	"docker-images.yml/go-build/Upload digest":           "go-merge downloads the digests to build the manifest list",
	"docker-images.yml/go-build-dho-arm64/Upload digest": "go-merge downloads the digests to build the manifest list",
	"docker-images.yml/go-api-tools-build/Upload digest": "go-api-tools-merge downloads the digests to build the manifest list",
}

func uploadArtifactViolations(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	for _, name := range gatedUploadWorkflows {
		jobs := jobsOf(loadWf(t, root, name))
		for _, jobName := range sortedKeys(jobs) {
			for _, step := range stepsOf(jobs[jobName]) {
				if !strings.HasPrefix(asString(step["uses"]), "actions/upload-artifact@") {
					continue
				}
				id := filepath.ToSlash(filepath.Join(name, jobName, asString(step["name"])))
				_, essential := essentialUploads[id]
				if step["continue-on-error"] == true || essential {
					continue
				}
				out = append(out, id+": upload-artifact in a gated job needs continue-on-error: true or an essentialUploads row")
			}
		}
	}
	return out
}

func TestGatedUploadArtifactStepsCannotFailTheJob(t *testing.T) {
	expectNoViolations(t, uploadArtifactViolations(t, repoRoot(t)))
}

func TestGatedUploadArtifactPlantedDefect(t *testing.T) {
	root := repoRoot(t)
	const wf = ".github/workflows/go.yml"
	files := make([]string, 0, len(gatedUploadWorkflows))
	for _, n := range gatedUploadWorkflows {
		files = append(files, ".github/workflows/"+n)
	}
	planted := plantTree(t, root, files, mutation{wf, "      - name: Upload per-test timing json\n        continue-on-error: true\n", "      - name: Upload per-test timing json\n"})
	got := uploadArtifactViolations(t, planted)
	if !containsAll(got, "go.yml/go-storage-integration-shard/Upload per-test timing json") {
		t.Fatalf("planted defect not reported: %v", got)
	}
}
