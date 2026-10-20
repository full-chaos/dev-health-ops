package docstools_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// queuemapSeed copies the recorded gen-queue-mapping-ok inputs into a fresh work directory and makes it the cwd.
func queuemapSeed(t *testing.T) string {
	t.Helper()
	work := t.TempDir()
	in, err := filepath.Abs(filepath.Join("testdata", "parity", "genqm-ok", "in"))
	if err != nil {
		t.Fatal(err)
	}
	_ = filepath.Walk(in, func(path string, info os.FileInfo, err error) error {
		if err == nil && info.Mode().IsRegular() {
			rel, _ := filepath.Rel(in, path)
			writeTree(t, filepath.Join(work, rel), mustRead(t, path), 0o644)
		}
		return nil
	})
	t.Chdir(work)
	return work
}

const queuemapDoc = "docs/contribute/architecture/go-worker-runtime.md"

func TestQueuemapGenerateIsIdempotentAndDriftCheckPasses(t *testing.T) {
	queuemapSeed(t)
	if code, _, stderr := runDocs("gen-queue-mapping"); code != 0 {
		t.Fatalf("first generate rc %d: %s", code, stderr)
	}
	first := mustRead(t, queuemapDoc)
	if code, _, stderr := runDocs("gen-queue-mapping"); code != 0 {
		t.Fatalf("second generate rc %d: %s", code, stderr)
	}
	if second := mustRead(t, queuemapDoc); second != first {
		t.Fatalf("generator not idempotent")
	}
	if code, out, _ := runDocs("check-queue-mapping-drift"); code != 0 || !strings.Contains(out, "passed") {
		t.Fatalf("clean tree: rc %d out %q", code, out)
	}
}

func TestQueuemapDriftDetectedAndRepaired(t *testing.T) {
	queuemapSeed(t)
	if code, out, _ := runDocs("check-queue-mapping-drift"); code != 1 || !strings.Contains(out, "is stale") {
		t.Fatalf("stale doc: rc %d out %q", code, out)
	}
	runDocs("gen-queue-mapping")
	// a producer change makes the published block stale again
	reg := mustRead(t, "contracts/jobs/v1/registry.json")
	writeTree(t, "contracts/jobs/v1/registry.json", strings.Replace(reg, "7200", "7201", 1), 0o644)
	if code, out, _ := runDocs("check-queue-mapping-drift"); code != 1 || !strings.Contains(out, "is stale") {
		t.Fatalf("producer drift: rc %d out %q", code, out)
	}
	runDocs("gen-queue-mapping")
	if code, _, _ := runDocs("check-queue-mapping-drift"); code != 0 {
		t.Fatalf("regenerated doc still drifts")
	}
}

func TestQueuemapRefusals(t *testing.T) {
	cases := []struct {
		name, file, from, to, want string
	}{
		{"missing markers", queuemapDoc, "BEGIN GENERATED QUEUE MAP", "BEGIN X", "markers"},
		{"unknown process", "deploy/go-workers/deployment.json", `"name": "reconciler"`, `"name": "reconciler2"`, "process set"},
		{"kind without migration row", "contracts/jobs/v1/migration-state.json", `"kind": "webhooks.w"`, `"kind": "webhooks.z"`, "kinds"},
		{"queue without curated entry", "deploy/go-workers/deployment.json", `"sync_provider"`, `"sync_other"`, "no qmCorrespondence entry"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			queuemapSeed(t)
			writeTree(t, c.file, strings.Replace(mustRead(t, c.file), c.from, c.to, 1), 0o644)
			code, _, stderr := runDocs("gen-queue-mapping")
			if code != 1 || !strings.Contains(stderr, c.want) {
				t.Fatalf("generate: rc %d stderr %q", code, stderr)
			}
			code, out, _ := runDocs("check-queue-mapping-drift")
			if code != 1 || !strings.Contains(out, "ERROR:") {
				t.Fatalf("drift: rc %d out %q", code, out)
			}
		})
	}
}
