package docstools_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

// Carried from tests/docs/test_investment_drift.py and tests/docs/test_queue_mapping_drift.py (CHAOS-10374). The
// tests that run a verb on the real tree use the repository root as the work directory.

const (
	invTxBegin = "<!-- BEGIN GENERATED TAXONOMY -->"
	invTxEnd   = "<!-- END GENERATED TAXONOMY -->"
	invTxDoc   = "docs/reference/taxonomies/investment.md"
)

// startRoot is the repository root, found from the package directory where the test binary starts (not
// runtime.Caller: under -trimpath that holds no real path).
var startRoot = func() string {
	dir, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			panic("go.mod not found above the test directory")
		}
		dir = parent
	}
}()

func repoRoot(t *testing.T) string {
	t.Helper()
	return startRoot
}

// realBlock returns doc[BEGIN : END+len(END)] or fails.
func realBlock(t *testing.T, doc, begin, end string) string {
	t.Helper()
	start, stop := strings.Index(doc, begin), strings.Index(doc, end)
	if start == -1 || stop <= start {
		t.Fatalf("generated markers %q missing", begin)
	}
	return doc[start : stop+len(end)]
}

// seedFromRoot copies the named repository files into a fresh work directory and makes it the cwd.
func seedFromRoot(t *testing.T, rels ...string) string {
	t.Helper()
	root := repoRoot(t)
	work := t.TempDir()
	for _, rel := range rels {
		writeTree(t, filepath.Join(work, rel), mustRead(t, filepath.Join(root, rel)), 0o644)
	}
	t.Chdir(work)
	return work
}

func TestInvestmentTaxonomyDriftCheckExitsClean(t *testing.T) {
	t.Chdir(repoRoot(t))
	code, stdout, stderr := runDocs("check-investment-drift")
	if code != 0 {
		t.Fatalf("Investment docs drift check failed:\n%s\n%s", stdout, stderr)
	}
	if strings.Contains(stdout, "ERROR:") {
		t.Fatalf("drift check reported errors:\n%s", stdout)
	}
}

func TestCanonicalTaxonomyGeneratedBlockMatchesRegistry(t *testing.T) {
	root := repoRoot(t)
	actual := realBlock(t, mustRead(t, filepath.Join(root, invTxDoc)), invTxBegin, invTxEnd)
	seedFromRoot(t, invTxDoc) // read-only for the real tree: the generator runs on a copy
	if code, _, stderr := runDocs("gen-taxonomy"); code != 0 {
		t.Fatalf("gen-taxonomy rc %d: %s", code, stderr)
	}
	expected := realBlock(t, mustRead(t, invTxDoc), invTxBegin, invTxEnd)
	if actual != expected {
		t.Fatalf("Generated block in %s is stale. Run 'cigate docs gen-taxonomy' and commit the result.", invTxDoc)
	}
}

func TestCanonicalPublicTaxonomyDocumentsCurrentVocabulary(t *testing.T) {
	content := strings.ToLower(mustRead(t, filepath.Join(repoRoot(t), invTxDoc)))
	raw := mustRead(t, filepath.Join(repoRoot(t), invTxDoc))
	for _, want := range []string{"canonical", "not workspace-configurable", "must not define a competing vocabulary"} {
		if !strings.Contains(content, want) {
			t.Errorf("canonical taxonomy page must contain %q", want)
		}
	}
	if !strings.Contains(raw, "internal/jobs/workgraph/units/taxonomy.go") {
		t.Errorf("canonical taxonomy page must reference the canonical source")
	}
	for _, theme := range units.SortedThemes {
		if !strings.Contains(raw, "`"+theme+"`") {
			t.Errorf("theme key %q is not documented in the canonical taxonomy page", theme)
		}
	}
}

func TestInvestmentTaxonomyFixtureReportsOperationalExternal(t *testing.T) {
	fixture := mustRead(t, filepath.Join("testdata", "operational-external.md"))
	work := seedFromRoot(t, invTxDoc)
	writeTree(t, filepath.Join(work, "docs", "operational-external.md"), fixture, 0o644)
	code, stdout, _ := runDocs("check-investment-drift")
	want := "ERROR: operational-external.md JSON example #1 unknown subcategories keys: ['operational.external']\n"
	if code != 1 || stdout != want {
		t.Fatalf("rc %d stdout %q, want %q", code, stdout, want)
	}
}

const qmRealDoc = "docs/contribute/architecture/go-worker-runtime.md"

func TestQueueMappingDriftCheckExitsClean(t *testing.T) {
	t.Chdir(repoRoot(t))
	code, stdout, stderr := runDocs("check-queue-mapping-drift")
	if code != 0 {
		t.Fatalf("Queue mapping docs drift check failed:\n%s\n%s", stdout, stderr)
	}
	if strings.Contains(stdout, "ERROR:") {
		t.Fatalf("drift check reported errors:\n%s", stdout)
	}
}

func TestGoWorkerRuntimeGeneratedBlockMatchesProducers(t *testing.T) {
	const begin, end = "<!-- BEGIN GENERATED QUEUE MAP -->", "<!-- END GENERATED QUEUE MAP -->"
	root := repoRoot(t)
	actual := realBlock(t, mustRead(t, filepath.Join(root, qmRealDoc)), begin, end)
	seedFromRoot(t, qmRealDoc, "deploy/go-workers/deployment.json",
		"contracts/jobs/v1/registry.json", "contracts/jobs/v1/migration-state.json")
	if code, _, stderr := runDocs("gen-queue-mapping"); code != 0 {
		t.Fatalf("gen-queue-mapping rc %d: %s", code, stderr)
	}
	if expected := realBlock(t, mustRead(t, qmRealDoc), begin, end); actual != expected {
		t.Fatalf("Generated block in %s is stale. Run 'cigate docs gen-queue-mapping' and commit the result.", qmRealDoc)
	}
}

func TestGoWorkerRuntimeDoesNotEnshrineWorkerEnabledSwitches(t *testing.T) {
	doc := mustRead(t, filepath.Join(repoRoot(t), qmRealDoc))
	if !strings.Contains(doc, "CHAOS-4054") {
		t.Error("go-worker-runtime.md must reference the CHAOS-4054 two-plane decision record")
	}
	if !strings.Contains(doc, "are **deleted**") && !strings.Contains(doc, "surface is deleted") {
		t.Error("go-worker-runtime.md must state that the WORKER_*_ENABLED surface is deleted")
	}
	for _, phrase := range []string{"being retired", "is dying", "on its way out"} {
		if strings.Contains(doc, phrase) {
			t.Errorf("go-worker-runtime.md still describes the route-switch surface as in-flight (%q)", phrase)
		}
	}
	if !strings.Contains(doc, "IntegrationDataset.is_enabled") || !strings.Contains(doc, "-Q") {
		t.Error("go-worker-runtime.md must name both replacement planes: IntegrationDataset.is_enabled and -Q topology")
	}
}
