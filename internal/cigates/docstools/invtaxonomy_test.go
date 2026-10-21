package docstools_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cigates/docstools"
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

// investmentBlockStale renders the block from the registry on a copy of the page and compares it with actual;
// it returns the guard's failure message, or "" when they are equal.
func investmentBlockStale(t *testing.T, actual string) string {
	t.Helper()
	seedFromRoot(t, invTxDoc) // read-only for the real tree: the generator runs on a copy
	if code, _, stderr := runDocs("gen-taxonomy"); code != 0 {
		t.Fatalf("gen-taxonomy rc %d: %s", code, stderr)
	}
	if expected := realBlock(t, mustRead(t, invTxDoc), invTxBegin, invTxEnd); actual != expected {
		return "Generated block in " + invTxDoc + " is stale. Run 'cigate docs gen-taxonomy' and commit the result."
	}
	return ""
}

func TestCanonicalTaxonomyGeneratedBlockMatchesRegistry(t *testing.T) {
	actual := realBlock(t, mustRead(t, filepath.Join(repoRoot(t), invTxDoc)), invTxBegin, invTxEnd)
	if msg := investmentBlockStale(t, actual); msg != "" {
		t.Fatal(msg)
	}
}

// Planted producer defect: the registry maps feature_delivery.customer to risk. Both the block guard and the
// drift verb must fail on the unchanged page.
func TestInvestmentGuardsFailWhenTheRegistryMapsCustomerToRisk(t *testing.T) {
	root := repoRoot(t)
	actual := realBlock(t, mustRead(t, filepath.Join(root, invTxDoc)), invTxBegin, invTxEnd)
	defer docstools.SetThemeOfForTest(func(s string) string {
		if s == "feature_delivery.customer" {
			return "risk"
		}
		return units.ThemeOf(s)
	})()
	want := "Generated block in docs/reference/taxonomies/investment.md is stale."
	if msg := investmentBlockStale(t, actual); !strings.HasPrefix(msg, want) {
		t.Fatalf("block guard passed the planted map (message %q, want prefix %q)", msg, want)
	}
	t.Chdir(root)
	code, stdout, _ := runDocs("check-investment-drift")
	if code != 1 || !strings.Contains(stdout, "generated subcategory mapping drift") || !strings.Contains(stdout, "'feature_delivery.customer': 'risk'") {
		t.Fatalf("check-investment-drift passed the planted map: rc %d\n%s", code, stdout)
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

const qmBegin, qmEnd = "<!-- BEGIN GENERATED QUEUE MAP -->", "<!-- END GENERATED QUEUE MAP -->"

var qmProducers = []string{"deploy/go-workers/deployment.json", "contracts/jobs/v1/registry.json", "contracts/jobs/v1/migration-state.json"}

// queueMapBlockStale renders the block from the real producers on a copy and compares it with actual; it returns
// the guard's failure message, or "" when they are equal.
func queueMapBlockStale(t *testing.T, actual string) string {
	t.Helper()
	seedFromRoot(t, append([]string{qmRealDoc}, qmProducers...)...)
	if code, _, stderr := runDocs("gen-queue-mapping"); code != 0 {
		t.Fatalf("gen-queue-mapping rc %d: %s", code, stderr)
	}
	if expected := realBlock(t, mustRead(t, qmRealDoc), qmBegin, qmEnd); actual != expected {
		return "Generated block in " + qmRealDoc + " is stale. Run 'cigate docs gen-queue-mapping' and commit the result."
	}
	return ""
}

func TestGoWorkerRuntimeGeneratedBlockMatchesProducers(t *testing.T) {
	actual := realBlock(t, mustRead(t, filepath.Join(repoRoot(t), qmRealDoc)), qmBegin, qmEnd)
	if msg := queueMapBlockStale(t, actual); msg != "" {
		t.Fatal(msg)
	}
}

// Planted page defect: a line "STALE QUEUE MAPPING" inside the generated block. Both the block guard and the drift
// verb must fail.
func TestQueueMapGuardsFailOnAStaleLineInTheBlock(t *testing.T) {
	root := repoRoot(t)
	doc := mustRead(t, filepath.Join(root, qmRealDoc))
	at := strings.Index(doc, qmBegin)
	if at == -1 {
		t.Fatal("queue-map begin marker missing")
	}
	at += len(qmBegin)
	planted := doc[:at] + "\nSTALE QUEUE MAPPING" + doc[at:]
	if !strings.Contains(realBlock(t, planted, qmBegin, qmEnd), "STALE QUEUE MAPPING") {
		t.Fatal("the plant did not land inside the generated block")
	}
	want := "Generated block in docs/contribute/architecture/go-worker-runtime.md is stale."
	if msg := queueMapBlockStale(t, realBlock(t, planted, qmBegin, qmEnd)); !strings.HasPrefix(msg, want) {
		t.Fatalf("block guard passed the planted line (message %q, want prefix %q)", msg, want)
	}
	work := seedFromRoot(t, qmProducers...)
	writeTree(t, filepath.Join(work, qmRealDoc), planted, 0o644)
	code, stdout, _ := runDocs("check-queue-mapping-drift")
	if code != 1 || !strings.Contains(stdout, "is stale") {
		t.Fatalf("check-queue-mapping-drift passed the planted line: rc %d\n%s", code, stdout)
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
