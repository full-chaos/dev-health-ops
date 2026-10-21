package docstools_test

import (
	"os"
	"path/filepath"
	"regexp"
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

// taxonomyPageDefects is every defect of the investment taxonomy page text: a missing vocabulary statement, a missing
// source reference or an undocumented theme key.
func taxonomyPageDefects(raw string) []string {
	var out []string
	content := strings.ToLower(raw)
	for _, want := range []string{"canonical", "not workspace-configurable", "must not define a competing vocabulary"} {
		if !strings.Contains(content, want) {
			out = append(out, "canonical taxonomy page must contain "+want)
		}
	}
	if !strings.Contains(raw, "internal/jobs/workgraph/units/taxonomy.go") {
		out = append(out, "canonical taxonomy page must reference the canonical source")
	}
	for _, theme := range units.SortedThemes {
		if !strings.Contains(raw, "`"+theme+"`") {
			out = append(out, "theme key "+theme+" is not documented in the canonical taxonomy page")
		}
	}
	return out
}

// plant returns doc with every case-insensitive occurrence of old replaced by repl; a plant that changes nothing is a
// test error, because it would prove nothing.
func plant(t *testing.T, doc, old, repl string) string {
	t.Helper()
	planted := regexp.MustCompile("(?i)"+regexp.QuoteMeta(old)).ReplaceAllLiteralString(doc, repl)
	if planted == doc {
		t.Fatalf("the plant of %q changed nothing: the page changed, re-derive the plant", old)
	}
	return planted
}

func TestCanonicalPublicTaxonomyDocumentsCurrentVocabulary(t *testing.T) {
	raw := mustRead(t, filepath.Join(repoRoot(t), invTxDoc))
	if got := taxonomyPageDefects(raw); len(got) > 0 {
		t.Fatalf("the real taxonomy page has defects: %q", got)
	}
	for _, c := range []struct{ name, old, repl, want string }{
		{"the canonical statement is removed", "canonical", "standard", "must contain canonical"},
		{"the workspace statement is changed", "not workspace-configurable", "workspace-configurable", "must contain not workspace-configurable"},
		{"the competing vocabulary rule is changed", "must not define a competing vocabulary", "may define a vocabulary", "must contain must not define a competing vocabulary"},
		{"the source reference is changed", "internal/jobs/workgraph/units/taxonomy.go", "the registry", "must reference the canonical source"},
		{"a theme key loses its backticks", "`risk`", "risk", "theme key risk is not documented"},
	} {
		t.Run("plant/"+c.name, func(t *testing.T) {
			got := strings.Join(taxonomyPageDefects(plant(t, raw, c.old, c.repl)), "\n")
			if !strings.Contains(got, c.want) {
				t.Fatalf("planted defect %q: defects %q do not mention %q", c.name, got, c.want)
			}
		})
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

// workerRuntimePageDefects is every defect of the go-worker-runtime page text: the WORKER_*_ENABLED surface must be
// described as deleted, never as in flight, and both replacement planes must be named.
func workerRuntimePageDefects(doc string) []string {
	var out []string
	if !strings.Contains(doc, "CHAOS-4054") {
		out = append(out, "go-worker-runtime.md must reference the CHAOS-4054 two-plane decision record")
	}
	if !strings.Contains(doc, "are **deleted**") && !strings.Contains(doc, "surface is deleted") {
		out = append(out, "go-worker-runtime.md must state that the WORKER_*_ENABLED surface is deleted")
	}
	for _, phrase := range []string{"being retired", "is dying", "on its way out"} {
		if strings.Contains(doc, phrase) {
			out = append(out, "go-worker-runtime.md still describes the route-switch surface as in-flight ("+phrase+")")
		}
	}
	if !strings.Contains(doc, "IntegrationDataset.is_enabled") || !strings.Contains(doc, "-Q") {
		out = append(out, "go-worker-runtime.md must name both replacement planes: IntegrationDataset.is_enabled and -Q topology")
	}
	return out
}

func TestGoWorkerRuntimeDoesNotEnshrineWorkerEnabledSwitches(t *testing.T) {
	doc := mustRead(t, filepath.Join(repoRoot(t), qmRealDoc))
	if got := workerRuntimePageDefects(doc); len(got) > 0 {
		t.Fatalf("the real go-worker-runtime page has defects: %q", got)
	}
	for _, c := range []struct{ name, old, repl, want string }{
		{"the decision record reference is removed", "CHAOS-4054", "the decision record", "must reference the CHAOS-4054"},
		{"the deleted statement is changed", "are **deleted**", "are **dying**", "must state that the WORKER_*_ENABLED surface is deleted"},
		{"the in-flight wording returns", "IntegrationDataset.is_enabled", "IntegrationDataset.is_enabled (being retired)", "route-switch surface as in-flight (being retired)"},
		{"the intent plane is no longer named", "IntegrationDataset.is_enabled", "the dataset flag", "must name both replacement planes"},
		{"the serving plane is no longer named", "-Q", "-X", "must name both replacement planes"},
	} {
		t.Run("plant/"+c.name, func(t *testing.T) {
			planted := plant(t, doc, c.old, c.repl)
			if c.old == "are **deleted**" {
				planted = strings.ReplaceAll(planted, "surface is deleted", "surface is gone")
			}
			got := strings.Join(workerRuntimePageDefects(planted), "\n")
			if !strings.Contains(got, c.want) {
				t.Fatalf("planted defect %q: defects %q do not mention %q", c.name, got, c.want)
			}
		})
	}
}
