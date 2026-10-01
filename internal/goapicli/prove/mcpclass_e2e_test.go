package prove

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/goapidigest"
	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
	"github.com/full-chaos/dev-health-ops/internal/mcpclass"
)

// The MCP class proof in doc-route reference mode, driven through run(): fake
// registry and /buildinfo, a fake receipt store, the MCP proof route and query-api's
// own /graphql as two Go servers. The receipt the command WRITES is what is asserted.

const hotspotsDocument = "query Hotspots { hotspots { rows { filePath repoId churnCommits30d blameConcentration riskScore } } }"

func goServer(t *testing.T, path, body string) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != path {
			http.NotFound(w, r)
			return
		}
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("x-dev-health-plane", "go")
		w.Header().Set("x-dev-health-build", e2eBuildSHA)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(server.Close)
	return server
}

func runClassDocRoute(t *testing.T, mcpAnswer, docRouteAnswer string, backed []string, extra ...string) (stdout string, runErr error, pool *e2ePool) {
	t.Helper()
	digest := goapidigest.Document(hotspotsDocument)
	type registryOp struct {
		Operation      string `json:"operation"`
		DocumentDigest string `json:"document_digest"`
	}
	var ops []registryOp
	for _, name := range goapiproof.KnownOperations() {
		entry := registryOp{Operation: name, DocumentDigest: "sha256:unused-" + name}
		if name == "hotspots" {
			entry.DocumentDigest = digest
		}
		ops = append(ops, entry)
	}
	registryBody, _ := json.Marshal(struct {
		SchemaDigest string       `json:"schema_digest"`
		Operations   []registryOp `json:"operations"`
	}{SchemaDigest: "sha256:e2e29d509cd", Operations: ops})
	registry := httptest.NewServer(writeStaticJSONHandler(string(registryBody)))
	t.Cleanup(registry.Close)
	buildinfo := httptest.NewServer(writeStaticJSONHandler(`{"commit":"` + e2eBuildSHA + `","modified":false}`))
	t.Cleanup(buildinfo.Close)
	proof := goServer(t, "/query/proof-mcp", mcpAnswer)
	edge := goServer(t, "/graphql", docRouteAnswer)

	docsJSON, _ := json.Marshal([]map[string]string{{"operation": "hotspots", "document": hotspotsDocument}})
	docsPath := filepath.Join(t.TempDir(), "documents.json")
	if err := os.WriteFile(docsPath, docsJSON, 0o600); err != nil {
		t.Fatal(err)
	}
	pool = &e2ePool{backedOps: backed}
	withFakePool(t, pool)
	withOrgMinter(t, "70d529e0", "70d529e0")
	stdout = captureStdout(t, func() {
		runErr = runCLI(t, append([]string{
			"-registry-url=" + registry.URL + "/registry",
			"-buildinfo-url=" + buildinfo.URL + "/buildinfo",
			"-edge-url=" + edge.URL + "/graphql",
			"-proof-url=" + proof.URL + "/query/proof-mcp",
			"-mcp-roots=mcp:hotspots", "-mcp-reference=doc-route",
			"-documents=" + docsPath,
			"-postgres-uri=postgres://fake/ignored",
			"-org=70d529e0",
			"-artifact-dir=" + t.TempDir(),
			"-recorded-by=harness", "-review-evidence=class doc-route harness",
			"-timeout=5s",
		}, extra...))
	})
	return stdout, runErr, pool
}

func classReceiptArgs(t *testing.T, pool *e2ePool) (op, document, terminal, evidence string) {
	t.Helper()
	inserts := pool.proofRunInserts()
	if len(inserts) != 1 {
		t.Fatalf("%d go_api_proof_run rows written, want exactly one class receipt", len(inserts))
	}
	a := inserts[0].args
	return a[3].(string), a[2].(string), a[7].(string), a[12].(string)
}

// Two Go pipelines agreeing, and the document operation receipt-backed: ONE class
// receipt, a match, keyed to the class digest and naming the reference.
func TestRunWritesAClassMatchReceiptWhenTheDocRouteAgreesAndTheOperationIsBacked(t *testing.T) {
	withProverCommit(t, e2eBuildSHA)
	body := hotspotsBody(hotspotsRow("a.go", "r1", 3, "0.5"))
	stdout, runErr, pool := runClassDocRoute(t, body, body, []string{"hotspots"})
	if runErr != nil {
		t.Fatalf("run(): %v\nstdout:\n%s", runErr, stdout)
	}
	op, document, terminal, evidence := classReceiptArgs(t, pool)
	if op != "mcp:hotspots" || document != mcpclass.DocumentDigest() || terminal != "match" {
		t.Fatalf("receipt op=%s document=%s terminal=%s, want a class match", op, document, terminal)
	}
	if !strings.Contains(evidence, `"reference":"go_document_route"`) || !strings.Contains(evidence, `"edge_mode":"doc_route"`) {
		t.Fatalf("receipt provenance %q does not name the reference", evidence)
	}
	if !strings.Contains(stdout, "mcp-class mcp:hotspots state=match") {
		t.Fatalf("stdout lacks the class verdict line:\n%s", stdout)
	}
	// CHAOS-7500: the verb's own mode line names the doc-route proof, never "undetermined" on a completed run.
	if !strings.Contains(stdout, "go-api-prove: edge_mode=doc_route (") || strings.Contains(stdout, "edge_mode=undetermined") {
		t.Fatalf("a completed doc-route run must say edge_mode=doc_route and never undetermined:\n%s", stdout)
	}
}

// The MCP pipeline and the document route disagreeing is a mismatch receipt, never a match.
func TestRunWritesAClassMismatchWhenTheTwoPipelinesDiffer(t *testing.T) {
	withProverCommit(t, e2eBuildSHA)
	_, _, pool := runClassDocRoute(t, hotspotsBody(hotspotsRow("a.go", "r1", 3, "0.5")), hotspotsBody(hotspotsRow("a.go", "r1", 4, "0.5")), []string{"hotspots"})
	if _, _, terminal, _ := classReceiptArgs(t, pool); terminal != "mismatch" {
		t.Fatalf("terminal = %s, want mismatch", terminal)
	}
}

// A matching shape of a document operation that is NOT receipt-backed is excluded
// and named, and a root left with nothing counted writes no receipt at all.
func TestRunWritesNoClassReceiptWhenNoDocumentOperationIsReceiptBacked(t *testing.T) {
	withProverCommit(t, e2eBuildSHA)
	body := hotspotsBody(hotspotsRow("a.go", "r1", 3, "0.5"))
	stdout, _, pool := runClassDocRoute(t, body, body, nil)
	if got := len(pool.proofRunInserts()); got != 0 {
		t.Fatalf("%d receipts written, want none", got)
	}
	if !strings.Contains(stdout, "state=NOT_MEASURED") || !strings.Contains(stdout, "doc_operation_not_receipt_backed") {
		t.Fatalf("stdout does not say the shape was excluded for lack of a receipt:\n%s", stdout)
	}
}
