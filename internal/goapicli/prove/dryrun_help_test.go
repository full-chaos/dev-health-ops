package prove

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/goapidigest"
	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

// CHAOS-7440: the help text of -dry-run said "execute and compare, but write NO receipts" while the run opens no
// database, reads no routing row, refuses every operation and measures nothing. This test runs -dry-run end to
// end and pins what it does (no pool is opened, no operation request reaches the edge, nothing executed, no
// receipt written, the run ends in ErrNothingMeasured), then pins that the help text says exactly that and does
// not claim execution or comparison.
func TestDryRunHelpTextAgreesWithWhatTheRunDoes(t *testing.T) {
	var graphqlHits atomic.Int32
	args, reportPath := exitFixture(t, func(w http.ResponseWriter, _ bool) {
		graphqlHits.Add(1)
		w.WriteHeader(http.StatusOK)
	})
	var kept []string
	for _, arg := range args {
		if !strings.HasPrefix(arg, "-postgres-uri") {
			kept = append(kept, arg)
		}
	}
	kept = append(kept, "-dry-run")
	var opened atomic.Int32
	original := openPostgresPool
	t.Cleanup(func() { openPostgresPool = original })
	openPostgresPool = func(context.Context, string) (dbPool, error) {
		opened.Add(1)
		return nil, errors.New("a dry run must open no database")
	}

	err := runCLI(t, kept)
	if !errors.Is(err, goapiproof.ErrNothingMeasured) {
		t.Fatalf("a dry run ended with %v, want the measured-nothing error", err)
	}
	if opened.Load() != 0 {
		t.Fatalf("a dry run opened %d database connection(s)", opened.Load())
	}
	if graphqlHits.Load() != 0 {
		t.Fatalf("a dry run sent %d operation request(s) to the edge", graphqlHits.Load())
	}
	raw, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatal(err)
	}
	var report struct {
		Summary struct {
			Attempted       int            `json:"attempted"`
			Executed        int            `json:"executed"`
			Refused         int            `json:"refused"`
			ReceiptsWritten int            `json:"receipts_written"`
			ByRefusalReason map[string]int `json:"by_refusal_reason"`
		} `json:"summary"`
	}
	if err := json.Unmarshal(raw, &report); err != nil {
		t.Fatalf("the report does not decode: %v", err)
	}
	if report.Summary.Attempted == 0 || report.Summary.Refused != report.Summary.Attempted || report.Summary.Executed != 0 || report.Summary.ReceiptsWritten != 0 {
		t.Fatalf("a dry run's summary = %+v, want every attempted operation refused, none executed, no receipt", report.Summary)
	}
	// "refused as not routed": the one operation the fixture gives a matching document (featureFlags) is refused for the
	// routing reason, which the help text names; the others have no document in the fixture.
	if report.Summary.ByRefusalReason[goapiproof.RefusalNotRouted] != 1 {
		t.Fatalf("a dry run's refusal reasons = %v, want exactly one %s (the documented operation)", report.Summary.ByRefusalReason, goapiproof.RefusalNotRouted)
	}

	fs, _ := registerFlags()
	flag := fs.Lookup("dry-run")
	if flag == nil {
		t.Fatal("no -dry-run flag")
	}
	usage := strings.ToLower(flag.Usage)
	for _, fact := range []string{"no database", "no receipts", "refused", "nothing is executed or compared", "measured-nothing error", "with -mcp-roots the class proof still runs", "only the receipt write is skipped", "-mcp-reference doc-route refuses a dry run", "unless -go-edge"} {
		if !strings.Contains(usage, fact) {
			t.Errorf("the -dry-run help text does not say %q, which is what the run does: %q", fact, flag.Usage)
		}
	}
	// No other claim of execution, comparison or measurement may be in the text: strip the statements the runs above
	// prove and nothing with these stems may remain (so a sentence such as "executes operations" fails, not only one phrase).
	residue := strings.NewReplacer("nothing is executed or compared", "", "measured-nothing error", "", "reports what it measured", "").Replace(usage)
	for _, stem := range []string{"execut", "compar", "measur"} {
		if strings.Contains(residue, stem) {
			t.Errorf("the -dry-run help text claims something with %q beyond the proven statements: %q", stem, flag.Usage)
		}
	}
}

// TestDryRunWithMCPRootsExecutesTheClassProofAndWritesNoReceipt pins the exception the help text names: with -mcp-roots
// the dry run is not a refusal run. The class proof sends its request to -proof-url and the reference to the edge,
// reports what it measured, opens no database and writes no receipt.
func TestDryRunWithMCPRootsExecutesTheClassProofAndWritesNoReceipt(t *testing.T) {
	withProverCommit(t, e2eBuildSHA)
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
	var proofHits, edgeHits atomic.Int32
	proofAnswer := hotspotsBody(hotspotsRow("a.go", "r1", 3, "0.5"))
	edgeAnswer := hotspotsBody(hotspotsRow("a.go", "r1", 4, "0.5"))
	proof := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proofHits.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("x-dev-health-plane", "go")
		w.Header().Set("x-dev-health-build", e2eBuildSHA)
		_, _ = io.WriteString(w, proofAnswer)
	}))
	t.Cleanup(proof.Close)
	edge := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == goapiproof.ReferencePrincipalPath {
			w.Header().Set("Server", goapiproof.ReferencePlaneServer)
			_, _ = w.Write([]byte(`{"org_id":"70d529e0"}`))
			return
		}
		edgeHits.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("x-dev-health-plane", "python")
		w.Header().Set("x-dev-health-build", e2eBuildSHA)
		_, _ = io.WriteString(w, edgeAnswer)
	}))
	t.Cleanup(edge.Close)
	docsJSON, _ := json.Marshal([]map[string]string{{"operation": "hotspots", "document": hotspotsDocument}})
	docsPath := filepath.Join(t.TempDir(), "documents.json")
	if err := os.WriteFile(docsPath, docsJSON, 0o600); err != nil {
		t.Fatal(err)
	}
	withOrgMinter(t, "70d529e0", "70d529e0")
	var opened atomic.Int32
	original := openPostgresPool
	t.Cleanup(func() { openPostgresPool = original })
	openPostgresPool = func(context.Context, string) (dbPool, error) {
		opened.Add(1)
		return nil, errors.New("a dry run must open no database")
	}
	reportPath := filepath.Join(t.TempDir(), "report.json")
	err := runCLI(t, []string{
		"-registry-url=" + registry.URL + "/registry",
		"-buildinfo-url=" + buildinfo.URL + "/buildinfo",
		"-edge-url=" + edge.URL + "/graphql",
		"-proof-url=" + proof.URL + "/query/proof-mcp",
		"-mcp-roots=mcp:hotspots",
		"-documents=" + docsPath,
		"-org=70d529e0",
		"-artifact-dir=" + t.TempDir(),
		"-recorded-by=harness", "-review-evidence=dry run mcp harness",
		"-report=" + reportPath,
		"-timeout=5s",
		"-dry-run",
	})
	if err != nil {
		t.Fatalf("an MCP class dry run ended with %v", err)
	}
	if opened.Load() != 0 {
		t.Fatalf("an MCP class dry run opened %d database connection(s)", opened.Load())
	}
	if proofHits.Load() == 0 || edgeHits.Load() == 0 {
		t.Fatalf("an MCP class dry run sent %d request(s) to -proof-url and %d to the edge: it measures through both", proofHits.Load(), edgeHits.Load())
	}
	raw, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatal(err)
	}
	var report struct {
		Summary struct {
			Executed        int `json:"executed"`
			ReceiptsWritten int `json:"receipts_written"`
		} `json:"summary"`
	}
	if err := json.Unmarshal(raw, &report); err != nil {
		t.Fatalf("the report does not decode: %v", err)
	}
	if report.Summary.Executed == 0 || report.Summary.ReceiptsWritten != 0 {
		t.Fatalf("an MCP class dry run's summary = %+v, want something executed and no receipt", report.Summary)
	}
}

// dryRunCounters counts what a dry run sends to each fake server.
type dryRunCounters struct{ registry, buildinfo, principal, posts, otherGets atomic.Int32 }

// dryRunFixture is a registry, a /buildinfo and an edge that count every request, plus the documents file.
func dryRunFixture(t *testing.T) (args []string, counters *dryRunCounters) {
	t.Helper()
	withProverCommit(t, e2eBuildSHA)
	counters = new(dryRunCounters)
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
	registry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		counters.registry.Add(1)
		writeStaticJSONHandler(string(registryBody))(w, r)
	}))
	t.Cleanup(registry.Close)
	buildinfo := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		counters.buildinfo.Add(1)
		writeStaticJSONHandler(`{"commit":"`+e2eBuildSHA+`","modified":false}`)(w, r)
	}))
	t.Cleanup(buildinfo.Close)
	edge := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == goapiproof.ReferencePrincipalPath:
			counters.principal.Add(1)
			w.Header().Set("Server", goapiproof.ReferencePlaneServer)
			_, _ = w.Write([]byte(`{"org_id":"70d529e0"}`))
		case r.Method == http.MethodPost:
			counters.posts.Add(1)
			_, _ = io.Copy(io.Discard, r.Body)
			w.WriteHeader(http.StatusNotFound)
		default:
			counters.otherGets.Add(1)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(edge.Close)
	docsJSON, _ := json.Marshal([]map[string]string{{"operation": "hotspots", "document": hotspotsDocument}})
	docsPath := filepath.Join(t.TempDir(), "documents.json")
	if err := os.WriteFile(docsPath, docsJSON, 0o600); err != nil {
		t.Fatal(err)
	}
	withOrgMinter(t, "70d529e0", "70d529e0")
	original := openPostgresPool
	t.Cleanup(func() { openPostgresPool = original })
	openPostgresPool = func(context.Context, string) (dbPool, error) {
		t.Error("a dry run must open no database")
		return nil, errors.New("a dry run must open no database")
	}
	return []string{
		"-registry-url=" + registry.URL + "/registry",
		"-buildinfo-url=" + buildinfo.URL + "/buildinfo",
		"-edge-url=" + edge.URL + "/graphql",
		"-documents=" + docsPath,
		"-org=70d529e0",
		"-artifact-dir=" + t.TempDir(),
		"-recorded-by=harness", "-review-evidence=dry run flag modes",
		"-timeout=5s",
		"-dry-run",
	}, counters
}

// TestDryRunWithDocRouteIsRefusedBeforeAnyRequest pins "(-mcp-reference doc-route refuses a dry run)".
func TestDryRunWithDocRouteIsRefusedBeforeAnyRequest(t *testing.T) {
	args, counters := dryRunFixture(t)
	err := runCLI(t, append(args, "-mcp-roots=mcp:hotspots", "-mcp-reference=doc-route", "-proof-url=http://127.0.0.1:1/query/proof-mcp"))
	if err == nil || !strings.Contains(err.Error(), "cannot run with -dry-run") {
		t.Fatalf("a doc-route dry run ended with %v, want the dry-run refusal", err)
	}
	if got := counters.registry.Load() + counters.buildinfo.Load() + counters.principal.Load() + counters.posts.Load() + counters.otherGets.Load(); got != 0 {
		t.Fatalf("a doc-route dry run sent %d request(s) before refusing", got)
	}
}

// TestDryRunWithGoEdgeReadsNoReferencePrincipalAndPostsTheControlDocument pins "the reference-principal check too unless
// -go-edge": in Go-edge mode there is no Python plane to ask, and the edge still gets the control document.
func TestDryRunWithGoEdgeReadsNoReferencePrincipalAndPostsTheControlDocument(t *testing.T) {
	args, counters := dryRunFixture(t)
	_ = runCLI(t, append(args, "-go-edge"))
	if counters.principal.Load() != 0 {
		t.Fatalf("a -go-edge dry run read the reference principal %d time(s): the help text says it does not", counters.principal.Load())
	}
	if counters.posts.Load() != 1 {
		t.Fatalf("a -go-edge dry run POSTed %d request(s) to the edge, want exactly the control document", counters.posts.Load())
	}
	if counters.registry.Load() == 0 || counters.buildinfo.Load() == 0 {
		t.Fatalf("a dry run read the registry %d and /buildinfo %d time(s): the help text says it reads both", counters.registry.Load(), counters.buildinfo.Load())
	}
}
