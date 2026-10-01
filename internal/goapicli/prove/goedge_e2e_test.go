package prove

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/goapidigest"
	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

// The command in Go-edge mode, driven through run() like e2e_test.go:
// fake registry and /buildinfo, a fake receipt store, and an edge that is
// query-api answering alone -- no Python plane and no "who am I" route.

// queryAPIAloneEdge is query-api's own /graphql: a registered document is
// served by plane go from the named build, every other document is refused
// 404 UNREGISTERED_DOCUMENT, and the Python app's reference route does not
// exist. asked counts requests for that route.
func queryAPIAloneEdge(t *testing.T, asked *int) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == goapiproof.ReferencePrincipalPath {
			*asked++
			http.NotFound(w, r)
			return
		}
		raw, _ := io.ReadAll(r.Body)
		var parsed struct {
			Query string `json:"query"`
		}
		_ = json.Unmarshal(raw, &parsed)
		w.Header().Set("x-dev-health-plane", "go")
		w.Header().Set("x-dev-health-build", e2eBuildSHA)
		w.Header().Set("Content-Type", "application/json")
		if parsed.Query != goEdgeFeatureFlagsDocument {
			w.WriteHeader(http.StatusNotFound)
			_, _ = fmt.Fprintf(w, `{"errors":[{"message":"This GraphQL document is not registered.","extensions":{"code":%q}}],"data":null}`, goapiproof.UnregisteredDocumentCode)
			return
		}
		_, _ = io.WriteString(w, `{"data":{"featureFlags":[{"key":"a"}]}}`)
	}))
	t.Cleanup(server.Close)
	return server
}

const goEdgeFeatureFlagsDocument = "query FeatureFlags { featureFlags { key } }"

// runAgainstEdge drives run() for the one featureFlags operation against
// edgeURL; extraArgs are the operator's own extra flags.
func runAgainstEdge(t *testing.T, edgeURL string, extraArgs ...string) (stdout string, runErr error, reportPath string, pool *e2ePool) {
	t.Helper()
	digest := goapidigest.Document(goEdgeFeatureFlagsDocument)
	type registryOp struct {
		Operation      string `json:"operation"`
		DocumentDigest string `json:"document_digest"`
	}
	var ops []registryOp
	for _, name := range goapiproof.KnownOperations() {
		entry := registryOp{Operation: name, DocumentDigest: "sha256:unused-" + name}
		if name == "featureFlags" {
			entry.DocumentDigest = digest
		}
		ops = append(ops, entry)
	}
	registryBody, err := json.Marshal(struct {
		SchemaDigest string       `json:"schema_digest"`
		Operations   []registryOp `json:"operations"`
	}{SchemaDigest: "sha256:e2e29d509cd", Operations: ops})
	if err != nil {
		t.Fatal(err)
	}
	registry := httptest.NewServer(writeStaticJSONHandler(string(registryBody)))
	t.Cleanup(registry.Close)
	buildinfo := httptest.NewServer(writeStaticJSONHandler(`{"commit":"` + e2eBuildSHA + `","modified":false}`))
	t.Cleanup(buildinfo.Close)

	docsJSON, err := json.Marshal([]map[string]string{{"operation": "featureFlags", "document": goEdgeFeatureFlagsDocument}})
	if err != nil {
		t.Fatal(err)
	}
	docsPath := filepath.Join(t.TempDir(), "documents.json")
	if err := os.WriteFile(docsPath, docsJSON, 0o600); err != nil {
		t.Fatal(err)
	}
	pool = &e2ePool{routingRows: [][]any{{"featureFlags", digest, "canary", e2eBuildSHA}}}
	withFakePool(t, pool)
	withOrgMinter(t, "70d529e0", "70d529e0")
	reportPath = filepath.Join(t.TempDir(), "report.json")
	stdout = captureStdout(t, func() {
		runErr = runCLI(t, append([]string{
			"-registry-url=" + registry.URL + "/registry",
			"-buildinfo-url=" + buildinfo.URL + "/buildinfo",
			"-edge-url=" + edgeURL + "/graphql",
			"-documents=" + docsPath,
			"-postgres-uri=postgres://fake/ignored",
			"-org=70d529e0",
			"-artifact-dir=" + t.TempDir(),
			"-recorded-by=harness",
			"-review-evidence=go-edge harness run",
			"-report=" + reportPath,
			"-timeout=5s",
		}, extraArgs...))
	})
	return stdout, runErr, reportPath, pool
}

// TestRunInGoEdgeModeProvesAgainstQueryAPIAlone: with -go-edge the command
// proves the operation against an edge that has no Python plane, never asks
// the Python app who the caller is, and says the mode on the run line, on
// the verdict line, on the count, in the report and in the receipt.
func TestRunInGoEdgeModeProvesAgainstQueryAPIAlone(t *testing.T) {
	withProverCommit(t, e2eBuildSHA)
	asked := 0
	edge := queryAPIAloneEdge(t, &asked)
	stdout, runErr, reportPath, pool := runAgainstEdge(t, edge.URL, "-go-edge")
	if runErr != nil {
		t.Fatalf("run(): %v\nstdout:\n%s", runErr, stdout)
	}
	if asked != 0 {
		t.Errorf("the Python reference route was asked %d time(s); Go-edge mode has no Python app to ask", asked)
	}
	for _, want := range []string{
		"go-api-prove: edge_mode=go (Go-edge:",
		goapiproof.VerdictGoOnly + " (go-edge mode: the candidate alone, the edge is query-api, no Python answer was read)",
		"go-api-prove:   " + goapiproof.VerdictGoOnly + " = 1 (go-edge mode)",
		"receipts_written=1",
	} {
		if !strings.Contains(stdout, want) {
			t.Errorf("stdout lacks %q:\n%s", want, stdout)
		}
	}
	raw, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatal(err)
	}
	var report struct {
		Summary struct {
			EdgeMode     string `json:"edge_mode"`
			ProvenGoOnly int    `json:"proven_go_only"`
		} `json:"summary"`
	}
	if err := json.Unmarshal(raw, &report); err != nil || report.Summary.EdgeMode != goapiproof.EdgeModeGo || report.Summary.ProvenGoOnly != 1 {
		t.Errorf("report summary %+v (err %v), want edge_mode go and one go-only proof:\n%s", report.Summary, err, raw)
	}
	inserts := pool.proofRunInserts()
	if len(inserts) != 1 || !strings.Contains(fmt.Sprint(inserts[0].args...), `"edge_mode":"go"`) {
		t.Errorf("want one go_api_proof_run insert whose provenance says edge_mode go, got %+v", inserts)
	}
}

// TestRunWithoutTheFlagRefusesAnEdgeWithNoPythonPlane: the mode is never
// detected. Against the same edge with no -go-edge, the run is the
// Python-reference one and stops at the reference check; nothing is proven.
func TestRunWithoutTheFlagRefusesAnEdgeWithNoPythonPlane(t *testing.T) {
	withProverCommit(t, e2eBuildSHA)
	asked := 0
	edge := queryAPIAloneEdge(t, &asked)
	stdout, runErr, _, pool := runAgainstEdge(t, edge.URL)
	if !errors.Is(runErr, goapiproof.ErrReferencePrincipal) || asked == 0 {
		t.Fatalf("run() = %v, reference route asked %d time(s); want the Python-reference refusal\nstdout:\n%s", runErr, asked, stdout)
	}
	if strings.Contains(stdout, goapiproof.VerdictGoOnly+" (") || len(pool.proofRunInserts()) != 0 {
		t.Errorf("nothing may be proven or written:\n%s", stdout)
	}
}

// pythonPlaneEdge is the edge the Python-reference mode proves: a registered
// document is served by plane go, and every other document is answered by
// the Python plane behind it.
func pythonPlaneEdge(t *testing.T) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var parsed struct {
			Query string `json:"query"`
		}
		_ = json.Unmarshal(raw, &parsed)
		w.Header().Set("Content-Type", "application/json")
		if parsed.Query == goEdgeFeatureFlagsDocument {
			w.Header().Set("x-dev-health-plane", "go")
			w.Header().Set("x-dev-health-build", e2eBuildSHA)
			_, _ = io.WriteString(w, `{"data":{"featureFlags":[{"key":"a"}]}}`)
			return
		}
		w.Header().Set("x-dev-health-plane", "python")
		_, _ = io.WriteString(w, `{"data":{"__typename":"Query"}}`)
	}))
	t.Cleanup(server.Close)
	return server
}

// TestRunInGoEdgeModeRefusesAnEdgeWithAPythonPlane: -go-edge against the
// Python edge (its control document is answered by plane python) stops
// before any case, by name.
func TestRunInGoEdgeModeRefusesAnEdgeWithAPythonPlane(t *testing.T) {
	withProverCommit(t, e2eBuildSHA)
	stdout, runErr, _, pool := runAgainstEdge(t, pythonPlaneEdge(t).URL, "-go-edge")
	if !errors.Is(runErr, goapiproof.ErrGoEdge) || !strings.Contains(runErr.Error(), goapiproof.RefusalGoEdgeControlOtherPlane) {
		t.Fatalf("run() = %v, want %s\nstdout:\n%s", runErr, goapiproof.RefusalGoEdgeControlOtherPlane, stdout)
	}
	if !strings.Contains(stdout, "executed=0") || len(pool.proofRunInserts()) != 0 {
		t.Errorf("nothing may be executed or written:\n%s", stdout)
	}
}

// TestEdgeModeLinesNameTheMode: the Python-reference run says its mode too,
// and its go-only verdict keeps its own words.
func TestEdgeModeLinesNameTheMode(t *testing.T) {
	if line := edgeModeLine(goapiproof.EdgeModePython); !strings.HasPrefix(line, "edge_mode=python ") {
		t.Errorf("python-reference mode line %q", line)
	}
	if note := goEdgeCountNote(goapiproof.EdgeModePython); note != "" {
		t.Errorf("python-reference count note %q, want none", note)
	}
	pythonLine := executedOutcomeLine(goapiproof.Outcome{Operation: "featureFlags", ProvenUnder: goapiproof.ProvenUnderGoOnly})
	goLine := executedOutcomeLine(goapiproof.Outcome{Operation: "featureFlags", ProvenUnder: goapiproof.ProvenUnderGoOnly, EdgeMode: goapiproof.EdgeModeGo})
	if !strings.Contains(pythonLine, goapiproof.VerdictGoOnly+" (no two-plane baseline)") || strings.Contains(pythonLine, "go-edge") {
		t.Errorf("python-reference go-only line %q", pythonLine)
	}
	if !strings.Contains(goLine, "go-edge mode") || strings.Contains(goLine, "no two-plane baseline") {
		t.Errorf("go-edge go-only line %q", goLine)
	}
}

// TestAPythonReferenceRunIsUnchangedExceptForItsModeLine: without -go-edge
// the command is the run it always was. It still asks the Python app who
// the caller is and proves the same two operations with the same verdicts
// and receipts; no receipt mentions an edge mode (the field is written only
// by a Go-edge run). The only additions are the mode line on stdout and the
// summary's edge_mode, both saying python.
func TestAPythonReferenceRunIsUnchangedExceptForItsModeLine(t *testing.T) {
	withProverCommit(t, e2eBuildSHA)
	stdout, runErr, reportPath, pool := runTwoOperationsEndToEnd(t)
	if runErr != nil {
		t.Fatalf("run(): %v\nstdout:\n%s", runErr, stdout)
	}
	assertTwoOperationsProven(t, stdout, reportPath, pool)
	if !strings.Contains(stdout, "go-api-prove: edge_mode=python (Python-reference:") || strings.Contains(stdout, "go-edge") {
		t.Errorf("a Python-reference run must say edge_mode=python and never go-edge:\n%s", stdout)
	}
	for _, insert := range pool.proofRunInserts() {
		if strings.Contains(fmt.Sprint(insert.args...), "edge_mode") {
			t.Errorf("a Python-reference receipt mentions an edge mode: %v", insert.args)
		}
	}
	raw, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatal(err)
	}
	var report struct {
		Summary struct {
			EdgeMode string `json:"edge_mode"`
		} `json:"summary"`
		Outcomes []map[string]any `json:"outcomes"`
	}
	if err := json.Unmarshal(raw, &report); err != nil || report.Summary.EdgeMode != goapiproof.EdgeModePython {
		t.Fatalf("report summary edge_mode %q (err %v), want python", report.Summary.EdgeMode, err)
	}
	for _, outcome := range report.Outcomes {
		if _, present := outcome["edge_mode"]; present {
			t.Errorf("a Python-reference outcome carries edge_mode: %v", outcome)
		}
	}
}

// preflightRefusal runs the command with a command line that is refused
// before anything is measured, and returns stdout, the error and the report.
func preflightRefusal(t *testing.T, args ...string) (string, error, map[string]any) {
	t.Helper()
	reportPath := filepath.Join(t.TempDir(), "report.json")
	var runErr error
	stdout := captureStdout(t, func() {
		runErr = run(append([]string{"-report=" + reportPath}, args...))
	})
	raw, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatalf("a refused run must still write its report: %v (run error %v)", err, runErr)
	}
	var report map[string]any
	if err := json.Unmarshal(raw, &report); err != nil {
		t.Fatalf("report: %v\n%s", err, raw)
	}
	return stdout, runErr, report
}

// TestARunRefusedBeforeMeasuringNamesItsMode: the mode is on every exit, not
// only on the exits that reach the runner. A command line refused before
// anything is measured still says which proof was refused, on stdout and in
// the report, and a command line that did not parse says that no mode was
// selected instead of naming one.
func TestARunRefusedBeforeMeasuringNamesItsMode(t *testing.T) {
	complete := []string{
		"-registry-url=http://127.0.0.1:1/registry",
		"-buildinfo-url=http://127.0.0.1:1/buildinfo",
		"-edge-url=://not-a-url",
		"-documents=" + filepath.Join(t.TempDir(), "documents.json"),
		"-postgres-uri=postgres://fake/ignored",
		"-org=70d529e0",
		"-artifact-dir=" + t.TempDir(),
		"-recorded-by=harness",
		"-review-evidence=preflight refusal",
	}
	for name, tc := range map[string]struct {
		args []string
		mode string
	}{
		"go-edge, refused after the flags parsed":          {append([]string{"-go-edge"}, complete...), goapiproof.EdgeModeGo},
		"python-reference, refused after the flags parsed": {complete, goapiproof.EdgeModePython},
		"go-edge, a required flag is missing":              {[]string{"-go-edge"}, goapiproof.EdgeModeGo},
		"the command line did not parse":                   {[]string{"-no-such-flag", "-go-edge"}, goapiproof.EdgeModeUndetermined},
	} {
		t.Run(name, func(t *testing.T) {
			stdout, runErr, report := preflightRefusal(t, tc.args...)
			if runErr == nil {
				t.Fatalf("the command line must be refused\nstdout:\n%s", stdout)
			}
			if report["exit_cause"] != exitRefusedBeforeMeasuring {
				t.Fatalf("exit_cause %v, want %s: this test is about the exit before the runner", report["exit_cause"], exitRefusedBeforeMeasuring)
			}
			summary, _ := report["summary"].(map[string]any)
			if summary["edge_mode"] != tc.mode {
				t.Errorf("report summary edge_mode %v, want %q", summary["edge_mode"], tc.mode)
			}
			for _, want := range []string{
				"go-api-prove: edge_mode=" + tc.mode + " (",
				"go-api-prove: exit_cause=" + exitRefusedBeforeMeasuring,
			} {
				if !strings.Contains(stdout, want) {
					t.Errorf("stdout lacks %q:\n%s", want, stdout)
				}
			}
			for _, other := range []string{goapiproof.EdgeModeGo, goapiproof.EdgeModePython, goapiproof.EdgeModeUndetermined} {
				if other != tc.mode && strings.Contains(stdout, "edge_mode="+other) {
					t.Errorf("stdout names a second mode (%s):\n%s", other, stdout)
				}
			}
		})
	}
}

// TestAskingForHelpIsNotARefusedRun: -h is not a run, so it names no mode and
// writes no report.
func TestAskingForHelpIsNotARefusedRun(t *testing.T) {
	reportPath := filepath.Join(t.TempDir(), "report.json")
	var runErr error
	stdout := captureStdout(t, func() { runErr = run([]string{"-report=" + reportPath, "-h"}) })
	if !errors.Is(runErr, flag.ErrHelp) {
		t.Fatalf("run(-h) = %v, want flag.ErrHelp", runErr)
	}
	if strings.Contains(stdout, "edge_mode=") || strings.Contains(stdout, "exit_cause=") {
		t.Errorf("-h printed a run line:\n%s", stdout)
	}
	if _, err := os.Stat(reportPath); !os.IsNotExist(err) {
		t.Errorf("-h wrote a report (stat error %v)", err)
	}
}

// TestEveryReportNamesAMode: the report constructor never writes an empty
// mode, whatever summary it is handed, and it keeps a mode that was named.
func TestEveryReportNamesAMode(t *testing.T) {
	if got := newReport(flags{}, goapiproof.Summary{}, nil, exitRefusedBeforeMeasuring); got.Summary.EdgeMode != goapiproof.EdgeModeUndetermined || got.Outcomes == nil {
		t.Errorf("a summary with no mode: edge_mode %q outcomes nil=%v, want undetermined and an empty list", got.Summary.EdgeMode, got.Outcomes == nil)
	}
	for _, mode := range []string{goapiproof.EdgeModeGo, goapiproof.EdgeModePython} {
		if got := newReport(flags{}, goapiproof.Summary{EdgeMode: mode}, nil, exitRefusedBeforeMeasuring); got.Summary.EdgeMode != mode {
			t.Errorf("a summary that names %q was recorded as %q", mode, got.Summary.EdgeMode)
		}
	}
	if line := edgeModeLine(goapiproof.EdgeModeUndetermined); !strings.HasPrefix(line, "edge_mode=undetermined (") {
		t.Errorf("undetermined mode line %q", line)
	}
	if line := edgeModeLine(""); !strings.HasPrefix(line, "edge_mode=undetermined (") {
		t.Errorf("a mode that was never set must read as undetermined, got %q", line)
	}
	if selectedEdgeMode(flags{goEdge: true}) != goapiproof.EdgeModeUndetermined {
		t.Error("flags that were not parsed to the end select no mode, whatever was read before the error")
	}
	if selectedEdgeMode(flags{parsed: true, goEdge: true}) != goapiproof.EdgeModeGo || selectedEdgeMode(flags{parsed: true}) != goapiproof.EdgeModePython {
		t.Error("parsed flags select the mode -go-edge names")
	}
}
