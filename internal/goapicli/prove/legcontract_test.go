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
	"syscall"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/goapidigest"
	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

// TestValidateEndpointFlags_RefusesAComponentThatWouldChangeTheRequest
// runs every endpoint flag through a fragment and a query.
func TestValidateEndpointFlags_RefusesAComponentThatWouldChangeTheRequest(t *testing.T) {
	base := func() flags {
		return flags{registryURL: "http://q:8090/registry", buildInfoURL: "http://q:8090/buildinfo", edgeURL: "http://api:8000/graphql", proofURL: "http://q:8090/query/proof"}
	}
	for _, field := range []string{"registry", "buildinfo", "edge", "proof"} {
		for _, suffix := range []string{"#frag", "#", "?x=1", "?"} {
			f := base()
			switch field {
			case "registry":
				f.registryURL += suffix
			case "buildinfo":
				f.buildInfoURL += suffix
			case "edge":
				f.edgeURL += suffix
			case "proof":
				f.proofURL += suffix
			}
			if err := validateEndpointFlags(f); !errors.Is(err, goapiproof.ErrBaseURLComponents) {
				t.Errorf("%s with %q: err = %v, want ErrBaseURLComponents", field, suffix, err)
			}
		}
	}
	if err := validateEndpointFlags(base()); err != nil {
		t.Fatalf("control: %v", err)
	}
}

func TestOriginOfKeepsOnlySchemeAndHost(t *testing.T) {
	for raw, want := range map[string]string{
		"http://api:8000/graphql":        "http://api:8000",
		"https://api.internal/x/graphql": "https://api.internal",
		"http://api:8000":                "http://api:8000",
	} {
		if got := originOf(raw); got != want {
			t.Errorf("originOf(%q) = %q, want %q", raw, got, want)
		}
	}
}

// TestRunRefusesABaselinePrincipalThatIsNotTheNamedOrg drives run() to
// its reference-principal check and pins, per cell, that the run stops
// there -- no GraphQL request reaches the edge -- when a credential names
// another org, or the Python app answers for another org or under an
// impersonation session.
func TestRunRefusesABaselinePrincipalThatIsNotTheNamedOrg(t *testing.T) {
	const org = "70d529e0"
	const buildSHA = "b18e56fa79cfe20ce0f75df148144b832d92be36"
	withProverCommit(t, buildSHA)
	registry := httptest.NewServer(writeStaticJSONHandler(`{"schema_digest":"sha256:e2e","operations":[{"operation":"featureFlags","document_digest":"sha256:x"}]}`))
	t.Cleanup(registry.Close)
	buildinfo := httptest.NewServer(writeStaticJSONHandler(`{"commit":"` + buildSHA + `","modified":false}`))
	t.Cleanup(buildinfo.Close)

	cells := []struct {
		name              string
		edgeOrg, proofOrg string
		meOrg             string
		meImpersonating   bool
		want              error
	}{
		{"edge credential names another org", "other", org, org, false, goapiproof.ErrCredentialNamesAnotherOrg},
		{"proof credential names another org", org, "other", org, false, goapiproof.ErrCredentialNamesAnotherOrg},
		{"python resolves the credential to another org", org, org, "other", false, goapiproof.ErrReferencePrincipal},
		{"python answers under an impersonation session", org, org, org, true, goapiproof.ErrReferencePrincipal},
	}
	for _, cell := range cells {
		var graphqlHits, meHits atomic.Int32
		edge := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == goapiproof.ReferencePrincipalPath {
				meHits.Add(1)
				w.Header().Set("Server", goapiproof.ReferencePlaneServer)
				if cell.meImpersonating {
					w.Header().Set("x-impersonating", "true")
				}
				_, _ = w.Write([]byte(`{"org_id":"` + cell.meOrg + `"}`))
				return
			}
			graphqlHits.Add(1)
			w.WriteHeader(http.StatusOK)
		}))
		err := runCLI(t, []string{
			"-registry-url=" + registry.URL + "/registry",
			"-buildinfo-url=" + buildinfo.URL + "/buildinfo",
			"-edge-url=" + edge.URL + "/graphql",
			"-documents=/nonexistent/documents.json",
			"-org=" + org,
			"-artifact-dir=" + t.TempDir(),
			"-recorded-by=harness",
			"-review-evidence=e2e harness",
			"-dry-run",
			"-edge-bearer-exec=" + jsonArgv(t, e2eBearerHelper(t, syntheticJWT(t, map[string]string{"sub": "edge", "org_id": cell.edgeOrg}))),
			"-proof-bearer-exec=" + jsonArgv(t, e2eBearerHelper(t, syntheticJWT(t, map[string]string{"sub": "proof", "org_id": cell.proofOrg}))),
			"-timeout=5s",
		})
		edge.Close()
		if !errors.Is(err, cell.want) {
			t.Errorf("%s: run() err = %v, want %v", cell.name, err, cell.want)
		}
		if err != nil && strings.Contains(err.Error(), "documents.json") {
			t.Errorf("%s: the run got past the principal check to the documents: %v", cell.name, err)
		}
		if graphqlHits.Load() != 0 {
			t.Errorf("%s: %d GraphQL request(s) reached the edge", cell.name, graphqlHits.Load())
		}
	}
}

// exitFixture is a one-operation deployment: a registry naming every
// known operation (only featureFlags with a matching document), a
// /buildinfo, and an edge that answers the principal check and the
// featureFlags legs through onGraphQL.
func exitFixture(t *testing.T, onGraphQL func(w http.ResponseWriter, baseline bool)) (args []string, reportPath string) {
	t.Helper()
	const buildSHA = "b18e56fa79cfe20ce0f75df148144b832d92be36"
	withProverCommit(t, buildSHA)
	const org = "70d529e0"
	doc := "query FeatureFlags { featureFlags { key } }"
	digest := goapidigest.Document(doc)
	type registryOp struct {
		Operation      string `json:"operation"`
		DocumentDigest string `json:"document_digest"`
	}
	var ops []registryOp
	for _, name := range goapiproof.KnownOperations() {
		d := "sha256:unused-" + name
		if name == "featureFlags" {
			d = digest
		}
		ops = append(ops, registryOp{Operation: name, DocumentDigest: d})
	}
	registryBody, _ := json.Marshal(struct {
		SchemaDigest string       `json:"schema_digest"`
		Operations   []registryOp `json:"operations"`
	}{SchemaDigest: "sha256:exit", Operations: ops})
	registry := httptest.NewServer(writeStaticJSONHandler(string(registryBody)))
	t.Cleanup(registry.Close)
	buildinfo := httptest.NewServer(writeStaticJSONHandler(`{"commit":"` + buildSHA + `","modified":false}`))
	t.Cleanup(buildinfo.Close)
	edge := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == goapiproof.ReferencePrincipalPath {
			w.Header().Set("Server", goapiproof.ReferencePlaneServer)
			_, _ = w.Write([]byte(`{"org_id":"` + org + `"}`))
			return
		}
		raw, _ := io.ReadAll(r.Body)
		baseline := strings.Contains(string(raw), "python-plane control")
		w.Header().Set("x-dev-health-build", buildSHA)
		if baseline {
			w.Header().Set("x-dev-health-plane", "python")
		} else {
			w.Header().Set("x-dev-health-plane", "go")
		}
		onGraphQL(w, baseline)
	}))
	t.Cleanup(edge.Close)
	docs, _ := json.Marshal([]map[string]string{{"operation": "featureFlags", "document": doc}})
	docsPath := filepath.Join(t.TempDir(), "documents.json")
	if err := os.WriteFile(docsPath, docs, 0o600); err != nil {
		t.Fatal(err)
	}
	withFakePool(t, &e2ePool{routingRows: [][]any{{"featureFlags", digest, "canary", buildSHA}}})
	reportPath = filepath.Join(t.TempDir(), "report.json")
	return []string{
		"-registry-url=" + registry.URL + "/registry",
		"-buildinfo-url=" + buildinfo.URL + "/buildinfo",
		"-edge-url=" + edge.URL + "/graphql",
		"-documents=" + docsPath,
		"-postgres-uri=postgres://fake/ignored",
		"-org=" + org,
		"-artifact-dir=" + t.TempDir(),
		"-recorded-by=harness",
		"-review-evidence=exit-path harness",
		"-edge-bearer-exec=" + jsonArgv(t, e2eBearerHelper(t, syntheticJWT(t, map[string]string{"sub": "edge", "org_id": org}))),
		"-proof-bearer-exec=" + jsonArgv(t, e2eBearerHelper(t, syntheticJWT(t, map[string]string{"sub": "proof", "org_id": org}))),
		"-report=" + reportPath,
		"-timeout=5s",
	}, reportPath
}

func readGraphQLReport(t *testing.T, path string) report {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	var r report
	if err := json.Unmarshal(raw, &r); err != nil {
		t.Fatalf("decode report: %v (%s)", err, raw)
	}
	return r
}

// TestEveryGraphQLExitPathWritesTheReportWithItsCause enumerates each
// way run() can end, from the code: flags refused, a refusal before
// anything is measured, a completed run, a run whose measurement ends in
// a run error, and a SIGTERM delivered mid-run. Every row writes -report
// with its exit_cause.
func TestEveryGraphQLExitPathWritesTheReportWithItsCause(t *testing.T) {
	t.Run("flags refused", func(t *testing.T) {
		reportPath := filepath.Join(t.TempDir(), "report.json")
		err := runCLI(t, []string{"-report=" + reportPath, "-org=70d529e0"})
		if err == nil {
			t.Fatal("want a flag refusal")
		}
		if r := readGraphQLReport(t, reportPath); r.ExitCause != exitRefusedBeforeMeasuring || r.ExitDetail == "" {
			t.Fatalf("report = %+v", r)
		}
	})
	t.Run("refused before anything is measured", func(t *testing.T) {
		args, reportPath := exitFixture(t, func(w http.ResponseWriter, _ bool) { t.Error("a GraphQL request was sent") })
		for i, a := range args {
			if strings.HasPrefix(a, "-edge-bearer-exec=") {
				args[i] = "-edge-bearer-exec=" + jsonArgv(t, e2eBearerHelper(t, syntheticJWT(t, map[string]string{"sub": "edge", "org_id": "other"})))
			}
		}
		if err := runCLI(t, args); !errors.Is(err, goapiproof.ErrCredentialNamesAnotherOrg) {
			t.Fatalf("err = %v", err)
		}
		if r := readGraphQLReport(t, reportPath); r.ExitCause != exitRefusedBeforeMeasuring || len(r.Outcomes) != 0 {
			t.Fatalf("report = %+v", r)
		}
	})
	t.Run("completed", func(t *testing.T) {
		args, reportPath := exitFixture(t, func(w http.ResponseWriter, _ bool) { _, _ = w.Write([]byte(`{"data":{"featureFlags":[{"key":"a"}]}}`)) })
		var err error
		captureStdout(t, func() { err = runCLI(t, args) })
		if err != nil {
			t.Fatalf("run: %v", err)
		}
		if r := readGraphQLReport(t, reportPath); r.ExitCause != exitCompleted || r.Summary.Executed != 1 {
			t.Fatalf("report exit_cause=%q executed=%d", r.ExitCause, r.Summary.Executed)
		}
	})
	t.Run("measured, then a run error", func(t *testing.T) {
		args, reportPath := exitFixture(t, func(w http.ResponseWriter, _ bool) { w.WriteHeader(http.StatusBadGateway) })
		var err error
		captureStdout(t, func() { err = runCLI(t, args) })
		if err == nil {
			t.Fatal("want the run error: nothing was executed")
		}
		r := readGraphQLReport(t, reportPath)
		if r.ExitCause != exitCompletedWithRunError || r.ExitDetail == "" || len(r.Outcomes) == 0 {
			t.Fatalf("report exit_cause=%q detail=%q outcomes=%d", r.ExitCause, r.ExitDetail, len(r.Outcomes))
		}
	})
	t.Run("SIGTERM mid-run", func(t *testing.T) {
		var sent atomic.Bool
		args, reportPath := exitFixture(t, func(w http.ResponseWriter, _ bool) {
			if sent.CompareAndSwap(false, true) {
				_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
				time.Sleep(200 * time.Millisecond)
			}
			_, _ = w.Write([]byte(`{"data":{"featureFlags":[{"key":"a"}]}}`))
		})
		var err error
		captureStdout(t, func() { err = runCLI(t, args) })
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want the run stopped by the signal", err)
		}
		if r := readGraphQLReport(t, reportPath); r.ExitCause != exitStoppedBySignal || len(r.Outcomes) == 0 {
			t.Fatalf("report exit_cause=%q outcomes=%d", r.ExitCause, len(r.Outcomes))
		}
	})
}
