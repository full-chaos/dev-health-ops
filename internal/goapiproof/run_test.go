package goapiproof

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// fakeEdge stands in for the Python edge. It answers by PLANE the way the
// real dispatcher does: a registered document is served by Go (and
// stamped x-dev-health-plane: go), while the comment-suffixed control
// document misses the digest and is served by Python -- exactly the
// mechanism baselineComment relies on.
type fakeEdge struct {
	goBody     string
	pythonBody string
	goStatus   int
	goPlane    string
	seen       []string
}

func (e *fakeEdge) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var parsed struct {
			Query string `json:"query"`
		}
		_ = json.Unmarshal(raw, &parsed)
		e.seen = append(e.seen, parsed.Query)

		if strings.Contains(parsed.Query, "python-plane control") {
			w.Header().Set(planeHeader, "python")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(e.pythonBody))
			return
		}
		plane := e.goPlane
		if plane == "" {
			plane = "go"
		}
		status := e.goStatus
		if status == 0 {
			status = http.StatusOK
		}
		w.Header().Set(planeHeader, plane)
		w.WriteHeader(status)
		_, _ = w.Write([]byte(e.goBody))
	}
}

func newRunner(t *testing.T, edge *fakeEdge, mode string) *Runner {
	t.Helper()
	server := httptest.NewServer(edge.handler())
	t.Cleanup(server.Close)

	store, err := NewArtifactStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewArtifactStore: %v", err)
	}

	return &Runner{
		Client:    server.Client(),
		Documents: map[string]string{"featureFlags": "query FeatureFlags { featureFlags { key } }"},
		Registry: RegistryView{
			SchemaDigest:   "sha256:29d509cd",
			BuildIdentity:  "b18e56fa79cfe20ce0f75df148144b832d92be36",
			DocumentDigest: map[string]string{"featureFlags": "06ca28a0"},
		},
		Routing:   map[string]RoutingRow{"featureFlags": {Mode: mode, CandidateBuild: "b18e56fa79cfe20ce0f75df148144b832d92be36"}},
		Artifacts: store,
		Config: Config{
			OrgID:         "70d529e0",
			Window:        DefaultWindow(),
			PythonEdgeURL: server.URL,
			Auth:          AuthContext{PrincipalKind: "stored_account", Audience: "query-api", KeyID: "local-dev-20260906"},
		},
	}
}

func TestRunRecordsAMatchWhenBothPlanesAgree(t *testing.T) {
	body := `{"data":{"featureFlags":[{"key":"a"}]}}`
	runner := newRunner(t, &fakeEdge{goBody: body, pythonBody: body}, "canary")

	outcomes, summary, err := runner.Run(context.Background(), nil)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if summary.Attempted != 1 || summary.Executed != 1 || summary.Refused != 0 {
		t.Fatalf("unexpected summary: %+v", summary)
	}
	if outcomes[0].TerminalState != TerminalStateMatch {
		t.Fatalf("expected match, got %s (%v)", outcomes[0].TerminalState, outcomes[0].Findings)
	}
	if outcomes[0].Candidate.Plane != "go" || outcomes[0].Baseline.Plane != "python" {
		t.Fatalf("legs hit the wrong planes: candidate=%s baseline=%s", outcomes[0].Candidate.Plane, outcomes[0].Baseline.Plane)
	}
	if outcomes[0].Candidate.BodyRef == "" || outcomes[0].Baseline.BodyRef == "" {
		t.Fatal("both legs must store a durable artifact reference")
	}
}

// The candidate document must be the REGISTERED text, byte for byte --
// anything else misses the digest and is served by Python, and the
// baseline must be that text plus the inert comment.
func TestRunSendsTheRegisteredDocumentUnmodified(t *testing.T) {
	body := `{"data":{"featureFlags":[]}}`
	edge := &fakeEdge{goBody: body, pythonBody: body}
	runner := newRunner(t, edge, "canary")
	if _, _, err := runner.Run(context.Background(), nil); err != nil {
		t.Fatalf("Run: %v", err)
	}
	registered := runner.Documents["featureFlags"]
	if len(edge.seen) != 2 {
		t.Fatalf("expected two legs, saw %d", len(edge.seen))
	}
	if edge.seen[0] != registered {
		t.Fatalf("candidate leg must send the registered document verbatim, got %q", edge.seen[0])
	}
	if edge.seen[1] != registered+baselineComment {
		t.Fatalf("baseline leg must send the registered document plus the inert comment, got %q", edge.seen[1])
	}
}

// A fallback is a real outcome and must be recorded AS a fallback -- never
// as a proof that Go served the request.
func TestRunRefusesWhenTheEdgeFellBackToPython(t *testing.T) {
	body := `{"data":{"featureFlags":[]}}`
	runner := newRunner(t, &fakeEdge{goBody: body, pythonBody: body, goPlane: "python"}, "canary")

	outcomes, summary, err := runner.Run(context.Background(), nil)
	if !errors.Is(err, ErrNothingMeasured) {
		t.Fatalf("a run whose only operation fell back measured nothing: %v", err)
	}
	if summary.Executed != 0 || summary.Refused != 1 {
		t.Fatalf("unexpected summary: %+v", summary)
	}
	if outcomes[0].RefusalReason != RefusalWrongPlane || outcomes[0].TerminalState != "fallback" {
		t.Fatalf("expected a named fallback refusal, got %s/%s", outcomes[0].RefusalReason, outcomes[0].TerminalState)
	}
}

// mode=shadow with no measurement route must refuse BY NAME. The
// production switch admits canary|primary only, so a shadow operation
// sent to the product edge would be served by Python and a receipt built
// from it would be a lie.
func TestRunRefusesShadowWithoutAMeasurementRoute(t *testing.T) {
	body := `{"data":{"featureFlags":[]}}`
	runner := newRunner(t, &fakeEdge{goBody: body, pythonBody: body}, "shadow")

	outcomes, _, err := runner.Run(context.Background(), nil)
	if !errors.Is(err, ErrNothingMeasured) {
		t.Fatalf("expected ErrNothingMeasured, got %v", err)
	}
	if outcomes[0].RefusalReason != RefusalShadowUnmeasurable {
		t.Fatalf("expected %s, got %s", RefusalShadowUnmeasurable, outcomes[0].RefusalReason)
	}
	if len(outcomes[0].RefusalDetail) == 0 {
		t.Fatal("a refusal must carry a detail an operator can act on")
	}
}

func TestRunRefusesAnOperationThatIsNotRoutedToGo(t *testing.T) {
	body := `{"data":{"featureFlags":[]}}`
	for _, mode := range []string{"python", "disabled", ""} {
		t.Run("mode="+mode, func(t *testing.T) {
			runner := newRunner(t, &fakeEdge{goBody: body, pythonBody: body}, mode)
			outcomes, _, err := runner.Run(context.Background(), nil)
			if !errors.Is(err, ErrNothingMeasured) {
				t.Fatalf("expected ErrNothingMeasured, got %v", err)
			}
			if outcomes[0].RefusalReason != RefusalNotRouted {
				t.Fatalf("expected %s, got %s", RefusalNotRouted, outcomes[0].RefusalReason)
			}
		})
	}
}

// D15/R4 in one assertion: a run that executed nothing must FAIL, and the
// summary must say so with explicit zeros rather than printing nothing.
func TestRunFailsWhenNothingWasMeasured(t *testing.T) {
	body := `{"data":{"featureFlags":[]}}`
	runner := newRunner(t, &fakeEdge{goBody: body, pythonBody: body}, "python")

	_, summary, err := runner.Run(context.Background(), nil)
	if !errors.Is(err, ErrNothingMeasured) {
		t.Fatalf("expected ErrNothingMeasured, got %v", err)
	}
	if summary.Executed != 0 {
		t.Fatalf("executed must be an explicit zero, got %d", summary.Executed)
	}
	if summary.ByRefusalReason[RefusalNotRouted] != 1 {
		t.Fatalf("the refusal must be counted by name: %+v", summary.ByRefusalReason)
	}
}

func TestRunRecordsAMismatchWhenThePlanesDisagree(t *testing.T) {
	runner := newRunner(t,
		&fakeEdge{
			goBody:     `{"data":{"featureFlags":[{"key":"a"}]}}`,
			pythonBody: `{"data":{"featureFlags":[{"key":"b"}]}}`,
		}, "canary")

	outcomes, summary, err := runner.Run(context.Background(), nil)
	if err != nil {
		t.Fatalf("a recorded mismatch is a SUCCESSFUL measurement, not a run error: %v", err)
	}
	if summary.Executed != 1 || outcomes[0].TerminalState != TerminalStateMismatch {
		t.Fatalf("expected an executed mismatch, got %+v / %s", summary, outcomes[0].TerminalState)
	}
	if len(outcomes[0].Findings) == 0 {
		t.Fatal("a mismatch must carry the findings behind it")
	}
}

// Two bodies that compare equal under DIFFERENT HTTP statuses are not
// parity: the status is part of the observable response.
func TestRunTreatsAStatusCodeDifferenceAsAMismatch(t *testing.T) {
	body := `{"data":{"featureFlags":[]}}`
	runner := newRunner(t, &fakeEdge{goBody: body, pythonBody: body, goStatus: http.StatusAccepted}, "canary")

	outcomes, _, err := runner.Run(context.Background(), nil)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if outcomes[0].TerminalState != TerminalStateMismatch {
		t.Fatalf("expected mismatch on a status difference, got %s", outcomes[0].TerminalState)
	}
	var sawStatus bool
	for _, finding := range outcomes[0].Findings {
		if finding.Path == "$.http.status" {
			sawStatus = true
		}
	}
	if !sawStatus {
		t.Fatalf("the status difference must be a named finding: %v", outcomes[0].Findings)
	}
}

// A declared exclusion that matched nothing invalidates the verdict: the
// comparison that ran is not the comparison anybody declared.
func TestRunRefusesOnAStaleDeclaredExclusion(t *testing.T) {
	body := `{"data":{"capacityForecast":{"backlogSize":8}}}`
	edge := &fakeEdge{goBody: body, pythonBody: body}
	runner := newRunner(t, edge, "canary")
	runner.Documents = map[string]string{"capacityForecast": "query CapacityForecast { capacityForecast { backlogSize } }"}
	runner.Registry.DocumentDigest = map[string]string{"capacityForecast": "b4fb8f07"}
	runner.Routing = map[string]RoutingRow{"capacityForecast": {Mode: "canary"}}

	outcomes, _, err := runner.Run(context.Background(), nil)
	if !errors.Is(err, ErrNothingMeasured) {
		t.Fatalf("expected the run to measure nothing, got %v", err)
	}
	if outcomes[0].RefusalReason != RefusalStaleExclusion {
		t.Fatalf("expected %s, got %s (%s)", RefusalStaleExclusion, outcomes[0].RefusalReason, outcomes[0].RefusalDetail)
	}
}

// An operation the running process registers but this checkout has no
// document text for is a digest-drift refusal, never a skip.
func TestRunRefusesAnOperationWithNoLocalDocument(t *testing.T) {
	body := `{"data":{}}`
	runner := newRunner(t, &fakeEdge{goBody: body, pythonBody: body}, "canary")
	runner.Registry.DocumentDigest["hotspots"] = "6ccfcc78"
	runner.Routing["hotspots"] = RoutingRow{Mode: "canary"}

	outcomes, summary, err := runner.Run(context.Background(), nil)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if summary.Attempted != 2 || summary.Refused != 1 {
		t.Fatalf("unexpected summary: %+v", summary)
	}
	for _, outcome := range outcomes {
		if outcome.Operation == "hotspots" && outcome.RefusalReason != RefusalDocumentDigestDrift {
			t.Fatalf("expected %s, got %s", RefusalDocumentDigestDrift, outcome.RefusalReason)
		}
	}
}

func TestArtifactStoreIsContentAddressedAndIdempotent(t *testing.T) {
	store, err := NewArtifactStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewArtifactStore: %v", err)
	}
	first, err := store.Put([]byte(`{"a":1}`))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	second, err := store.Put([]byte(`{"a":1}`))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if first != second {
		t.Fatalf("identical bytes must yield one ref: %s != %s", first, second)
	}
	other, err := store.Put([]byte(`{"a":2}`))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if other == first {
		t.Fatal("different bytes must yield different refs")
	}
}

func TestNewArtifactStoreRefusesAnEmptyDirectory(t *testing.T) {
	if _, err := NewArtifactStore(""); err == nil {
		t.Fatal("a receipt with no stored response is not reviewable evidence")
	}
}
