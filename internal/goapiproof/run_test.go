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
	"time"
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
	// goBuild is the serving-build header the edge passes through
	// (CHAOS-5479). Empty means the deployment predates the pass-through,
	// which is what makes an edge measurement unbound.
	goBuild string
	seen    []string
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
		if e.goBuild != "" {
			w.Header().Set(buildHeader, e.goBuild)
		}
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
			// The fake edge does not check credentials; these are set
			// because Apply refuses a nil one, which is the point of
			// CHAOS-5425's credential fix -- an unauthenticated request
			// must never look like a rejected one.
			EdgeCredential:  StaticCredential("Authorization", "edge access token", "Bearer edge"),
			ProofCredential: StaticCredential("Authorization", "envelope", "Bearer envelope"),
		},
	}
}

func TestRunRecordsAMatchWhenBothPlanesAgree(t *testing.T) {
	body := `{"data":{"featureFlags":[{"key":"a"}]}}`
	// The serving-build header is present because an edge measurement
	// WITHOUT one can no longer produce a match -- see
	// TestAnEdgeMeasurementWithNoBuildBindingCannotBeEnablementEligible.
	edge := &fakeEdge{goBody: body, pythonBody: body, goBuild: "b18e56fa79cfe20ce0f75df148144b832d92be36"}
	runner := newRunner(t, edge, "canary")

	outcomes, summary, err := runner.Run(context.Background())
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
	if _, _, err := runner.Run(context.Background()); err != nil {
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

	outcomes, summary, err := runner.Run(context.Background())
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

	outcomes, _, err := runner.Run(context.Background())
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
			outcomes, _, err := runner.Run(context.Background())
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

	_, summary, err := runner.Run(context.Background())
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

	outcomes, summary, err := runner.Run(context.Background())
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

	outcomes, _, err := runner.Run(context.Background())
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

	outcomes, _, err := runner.Run(context.Background())
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

	outcomes, summary, err := runner.Run(context.Background())
	if !errors.Is(err, ErrNothingMeasured) {
		t.Fatalf("expected ErrNothingMeasured, got %v", err)
	}
	// BOTH refuse, for DIFFERENT named reasons, and the test pins each to
	// its own operation: hotspots has no local document, and featureFlags'
	// body here is `{"data":{}}`, which carries no resolved root field.
	// Asserting only a count would pass if both refused for the same reason.
	if summary.Attempted != 2 || summary.Refused != 2 {
		t.Fatalf("unexpected summary: %+v", summary)
	}
	byOperation := map[string]string{}
	for _, outcome := range outcomes {
		byOperation[outcome.Operation] = outcome.RefusalReason
	}
	if byOperation["hotspots"] != RefusalDocumentDigestDrift {
		t.Fatalf("hotspots: expected %s, got %s", RefusalDocumentDigestDrift, byOperation["hotspots"])
	}
	if byOperation["featureFlags"] != RefusalEmptyResponseRoot {
		t.Fatalf("featureFlags: expected %s, got %s", RefusalEmptyResponseRoot, byOperation["featureFlags"])
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

// planeStampingEdge answers both legs with explicit plane headers, and lets
// a test suppress or alter either one. It exists because the regression
// tests below are all about what happens when plane evidence is WRONG or
// MISSING, which fakeEdge (which always stamps correctly) cannot express.
type planeStampingEdge struct {
	body            string
	candidatePlane  string
	baselinePlane   string
	candidateBuild  string
	candidateType   string
	baselineType    string
	candidateStatus int
}

func (e *planeStampingEdge) server(t *testing.T) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var parsed struct {
			Query string `json:"query"`
		}
		_ = json.Unmarshal(raw, &parsed)
		isBaseline := strings.Contains(parsed.Query, "python-plane control")

		plane, contentType, status := e.candidatePlane, e.candidateType, e.candidateStatus
		if isBaseline {
			plane, contentType, status = e.baselinePlane, e.baselineType, http.StatusOK
		}
		if plane != "" {
			w.Header().Set(planeHeader, plane)
		}
		if !isBaseline && e.candidateBuild != "" {
			w.Header().Set(buildHeader, e.candidateBuild)
		}
		if contentType == "" {
			contentType = "application/json"
		}
		w.Header().Set("Content-Type", contentType)
		if status == 0 {
			status = http.StatusOK
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(e.body))
	}))
	t.Cleanup(server.Close)
	return server
}

func runnerAgainst(t *testing.T, edge *planeStampingEdge, mode string) *Runner {
	t.Helper()
	server := edge.server(t)
	runner := newRunner(t, &fakeEdge{goBody: edge.body, pythonBody: edge.body}, mode)
	runner.Config.PythonEdgeURL = server.URL
	runner.Client = server.Client()
	return runner
}

// A shadow operation is NOT exempt from the plane assertion. The exemption
// existed because /query/proof carried no plane header; the proof handler
// now stamps one, so a --proof-url pointed at anything returning a
// plausible 200 must refuse instead of producing a receipt.
func TestShadowCandidateWithNoPlaneEvidenceIsRefused(t *testing.T) {
	body := `{"data":{"featureFlags":[]}}`
	runner := newRunner(t, &fakeEdge{goBody: body, pythonBody: body}, "shadow")
	bogus := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(bogus.Close)
	runner.Config.GoProofURL = bogus.URL

	outcomes, _, err := runner.Run(context.Background())
	if !errors.Is(err, ErrNothingMeasured) {
		t.Fatalf("a candidate with no plane evidence measured nothing, got %v", err)
	}
	if outcomes[0].RefusalReason != RefusalPlaneUnidentified {
		t.Fatalf("expected %s, got %s", RefusalPlaneUnidentified, outcomes[0].RefusalReason)
	}
}

// The BASELINE must be positively identified as Python. Accepting an absent
// header let the control be Go, silently comparing Go against Go.
func TestBaselineWithNoPlaneEvidenceIsRefused(t *testing.T) {
	runner := runnerAgainst(t, &planeStampingEdge{
		body:           `{"data":{"featureFlags":[]}}`,
		candidatePlane: "go",
		baselinePlane:  "", // header suppressed
	}, "canary")

	outcomes, _, err := runner.Run(context.Background())
	if !errors.Is(err, ErrNothingMeasured) {
		t.Fatalf("an unidentified baseline measured nothing, got %v", err)
	}
	if outcomes[0].RefusalReason != RefusalPlaneUnidentified {
		t.Fatalf("expected %s, got %s (%s)", RefusalPlaneUnidentified, outcomes[0].RefusalReason, outcomes[0].RefusalDetail)
	}
}

// Two identical GraphQL error envelopes must not back a proof at all.
//
// This test used to assert an EXECUTED outcome with terminal_state
// `unsupported` -- the blacklist-era shape, where a disqualifying response
// still counted as a measurement. Under the admission gate an errored
// response never reaches comparison, so the assertion is now stricter: a
// named refusal AND executed=false AND admitted=false.
func TestIdenticalErrorsAreRefusedNotExecuted(t *testing.T) {
	errBody := `{"errors":[{"message":"boom","path":["featureFlags"],"extensions":{"code":"INTERNAL"}}]}`
	runner := runnerAgainst(t, &planeStampingEdge{
		body: errBody, candidatePlane: "go", baselinePlane: "python",
	}, "canary")

	outcomes, summary, err := runner.Run(context.Background())
	if !errors.Is(err, ErrNothingMeasured) {
		t.Fatalf("an errored pair measured nothing, got %v", err)
	}
	if outcomes[0].RefusalReason != RefusalErroredResponse {
		t.Fatalf("expected %s, got %s (%s)", RefusalErroredResponse, outcomes[0].RefusalReason, outcomes[0].RefusalDetail)
	}
	if outcomes[0].Executed || outcomes[0].Admitted {
		t.Fatalf("an errored pair must be neither admitted nor executed: admitted=%v executed=%v", outcomes[0].Admitted, outcomes[0].Executed)
	}
	if summary.Admitted != 0 || summary.ByTerminalState[TerminalStateMatch] != 0 {
		t.Fatalf("nothing may be admitted or matched: %+v", summary)
	}
}

// A candidate that returns no data at all cannot back a proof either --
// again a refusal now, not a softened executed outcome.
func TestCandidateWithNoDataIsRefused(t *testing.T) {
	runner := runnerAgainst(t, &planeStampingEdge{
		body: `{"data":null}`, candidatePlane: "go", baselinePlane: "python",
	}, "canary")

	outcomes, _, err := runner.Run(context.Background())
	if !errors.Is(err, ErrNothingMeasured) {
		t.Fatalf("a data-less candidate measured nothing, got %v", err)
	}
	if outcomes[0].RefusalReason != RefusalEmptyResponseRoot {
		t.Fatalf("expected %s, got %s (%s)", RefusalEmptyResponseRoot, outcomes[0].RefusalReason, outcomes[0].RefusalDetail)
	}
	if outcomes[0].Admitted {
		t.Fatalf("a data-less candidate must not be admitted")
	}
}

// Identical bodies under DIFFERENT content types are not parity: the client
// is being told to interpret the same bytes differently.
func TestContentTypeDivergenceIsAMismatch(t *testing.T) {
	runner := runnerAgainst(t, &planeStampingEdge{
		body:           `{"data":{"featureFlags":[]}}`,
		candidatePlane: "go", baselinePlane: "python",
		candidateType: "application/problem+json", baselineType: "application/json",
	}, "canary")

	outcomes, _, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if outcomes[0].TerminalState != TerminalStateMismatch {
		t.Fatalf("a content-type divergence must mismatch, got %s", outcomes[0].TerminalState)
	}
	var named bool
	for _, finding := range outcomes[0].Findings {
		if finding.Path == "$.http.header.content-type" {
			named = true
		}
	}
	if !named {
		t.Fatalf("the divergent header must be a named finding: %v", outcomes[0].Findings)
	}
}

// The serving process's own build must equal the one the receipt will name.
func TestServingBuildMustMatchTheNamedBuild(t *testing.T) {
	runner := runnerAgainst(t, &planeStampingEdge{
		body:           `{"data":{"featureFlags":[]}}`,
		candidatePlane: "go", baselinePlane: "python",
		candidateBuild: "some-other-build",
	}, "canary")

	outcomes, _, err := runner.Run(context.Background())
	if !errors.Is(err, ErrNothingMeasured) {
		t.Fatalf("a build mismatch measured nothing, got %v", err)
	}
	if outcomes[0].RefusalReason != RefusalBuildMismatch {
		t.Fatalf("expected %s, got %s", RefusalBuildMismatch, outcomes[0].RefusalReason)
	}
}

// And the agreeing case still passes, so the check above is discriminating
// rather than refusing everything.
func TestServingBuildAgreementStillMatches(t *testing.T) {
	runner := runnerAgainst(t, &planeStampingEdge{
		body:           `{"data":{"featureFlags":[{"key":"a"}]}}`,
		candidatePlane: "go", baselinePlane: "python",
		candidateBuild: "b18e56fa79cfe20ce0f75df148144b832d92be36",
	}, "canary")

	outcomes, _, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if outcomes[0].TerminalState != TerminalStateMatch {
		t.Fatalf("an agreeing build must not block a match, got %s (%s)", outcomes[0].TerminalState, outcomes[0].RefusalDetail)
	}
	if outcomes[0].Candidate.Build != "b18e56fa79cfe20ce0f75df148144b832d92be36" {
		t.Fatalf("the serving build must be recorded, got %q", outcomes[0].Candidate.Build)
	}
}

// F6: three sibling guards refuse a run whose declared relaxations matched
// nothing. Only RefusalStaleExclusion was pinned; the other two survived
// removal under both suites.
//
// They are not cosmetic. A stale declared BASELINE DEFECT is a citation
// that no longer covers any difference -- with the refusal gone the run
// proceeds and lands on `match` where it should have refused, which is a
// receipt asserting parity on the strength of a ticket that no longer
// applies. A stale Tier-B declaration means the comparison that ran is not
// the comparison anybody declared.
//
// Driven through the REAL committed specs rather than a test-only hook:
// flowMatrix declares a baseline defect and investmentBreakdown declares
// Tier-B float paths, so identical response bodies make each declaration
// match nothing.
func TestRunRefusesOnEveryStaleDeclaration(t *testing.T) {
	for name, testCase := range map[string]struct {
		operation string
		document  string
		digest    string
		body      string
		want      string
	}{
		"a baseline defect that covered no difference": {
			operation: "flowMatrix",
			document:  "query FlowMatrix { analytics { flowMatrix { nodes { value } } } }",
			digest:    "aa11bb22",
			body:      `{"data":{"analytics":{"flowMatrix":{"nodes":[{"value":1}]}}}}`,
			want:      RefusalStaleBaselineDefect,
		},
		"a Tier-B float declaration that relaxed nothing": {
			operation: "investmentBreakdown",
			document:  "query InvestmentBreakdown { analytics { breakdowns { items { value } } } }",
			digest:    "cc33dd44",
			body:      `{"data":{"analytics":{"breakdowns":{"items":[{"value":1}]}}}}`,
			want:      RefusalStaleTierB,
		},
	} {
		t.Run(name, func(t *testing.T) {
			edge := &fakeEdge{goBody: testCase.body, pythonBody: testCase.body}
			runner := newRunner(t, edge, "canary")
			edge.goBuild = runner.Registry.BuildIdentity
			runner.Documents = map[string]string{testCase.operation: testCase.document}
			runner.Registry.DocumentDigest = map[string]string{testCase.operation: testCase.digest}
			runner.Routing = map[string]RoutingRow{testCase.operation: {Mode: "canary", CandidateBuild: runner.Registry.BuildIdentity}}

			outcomes, _, err := runner.Run(context.Background())
			if !errors.Is(err, ErrNothingMeasured) {
				t.Fatalf("a declaration matching nothing must refuse the run, got %v", err)
			}
			if outcomes[0].RefusalReason != testCase.want {
				t.Fatalf("expected %s, got %s (%s)", testCase.want, outcomes[0].RefusalReason, outcomes[0].RefusalDetail)
			}
		})
	}
}

// withOverriddenParity temporarily replaces operation's registered Parity
// for the duration of the test, restoring the original via t.Cleanup.
// Used below to isolate OrderInsensitiveList's two refusal guards from
// investmentFull's OWN FloatTierB declarations on the same sankey.nodes/
// edges paths -- those would otherwise ALSO read as stale the moment the
// sankey list is absent/malformed, and run.go checks UnusedTierB first,
// masking the exact guard these tests exist to pin.
func withOverriddenParity(t *testing.T, operation string, parity Options) {
	t.Helper()
	original, err := SpecFor(operation)
	if err != nil {
		t.Fatalf("SpecFor(%s): %v", operation, err)
	}
	overridden := original
	overridden.Parity = parity
	operationSpecs[operation] = overridden
	t.Cleanup(func() { operationSpecs[operation] = original })
}

// TestRunRefusesOnAnOrderInsensitiveListDeclarationMatchingNothing is
// CHAOS-5546 r1's P3 fix pin, first half: OrderInsensitiveList's stale-
// declaration guard (run.go) must actually terminate the run as a
// refusal, not just populate a Result field nothing reads. Deleting the
// guard (`if len(result.UnusedOrderInsensitiveLists) > 0 { return
// refuse(...) }`) left the full `internal/goapiproof` package green
// before this test existed.
func TestRunRefusesOnAnOrderInsensitiveListDeclarationMatchingNothing(t *testing.T) {
	withOverriddenParity(t, "investmentFull", Options{
		OrderInsensitiveLists: []OrderInsensitiveList{
			{Path: "data.analytics.sankey.nodes", KeyFields: []string{"id"}, Reason: "test", Ticket: "CHAOS-0000"},
		},
	})

	// No "sankey" key at all -- the declared path matches nothing, and
	// (with the override above) nothing else in Parity could make this
	// stale for any other reason.
	body := `{"data":{"analytics":{"breakdowns":{"items":[{"value":1}]}}}}`
	edge := &fakeEdge{goBody: body, pythonBody: body}
	runner := newRunner(t, edge, "canary")
	edge.goBuild = runner.Registry.BuildIdentity
	runner.Documents = map[string]string{"investmentFull": "query InvestmentFull { analytics { breakdowns { items { value } } } }"}
	runner.Registry.DocumentDigest = map[string]string{"investmentFull": "ee55ff66"}
	runner.Routing = map[string]RoutingRow{"investmentFull": {Mode: "canary", CandidateBuild: runner.Registry.BuildIdentity}}

	outcomes, _, err := runner.Run(context.Background())
	if !errors.Is(err, ErrNothingMeasured) {
		t.Fatalf("a declaration matching nothing must refuse the run, got %v", err)
	}
	if outcomes[0].RefusalReason != RefusalStaleOrderInsensitiveList {
		t.Fatalf("expected %s, got %s (%s)", RefusalStaleOrderInsensitiveList, outcomes[0].RefusalReason, outcomes[0].RefusalDetail)
	}
}

// TestRunRefusesOnAnOrderInsensitiveListElementMissingItsKeyField is
// CHAOS-5546 r1's P3 fix pin, second half: the missing-key-field guard
// must also actually terminate the run. Deleting it (`if
// len(result.OrderInsensitiveListRefusals) > 0 { return refuse(...) }`)
// left the full package green before this test existed.
func TestRunRefusesOnAnOrderInsensitiveListElementMissingItsKeyField(t *testing.T) {
	withOverriddenParity(t, "investmentFull", Options{
		OrderInsensitiveLists: []OrderInsensitiveList{
			{Path: "data.analytics.sankey.nodes", KeyFields: []string{"id"}, Reason: "test", Ticket: "CHAOS-0000"},
		},
	})

	// The sole sankey.nodes element carries no "id" -- the declared
	// KeyFields ["id"] cannot pair it.
	body := `{"data":{"analytics":{"breakdowns":{"items":[{"value":1}]},"sankey":{"nodes":[{"value":1}]}}}}`
	edge := &fakeEdge{goBody: body, pythonBody: body}
	runner := newRunner(t, edge, "canary")
	edge.goBuild = runner.Registry.BuildIdentity
	runner.Documents = map[string]string{"investmentFull": "query InvestmentFull { analytics { breakdowns { items { value } } sankey { nodes { value } } } }"}
	runner.Registry.DocumentDigest = map[string]string{"investmentFull": "ee55ff67"}
	runner.Routing = map[string]RoutingRow{"investmentFull": {Mode: "canary", CandidateBuild: runner.Registry.BuildIdentity}}

	outcomes, _, err := runner.Run(context.Background())
	if !errors.Is(err, ErrNothingMeasured) {
		t.Fatalf("a missing key field must refuse the run, got %v", err)
	}
	if outcomes[0].RefusalReason != RefusalOrderInsensitiveListKeyMissing {
		t.Fatalf("expected %s, got %s (%s)", RefusalOrderInsensitiveListKeyMissing, outcomes[0].RefusalReason, outcomes[0].RefusalDetail)
	}
}

// r9 F11: Run resets r.sealed so a SECOND Run cannot emit receipts for the
// FIRST one's measurements. Deleting the reset survived every test,
// because no test ever called Run twice -- yet the whole point of sealing
// is that a receipt describes what THIS run measured.
func TestASecondRunCannotEmitTheFirstRunsReceipts(t *testing.T) {
	body := `{"data":{"featureFlags":[{"key":"a"}]}}`
	edge := &fakeEdge{goBody: body, pythonBody: body, goBuild: "b18e56fa79cfe20ce0f75df148144b832d92be36"}
	runner := newRunner(t, edge, "canary")
	observedAt := time.Unix(1757000000, 0).UTC()

	if _, _, err := runner.Run(context.Background()); err != nil {
		t.Fatalf("first Run: %v", err)
	}
	first, err := runner.ReceiptsFor(observedAt)
	if err != nil {
		t.Fatalf("ReceiptsFor after the first run: %v", err)
	}
	if len(first) != 1 {
		t.Fatalf("the first run must produce exactly one receipt to make this test meaningful, got %d", len(first))
	}

	if first[0].TerminalState != "match" {
		t.Fatalf("the first run must record a match to make this test meaningful, got %q", first[0].TerminalState)
	}

	// The second run measures the SAME operation and reaches the opposite
	// verdict: the Go plane now disagrees with Python.
	edge.goBody = `{"data":{"featureFlags":[{"key":"b"}]}}`
	if _, _, err := runner.Run(context.Background()); err != nil {
		t.Fatalf("second Run: %v", err)
	}

	second, err := runner.ReceiptsFor(observedAt)
	if err != nil {
		t.Fatalf("ReceiptsFor after the second run: %v", err)
	}
	if len(second) != 1 {
		t.Fatalf("the second run emitted %d receipts for ONE operation -- the first run's sealed measurements are still there, which is exactly the forgery sealing exists to prevent: %+v", len(second), second)
	}
	if second[0].TerminalState != "mismatch" {
		t.Fatalf("the receipt describes the PREVIOUS run: terminal state %q, expected \"mismatch\"", second[0].TerminalState)
	}

	refusals, err := runner.RefusalReceipts(observedAt, "proof route unreachable")
	if err != nil {
		t.Fatalf("RefusalReceipts after the second run: %v", err)
	}
	if len(refusals) != 1 {
		t.Fatalf("the refusal path carries the stale seal too: %d receipt(s)", len(refusals))
	}
}

// r10 item (4), prover side: a serving-build header of "unknown" must
// never bind a receipt.
//
// "unknown" is internal/platform/version's default for a build with no
// -ldflags and no VCS stamp -- FetchBuildIdentity already refuses it from
// /buildinfo's JSON body. This pins the HEADER route, which had no test
// at all: `proven` is keyed on candidate_build, so a row reading
// "unknown" can never be matched by any deployment yet LOOKS bound.
//
// The correct behaviour is a REFUSAL (the stamp does not name the
// expected build), not a downgrade to "no binding" -- a downgrade would
// still execute and still write a match. An earlier attempt at this fix
// normalised "unknown" to "" in the header read, which loosened exactly
// that; this test is what caught it.
func TestAServingBuildHeaderOfUnknownIsRefusedNotBound(t *testing.T) {
	body := `{"data":{"featureFlags":[{"key":"a"}]}}`
	for _, header := range []string{"unknown", "  unknown  "} {
		edge := &fakeEdge{goBody: body, pythonBody: body, goBuild: header}
		runner := newRunner(t, edge, "canary")

		outcomes, summary, err := runner.Run(context.Background())
		if err == nil {
			t.Fatalf("header %q: Run succeeded; a build stamp that does not name the expected build must be refused", header)
		}
		if summary.Executed != 0 {
			t.Fatalf("header %q: %d operation(s) executed on an unidentifiable serving build", header, summary.Executed)
		}
		for _, outcome := range outcomes {
			if outcome.RefusalReason == "" {
				t.Fatalf("header %q: refused with NO named reason", header)
			}
		}

		receipts, err := runner.ReceiptsFor(time.Unix(1757000000, 0).UTC())
		if err != nil {
			t.Fatalf("header %q: ReceiptsFor: %v", header, err)
		}
		if len(receipts) != 0 {
			t.Fatalf("header %q: %d receipt(s) written for an unidentifiable build: %+v", header, len(receipts), receipts)
		}
		for _, receipt := range receipts {
			if strings.TrimSpace(receipt.CandidateBuild) == "unknown" {
				t.Fatalf("header %q bound a receipt to candidate build %q -- no deployment can ever match that row", header, receipt.CandidateBuild)
			}
		}
	}
}

// The refusal itself, at the one place it can actually fire: an
// operation that IS routed to Go but whose request needs an identifier
// the table cannot supply.
//
// Today `pr` is unrouted, so a real run reports it as not_routed -- the
// operative fact about it, and the honest one. This pins what happens on
// the day someone routes it: a NAMED refusal, never a request built with
// an invented id whose null-vs-null comparison would read as a match.
func TestARoutedOperationNeedingAnInstanceIDIsRefusedByName(t *testing.T) {
	body := `{"data":{"pr":null}}`
	edge := &fakeEdge{goBody: body, pythonBody: body, goBuild: "b18e56fa79cfe20ce0f75df148144b832d92be36"}
	runner := newRunner(t, edge, "canary")

	// Route `pr` to Go, which is the state this refusal exists for.
	runner.Documents = map[string]string{"pr": "query PrDetail($orgId: String!, $id: ID!) { pr(orgId: $orgId, id: $id) { id } }"}
	runner.Registry.DocumentDigest = map[string]string{"pr": "06ca28a0"}
	runner.Routing = map[string]RoutingRow{"pr": {Mode: "canary", CandidateBuild: "b18e56fa79cfe20ce0f75df148144b832d92be36"}}

	outcomes, summary, err := runner.Run(context.Background())
	if err == nil {
		t.Fatal("a routed operation needing an instance identifier must be refused, not measured with an invented one")
	}
	if summary.Executed != 0 {
		t.Fatalf("%d operation(s) executed with an invented identifier", summary.Executed)
	}
	if len(outcomes) != 1 {
		t.Fatalf("expected one outcome, got %d", len(outcomes))
	}
	// r1 P3: comparing against the constant under test is a tautology --
	// renaming RefusalNeedsInstanceID passed this assertion. The WIRE
	// string is what a reader of ByRefusalReason or review_evidence sees,
	// so that is what is pinned.
	if outcomes[0].RefusalReason != "operation_needs_an_instance_identifier" {
		t.Fatalf("refusal reason = %q, want %q -- the run must SAY why, not report a generic failure", outcomes[0].RefusalReason, "operation_needs_an_instance_identifier")
	}
	if RefusalNeedsInstanceID != "operation_needs_an_instance_identifier" {
		t.Fatalf("the constant moved to %q: refusal reasons are read from stored receipts, so renaming one silently reclassifies every row already written", RefusalNeedsInstanceID)
	}
	if !strings.Contains(outcomes[0].RefusalDetail, "$id") {
		t.Fatalf("the refusal must NAME the variable it cannot supply, got %q", outcomes[0].RefusalDetail)
	}

	// r1 P3: the point of refusing BEFORE the request is that no request
	// happens. Without this, moving the refusal below both HTTP legs left
	// every assertion above green -- the run would have sent an invented
	// id to both planes and merely declined to record the result.
	if len(edge.seen) != 0 {
		t.Fatalf("the refusal fired but %d request(s) still went out: %v -- an operation whose identifier cannot be built must never reach the wire", len(edge.seen), edge.seen)
	}

	// And no receipt is produced from it.
	receipts, err := runner.ReceiptsFor(time.Unix(1757000000, 0).UTC())
	if err != nil {
		t.Fatalf("ReceiptsFor: %v", err)
	}
	if len(receipts) != 0 {
		t.Fatalf("%d receipt(s) written for an operation that was never measured", len(receipts))
	}
}
