package goapiproof

// CHAOS-7442: the MCP class proof's doc-route reference mode. The candidate is
// the MCP pipeline (the proof route); the reference is query-api's OWN /graphql
// answering the same registered document. Two fake servers stand for the two Go
// pipelines, so each test plants one way they can agree or differ.

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/mcpclass"
)

const docRouteAnswer = `{"data":{"featureFlags":[{"key":"a"}]}}`

type docRouteLeg struct {
	plane, build, body string
	status             int
}

func serveLeg(seen *[]string, leg docRouteLeg) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var parsed struct {
			Query string `json:"query"`
		}
		_ = json.Unmarshal(raw, &parsed)
		if seen != nil {
			*seen = append(*seen, parsed.Query)
		}
		if leg.plane != "-" {
			w.Header().Set(planeHeader, leg.plane)
		}
		if leg.build != "-" {
			w.Header().Set(buildHeader, leg.build)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(leg.status)
		_, _ = io.WriteString(w, leg.body)
	}
}

func newDocRouteRunner(t *testing.T, candidate, reference docRouteLeg, seenReference *[]string) *Runner {
	t.Helper()
	proof := httptest.NewServer(serveLeg(nil, candidate))
	t.Cleanup(proof.Close)
	docRoute := httptest.NewServer(serveLeg(seenReference, reference))
	t.Cleanup(docRoute.Close)
	store, err := NewArtifactStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := DefaultGoServedLedger()
	if err != nil {
		t.Fatal(err)
	}
	return &Runner{
		Client:    NewLegClient(0),
		GoServed:  ledger,
		Documents: map[string]string{"featureFlags": "query FeatureFlags { featureFlags { key } }"},
		Registry: RegistryView{
			SchemaDigest: "sha256:29d509cd", BuildIdentity: goEdgeBuild,
			DocumentDigest: map[string]string{"featureFlags": "06ca28a0"},
		},
		Routing:   map[string]RoutingRow{"featureFlags": {Mode: "shadow", CandidateBuild: goEdgeBuild}},
		Artifacts: store,
		Config: Config{
			OrgID: goEdgeOrg, Window: DefaultWindow(),
			DocRouteReference: true, DocRouteURL: docRoute.URL, GoProofURL: proof.URL,
			Auth:            AuthContext{PrincipalKind: "stored_account", Audience: "query-api", KeyID: "local-dev-20260906"},
			EdgeCredential:  StaticCredential("Authorization", "edge access token", orgToken(goEdgeOrg)).BindOrg(goEdgeOrg),
			ProofCredential: StaticCredential("Authorization", "envelope", "Bearer envelope"),
			RecordedBy:      "test", ReviewEvidence: "operator note",
		},
	}
}

func goLeg(body string) docRouteLeg {
	return docRouteLeg{plane: "go", build: goEdgeBuild, body: body, status: http.StatusOK}
}

// Two Go pipelines giving the same answer for the same registered document: a
// match, bound to the build, and the reference leg carried the REGISTERED text
// exactly (no inert comment, which is what would send it to Python).
func TestDocRouteReferenceMatchesTwoGoPipelinesThatAgree(t *testing.T) {
	var seen []string
	runner := newDocRouteRunner(t, goLeg(docRouteAnswer), goLeg(docRouteAnswer), &seen)
	outcomes, summary, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	o := outcomes[0]
	if summary.EdgeMode != EdgeModeDocRoute || o.EdgeMode != EdgeModeDocRoute || !o.Executed || o.TerminalState != TerminalStateMatch ||
		o.DifferencesOutsideBaselineDefect != 0 || o.EdgeBuildBinding != EdgeBuildPresent || o.Route != RouteProof || o.ProvenUnder != "" {
		t.Fatalf("outcome %+v summary %+v, want a bound proof-route match in doc_route mode", o, summary)
	}
	if len(seen) != 1 || seen[0] != runner.Documents["featureFlags"] {
		t.Fatalf("the document route saw %q, want exactly the registered text", seen)
	}
}

// The two pipelines disagreeing is a mismatch outside any declaration: it can never
// be a match, and it is not excused by a Python baseline defect (none applies).
func TestDocRouteReferenceReportsADivergenceBetweenTheTwoPipelines(t *testing.T) {
	runner := newDocRouteRunner(t, goLeg(docRouteAnswer), goLeg(`{"data":{"featureFlags":[{"key":"b"}]}}`), nil)
	outcomes, _, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	o := outcomes[0]
	if !o.Executed || o.TerminalState != TerminalStateMismatch || o.DifferencesOutsideBaselineDefect == 0 || len(o.BaselineDefects) != 0 {
		t.Fatalf("outcome %+v, want a mismatch with differences outside any declaration and no citation", o)
	}
}

// The reference must be query-api itself: the Python plane, no plane evidence, or a
// different build each refuse by name.
func TestDocRouteReferenceRefusesAReferenceThatIsNotTheSameGoBuild(t *testing.T) {
	for name, tc := range map[string]struct {
		reference docRouteLeg
		reason    string
	}{
		"python plane":      {docRouteLeg{plane: "python", build: "-", body: docRouteAnswer, status: 200}, RefusalWrongPlane},
		"no plane evidence": {docRouteLeg{plane: "-", build: goEdgeBuild, body: docRouteAnswer, status: 200}, RefusalPlaneUnidentified},
		"another build":     {docRouteLeg{plane: "go", build: "deadbeef", body: docRouteAnswer, status: 200}, RefusalBuildMismatch},
		"no build header":   {docRouteLeg{plane: "go", build: "-", body: docRouteAnswer, status: 200}, RefusalBuildUnbound},
	} {
		t.Run(name, func(t *testing.T) {
			runner := newDocRouteRunner(t, goLeg(docRouteAnswer), tc.reference, nil)
			outcomes, _, _ := runner.Run(context.Background())
			if outcomes[0].Executed || outcomes[0].RefusalReason != tc.reason {
				t.Fatalf("outcome %+v, want refusal %q", outcomes[0], tc.reason)
			}
		})
	}
}

// A reference that errors answers an error on one side: refused, never a match
// (agreement on a failure is not proof).
func TestDocRouteReferenceRefusesAnErroredReference(t *testing.T) {
	runner := newDocRouteRunner(t, goLeg(docRouteAnswer), goLeg(`{"errors":[{"message":"x"}],"data":null}`), nil)
	outcomes, _, _ := runner.Run(context.Background())
	if outcomes[0].Executed {
		t.Fatalf("outcome %+v, want a refusal", outcomes[0])
	}
}

func TestDocRouteReferenceNeedsItsOwnConfiguration(t *testing.T) {
	both := newDocRouteRunner(t, goLeg(docRouteAnswer), goLeg(docRouteAnswer), nil)
	both.Config.GoEdge = true
	if _, _, err := both.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "exclusive") {
		t.Fatalf("GoEdge with DocRouteReference: err = %v, want the exclusivity refusal", err)
	}
	none := newDocRouteRunner(t, goLeg(docRouteAnswer), goLeg(docRouteAnswer), nil)
	none.Config.DocRouteURL = ""
	if _, _, err := none.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "DocRouteURL") {
		t.Fatalf("no DocRouteURL: err = %v, want the missing-URL refusal", err)
	}
}

// In doc-route mode the go-only class does not exist: a reference that is the
// Python deletion error is NOT a go-only proof.
func TestDocRouteReferenceNeverAdmitsTheGoOnlyClass(t *testing.T) {
	ledger, err := DefaultGoServedLedger()
	if err != nil {
		t.Fatal(err)
	}
	deletion, _ := json.Marshal(map[string]any{
		"errors": []any{map[string]any{"message": ledger.ExpectedMessage("featureFlags"), "path": []any{"featureFlags"}}},
		"data":   map[string]any{"featureFlags": nil},
	})
	runner := newDocRouteRunner(t, goLeg(docRouteAnswer), goLeg(string(deletion)), nil)
	outcomes, _, _ := runner.Run(context.Background())
	if outcomes[0].Executed || outcomes[0].ProvenUnder == ProvenUnderGoOnly {
		t.Fatalf("outcome %+v, want a refusal, never a go-only proof", outcomes[0])
	}
}

func TestMCPClassReceiptsInDocRouteMode(t *testing.T) {
	sources := map[string][]string{mcpclass.Operation("hotspots"): {"hotspots", "hotspotsAll"}}
	mismatch := sealedMatch("hotspotsAll", "")
	mismatch.terminalState = TerminalStateMismatch
	mismatch.differencesOutsideBaselineDefect = 1
	backed := map[string]bool{"hotspots": true, "hotspotsAll": true}
	onlyOne := map[string]bool{"hotspots": true}
	none := map[string]bool{}
	for name, tc := range map[string]struct {
		sealed []sealedOutcome
		backed map[string]bool
		state  string
		count  int // receipts
	}{
		"both shapes match and are backed":                      {[]sealedOutcome{sealedMatch("hotspots", ""), sealedMatch("hotspotsAll", "")}, backed, TerminalStateMatch, 1},
		"a matching shape of an unbacked operation is excluded": {[]sealedOutcome{sealedMatch("hotspots", ""), sealedMatch("hotspotsAll", "")}, onlyOne, TerminalStateMatch, 1},
		"nothing backed: nothing counted, no receipt":           {[]sealedOutcome{sealedMatch("hotspots", ""), sealedMatch("hotspotsAll", "")}, none, "", 0},
		"a mismatch blocks even on an unbacked operation":       {[]sealedOutcome{sealedMatch("hotspots", ""), mismatch}, onlyOne, TerminalStateMismatch, 1},
		"a mismatch blocks on a backed operation":               {[]sealedOutcome{sealedMatch("hotspots", ""), mismatch}, backed, TerminalStateMismatch, 1},
	} {
		t.Run(name, func(t *testing.T) {
			runner := classRunner(tc.sealed)
			runner.Config.DocRouteReference = true
			outcomes := []Outcome{executedOutcome("hotspots", ""), executedOutcome("hotspotsAll", "")}
			receipts, verdicts, err := runner.MCPClassReceipts(outcomes, sources, tc.backed, time.Now().UTC())
			if err != nil {
				t.Fatal(err)
			}
			if len(receipts) != tc.count || len(verdicts) != 1 || verdicts[0].TerminalState != tc.state {
				t.Fatalf("receipts=%d verdicts=%+v, want %d receipts and state %q", len(receipts), verdicts, tc.count, tc.state)
			}
			if tc.count == 1 {
				var provenance ReceiptProvenance
				if err := json.Unmarshal([]byte(receipts[0].ReviewEvidence), &provenance); err != nil || provenance.EdgeMode != EdgeModeDocRoute ||
					provenance.MCPClass == nil || provenance.MCPClass.Reference != "go_document_route" {
					t.Fatalf("provenance %q (err %v), want the doc-route reference named", receipts[0].ReviewEvidence, err)
				}
			}
			if name == "a matching shape of an unbacked operation is excluded" {
				if len(verdicts[0].Excluded) != 1 || !strings.Contains(verdicts[0].Excluded[0], "doc_operation_not_receipt_backed") {
					t.Fatalf("excluded %v, want the unbacked shape named", verdicts[0].Excluded)
				}
			}
		})
	}
	// A shape refused for measuring nothing: excluded for an unbacked operation (named),
	// a failure for a backed one, and any other refusal of an unbacked operation still blocks.
	vacuous := Outcome{Operation: "hotspotsAll", RefusalReason: RefusalVacuousEmptyLegs}
	transport := Outcome{Operation: "hotspotsAll", RefusalReason: RefusalTransport}
	for name, tc := range map[string]struct {
		refused Outcome
		backed  map[string]bool
		state   string
	}{
		"vacuous on an unbacked operation":          {vacuous, onlyOne, TerminalStateMatch},
		"vacuous on a backed operation blocks":      {vacuous, backed, "proof_failed"},
		"transport on an unbacked operation blocks": {transport, onlyOne, "proof_failed"},
	} {
		t.Run(name, func(t *testing.T) {
			runner := classRunner([]sealedOutcome{sealedMatch("hotspots", ""), {}})
			runner.Config.DocRouteReference = true
			_, verdicts, err := runner.MCPClassReceipts([]Outcome{executedOutcome("hotspots", ""), tc.refused}, sources, tc.backed, time.Now().UTC())
			if err != nil || len(verdicts) != 1 || verdicts[0].TerminalState != tc.state {
				t.Fatalf("verdicts %+v err %v, want state %q", verdicts, err, tc.state)
			}
		})
	}
	// Doc-route mode without the backed set is refused, never read as "all backed".
	runner := classRunner([]sealedOutcome{sealedMatch("hotspots", "")})
	runner.Config.DocRouteReference = true
	if _, _, err := runner.MCPClassReceipts([]Outcome{executedOutcome("hotspots", "")}, sources, nil, time.Now()); err == nil {
		t.Fatal("doc-route mode accepted a nil backed set")
	}
}

// A Python baseline defect describes a difference from Python; in doc-route mode the
// reference is Go, so the operation's declared defects are dropped and equal answers
// match (kept, the declaration would match nothing and refuse as stale).
func TestDocRouteReferenceDropsDeclaredPythonBaselineDefects(t *testing.T) {
	answer := `{"data":{"capacityForecasts":{"edges":[{"node":{"computedAt":"2026-01-01T00:00:00Z"}}]}}}`
	runner := newDocRouteRunner(t, goLeg(answer), goLeg(answer), nil)
	runner.Documents = map[string]string{"capacityForecasts": "query CapacityForecasts($orgId: String!) { capacityForecasts(orgId: $orgId) { edges { node { computedAt } } } }"}
	runner.Registry.DocumentDigest = map[string]string{"capacityForecasts": "06ca28a1"}
	runner.Routing = map[string]RoutingRow{"capacityForecasts": {Mode: "shadow", CandidateBuild: goEdgeBuild}}
	outcomes, _, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if o := outcomes[0]; !o.Executed || o.TerminalState != TerminalStateMatch || len(o.BaselineDefects) != 0 {
		t.Fatalf("outcome %+v, want a plain match with no Python citation", o)
	}
}

// The exclusions of the doc-route mode do not leak into the Python-reference mode: there a
// shape refused for measuring nothing is a failure whatever its operation.
func TestPythonReferenceModeDoesNotExcludeAVacuousShape(t *testing.T) {
	sources := map[string][]string{mcpclass.Operation("hotspots"): {"hotspots", "hotspotsAll"}}
	runner := classRunner([]sealedOutcome{sealedMatch("hotspots", ""), {}})
	vacuous := Outcome{Operation: "hotspotsAll", RefusalReason: RefusalVacuousEmptyLegs}
	_, verdicts, err := runner.MCPClassReceipts([]Outcome{executedOutcome("hotspots", ""), vacuous}, sources, nil, time.Now().UTC())
	if err != nil || len(verdicts) != 1 || verdicts[0].TerminalState != "proof_failed" || len(verdicts[0].Excluded) != 0 {
		t.Fatalf("verdicts %+v err %v, want proof_failed with nothing excluded", verdicts, err)
	}
}
