package restprove

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

// TestSkewAdmittedCaseProducesItsIDsFromTheSecondBaselineRead (R8): a
// skew-admitted case stands on B2 against C1, so an id the request
// produces is read from B2 -- the reference's own answer after the write
// -- never from B1, which the write made stale.
func TestSkewAdmittedCaseProducesItsIDsFromTheSecondBaselineRead(t *testing.T) {
	const build = "abc123def456"
	body := func(ref string) string { return `{"items":[{"ref":"` + ref + `"}]}` }
	candidateURL, baselineURL, _, baselineCalls := skewServers(t, build, body("NEW-1"), []string{body("OLD-1"), body("NEW-1")})
	f := flags{queryAPIURL: candidateURL, pythonAPIURL: baselineURL, org: "org-1", recordedBy: "r", reviewEvidence: "e", timeout: 5 * time.Second}
	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/things"}
	request := goapiproof.RESTRequest{Name: "things", WantCandidateStatus: 200, WantBaselineStatus: 200, BodyMode: goapiproof.RESTBodyModeJSON,
		Produces: []goapiproof.RESTIDProducer{{Name: "thing_ref", ListPath: "items", IDField: "ref"}}}
	writer := &fakeReceiptWriter{}
	out, err := proveOneRESTRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/things", spec, request,
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, nil, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if out.WriteSkew == nil || out.WriteSkew.Verdict != goapiproof.WriteSkewAdmitted || baselineCalls() != 2 {
		t.Fatalf("out = %+v (write skew %+v), baseline calls %d: want skew-admitted after one re-read", out, out.WriteSkew, baselineCalls())
	}
	if out.WriteSkew.SecondBaselineWireAttempts != 1 {
		t.Fatalf("second baseline wire attempts = %d, want 1 (recorded like every leg's)", out.WriteSkew.SecondBaselineWireAttempts)
	}
	if out.producedIDs["thing_ref"] != "NEW-1" {
		t.Fatalf("produced %q, want %q from the second baseline read", out.producedIDs["thing_ref"], "NEW-1")
	}
	cited := false
	for _, citation := range out.BaselineDefectsMatched {
		if citation == goapiproof.WriteSkewCitation {
			cited = true
		}
	}
	if !cited {
		t.Fatalf("baseline_defect = %v, want it to cite %q", out.BaselineDefectsMatched, goapiproof.WriteSkewCitation)
	}
	if len(writer.receipts) != 1 || out.TerminalState != goapiproof.TerminalStateMismatch || out.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("receipts=%d terminal=%q outside=%d, want one mismatch receipt with nothing outside", len(writer.receipts), out.TerminalState, out.DifferencesOutsideBaselineDefect)
	}
}

// TestSkewAdmittedIteratingCasePassesTheBoundedSearchGateOnTheSecondComparison
// (R7i): an iterating request whose case is skew-admitted is judged by the
// bounded-search gate on B2 against C1, exactly as the normal path judges
// a clean comparison. Here B2/C1 match but the declared id list carries
// elements without the id, so the case is refused by the gate's own name
// and writes no receipt.
func TestSkewAdmittedIteratingCasePassesTheBoundedSearchGateOnTheSecondComparison(t *testing.T) {
	const build = "abc123def456"
	body := func(v string) string { return `{"items":[{"v":` + v + `}]}` }
	candidateURL, baselineURL, _, baselineCalls := skewServers(t, build, body("2"), []string{body("3"), body("2")})
	f := flags{queryAPIURL: candidateURL, pythonAPIURL: baselineURL, org: "org-1", recordedBy: "r", reviewEvidence: "e", timeout: 5 * time.Second}
	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/things/p-1"}
	binding := goapiproof.RESTIDBinding{Producer: "thing_id", PathParam: "thing_id", Candidates: 10, ExposeAs: "chosen_thing_id"}
	request := goapiproof.RESTRequest{Name: "things", WantCandidateStatus: 200, WantBaselineStatus: 200, BodyMode: goapiproof.RESTBodyModeJSON,
		IDBindings: []goapiproof.RESTIDBinding{binding},
		Produces:   []goapiproof.RESTIDProducer{{Name: "item_id", ListPath: "items", IDField: "id"}}}
	writer := &fakeReceiptWriter{}
	out, err := proveOneRESTRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/things/{thing_id}", spec, request,
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, nil, false, map[string]string{"thing_id": "p-1"})
	if err != nil {
		t.Fatal(err)
	}
	if out.WriteSkew == nil || out.WriteSkew.Verdict != goapiproof.WriteSkewAdmitted || baselineCalls() != 2 {
		t.Fatalf("write skew %+v, baseline calls %d: want the case skew-admitted before the gate", out.WriteSkew, baselineCalls())
	}
	if out.Admitted || out.Refusal != goapiproof.RESTRefusalDeclaredIDListUnrecognised || len(writer.receipts) != 0 {
		t.Fatalf("admitted=%v refusal=%q receipts=%d, want refused %q with no receipt", out.Admitted, out.Refusal, len(writer.receipts), goapiproof.RESTRefusalDeclaredIDListUnrecognised)
	}
}

// TestSkewAdmittedCasesAreCountedPerRoute: the run summary counts the
// skew-admitted cases per operation, so a route admitted by skew run
// after run shows as a pattern.
func TestSkewAdmittedCasesAreCountedPerRoute(t *testing.T) {
	outcomes := []outcome{
		{Operation: "REST:GET:/a", WriteSkew: &writeSkewRecord{Verdict: goapiproof.WriteSkewAdmitted}},
		{Operation: "REST:GET:/a", WriteSkew: &writeSkewRecord{Verdict: goapiproof.WriteSkewAdmitted}},
		{Operation: "REST:GET:/a", WriteSkew: &writeSkewRecord{Verdict: goapiproof.WriteSkewStands}},
		{Operation: "REST:GET:/b", WriteSkew: &writeSkewRecord{Verdict: goapiproof.WriteSkewRefused}},
		{Operation: "REST:GET:/c"},
	}
	got := skewAdmittedByOperation(outcomes)
	if len(got) != 1 || got["REST:GET:/a"] != 2 {
		t.Fatalf("skewAdmittedByOperation = %v, want map[REST:GET:/a:2]", got)
	}
	if skewAdmittedByOperation(nil) != nil {
		t.Fatal("no skew-admitted case must count as absent")
	}
}

// TestTheSecondBaselineReadIsHeldToTheLegReadContract: B2 is read through
// the same leg client and admitted by the same RESTAdmit as B1, so a
// second read that is not provably the Python app's own answer is refused
// by the contract's own name (R2), and one cut by the run's own deadline
// is named for that (R1), never as a leg timeout.
func TestTheSecondBaselineReadIsHeldToTheLegReadContract(t *testing.T) {
	const build = "abc123def456"
	for _, cell := range []struct {
		name        string
		second      func(w http.ResponseWriter, r *http.Request)
		runDeadline time.Duration
		want        string
	}{
		{"second read without the Python server header", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"v":2}`))
		}, 0, goapiproof.RESTRefusalBaselineNotReferencePlane},
		{"second read carrying query-api's build header", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Server", goapiproof.ReferencePlaneServer)
			w.Header().Set("x-dev-health-build", build)
			_, _ = w.Write([]byte(`{"v":2}`))
		}, 0, goapiproof.RESTRefusalBaselineNotReferencePlane},
		{"second read served under an impersonation session", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Server", goapiproof.ReferencePlaneServer)
			w.Header().Set("x-impersonating", "true")
			_, _ = w.Write([]byte(`{"v":2}`))
		}, 0, goapiproof.RESTRefusalServedUnderImpersonation},
		{"second read cut by the run deadline", func(_ http.ResponseWriter, r *http.Request) {
			<-r.Context().Done()
		}, 700 * time.Millisecond, goapiproof.RESTRefusalBaselineLegCutByRunDeadline},
	} {
		t.Run(cell.name, func(t *testing.T) {
			candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("x-dev-health-build", build)
				_, _ = w.Write([]byte(`{"v":2}`))
			}))
			defer candidate.Close()
			var calls atomic.Int32
			baseline := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if calls.Add(1) == 1 {
					w.Header().Set("Server", goapiproof.ReferencePlaneServer)
					_, _ = w.Write([]byte(`{"v":3}`))
					return
				}
				cell.second(w, r)
			}))
			defer baseline.Close()
			ctx := context.Background()
			if cell.runDeadline > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, cell.runDeadline)
				defer cancel()
			}
			f := flags{queryAPIURL: candidate.URL, pythonAPIURL: baseline.URL, org: "org-1", recordedBy: "r", reviewEvidence: "e", timeout: time.Minute}
			spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/v"}
			request := goapiproof.RESTRequest{Name: "v", WantCandidateStatus: 200, WantBaselineStatus: 200, BodyMode: goapiproof.RESTBodyModeJSON}
			writer := &fakeReceiptWriter{}
			out, err := proveOneRESTRequest(ctx, goapiproof.NewLegClient(0), f, "REST:GET:/v", spec, request,
				staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, nil, false, nil)
			if err != nil {
				t.Fatal(err)
			}
			if calls.Load() != 2 {
				t.Fatalf("baseline reads = %d, want 2 (the case must reach its re-read)", calls.Load())
			}
			if out.Admitted || out.Refusal != cell.want || len(writer.receipts) != 0 || out.WriteSkew == nil {
				t.Fatalf("admitted=%v refusal=%q receipts=%d write_skew=%+v, want refused %q with no receipt", out.Admitted, out.Refusal, len(writer.receipts), out.WriteSkew, cell.want)
			}
		})
	}
}

// TestAnAmbiguousLeafPathIsRefusedThroughTheProver runs the shape of a
// nested field and a literal key spelling the same path through
// proveOneRESTRequest: the nested leaf's second read is a third value
// within any float tolerance, the literal key's equals the candidate.
// The case is refused by name with no receipt -- never admitted on the
// literal key's witness.
func TestAnAmbiguousLeafPathIsRefusedThroughTheProver(t *testing.T) {
	const build = "abc123def456"
	candidateURL, baselineURL, _, baselineCalls := skewServers(t, build,
		`{"s":{"mean":0.5},"s.mean":0.5}`,
		[]string{`{"s":{"mean":0.25},"s.mean":0.25}`, `{"s":{"mean":0.50000000001},"s.mean":0.5}`})
	f := flags{queryAPIURL: candidateURL, pythonAPIURL: baselineURL, org: "org-1", recordedBy: "r", reviewEvidence: "e", timeout: 5 * time.Second}
	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/s"}
	request := goapiproof.RESTRequest{Name: "s", WantCandidateStatus: 200, WantBaselineStatus: 200, BodyMode: goapiproof.RESTBodyModeJSON}
	writer := &fakeReceiptWriter{}
	out, err := proveOneRESTRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/s", spec, request,
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, nil, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if baselineCalls() != 2 || out.Admitted || out.Refusal != goapiproof.RESTRefusalRereadLeafAmbiguous || len(writer.receipts) != 0 {
		t.Fatalf("baseline calls=%d admitted=%v refusal=%q receipts=%d, want refused %q with no receipt", baselineCalls(), out.Admitted, out.Refusal, len(writer.receipts), goapiproof.RESTRefusalRereadLeafAmbiguous)
	}
}
