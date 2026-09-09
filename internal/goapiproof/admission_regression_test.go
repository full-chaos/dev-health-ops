package goapiproof

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// cEdge answers both legs with full control over status, body and headers.
type cEdge struct {
	candidateBody, baselineBody     string
	candidateStatus, baselineStatus int
	candidateBuild                  string
	stampBuild                      bool
}

func (e *cEdge) run(t *testing.T, mode string, viaProofRoute bool) []Outcome {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var parsed struct {
			Query string `json:"query"`
		}
		_ = json.Unmarshal(raw, &parsed)
		isBaseline := strings.Contains(parsed.Query, "python-plane control")

		if isBaseline {
			w.Header().Set(planeHeader, "python")
			w.Header().Set("Content-Type", "application/json")
			status := e.baselineStatus
			if status == 0 {
				status = http.StatusOK
			}
			w.WriteHeader(status)
			_, _ = w.Write([]byte(e.baselineBody))
			return
		}
		w.Header().Set(planeHeader, "go")
		w.Header().Set("Content-Type", "application/json")
		if e.stampBuild {
			w.Header().Set(buildHeader, e.candidateBuild)
		}
		status := e.candidateStatus
		if status == 0 {
			status = http.StatusOK
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(e.candidateBody))
	}))
	t.Cleanup(server.Close)

	runner := newRunner(t, &fakeEdge{goBody: e.candidateBody, pythonBody: e.baselineBody}, mode)
	runner.Client = server.Client()
	if viaProofRoute {
		runner.Config.GoProofURL = server.URL
		runner.Config.PythonEdgeURL = server.URL
	} else {
		runner.Config.PythonEdgeURL = server.URL
	}
	outcomes, _, _ := runner.Run(context.Background())
	return outcomes
}

// The proof route stamps the serving build on every response, so an ABSENT
// build header there means the response did not come from the proof route
// -- or came from a build that cannot say what it is. Either way it cannot
// back a receipt that names a build. (Confirmation pass C1: this produced
// route=proof terminal=match build="" with no refusal at all.)
func TestProofRouteRefusesAnUnboundServingBuild(t *testing.T) {
	body := `{"data":{"featureFlags":[{"key":"a"}]}}`
	out := (&cEdge{candidateBody: body, baselineBody: body, stampBuild: false}).run(t, "shadow", true)
	if out[0].RefusalReason != RefusalBuildUnbound {
		t.Fatalf("expected %s, got %s (terminal=%s admitted=%v)", RefusalBuildUnbound, out[0].RefusalReason, out[0].TerminalState, out[0].Admitted)
	}
	if out[0].Admitted {
		t.Fatal("an unbound serving build must not be admitted")
	}
}

// Two planes failing the same way is agreement about a failure, not parity.
// (Confirmation pass C2: identical HTTP 500s carrying decodable data
// produced terminal=match executed=true.)
func TestIdenticalNonSuccessStatusIsRefused(t *testing.T) {
	body := `{"data":{"featureFlags":[{"key":"a"}]}}`
	out := (&cEdge{
		candidateBody: body, baselineBody: body,
		candidateStatus: 500, baselineStatus: 500,
		stampBuild: true, candidateBuild: "b18e56fa79cfe20ce0f75df148144b832d92be36",
	}).run(t, "canary", false)
	if out[0].RefusalReason != RefusalNonSuccessStatus {
		t.Fatalf("expected %s, got %s (terminal=%s)", RefusalNonSuccessStatus, out[0].RefusalReason, out[0].TerminalState)
	}
	if out[0].Admitted || out[0].Executed {
		t.Fatalf("a non-2xx pair must be neither admitted nor executed: admitted=%v executed=%v", out[0].Admitted, out[0].Executed)
	}
	if out[0].TerminalState != "dependency_failed" {
		t.Fatalf("the recorded terminal state must stay meaningful, got %s", out[0].TerminalState)
	}
}

// A decoder stops at the end of the first JSON value, so bytes after it are
// never compared -- the comparison would not be over what was served.
// (Confirmation pass C3: a 67-byte candidate and a 39-byte baseline
// produced terminal=match.)
func TestTrailingBytesAreRefused(t *testing.T) {
	clean := `{"data":{"featureFlags":[{"key":"a"}]}}`
	out := (&cEdge{
		candidateBody: clean + `{"data":{"featureFlags":[]}}`,
		baselineBody:  clean,
		stampBuild:    true, candidateBuild: "b18e56fa79cfe20ce0f75df148144b832d92be36",
	}).run(t, "canary", false)
	if out[0].RefusalReason != RefusalTrailingBytes {
		t.Fatalf("expected %s, got %s (terminal=%s, candidate %d bytes vs baseline %d)",
			RefusalTrailingBytes, out[0].RefusalReason, out[0].TerminalState,
			len(out[0].Candidate.Body), len(out[0].Baseline.Body))
	}
	if out[0].Admitted {
		t.Fatal("a body with trailing bytes must not be admitted")
	}
}

// An empty `data` object means nothing was resolved: the operation's own
// root field is absent. (Confirmation pass C4: `{"data":{}}` produced
// terminal=match executed=true.)
func TestEmptyDataObjectIsRefused(t *testing.T) {
	body := `{"data":{}}`
	out := (&cEdge{
		candidateBody: body, baselineBody: body,
		stampBuild: true, candidateBuild: "b18e56fa79cfe20ce0f75df148144b832d92be36",
	}).run(t, "canary", false)
	if out[0].RefusalReason != RefusalEmptyResponseRoot {
		t.Fatalf("expected %s, got %s (terminal=%s)", RefusalEmptyResponseRoot, out[0].RefusalReason, out[0].TerminalState)
	}
	if out[0].Admitted {
		t.Fatal("a response with no resolved root field must not be admitted")
	}
}

// The inverse control: an empty LIST is a legitimate result -- "this org has
// no feature flags" is a real answer -- and must still be admitted, or the
// instrument could never prove an operation over empty data.
func TestEmptyListRootIsStillAdmitted(t *testing.T) {
	body := `{"data":{"featureFlags":[]}}`
	out := (&cEdge{
		candidateBody: body, baselineBody: body,
		stampBuild: true, candidateBuild: "b18e56fa79cfe20ce0f75df148144b832d92be36",
	}).run(t, "canary", false)
	if !out[0].Admitted || out[0].TerminalState != TerminalStateMatch {
		t.Fatalf("an empty list is a real result: admitted=%v terminal=%s refusal=%s (%s)",
			out[0].Admitted, out[0].TerminalState, out[0].RefusalReason, out[0].RefusalDetail)
	}
}

// Round 2's F6. `decoder.More()` is implemented as
// `err == nil && c != ']' && c != '}'`, so a body ending in a stray `}` or
// `]` -- a serializer emitting one closing brace too many, the likeliest
// real shape -- reported FALSE and sailed through the check added to close
// C3. Reproduced before the fix: `{"data":{...}}}` gave TrailingBytes=false.
func TestTrailingClosingTokensAreDetected(t *testing.T) {
	for _, body := range []string{
		`{"data":{"featureFlags":[]}}}`,
		`{"data":{"featureFlags":[]}}]`,
		`{"data":{"featureFlags":[]}}{"x":1}`,
		`{"data":{"featureFlags":[]}} garbage`,
	} {
		snapshot, err := DecodeSnapshot([]byte(body))
		if err != nil {
			continue // an outright decode failure is also a refusal
		}
		if !snapshot.TrailingBytes {
			t.Errorf("%q has bytes after its JSON value but TrailingBytes=false", body)
		}
	}
	// The control: a clean body must NOT be flagged, or the check refuses
	// every measurement and proves nothing.
	clean, err := DecodeSnapshot([]byte(`{"data":{"featureFlags":[]}}`))
	if err != nil {
		t.Fatalf("DecodeSnapshot: %v", err)
	}
	if clean.TrailingBytes {
		t.Fatal("a clean body must not be flagged as carrying trailing bytes")
	}
}

// Round 2's F5. A scalar root, or an object carrying only __typename, is
// what a resolver producing nothing looks like on the wire -- gqlgen adds
// __typename to every selection set. Both were admitted before the fix.
func TestMeaninglessRootsAreRefused(t *testing.T) {
	for name, root := range map[string]any{
		"scalar":        "just-a-string",
		"typename only": map[string]any{"__typename": "FeatureFlagRegistryResult"},
	} {
		t.Run(name, func(t *testing.T) {
			snapshot := Snapshot{DataPresent: true, Data: map[string]any{"featureFlags": root}}
			got := Admit(AdmissionInput{
				Route: RouteEdge, NamedBuild: "b", ResponseRoot: "featureFlags",
				Candidate:     Observation{StatusCode: 200, Plane: "go"},
				Baseline:      Observation{StatusCode: 200, Plane: "python"},
				CandidateSnap: snapshot, BaselineSnap: snapshot,
			})
			if got.Admitted {
				t.Fatalf("%s root must not be admitted", name)
			}
			if got.Reason != RefusalEmptyResponseRoot {
				t.Fatalf("expected %s, got %s", RefusalEmptyResponseRoot, got.Reason)
			}
		})
	}
}

// Round 2's F4, in both directions. capacityForecast and throughputForecast
// declare a NULLABLE root and their resolver documents null as a tolerated
// empty, so both planes returning null is real parity and must be
// admissible -- refusing it made those two operations unprovable against an
// org with no history. Every other operation's root is non-null in the SDL,
// where a null means the operation failed to produce its own result.
func TestNullRootIsAdmissibleOnlyWhereTheSDLAllowsIt(t *testing.T) {
	for _, testCase := range []struct {
		operation string
		nullable  bool
	}{
		{"capacityForecast", true},
		{"throughputForecast", true},
		{"featureFlags", false},
		{"hotspots", false},
	} {
		t.Run(testCase.operation, func(t *testing.T) {
			spec, err := SpecFor(testCase.operation)
			if err != nil {
				t.Fatalf("SpecFor: %v", err)
			}
			if spec.RootNullable != testCase.nullable {
				t.Fatalf("%s: RootNullable=%v want %v", testCase.operation, spec.RootNullable, testCase.nullable)
			}
			snapshot := Snapshot{DataPresent: true, Data: map[string]any{spec.ResponseRoot: nil}}
			got := Admit(AdmissionInput{
				Route: RouteEdge, NamedBuild: "b",
				ResponseRoot: spec.ResponseRoot, RootNullable: spec.RootNullable,
				Candidate:     Observation{StatusCode: 200, Plane: "go"},
				Baseline:      Observation{StatusCode: 200, Plane: "python"},
				CandidateSnap: snapshot, BaselineSnap: snapshot,
			})
			if got.Admitted != testCase.nullable {
				t.Fatalf("%s: a null root gave admitted=%v, want %v (%s)",
					testCase.operation, got.Admitted, testCase.nullable, got.Detail)
			}
		})
	}
}

// Receipt construction requires ADMISSION, not merely Executed.
//
// `Run` sets Executed only after Admit passes, so the two cannot disagree
// today -- but ReceiptsFor is exported and takes whatever it is handed, and
// a confirmation-pass probe drove exactly that: a hand-built outcome with
// Executed true and Admitted false produced a receipt. Given this file's
// entire history is defaults that admitted things nobody thought about, the
// flag is checked rather than assumed.
func TestReceiptsRequireAdmissionNotJustExecution(t *testing.T) {
	runner := &Runner{
		Registry: RegistryView{SchemaDigest: "sha256:x", BuildIdentity: "b"},
		Config:   Config{OrgID: "70d529e0", Window: DefaultWindow()},
	}
	unadmitted := []Outcome{{
		Operation:      "featureFlags",
		DocumentDigest: "d",
		Route:          RouteEdge,
		Executed:       true,
		Admitted:       false,
		TerminalState:  TerminalStateMatch,
	}}

	receipts, err := runner.ReceiptsFor(unadmitted, time.Now().UTC())
	if err != nil {
		t.Fatalf("ReceiptsFor: %v", err)
	}
	if len(receipts) != 0 {
		t.Fatalf("an unadmitted outcome must produce no receipt, got %d", len(receipts))
	}

	refusals, err := runner.RefusalReceipts(unadmitted, time.Now().UTC(), "build moved")
	if err != nil {
		t.Fatalf("RefusalReceipts: %v", err)
	}
	if len(refusals) != 0 {
		t.Fatalf("an unadmitted outcome has no withheld result either, got %d", len(refusals))
	}

	// The control: flipping Admitted true DOES produce one, so the check
	// above is discriminating rather than returning nothing regardless.
	unadmitted[0].Admitted = true
	receipts, err = runner.ReceiptsFor(unadmitted, time.Now().UTC())
	if err != nil {
		t.Fatalf("ReceiptsFor: %v", err)
	}
	if len(receipts) != 1 {
		t.Fatalf("an admitted outcome must produce exactly one receipt, got %d", len(receipts))
	}
}
