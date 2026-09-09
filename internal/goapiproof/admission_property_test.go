package goapiproof

import "testing"

// This file pins the INVARIANT, not a list of cases.
//
// The four confirmation-pass findings were not four bugs; they were four
// consequences of one structure. Ten instances across two rounds all had the
// same shape: a response the code had no specific rule against produced a
// `deployed_executed`/`match` receipt. Enumerating a fifth, sixth and
// seventh rule would have kept that structure and kept paying for it.
//
// So the property tested here is the one that makes the next unknown shape a
// non-event: `match` and `executed` are reachable ONLY through Admit. A
// mutant that bypasses the gate must turn this file red -- that mutant proof
// is what distinguishes a test that pins the invariant from one that merely
// agrees with today's code.

// admissionCases enumerates one violation per precondition, plus the
// admissible control. Each case names the precondition it breaks so a
// failure says which one stopped holding.
func admissionCases() []struct {
	name          string
	violates      string
	input         AdmissionInput
	wantAdmitted  bool
	wantRefusedAs string
} {
	good := func() AdmissionInput {
		body := Snapshot{
			DataPresent: true,
			Data:        map[string]any{"featureFlags": []any{map[string]any{"key": "a"}}},
		}
		return AdmissionInput{
			Route:         RouteEdge,
			NamedBuild:    "b18e56fa7",
			ResponseRoot:  "featureFlags",
			Candidate:     Observation{StatusCode: 200, Plane: "go"},
			Baseline:      Observation{StatusCode: 200, Plane: "python"},
			CandidateSnap: body,
			BaselineSnap:  body,
		}
	}
	with := func(mutate func(*AdmissionInput)) AdmissionInput {
		in := good()
		mutate(&in)
		return in
	}

	return []struct {
		name          string
		violates      string
		input         AdmissionInput
		wantAdmitted  bool
		wantRefusedAs string
	}{
		{name: "admissible control", violates: "nothing", input: good(), wantAdmitted: true},

		{name: "candidate plane absent", violates: "plane identified",
			input:         with(func(in *AdmissionInput) { in.Candidate.Plane = "" }),
			wantRefusedAs: RefusalPlaneUnidentified},
		{name: "candidate served by python", violates: "plane is go",
			input:         with(func(in *AdmissionInput) { in.Candidate.Plane = "python" }),
			wantRefusedAs: RefusalWrongPlane},
		{name: "baseline plane absent", violates: "plane identified",
			input:         with(func(in *AdmissionInput) { in.Baseline.Plane = "" }),
			wantRefusedAs: RefusalPlaneUnidentified},
		{name: "baseline served by go", violates: "control is python",
			input:         with(func(in *AdmissionInput) { in.Baseline.Plane = "go" }),
			wantRefusedAs: RefusalWrongPlane},

		{name: "proof route with no serving build", violates: "build bound where the route can bind it",
			input:         with(func(in *AdmissionInput) { in.Route = RouteProof }),
			wantRefusedAs: RefusalBuildUnbound},
		{name: "proof route with the right serving build", violates: "nothing",
			input: with(func(in *AdmissionInput) {
				in.Route = RouteProof
				in.Candidate.Build = "b18e56fa7"
			}),
			wantAdmitted: true},
		{name: "serving build disagrees", violates: "build binding",
			input:         with(func(in *AdmissionInput) { in.Candidate.Build = "some-other-build" }),
			wantRefusedAs: RefusalBuildMismatch},
		{name: "unknown route", violates: "route is a measurement route",
			input:         with(func(in *AdmissionInput) { in.Route = "sideways" }),
			wantRefusedAs: RefusalNotRouted},

		{name: "candidate not 2xx", violates: "HTTP success",
			input:         with(func(in *AdmissionInput) { in.Candidate.StatusCode = 500 }),
			wantRefusedAs: RefusalNonSuccessStatus},
		{name: "baseline not 2xx", violates: "HTTP success",
			input:         with(func(in *AdmissionInput) { in.Baseline.StatusCode = 503 }),
			wantRefusedAs: RefusalNonSuccessStatus},
		{name: "candidate 3xx", violates: "HTTP success",
			input:         with(func(in *AdmissionInput) { in.Candidate.StatusCode = 302 }),
			wantRefusedAs: RefusalNonSuccessStatus},

		{name: "candidate body has trailing bytes", violates: "one JSON value consuming the body",
			input:         with(func(in *AdmissionInput) { in.CandidateSnap.TrailingBytes = true }),
			wantRefusedAs: RefusalTrailingBytes},
		{name: "baseline body has trailing bytes", violates: "one JSON value consuming the body",
			input:         with(func(in *AdmissionInput) { in.BaselineSnap.TrailingBytes = true }),
			wantRefusedAs: RefusalTrailingBytes},

		{name: "candidate carries graphql errors", violates: "no errors",
			input: with(func(in *AdmissionInput) {
				in.CandidateSnap.Errors = []map[string]any{{"message": "boom"}}
			}),
			wantRefusedAs: RefusalErroredResponse},
		{name: "baseline carries graphql errors", violates: "no errors",
			input: with(func(in *AdmissionInput) {
				in.BaselineSnap.Errors = []map[string]any{{"message": "boom"}}
			}),
			wantRefusedAs: RefusalErroredResponse},

		{name: "data absent entirely", violates: "root field present",
			input: with(func(in *AdmissionInput) {
				in.CandidateSnap.DataPresent = false
				in.CandidateSnap.Data = nil
			}),
			wantRefusedAs: RefusalEmptyResponseRoot},
		{name: "data is an empty object", violates: "root field present",
			input:         with(func(in *AdmissionInput) { in.CandidateSnap.Data = map[string]any{} }),
			wantRefusedAs: RefusalEmptyResponseRoot},
		{name: "root field missing from data", violates: "root field present",
			input: with(func(in *AdmissionInput) {
				in.CandidateSnap.Data = map[string]any{"somethingElse": 1}
			}),
			wantRefusedAs: RefusalEmptyResponseRoot},
		{name: "root field is null", violates: "root field non-empty",
			input: with(func(in *AdmissionInput) {
				in.CandidateSnap.Data = map[string]any{"featureFlags": nil}
			}),
			wantRefusedAs: RefusalEmptyResponseRoot},
		{name: "root field is an empty object", violates: "root field non-empty",
			input: with(func(in *AdmissionInput) {
				in.CandidateSnap.Data = map[string]any{"featureFlags": map[string]any{}}
			}),
			wantRefusedAs: RefusalEmptyResponseRoot},
		{name: "baseline root field missing", violates: "root field present",
			input: with(func(in *AdmissionInput) {
				in.BaselineSnap.Data = map[string]any{}
			}),
			wantRefusedAs: RefusalEmptyResponseRoot},
		{name: "operation declares no response root", violates: "root field checkable",
			input:         with(func(in *AdmissionInput) { in.ResponseRoot = "" }),
			wantRefusedAs: RefusalEmptyResponseRoot},

		{name: "empty list root is a real result", violates: "nothing",
			input: with(func(in *AdmissionInput) {
				in.CandidateSnap.Data = map[string]any{"featureFlags": []any{}}
				in.BaselineSnap.Data = map[string]any{"featureFlags": []any{}}
			}),
			wantAdmitted: true},
	}
}

// Every enumerated precondition, violated one at a time, must refuse by its
// own name -- and every refusal must carry a reason and a detail, because a
// refusal an operator cannot act on is barely better than a false proof.
func TestAdmitEnforcesEveryPrecondition(t *testing.T) {
	for _, testCase := range admissionCases() {
		t.Run(testCase.name, func(t *testing.T) {
			got := Admit(testCase.input)
			if got.Admitted != testCase.wantAdmitted {
				t.Fatalf("violates %q: admitted=%v want %v (reason=%s detail=%s)",
					testCase.violates, got.Admitted, testCase.wantAdmitted, got.Reason, got.Detail)
			}
			if testCase.wantAdmitted {
				if got.Reason != "" {
					t.Fatalf("an admitted input must carry no refusal reason, got %s", got.Reason)
				}
				return
			}
			if got.Reason != testCase.wantRefusedAs {
				t.Fatalf("violates %q: refused as %s, want %s", testCase.violates, got.Reason, testCase.wantRefusedAs)
			}
			if got.Detail == "" {
				t.Fatalf("refusal %s carries no detail: an operator cannot act on it", got.Reason)
			}
		})
	}
}

// Non-vacuity: the case table must actually exercise every refusal reason
// Admit can produce. A precondition added to Admit without a case here would
// otherwise leave this suite silently agreeing with itself.
func TestAdmissionCasesCoverEveryRefusalReason(t *testing.T) {
	covered := map[string]bool{}
	admissibleCases := 0
	for _, testCase := range admissionCases() {
		if testCase.wantAdmitted {
			admissibleCases++
			continue
		}
		covered[testCase.wantRefusedAs] = true
	}

	for _, reason := range []string{
		RefusalPlaneUnidentified, RefusalWrongPlane, RefusalBuildUnbound, RefusalBuildMismatch,
		RefusalNotRouted, RefusalNonSuccessStatus, RefusalTrailingBytes,
		RefusalErroredResponse, RefusalEmptyResponseRoot,
	} {
		if !covered[reason] {
			t.Errorf("no case violates the precondition behind %s", reason)
		}
	}
	// A suite of only-refusals would pass every negative and prove nothing.
	if admissibleCases < 3 {
		t.Fatalf("only %d admissible control(s): a gate that refuses everything passes every negative case", admissibleCases)
	}
}

// THE INVARIANT.
//
// Drive the whole runner across every shape the case table describes and
// assert the structural property directly: a terminal state of `match`, and
// `Executed` at all, occur ONLY on an outcome Admit admitted. This is the
// test a bypass of the gate must break -- see the mutant proof recorded in
// the commit message.
func TestMatchIsReachableOnlyThroughAdmission(t *testing.T) {
	shapes := []struct {
		name                        string
		candidateBody, baselineBody string
		candidateStatus             int
		stampBuild                  bool
		mode                        string
		viaProof                    bool
	}{
		{name: "clean pair", candidateBody: `{"data":{"featureFlags":[{"key":"a"}]}}`, baselineBody: `{"data":{"featureFlags":[{"key":"a"}]}}`, stampBuild: true, mode: "canary"},
		{name: "differing data", candidateBody: `{"data":{"featureFlags":[{"key":"a"}]}}`, baselineBody: `{"data":{"featureFlags":[{"key":"b"}]}}`, stampBuild: true, mode: "canary"},
		{name: "identical errors", candidateBody: `{"errors":[{"message":"x"}]}`, baselineBody: `{"errors":[{"message":"x"}]}`, stampBuild: true, mode: "canary"},
		{name: "non-success status", candidateBody: `{"data":{"featureFlags":[]}}`, baselineBody: `{"data":{"featureFlags":[]}}`, candidateStatus: 500, stampBuild: true, mode: "canary"},
		{name: "trailing bytes", candidateBody: `{"data":{"featureFlags":[]}}{"x":1}`, baselineBody: `{"data":{"featureFlags":[]}}`, stampBuild: true, mode: "canary"},
		{name: "empty data object", candidateBody: `{"data":{}}`, baselineBody: `{"data":{}}`, stampBuild: true, mode: "canary"},
		{name: "proof route without a build", candidateBody: `{"data":{"featureFlags":[]}}`, baselineBody: `{"data":{"featureFlags":[]}}`, mode: "shadow", viaProof: true},
		{name: "proof route with a build", candidateBody: `{"data":{"featureFlags":[]}}`, baselineBody: `{"data":{"featureFlags":[]}}`, stampBuild: true, mode: "shadow", viaProof: true},
	}

	sawMatch, sawRefusal := 0, 0
	for _, shape := range shapes {
		t.Run(shape.name, func(t *testing.T) {
			edge := &cEdge{
				candidateBody: shape.candidateBody, baselineBody: shape.baselineBody,
				candidateStatus: shape.candidateStatus,
				stampBuild:      shape.stampBuild, candidateBuild: "b18e56fa79cfe20ce0f75df148144b832d92be36",
			}
			outcomes := edge.run(t, shape.mode, shape.viaProof)
			for _, outcome := range outcomes {
				if outcome.TerminalState == TerminalStateMatch && !outcome.Admitted {
					t.Fatalf("MATCH without admission -- the gate was bypassed: %+v", outcome)
				}
				if outcome.Executed && !outcome.Admitted {
					t.Fatalf("executed without admission -- the gate was bypassed: %+v", outcome)
				}
				if !outcome.Admitted && outcome.RefusalReason == "" {
					t.Fatalf("a non-admitted outcome must name its refusal: %+v", outcome)
				}
				if outcome.TerminalState == TerminalStateMatch {
					sawMatch++
				}
				if !outcome.Admitted {
					sawRefusal++
				}
			}
		})
	}

	// Both directions must actually occur, or the invariant above holds
	// vacuously over a set that never produced a match (or never a refusal).
	if sawMatch == 0 {
		t.Fatal("no shape produced a match: this test cannot discriminate a bypass")
	}
	if sawRefusal == 0 {
		t.Fatal("no shape was refused: this test cannot discriminate a bypass")
	}
}
