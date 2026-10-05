package goapiproof

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/mcpclass"
)

func TestEnableRefusesClassRequestsThatBreakTheClassRules(t *testing.T) {
	op, digest := mcpclass.Operation("hotspots"), mcpclass.DocumentDigest()
	base := func() EnableRequest {
		return EnableRequest{
			SchemaDigest: "s", RunningBuild: "b", Operations: []string{op}, OperationKinds: map[string]string{op: OperationKindMCPClass},
			DocumentDigest: map[string]string{op: digest}, Mode: TargetModeCanary, RolloutPercentage: 100,
			RecordedBy: "r", ReviewEvidence: "e", PrincipalID: "p",
		}
	}
	if err := base().validate(); err != nil {
		t.Fatalf("a well-formed class request was refused: %v", err)
	}
	for name, mutate := range map[string]func(*EnableRequest){
		"primary": func(r *EnableRequest) { r.Mode = TargetModePrimary },
		"not allowlisted": func(r *EnableRequest) {
			bad := mcpclass.Operation("dataHealth")
			r.Operations = []string{bad}
			r.OperationKinds = map[string]string{bad: OperationKindMCPClass}
			r.DocumentDigest = map[string]string{bad: digest}
		},
		"wrong digest": func(r *EnableRequest) { r.DocumentDigest = map[string]string{op: "sha256:" + strings.Repeat("1", 64)} },
		"wrong kind":   func(r *EnableRequest) { r.OperationKinds = map[string]string{op: OperationKindQuery} },
		"mixed with a document": func(r *EnableRequest) {
			r.Operations = []string{op, "featureFlags"}
			r.OperationKinds["featureFlags"] = OperationKindQuery
			r.DocumentDigest["featureFlags"] = "sha256:" + strings.Repeat("2", 64)
		},
		"class kind on a document": func(r *EnableRequest) {
			r.Operations = []string{"featureFlags"}
			r.OperationKinds = map[string]string{"featureFlags": OperationKindMCPClass}
			r.DocumentDigest = map[string]string{"featureFlags": "sha256:" + strings.Repeat("2", 64)}
		},
	} {
		t.Run(name, func(t *testing.T) {
			r := base()
			mutate(&r)
			if err := r.validate(); err == nil {
				t.Fatal("the request was not refused")
			}
		})
	}
}

func TestClassifyMCPOutcome(t *testing.T) {
	refusedWith := func(candidate int, reason string, baseline int) Outcome {
		return Outcome{RefusalReason: RefusalNonSuccessStatus, Candidate: &Observation{StatusCode: candidate, Body: mcpRefusalBody(reason)}, Baseline: &Observation{StatusCode: baseline}}
	}
	refused := func(candidate, baseline int) Outcome { return refusedWith(candidate, "person_scope", baseline) }
	for name, tc := range map[string]struct {
		outcome Outcome
		state   string
	}{
		"executed":                  {Outcome{Executed: true}, "executed"},
		"listener policy 403":       {refused(http.StatusForbidden, 200), "excluded"},
		"listener limit 400":        {refusedWith(http.StatusBadRequest, "input_limit", 200), "excluded"},
		"listener budget 422":       {refusedWith(http.StatusUnprocessableEntity, "rows_ceiling", 200), "excluded"},
		"cost cap 400":              {refusedWith(http.StatusBadRequest, "complexity_limit", 200), "excluded"},
		"identity: org mismatch":    {refusedWith(http.StatusForbidden, "org_mismatch", 200), "failed"},
		"identity: invalid org arg": {refusedWith(http.StatusForbidden, "invalid_org_argument", 200), "failed"},
		"identity: elevated claim":  {refusedWith(http.StatusForbidden, "elevated_claim", 200), "failed"},
		"identity: no carrier":      {refusedWith(http.StatusUnauthorized, "no_carrier", 200), "failed"},
		"document invalid":          {refusedWith(http.StatusBadRequest, "invalid_document", 200), "failed"},
		"root not allowed":          {refusedWith(http.StatusForbidden, "root_field_not_allowed", 200), "failed"},
		"unknown reason":            {refusedWith(http.StatusForbidden, "something_new", 200), "failed"},
		"no reason in the body":     {Outcome{RefusalReason: RefusalNonSuccessStatus, Candidate: &Observation{StatusCode: 403, Body: []byte("forbidden")}, Baseline: &Observation{StatusCode: 200}}, "failed"},
		"empty body":                {Outcome{RefusalReason: RefusalNonSuccessStatus, Candidate: &Observation{StatusCode: 403}, Baseline: &Observation{StatusCode: 200}}, "failed"},
		"two errors":                {Outcome{RefusalReason: RefusalNonSuccessStatus, Candidate: &Observation{StatusCode: 403, Body: []byte(`{"errors":[{"extensions":{"reason":"person_scope"}},{"extensions":{"reason":"person_scope"}}]}`)}, Baseline: &Observation{StatusCode: 200}}, "failed"},
		"root not enabled 404":      {refused(http.StatusNotFound, 200), "failed"},
		"server error 500":          {refused(http.StatusInternalServerError, 200), "failed"},
		"python also refused":       {refused(http.StatusForbidden, 403), "failed"},
		"python server error":       {refused(http.StatusForbidden, 500), "failed"},
		"python redirected":         {refused(http.StatusForbidden, 302), "failed"},
		"python informational":      {refused(http.StatusForbidden, 199), "failed"},
		"known refusal":             {Outcome{KnownRefusal: &KnownRefusal{}}, "excluded"},
		"needs an instance id":      {Outcome{RefusalReason: RefusalNeedsInstanceID}, "excluded"},
		"known refusal tag, doc-route, needs an instance id": {Outcome{EdgeMode: EdgeModeDocRoute, KnownRefusal: &KnownRefusal{}, RefusalReason: RefusalNeedsInstanceID}, "excluded"},
		"known refusal tag, doc-route, any other refusal":    {Outcome{EdgeMode: EdgeModeDocRoute, KnownRefusal: &KnownRefusal{}, RefusalReason: RefusalWrongPlane}, "failed"},
		"admission refusal":        {Outcome{RefusalReason: RefusalWrongPlane}, "failed"},
		"non-success without legs": {Outcome{RefusalReason: RefusalNonSuccessStatus}, "failed"},
	} {
		state, reason := classifyMCPOutcome(tc.outcome)
		if state != tc.state {
			t.Errorf("%s: state = %s, want %s", name, state, tc.state)
		}
		// An exclusion names the listener's own reason, so the receipt says WHY.
		if state == "excluded" && tc.outcome.Candidate != nil && !strings.HasPrefix(reason, "listener_policy:") {
			t.Errorf("%s: exclusion reason %q does not carry the listener's reason", name, reason)
		}
	}
}

func sealedMatch(operation, variant string) sealedOutcome {
	return sealedOutcome{operation: operation, variant: variant, schemaDigest: "s", candidateBuild: "b", orgID: "org",
		route: RouteProof, edgeBinding: EdgeBuildPresent, terminalState: TerminalStateMatch, executed: true, admitted: true,
		baselineRef: "b-ref", candidateRef: "c-ref"}
}

func classRunner(sealed []sealedOutcome) *Runner {
	return &Runner{sealed: sealed, Config: Config{RecordedBy: "test", ReviewEvidence: "operator note"}}
}

func executedOutcome(operation, variant string) Outcome {
	return Outcome{Operation: operation, Variant: variant, Executed: true, Admitted: true, TerminalState: TerminalStateMatch}
}

func TestMCPClassReceiptsRule(t *testing.T) {
	sources := map[string][]string{mcpclass.Operation("hotspots"): {"hotspots", "hotspotsAll"}}
	refused403 := Outcome{Operation: "hotspotsAll", RefusalReason: RefusalNonSuccessStatus,
		Candidate: &Observation{StatusCode: 403, Body: mcpRefusalBody("person_scope")}, Baseline: &Observation{StatusCode: 200}}
	notFound := Outcome{Operation: "hotspotsAll", RefusalReason: RefusalNonSuccessStatus,
		Candidate: &Observation{StatusCode: 404}, Baseline: &Observation{StatusCode: 200}}
	mismatch := sealedMatch("hotspotsAll", "")
	mismatch.terminalState = TerminalStateMismatch
	mismatch.differencesOutsideBaselineDefect = 1
	identity403 := Outcome{Operation: "hotspotsAll", RefusalReason: RefusalNonSuccessStatus,
		Candidate: &Observation{StatusCode: 403, Body: mcpRefusalBody("org_mismatch")}, Baseline: &Observation{StatusCode: 200}}
	unbound := sealedMatch("hotspots", "")
	unbound.edgeBinding = EdgeBuildAbsent
	edgeRoute := sealedMatch("hotspots", "")
	edgeRoute.route = RouteEdge

	outside := sealedMatch("hotspots", "")
	outside.differencesOutsideBaselineDefect = 1
	notAdmitted := sealedMatch("hotspots", "")
	notAdmitted.admitted = false
	notExecuted := sealedMatch("hotspots", "")
	notExecuted.executed = false
	cited := sealedMatch("hotspots", "")
	cited.terminalState = EnablementCitedMismatchState
	type want struct {
		state    string
		receipts int
	}
	for name, tc := range map[string]struct {
		outcomes []Outcome
		sealed   []sealedOutcome
		want     want
	}{
		"every shape matched": {[]Outcome{executedOutcome("hotspots", ""), executedOutcome("hotspotsAll", "")},
			[]sealedOutcome{sealedMatch("hotspots", ""), sealedMatch("hotspotsAll", "")}, want{TerminalStateMatch, 1}},
		"one shape the listener refuses by policy is excluded and named": {[]Outcome{executedOutcome("hotspots", ""), refused403},
			[]sealedOutcome{sealedMatch("hotspots", ""), {}}, want{TerminalStateMatch, 1}},
		"an identity refusal (403 org_mismatch) while the reference served blocks the root": {[]Outcome{executedOutcome("hotspots", ""), identity403},
			[]sealedOutcome{sealedMatch("hotspots", ""), {}}, want{"proof_failed", 1}},
		"a root-not-enabled shape blocks the match": {[]Outcome{executedOutcome("hotspots", ""), notFound},
			[]sealedOutcome{sealedMatch("hotspots", ""), {}}, want{"proof_failed", 1}},
		"a mismatching shape blocks the match": {[]Outcome{executedOutcome("hotspots", ""), executedOutcome("hotspotsAll", "")},
			[]sealedOutcome{sealedMatch("hotspots", ""), mismatch}, want{TerminalStateMismatch, 1}},
		"nothing executed writes no receipt": {[]Outcome{refused403, notFound},
			[]sealedOutcome{{}, {}}, want{"", 0}},
		"an unbound response is not a match": {[]Outcome{executedOutcome("hotspots", "")},
			[]sealedOutcome{unbound}, want{"proof_failed", 1}},
		"a difference outside every declaration is not a match": {[]Outcome{executedOutcome("hotspots", "")},
			[]sealedOutcome{outside}, want{"proof_failed", 1}},
		"an unadmitted measurement is not a match": {[]Outcome{executedOutcome("hotspots", "")},
			[]sealedOutcome{notAdmitted}, want{"proof_failed", 1}},
		"an unexecuted sealed measurement is not a match": {[]Outcome{executedOutcome("hotspots", "")},
			[]sealedOutcome{notExecuted}, want{"proof_failed", 1}},
		"a cited mismatch is not admitted for the class": {[]Outcome{executedOutcome("hotspots", "")},
			[]sealedOutcome{cited}, want{TerminalStateMismatch, 1}},
		"a non-proof route is not a match": {[]Outcome{executedOutcome("hotspots", "")},
			[]sealedOutcome{edgeRoute}, want{"proof_failed", 1}},
	} {
		t.Run(name, func(t *testing.T) {
			receipts, verdicts, err := classRunner(tc.sealed).MCPClassReceipts(tc.outcomes, sources, nil, time.Now().UTC())
			if err != nil {
				t.Fatal(err)
			}
			if len(receipts) != tc.want.receipts || len(verdicts) != 1 || verdicts[0].TerminalState != tc.want.state {
				t.Fatalf("receipts=%d verdicts=%+v, want %d receipts and state %q", len(receipts), verdicts, tc.want.receipts, tc.want.state)
			}
			for _, r := range receipts {
				if r.SelectedOperation != mcpclass.Operation("hotspots") || r.DocumentDigest != mcpclass.DocumentDigest() ||
					r.MeasurementRoute != RouteProof || r.Stage != EnablementProofStage {
					t.Fatalf("receipt is not a class receipt: %+v", r)
				}
			}
		})
	}
}

func TestMCPClassReceiptsRefusesOutcomesTheRunDidNotSeal(t *testing.T) {
	sources := map[string][]string{mcpclass.Operation("hotspots"): {"hotspots"}}
	if _, _, err := classRunner(nil).MCPClassReceipts([]Outcome{executedOutcome("hotspots", "")}, sources, nil, time.Now()); err == nil {
		t.Fatal("outcomes without matching sealed measurements were accepted")
	}
	if _, _, err := classRunner([]sealedOutcome{sealedMatch("featureFlags", "")}).MCPClassReceipts([]Outcome{executedOutcome("featureFlags", "")}, sources, nil, time.Now()); err == nil {
		t.Fatal("an outcome of an operation outside every requested root was accepted")
	}
}

// A document operation given the class kind is refused as exactly that, not as
// whatever unknown-kind refusal happens to follow.
func TestEnableNamesAClassKindOnADocumentOperation(t *testing.T) {
	r := EnableRequest{
		SchemaDigest: "s", RunningBuild: "b", Operations: []string{"featureFlags"},
		OperationKinds: map[string]string{"featureFlags": OperationKindMCPClass},
		DocumentDigest: map[string]string{"featureFlags": "sha256:" + strings.Repeat("2", 64)},
		Mode:           TargetModeCanary, RolloutPercentage: 100, RecordedBy: "r", ReviewEvidence: "e", PrincipalID: "p",
	}
	if err := r.validate(); err == nil || !strings.Contains(err.Error(), "is not an MCP class operation") {
		t.Fatalf("err = %v, want the class-kind-on-a-document refusal", err)
	}
}

// mcpRefusalBody is the listener's typed refusal body (errors[0].extensions.reason).
func mcpRefusalBody(reason string) []byte {
	return []byte(`{"errors":[{"message":"refused","extensions":{"code":"MCP_REFUSED","reason":"` + reason + `"}}]}`)
}

// CHAOS-7500: the reason named on the receipt is the TRUE one. In the Python reference a known-refusal tag excludes the shape as such; in doc-route
// mode the tag does not apply and the shape that needs an instance id says so.
func TestClassifyMCPOutcomeNamesTheTrueReasonInDocRouteMode(t *testing.T) {
	tagged := Outcome{KnownRefusal: &KnownRefusal{}, RefusalReason: RefusalNeedsInstanceID}
	if state, reason := classifyMCPOutcome(tagged); state != "excluded" || reason != "known_refusal" {
		t.Fatalf("python reference: %s %s", state, reason)
	}
	tagged.EdgeMode = EdgeModeDocRoute
	if state, reason := classifyMCPOutcome(tagged); state != "excluded" || reason != "needs_instance_identifier" {
		t.Fatalf("doc-route: %s %s, want excluded needs_instance_identifier", state, reason)
	}
	tagged.RefusalReason = RefusalWrongPlane
	if state, reason := classifyMCPOutcome(tagged); state != "failed" || reason != RefusalWrongPlane {
		t.Fatalf("doc-route, other refusal: %s %s, want failed with the true reason", state, reason)
	}
}
