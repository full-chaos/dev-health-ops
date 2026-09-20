package goapiproof

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const testBaselineErrorMessage = "Received ClickHouse exception, code: 386. DB::Exception: There is no supertype for types String, UUID (NO_COMMON_TYPE)"

func testDeclaredError(answer GoAnswerShape, uniform string) *DeclaredBaselineError {
	return aiOpportunityRepoScopeError(answer, uniform)
}

func aiRecommendation(repoID string) map[string]any {
	return map[string]any{"opportunityId": "o-" + repoID, "repoId": repoID}
}

func aiAnswer(recommendations ...any) map[string]any {
	if recommendations == nil {
		recommendations = []any{}
	}
	return map[string]any{"aiOpportunities": map[string]any{"recommendations": recommendations, "detectorReady": true}}
}

func decodeForTest(t *testing.T, body string) Snapshot {
	t.Helper()
	snapshot, err := DecodeSnapshot([]byte(body))
	if err != nil {
		t.Fatalf("decode %s: %v", body, err)
	}
	return snapshot
}

type baselineErrorBaselineCell struct {
	name         string
	body         string
	declaredOnly bool
}

func baselineErrorBaselines(t *testing.T) []baselineErrorBaselineCell {
	t.Helper()
	root := "aiOpportunities"
	declared := deletionErrorJSON(root, testBaselineErrorMessage, nil)
	return []baselineErrorBaselineCell{
		{"data answers", envelope(aiAnswer(aiRecommendation("r1"))), false},
		{"declared error, root null", envelope(map[string]any{root: nil}, declared), true},
		{"declared error, data null", envelope(nil, declared), true},
		{"other message", envelope(map[string]any{root: nil}, deletionErrorJSON(root, "boom", nil)), false},
		{"two errors", envelope(map[string]any{root: nil}, declared, declared), false},
		{"wrong path", envelope(map[string]any{root: nil}, deletionErrorJSON(root, testBaselineErrorMessage, map[string]any{"path": []any{"other"}})), false},
		{"path deeper", envelope(map[string]any{root: nil}, deletionErrorJSON(root, testBaselineErrorMessage, map[string]any{"path": []any{root, "recommendations"}})), false},
		{"error with data alongside", envelope(aiAnswer(aiRecommendation("r1")), declared), false},
		{"extra data key", envelope(map[string]any{root: nil, "other": 1}, declared), false},
		{"error without a path", envelope(map[string]any{root: nil}, deletionErrorJSON(root, testBaselineErrorMessage, map[string]any{"path": nil})), false},
	}
}

func baselineErrorCandidates() []struct {
	name string
	body string
	ok   bool
} {
	return []struct {
		name string
		body string
		ok   bool
	}{
		{"object with a list", envelope(aiAnswer(aiRecommendation("r1"))), true},
		{"object with an empty list", envelope(aiAnswer()), true},
		{"root null", envelope(map[string]any{"aiOpportunities": nil}), false},
		{"root absent", envelope(map[string]any{}), false},
		{"carries an error", envelope(aiAnswer(aiRecommendation("r1")), deletionErrorJSON("aiOpportunities", "boom", nil)), false},
	}
}

// Every combination of (declaration present or absent) x (baseline outcome
// class) x (candidate outcome class) through Admit. The declaration changes
// the verdict only for a baseline that is exactly the declared failure: that
// pair is admitted on the candidate alone; a baseline that answers cleanly
// against a declaration is refused as stale; every other baseline error is
// refused as an errored response, with or without a declaration.
func TestDeclaredBaselineErrorEnumerationThroughAdmit(t *testing.T) {
	baselines := baselineErrorBaselines(t)
	candidates := baselineErrorCandidates()
	cells, admittedUnderClass := 0, 0
	for _, declared := range []bool{false, true} {
		for _, baseline := range baselines {
			for _, candidate := range candidates {
				cells++
				name := fmt.Sprintf("declared=%v/%s/%s", declared, baseline.name, candidate.name)
				input := AdmissionInput{
					Route: RouteEdge, NamedBuild: "b", ResponseRoot: "aiOpportunities", Operation: "aiOpportunities",
					Candidate:     Observation{StatusCode: 200, Plane: "go", Build: "b"},
					Baseline:      Observation{StatusCode: 200, Plane: "python"},
					CandidateSnap: decodeForTest(t, candidate.body), BaselineSnap: decodeForTest(t, baseline.body),
				}
				if declared {
					input.DeclaredBaselineError = testDeclaredError(GoAnswerNonEmpty, "repoId")
				}
				got := Admit(input)
				baselineErrored := !strings.HasPrefix(baseline.name, "data answers")
				var wantAdmitted, wantClass bool
				var wantReason string
				switch {
				case !baselineErrored && declared:
					wantReason = RefusalStaleBaselineErrorDeclaration
				case !baselineErrored:
					wantAdmitted = candidate.ok
					if !candidate.ok {
						wantReason = wantCandidateRefusal(candidate.name)
					}
				case declared && baseline.declaredOnly:
					wantAdmitted, wantClass = candidate.ok, candidate.ok
					if !candidate.ok {
						wantReason = wantCandidateRefusal(candidate.name)
					}
				default:
					wantReason = RefusalErroredResponse
				}
				if got.Admitted != wantAdmitted || got.DeclaredBaselineError != wantClass || (!wantAdmitted && got.Reason != wantReason) {
					t.Fatalf("%s: admitted=%v class=%v reason=%s (%s), want admitted=%v class=%v reason=%s",
						name, got.Admitted, got.DeclaredBaselineError, got.Reason, got.Detail, wantAdmitted, wantClass, wantReason)
				}
				if wantClass {
					admittedUnderClass++
					// What the class admits, the same input without the
					// declaration refuses.
					input.DeclaredBaselineError = nil
					if without := Admit(input); without.Admitted || without.Reason != RefusalErroredResponse {
						t.Fatalf("%s: the class admitted what a declaration-less Admit also admits (%v/%s)", name, without.Admitted, without.Reason)
					}
				}
				if got.GoOnly {
					t.Fatalf("%s: the declared class is never the go-only class", name)
				}
			}
		}
	}
	if want := 2 * len(baselines) * len(candidates); cells != want {
		t.Fatalf("enumerated %d cells, want %d", cells, want)
	}
	// Two baseline shapes are exactly the declared failure; the candidate is
	// an object with a list or an object with an empty list.
	if want := 2 * 2; admittedUnderClass != want {
		t.Fatalf("the class admitted %d cells, want %d", admittedUnderClass, want)
	}
}

func wantCandidateRefusal(candidate string) string {
	if candidate == "carries an error" {
		return RefusalErroredResponse
	}
	return RefusalEmptyResponseRoot
}

// A go-served operation keeps its own class: a declaration on it is refused
// by name rather than merged with the deletion rule.
func TestDeclaredBaselineErrorOnAGoServedOperationIsRefused(t *testing.T) {
	ledger := defaultLedgerForTest(t)
	operation := ledger.Entries[0].Operation
	message := ledger.ExpectedMessage(operation)
	got := Admit(AdmissionInput{
		Route: RouteEdge, NamedBuild: "b", ResponseRoot: operation, RootNullable: true, Operation: operation, GoServed: ledger,
		DeclaredBaselineError: testDeclaredError(GoAnswerNonEmpty, ""),
		Candidate:             Observation{StatusCode: 200, Plane: "go", Build: "b"},
		Baseline:              Observation{StatusCode: 200, Plane: "python"},
		CandidateSnap:         decodeForTest(t, envelope(map[string]any{operation: map[string]any{"a": 1}})),
		BaselineSnap:          decodeForTest(t, envelope(nil, deletionErrorJSON(operation, message, nil))),
	})
	if got.Admitted || got.Reason != RefusalInvalidDeclaredBaselineError {
		t.Fatalf("admitted=%v reason=%s", got.Admitted, got.Reason)
	}
}

// The Go leg's declared shape, over the whole alphabet of answers.
func TestDeclaredBaselineErrorGoLegShapes(t *testing.T) {
	const a, b = "11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222"
	echoA := []ScopeEcho{{List: "data.aiOpportunities.recommendations", Fields: []string{"repoId"}, Value: a}}
	cases := []struct {
		name     string
		decl     *DeclaredBaselineError
		body     string
		echo     []ScopeEcho
		wantMiss string
	}{
		{"empty answer, empty declared", testDeclaredError(GoAnswerEmpty, ""), envelope(aiAnswer()), nil, ""},
		{"rows answer, empty declared", testDeclaredError(GoAnswerEmpty, ""), envelope(aiAnswer(aiRecommendation(a))), nil, "declared answer is empty"},
		{"list absent", testDeclaredError(GoAnswerEmpty, ""), envelope(map[string]any{"aiOpportunities": map[string]any{"detectorReady": true}}), nil, "answered no list"},
		{"list null", testDeclaredError(GoAnswerEmpty, ""), envelope(map[string]any{"aiOpportunities": map[string]any{"recommendations": nil}}), nil, "answered no list"},
		{"empty answer, non-empty declared", testDeclaredError(GoAnswerNonEmpty, "repoId"), envelope(aiAnswer()), nil, "nothing shows the scope was applied"},
		{"one scope, id echoed", testDeclaredError(GoAnswerNonEmpty, "repoId"), envelope(aiAnswer(aiRecommendation(a), aiRecommendation(a))), echoA, ""},
		{"one scope, id echoed in another case", testDeclaredError(GoAnswerNonEmpty, "repoId"), envelope(aiAnswer(aiRecommendation(strings.ToUpper(a)))), echoA, ""},
		{"scope answered for another id", testDeclaredError(GoAnswerNonEmpty, "repoId"), envelope(aiAnswer(aiRecommendation(b))), echoA, "does not carry the requested scope"},
		{"org-wide answer for a repo scope", testDeclaredError(GoAnswerNonEmpty, "repoId"), envelope(aiAnswer(aiRecommendation(a), aiRecommendation(b))), echoA, "does not carry the requested scope"},
		{"two repos, no echo", testDeclaredError(GoAnswerNonEmpty, "repoId"), envelope(aiAnswer(aiRecommendation(a), aiRecommendation(b))), nil, "not for one scope"},
		{"one repo, no echo", testDeclaredError(GoAnswerNonEmpty, "repoId"), envelope(aiAnswer(aiRecommendation(a))), nil, ""},
		{"uniform field missing", testDeclaredError(GoAnswerNonEmpty, "repoId"), envelope(aiAnswer(map[string]any{"opportunityId": "o"})), nil, "carries no repoId"},
		{"uniform field empty", testDeclaredError(GoAnswerNonEmpty, "repoId"), envelope(aiAnswer(aiRecommendation(""))), nil, "carries no repoId"},
		{"list of the wrong container type", testDeclaredError(GoAnswerEmpty, ""), envelope(map[string]any{"aiOpportunities": map[string]any{"recommendations": map[string]any{}}}), nil, "answered no list"},
	}
	for _, c := range cases {
		got := c.decl.goLegUnmet(decodeForTest(t, c.body), c.echo)
		if (c.wantMiss == "") != (got == "") || (c.wantMiss != "" && !strings.Contains(got, c.wantMiss)) {
			t.Errorf("%s: unmet=%q, want it to contain %q", c.name, got, c.wantMiss)
		}
	}
}

// Every way a declaration can be malformed is refused before a request is
// sent, and the corpus's own declarations validate.
func TestDeclaredBaselineErrorValidation(t *testing.T) {
	good := func() *DeclaredBaselineError { return testDeclaredError(GoAnswerNonEmpty, "repoId") }
	cases := []struct {
		name   string
		mutate func(d *DeclaredBaselineError)
		echo   []ScopeEcho
	}{
		{"no ticket", func(d *DeclaredBaselineError) { d.Ticket = "" }, nil},
		{"ticket not an id", func(d *DeclaredBaselineError) { d.Ticket = "later" }, nil},
		{"no reason", func(d *DeclaredBaselineError) { d.Reason = "  " }, nil},
		{"no token", func(d *DeclaredBaselineError) { d.MessageToken = "" }, nil},
		{"token with a space", func(d *DeclaredBaselineError) { d.MessageToken = "NO COMMON" }, nil},
		{"list without data prefix", func(d *DeclaredBaselineError) { d.List = "aiOpportunities.recommendations" }, nil},
		{"unknown answer", func(d *DeclaredBaselineError) { d.Answer = "some" }, nil},
		{"uniform field on an empty answer", func(d *DeclaredBaselineError) { d.Answer = GoAnswerEmpty }, nil},
		{"scope echo on an empty answer", func(d *DeclaredBaselineError) { d.Answer, d.UniformField = GoAnswerEmpty, "" }, []ScopeEcho{{List: "data.x", Fields: []string{"y"}, Value: "z"}}},
	}
	if err := validateDeclaredBaselineError(good(), nil); err != nil {
		t.Fatalf("the corpus declaration is refused: %v", err)
	}
	if err := validateDeclaredBaselineError(nil, nil); err != nil {
		t.Fatalf("an absent declaration is refused: %v", err)
	}
	for _, c := range cases {
		d := good()
		c.mutate(d)
		if err := validateDeclaredBaselineError(d, c.echo); err == nil {
			t.Errorf("%s: accepted", c.name)
		}
	}
}

// One request through the runner: the Python leg answers the declared failure,
// the Go leg answers the list. The request is measured on the Go leg alone,
// counted as proven under the class, and is not a match, so the enablement
// predicate does not read it.
func TestRunMeasuresADeclaredBaselineErrorOnTheCandidateAlone(t *testing.T) {
	const repo = "9f5c2e6a-1111-2222-3333-444455556666"
	cases := []struct {
		name        string
		variantName string
		goBody      string
		python      string
		instance    map[string]string
		wantProven  bool
		wantReason  string
	}{
		{"unknown id answers empty", "REPO_UNKNOWN", envelope(aiAnswer()), envelope(map[string]any{"aiOpportunities": nil}, deletionErrorJSON("aiOpportunities", testBaselineErrorMessage, nil)), nil, true, ""},
		{"unknown id answers rows", "REPO_UNKNOWN", envelope(aiAnswer(aiRecommendation(repo))), envelope(map[string]any{"aiOpportunities": nil}, deletionErrorJSON("aiOpportunities", testBaselineErrorMessage, nil)), nil, false, RefusalDeclaredBaselineErrorGoLeg},
		{"valid id answers its rows", "REPO_VALID", envelope(aiAnswer(aiRecommendation(repo))), envelope(map[string]any{"aiOpportunities": nil}, deletionErrorJSON("aiOpportunities", testBaselineErrorMessage, nil)), map[string]string{"aiOpportunities.REPO_VALID": repo}, true, ""},
		{"valid id answers empty", "REPO_VALID", envelope(aiAnswer()), envelope(map[string]any{"aiOpportunities": nil}, deletionErrorJSON("aiOpportunities", testBaselineErrorMessage, nil)), map[string]string{"aiOpportunities.REPO_VALID": repo}, false, RefusalDeclaredBaselineErrorGoLeg},
		{"valid id answers another repo", "REPO_VALID", envelope(aiAnswer(aiRecommendation("33333333-3333-3333-3333-333333333333"))), envelope(map[string]any{"aiOpportunities": nil}, deletionErrorJSON("aiOpportunities", testBaselineErrorMessage, nil)), map[string]string{"aiOpportunities.REPO_VALID": repo}, false, RefusalDeclaredBaselineErrorGoLeg},
		{"python fixed", "REPO_UNKNOWN", envelope(aiAnswer()), envelope(aiAnswer()), nil, false, RefusalStaleBaselineErrorDeclaration},
		{"python fails another way", "REPO_UNKNOWN", envelope(aiAnswer()), envelope(map[string]any{"aiOpportunities": nil}, deletionErrorJSON("aiOpportunities", "connection reset", nil)), nil, false, RefusalErroredResponse},
	}
	spec, err := SpecFor("aiOpportunities")
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			raw, _ := io.ReadAll(r.Body)
			var parsed struct {
				Query string `json:"query"`
			}
			_ = json.Unmarshal(raw, &parsed)
			w.Header().Set(buildHeader, "b18e56fa79cfe20ce0f75df148144b832d92be36")
			body := c.goBody
			if strings.Contains(parsed.Query, "python-plane control") {
				w.Header().Set(planeHeader, "python")
				body = c.python
			} else {
				w.Header().Set(planeHeader, "go")
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(body))
		}))
		store, err := NewArtifactStore(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		runner := &Runner{
			Client:    NewLegClient(0),
			Documents: map[string]string{"aiOpportunities": "query AiOpportunities($orgId: String!, $scope: AIScopeInput = null, $limit: Int = 25) { aiOpportunities(orgId: $orgId, scope: $scope, limit: $limit) { detectorReady } }"},
			Registry: RegistryView{SchemaDigest: "sha256:29d509cd", BuildIdentity: "b18e56fa79cfe20ce0f75df148144b832d92be36",
				DocumentDigest: map[string]string{"aiOpportunities": "06ca28a0"}},
			Routing:   map[string]RoutingRow{"aiOpportunities": {Mode: "canary", CandidateBuild: "b18e56fa79cfe20ce0f75df148144b832d92be36"}},
			Artifacts: store,
			Config: Config{OrgID: "70d529e0", Window: DefaultWindow(), PythonEdgeURL: server.URL,
				Auth:            AuthContext{PrincipalKind: "stored_account", Audience: "query-api", KeyID: "local-dev-20260906"},
				EdgeCredential:  StaticCredential("Authorization", "edge access token", "Bearer edge"),
				ProofCredential: StaticCredential("Authorization", "envelope", "Bearer envelope"),
				InstanceIDs:     c.instance},
		}
		var variant Variant
		for _, v := range spec.Variants {
			if v.Name == c.variantName {
				variant = v
			}
		}
		if variant.Name == "" {
			t.Fatalf("%s: no variant %s", c.name, c.variantName)
		}
		outcome := runner.proveVariant(context.Background(), "aiOpportunities", variant)
		server.Close()
		if c.wantProven {
			if !outcome.Executed || outcome.ProvenUnder != ProvenUnderDeclaredBaselineError || outcome.TerminalState != TerminalStateUnsupported {
				t.Errorf("%s: executed=%v provenUnder=%q terminal=%s refusal=%s (%s)", c.name, outcome.Executed, outcome.ProvenUnder, outcome.TerminalState, outcome.RefusalReason, outcome.RefusalDetail)
				continue
			}
			if outcome.DeclaredBaselineError == nil || outcome.DeclaredBaselineError.Ticket != "CHAOS-6147" || len(outcome.BaselineDefects) != 0 {
				t.Errorf("%s: record %+v citations %v", c.name, outcome.DeclaredBaselineError, outcome.BaselineDefects)
			}
			if outcome.TerminalState == EnablementProofTerminalState || outcome.TerminalState == EnablementCitedMismatchState {
				t.Errorf("%s: the class produced an enablement-eligible verdict %s", c.name, outcome.TerminalState)
			}
			var s Summary
			s.countProofState("aiOpportunities", outcome)
			if v := s.ByOperation["aiOpportunities"]; v.Proven != 1 || v.NotProving != 0 {
				t.Errorf("%s: verdict %+v", c.name, v)
			}
			continue
		}
		if outcome.Executed || outcome.RefusalReason != c.wantReason {
			t.Errorf("%s: executed=%v refusal=%s (%s), want %s", c.name, outcome.Executed, outcome.RefusalReason, outcome.RefusalDetail, c.wantReason)
		}
		var s Summary
		s.countProofState("aiOpportunities", outcome)
		if v := s.ByOperation["aiOpportunities"]; v.Proven != 0 {
			t.Errorf("%s: a refused case counts as proven: %+v", c.name, v)
		}
	}
}

// Exactly the three repository-scope cases carry the declaration, with the
// answer each must have on the Go leg.
func TestAIOpportunitiesRepoScopeCasesDeclareTheBaselineError(t *testing.T) {
	spec, ok := operationSpecs["aiOpportunities"]
	if !ok {
		t.Fatal("aiOpportunities has no corpus entry")
	}
	if spec.Parity.DeclaredBaselineError != nil {
		t.Fatal("the base request declares a baseline error: its baseline answers")
	}
	want := map[string]GoAnswerShape{"REPO_UNKNOWN": GoAnswerEmpty, "REPO_VALID": GoAnswerNonEmpty, "REPO_NAME_VALID": GoAnswerNonEmpty}
	got := map[string]GoAnswerShape{}
	for _, v := range spec.Variants {
		if v.KnownRefusal != nil && v.Parity.DeclaredBaselineError != nil {
			t.Errorf("%s is both a known refusal and a declared baseline error", v.Name)
		}
		d := v.Parity.DeclaredBaselineError
		if d == nil {
			continue
		}
		got[v.Name] = d.Answer
		if err := validateDeclaredBaselineError(d, v.Parity.ScopeEcho); err != nil {
			t.Errorf("%s: %v", v.Name, err)
		}
		if !strings.Contains(d.Reason, "NO_COMMON_TYPE") {
			t.Errorf("%s: reason does not state the failure: %q", v.Name, d.Reason)
		}
	}
	if len(got) != len(want) {
		t.Fatalf("declared cases %v, want %v", got, want)
	}
	for name, answer := range want {
		if got[name] != answer {
			t.Errorf("%s declares answer %q, want %q", name, got[name], answer)
		}
	}
	for _, name := range []string{"REPO_VALID"} {
		for _, v := range spec.Variants {
			if v.Name == name && len(v.Instance.Echo("x")) == 0 {
				t.Errorf("%s does not echo the repository id it was given", name)
			}
		}
	}
}

// An outcome counts as proof under the class only when it was executed, was
// measured under the class, is the `unsupported` verdict and has nothing
// outside it; a bare `unsupported` outcome never proves anything.
func TestOutcomeProvesUnderTheDeclaredBaselineErrorClass(t *testing.T) {
	cases := []struct {
		name string
		o    Outcome
		want bool
	}{
		{"executed under the class", Outcome{Executed: true, TerminalState: TerminalStateUnsupported, ProvenUnder: ProvenUnderDeclaredBaselineError}, true},
		{"not executed", Outcome{TerminalState: TerminalStateUnsupported, ProvenUnder: ProvenUnderDeclaredBaselineError}, false},
		{"a difference outside", Outcome{Executed: true, TerminalState: TerminalStateUnsupported, ProvenUnder: ProvenUnderDeclaredBaselineError, DifferencesOutsideBaselineDefect: 1}, false},
		{"turned into a mismatch", Outcome{Executed: true, TerminalState: TerminalStateMismatch, ProvenUnder: ProvenUnderDeclaredBaselineError}, false},
		{"unsupported outside the class", Outcome{Executed: true, TerminalState: TerminalStateUnsupported}, false},
	}
	for _, c := range cases {
		if got := outcomeProves(c.o); got != c.want {
			t.Errorf("%s: outcomeProves = %v, want %v", c.name, got, c.want)
		}
	}
}
