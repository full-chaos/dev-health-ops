package investment

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize/decision"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// stubOrgModel stands in for an org's own model. No test reaches a real BYO or
// LLM-style model: this stub, or jev, is all a test sees.
type stubOrgModel struct {
	model string
	// returned is the model the stub reports back; empty is model.
	returned string
	// returnedEmpty makes the stub report no model at all.
	returnedEmpty bool
	reply         func(request categorize.CompletionRequest) string
	calls         []categorize.CompletionRequest
	closed        int
	failure       error
}

func (stub *stubOrgModel) Complete(_ context.Context, request categorize.CompletionRequest) (categorize.CompletionResult, error) {
	stub.calls = append(stub.calls, request)
	if stub.failure != nil {
		return categorize.CompletionResult{}, stub.failure
	}
	in, out := 100, 20
	return categorize.CompletionResult{Text: stub.reply(request), InputTokens: &in, OutputTokens: &out, Model: stub.returnedModel()}, nil
}
func (stub *stubOrgModel) returnedModel() string {
	if stub.returnedEmpty {
		return ""
	}
	if stub.returned != "" {
		return stub.returned
	}
	return stub.model
}
func (stub *stubOrgModel) Close() error  { stub.closed++; return nil }
func (stub *stubOrgModel) Model() string { return stub.model }

// answerAllQuestions answers every question of the reply schema: level 2 for a
// score, the first option for a choice.
func answerAllQuestions(request categorize.CompletionRequest) string {
	answers := map[string]any{}
	properties := request.JSONSchema["properties"].(map[string]any)["answers"].(map[string]any)["properties"].(map[string]any)
	for id, raw := range properties {
		fields := raw.(map[string]any)["properties"].(map[string]any)
		if level, ok := fields["level"]; ok {
			levels := level.(map[string]any)["enum"].([]int)
			answers[id] = map[string]any{"level": levels[2], "confidence": 0.9}
			continue
		}
		options := fields["choice"].(map[string]any)["enum"].([]string)
		answers[id] = map[string]any{"choice": options[0], "confidence": 0.8}
	}
	encoded, _ := json.Marshal(map[string]any{"answers": answers})
	return string(encoded)
}

func jevRequestBody(t *testing.T) []byte {
	t.Helper()
	golden, err := os.ReadFile(filepath.Join("testdata", "systemone_request.golden"))
	if err != nil {
		t.Fatal(err)
	}
	return []byte(strings.SplitN(string(golden), "\n--\n", 2)[0])
}

// The org's model answers the SystemOne questions through the same completer
// and adapter as jev, and the rows carry a stamp and usage identity of their own.
func TestAnOrgModelAnswersTheSystemOneQuestionsWithItsOwnStampAndIdentity(t *testing.T) {
	stub := &stubOrgModel{model: "org-model-1", reply: answerAllQuestions}
	served, err := newBYOSystemOne(stub, "openai", testLogger())
	if err != nil {
		t.Fatal(err)
	}
	m := &Materializer{}
	m.SetServed(served)
	outcome, err := m.categorizeEntry(context.Background(), shadowTestConfig(), shadowEntry(0, "u1", shadowTestBundle(t, "u1", 6)))
	if err != nil {
		t.Fatalf("categorize: %v", err)
	}
	if len(stub.calls) != 1 {
		t.Fatalf("the org model was asked %d times, want 1", len(stub.calls))
	}
	if outcome.LLMCalls != 1 || outcome.LLMModel != "org-model-1" {
		t.Fatalf("outcome calls=%d model=%q", outcome.LLMCalls, outcome.LLMModel)
	}
	stamp := m.modelVersion(Config{})
	if !strings.Contains(stamp, "provider=byo-openai;") || !strings.Contains(stamp, "model=org-model-1;") || strings.Contains(stamp, "jev") {
		t.Fatalf("stamp = %q", stamp)
	}
	jevStamp := strings.SplitN(strings.SplitN(string(mustRead(t, "systemone_request.golden")), "stamp=", 2)[1], "\n", 2)[0]
	if stamp == jevStamp {
		t.Fatal("a BYO row can share a result-reuse key with a jev row")
	}
	if provider, model := m.usageIdentity(Config{}); provider != "byo-openai" || model != "org-model-1" {
		t.Fatalf("usage identity = %q/%q", provider, model)
	}
	if !strings.Contains(stub.calls[0].Prompt, "source_block") || stub.calls[0].JSONSchema == nil {
		t.Fatal("the question text or the reply schema did not reach the org model")
	}
	if err := served.Close(); err != nil || stub.closed != 1 {
		t.Fatalf("close: %v closed=%d", err, stub.closed)
	}
}

func mustRead(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// A reply that is not the expected JSON, a level out of range and a failed call
// each end the unit as a failed request or an invalid_llm_output row; none
// becomes a level.
func TestAnOrgModelReplyThatIsNotUsableIsNeverALevel(t *testing.T) {
	for name, reply := range map[string]func(categorize.CompletionRequest) string{
		"not json":   func(categorize.CompletionRequest) string { return "I cannot do that" },
		"no answers": func(categorize.CompletionRequest) string { return `{"answers":{}}` },
	} {
		t.Run(name, func(t *testing.T) {
			stub := &stubOrgModel{model: "org-model-1", reply: reply}
			served, err := newBYOSystemOne(stub, "openai", testLogger())
			if err != nil {
				t.Fatal(err)
			}
			m := &Materializer{}
			m.SetServed(served)
			outcome, err := m.categorizeEntry(context.Background(), shadowTestConfig(), shadowEntry(0, "u1", shadowTestBundle(t, "u1", 6)))
			if err == nil && outcome.Status != categorize.StatusInvalidLLMOutput {
				t.Fatalf("an unusable reply produced a usable outcome: %+v", outcome)
			}
		})
	}
	stub := &stubOrgModel{model: "org-model-1", failure: errors.New("provider said https://secret.example.invalid/v1 key=sk-test")}
	served, err := newBYOSystemOne(stub, "openai", testLogger())
	if err != nil {
		t.Fatal(err)
	}
	m := &Materializer{}
	m.SetServed(served)
	_, err = m.categorizeEntry(context.Background(), shadowTestConfig(), shadowEntry(0, "u1", shadowTestBundle(t, "u1", 6)))
	if err == nil || strings.Contains(err.Error(), "secret.example.invalid") || strings.Contains(err.Error(), "sk-test") {
		t.Fatalf("a failed org call must end the unit without provider text: %v", err)
	}
}

// byoStubGate builds the production orgCategorizeBYO over stubs only.
type byoStub struct {
	decision    llmorgsettings.Decision
	cfg         llmorgsettings.OrgConfig
	found       bool
	configErr   error
	providerErr error
	builds      int
	provider    *stubOrgModel
	guarded     int
}

func (stub *byoStub) build() *orgCategorizeBYO {
	return &orgCategorizeBYO{
		logger:  testLogger(),
		optedIn: func(context.Context, string) (bool, error) { return true, nil },
		gate:    func(context.Context, string) llmorgsettings.Decision { return stub.decision },
		config: func(context.Context, string) (llmorgsettings.OrgConfig, bool, error) {
			return stub.cfg, stub.found, stub.configErr
		},
		newProvider: func(categorize.ProviderKind, string, string, string) (categorize.Provider, error) {
			stub.builds++
			if stub.providerErr != nil {
				return nil, stub.providerErr
			}
			stub.provider = &stubOrgModel{model: stub.cfg.Model, reply: answerAllQuestions}
			return stub.provider, nil
		},
		guard: func(provider categorize.Provider, _ string, _ categorize.ProviderKind, _ string) categorize.Provider {
			stub.guarded++
			return provider
		},
	}
}

func usableOrgConfig() llmorgsettings.OrgConfig {
	return llmorgsettings.OrgConfig{Provider: "openai", Model: "org-model-1", APIKey: secrets.NewHidden("sk-org-test"), BaseURL: "https://models.example.invalid/v1"}
}

// D6107: an opted-in org with no usable own model is refused with a code of the
// closed set, and no provider is built, so nothing can reach the platform model.
func TestAnOptedInOrgWithoutAUsableOwnModelIsRefusedWithAClosedCode(t *testing.T) {
	for name, tc := range map[string]struct {
		stub *byoStub
		want string
	}{
		"gate closed":      {&byoStub{decision: llmorgsettings.Decision{Kind: llmorgsettings.Refuse, Code: llmorgsettings.CodeOwnModelGateClosed}}, "own_model_gate_closed"},
		"turned off":       {&byoStub{decision: llmorgsettings.Decision{Kind: llmorgsettings.Refuse, Code: llmorgsettings.CodeOwnModelTurnedOff}}, "own_model_turned_off"},
		"invalid base url": {&byoStub{decision: llmorgsettings.Decision{Kind: llmorgsettings.Refuse, Code: string(llmorgsettings.ConfigInvalidBaseURL)}}, "invalid_base_url"},
		"gate error text":  {&byoStub{decision: llmorgsettings.Decision{Kind: llmorgsettings.Refuse, Code: "dial tcp 10.0.0.1:5432: connection refused"}}, llmorgsettings.CodeCategorizeBYOUnavailable},
		"platform verdict": {&byoStub{decision: llmorgsettings.Decision{Kind: llmorgsettings.Platform}}, "not_configured"},
		"not found":        {&byoStub{decision: llmorgsettings.Decision{Kind: llmorgsettings.Own}}, "not_configured"},
		"config error": {&byoStub{decision: llmorgsettings.Decision{Kind: llmorgsettings.Own}, configErr: errors.New("row https://x.example.invalid failed")},
			"unreadable_row"},
		"no model": {&byoStub{decision: llmorgsettings.Decision{Kind: llmorgsettings.Own}, found: true,
			cfg: llmorgsettings.OrgConfig{Provider: "openai", APIKey: secrets.NewHidden("sk-org-test")}}, "model_required"},
		"mock provider": {&byoStub{decision: llmorgsettings.Decision{Kind: llmorgsettings.Own}, found: true,
			cfg: llmorgsettings.OrgConfig{Provider: "mock", Model: "m"}}, "unknown_provider"},
		"typesafe provider": {&byoStub{decision: llmorgsettings.Decision{Kind: llmorgsettings.Own}, found: true,
			cfg: llmorgsettings.OrgConfig{Provider: "typesafe", Model: "m"}}, "unknown_provider"},
		"provider build error": {&byoStub{decision: llmorgsettings.Decision{Kind: llmorgsettings.Own}, found: true, cfg: usableOrgConfig(),
			providerErr: errors.New("bad key sk-org-test at https://models.example.invalid/v1")}, llmorgsettings.CodeCategorizeBYOUnavailable},
	} {
		t.Run(name, func(t *testing.T) {
			if tc.stub.found || tc.stub.configErr != nil || tc.stub.cfg.Provider != "" {
				// as built
			}
			served, err := tc.stub.build().Build(context.Background(), "org-1")
			var refusal *BYORefusal
			if !errors.As(err, &refusal) || served != nil {
				t.Fatalf("Build = %v, %v; want a BYORefusal", served, err)
			}
			if refusal.Code != tc.want {
				t.Fatalf("code = %q, want %q", refusal.Code, tc.want)
			}
			for _, leak := range []string{"example.invalid", "sk-org-test", "dial tcp", "10.0.0.1"} {
				if strings.Contains(err.Error(), leak) {
					t.Fatalf("refusal %q carries %q", err.Error(), leak)
				}
			}
			if name != "provider build error" && tc.stub.builds != 0 {
				t.Fatalf("a provider was built for a refused org (%d)", tc.stub.builds)
			}
		})
	}
}

func TestAnOptedInOrgWithAUsableOwnModelGetsItsOwnBackend(t *testing.T) {
	stub := &byoStub{decision: llmorgsettings.Decision{Kind: llmorgsettings.Own}, found: true, cfg: usableOrgConfig()}
	served, err := stub.build().Build(context.Background(), "org-1")
	if err != nil || served == nil {
		t.Fatalf("Build = %v, %v", served, err)
	}
	defer func() { _ = served.Close() }()
	if stub.guarded != 1 {
		t.Fatalf("the org budget guard wrapped the provider %d times, want 1", stub.guarded)
	}
	if provider, model := served.Identity(); provider != "byo-openai" || model != "org-model-1" {
		t.Fatalf("identity = %q/%q", provider, model)
	}
}

// The closed set of refusal codes is pinned: a new code is a new decision.
func TestTheCategorizeRefusalCodeSetIsClosed(t *testing.T) {
	want := []string{
		"ai_features_turned_off", "categorize_byo_model_unreported", "categorize_byo_model_unsupported", "categorize_byo_optin_unreadable", "categorize_byo_unavailable", "invalid_base_url", "missing_credentials", "model_required",
		"not_configured", "own_model_gate_closed", "own_model_turned_off", "service_model_not_entitled",
		"unknown_provider", "unreadable_row",
	}
	got := llmorgsettings.CategorizeRefusalCodes()
	sort.Strings(got)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("closed set = %v, want %v", got, want)
	}
	for _, free := range []string{"", "Service unavailable", "https://x.example.invalid", "sk-test", "dial tcp: lookup"} {
		if code := llmorgsettings.SafeCategorizeRefusalCode(free); code != llmorgsettings.CodeCategorizeBYOUnavailable {
			t.Fatalf("code %q maps to %q, want the generic code", free, code)
		}
	}
}

type stubByo struct {
	optedIn    bool
	optedInErr error
	buildErr   error
	asked      int
	built      int
}

func (stub *stubByo) OptedIn(context.Context, string) (bool, error) {
	stub.asked++
	return stub.optedIn, stub.optedInErr
}
func (stub *stubByo) Build(context.Context, string) (*ServedDecision, error) {
	stub.built++
	return nil, stub.buildErr
}

func platformPathReached(reached *int) func(string, string) (categorize.Provider, categorize.ProviderKind, error) {
	return func(string, string) (categorize.Provider, categorize.ProviderKind, error) {
		*reached++
		return categorize.MockProvider{}, categorize.ProviderKindNone, nil
	}
}

func deterministicClass(err error) string {
	var deterministic *workgraph.DeterministicError
	if errors.As(err, &deterministic) {
		return deterministic.Class
	}
	return ""
}

// Off by default: an org that did not opt in, an explicit provider, an unscoped
// request and the operator kill-switch all stay on the platform path, and the
// org's model is never built.
func TestOnlyAnOptedInOrgAskingForAutoIsOfferedItsOwnModel(t *testing.T) {
	for name, tc := range map[string]struct {
		scope    string
		org      string
		env      string
		byo      *stubByo
		wantAsk  int
		wantPath int
	}{
		"not opted in":      {`{}`, "org-1", "", &stubByo{}, 1, 1},
		"explicit mock":     {`{"llm_provider":"mock"}`, "org-1", "", &stubByo{optedIn: true}, 0, 1},
		"explicit typesafe": {`{"llm_provider":"typesafe"}`, "org-1", "", &stubByo{optedIn: true}, 0, 1},
		"no org":            {`{"allow_unscoped":true}`, "", "", &stubByo{optedIn: true}, 0, 1},
		"kill switch":       {`{}`, "org-1", "mock", &stubByo{optedIn: true}, 0, 1},
	} {
		t.Run(name, func(t *testing.T) {
			if tc.env != "" {
				t.Setenv("LLM_PROVIDER", tc.env)
			}
			reached := 0
			executor := &NativeExecutor{reader: unusedReader(t), writer: unusedWriter(t), logger: testLogger(), now: stubNow,
				newProvider: platformPathReached(&reached), byo: tc.byo}
			claim := materializeClaim(t, tc.scope)
			claim.Request.OrganizationID = tc.org
			_, err := executor.Execute(context.Background(), claim)
			if deterministicClass(err) != workgraph.ClassLLMProviderInvalid {
				t.Fatalf("Execute = %v", err)
			}
			if tc.byo.asked != tc.wantAsk || tc.byo.built != 0 || reached != tc.wantPath {
				t.Fatalf("asked=%d built=%d platform path=%d", tc.byo.asked, tc.byo.built, reached)
			}
		})
	}
}

// Fail closed: an opted-in org that is refused never falls to jev, and an
// unreadable opt-in is not read as off.
func TestAnOptedInOrgThatIsRefusedIsNeverServedByTheCannedPlatformPath(t *testing.T) {
	refusal := &stubByo{optedIn: true, buildErr: &BYORefusal{Code: llmorgsettings.CodeOwnModelTurnedOff}}
	reached := 0
	executor := &NativeExecutor{reader: unusedReader(t), writer: unusedWriter(t), logger: testLogger(), now: stubNow,
		newProvider: platformPathReached(&reached), byo: refusal}
	_, err := executor.Execute(context.Background(), materializeClaim(t, `{}`))
	if deterministicClass(err) != workgraph.ClassLLMProviderInvalid || !strings.Contains(err.Error(), "own_model_turned_off") {
		t.Fatalf("Execute = %v", err)
	}
	if reached != 0 {
		t.Fatal("a refused org reached the platform provider path")
	}

	var reasons []string
	recordOptInUnreadable = func(_ context.Context, reason string) { reasons = append(reasons, reason) }
	t.Cleanup(func() { recordOptInUnreadable = defaultRecordOptInUnreadable })
	for cause, want := range map[error]string{
		errors.New("postgres: connection refused at 10.0.0.1"):                               "unavailable",
		fmt.Errorf("read: %w", &pgconn.PgError{Code: "42501", Message: "permission denied"}): "permission_denied",
		// A worker up before pgmigrate 0156 created the function: refused once,
		// deterministically, never retried (roll order: 0156, then the grants).
		fmt.Errorf("read: %w", &pgconn.PgError{Code: "42883", Message: "function does not exist"}): "function_missing",
	} {
		reasons = nil
		unreadable := &stubByo{optedInErr: cause}
		executor.byo = unreadable
		_, err = executor.Execute(context.Background(), materializeClaim(t, `{}`))
		if deterministicClass(err) != workgraph.ClassLLMProviderInvalid || !strings.Contains(err.Error(), llmorgsettings.CodeCategorizeBYOOptInUnreadable) ||
			reached != 0 || unreadable.built != 0 || strings.Contains(err.Error(), "10.0.0.1") || strings.Contains(err.Error(), "permission") || strings.Contains(err.Error(), "does not exist") {
			t.Fatalf("an unreadable opt-in must refuse the run (no retry, no jev, no text): %v reached=%d", err, reached)
		}
		if len(reasons) != 1 || reasons[0] != want {
			t.Fatalf("opt-in unreadable metric reasons = %v, want [%s]", reasons, want)
		}
	}

	other := &stubByo{optedIn: true, buildErr: errors.New("provider text https://x.example.invalid")}
	executor.byo = other
	_, err = executor.Execute(context.Background(), materializeClaim(t, `{}`))
	if deterministicClass(err) != workgraph.ClassLLMProviderInvalid || strings.Contains(err.Error(), "example.invalid") || reached != 0 {
		t.Fatalf("an unexpected build error must refuse with the generic code: %v", err)
	}
}

// D6279: BYO is never used in tests. No test file of this package builds the
// production factory or the org-credential provider.
func TestNoTestOfThisPackageBuildsTheProductionBYOFactory(t *testing.T) {
	forbidden := []string{"NewOrgCategorize" + "BYO(", "NewProviderFrom" + "Credentials("}
	files, err := filepath.Glob("*_test.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		for _, token := range forbidden {
			if strings.Contains(string(data), token) {
				t.Fatalf("%s calls %s: a test must use jev or a stub", file, token)
			}
		}
	}
}

// The jev request is untouched by the org-model path: the sender reads it
// without changing a byte.
func TestTheOrgModelSenderDoesNotChangeTheRequestItReads(t *testing.T) {
	body := jevRequestBody(t)
	before := string(body)
	stub := &stubOrgModel{model: "org-model-1", reply: answerAllQuestions}
	result, err := byoSender{provider: stub}.PostSystemOneDetailed(context.Background(), body)
	if err != nil || string(body) != before {
		t.Fatalf("err=%v changed=%v", err, string(body) != before)
	}
	if result.StatusCode != http.StatusOK || !json.Valid(result.Body) || !result.Usage.Reported {
		t.Fatalf("result = %+v", result)
	}
}

// An answer from a model other than the configured one (a gateway that routes
// to jev, say) is a failed request, never a row stamped with the org's model.
func TestAnAnswerFromAnotherModelIsNeverRecordedAsTheOrgModel(t *testing.T) {
	stub := &stubOrgModel{model: "org-model-1", returned: "jev-1.13.0", reply: answerAllQuestions}
	served, err := newBYOSystemOne(stub, "openai", testLogger())
	if err != nil {
		t.Fatal(err)
	}
	m := &Materializer{}
	m.SetServed(served)
	outcome, err := m.categorizeEntry(context.Background(), shadowTestConfig(), shadowEntry(0, "u1", shadowTestBundle(t, "u1", 6)))
	if err == nil {
		t.Fatalf("a mismatched model was accepted: %+v", outcome)
	}
	if outcome.LLMModel == "org-model-1" {
		t.Fatalf("the outcome carries the org model: %+v", outcome)
	}
	dated := &stubOrgModel{model: "org-model-1", returned: "org-model-1-20261001", reply: answerAllQuestions}
	servedDated, err := newBYOSystemOne(dated, "openai", testLogger())
	if err != nil {
		t.Fatal(err)
	}
	m.SetServed(servedDated)
	if _, err := m.categorizeEntry(context.Background(), shadowTestConfig(), shadowEntry(0, "u1", shadowTestBundle(t, "u1", 6))); err != nil {
		t.Fatalf("a dated suffix of the configured model must stay accepted: %v", err)
	}
}

// An answer that names no model cannot be tied to the configured model. It
// fails closed: the first unit is refused with the closed code, the failure is
// deterministic, and the run stops after that one billed request. A jev
// backend never takes this path.
func TestAnEmptyReturnedModelRefusesAndStopsTheRun(t *testing.T) {
	stub := &stubOrgModel{model: "org-model-1", returnedEmpty: true, reply: answerAllQuestions}
	served, err := newBYOSystemOne(stub, "openai", testLogger())
	if err != nil {
		t.Fatal(err)
	}
	m := &Materializer{logger: testLogger()}
	m.SetServed(served)
	outcomes := map[int]categorize.CategorizationOutcome{}
	stats := Stats{LLMFailureCounts: map[string]int{}}
	cfg := shadowTestConfig()
	cfg.LLMConcurrency = 1
	err = m.categorizePending(context.Background(), cfg, shadowTestEntries(t, "u1", "u2", "u3"), outcomes, &stats)
	if deterministicClass(err) != workgraph.ClassLLMDeterministic {
		t.Fatalf("categorizePending = %v, want the llm_deterministic class", err)
	}
	if !strings.Contains(err.Error(), llmorgsettings.CodeCategorizeBYOModelUnreported) {
		t.Fatalf("the refusal does not carry the closed code: %v", err)
	}
	for _, leak := range []string{"org-model-1", "example.invalid", "sk-"} {
		if strings.Contains(err.Error(), leak) {
			t.Fatalf("the refusal carries %q: %v", leak, err)
		}
	}
	if len(stub.calls) != 1 || len(outcomes) != 0 {
		t.Fatalf("the org model was asked %d times and %d units have an outcome; the run must stop after one request", len(stub.calls), len(outcomes))
	}
}

func TestOnlyAnOrgModelBackendRefusesAnEmptyModelWithTheCode(t *testing.T) {
	jev, err := newServedDecision(&recordingSender{}, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	if jev.byo {
		t.Fatal("a jev backend is marked as an org model backend")
	}
	own, err := newBYOSystemOne(&stubOrgModel{model: "m", reply: answerAllQuestions}, "openai", testLogger())
	if err != nil || !own.byo {
		t.Fatalf("an org model backend is not marked: %v", err)
	}
	empty := decision.Classification{Errors: []string{"request_failed:model_mismatch:"}}
	if jev.unreportedModel(empty) || !own.unreportedModel(empty) {
		t.Fatal("only the org model backend may take the empty-model refusal")
	}
	if own.unreportedModel(decision.Classification{Errors: []string{"request_failed:llm_error"}}) {
		t.Fatal("a failed request with no answer is not an unreported model")
	}
	if own.unreportedModel(decision.Classification{ModelReturned: "other", Errors: []string{"request_failed:model_mismatch:other"}}) {
		t.Fatal("another model's answer is a mismatch, not an unreported model")
	}
}
