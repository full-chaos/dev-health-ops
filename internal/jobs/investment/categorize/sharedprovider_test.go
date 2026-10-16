package categorize

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/full-chaos/dev-health-go/llm"

	"github.com/full-chaos/dev-health-ops/internal/llmbudget"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// wireModels serves a JSON answer and records the "model" of every request.
type wireModels struct {
	mu     sync.Mutex
	models []string
	server *httptest.Server
}

func newWireModels(t *testing.T) *wireModels {
	t.Helper()
	w := &wireModels{}
	w.server = httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		var body struct {
			Model string `json:"model"`
		}
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &body)
		w.mu.Lock()
		w.models = append(w.models, body.Model)
		w.mu.Unlock()
		_, _ = rw.Write([]byte(`{"output_text":"{\"ok\":true}","usage":{"input_tokens":3,"output_tokens":2,"input_tokens_details":{"cached_tokens":0}}}`))
	}))
	t.Cleanup(w.server.Close)
	return w
}

func (w *wireModels) sent() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.models...)
}

// The platform is configured to gpt-5-nano while the shared package defaults
// OpenAI to gpt-5-mini: the configured model must be the one on the wire.
func TestSharedOpenAIConfiguredNanoWinsOverThePackageDefault(t *testing.T) {
	if llm.DefaultModel(llm.KindOpenAI) == "gpt-5-nano" {
		t.Fatal("the package default is nano: this test no longer tells a configured model from the default")
	}

	t.Run("platform env LLM_MODEL", func(t *testing.T) {
		clearProviderEnv(t)
		wire := newWireModels(t)
		t.Setenv("OPENAI_API_KEY", "test-key-value")
		t.Setenv("LLM_BASE_URL", wire.server.URL)
		t.Setenv("LLM_MODEL", "gpt-5-nano")

		kind, err := ResolveProviderKind("auto")
		if err != nil || kind != ProviderKindOpenAI {
			t.Fatalf("kind = %q, err = %v", kind, err)
		}
		model := ResolveModelName(kind, "")
		provider, err := NewProviderFromEnvWithModel(kind, model)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := provider.(*SharedProvider); !ok {
			t.Fatalf("provider = %T, want the shared provider", provider)
		}
		if model != "gpt-5-nano" || provider.Model() != "gpt-5-nano" {
			t.Fatalf("resolved %q, Model() %q, want gpt-5-nano", model, provider.Model())
		}
		result, err := provider.Complete(context.Background(), CategorizationRequest("p"))
		if err != nil {
			t.Fatal(err)
		}
		if got := wire.sent(); len(got) != 1 || got[0] != "gpt-5-nano" || result.Model != "gpt-5-nano" {
			t.Fatalf("wire models %v, result model %q, want gpt-5-nano", got, result.Model)
		}
	})

	t.Run("org BYO model", func(t *testing.T) {
		clearProviderEnv(t)
		t.Setenv("LLM_MODEL", "platform-model-must-not-be-used")
		wire := newWireModels(t)
		provider, err := newProviderFromCredentialsWithTransport(
			ProviderKindOpenAI, "org-key-value", wire.server.URL, "gpt-5-nano", http.DefaultTransport)
		if err != nil {
			t.Fatal(err)
		}
		if provider.Model() != "gpt-5-nano" {
			t.Fatalf("Model() = %q, want gpt-5-nano", provider.Model())
		}
		guarded := WithBudget(provider, llmbudget.Guard{Store: &recordingStore{}, OrgID: "org", Provider: "openai", Model: provider.Model()})
		if _, err := guarded.Complete(context.Background(), InvestmentMixExplanationRequest("p")); err != nil {
			t.Fatal(err)
		}
		if got := wire.sent(); len(got) != 1 || got[0] != "gpt-5-nano" {
			t.Fatalf("wire models %v, want gpt-5-nano", got)
		}
	})
}

// statusOnce answers every request with status and body.
func statusOnce(t *testing.T, status int, body string) (*httptest.Server, *int) {
	t.Helper()
	calls := 0
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		calls++
		mu.Unlock()
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	return server, &calls
}

// Named behaviour changes of the shared classifier against the ops port: the
// status code alone now decides these classes, where the ops port needed
// matching provider text and otherwise answered a generic failure.
func TestSharedOpenAIStatusOnlyFailuresAreClassifiedByStatus(t *testing.T) {
	cases := []struct {
		status           int
		class            llm.ErrorClass
		failureClass     string
		deterministic    bool
		calls            int
		opsFailureClass  string
		opsDeterministic bool
	}{
		{http.StatusPaymentRequired, llm.ClassAuth, "auth", true, 1, "llm_error", false},
		{http.StatusForbidden, llm.ClassAuth, "auth", true, 1, "llm_error", false},
		{http.StatusNotFound, llm.ClassModelNotFound, "model_not_found", true, 1, "llm_error", false},
		{http.StatusRequestTimeout, llm.ClassTimeout, "llm_error", false, 2, "llm_error", false},
		{http.StatusRequestEntityTooLarge, llm.ClassInvalidRequest, "llm_error", false, 1, "llm_error", false},
	}
	for _, tc := range cases {
		t.Run(http.StatusText(tc.status), func(t *testing.T) {
			server, calls := statusOnce(t, tc.status, `{}`)
			shared, err := NewSharedProvider(SharedProviderConfig{Kind: ProviderKindOpenAI, APIKey: secrets.NewHidden("test-key-value"), BaseURL: server.URL, Model: "gpt-5-nano"})
			if err != nil {
				t.Fatal(err)
			}
			_, err = shared.Complete(context.Background(), CategorizationRequest("p"))
			if class, ok := llm.ClassOf(err); !ok || class != tc.class {
				t.Fatalf("class = %q (ok %v), want %q: %v", class, ok, tc.class, err)
			}
			if FailureClass(err) != tc.failureClass || IsDeterministicFailure(err) != tc.deterministic || *calls != tc.calls {
				t.Fatalf("FailureClass %q deterministic %v calls %d, want %q %v %d",
					FailureClass(err), IsDeterministicFailure(err), *calls, tc.failureClass, tc.deterministic, tc.calls)
			}

			oldServer, _ := statusOnce(t, tc.status, `{}`)
			old := NewOpenAIProvider(OpenAIProviderConfig{APIKey: secrets.NewHidden("test-key-value"), BaseURL: oldServer.URL, Model: "gpt-5-nano"})
			_, oldErr := old.Complete(context.Background(), CategorizationRequest("p"))
			if FailureClass(oldErr) != tc.opsFailureClass || IsDeterministicFailure(oldErr) != tc.opsDeterministic {
				t.Fatalf("ops port FailureClass %q deterministic %v, recorded as %q %v",
					FailureClass(oldErr), IsDeterministicFailure(oldErr), tc.opsFailureClass, tc.opsDeterministic)
			}
		})
	}
}

func TestSharedProviderErrorMapsToTheOpsClasses(t *testing.T) {
	for _, tc := range []struct {
		class        llm.ErrorClass
		failureClass string
		httpClass    LLMErrorClass
	}{
		{llm.ClassAuth, "auth", LLMErrorClassAuth},
		{llm.ClassRateLimit, "rate_limit", LLMErrorClassRateLimit},
		{llm.ClassServer, "server_error", LLMErrorClassServer},
		{llm.ClassContextLength, "context_length", LLMErrorClassOther},
		{llm.ClassOutput, "output_error", LLMErrorClassOther},
		{llm.ClassBatchTimeout, "batch_timeout", LLMErrorClassOther},
		{llm.ClassModelNotFound, "model_not_found", LLMErrorClassOther},
		{llm.ClassTimeout, "llm_error", LLMErrorClassOther},
		{llm.ClassTransport, "llm_error", LLMErrorClassOther},
		{llm.ClassInvalidRequest, "llm_error", LLMErrorClassOther},
		{llm.ClassGeneric, "llm_error", LLMErrorClassOther},
	} {
		err := llm.NewError(tc.class, llm.KindOpenAI, "gpt-5-nano", "A neutral provider failure.")
		if got := FailureClass(err); got != tc.failureClass {
			t.Errorf("%s: FailureClass = %q, want %q", tc.class, got, tc.failureClass)
		}
		got, isLLM := ClassifyLLMError(err)
		if !isLLM || got != tc.httpClass {
			t.Errorf("%s: ClassifyLLMError = %v/%v, want %v/true", tc.class, got, isLLM, tc.httpClass)
		}
		if want := tc.class == llm.ClassAuth || tc.class == llm.ClassModelNotFound; IsDeterministicFailure(err) != want {
			t.Errorf("%s: IsDeterministicFailure = %v, want %v", tc.class, !want, want)
		}
	}
	// A budget refusal is no provider failure.
	if _, isLLM := ClassifyLLMError(errors.New("unrelated")); isLLM {
		t.Error("an unrelated error classified as an LLM failure")
	}
}

func budgetedShared(t *testing.T, store llmbudget.Store, handler http.HandlerFunc) (Provider, *int) {
	t.Helper()
	calls := 0
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls++
		mu.Unlock()
		handler(w, r)
	}))
	t.Cleanup(server.Close)
	shared, err := NewSharedProvider(SharedProviderConfig{Kind: ProviderKindOpenAI, APIKey: secrets.NewHidden("test-key"), BaseURL: server.URL, Model: "gpt-5-mini"})
	if err != nil {
		t.Fatal(err)
	}
	// The guard prices the org's configured endpoint (the official one); the
	// test server only stands in for the transport.
	return WithBudget(shared, llmbudget.Guard{Store: store, OrgID: "org", Provider: "openai", Model: "gpt-5-mini"}), &calls
}

func TestBudgetedSharedProviderDoesNotCallTheProviderOverBudget(t *testing.T) {
	store := &recordingStore{admitErr: llmbudget.ErrBudgetExceeded}
	provider, calls := budgetedShared(t, store, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	if _, ok := provider.(*SharedProvider); !ok {
		t.Fatalf("guarded provider = %T, want the shared provider guarded per attempt", provider)
	}
	_, err := provider.Complete(context.Background(), CategorizationRequest("p"))
	if !errors.Is(err, llmbudget.ErrBudgetExceeded) || !llm.IsBudgetRefusal(err) {
		t.Fatalf("err = %v, want ErrBudgetExceeded as a budget refusal", err)
	}
	if *calls != 0 || len(store.admits) != 1 {
		t.Fatalf("calls = %d admits = %d, want 0 and 1 (a refusal is final, never retried)", *calls, len(store.admits))
	}
	if FailureClass(err) != "llm_error" || IsDeterministicFailure(err) {
		t.Fatalf("a budget refusal classified as provider failure %q", FailureClass(err))
	}
}

func TestBudgetedSharedProviderReservesAndSettlesEveryAttempt(t *testing.T) {
	store := &recordingStore{}
	var mu sync.Mutex
	attempt := 0
	var bodies []int
	provider, calls := budgetedShared(t, store, func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		mu.Lock()
		attempt++
		current := attempt
		bodies = append(bodies, len(raw))
		mu.Unlock()
		if current == 1 {
			_, _ = w.Write([]byte(`{"incomplete_details":{"reason":"max_output_tokens"},"usage":{"input_tokens":100,"output_tokens":50,"input_tokens_details":{"cached_tokens":0}}}`))
			return
		}
		_, _ = w.Write([]byte(`{"output_text":"{\"ok\": true}","usage":{"input_tokens":100,"output_tokens":50,"input_tokens_details":{"cached_tokens":0}}}`))
	})
	if _, err := provider.Complete(context.Background(), CategorizationRequest("p")); err != nil {
		t.Fatal(err)
	}
	if *calls != 2 || len(store.admits) != 2 || len(store.settles) != 2 {
		t.Fatalf("calls=%d admits=%d settles=%d, want 2 each (every billable attempt is reserved and settled)", *calls, len(store.admits), len(store.settles))
	}
	if store.admits[0].IdempotencyKey == store.admits[1].IdempotencyKey {
		t.Fatal("two attempts shared one idempotency key")
	}
	for index, admit := range store.admits {
		// The reservation covers the whole serialized request as its input
		// bound: cost = ceil((bytes*250000 + max_output*2000000)/1e6).
		maxOutput := []int64{2048, 4096}[index]
		want, err := llmbudget.CostMicroUSD("openai", "gpt-5-mini", "", int64(bodies[index]), maxOutput, 0)
		if err != nil || admit.MaxCost == nil || want == nil || *admit.MaxCost != *want {
			t.Fatalf("admit %d MaxCost = %v, want %v for %d body bytes and %d output tokens", index, admit.MaxCost, want, bodies[index], maxOutput)
		}
	}
	for _, s := range store.settles {
		if s.Outcome != llmbudget.OutcomeSucceeded || s.Cost == nil || *s.Cost != 125 {
			t.Fatalf("settle = %+v, want succeeded at 125", s)
		}
	}
}

type unbudgetedStore struct{ recordingStore }

func (s *unbudgetedStore) Admit(_ context.Context, r llmbudget.AdmitRequest) (llmbudget.Admission, error) {
	s.admits = append(s.admits, r)
	return llmbudget.Admission{Unbudgeted: true}, nil
}

func TestBudgetedSharedBatchIsAdmittedOnlyWithoutAConfiguredBudget(t *testing.T) {
	items := []BatchItem{{CustomID: "u1", Request: CategorizationRequest("p")}}

	configured := &recordingStore{}
	server := newBatchServer(t, "completed", "")
	shared := sharedBatchFor(t, server.server.URL, "gpt-5-mini", 0).(sharedBatchProvider).SharedProvider
	batch, ok := AsBatchProvider(WithBudget(shared, llmbudget.Guard{Store: configured, OrgID: "org", Provider: "openai", Model: "gpt-5-mini"}))
	if !ok {
		t.Fatal("the guarded shared provider lost its Batch API")
	}
	_, err := batch.SubmitBatch(context.Background(), items)
	var accounting *llmbudget.AccountingError
	if !errors.As(err, &accounting) || !llm.IsBudgetRefusal(err) {
		t.Fatalf("err = %v, want the unpriced-batch accounting refusal", err)
	}
	server.mu.Lock()
	uploaded := server.upload
	server.mu.Unlock()
	if uploaded != "" || len(configured.admits) != 1 || configured.admits[0].MaxCost != nil {
		t.Fatalf("upload %q admits %+v: a refused batch must send nothing and be admitted unpriced", uploaded, configured.admits)
	}

	open := &unbudgetedStore{}
	batch, _ = AsBatchProvider(WithBudget(shared, llmbudget.Guard{Store: open, OrgID: "org", Provider: "openai", Model: "gpt-5-mini"}))
	if _, err := batch.SubmitBatch(context.Background(), items); err != nil {
		t.Fatalf("an org with no budget: %v", err)
	}
	if len(open.admits) != 1 {
		t.Fatalf("admits = %d, want 1", len(open.admits))
	}
}

func TestSharedProviderSpanCarriesTheOpsRole(t *testing.T) {
	recorder := recordSpans(t)
	wire := newWireModels(t)
	shared, err := NewSharedProvider(SharedProviderConfig{Kind: ProviderKindOpenAI, APIKey: secrets.NewHidden("test-key-value"), BaseURL: wire.server.URL, Model: "gpt-5-nano"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := shared.Complete(WithLLMRole(context.Background(), LLMRoleServed), CategorizationRequest("p")); err != nil {
		t.Fatal(err)
	}
	spans := llmSpans(recorder)
	if len(spans) != 1 {
		t.Fatalf("spans = %d, want 1", len(spans))
	}
	attrs := spanAttrs(spans[0])
	if attrs[llmAttrRole].AsString() != string(LLMRoleServed) || attrs[llmAttrProvider].AsString() != "openai" || attrs[llmAttrModel].AsString() != "gpt-5-nano" {
		t.Fatalf("span attributes = %v", attrs)
	}
}
