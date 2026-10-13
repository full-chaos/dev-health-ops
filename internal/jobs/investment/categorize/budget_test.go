package categorize

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/llmbudget"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

type recordingStore struct {
	admitErr error
	admits   []llmbudget.AdmitRequest
	settles  []llmbudget.SettleRequest
}

func (s *recordingStore) Admit(_ context.Context, r llmbudget.AdmitRequest) (llmbudget.Admission, error) {
	s.admits = append(s.admits, r)
	if s.admitErr != nil {
		return llmbudget.Admission{}, s.admitErr
	}
	return llmbudget.Admission{ID: "r", BudgetConfigured: true}, nil
}

func (s *recordingStore) Settle(_ context.Context, r llmbudget.SettleRequest) error {
	s.settles = append(s.settles, r)
	return nil
}

func budgetedOpenAI(t *testing.T, store llmbudget.Store, handler http.HandlerFunc) (Provider, *int) {
	t.Helper()
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; handler(w, r) }))
	t.Cleanup(server.Close)
	provider := NewOpenAIProvider(OpenAIProviderConfig{APIKey: secrets.NewHidden("test-key"), BaseURL: server.URL, Model: "gpt-5-mini"})
	// The guard prices the org's configured endpoint (the official one); the
	// test server only stands in for the transport.
	return WithBudget(provider, llmbudget.Guard{Store: store, OrgID: "org", Provider: "openai", Model: "gpt-5-mini"}), &calls
}

func TestBudgetedOpenAIProviderDoesNotCallTheProviderOverBudget(t *testing.T) {
	store := &recordingStore{admitErr: llmbudget.ErrBudgetExceeded}
	provider, calls := budgetedOpenAI(t, store, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	_, err := provider.Complete(context.Background(), CategorizationRequest("p"))
	if !errors.Is(err, llmbudget.ErrBudgetExceeded) {
		t.Fatalf("err = %v, want ErrBudgetExceeded", err)
	}
	if *calls != 0 {
		t.Fatalf("the provider received %d requests, want 0", *calls)
	}
	if len(store.admits) != 1 {
		t.Fatalf("admission was asked %d times, want 1 (a refusal is final, never retried)", len(store.admits))
	}
}

func TestBudgetedOpenAIProviderReservesAndSettlesEveryAttempt(t *testing.T) {
	store := &recordingStore{}
	attempt := 0
	provider, calls := budgetedOpenAI(t, store, func(w http.ResponseWriter, _ *http.Request) {
		attempt++
		in, out, cached := 100, 50, 0
		response := openAIResponsesResponse{Usage: &openAIUsage{InputTokens: &in, OutputTokens: &out, InputTokensDetails: &openAITokenDetails{CachedTokens: &cached}}}
		if attempt == 1 {
			// An empty, truncated answer: the provider retries with more tokens.
			response.IncompleteDetails = &openAIIncompleteDetails{Reason: "max_output_tokens"}
		} else {
			response.OutputText = `{"ok": true}`
		}
		_ = json.NewEncoder(w).Encode(response)
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
	if store.admits[1].MaxCost == nil || store.admits[0].MaxCost == nil || *store.admits[1].MaxCost <= *store.admits[0].MaxCost {
		t.Fatalf("the retry (doubled max_output_tokens) must reserve more: %v then %v", store.admits[0].MaxCost, store.admits[1].MaxCost)
	}
	for _, s := range store.settles {
		if s.Outcome != llmbudget.OutcomeSucceeded || s.Cost == nil || *s.Cost != 125 {
			// ceil((100*250000 + 50*2000000)/1e6) = ceil(125) = 125
			t.Fatalf("settle = %+v, want succeeded at 125", s)
		}
	}
}

type countingProvider struct{ calls int }

func (p *countingProvider) Complete(context.Context, CompletionRequest) (CompletionResult, error) {
	p.calls++
	return CompletionResult{Text: "x"}, nil
}
func (p *countingProvider) Close() error  { return nil }
func (p *countingProvider) Model() string { return "local-model" }

func TestBudgetedUnpricedProviderIsRefusedUnderABudget(t *testing.T) {
	inner := &countingProvider{}
	store := &recordingStore{admitErr: &llmbudget.AccountingError{Message: "BYO LLM pricing is unavailable for the configured budget."}}
	provider := WithBudget(inner, llmbudget.Guard{Store: store, OrgID: "org", Provider: "local", Model: "local-model"})
	_, err := provider.Complete(context.Background(), CategorizationRequest("p"))
	var accountingErr *llmbudget.AccountingError
	if !errors.As(err, &accountingErr) || inner.calls != 0 {
		t.Fatalf("err=%v calls=%d, want an accounting refusal and no provider call", err, inner.calls)
	}
	if store.admits[0].MaxCost != nil {
		t.Fatal("an unpriced provider must be admitted with no price")
	}
}

func TestWithBudgetLeavesMockAndNoneAlone(t *testing.T) {
	g := llmbudget.Guard{}
	if _, ok := WithBudget(MockProvider{}, g).(MockProvider); !ok {
		t.Fatal("MockProvider was wrapped")
	}
	if _, ok := WithBudget(NoneProvider{}, g).(NoneProvider); !ok {
		t.Fatal("NoneProvider was wrapped")
	}
}

// Review r1 P1: the reservation covers the whole serialized request, including
// the structured-output schema, not only the prompt text.
func TestBudgetedOpenAIReservationCoversTheWholeRequest(t *testing.T) {
	store := &recordingStore{}
	provider, _ := budgetedOpenAI(t, store, func(w http.ResponseWriter, _ *http.Request) {
		in, out, cached := 10, 10, 0
		_ = json.NewEncoder(w).Encode(openAIResponsesResponse{OutputText: `{"ok": true}`,
			Usage: &openAIUsage{InputTokens: &in, OutputTokens: &out, InputTokensDetails: &openAITokenDetails{CachedTokens: &cached}}})
	})
	request := CategorizationRequest("p")
	if _, err := provider.Complete(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	body := provider.(*OpenAIProvider).responsesBody(request, provider.(*OpenAIProvider).initialMaxOutputTokens(request))
	encoded, _ := json.Marshal(body)
	if len(encoded) <= len(body.Input)+len(body.Instructions) {
		t.Fatalf("the test request carries no schema beyond its text (%d bytes)", len(encoded))
	}
	want, _ := llmbudget.CostMicroUSD("openai", "gpt-5-mini", "", int64(len(encoded)), int64(body.MaxOutputTokens), 0)
	if got := store.admits[0].MaxCost; got == nil || *got != *want {
		t.Fatalf("reserved %v, want the cost of the whole %d-byte request = %d", got, len(encoded), *want)
	}
}

// Review r1 P1: a budgeted provider cannot batch around its guard.
func TestBudgetedOpenAIBatchIsRefusedBeforeAnyUpload(t *testing.T) {
	store := &recordingStore{admitErr: &llmbudget.AccountingError{Message: "BYO LLM batch pricing is unavailable for the configured budget."}}
	provider, calls := budgetedOpenAI(t, store, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusInternalServerError) })
	batch, ok := provider.(BatchProvider)
	if !ok {
		t.Fatal("the budgeted OpenAI provider no longer implements BatchProvider")
	}
	_, err := batch.SubmitBatch(context.Background(), []BatchItem{{CustomID: "a", Request: CategorizationRequest("p")}})
	var accountingErr *llmbudget.AccountingError
	if !errors.As(err, &accountingErr) {
		t.Fatalf("err = %v, want an accounting refusal", err)
	}
	if *calls != 0 {
		t.Fatalf("the batch made %d HTTP calls, want 0 (refused before upload)", *calls)
	}
}
