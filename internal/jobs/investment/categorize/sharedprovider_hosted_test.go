package categorize

import (
	"context"
	"net/http"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/llmbudget"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// A budgeted org on a hosted provider: the call is reserved at the list
// price and settled at the reported usage. Chat Completions servers report
// no cache reads, so the settle counts none (every input token at the full
// input rate) instead of refusing an answered call as usage-unavailable.
func TestBudgetedHostedProvidersSettleAtTheListPrice(t *testing.T) {
	for _, c := range []struct {
		kind  ProviderKind
		model string
		body  string
		// cost = ceil((input-cached)*in + cached*cachedRate + out*outRate) / 1e6
		wantCost int64
	}{
		// gemini-3.8-flash before 2027: 750000 in, 3750000 out per MTok.
		{ProviderKindGemini, "gemini-3.8-flash", `{"choices":[{"message":{"role":"assistant","content":"{\"ok\":true}"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1000,"completion_tokens":200}}`, 1500},
		// qwen-plus on DashScope's default (China) endpoint, under 128K: 115000 in, 287000 out.
		{ProviderKindQwen, "qwen-plus", `{"choices":[{"message":{"role":"assistant","content":"{\"ok\":true}"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1000,"completion_tokens":200}}`, 173},
		// claude-haiku-5-5 under 100k: input_tokens 900 + cache read 100 = 1000 input, 100 at 10000.
		{ProviderKindAnthropic, "claude-haiku-5-5", `{"content":[{"type":"text","text":"{\"ok\":true}"}],"stop_reason":"end_turn","usage":{"input_tokens":900,"cache_read_input_tokens":100,"output_tokens":200}}`, 191},
	} {
		t.Run(string(c.kind), func(t *testing.T) {
			server, _ := statusOnce(t, http.StatusOK, c.body)
			shared, err := NewSharedProvider(SharedProviderConfig{Kind: c.kind, APIKey: secrets.NewHidden("test-key-value"), BaseURL: server.URL, Model: c.model})
			if err != nil {
				t.Fatal(err)
			}
			store := &recordingStore{}
			// The guard prices the org's configured endpoint (the default
			// one); the test server only stands in for the transport.
			guarded := WithBudget(shared, llmbudget.Guard{Store: store, OrgID: "org", Provider: string(c.kind), Model: shared.Model()})
			result, err := guarded.Complete(context.Background(), CategorizationRequest("p"))
			if err != nil || result.Text != `{"ok":true}` {
				t.Fatalf("result %+v err %v", result, err)
			}
			if len(store.admits) != 1 || store.admits[0].MaxCost == nil || len(store.settles) != 1 {
				t.Fatalf("admits %+v settles %+v, want one priced reservation settled once", store.admits, store.settles)
			}
			settle := store.settles[0]
			if settle.Outcome != llmbudget.OutcomeSucceeded || settle.Cost == nil || *settle.Cost != c.wantCost {
				t.Fatalf("settle %+v, want succeeded at %d micro-USD", settle, c.wantCost)
			}
			if *store.admits[0].MaxCost < c.wantCost {
				t.Fatalf("reserved %d below the billed %d", *store.admits[0].MaxCost, c.wantCost)
			}
		})
	}
}

// An unpriced model (here a self-hosted gateway's) stays refused under a
// configured budget, before any request is sent.
func TestBudgetedHostedProviderUnpricedIsRefusedUnderABudget(t *testing.T) {
	server, calls := statusOnce(t, http.StatusOK, `{}`)
	shared, err := NewSharedProvider(SharedProviderConfig{Kind: ProviderKindGemini, APIKey: secrets.NewHidden("test-key-value"), BaseURL: server.URL, Model: "gemini-3.8-flash"})
	if err != nil {
		t.Fatal(err)
	}
	store := &recordingStore{admitErr: &llmbudget.AccountingError{Message: "BYO LLM pricing is unavailable for the configured budget."}}
	guarded := WithBudget(shared, llmbudget.Guard{Store: store, OrgID: "org", Provider: "gemini", Model: "gemini-3.8-flash", BaseURL: "https://gateway.example.com/v1"})
	_, err = guarded.Complete(context.Background(), CategorizationRequest("p"))
	if err == nil || *calls != 0 || len(store.admits) != 1 || store.admits[0].MaxCost != nil {
		t.Fatalf("err %v calls %d admits %+v, want an unpriced admission refused before any request", err, *calls, store.admits)
	}
}
