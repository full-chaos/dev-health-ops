package llmbudget

import (
	"context"
	"testing"
	"time"
)

var beforeChanges = time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)

// Every list-price row at the moment it applies, priced through the guard's
// cost function: one million input and output tokens, so each expected number
// is the published per-MTok price in micro-USD. A model priced by prompt
// length is at its most expensive tier for every request, small ones
// included (ruling D6042).
func TestListPricesMatchThePublishedPages(t *testing.T) {
	type perMTok struct{ input, cached, output int64 }
	for _, c := range []struct {
		provider, model, baseURL string
		at                       time.Time
		want                     perMTok
	}{
		{"anthropic", "claude-haiku-5-5", "", beforeChanges, perMTok{500_000, 50_000, 2_500_000}},
		{"anthropic", "claude-sonnet-5-5", "https://api.anthropic.com", beforeChanges, perMTok{2_000_000, 100_000, 10_000_000}},
		{"anthropic", "claude-opus-5-5", "https://api.anthropic.com/v1", beforeChanges, perMTok{4_000_000, 200_000, 20_000_000}},
		{"gemini", "gemini-3.8-flash", "", utcDay(2026, 12, 31).Add(23 * time.Hour), perMTok{750_000, 75_000, 3_750_000}},
		{"gemini", "gemini-3.8-flash", "https://generativelanguage.googleapis.com/v1beta/openai", utcDay(2027, 1, 1), perMTok{1_500_000, 150_000, 7_500_000}},
		{"gemini", "gemini-4-argon", "", utcDay(2026, 11, 30).Add(23 * time.Hour), perMTok{2_000_000, 100_000, 10_000_000}},
		{"gemini", "gemini-4-argon", "", utcDay(2026, 12, 1), perMTok{4_000_000, 200_000, 20_000_000}},
		{"gemini", "gemini-3.5-flash-lite", "", beforeChanges, perMTok{300_000, 30_000, 2_500_000}},
		{"qwen", "qwen-plus", "", beforeChanges, perMTok{689_000, 689_000, 6_881_000}},
		{"qwen", "qwen-plus", "https://dashscope-intl.aliyuncs.com/compatible-mode/v1", beforeChanges, perMTok{1_200_000, 1_200_000, 3_600_000}},
		{"qwen", "@cf/qwen/qwen3-30b-a3b-fp8", "https://api.cloudflare.com/client/v4/accounts/acct/ai/v1", beforeChanges, perMTok{51_000, 51_000, 335_000}},
		{"qwen", "@cf/qwen/qwen3.8-27b", "https://api.cloudflare.com/client/v4/accounts/acct/ai/v1", beforeChanges, perMTok{450_000, 450_000, 3_200_000}},
		{"qwen", "@cf/qwen/qwq-32b", "https://api.cloudflare.com/client/v4/accounts/acct/ai/v1", beforeChanges, perMTok{660_000, 660_000, 1_000_000}},
		{"qwen", "@cf/qwen/qwen2.5-coder-32b-instruct", "https://api.cloudflare.com/client/v4/accounts/acct/ai/v1", beforeChanges, perMTok{660_000, 660_000, 1_000_000}},
		{"qwen", "@cf/qwen/qwen3-30b-a3b-fp8", "https://api.cloudflare.com/client/v4/accounts/acct/ai/v1/", beforeChanges, perMTok{51_000, 51_000, 335_000}},
	} {
		name := c.provider + "/" + c.model + "@" + c.at.Format(time.RFC3339)
		price, err := PriceAt(c.provider, c.model, c.baseURL, c.at)
		if err != nil || price == nil || price.Version != ListPricingVersion {
			t.Fatalf("%s: price %+v err %v, want version %s", name, price, err, ListPricingVersion)
		}
		if price.Rates != (Rates{Input: c.want.input, CachedInput: c.want.cached, Output: c.want.output}) {
			t.Errorf("%s: %+v, want %+v", name, price.Rates, c.want)
		}
		for _, call := range []struct{ input, output, cached, want int64 }{
			{1_000_000, 0, 0, c.want.input},
			{1_000_000, 0, 1_000_000, c.want.cached},
			{0, 1_000_000, 0, c.want.output},
			// A small prompt is billed at the same row: never below the bill.
			{1_000, 1_000, 0, (1_000*c.want.input + 1_000*c.want.output + 999_999) / 1_000_000},
		} {
			cost, err := CostMicroUSDAt(c.provider, c.model, c.baseURL, call.input, call.output, call.cached, c.at)
			if err != nil || cost == nil || *cost != call.want {
				t.Errorf("%s: cost of %d in (%d cached) + %d out = %v (err %v), want %d", name, call.input, call.cached, call.output, cost, err, call.want)
			}
		}
	}
}

// A gateway, a self-hosted server, plain http, another provider's host, a
// look-alike host, a Workers AI model off the Workers AI route and an unlisted
// model are all unpriced: their prices are
// not the published list.
func TestListPricesOnlyOnTheOfficialEndpoint(t *testing.T) {
	for _, c := range []struct{ provider, model, baseURL string }{
		{"anthropic", "claude-haiku-5-5", "https://gateway.example.com/v1"},
		{"anthropic", "claude-haiku-5-5", "http://api.anthropic.com"},
		{"anthropic", "claude-haiku-5-5", "https://api.anthropic.com.evil.example"},
		{"anthropic", "claude-haiku-5-5", "https://generativelanguage.googleapis.com"},
		{"anthropic", "claude-3-haiku-20240307", ""},
		{"gemini", "gemini-3.8-flash", "https://gateway.ai.cloudflare.com/v1/acct/gw/google-ai-studio"},
		{"gemini", "gemini-3", ""},
		{"qwen", "qwen-plus", "http://localhost:11434/v1"},
		{"qwen", "qwen-plus", "https://api.cloudflare.com/client/v4/accounts/acct/ai/v1"},
		{"qwen", "@cf/qwen/qwen3-30b-a3b-fp8", ""},
		{"qwen", "@cf/qwen/qwen3-30b-a3b-fp8", "https://dashscope.aliyuncs.com/compatible-mode/v1"},
		{"qwen", "@cf/meta/llama-3.1-8b-instruct", "https://api.cloudflare.com/client/v4/accounts/acct/ai/v1"},
		{"qwen", "@cf/qwen/qwen3-30b-a3b-fp8", "https://api.cloudflare.com/not-workers-ai"},
		{"qwen", "@cf/qwen/qwen3-30b-a3b-fp8", "https://api.cloudflare.com/client/v4/accounts/acct/ai/run"},
		{"qwen", "@cf/qwen/qwen3-30b-a3b-fp8", "https://api.cloudflare.com/client/v4/accounts/acct/ai/v1/extra"},
		{"qwen", "@cf/qwen/qwen3-30b-a3b-fp8", "https://api.cloudflare.com/client/v4/accounts//ai/v1"},
		{"local", "llama3.2", ""},
		{"ollama", "llama3.2", ""},
		{"gemini", "claude-haiku-5-5", ""},
		{"anthropic", "claude-haiku-5-5", "not a url"},
	} {
		price, err := PriceAt(c.provider, c.model, c.baseURL, beforeChanges)
		if err != nil || price != nil {
			t.Errorf("%s/%s at %q priced %+v (err %v), want unpriced", c.provider, c.model, c.baseURL, price, err)
		}
		if cost, _ := CostMicroUSDAt(c.provider, c.model, c.baseURL, 10, 10, 0, beforeChanges); cost != nil {
			t.Errorf("%s/%s at %q cost %d, want unpriced", c.provider, c.model, c.baseURL, *cost)
		}
	}
}

// The OpenAI book is unchanged and keeps its own version.
func TestOpenAIPriceKeepsItsVersion(t *testing.T) {
	price, err := PriceAt("openai", "gpt-5-nano", "", beforeChanges)
	if err != nil || price == nil || price.Version != PricingVersion || price.Rates != (Rates{50_000, 50_000, 400_000}) {
		t.Fatalf("openai nano: %+v, %v", price, err)
	}
}

// A call is priced once, at the moment it is admitted: one that is reserved
// before a dated price change and settled after it settles at the price it
// was reserved at, never at the new one.
func TestACallSettlesAtThePriceItWasReservedAt(t *testing.T) {
	clock := []time.Time{
		time.Date(2026, 12, 31, 23, 59, 59, 0, time.UTC),
		time.Date(2027, 1, 1, 0, 0, 1, 0, time.UTC),
	}
	reads := 0
	store := &fakeStore{admit: Admission{ID: "r", BudgetConfigured: true}}
	guard := Guard{Store: store, OrgID: "org", Provider: "gemini", Model: "gemini-3.8-flash", Now: func() time.Time {
		at := clock[min(reads, len(clock)-1)]
		reads++
		return at
	}}
	err := guard.Run(context.Background(), Call{IdempotencyKey: "k", MaxInputTokens: 1_000_000, MaxOutputTokens: 0},
		func(context.Context) (Usage, error) { return usage(1_000_000, 0, 0), nil })
	if err != nil {
		t.Fatal(err)
	}
	if reads < 2 {
		t.Fatalf("the clock was read %d times: the test does not cross the boundary", reads)
	}
	if len(store.admitReq) != 1 || store.admitReq[0].MaxCost == nil || *store.admitReq[0].MaxCost != 750_000 {
		t.Fatalf("admits %+v, want one reservation at the 2026 price 750000", store.admitReq)
	}
	if len(store.settleReq) != 1 || store.settleReq[0].Cost == nil || *store.settleReq[0].Cost != 750_000 {
		t.Fatalf("settles %+v, want the call settled at the price it was reserved at, 750000", store.settleReq)
	}
}
