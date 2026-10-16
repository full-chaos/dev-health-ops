package llmbudget

import (
	"testing"
	"time"
)

var beforeChanges = time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)

// Every list-price row at the moment it applies, priced through the public
// cost function: one million input, cached and output tokens each, so each
// expected number is the published per-MTok price in micro-USD.
func TestListPricesMatchThePublishedPages(t *testing.T) {
	type rates struct{ input, cached, output int64 }
	for _, c := range []struct {
		provider, model, baseURL string
		at                       time.Time
		smallPrompt              rates
		largePrompt              rates
		version                  string
	}{
		{"anthropic", "claude-haiku-5-5", "", beforeChanges, rates{100_000, 10_000, 500_000}, rates{500_000, 50_000, 2_500_000}, anthropicPricing},
		{"anthropic", "claude-sonnet-5-5", "https://api.anthropic.com", beforeChanges, rates{2_000_000, 100_000, 10_000_000}, rates{2_000_000, 100_000, 10_000_000}, anthropicPricing},
		{"anthropic", "claude-opus-5-5", "https://api.anthropic.com/v1", beforeChanges, rates{4_000_000, 200_000, 20_000_000}, rates{4_000_000, 200_000, 20_000_000}, anthropicPricing},
		{"gemini", "gemini-3.8-flash", "", beforeChanges, rates{750_000, 75_000, 3_750_000}, rates{750_000, 75_000, 3_750_000}, geminiPricing},
		{"gemini", "gemini-3.8-flash", "https://generativelanguage.googleapis.com/v1beta/openai", utcDay(2027, 1, 1), rates{1_500_000, 150_000, 7_500_000}, rates{1_500_000, 150_000, 7_500_000}, geminiPricing},
		{"gemini", "gemini-4-argon", "", utcDay(2026, 11, 30).Add(23 * time.Hour), rates{2_000_000, 100_000, 10_000_000}, rates{2_000_000, 100_000, 10_000_000}, geminiPricing},
		{"gemini", "gemini-4-argon", "", utcDay(2026, 12, 1), rates{4_000_000, 200_000, 20_000_000}, rates{4_000_000, 200_000, 20_000_000}, geminiPricing},
		{"gemini", "gemini-3.5-flash-lite", "", beforeChanges, rates{300_000, 30_000, 2_500_000}, rates{300_000, 30_000, 2_500_000}, geminiPricing},
		{"qwen", "qwen-plus", "", beforeChanges, rates{115_000, 115_000, 287_000}, rates{689_000, 689_000, 6_881_000}, dashscopePricing},
		{"qwen", "qwen-plus", "https://dashscope-intl.aliyuncs.com/compatible-mode/v1", beforeChanges, rates{400_000, 400_000, 1_200_000}, rates{1_200_000, 1_200_000, 3_600_000}, dashscopePricing},
		{"qwen", "@cf/qwen/qwen3-30b-a3b-fp8", "https://api.cloudflare.com/client/v4/accounts/acct/ai/v1", beforeChanges, rates{51_000, 51_000, 335_000}, rates{51_000, 51_000, 335_000}, workersAIPricing},
		{"qwen", "@cf/qwen/qwen3.8-27b", "https://api.cloudflare.com/client/v4/accounts/acct/ai/v1", beforeChanges, rates{450_000, 450_000, 3_200_000}, rates{450_000, 450_000, 3_200_000}, workersAIPricing},
		{"qwen", "@cf/qwen/qwq-32b", "https://api.cloudflare.com/client/v4/accounts/acct/ai/v1", beforeChanges, rates{660_000, 660_000, 1_000_000}, rates{660_000, 660_000, 1_000_000}, workersAIPricing},
		{"qwen", "@cf/qwen/qwen2.5-coder-32b-instruct", "https://api.cloudflare.com/client/v4/accounts/acct/ai/v1", beforeChanges, rates{660_000, 660_000, 1_000_000}, rates{660_000, 660_000, 1_000_000}, workersAIPricing},
	} {
		name := c.provider + "/" + c.model + "@" + c.at.Format(time.DateOnly)
		price, err := PriceAt(c.provider, c.model, c.baseURL, c.at)
		if err != nil || price == nil || price.Version != c.version {
			t.Fatalf("%s: price %+v err %v, want version %s", name, price, err, c.version)
		}
		for _, tier := range []struct {
			input int64
			want  rates
		}{{100_000, c.smallPrompt}, {1_000_000, c.largePrompt}} {
			got := price.RatesFor(tier.input)
			if got.Input != tier.want.input || got.CachedInput != tier.want.cached || got.Output != tier.want.output {
				t.Errorf("%s at %d input tokens: %+v, want %+v", name, tier.input, got, tier.want)
			}
		}
		// Cost through the guard's own function: 1M input of which none
		// cached, plus 1M output, at the large-prompt tier.
		cost, err := CostMicroUSDAt(c.provider, c.model, c.baseURL, 1_000_000, 1_000_000, 0, c.at)
		if err != nil || cost == nil || *cost != c.largePrompt.input+c.largePrompt.output {
			t.Errorf("%s: cost %v err %v, want %d", name, cost, err, c.largePrompt.input+c.largePrompt.output)
		}
	}
}

// The tier boundaries are inclusive of their own limit, and a request is
// billed whole at the tier its input falls in.
func TestListPriceTierBoundaries(t *testing.T) {
	for _, c := range []struct {
		provider, model, baseURL string
		input                    int64
		wantInputRate            int64
	}{
		{"anthropic", "claude-haiku-5-5", "", 100_000, 100_000},
		{"anthropic", "claude-haiku-5-5", "", 100_001, 500_000},
		{"qwen", "qwen-plus", "", 128_000, 115_000},
		{"qwen", "qwen-plus", "", 128_001, 345_000},
		{"qwen", "qwen-plus", "", 256_000, 345_000},
		{"qwen", "qwen-plus", "", 256_001, 689_000},
		{"qwen", "qwen-plus", "https://dashscope-intl.aliyuncs.com/compatible-mode/v1", 256_000, 400_000},
		{"qwen", "qwen-plus", "https://dashscope-intl.aliyuncs.com/compatible-mode/v1", 256_001, 1_200_000},
	} {
		cost, err := CostMicroUSDAt(c.provider, c.model, c.baseURL, c.input, 0, 0, beforeChanges)
		want := (c.input*c.wantInputRate + 999_999) / 1_000_000
		if err != nil || cost == nil || *cost != want {
			t.Errorf("%s/%s %s at %d input: %v (err %v), want %d", c.provider, c.model, c.baseURL, c.input, cost, err, want)
		}
	}
}

// A gateway, a self-hosted server, plain http, another provider's host, a
// look-alike host and an unlisted model are all unpriced: their prices are
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
	if err != nil || price == nil || price.Version != PricingVersion || price.RatesFor(1) != (Rates{50_000, 50_000, 400_000}) {
		t.Fatalf("openai nano: %+v, %v", price, err)
	}
}
