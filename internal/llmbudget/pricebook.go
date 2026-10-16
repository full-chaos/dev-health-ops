package llmbudget

import (
	"time"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// The list prices of the BYO providers other than OpenAI, as each provider
// publishes them for its own API. A call is priced only on the provider's
// official endpoint: a gateway or a self-hosted server sets its own prices,
// so its calls stay unpriced and are refused under a configured budget. A
// model that is not listed is unpriced for the same reason. A cache read is
// priced at the input rate where the provider lists no cache price, which
// over-reserves and never under-bills.
//
// Sources (each row's version names its page and the page's own date):
//   - anthropic: https://platform.claude.com/docs/en/about-claude/pricing,
//     read 2026-10-16. claude-haiku-5-5 is priced by prompt length: a request
//     over 100,000 input tokens (cache reads and writes included) pays the
//     higher prices.
//   - gemini: https://ai.google.dev/gemini-api/docs/pricing ("Last updated
//     2026-10-15 UTC"), paid tier, standard. The page announces the dated
//     price changes the rows below carry.
//   - qwen on DashScope: https://www.alibabacloud.com/help/en/model-studio/model-pricing
//     (last updated 2026-10-15). International (Singapore) and Chinese
//     mainland (Beijing) are priced apart by host; a request is billed whole at
//     the tier its input tokens fall in. The page lists no qwen-plus cache
//     price.
//   - qwen on Cloudflare Workers AI: https://developers.cloudflare.com/workers-ai/platform/pricing/
//     (last updated 2026-10-15). The daily free neurons are not deducted.

// priceTier covers the requests with at most maxInput input tokens; maxInput
// 0 has no ceiling. Tiers are in ascending order.
type priceTier struct {
	maxInput int64
	rates    Rates
}

type listPriceRow struct {
	provider string
	// hosts are the official API hosts the row's prices apply to.
	hosts   []string
	model   string
	from    time.Time
	version string
	tiers   []priceTier
}

const (
	anthropicPricing = "anthropic-public-2026-10-16.v1"
	geminiPricing    = "gemini-public-2026-10-15.v1"
	dashscopePricing = "dashscope-public-2026-10-15.v1"
	workersAIPricing = "workers-ai-public-2026-10-15.v1"
)

var (
	anthropicHosts      = []string{"api.anthropic.com"}
	geminiHosts         = []string{"generativelanguage.googleapis.com"}
	dashscopeChinaHosts = []string{"dashscope.aliyuncs.com"}
	dashscopeIntlHosts  = []string{"dashscope-intl.aliyuncs.com"}
	workersAIHosts      = []string{"api.cloudflare.com"}

	// defaultHosts is the host of each provider's default endpoint in the
	// shared llm package: the endpoint a call with no base URL reaches.
	defaultHosts = map[string]string{
		"anthropic": "api.anthropic.com",
		"gemini":    "generativelanguage.googleapis.com",
		"qwen":      "dashscope.aliyuncs.com",
	}
)

func flat(input, cached, output int64) []priceTier {
	return []priceTier{{rates: Rates{Input: input, CachedInput: cached, Output: output}}}
}

var listPrices = []listPriceRow{
	{provider: "anthropic", hosts: anthropicHosts, model: "claude-haiku-5-5", version: anthropicPricing, tiers: []priceTier{
		{maxInput: 100_000, rates: Rates{Input: 100_000, CachedInput: 10_000, Output: 500_000}},
		{rates: Rates{Input: 500_000, CachedInput: 50_000, Output: 2_500_000}},
	}},
	{provider: "anthropic", hosts: anthropicHosts, model: "claude-sonnet-5-5", version: anthropicPricing, tiers: flat(2_000_000, 100_000, 10_000_000)},
	{provider: "anthropic", hosts: anthropicHosts, model: "claude-opus-5-5", version: anthropicPricing, tiers: flat(4_000_000, 200_000, 20_000_000)},

	{provider: "gemini", hosts: geminiHosts, model: "gemini-3.8-flash", version: geminiPricing, tiers: flat(750_000, 75_000, 3_750_000)},
	{provider: "gemini", hosts: geminiHosts, model: "gemini-3.8-flash", from: utcDay(2027, 1, 1), version: geminiPricing, tiers: flat(1_500_000, 150_000, 7_500_000)},
	{provider: "gemini", hosts: geminiHosts, model: "gemini-4-argon", version: geminiPricing, tiers: flat(2_000_000, 100_000, 10_000_000)},
	{provider: "gemini", hosts: geminiHosts, model: "gemini-4-argon", from: utcDay(2026, 12, 1), version: geminiPricing, tiers: flat(4_000_000, 200_000, 20_000_000)},
	{provider: "gemini", hosts: geminiHosts, model: "gemini-3.5-flash-lite", version: geminiPricing, tiers: flat(300_000, 30_000, 2_500_000)},

	{provider: "qwen", hosts: dashscopeChinaHosts, model: "qwen-plus", version: dashscopePricing, tiers: []priceTier{
		{maxInput: 128_000, rates: Rates{Input: 115_000, CachedInput: 115_000, Output: 287_000}},
		{maxInput: 256_000, rates: Rates{Input: 345_000, CachedInput: 345_000, Output: 2_868_000}},
		{rates: Rates{Input: 689_000, CachedInput: 689_000, Output: 6_881_000}},
	}},
	{provider: "qwen", hosts: dashscopeIntlHosts, model: "qwen-plus", version: dashscopePricing, tiers: []priceTier{
		{maxInput: 256_000, rates: Rates{Input: 400_000, CachedInput: 400_000, Output: 1_200_000}},
		{rates: Rates{Input: 1_200_000, CachedInput: 1_200_000, Output: 3_600_000}},
	}},
	{provider: "qwen", hosts: workersAIHosts, model: "@cf/qwen/qwen3-30b-a3b-fp8", version: workersAIPricing, tiers: flat(51_000, 51_000, 335_000)},
	{provider: "qwen", hosts: workersAIHosts, model: "@cf/qwen/qwen3.8-27b", version: workersAIPricing, tiers: flat(450_000, 450_000, 3_200_000)},
	{provider: "qwen", hosts: workersAIHosts, model: "@cf/qwen/qwq-32b", version: workersAIPricing, tiers: flat(660_000, 660_000, 1_000_000)},
	{provider: "qwen", hosts: workersAIHosts, model: "@cf/qwen/qwen2.5-coder-32b-instruct", version: workersAIPricing, tiers: flat(660_000, 660_000, 1_000_000)},
}

func utcDay(year int, month time.Month, day int) time.Time {
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

// Price is the price book's answer for one provider, model and endpoint at
// one moment: the rates by input size, and the version of the list they come
// from (stored with each reservation).
type Price struct {
	Version string
	tiers   []priceTier
}

// RatesFor is the rates of a request with this many input tokens.
func (p *Price) RatesFor(inputTokens int64) Rates {
	for _, tier := range p.tiers {
		if tier.maxInput == 0 || inputTokens <= tier.maxInput {
			return tier.rates
		}
	}
	return p.tiers[len(p.tiers)-1].rates
}

// PriceAt prices a call made at the moment at: the OpenAI book (PriceRates,
// unchanged) or a provider's list price, nil when neither prices it.
func PriceAt(provider, model, baseURL string, at time.Time) (*Price, error) {
	rates, err := PriceRates(provider, model, baseURL)
	if err != nil {
		return nil, err
	}
	if rates != nil {
		return &Price{Version: PricingVersion, tiers: []priceTier{{rates: *rates}}}, nil
	}
	normalizedProvider := pythonparity.Lower(pythonparity.Strip(provider))
	normalized := pythonparity.Lower(pythonparity.Strip(model))
	var found *listPriceRow
	for index := range listPrices {
		row := &listPrices[index]
		if row.provider != normalizedProvider || row.model != normalized || at.Before(row.from) || !officialHost(normalizedProvider, baseURL, row.hosts) {
			continue
		}
		if found == nil || row.from.After(found.from) {
			found = row
		}
	}
	if found == nil {
		return nil, nil
	}
	return &Price{Version: found.version, tiers: found.tiers}, nil
}

// officialHost reports whether baseURL is one of hosts over https; an empty
// base URL is the provider's default endpoint.
func officialHost(provider, baseURL string, hosts []string) bool {
	host := defaultHosts[provider]
	if pythonparity.Strip(baseURL) != "" {
		parsed, err := pythonparity.SplitURL(baseURL)
		if err != nil || parsed.Scheme != "https" {
			return false
		}
		host, _ = parsed.Hostname()
	}
	for _, official := range hosts {
		if host == official {
			return true
		}
	}
	return false
}
