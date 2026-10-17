package llmbudget

import (
	"regexp"
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
// A model priced by prompt length has ONE row, at its most expensive tier:
// the guard must never count a call below its bill (ruling D6042). A
// scheduled price change has a second row from the change date, and the row
// in effect at the call's time prices it.
//
// Sources, read 2026-10-16:
//   - anthropic: https://platform.claude.com/docs/en/about-claude/pricing.
//     claude-haiku-5-5 is priced by prompt length (over 100,000 input tokens,
//     cache reads and writes included, pays 0.50 / 0.05 / 2.50 per MTok
//     instead of 0.10 / 0.01 / 0.50); its row is the higher tier.
//   - gemini: https://ai.google.dev/gemini-api/docs/pricing ("Last updated
//     2026-10-15 UTC"), paid tier, standard, with the dated price changes the
//     page announces.
//   - qwen on DashScope: https://www.alibabacloud.com/help/en/model-studio/model-pricing
//     (last updated 2026-10-15). International (Singapore) and Chinese
//     mainland (Beijing) are priced apart by host. qwen-plus is priced by the
//     request's input tokens; each row is its host's top tier (Beijing over
//     256K, Singapore over 256K). The page lists no qwen-plus cache price.
//   - qwen on Cloudflare Workers AI: https://developers.cloudflare.com/workers-ai/platform/pricing/
//     (last updated 2026-10-15). The daily free neurons are not deducted.

// ListPricingVersion is the version of this list, stored with each
// reservation it prices. The OpenAI book keeps PricingVersion.
const ListPricingVersion = "provider-list-2026-10-16.v1"

type listPriceRow struct {
	provider string
	// hosts are the official API hosts the row's prices apply to.
	hosts []string
	// route, when set, is the base URL path the row's prices apply to: a
	// host that serves more than this API (api.cloudflare.com) prices only
	// the route the provider client calls.
	route *regexp.Regexp
	model string
	from  time.Time
	rates Rates
}

var (
	anthropicHosts      = []string{"api.anthropic.com"}
	geminiHosts         = []string{"generativelanguage.googleapis.com"}
	dashscopeChinaHosts = []string{"dashscope.aliyuncs.com"}
	dashscopeIntlHosts  = []string{"dashscope-intl.aliyuncs.com"}
	workersAIHosts      = []string{"api.cloudflare.com"}
	// workersAIRoute is the Workers AI OpenAI-compatible base path; the client
	// posts to <base>/chat/completions, so no other path reaches Workers AI.
	workersAIRoute = regexp.MustCompile(`\A/client/v4/accounts/[^/]+/ai/v1/?\z`)

	// defaultHosts is the host of each provider's default endpoint in the
	// shared llm package: the endpoint a call with no base URL reaches.
	defaultHosts = map[string]string{
		"anthropic": "api.anthropic.com",
		"gemini":    "generativelanguage.googleapis.com",
		"qwen":      "dashscope.aliyuncs.com",
	}
)

func listRates(input, cached, output int64) Rates {
	return Rates{Input: input, CachedInput: cached, Output: output}
}

var listPrices = []listPriceRow{
	{provider: "anthropic", hosts: anthropicHosts, model: "claude-haiku-5-5", rates: listRates(500_000, 50_000, 2_500_000)},
	{provider: "anthropic", hosts: anthropicHosts, model: "claude-sonnet-5-5", rates: listRates(2_000_000, 100_000, 10_000_000)},
	{provider: "anthropic", hosts: anthropicHosts, model: "claude-opus-5-5", rates: listRates(4_000_000, 200_000, 20_000_000)},

	{provider: "gemini", hosts: geminiHosts, model: "gemini-3.8-flash", rates: listRates(750_000, 75_000, 3_750_000)},
	{provider: "gemini", hosts: geminiHosts, model: "gemini-3.8-flash", from: utcDay(2027, 1, 1), rates: listRates(1_500_000, 150_000, 7_500_000)},
	{provider: "gemini", hosts: geminiHosts, model: "gemini-4-argon", rates: listRates(2_000_000, 100_000, 10_000_000)},
	{provider: "gemini", hosts: geminiHosts, model: "gemini-4-argon", from: utcDay(2026, 12, 1), rates: listRates(4_000_000, 200_000, 20_000_000)},
	{provider: "gemini", hosts: geminiHosts, model: "gemini-3.5-flash-lite", rates: listRates(300_000, 30_000, 2_500_000)},

	{provider: "qwen", hosts: dashscopeChinaHosts, model: "qwen-plus", rates: listRates(689_000, 689_000, 6_881_000)},
	{provider: "qwen", hosts: dashscopeIntlHosts, model: "qwen-plus", rates: listRates(1_200_000, 1_200_000, 3_600_000)},
	{provider: "qwen", hosts: workersAIHosts, route: workersAIRoute, model: "@cf/qwen/qwen3-30b-a3b-fp8", rates: listRates(51_000, 51_000, 335_000)},
	{provider: "qwen", hosts: workersAIHosts, route: workersAIRoute, model: "@cf/qwen/qwen3.8-27b", rates: listRates(450_000, 450_000, 3_200_000)},
	{provider: "qwen", hosts: workersAIHosts, route: workersAIRoute, model: "@cf/qwen/qwq-32b", rates: listRates(660_000, 660_000, 1_000_000)},
	{provider: "qwen", hosts: workersAIHosts, route: workersAIRoute, model: "@cf/qwen/qwen2.5-coder-32b-instruct", rates: listRates(660_000, 660_000, 1_000_000)},
}

func utcDay(year int, month time.Month, day int) time.Time {
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

// Price is the price book's answer for one provider, model and endpoint at
// one moment: its rates, and the version of the list they come from.
type Price struct {
	Version string
	Rates   Rates
}

// PriceAt prices a call made at the moment at: the OpenAI book (PriceRates,
// unchanged) or a provider's list price, nil when neither prices it.
func PriceAt(provider, model, baseURL string, at time.Time) (*Price, error) {
	openAI, err := PriceRates(provider, model, baseURL)
	if err != nil {
		return nil, err
	}
	if openAI != nil {
		return &Price{Version: PricingVersion, Rates: *openAI}, nil
	}
	normalizedProvider := pythonparity.Lower(pythonparity.Strip(provider))
	normalized := pythonparity.Lower(pythonparity.Strip(model))
	var found *listPriceRow
	for index := range listPrices {
		row := &listPrices[index]
		if row.provider != normalizedProvider || row.model != normalized || at.Before(row.from) || !officialEndpoint(normalizedProvider, baseURL, row.hosts, row.route) {
			continue
		}
		if found == nil || row.from.After(found.from) {
			found = row
		}
	}
	if found == nil {
		return nil, nil
	}
	return &Price{Version: ListPricingVersion, Rates: found.rates}, nil
}

// officialEndpoint reports whether baseURL is one of hosts over https, on
// route when the row names one; an empty base URL is the provider's default
// endpoint.
func officialEndpoint(provider, baseURL string, hosts []string, route *regexp.Regexp) bool {
	host, path := defaultHosts[provider], ""
	if pythonparity.Strip(baseURL) != "" {
		parsed, err := pythonparity.SplitURL(baseURL)
		if err != nil || parsed.Scheme != "https" {
			return false
		}
		host, _ = parsed.Hostname()
		path = parsed.Path
	}
	if route != nil && !route.MatchString(path) {
		return false
	}
	for _, official := range hosts {
		if host == official {
			return true
		}
	}
	return false
}
