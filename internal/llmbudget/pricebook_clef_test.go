package llmbudget

import (
	"testing"
	"time"
)

const clefTestAccount = "0123456789abcdef0123456789abcdef"

func clefTestURL(model string) string {
	return "https://api.cloudflare.com/client/v4/accounts/" + clefTestAccount + "/ai/run/@cf/cloudflare/" + model
}

// The path the guard really sees is priced, for both models: clef at $0.24 per
// M input tokens and clef-flash at $0.038, output free.
func TestAClefRequestPathIsPricedForBothModels(t *testing.T) {
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	for model, wantInput := range map[string]int64{"clef": 240_000, "clef-flash": 38_000} {
		price, err := PriceAt("clef", model, clefTestURL(model), now)
		if err != nil || price == nil {
			t.Fatalf("%s: %v, %v: the real request path is unpriced", model, price, err)
		}
		if price.Rates.Input != wantInput || price.Rates.Output != 0 {
			t.Errorf("%s rates = %+v, want input %d output 0", model, price.Rates, wantInput)
		}
		cost, err := CostMicroUSDAt("clef", model, clefTestURL(model), 1_000_000, 1_000_000, 0, 0, now)
		if err != nil || cost == nil || *cost != wantInput {
			t.Errorf("%s: a million input and a million output tokens cost %v, want %d micro-USD (output is free)", model, cost, wantInput)
		}
	}
}

// Anything but the exact request path, host and model is unpriced, so a capped
// org's call is refused instead of metered at a wrong rate.
func TestAClefCallAnywhereElseIsUnpriced(t *testing.T) {
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	for name, call := range map[string][3]string{
		"other host":     {"clef", "clef", "https://api.typesafe.ai/client/v4/accounts/" + clefTestAccount + "/ai/run/@cf/cloudflare/clef"},
		"base path":      {"clef", "clef", "https://api.cloudflare.com/client/v4/accounts/" + clefTestAccount + "/ai"},
		"other model":    {"clef", "clef-pro", clefTestURL("clef-pro")},
		"model/path":     {"clef", "clef", clefTestURL("clef-flash") + "/x"},
		"short account":  {"clef", "clef", "https://api.cloudflare.com/client/v4/accounts/abc/ai/run/@cf/cloudflare/clef"},
		"other provider": {"openai", "clef", clefTestURL("clef")},
		"http":           {"clef", "clef", "http://api.cloudflare.com/client/v4/accounts/" + clefTestAccount + "/ai/run/@cf/cloudflare/clef"},
		"no url":         {"clef", "clef", ""},
	} {
		price, err := PriceAt(call[0], call[1], call[2], now)
		if err != nil || price != nil {
			t.Errorf("%s: priced as %+v (%v)", name, price, err)
		}
	}
}

// A reservation records the version of the list that priced it. Rows were added
// to the list for clef, so the version moved with them and the clef rows carry
// it; an older reservation keeps the version it was priced under.
func TestTheListVersionThatPricesClefIsPinned(t *testing.T) {
	const want = "provider-list-2026-10-24.v1"
	if ListPricingVersion != want {
		t.Fatalf("ListPricingVersion = %q, want %q: the list changed with the clef rows", ListPricingVersion, want)
	}
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	for _, model := range []string{"clef", "clef-flash"} {
		price, err := PriceAt("clef", model, clefTestURL(model), now)
		if err != nil || price == nil || price.Version != want {
			t.Fatalf("%s: price %+v err %v, want version %s", model, price, err, want)
		}
	}
}
