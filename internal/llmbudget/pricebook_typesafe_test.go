package llmbudget

import (
	"testing"
	"time"
)

const typeSafeTestPostURL = "https://api.typesafe.ai/v1/systemone"

// An org's own TypeSafe call is priced on the one URL it is sent to: jev-1.13.0
// at $0.042 per M input tokens, output free.
func TestATypeSafeRequestIsPricedOnItsSystemOneURL(t *testing.T) {
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	price, err := PriceAt("typesafe", "jev-1.13.0", typeSafeTestPostURL, now)
	if err != nil || price == nil {
		t.Fatalf("PriceAt = %v, %v: the real request URL is unpriced", price, err)
	}
	if price.Rates.Input != 42_000 || price.Rates.CachedInput != 42_000 || price.Rates.Output != 0 || price.Version != ListPricingVersion {
		t.Fatalf("price = %+v, want input 42000 cached 42000 output 0 version %s", price, ListPricingVersion)
	}
	cost, err := CostMicroUSDAt("typesafe", "jev-1.13.0", typeSafeTestPostURL, 1_000_000, 1_000_000, 0, 0, now)
	if err != nil || cost == nil || *cost != 42_000 {
		t.Fatalf("a million input and a million output tokens cost %v (%v), want 42000 micro-USD", cost, err)
	}
}

// Any other URL, model or provider is unpriced, so a capped org's call is
// refused instead of metered at a wrong rate.
func TestATypeSafeCallAnywhereElseIsUnpriced(t *testing.T) {
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	for name, call := range map[string][3]string{
		"base url":       {"typesafe", "jev-1.13.0", "https://api.typesafe.ai"},
		"no url":         {"typesafe", "jev-1.13.0", ""},
		"other path":     {"typesafe", "jev-1.13.0", "https://api.typesafe.ai/v1/systemone/x"},
		"other host":     {"typesafe", "jev-1.13.0", "https://api.typesafe.ai.example.com/v1/systemone"},
		"http":           {"typesafe", "jev-1.13.0", "http://api.typesafe.ai/v1/systemone"},
		"latest":         {"typesafe", "jev-latest", typeSafeTestPostURL},
		"other version":  {"typesafe", "jev-1.14.0", typeSafeTestPostURL},
		"other provider": {"clef", "jev-1.13.0", typeSafeTestPostURL},
	} {
		price, err := PriceAt(call[0], call[1], call[2], now)
		if err != nil || price != nil {
			t.Errorf("%s: priced as %+v (%v)", name, price, err)
		}
	}
}
