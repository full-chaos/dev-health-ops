//go:build integration

package llmbudget

import (
	"context"
	"testing"
)

// A budgeted call priced from a provider list is reserved and settled at that
// list's price, the reservation stores that list's version, and the status
// names it; the OpenAI book's version is not stamped on it.
func TestPostgresListPricedCallStoresItsListVersion(t *testing.T) {
	pool := newPool(t)
	setLimit(t, pool, "100000")
	guard := guardOn(pool)
	guard.Provider, guard.Model = "gemini", "gemini-3.8-flash"
	err := guard.Run(context.Background(), Call{IdempotencyKey: "gemini-1", MaxInputTokens: 1000, MaxOutputTokens: 1000},
		func(context.Context) (Usage, error) { return usage(1000, 200, 0), nil })
	if err != nil {
		t.Fatal(err)
	}
	var version string
	var reserved, actual int64
	if err := pool.QueryRow(context.Background(),
		`SELECT pricing_version, reserved_micro_usd, actual_micro_usd FROM byo_llm_budget_reservations WHERE org_id = $1`, testOrg,
	).Scan(&version, &reserved, &actual); err != nil {
		t.Fatal(err)
	}
	// 1000 x 0.75 + 1000 x 3.75 reserved; 1000 x 0.75 + 200 x 3.75 billed.
	if version != geminiPricing || reserved != 4500 || actual != 1500 {
		t.Fatalf("row version %q reserved %d actual %d, want %q 4500 1500", version, reserved, actual, geminiPricing)
	}
	status, err := Compute(context.Background(), pool, guard.Lookup, testOrg,
		Input{Provider: "gemini", Model: "gemini-3.8-flash", RawLimit: strPtr("100000")}, testNow)
	if err != nil {
		t.Fatal(err)
	}
	if status.PricingVersion == nil || *status.PricingVersion != geminiPricing || status.Used.Int64() != 1500 {
		t.Fatalf("status version %v used %v, want %s 1500", status.PricingVersion, status.Used, geminiPricing)
	}
}
