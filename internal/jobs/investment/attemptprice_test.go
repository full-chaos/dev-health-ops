package investment

import (
	"context"
	"math"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize/decision"
	"github.com/full-chaos/dev-health-ops/internal/llmbudget"
)

func lastServedRow(t *testing.T, served *ServedDecision, m *Materializer) (row struct {
	cost    float64
	version string
	tokens  uint32
}) {
	t.Helper()
	if _, err := m.categorizeEntry(context.Background(), shadowTestConfig(), shadowEntry(0, "u1", shadowTestBundle(t, "u1", 6))); err != nil {
		t.Fatalf("categorize: %v", err)
	}
	rows := servedAttempts(t, served)
	last := rows[len(rows)-1]
	t.Logf("row: provider=%s model=%s input_tokens=%d billed_cost_usd=%.9f rates_version=%s",
		last.Provider, last.ModelRequested, last.InputTokens, last.BilledCostUSD, last.RatesVersion)
	row.cost, row.version, row.tokens = last.BilledCostUSD, last.RatesVersion, uint32(last.InputTokens)
	return row
}

func closeEnough(got, want float64) bool { return math.Abs(got-want) < 1e-12 }

// CHAOS-13097: a byo-clef attempt row is priced at the clef rate ($0.24 per M
// input), not the TypeSafe rate ($0.042).
func TestQ10AByoClefAttemptRowIsPricedAtTheClefRate(t *testing.T) {
	stub := newClefBYO(clefAnswers("clef"))
	served, err := stub.build().Build(context.Background(), "org-1")
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	defer func() { _ = served.Close() }()
	m := &Materializer{}
	m.SetServed(served)
	row := lastServedRow(t, served, m)
	clef := float64(row.tokens) * 0.24 / 1e6
	if !closeEnough(row.cost, clef) || row.version != llmbudget.ListPricingVersion {
		t.Fatalf("byo-clef row billed %.9f USD under %q; the clef rate gives %.9f USD under %q", row.cost, row.version, clef, llmbudget.ListPricingVersion)
	}
	if spent := served.ledger.spent(); spent != int64(row.tokens)*shadowNanoUSDPerInputToken {
		t.Fatalf("cap ledger spent = %d nano-USD, want the unchanged TypeSafe-rate %d", spent, int64(row.tokens)*shadowNanoUSDPerInputToken)
	}
}

func TestByoTypeSafeAttemptRowIsPricedAtTheTypeSafeListRate(t *testing.T) {
	stub := newTypeSafeBYO(typeSafeAnswers(testTypeSafeModel))
	served, m, err := stub.categorizeOne(t)
	if err != nil {
		t.Fatalf("categorize: %v", err)
	}
	defer func() { _ = served.Close() }()
	rows := servedAttempts(t, served)
	last := rows[len(rows)-1]
	want := float64(testTypeSafeInTokens) * 0.042 / 1e6
	if !closeEnough(last.BilledCostUSD, want) || last.RatesVersion != llmbudget.ListPricingVersion {
		t.Fatalf("byo-typesafe row billed %.9f under %q, want %.9f under %q", last.BilledCostUSD, last.RatesVersion, want, llmbudget.ListPricingVersion)
	}
	_ = m
}

func TestPlatformTypeSafeAttemptRowKeepsThePlatformRate(t *testing.T) {
	exchange := &shadowExchange{
		attempts: []categorize.SystemOneAttempt{{StatusCode: 200}},
		usage:    categorize.SystemOneUsage{Reported: true, InputTokens: 2644, OutputTokens: 5},
		billed:   2644 * shadowNanoUSDPerInputToken,
	}
	rows := attemptRecords(shadowTestConfig(), "served", attemptBackend{}, decision.Identity{Model: "jev-1.13.0"}, "cfg", "u1", exchange, "ok", "jev-1.13.0")
	want := float64(2644) * 0.042 / 1e6
	if len(rows) != 1 || !closeEnough(rows[0].BilledCostUSD, want) || rows[0].RatesVersion != shadowRatesVersion {
		t.Fatalf("rows = %+v, want %.9f under %q", rows, want, shadowRatesVersion)
	}
}

func TestAnUnknownByoBackendAttemptRowIsUnpricedNotTypeSafe(t *testing.T) {
	usage := categorize.SystemOneUsage{Reported: true, InputTokens: 2644}
	backends := map[string]attemptBackend{
		"unlisted model on a listed provider": {provider: "clef", baseURL: "https://api.cloudflare.com/client/v4/accounts/0123456789abcdef0123456789abcdef/ai/run/@cf/cloudflare/clef", byo: true},
		"unknown provider":                    {provider: "nosuchprovider", byo: true},
		"gateway endpoint":                    {provider: "typesafe", baseURL: "https://gateway.example.invalid/v1/systemone", byo: true},
	}
	models := map[string]string{"unlisted model on a listed provider": "no-such-model", "unknown provider": "m", "gateway endpoint": "jev-1.13.0"}
	for name, backend := range backends {
		exchange := &shadowExchange{attempts: []categorize.SystemOneAttempt{{StatusCode: 200}}, usage: usage}
		rows := attemptRecords(shadowTestConfig(), "served", backend, decision.Identity{Model: models[name]}, "cfg", "u1", exchange, "ok", models[name])
		if len(rows) != 1 || rows[0].RatesVersion != attemptRatesUnpriced || rows[0].BilledCostUSD != 0 {
			t.Fatalf("%s: rows = %+v, want unpriced with cost 0", name, rows)
		}
	}
}
