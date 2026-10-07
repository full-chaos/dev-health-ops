package decisioneval

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

//go:embed testdata/rates-2026-10-06.json
var defaultRatesJSON []byte

// PriceRate is the price of one provider/api mode/model family, USD per 1M tokens.
type PriceRate struct {
	Provider           string   `json:"provider"`
	APIMode            string   `json:"api_mode"`
	ModelPrefix        string   `json:"model_prefix"`
	InputPerMTok       float64  `json:"input_per_mtok"`
	CachedInputPerMTok *float64 `json:"cached_input_per_mtok"`
	OutputPerMTok      float64  `json:"output_per_mtok"`
}

// RateTable is the versioned rate data.
type RateTable struct {
	Version string      `json:"rates_version"`
	Rates   []PriceRate `json:"rates"`
}

// LoadRates reads a rate table file. An empty path loads the embedded default.
func LoadRates(path string) (*RateTable, error) {
	data := defaultRatesJSON
	if path != "" {
		var err error
		if data, err = os.ReadFile(path); err != nil {
			return nil, fmt.Errorf("rates: %w", err)
		}
	}
	var t RateTable
	if err := json.Unmarshal(data, &t); err != nil {
		return nil, fmt.Errorf("rates: %w", err)
	}
	if t.Version == "" || len(t.Rates) == 0 {
		return nil, fmt.Errorf("rates: version or rates missing")
	}
	return &t, nil
}

// Lookup finds the rate of a provider, api mode and model. A model with no rate
// is an error: an unpriced request must not go out under a spend cap.
func (t *RateTable) Lookup(provider, apiMode, model string) (PriceRate, error) {
	best := -1
	for i, r := range t.Rates {
		if r.Provider == provider && r.APIMode == apiMode && strings.HasPrefix(model, r.ModelPrefix) {
			if best < 0 || len(r.ModelPrefix) > len(t.Rates[best].ModelPrefix) {
				best = i
			}
		}
	}
	if best < 0 {
		return PriceRate{}, fmt.Errorf("no rate for provider=%s api_mode=%s model=%s in %s", provider, apiMode, model, t.Version)
	}
	return t.Rates[best], nil
}

// Cost prices a usage report. It returns the billed cost (cached input at the
// cached rate when the table has one) and the cost with no cache discount.
func (r PriceRate) Cost(u UsageReport) (billed, noDiscount float64) {
	cached := u.CachedInputTokens
	if cached > u.InputTokens {
		cached = u.InputTokens
	}
	if cached < 0 {
		cached = 0
	}
	cachedRate := r.InputPerMTok
	if r.CachedInputPerMTok != nil {
		cachedRate = *r.CachedInputPerMTok
	}
	billed = (float64(u.InputTokens-cached)*r.InputPerMTok + float64(cached)*cachedRate + float64(u.OutputTokens)*r.OutputPerMTok) / 1e6
	noDiscount = (float64(u.InputTokens)*r.InputPerMTok + float64(u.OutputTokens)*r.OutputPerMTok) / 1e6
	return billed, noDiscount
}

// Estimate prices a request before it is sent: estimated input tokens plus the
// largest output the request may produce.
func (r PriceRate) Estimate(inputTokens, maxOutputTokens int) float64 {
	return (float64(inputTokens)*r.InputPerMTok + float64(maxOutputTokens)*r.OutputPerMTok) / 1e6
}
