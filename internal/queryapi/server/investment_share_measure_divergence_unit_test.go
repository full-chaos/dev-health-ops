package server

import (
	"math"
	"testing"
)

func TestInvestmentForOrderComparisonKeepsKeyOrderAndZeroesTheValues(t *testing.T) {
	goBody := `{"theme_distribution":{"quality":2.6,"feature_delivery":0.4},"subcategory_distribution":{"quality.bugfix":2.6},"evidence_quality_distribution":{"moderate":3.0},"edges":[],"measure":"COUNT","denominator":3.0}`
	pythonBody := `{"theme_distribution":{"quality":15.0,"feature_delivery":4.0},"subcategory_distribution":{"quality.bugfix":15.0},"evidence_quality_distribution":{"moderate":3.0},"edges":[]}`
	goOut, pythonOut, values, err := investmentForOrderComparison(goBody, pythonBody)
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"theme_distribution":{"quality":0,"feature_delivery":0},"subcategory_distribution":{"quality.bugfix":0},"evidence_quality_distribution":{"moderate":3.0},"edges":[]}`
	if goOut != want || pythonOut != want {
		t.Fatalf("go %s\npython %s\nwant %s", goOut, pythonOut, want)
	}
	if math.Abs(values.Denominator-3) > 1e-9 || math.Abs(values.Distribution["theme_distribution"]["quality"]-2.6) > 1e-9 {
		t.Fatalf("values = %+v", values)
	}
	if _, _, _, err := investmentForOrderComparison(`{"theme_distribution":{}}`, pythonBody); err == nil {
		t.Fatal("a body without the Go-only tail must be refused")
	}
	// A reordered key is still a difference.
	reordered := `{"theme_distribution":{"feature_delivery":15.0,"quality":4.0},"subcategory_distribution":{"quality.bugfix":15.0},"evidence_quality_distribution":{"moderate":3.0},"edges":[]}`
	_, reorderedOut, _, _ := investmentForOrderComparison(goBody, reordered)
	if reorderedOut == want {
		t.Fatal("the zeroed comparison lost the key order")
	}
}
