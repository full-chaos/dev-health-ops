package investmentexplain

import (
	"reflect"
	"testing"
)

// validMinimalCompletionText satisfies ParseInvestmentMixResponse's
// TOP_LEVEL_KEYS/validConfidence/etc. checks with the smallest shape that
// passes -- its own confidence.band_mix content is IRRELEVANT to the
// output (the parser always substitutes ParseOptions.FallbackBandMix,
// investment_mix_parser.py:119, never the LLM's own reported band_mix --
// see Confidence.BandMix's doc comment, types.go). Kept as its own
// constant, distinct from explain_golden_test.go's
// recordedFixtureCompletionText, so this file has no golden-fixture
// dependency at all.
const validMinimalCompletionText = `{"summary": "A minimal valid summary.", "top_findings": [], "confidence": {"level": "moderate", "quality_mean": null, "quality_stddev": null, "band_mix": {"high": 0, "moderate": 0, "low": 0, "very_low": 0, "unknown": 0}, "drivers": []}, "what_to_check_next": [], "anti_claims": []}`

// TestParseInvestmentMixResponsePreservesFallbackBandMixOrder is a
// regression test for codex round 1's P1 (band_mix ordering): a request
// whose work units encounter "low" before "high" must produce a
// band_mix in THAT order in the successful-parse output, not a fixed
// high/moderate/low/very_low/unknown order -- exactly the case the
// existing explain_investment_mix goldens cannot expose (their own fixed
// fixture happens to encounter "high" first, so a fixed-order bug there
// is invisible; see the round's own note on this).
func TestParseInvestmentMixResponsePreservesFallbackBandMixOrder(t *testing.T) {
	lowThenHigh := BandMix{{Band: "low", Count: 1}, {Band: "high", Count: 1}}

	result := ParseInvestmentMixResponse(validMinimalCompletionText, ParseOptions{
		FallbackLevel:   "moderate",
		FallbackBandMix: lowThenHigh,
	})
	if result.Status != ParseStatusValid {
		t.Fatalf("status = %s, want valid", result.Status)
	}
	if !reflect.DeepEqual(result.Output.Confidence.BandMix, lowThenHigh) {
		t.Fatalf("parser BandMix = %+v, want %+v (order preserved)", result.Output.Confidence.BandMix, lowThenHigh)
	}

	explanation := explainOutputToExplanation(*result.Output)
	wantExplanationBandMix := BandMix{{Band: "low", Count: 1}, {Band: "high", Count: 1}}
	if !reflect.DeepEqual(explanation.Confidence.BandMix, wantExplanationBandMix) {
		t.Fatalf("explanation BandMix = %+v, want %+v (order preserved, not re-sorted into a fixed band list)",
			explanation.Confidence.BandMix, wantExplanationBandMix)
	}
}
