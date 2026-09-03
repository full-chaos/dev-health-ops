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

// TestBandMixUnmarshalJSONRejectsNonObject regresses codex round 2 (P2):
// a malformed cached explanation's band_mix ("[]"/"null"/anything not a
// JSON object) must fail to decode, not silently become an empty
// BandMix -- Python's InvestmentMixExplanation(**cached_data) is a
// Pydantic model with band_mix: dict[str, int] (api/models/schemas.py:266)
// and RAISES on exactly this shape, which explain_investment_mix's
// cache-read catches and falls through to recompute
// (investment_mix_explain.py:229/233). DecodeInvestmentMixExplanation's
// caller (explain.go's cache-read branch) already treats any decode
// error as a cache miss -- the fix is entirely in making this decoder
// return that error instead of a silent nil.
func TestBandMixUnmarshalJSONRejectsNonObject(t *testing.T) {
	cases := []struct {
		name string
		json string
	}{
		{"array", `[]`},
		{"null", `null`},
		{"string", `"oops"`},
		{"number", `1`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var m BandMix
			if err := m.UnmarshalJSON([]byte(tc.json)); err == nil {
				t.Fatalf("UnmarshalJSON(%s) = nil error, want an error (band_mix must be a JSON object)", tc.json)
			}
		})
	}
}

// TestDecodeInvestmentMixExplanationRejectsMalformedBandMix is the
// end-to-end version: a cached explanation JSON blob with a malformed
// band_mix must fail to decode as a whole, so explain.go's cache-read
// branch treats it as a miss and recomputes -- matching Python's
// Pydantic-validation-then-recompute behavior exactly, not just the
// isolated BandMix type's own decode.
func TestDecodeInvestmentMixExplanationRejectsMalformedBandMix(t *testing.T) {
	malformed := `{"summary":"s","top_findings":[],"confidence":{"level":"low","quality_mean":null,"quality_stddev":null,"band_mix":[],"drivers":[]},"what_to_check_next":[],"anti_claims":[],"status":"valid"}`
	if _, err := DecodeInvestmentMixExplanation(malformed); err == nil {
		t.Fatal("DecodeInvestmentMixExplanation with band_mix=[] returned nil error, want an error (cache-read must treat this as a miss)")
	}
}
