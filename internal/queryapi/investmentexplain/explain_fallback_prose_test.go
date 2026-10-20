package investmentexplain

import "testing"

func TestInvalidFallbackProseHasNoRawKeys(t *testing.T) {
	fallback := invalidLLMOutputExplanation([]keyValue{{Key: "feature_delivery", Value: 6}, {Key: "maintenance", Value: 4}}, 10, "moderate", nil, nil, BandMix{}, nil, "work")
	if got := rawKeyCount(fallback); got != 0 {
		t.Errorf("rawKeyCount(fallback) = %d, want 0: %q", got, fallback.TopFindings[0].Finding)
	}
	if got := fallback.TopFindings[0].Evidence.Theme; got != "feature_delivery" {
		t.Errorf("Evidence.Theme = %q, want the key feature_delivery", got)
	}
	want := "Work appears concentrated in Feature Delivery (~60% of total)."
	if got := fallback.TopFindings[0].Finding; got != want {
		t.Errorf("finding = %q, want %q", got, want)
	}
}
