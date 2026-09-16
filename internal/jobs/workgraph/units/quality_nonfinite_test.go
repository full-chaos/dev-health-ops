package units

import (
	"math"
	"testing"
)

// TestNonFiniteEdgeConfidenceScoresNoStructuralCredit asserts a non-finite
// edge confidence is absent evidence, not full structural credit. A unit with
// two nodes, no text and no source-type agreement, whose only edge carries a
// NaN (or +/-Inf) confidence, scores 0.0 on the structural term -- the same
// as a unit with no edges at all, once the two nodes rule out
// GraphDensity's node_count<=1 special case that would otherwise make an
// empty edge set score 1.0 instead of 0.0.
func TestNonFiniteEdgeConfidenceScoresNoStructuralCredit(t *testing.T) {
	noText := map[string]map[string]string{"issue": {}, "pr": {}, "commit": {}}

	for _, testCase := range []struct {
		name       string
		confidence any
	}{
		{"nan", math.NaN()},
		{"positive_infinity", math.Inf(1)},
		{"negative_infinity", math.Inf(-1)},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			quality, rejected := ComputeEvidenceQuality(EvidenceQualityInput{
				SourceTexts: noText,
				NodesCount:  2,
				Confidences: []any{testCase.confidence},
			})

			if rejected != 1 {
				t.Errorf("rejected = %d, want 1", rejected)
			}
			// 0.4*0 (no text) + 0.3*0 (no agreement) + 0.3*0 (no structural credit).
			if quality != 0.0 {
				t.Errorf("quality = %v, want 0.0 -- a non-finite confidence must not "+
					"award structural credit", quality)
			}
			if band := EvidenceQualityBand(quality); band != "very_low" {
				t.Errorf("band = %q, want very_low", band)
			}
		})
	}
}

// TestNonFiniteEdgeConfidenceDoesNotInflateGraphDensity guards the other half
// of "absent": a rejected confidence must also not count as a structural edge
// for graph density, or an all-non-finite edge set would still score as a
// maximally dense graph.
func TestNonFiniteEdgeConfidenceDoesNotInflateGraphDensity(t *testing.T) {
	quality, rejected := ComputeEvidenceQuality(EvidenceQualityInput{
		SourceTexts: map[string]map[string]string{"issue": {}, "pr": {}, "commit": {}},
		NodesCount:  10,
		Confidences: []any{math.NaN(), math.Inf(1), math.Inf(-1)},
	})

	if rejected != 3 {
		t.Errorf("rejected = %d, want 3", rejected)
	}
	// GraphDensity(10, 0) is 0.0, not 1.0 -- ten nodes and zero usable edges is
	// sparse, and a rejected confidence must not be counted as an edge.
	if quality != 0.0 {
		t.Errorf("quality = %v, want 0.0", quality)
	}
}

// TestFiniteEdgeConfidenceIsUnaffectedByRejection is the control: a mix of
// finite and non-finite confidences keeps the finite ones' credit and rejects
// only the non-finite ones, rather than discarding the whole component.
func TestFiniteEdgeConfidenceIsUnaffectedByRejection(t *testing.T) {
	withNonFinite, rejectedWith := ComputeEvidenceQuality(EvidenceQualityInput{
		NodesCount:  2,
		Confidences: []any{1.0, math.NaN()},
	})
	withoutNonFinite, rejectedWithout := ComputeEvidenceQuality(EvidenceQualityInput{
		NodesCount:  2,
		Confidences: []any{1.0},
	})

	if rejectedWith != 1 {
		t.Errorf("rejected = %d, want 1", rejectedWith)
	}
	if rejectedWithout != 0 {
		t.Errorf("control: rejected = %d, want 0", rejectedWithout)
	}
	if withNonFinite != withoutNonFinite {
		t.Errorf("quality with a rejected NaN edge alongside a finite one = %v, "+
			"want the same as the finite edge alone (%v)", withNonFinite, withoutNonFinite)
	}
}
