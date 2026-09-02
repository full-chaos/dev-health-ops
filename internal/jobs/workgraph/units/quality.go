package units

// This file ports the arithmetic plane of work_graph/investment/evidence.py
// and the two helpers it borrows from utils/normalization.py.
//
// It is short, and almost every line of it is a place where the obvious Go
// spelling disagrees with Python under non-finite input. See Clamp.

// pythonMin is Python's two-argument `min`.
//
// Python returns `b if b < a else a`. Every comparison against NaN is False,
// so `min(a, nan)` returns **a**, the first argument -- it does NOT propagate.
// Go's `math.Min` propagates NaN instead. The two disagree on exactly the
// inputs that matter here.
func pythonMin(a, b float64) float64 {
	if b < a {
		return b
	}
	return a
}

// pythonMax is Python's two-argument `max`: `b if b > a else a`, so
// `max(a, nan)` also returns **a**. Go's `math.Max` propagates NaN.
func pythonMax(a, b float64) float64 {
	if b > a {
		return b
	}
	return a
}

// Clamp is utils.normalization.clamp: `max(low, min(high, value))`.
//
// # THE NESTING IS LOAD-BEARING AND THE RESULT IS SURPRISING
//
// Because Python's min/max return their FIRST argument when the comparison is
// False, and every comparison with NaN is False:
//
//	min(1.0, nan) -> 1.0    then    max(0.0, 1.0) -> 1.0
//
// so Clamp(NaN) is **1.0** -- the HIGH bound. Written the other way round,
// `min(high, max(low, value))`, it would be 0.0. The order is not a stylistic
// choice; it decides where NaN lands.
//
// A naive Go port using math.Min/math.Max returns NaN from both and therefore
// from Clamp, which then propagates into the stored evidence_quality column.
//
// # WHAT THIS MEANS IN PRODUCTION, WHICH IS WORSE THAN A ROUNDING DIFFERENCE
//
// compute_evidence_quality feeds edge confidences through here. A NaN
// confidence -- reachable, because `confidence` is an unconstrained Float32
// with no finite-value guard on the writer side -- makes structural_density
// clamp to 1.0, i.e. FULL structural credit. Measured end to end: a unit with
// no text, no agreement and a single NaN-confidence edge scores 0.3, entirely
// from the structural term, rather than 0.0.
//
// That is Python awarding maximum structural evidence to unusable data. It is
// reproduced here because parity is the contract, not because it is right; it
// is called out so that if it is ever fixed, it is fixed deliberately and in
// BOTH planes at once.
func Clamp(value, low, high float64) float64 {
	return pythonMax(low, pythonMin(high, value))
}

// ClampUnit is clamp() with Python's default bounds of 0.0 and 1.0.
func ClampUnit(value float64) float64 {
	return Clamp(value, 0.0, 1.0)
}

// EvidenceQualityBand is utils.normalization.evidence_quality_band.
//
// Unlike Clamp, this one needs no special handling: every `>=` against NaN is
// False in Go exactly as in Python, so NaN falls through to "very_low" in both.
// It is still pinned by the corpus, because the COMPOSITION is what ships --
// in the pipeline Clamp runs first, so a NaN quality reaches this function as
// 1.0 and is banded "high", never "very_low".
func EvidenceQualityBand(value float64) string {
	switch {
	case value >= 0.8:
		return "high"
	case value >= 0.6:
		return "moderate"
	case value >= 0.4:
		return "low"
	default:
		return "very_low"
	}
}

// GraphDensity is evidence._graph_density.
//
// The `node_count <= 1` branch returns 1.0, not 0.0: a single node (or none) is
// treated as maximally dense rather than as empty. That is deliberate in the
// source and it matters, because it is the common case for a work unit built
// from one issue.
func GraphDensity(nodeCount, edgeCount int) float64 {
	if nodeCount <= 1 {
		return 1.0
	}
	possible := float64(nodeCount) * float64(nodeCount-1) / 2.0
	if possible <= 0 {
		return 0.0
	}
	// Python's `min(1.0, edge_count / possible)`, so pythonMin rather than
	// math.Min. possible is >= 1 whenever nodeCount >= 2, so the quotient is
	// finite here; pythonMin is used anyway to keep the mapping to the source
	// exact rather than relying on that reasoning holding after an edit.
	return pythonMin(1.0, float64(edgeCount)/possible)
}

// MeanEdgeConfidence is evidence._edge_confidence -- note that this is a
// DIFFERENT function from components._edge_confidence, despite the identical
// name.
//
//	components._edge_confidence(edge)   -> one edge's coerced confidence
//	evidence._edge_confidence(edges)    -> the MEAN over many edges
//
// The per-value coercion inside this one (_float_value) is a second, separate
// copy of components._edge_confidence's body. They are currently identical, so
// this reuses ConfidenceFromValue rather than duplicating a third copy in Go --
// but that reuse is only valid while the two Python copies agree, and nothing
// in Python enforces that. The agreement is therefore recorded in the fixture
// and checked by the rot guard; if it ever breaks, this must be split into its
// own coercion rather than silently absorbing the divergence.
//
// An empty slice returns 0.0, NOT NaN: Python guards with `if not values`
// before dividing, so there is no 0/0 here.
func MeanEdgeConfidence(confidences []any) float64 {
	if len(confidences) == 0 {
		return 0.0
	}
	total := 0.0
	for _, value := range confidences {
		total += ConfidenceFromValue(value)
	}
	return total / float64(len(confidences))
}

// EvidenceQualityInput carries what compute_evidence_quality reads.
type EvidenceQualityInput struct {
	// TextSourceCount and TextCharCount come from the TextBundle.
	TextSourceCount int
	TextCharCount   int
	// SourceTexts is needed for the agreement term, which counts how many
	// source types have a NON-EMPTY MAP -- not how many have non-empty text.
	SourceTexts map[string]map[string]string
	NodesCount  int
	// Confidences is the `confidence` value of each edge, in any order; the
	// mean is order-independent up to floating-point associativity, and the
	// corpus pins the order the caller supplies.
	Confidences []any
}

// ComputeEvidenceQuality is evidence.compute_evidence_quality:
//
//	0.4 * text + 0.3 * agreement + 0.3 * structural
//
// Every intermediate is clamped, and then the total is clamped again -- so the
// weights summing to 1.0 is not what keeps the result in range, and changing
// one weight does not silently produce an out-of-range score.
func ComputeEvidenceQuality(input EvidenceQualityInput) float64 {
	textPresence := ClampUnit(float64(input.TextSourceCount) / 3.0)
	textRichness := ClampUnit(float64(input.TextCharCount) / 1200.0)
	textScore := ClampUnit((textPresence + textRichness) / 2.0)

	// `len([key for key, texts in source_texts.items() if texts])` -- the
	// predicate is the DICT's truthiness. A source type holding one entry whose
	// text is the empty string still counts, because the dict is non-empty.
	// Testing the texts instead would be the obvious reading and is wrong.
	sourceTypeCount := 0
	for _, texts := range input.SourceTexts {
		if len(texts) > 0 {
			sourceTypeCount++
		}
	}
	agreement := sourceTypeCount - 1
	if agreement < 0 {
		// Python's `max(0, source_type_count - 1)` on ints.
		agreement = 0
	}
	agreementScore := ClampUnit(float64(agreement) / 2.0)

	density := GraphDensity(input.NodesCount, len(input.Confidences))
	confidence := MeanEdgeConfidence(input.Confidences)
	structuralDensity := ClampUnit((density + confidence) / 2.0)

	// The float64() conversions are REQUIRED and must not be tidied away.
	//
	// Go's spec permits an implementation to fuse `x*y + z` into a single
	// fused-multiply-add, which rounds ONCE where Python rounds twice. arm64
	// does this; amd64 typically does not. Measured on this exact case
	// (text=0.25, agreement=0, structural=0.75):
	//
	//	bare  0.4*t + 0.3*a + 0.3*s  -> 0.325               bits ...cccd  (fused)
	//	split float64(0.4*t) + ...   -> 0.32499999999999996 bits ...cccc  (Python)
	//
	// One ULP, on 77 of the corpus's 2000 cases. The reason it matters more
	// than its size suggests: it is ARCHITECTURE-DEPENDENT. Without the
	// conversions this code is correct on amd64 CI and wrong on arm64, so the
	// defect ships past a green build and appears only on some workers. The
	// spec guarantees that an explicit float64() conversion rounds, which is
	// what blocks the fusion.
	//
	// evidence_quality is a stored column and feeds evidence_quality_band, so a
	// last-bit difference can cross a band boundary (0.4 / 0.6 / 0.8) and change
	// a categorical output, not just a decimal.
	value := float64(0.4*textScore) +
		float64(0.3*agreementScore) +
		float64(0.3*structuralDensity)
	return ClampUnit(value)
}
