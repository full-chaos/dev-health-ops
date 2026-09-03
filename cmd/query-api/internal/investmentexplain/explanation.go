package investmentexplain

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// The types below port api/models/schemas.py's API/cache-facing Pydantic
// models (InvestmentFindingEvidence, InvestmentFinding,
// InvestmentConfidence, InvestmentActionItem, InvestmentMixExplanation)
// -- DISTINCT from types.go's Finding/Confidence/ActionItem/
// InvestmentMixExplainOutput, which port investment_mix_types.py's
// PARSER-internal TypedDicts. Python keeps the same split: parse_result
// (investment_mix_types.InvestmentMixExplainOutput) is unpacked field by
// field into a FRESH schemas.InvestmentMixExplanation right after a
// successful parse (investment_mix_explain.py:497 on), never returned or
// cached directly. Both type families exist here for the identical
// reason.

// InvestmentMixFindingEvidence ports InvestmentFindingEvidence.
type InvestmentMixFindingEvidence struct {
	Theme               string   `json:"theme"`
	Subcategory         *string  `json:"subcategory"`
	SharePct            float64  `json:"share_pct"`
	DeltaPctPoints      *float64 `json:"delta_pct_points"`
	EvidenceQualityMean *float64 `json:"evidence_quality_mean"`
	EvidenceQualityBand *string  `json:"evidence_quality_band"`
}

// InvestmentMixFinding ports InvestmentFinding.
type InvestmentMixFinding struct {
	Finding  string                       `json:"finding"`
	Evidence InvestmentMixFindingEvidence `json:"evidence"`
}

// BandCount is one evidence_quality_band -> count pair.
type BandCount struct {
	Band  string
	Count int
}

// BandMix is Confidence.band_mix -- an ORDER-PRESERVING dict[str, int],
// because the orchestration layer's band_counts accumulator
// (explain.go) builds it in the order bands are first ENCOUNTERED across
// a request's work units (a real Python dict, not a fixed
// high/moderate/low/very_low/unknown order), and that same order must
// survive into the cached JSON row for a byte-exact write. UnmarshalJSON
// preserves the SOURCE document's key order too (walking json.Decoder's
// token stream, the same technique parseDistributionOrdered uses)
// rather than losing it through a map -- a cache-hit round-trip
// therefore reproduces the original write exactly, not just an
// equivalent one.
type BandMix []BandCount

func (m BandMix) Get(band string) (int, bool) {
	for _, bc := range m {
		if bc.Band == band {
			return bc.Count, true
		}
	}
	return 0, false
}

func (m *BandMix) UnmarshalJSON(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok || delim != '{' {
		// A non-object band_mix (e.g. a malformed cached row's "[]" or
		// "null") must be a decode ERROR, not a silent empty BandMix --
		// Python's InvestmentMixExplanation(**cached_data) is a Pydantic
		// model with band_mix: dict[str, int]
		// (api/models/schemas.py:266) and RAISES on exactly this shape,
		// which explain_investment_mix's cache-read catches and falls
		// through to recompute (investment_mix_explain.py:229/233).
		// DecodeInvestmentMixExplanation's caller (explain.go's cache-read
		// branch) already treats any decode error as a cache miss, so
		// returning an error here reproduces that same fall-through --
		// the previous silent-nil behavior instead served corrupted cache
		// data as if it were a valid empty band_mix. Caught by codex
		// round 2.
		return fmt.Errorf("investmentexplain: band_mix must be a JSON object, got %v", token)
	}
	var out BandMix
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return err
		}
		key, _ := keyToken.(string)
		var count int
		if err := decoder.Decode(&count); err != nil {
			return err
		}
		out = append(out, BandCount{Band: key, Count: count})
	}
	*m = out
	return nil
}

// InvestmentMixConfidence ports InvestmentConfidence.
type InvestmentMixConfidence struct {
	Level         string   `json:"level"`
	QualityMean   *float64 `json:"quality_mean"`
	QualityStddev *float64 `json:"quality_stddev"`
	BandMix       BandMix  `json:"band_mix"`
	Drivers       []string `json:"drivers"`
}

// InvestmentMixActionItem ports InvestmentActionItem.
type InvestmentMixActionItem struct {
	Action string `json:"action"`
	Why    string `json:"why"`
	Where  string `json:"where"`
}

// InvestmentMixExplanation ports api/models/schemas.py's
// InvestmentMixExplanation -- the type actually returned to the caller
// and cached (write_investment_explanation). Status covers every real
// outcome (valid/invalid_json/invalid_llm_output/llm_unavailable --
// forbidden_language reuses the invalid_llm_output fallback construction
// in explain_investment_mix, so it never appears here as its own
// literal, matching Python), unlike types.go's narrower ParseStatus (the
// strict parser's own four outcomes only).
type InvestmentMixExplanation struct {
	Summary         string                    `json:"summary"`
	TopFindings     []InvestmentMixFinding    `json:"top_findings"`
	Confidence      InvestmentMixConfidence   `json:"confidence"`
	WhatToCheckNext []InvestmentMixActionItem `json:"what_to_check_next"`
	AntiClaims      []string                  `json:"anti_claims"`
	Status          *string                   `json:"status"`
}

// DecodeInvestmentMixExplanation ports the cache-hit reconstruction:
//
//	cached_data = json.loads(cached.explanation_json)
//	return InvestmentMixExplanation(**cached_data)
func DecodeInvestmentMixExplanation(explanationJSON string) (InvestmentMixExplanation, error) {
	var explanation InvestmentMixExplanation
	err := json.Unmarshal([]byte(explanationJSON), &explanation)
	return explanation, err
}

// EncodeInvestmentMixExplanation ports the cache-write serialization
// exactly:
//
//	explanation_data = result.model_dump()
//	explanation_json = json.dumps(explanation_data)
//
// Pydantic's model_dump() (default mode="python") walks nested
// BaseModels into plain dicts in FIELD DECLARATION order, which
// json.dumps then writes in that same insertion order (no sort_keys) --
// exactly pythonparity.MarshalPythonJSONInsertionOrder's contract. Field
// order below mirrors each model's declaration order in schemas.py.
func EncodeInvestmentMixExplanation(explanation InvestmentMixExplanation) (string, error) {
	findings := make([]any, len(explanation.TopFindings))
	for i, f := range explanation.TopFindings {
		findings[i] = pythonparity.OrderedObject{
			{Key: "finding", Value: f.Finding},
			{Key: "evidence", Value: pythonparity.OrderedObject{
				{Key: "theme", Value: f.Evidence.Theme},
				{Key: "subcategory", Value: optionalStringToAny(f.Evidence.Subcategory)},
				{Key: "share_pct", Value: f.Evidence.SharePct},
				{Key: "delta_pct_points", Value: optionalFloatToAny(f.Evidence.DeltaPctPoints)},
				{Key: "evidence_quality_mean", Value: optionalFloatToAny(f.Evidence.EvidenceQualityMean)},
				{Key: "evidence_quality_band", Value: optionalStringToAny(f.Evidence.EvidenceQualityBand)},
			}},
		}
	}

	actions := make([]any, len(explanation.WhatToCheckNext))
	for i, a := range explanation.WhatToCheckNext {
		actions[i] = pythonparity.OrderedObject{
			{Key: "action", Value: a.Action},
			{Key: "why", Value: a.Why},
			{Key: "where", Value: a.Where},
		}
	}

	antiClaims := make([]any, len(explanation.AntiClaims))
	for i, c := range explanation.AntiClaims {
		antiClaims[i] = c
	}

	drivers := make([]any, len(explanation.Confidence.Drivers))
	for i, d := range explanation.Confidence.Drivers {
		drivers[i] = d
	}

	bandMix := make(pythonparity.OrderedObject, 0, len(explanation.Confidence.BandMix))
	for _, bc := range explanation.Confidence.BandMix {
		bandMix = append(bandMix, pythonparity.Member{Key: bc.Band, Value: bc.Count})
	}

	root := pythonparity.OrderedObject{
		{Key: "summary", Value: explanation.Summary},
		{Key: "top_findings", Value: findings},
		{Key: "confidence", Value: pythonparity.OrderedObject{
			{Key: "level", Value: explanation.Confidence.Level},
			{Key: "quality_mean", Value: optionalFloatToAny(explanation.Confidence.QualityMean)},
			{Key: "quality_stddev", Value: optionalFloatToAny(explanation.Confidence.QualityStddev)},
			{Key: "band_mix", Value: bandMix},
			{Key: "drivers", Value: drivers},
		}},
		{Key: "what_to_check_next", Value: actions},
		{Key: "anti_claims", Value: antiClaims},
		{Key: "status", Value: optionalStringToAny(explanation.Status)},
	}

	encoded, err := pythonparity.MarshalPythonJSONInsertionOrder(root)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func optionalFloatToAny(f *float64) any {
	if f == nil {
		return nil
	}
	return *f
}
