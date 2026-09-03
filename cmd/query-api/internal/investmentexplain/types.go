package investmentexplain

// FindingEvidence ports investment_mix_types.py's FindingEvidence TypedDict.
type FindingEvidence struct {
	Theme               string
	Subcategory         *string
	SharePct            float64
	DeltaPctPoints      *float64
	EvidenceQualityMean *float64
	EvidenceQualityBand *string
}

// Finding ports investment_mix_types.py's Finding TypedDict.
type Finding struct {
	Finding  string
	Evidence FindingEvidence
}

// Confidence ports investment_mix_types.py's Confidence TypedDict.
//
// BandMix is the order-preserving type from explanation.go, not
// map[string]int: parse_investment_mix_response's own confidence.band_mix
// (investment_mix_parser.py:119, "band_mix": fallback_band_mix or {}) is
// ALWAYS the caller-supplied fallback_band_mix, a real Python dict built
// by explain_investment_mix's own first-encounter accumulation
// (investment_mix_explain.py:278) -- never anything read back out of the
// LLM's own raw JSON text, in EITHER the valid or invalid_llm_output
// outcome. A map[string]int here would lose that order the moment
// explain.go's caller builds it, and a fixed re-imposed order (the bug
// codex round 1 caught, P1) is the only way that loss can even be
// partially papered over downstream -- keeping this order-preserving
// end to end removes the need for that repair entirely.
type Confidence struct {
	Level         string // one of "high", "moderate", "low", "unknown"
	QualityMean   *float64
	QualityStddev *float64
	BandMix       BandMix
	Drivers       []string
}

// ActionItem ports investment_mix_types.py's ActionItem TypedDict.
type ActionItem struct {
	Action string
	Why    string
	Where  string
}

// ParseStatus ports investment_mix_types.py's ParseStatus Literal.
//
// "llm_unavailable" is part of InvestmentMixExplainOutput's own status
// field in Python but is NEVER a value parse_investment_mix_response
// returns (explain_investment_mix sets it directly, bypassing the parser
// entirely, when there is no LLM provider to call at all) -- omitted here
// since this file only ports the parser's four real outcomes.
type ParseStatus string

const (
	ParseStatusValid             ParseStatus = "valid"
	ParseStatusInvalidJSON       ParseStatus = "invalid_json"
	ParseStatusInvalidLLMOutput  ParseStatus = "invalid_llm_output"
	ParseStatusForbiddenLanguage ParseStatus = "forbidden_language"
)

// InvestmentMixExplainOutput ports investment_mix_types.py's
// InvestmentMixExplainOutput TypedDict, restricted to the "valid" shape
// the parser actually constructs (status is always ParseStatusValid on
// this struct; Python's broader Optional[status] union belongs to the
// orchestration layer that also handles llm_unavailable/invalid_llm_output
// fallback construction, not this parser).
type InvestmentMixExplainOutput struct {
	Summary         string
	TopFindings     []Finding
	Confidence      Confidence
	WhatToCheckNext []ActionItem
	AntiClaims      []string
}

// ParseResult ports investment_mix_types.py's InvestmentMixParseResult
// dataclass.
type ParseResult struct {
	Status ParseStatus
	Output *InvestmentMixExplainOutput
}
