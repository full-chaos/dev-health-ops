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
type Confidence struct {
	Level         string // one of "high", "moderate", "low", "unknown"
	QualityMean   *float64
	QualityStddev *float64
	BandMix       map[string]int
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
