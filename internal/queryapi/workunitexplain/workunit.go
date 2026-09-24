package workunitexplain

import (
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/investmentexplain"
)

// ThemeWeight is one entry of a work unit's investment vector, in the order
// the precomputed view stored it.
type ThemeWeight struct {
	Key   string
	Value float64
}

// Evidence is api/models/schemas.py's WorkUnitEvidence. Each entry is a
// loosely-shaped map because the entries are heterogeneous by design -- a
// time_range entry carries different keys than a repo_scope one.
type Evidence struct {
	Textual    []map[string]any
	Structural []map[string]any
	Contextual []map[string]any
}

// WorkUnit is the precomputed investment view this package explains: the
// exact subset of api/models/schemas.py's WorkUnitInvestment that
// extract_allowed_inputs and _parse_llm_response read, and nothing else.
//
// This is the allowed-input boundary stated as a type. The wider record
// BuildWorkUnitInvestments assembles also carries effort, work-unit
// type/name and the subcategory vector, none of which any prompt or parser
// on either plane ever touches -- so they are absent here rather than
// present and unused, which makes "only allowed inputs reach the model" a
// property a reader can check by looking at the struct.
type WorkUnit struct {
	WorkUnitID     string
	TimeRangeStart time.Time
	TimeRangeEnd   time.Time
	// Themes is the theme-level vector only. extract_allowed_inputs passes
	// `investment.investment.themes` and never the subcategory map.
	Themes []ThemeWeight
	// EvidenceQualityValue and EvidenceQualityBand are nullable on both
	// planes, and the two nulls behave differently downstream -- see
	// extractEvidenceQualityLimits -- so neither is flattened here.
	EvidenceQualityValue *float64
	EvidenceQualityBand  *string
	Evidence             Evidence
}

// FromWorkUnitInvestment narrows one assembled investment record to the
// values this package is allowed to see.
func FromWorkUnitInvestment(investment investmentexplain.WorkUnitInvestment) WorkUnit {
	themes := make([]ThemeWeight, 0, len(investment.Investment.Themes))
	for _, theme := range investment.Investment.Themes {
		themes = append(themes, ThemeWeight{Key: theme.Key, Value: theme.Value})
	}
	return WorkUnit{
		WorkUnitID:           investment.WorkUnitID,
		TimeRangeStart:       investment.TimeRange.Start,
		TimeRangeEnd:         investment.TimeRange.End,
		Themes:               themes,
		EvidenceQualityValue: investment.EvidenceQuality.Value,
		EvidenceQualityBand:  investment.EvidenceQuality.Band,
		Evidence: Evidence{
			Textual:    investment.Evidence.Textual,
			Structural: investment.Evidence.Structural,
			Contextual: investment.Evidence.Contextual,
		},
	}
}
