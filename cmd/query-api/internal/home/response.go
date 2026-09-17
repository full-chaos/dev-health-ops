// Package home is the Go port of GET+POST /api/v1/home
// (api/services/home.py's build_home_response and every ClickHouse/
// Postgres reader it calls).
//
// This package's own response builder (BuildResponse) is exported: a
// future GET /api/v1/opportunities port -- which calls the same
// underlying builder in Python -- can call it with already-resolved
// filters without this package building opportunities itself.
//
// DEDUP (same convention as sankey/quadrant/heatmap): every
// ReplacingMergeTree table this package reads is read FINAL (or, where
// the natural key needs an aggregate collapse first, argMax(tuple(cols),
// version).1), org_id filtered inside the SAME top-level statement as
// the dedup source, never a separate unfiltered subquery.
// api/services/home.py and the query modules it calls read several of
// these tables with no dedup at all -- each such gap is a declared
// Python-plane defect, not replicated here; see this package's
// queries_freshness.go/queries_signals.go doc comments and
// internal/goapiproof/home_corpus.go for the specific citations.
package home

import "time"

// Coverage is the wire shape of Coverage (schemas.py:9-12).
type Coverage struct {
	ReposCoveredPct          float64 `json:"repos_covered_pct"`
	PRsLinkedToIssuesPct     float64 `json:"prs_linked_to_issues_pct"`
	IssuesWithCycleStatesPct float64 `json:"issues_with_cycle_states_pct"`
}

// Freshness is the wire shape of Freshness (schemas.py:15-19).
type Freshness struct {
	LastIngestedAt         *time.Time        `json:"last_ingested_at"`
	LatestSuccessfulSyncAt *time.Time        `json:"latest_successful_sync_at"`
	Sources                map[string]string `json:"sources"`
	Coverage               Coverage          `json:"coverage"`
}

// SparkPoint is the wire shape of SparkPoint (schemas.py:22-24).
type SparkPoint struct {
	TS    NaiveDateTime `json:"ts"`
	Value float64       `json:"value"`
}

// MetricDelta is the wire shape of MetricDelta (schemas.py:27-33).
type MetricDelta struct {
	Metric   string       `json:"metric"`
	Label    string       `json:"label"`
	Value    float64      `json:"value"`
	Unit     string       `json:"unit"`
	DeltaPct float64      `json:"delta_pct"`
	Spark    []SparkPoint `json:"spark"`
}

// ReworkThemeAllocation is the wire shape of ReworkThemeAllocation
// (schemas.py:36-42).
type ReworkThemeAllocation struct {
	Theme         string  `json:"theme"`
	Label         string  `json:"label"`
	Allocation    float64 `json:"allocation"`
	AllocationPct float64 `json:"allocation_pct"`
	PRsMerged     int64   `json:"prs_merged"`
	ChurnLOC      int64   `json:"churn_loc"`
}

// SummarySentence is the wire shape of SummarySentence (schemas.py:45-48).
type SummarySentence struct {
	ID           string `json:"id"`
	Text         string `json:"text"`
	EvidenceLink string `json:"evidence_link"`
}

// ConstraintEvidence is the wire shape of ConstraintEvidence
// (schemas.py:51-53).
type ConstraintEvidence struct {
	Label string `json:"label"`
	Link  string `json:"link"`
}

// ConstraintCard is the wire shape of ConstraintCard (schemas.py:56-60).
type ConstraintCard struct {
	Title       string               `json:"title"`
	Claim       string               `json:"claim"`
	Evidence    []ConstraintEvidence `json:"evidence"`
	Experiments []string             `json:"experiments"`
}

// EventItem is the wire shape of EventItem (schemas.py:63-67).
type EventItem struct {
	TS   time.Time `json:"ts"`
	Type string    `json:"type"`
	Text string    `json:"text"`
	Link string    `json:"link"`
}

// ScopeEntityRef is the wire shape of ScopeEntityRef (schemas.py:70-78).
type ScopeEntityRef struct {
	ID          string `json:"id"`
	DisplayName string `json:"display_name"`
}

// HealthState is the wire shape of HomeHealthState (schemas.py:81-85).
type HealthState struct {
	Status   string     `json:"status"`
	Headline string     `json:"headline"`
	Summary  string     `json:"summary"`
	AsOf     *time.Time `json:"as_of"`
}

// Signal is the wire shape of HomeSignal (schemas.py:88-106).
type Signal struct {
	ID                string          `json:"id"`
	Title             string          `json:"title"`
	Metric            string          `json:"metric"`
	CurrentValue      string          `json:"current_value"`
	PriorValue        *string         `json:"prior_value"`
	Delta             *string         `json:"delta"`
	Direction         string          `json:"direction"`
	Severity          string          `json:"severity"`
	Confidence        string          `json:"confidence"`
	AffectedScope     string          `json:"affected_scope"`
	EvidenceCount     int             `json:"evidence_count"`
	WhyItMatters      string          `json:"why_it_matters"`
	RecommendedAction string          `json:"recommended_action"`
	EvidenceRef       *string         `json:"evidence_ref"`
	Category          string          `json:"category"`
	ScopeEntity       *ScopeEntityRef `json:"scope_entity"`
}

// LimitingFactor is the wire shape of HomeLimitingFactor
// (schemas.py:109-116).
type LimitingFactor struct {
	Claim             string  `json:"claim"`
	WhyItMatters      string  `json:"why_it_matters"`
	RecommendedAction string  `json:"recommended_action"`
	Confidence        string  `json:"confidence"`
	EvidenceRef       *string `json:"evidence_ref"`
}

// DefaultLimitingFactor ports HomeLimitingFactor's own field defaults
// (schemas.py:109-116) -- the value build_limiting_factor returns for an
// empty signals list (services/home.py:566-568).
func DefaultLimitingFactor() LimitingFactor {
	return LimitingFactor{
		Claim:             "No single limiting factor appears dominant.",
		WhyItMatters:      "The cockpit should keep watching directional changes as more evidence arrives.",
		RecommendedAction: "Review the ranked signals and confirm data coverage before changing operating priorities.",
		Confidence:        "low",
	}
}

// DataConfidence is the wire shape of HomeDataConfidence
// (schemas.py:119-124).
type DataConfidence struct {
	Level            string   `json:"level"`
	CoveragePct      *float64 `json:"coverage_pct"`
	ConnectedSources []string `json:"connected_sources"`
	MissingSources   []string `json:"missing_sources"`
	Caveats          []string `json:"caveats"`
}

// Response is the wire shape of HomeResponse (schemas.py:127-138).
type Response struct {
	Freshness             Freshness               `json:"freshness"`
	Deltas                []MetricDelta           `json:"deltas"`
	ReworkThemeAllocation []ReworkThemeAllocation `json:"rework_theme_allocation"`
	Summary               []SummarySentence       `json:"summary"`
	Tiles                 map[string]Tile         `json:"tiles"`
	Constraint            ConstraintCard          `json:"constraint"`
	Events                []EventItem             `json:"events"`
	HealthState           HealthState             `json:"health_state"`
	Signals               []Signal                `json:"signals"`
	LimitingFactor        LimitingFactor          `json:"limiting_factor"`
	DataConfidence        DataConfidence          `json:"data_confidence"`
}

// Tile is one entry of HomeResponse.tiles (services/home.py:1187-1208) --
// Python types this as dict[str, Any] but every entry it ever
// constructs has exactly these three string fields.
type Tile struct {
	Title    string `json:"title"`
	Subtitle string `json:"subtitle"`
	Link     string `json:"link"`
}

// RequestError carries an HTTP status the way Python's HTTPException
// does, matching quadrant.RequestError's own shape/convention.
type RequestError struct {
	Status  int
	Message string
}

func (e *RequestError) Error() string { return e.Message }
