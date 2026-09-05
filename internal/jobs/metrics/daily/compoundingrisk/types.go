// Package compoundingrisk is the native Go port of the Python
// `compounding_risk` daily metrics family (CHAOS-4287), whose producer lives
// inline in the daily-job monolith rather than in its own module:
// `_write_compounding_risk_for_day` (src/dev_health_ops/metrics/job_daily.py:568)
// over the pure kernel in src/dev_health_ops/metrics/compounding_risk.py.
//
// # Scope: REPO rows only
//
// Python emits compounding-risk rows at TWO call sites and this package ports
// only the first:
//
//   - REPO scope, once per partition, job_daily.py:1955. Ported here.
//   - TEAM scope, once per org/day, from run_daily_metrics_finalize
//     (job_daily.py:2235 -> _write_compounding_risk_team_rows_for_day:613).
//     NOT ported: FinalizeHandler.Work hands the whole finalize step to the
//     Python bridge as one opaque compatibility.Finalize call, with no
//     per-family registration and no skip-list, so no family can be carved out
//     of finalize the way skipFamiliesForBridge carves families out of the
//     partition bridge. Team rows stay Python until that hook exists;
//     CHAOS-4287 stays open until then.
//
// The team split itself is deliberate (CHAOS-4264): this family runs once per
// REPO, so a team row written per partition sees only that one repo's inputs,
// and argMax(computed_at) dedup then keeps whichever repo's partition happened
// to run last -- silently dropping every other contributing repo.
//
// # Write mode
//
// compounding_risk_daily is a PLAIN MergeTree
// (migrations/clickhouse/040_compounding_risk_daily.sql:19-59), ORDER BY
// (org_id, scope, scope_id, day, computed_at), with NO version column. It is
// append-only: re-running a day duplicates rows physically, and readers are
// expected to argMax(<col>, computed_at) GROUP BY (org_id, scope, scope_id,
// day) themselves. The record docstring's "argMax dedup" is that READ
// convention, not an engine property -- do not model this table as a
// ReplacingMergeTree.
package compoundingrisk

import "time"

// Weights are the four component weights in force at compute time. They are
// persisted on every row so historical rows stay auditable if the defaults
// change (compounding_risk.py:53-66).
type Weights struct {
	Churn      float64
	Complexity float64
	Ownership  float64
	Review     float64
}

// Thresholds are the severity bucket boundaries, inclusive at the lower edge
// (compounding_risk.py:69-87).
type Thresholds struct {
	Elevated float64
	High     float64
}

// References are the normalization reference values (compounding_risk.py:46-50).
type References struct {
	Churn      float64
	Complexity float64
	Review     float64
}

// DefaultWeights mirrors compounding_risk.py's CompoundingWeights defaults.
var DefaultWeights = Weights{Churn: 0.30, Complexity: 0.30, Ownership: 0.20, Review: 0.20}

// DefaultThresholds mirrors CompoundingThresholds defaults.
var DefaultThresholds = Thresholds{Elevated: 0.40, High: 0.65}

// DefaultReferences mirrors REFERENCE_VALUES.
var DefaultReferences = References{Churn: 0.30, Complexity: 0.20, Review: 48.0}

// Severity bucket names, matching the compounding_risk_daily Enum8 exactly.
const (
	SeverityUnknown  = "unknown"
	SeverityLow      = "low"
	SeverityElevated = "elevated"
	SeverityHigh     = "high"
)

// ScopeRepo is the only scope this package emits. See the package doc comment
// for why "team" is absent.
const ScopeRepo = "repo"

// Inputs are the raw signals consumed by the composite
// (compounding_risk.py:99-128). A nil pointer is Python's None: data
// unavailable is NOT zero risk, it blocks the score.
type Inputs struct {
	ReworkChurn       *float64
	ComplexityDelta   *float64
	ReviewLatencyP90H *float64
	// SingleOwnerRatio and OwnershipGini feed one component via
	// max(single_owner_ratio, gini). Either alone is acceptable; both nil
	// blocks the ownership component and therefore the composite.
	SingleOwnerRatio *float64
	OwnershipGini    *float64
	// BusFactor is pure metadata, surfaced for inspectability and never part
	// of the formula.
	BusFactor *float64
}

// HasRequired ports CompoundingInputs.has_required_inputs
// (compounding_risk.py:119-128), including its exact short-circuit order.
func (inputs Inputs) HasRequired() bool {
	if inputs.ReworkChurn == nil {
		return false
	}
	if inputs.ComplexityDelta == nil {
		return false
	}
	if inputs.ReviewLatencyP90H == nil {
		return false
	}
	if inputs.SingleOwnerRatio == nil && inputs.OwnershipGini == nil {
		return false
	}
	return true
}

// RepoMetricsRow is one repo's slice of repo_metrics_daily, already
// argMax(computed_at)-deduplicated by the loader. It mirrors the five
// attributes build_compounding_risk_rows_for_day reads off its
// repo_metrics_rows via getattr (compounding_risk.py:507-516).
type RepoMetricsRow struct {
	RepoID                  string
	ReworkChurnRatio30D     *float64
	SingleOwnerFileRatio30D *float64
	CodeOwnershipGini       *float64
	BusFactor               *float64
	PRFirstReviewP90Hours   *float64
}

// Record is one compounding_risk_daily row, field-for-field
// CompoundingRiskDailyRecord (schemas.py:715-754). Column order on the wire is
// the writer's business, not this struct's.
type Record struct {
	OrgID   string
	Day     time.Time
	Scope   string
	ScopeID string

	// CompoundingRisk is nil (severity "unknown") when any required input is
	// missing. The row is still emitted so absence-of-signal is itself
	// inspectable.
	CompoundingRisk *float64
	Severity        string

	ChurnNorm      *float64
	ComplexityNorm *float64
	OwnershipNorm  *float64
	ReviewNorm     *float64

	ReworkChurn       *float64
	ComplexityDelta   *float64
	BusFactor         *float64
	OwnershipGini     *float64
	SingleOwnerRatio  *float64
	ReviewLatencyP90H *float64

	WChurn      float64
	WComplexity float64
	WOwnership  float64
	WReview     float64

	ThresholdElevated float64
	ThresholdHigh     float64

	ComputedAt time.Time
}
