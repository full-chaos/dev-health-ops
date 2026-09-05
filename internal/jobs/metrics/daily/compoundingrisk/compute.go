package compoundingrisk

import (
	"time"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// pythonMax reproduces CPython's two-argument builtin `max(a, b)`.
//
// IT IS NOT math.Max. CPython's max keeps the first argument and replaces it
// only when `b > a` is true, so a NaN operand is SWALLOWED rather than
// propagated:
//
//	Python: max(0.0, float("nan")) -> 0.0     Go: math.Max(0, NaN) -> NaN
//	Python: max(float("nan"), 1.0) -> nan     Go: math.Max(NaN, 1) -> NaN
//
// Every `max(...)` in compounding_risk.py is this shape -- the two clamps in
// _normalize_churn/_normalize_complexity/_normalize_review
// (compounding_risk.py:248,255,272), the ownership concentration norm (:266),
// and the complexity-delta denominator floor (:465). repo_metrics_daily
// columns are Nullable(Float64) and a NaN is reachable on the wire, so the
// asymmetry is a real parity surface, not a theoretical one.
func pythonMax(a, b float64) float64 {
	if b > a {
		return b
	}
	return a
}

// clamp01 ports _clamp01 (compounding_risk.py:237-242) branch-for-branch.
// NaN fails both comparisons and falls through unchanged, exactly as Python's
// does -- do not "simplify" this to math.Min/math.Max.
func clamp01(value float64) float64 {
	if value < 0.0 {
		return 0.0
	}
	if value > 1.0 {
		return 1.0
	}
	return value
}

// normalizeAgainstReference ports _normalize_churn / _normalize_complexity /
// _normalize_review (compounding_risk.py:245-272), which are the same function
// three times: clamp01(max(0.0, x) / ref). Falling complexity is not risk,
// which is why the negative clamp precedes the divide rather than following it.
func normalizeAgainstReference(value *float64, reference float64) *float64 {
	if value == nil {
		return nil
	}
	normalized := clamp01(pythonMax(0.0, *value) / reference)
	return &normalized
}

// normalizeOwnership ports _normalize_ownership (compounding_risk.py:258-266):
// the concentration norm is max(single_owner_ratio, gini) over whichever of
// the two are present, already in [0,1]. Python builds the candidate list in
// (single_owner_ratio, gini) order and folds max over it left to right, so the
// tie-break order is fixed here too.
func normalizeOwnership(singleOwnerRatio, ownershipGini *float64) *float64 {
	var candidates []float64
	if singleOwnerRatio != nil {
		candidates = append(candidates, *singleOwnerRatio)
	}
	if ownershipGini != nil {
		candidates = append(candidates, *ownershipGini)
	}
	if len(candidates) == 0 {
		return nil
	}
	highest := candidates[0]
	for _, candidate := range candidates[1:] {
		highest = pythonMax(highest, candidate)
	}
	normalized := clamp01(highest)
	return &normalized
}

// SeverityFor ports severity_for (compounding_risk.py:275-285). The comparisons
// are inclusive at the lower edge and evaluated high-first, so a score exactly
// on a boundary lands in the HIGHER bucket. A NaN score fails both `>=` and
// reads as "low" -- Python's behaviour, mirrored deliberately.
func SeverityFor(score *float64, thresholds Thresholds) string {
	if score == nil {
		return SeverityUnknown
	}
	if *score >= thresholds.High {
		return SeverityHigh
	}
	if *score >= thresholds.Elevated {
		return SeverityElevated
	}
	return SeverityLow
}

// Compute ports compute_compounding_risk (compounding_risk.py:319-402) for the
// REPO scope. It ALWAYS returns a row: when a required input is missing the
// score is nil and the severity is "unknown", and the row is still persisted
// so absence-of-signal stays inspectable.
//
// This signature is unchanged by CHAOS-5084's team-scope addition -- every
// existing repo-scope call site (ComputeForRepos, and every test that calls
// Compute directly) keeps working untouched. See ComputeTeam for the
// TEAM-scope sibling; both delegate to computeScored so a formula fix can
// never diverge between the two scopes.
func Compute(
	day time.Time,
	scopeID string,
	orgID string,
	inputs Inputs,
	computedAt time.Time,
	weights Weights,
	thresholds Thresholds,
	references References,
) Record {
	return computeScored(day, ScopeRepo, scopeID, orgID, inputs, computedAt, weights, thresholds, references)
}

// ComputeTeam is compute_compounding_risk called with scope="team"
// (compounding_risk.py:591-599, inside _build_team_rows) -- CHAOS-5084's
// TEAM-scope sibling of Compute. Identical formula; only the scope tag on the
// persisted row differs. See the package doc comment for why team rows are
// computed once per org/day at finalize time rather than per partition.
func ComputeTeam(
	day time.Time,
	teamID string,
	orgID string,
	inputs Inputs,
	computedAt time.Time,
	weights Weights,
	thresholds Thresholds,
	references References,
) Record {
	return computeScored(day, ScopeTeam, teamID, orgID, inputs, computedAt, weights, thresholds, references)
}

// computeScored is the scope-parametric core both Compute and ComputeTeam
// delegate to.
func computeScored(
	day time.Time,
	scope string,
	scopeID string,
	orgID string,
	inputs Inputs,
	computedAt time.Time,
	weights Weights,
	thresholds Thresholds,
	references References,
) Record {
	churnNorm := normalizeAgainstReference(inputs.ReworkChurn, references.Churn)
	complexityNorm := normalizeAgainstReference(inputs.ComplexityDelta, references.Complexity)
	ownershipNorm := normalizeOwnership(inputs.SingleOwnerRatio, inputs.OwnershipGini)
	reviewNorm := normalizeAgainstReference(inputs.ReviewLatencyP90H, references.Review)

	var score *float64
	if inputs.HasRequired() {
		// The required-input gate proves all four components are non-nil here,
		// mirroring Python's four asserts at compounding_risk.py:362-365.
		//
		// FMA BARRIERS (CHAOS-4818 class). Python evaluates this weighted sum
		// as four separately-rounded products folded left to right by three
		// separately-rounded adds. Go is free to contract `sum + w*n` into a
		// single arm64 FMA, which rounds ONCE and can differ in the last bit --
		// and this value is then compared against 0.40/0.65, so one ulp can
		// move a repo between severity buckets. Assigning to a variable does
		// NOT stop contraction; only an explicit float64 conversion does (Go
		// spec, "Floating-point operators"), so every product and every partial
		// sum carries one.
		churnTerm := float64(weights.Churn * *churnNorm)
		complexityTerm := float64(weights.Complexity * *complexityNorm)
		ownershipTerm := float64(weights.Ownership * *ownershipNorm)
		reviewTerm := float64(weights.Review * *reviewNorm)

		partial := float64(churnTerm + complexityTerm)
		partial = float64(partial + ownershipTerm)
		partial = float64(partial + reviewTerm)

		// Floating-point housekeeping: snap to [0, 1] in case of drift
		// (compounding_risk.py:373-374).
		clamped := clamp01(partial)
		score = &clamped
	}

	return Record{
		OrgID:   orgID,
		Day:     day,
		Scope:   scope,
		ScopeID: scopeID,

		CompoundingRisk: score,
		Severity:        SeverityFor(score, thresholds),

		ChurnNorm:      churnNorm,
		ComplexityNorm: complexityNorm,
		OwnershipNorm:  ownershipNorm,
		ReviewNorm:     reviewNorm,

		ReworkChurn:       inputs.ReworkChurn,
		ComplexityDelta:   inputs.ComplexityDelta,
		BusFactor:         inputs.BusFactor,
		OwnershipGini:     inputs.OwnershipGini,
		SingleOwnerRatio:  inputs.SingleOwnerRatio,
		ReviewLatencyP90H: inputs.ReviewLatencyP90H,

		WChurn:      weights.Churn,
		WComplexity: weights.Complexity,
		WOwnership:  weights.Ownership,
		WReview:     weights.Review,

		ThresholdElevated: thresholds.Elevated,
		ThresholdHigh:     thresholds.High,

		ComputedAt: computedAt,
	}
}

// ComplexityDeltaRatio ports load_repo_complexity_delta_30d's final expression
// (compounding_risk.py:465): (second - first) / max(first, 1.0). The floor
// keeps the denominator stable for low-LOC repos; it is Python's max, so a NaN
// first-half average propagates rather than being replaced by 1.0.
//
// The two halves themselves are averaged inside ClickHouse (see
// complexityDeltaQuery), exactly as Python asks it to -- this is the only
// arithmetic that happens on the Go side.
func ComplexityDeltaRatio(firstHalf, secondHalf float64) float64 {
	return (secondHalf - firstHalf) / pythonMax(firstHalf, 1.0)
}

// ComputeForRepos ports build_compounding_risk_rows_for_day's repo loop
// (compounding_risk.py:494-533) for the repo scope. complexityDeltaFor supplies
// the per-repo complexity delta, which Python fetches inside the same loop.
//
// Row order follows the input slice, which the loader has ordered by repo_id --
// Python's own order is whatever ClickHouse returned for an un-ORDER-BY'd
// GROUP BY, so this is strictly more deterministic and never fewer rows.
func ComputeForRepos(
	day time.Time,
	orgID string,
	rows []RepoMetricsRow,
	complexityDeltaFor map[string]*float64,
	computedAt time.Time,
	weights Weights,
	thresholds Thresholds,
	references References,
) []Record {
	if len(rows) == 0 {
		return nil
	}
	records := make([]Record, 0, len(rows))
	for _, row := range rows {
		if row.RepoID == "" {
			// Python's `if repo_id is None: continue` (compounding_risk.py:500).
			continue
		}
		inputs := Inputs{
			ReworkChurn:       row.ReworkChurnRatio30D,
			ComplexityDelta:   complexityDeltaFor[row.RepoID],
			ReviewLatencyP90H: row.PRFirstReviewP90Hours,
			SingleOwnerRatio:  row.SingleOwnerFileRatio30D,
			OwnershipGini:     row.CodeOwnershipGini,
			BusFactor:         row.BusFactor,
		}
		records = append(records, Compute(
			day, row.RepoID, orgID, inputs, computedAt, weights, thresholds, references,
		))
	}
	return records
}

// MeanOrNone ports _mean_or_none (compounding_risk.py:544-548): the
// arithmetic mean of the non-nil values, or nil if every value is nil.
//
// CPython's sum() over floats has been NEUMAIER-COMPENSATED since 3.12, not a
// naive running total (see pythonparity.Sum's own doc comment for measured
// divergence rates) -- a `total += v` loop here would silently disagree with
// Python on a meaningful fraction of real inputs once a team has enough
// repos contributing a component.
//
// ORDER MATTERS: compensated summation is not order-invariant at the bit
// level, so `values` must arrive in the SAME order Python's own list
// comprehension would produce for the identical team -- see BuildTeamRows'
// doc comment for how that order is established and kept identical on both
// sides.
func MeanOrNone(values []*float64) *float64 {
	nums := make([]float64, 0, len(values))
	for _, v := range values {
		if v != nil {
			nums = append(nums, *v)
		}
	}
	if len(nums) == 0 {
		return nil
	}
	mean := pythonparity.Sum(nums) / float64(len(nums))
	return &mean
}

// RepoInputs pairs one repo's Inputs with its repo_id, so BuildTeamRows can
// group by team while preserving the CALLER's row order rather than a Go
// map's undefined iteration order.
type RepoInputs struct {
	RepoID string
	Inputs Inputs
}

// BuildTeamRows ports _build_team_rows (compounding_risk.py:554-604):
// aggregate each team's repos into ONE row per team, via an unweighted mean
// of each *raw input* across the team's repos, then feed the means into the
// same Compute path (ComputeTeam) so the team score is auditable under the
// identical formula as repo rows.
//
// ORDER, and why it is asserted here rather than assumed: Python builds
// `by_team` by iterating `repo_inputs.items()` (dict insertion order) and
// appending into each team's list in THAT order; every mean computed from
// that list is therefore evaluated in a SPECIFIC, Python-determined order,
// and pythonparity.Sum is not order-invariant at the bit level (see its own
// doc comment). `repoInputs` here is a caller-supplied SLICE, not a map, for
// exactly this reason: it lets the caller establish one deterministic row
// order (LoadRepoMetricsForOrgDay's own explicit ORDER BY repo_id) and this
// function preserves it exactly, both for which repos land in which team's
// list AND the order within that list -- so a golden fixture generated by
// feeding Python's real _build_team_rows the SAME repo_id-ordered input, in
// the same order, is bit-exactly comparable to this function's output. This
// package's golden_rot_guard_test.go and the team-scope oracle it extends to
// depend on that equivalence; changing this function to reorder or
// deduplicate before grouping would silently break it without any test
// necessarily catching a small-magnitude drift.
//
// Team row emission order (the returned slice's order, as opposed to the
// per-team input order above) follows first-occurrence-of-team-id in
// `repoInputs`, mirroring Python dict insertion order for `by_team` exactly
// -- not sorted, because Python's own order is exactly this and a reader
// diffing two runs' output expects the same row order Python would have
// produced for the same input.
func BuildTeamRows(
	day time.Time,
	orgID string,
	repoInputs []RepoInputs,
	repoToTeam map[string]string,
	computedAt time.Time,
	weights Weights,
	thresholds Thresholds,
	references References,
) []Record {
	byTeam := make(map[string][]Inputs, len(repoToTeam))
	teamOrder := make([]string, 0, len(repoToTeam))
	for _, ri := range repoInputs {
		teamID, ok := repoToTeam[ri.RepoID]
		if !ok || teamID == "" {
			continue
		}
		if _, seen := byTeam[teamID]; !seen {
			teamOrder = append(teamOrder, teamID)
		}
		byTeam[teamID] = append(byTeam[teamID], ri.Inputs)
	}

	out := make([]Record, 0, len(teamOrder))
	for _, teamID := range teamOrder {
		allInputs := byTeam[teamID]
		reworkChurn := make([]*float64, len(allInputs))
		complexityDelta := make([]*float64, len(allInputs))
		reviewLatency := make([]*float64, len(allInputs))
		singleOwnerRatio := make([]*float64, len(allInputs))
		ownershipGini := make([]*float64, len(allInputs))
		busFactor := make([]*float64, len(allInputs))
		for i, inputs := range allInputs {
			reworkChurn[i] = inputs.ReworkChurn
			complexityDelta[i] = inputs.ComplexityDelta
			reviewLatency[i] = inputs.ReviewLatencyP90H
			singleOwnerRatio[i] = inputs.SingleOwnerRatio
			ownershipGini[i] = inputs.OwnershipGini
			busFactor[i] = inputs.BusFactor
		}
		teamInputs := Inputs{
			ReworkChurn:       MeanOrNone(reworkChurn),
			ComplexityDelta:   MeanOrNone(complexityDelta),
			ReviewLatencyP90H: MeanOrNone(reviewLatency),
			SingleOwnerRatio:  MeanOrNone(singleOwnerRatio),
			OwnershipGini:     MeanOrNone(ownershipGini),
			// BusFactor IS aggregated too (compounding_risk.py:590), even
			// though it is pure metadata never consumed by the formula -- a
			// team row still persists a mean bus_factor for inspectability,
			// same as a repo row persists its own. An earlier revision of this
			// function omitted it based on a misreading of the source; caught
			// by the golden oracle (team-alpha/beta/solo's bus_factor came back
			// non-null from live Python where this function was producing
			// null).
			BusFactor: MeanOrNone(busFactor),
		}
		out = append(out, ComputeTeam(
			day, teamID, orgID, teamInputs, computedAt, weights, thresholds, references,
		))
	}
	return out
}
