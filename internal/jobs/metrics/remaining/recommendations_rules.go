package remaining

import (
	"fmt"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// Thresholds mirror src/dev_health_ops/recommendations/thresholds.py.
//
// The reference calls these an "anti-config contract": named Python constants,
// never YAML, database or environment, and per-team overrides explicitly
// forbidden. They are constants here for the same reason.
//
// Their VALUES also reach persisted text. Every rationale and every success
// criterion interpolates them with an f-string, and `f"{x}"` on a float is
// format(x, "") which is str(x), which since Python 3 is repr(x). So 0.0
// renders "0.0" and 24.0 renders "24.0", where Go's %v gives "0" and "24".
// pythonparity.Repr is the mirror of that, and it is used at every site below
// rather than a hand-written literal, so a threshold change moves the stored
// text exactly as it does in the reference.
const (
	wipRisingSlopeThreshold       = 0.1
	throughputFlatDeltaThreshold  = 0.0
	reviewLatencyP75Hours         = 24.0
	reviewerGiniThreshold         = 0.6
	churnRatioThreshold           = 0.3
	throughputLowDeltaThreshold   = 0.0
	afterHoursRatioThreshold      = 0.2
	cycleTimeRisingSlopeThreshold = 0.1
	complexityDeltaThreshold      = 0.2
	hotspotChurnOverlapThreshold  = 0.4
)

// Success criteria mirror the module-level SUCCESS_CRITERION constants.
//
// In the reference these are computed ONCE at import time by an f-string over
// the thresholds, so they are vars here rather than an expression evaluated per
// call -- same cost model, and the same single point of truth.
//
// Read these against the reference carefully rather than by intuition. Two of
// them do not say what a reader expects:
//
//   - review-concentration says "review latency p75", not "p75 review latency".
//   - sustainability-risk joins its two clauses with AND, not OR, and its
//     second clause is "cycle time trend stabilises" -- NOT the negated firing
//     condition ("slope turns negative"), which is what the other four rules
//     use and what a port written from the firing logic would produce.
//
// Both were wrong in this file's first draft, written from the rule logic
// instead of from the string. These are persisted, user-visible text on every
// fired row, so a paraphrase is a silent data defect that no threshold test
// would catch.
var (
	saturationSuccessCriterion = "WIP trend turns negative or throughput trend turns positive in 2 cycles"

	reviewConcentrationSuccessCriterion = "Reviewer Gini drops below " +
		pythonparity.Repr(reviewerGiniThreshold) + " OR review latency p75 drops below " +
		pythonparity.Repr(reviewLatencyP75Hours) + "h in 2 cycles"

	thrashSuccessCriterion = "Churn ratio drops below " +
		pythonparity.Repr(churnRatioThreshold) + " OR throughput delta turns positive in 2 cycles"

	sustainabilityRiskSuccessCriterion = "After-hours ratio drops below " +
		pythonparity.Repr(afterHoursRatioThreshold) + " AND cycle time trend stabilises in 2 cycles"

	compoundingRiskSuccessCriterion = "Hotspot complexity delta drops below " +
		pythonparity.Repr(complexityDeltaThreshold) + " OR churn-complexity overlap drops below " +
		pythonparity.Repr(hotspotChurnOverlapThreshold) + " in 2 cycles"
)

// MetricsSnapshot mirrors the frozen dataclass the rules read from.
//
// Optional scalars are (value, known) pairs rather than pointers: the reference
// distinguishes "no data" from a real 0.0, and every one of these fields is
// compared against a threshold at or above 0.0, so collapsing None into the
// zero value would change firing behaviour rather than merely lose information.
//
// WindowStart and WindowEnd are `date` in the reference, not `datetime`. They
// are time.Time here (Go has no date type) carrying midnight UTC, and the
// window_end is EXCLUSIVE per the reference's docstring.
type MetricsSnapshot struct {
	TeamID      string
	OrgID       string
	WindowStart time.Time
	WindowEnd   time.Time

	WIPByDay          []float64
	ThroughputByCycle []float64

	ReviewLatencyP75Hours      float64
	ReviewLatencyP75HoursKnown bool
	ReviewerGini               float64
	ReviewerGiniKnown          bool

	ReworkChurnRatio      float64
	ReworkChurnRatioKnown bool

	AfterHoursRatio      float64
	AfterHoursRatioKnown bool
	CycleTimeByDay       []float64

	HotspotComplexityDelta      float64
	HotspotComplexityDeltaKnown bool
	HotspotChurnOverlap         float64
	HotspotChurnOverlapKnown    bool

	CompoundingRiskScore      float64
	CompoundingRiskScoreKnown bool
	CompoundingRiskSeverity   string
}

// EvidenceRef mirrors the reference's frozen EvidenceRef. Value is already
// rounded at construction, matching the reference's round(x, N) at each
// evidence site, because the rounded number is what reaches evidence_json.
type EvidenceRef struct {
	TeamID      string
	MetricTable string
	WindowStart time.Time
	WindowEnd   time.Time
	Field       string
	Value       float64
}

// Recommendation mirrors the reference's frozen Recommendation.
type Recommendation struct {
	RuleID           string
	TeamID           string
	OrgID            string
	ComputedAt       time.Time
	WindowStart      time.Time
	WindowEnd        time.Time
	Severity         string
	Title            string
	Rationale        string
	SuccessCriterion string
	Evidence         []EvidenceRef
}

// ruleEvaluator is one rule. A nil Recommendation with a nil error means "did
// not fire", mirroring the reference's `-> Recommendation | None`.
type ruleEvaluator struct {
	id       string
	evaluate func(snapshot MetricsSnapshot, now time.Time) (*Recommendation, error)
}

// orderedRuleEvaluators mirrors RULE_EVALUATORS in
// src/dev_health_ops/recommendations/rules/__init__.py:31-37.
//
// ORDER IS PART OF THE CONTRACT. The reference engine iterates that dict, and
// Python dicts are insertion-ordered by language guarantee -- the reference's
// own comment says so -- and the records for one team are emitted in exactly
// this sequence. A Go map would randomise it per run, differing not only from
// Python but from itself between runs, so this is a slice.
//
// Sorting the ids instead would be deterministic and still wrong: it gives
// compounding-risk, review-concentration, saturation, sustainability-risk,
// thrash.
var orderedRuleEvaluators = []ruleEvaluator{
	{id: "saturation", evaluate: evaluateSaturation},
	{id: "review-concentration", evaluate: evaluateReviewConcentration},
	{id: "thrash", evaluate: evaluateThrash},
	{id: "sustainability-risk", evaluate: evaluateSustainabilityRisk},
	{id: "compounding-risk", evaluate: evaluateCompoundingRisk},
}

// evidenceValue applies the reference's round(x, N) at an evidence site.
//
// CPython's round() is banker's rounding on the exact binary value and
// disagrees with math.Round on ordinary inputs (round(2.675, 2) is 2.67, not
// 2.68). The rounded number is serialised into the stored evidence_json, so a
// divergence here is a divergence in stored data.
//
// pythonparity.Round errors only where CPython raises OverflowError, which
// needs |ndigits| in a band no evidence site here comes near -- every call site
// passes a literal 2 or 4. So an error is a genuine impossibility, and it is
// returned rather than swallowed precisely because a silent fallback would hide
// the one case that would prove that assumption wrong.
func evidenceValue(value float64, digits int) (float64, error) {
	return pythonparity.Round(value, digits)
}

func evaluateSaturation(snapshot MetricsSnapshot, now time.Time) (*Recommendation, error) {
	wip := snapshot.WIPByDay
	throughput := snapshot.ThroughputByCycle

	if len(wip) < 2 || len(throughput) < 2 {
		return nil, nil
	}

	wipSlope := LinearSlope(wip)
	throughputDelta := throughput[len(throughput)-1] - throughput[0]

	// Both guards are early-RETURNS on the negated condition, so the boundary
	// value fires: `slope < 0.1` returns, therefore exactly 0.1 continues.
	// Writing the positive form as `slope > 0.1` would drop the boundary, and
	// the boundary is reachable -- LinearSlope returns exactly 0.1 for
	// [0.1, 0.0, 1.0, 0.8, 0.2] under compensated summation.
	if wipSlope < wipRisingSlopeThreshold {
		return nil, nil
	}
	if throughputDelta > throughputFlatDeltaThreshold {
		return nil, nil
	}

	slopeEvidence, err := evidenceValue(wipSlope, 4)
	if err != nil {
		return nil, err
	}
	deltaEvidence, err := evidenceValue(throughputDelta, 4)
	if err != nil {
		return nil, err
	}
	slopeText, err := pythonparity.FormatFixed(wipSlope, 3)
	if err != nil {
		return nil, err
	}
	deltaText, err := pythonparity.FormatFixed(throughputDelta, 1)
	if err != nil {
		return nil, err
	}

	return &Recommendation{
		RuleID:      "saturation",
		TeamID:      snapshot.TeamID,
		OrgID:       snapshot.OrgID,
		ComputedAt:  now,
		WindowStart: snapshot.WindowStart,
		WindowEnd:   snapshot.WindowEnd,
		Severity:    "warning",
		Title:       "Team is saturating. Reduce active work before adding scope.",
		Rationale: fmt.Sprintf(
			"WIP slope is %s items/day (threshold: %s) and throughput delta is "+
				"%s items/cycle (threshold: ≤%s).",
			slopeText,
			pythonparity.Repr(wipRisingSlopeThreshold),
			deltaText,
			pythonparity.Repr(throughputFlatDeltaThreshold),
		),
		SuccessCriterion: saturationSuccessCriterion,
		Evidence: []EvidenceRef{
			{snapshot.TeamID, "work_item_metrics_daily", snapshot.WindowStart, snapshot.WindowEnd, "wip_count_end_of_day", slopeEvidence},
			{snapshot.TeamID, "work_item_metrics_daily", snapshot.WindowStart, snapshot.WindowEnd, "items_completed_delta", deltaEvidence},
		},
	}, nil
}

func evaluateReviewConcentration(snapshot MetricsSnapshot, now time.Time) (*Recommendation, error) {
	if !snapshot.ReviewLatencyP75HoursKnown || !snapshot.ReviewerGiniKnown {
		return nil, nil
	}
	latency := snapshot.ReviewLatencyP75Hours
	giniScore := snapshot.ReviewerGini

	if latency < reviewLatencyP75Hours {
		return nil, nil
	}
	if giniScore < reviewerGiniThreshold {
		return nil, nil
	}

	// Note the asymmetry the reference deliberately keeps: latency rounds to 2
	// places and the Gini to 4. Not a typo to normalise.
	latencyEvidence, err := evidenceValue(latency, 2)
	if err != nil {
		return nil, err
	}
	giniEvidence, err := evidenceValue(giniScore, 4)
	if err != nil {
		return nil, err
	}
	latencyText, err := pythonparity.FormatFixed(latency, 1)
	if err != nil {
		return nil, err
	}
	giniText, err := pythonparity.FormatFixed(giniScore, 3)
	if err != nil {
		return nil, err
	}

	return &Recommendation{
		RuleID:      "review-concentration",
		TeamID:      snapshot.TeamID,
		OrgID:       snapshot.OrgID,
		ComputedAt:  now,
		WindowStart: snapshot.WindowStart,
		WindowEnd:   snapshot.WindowEnd,
		Severity:    "warning",
		Title:       "Review dependency risk. Add reviewers or rotate ownership.",
		Rationale: fmt.Sprintf(
			"Review latency p75 is %sh (threshold: %sh) and reviewer Gini is "+
				"%s (threshold: %s), indicating review load is concentrated in few individuals.",
			latencyText,
			pythonparity.Repr(reviewLatencyP75Hours),
			giniText,
			pythonparity.Repr(reviewerGiniThreshold),
		),
		SuccessCriterion: reviewConcentrationSuccessCriterion,
		Evidence: []EvidenceRef{
			{snapshot.TeamID, "repo_metrics_daily", snapshot.WindowStart, snapshot.WindowEnd, "review_latency_p75_hours", latencyEvidence},
			{snapshot.TeamID, "review_edge_daily", snapshot.WindowStart, snapshot.WindowEnd, "reviewer_gini", giniEvidence},
		},
	}, nil
}

func evaluateThrash(snapshot MetricsSnapshot, now time.Time) (*Recommendation, error) {
	if !snapshot.ReworkChurnRatioKnown {
		return nil, nil
	}
	churnRatio := snapshot.ReworkChurnRatio
	throughput := snapshot.ThroughputByCycle

	// Guard ORDER mirrors the reference exactly, and it is observable: the
	// churn threshold is checked BEFORE the length guard, so a snapshot with a
	// low churn ratio and a one-element throughput list exits on churn. That is
	// indistinguishable here (both return None) but the ordering is preserved
	// so any future telemetry on the exit reason stays faithful.
	if churnRatio < churnRatioThreshold {
		return nil, nil
	}
	if len(throughput) < 2 {
		return nil, nil
	}
	throughputDelta := throughput[len(throughput)-1] - throughput[0]
	if throughputDelta > throughputLowDeltaThreshold {
		return nil, nil
	}

	churnEvidence, err := evidenceValue(churnRatio, 4)
	if err != nil {
		return nil, err
	}
	deltaEvidence, err := evidenceValue(throughputDelta, 4)
	if err != nil {
		return nil, err
	}
	churnText, err := pythonparity.FormatFixed(churnRatio, 3)
	if err != nil {
		return nil, err
	}
	deltaText, err := pythonparity.FormatFixed(throughputDelta, 1)
	if err != nil {
		return nil, err
	}

	return &Recommendation{
		RuleID:      "thrash",
		TeamID:      snapshot.TeamID,
		OrgID:       snapshot.OrgID,
		ComputedAt:  now,
		WindowStart: snapshot.WindowStart,
		WindowEnd:   snapshot.WindowEnd,
		Severity:    "warning",
		Title:       "Thrash likely. Inspect hotspots and rework loops.",
		Rationale: fmt.Sprintf(
			"Rework churn ratio is %s (threshold: %s) and throughput delta is "+
				"%s items/cycle (threshold: ≤%s), "+
				"suggesting repeated rework is consuming capacity without advancing delivery.",
			churnText,
			pythonparity.Repr(churnRatioThreshold),
			deltaText,
			pythonparity.Repr(throughputLowDeltaThreshold),
		),
		SuccessCriterion: thrashSuccessCriterion,
		Evidence: []EvidenceRef{
			{snapshot.TeamID, "repo_metrics_daily", snapshot.WindowStart, snapshot.WindowEnd, "rework_churn_ratio_30d", churnEvidence},
			{snapshot.TeamID, "work_item_metrics_daily", snapshot.WindowStart, snapshot.WindowEnd, "items_completed_delta", deltaEvidence},
		},
	}, nil
}

func evaluateSustainabilityRisk(snapshot MetricsSnapshot, now time.Time) (*Recommendation, error) {
	if !snapshot.AfterHoursRatioKnown {
		return nil, nil
	}
	afterHours := snapshot.AfterHoursRatio
	cycleTimes := snapshot.CycleTimeByDay

	if afterHours < afterHoursRatioThreshold {
		return nil, nil
	}
	if len(cycleTimes) < 2 {
		return nil, nil
	}
	// The length guard above is redundant with LinearSlope's own < 2 check --
	// but only for the RESULT, not for the CONTROL FLOW. Without it a
	// one-element list would give slope 0.0, fail the threshold and return None
	// anyway; with a threshold of <= 0.0 it would not. Kept because the
	// reference has it and its redundancy is contingent on a constant.
	ctSlope := LinearSlope(cycleTimes)
	if ctSlope < cycleTimeRisingSlopeThreshold {
		return nil, nil
	}

	afterHoursEvidence, err := evidenceValue(afterHours, 4)
	if err != nil {
		return nil, err
	}
	slopeEvidence, err := evidenceValue(ctSlope, 4)
	if err != nil {
		return nil, err
	}
	afterHoursText, err := pythonparity.FormatFixed(afterHours, 3)
	if err != nil {
		return nil, err
	}
	slopeText, err := pythonparity.FormatFixed(ctSlope, 3)
	if err != nil {
		return nil, err
	}

	return &Recommendation{
		RuleID:      "sustainability-risk",
		TeamID:      snapshot.TeamID,
		OrgID:       snapshot.OrgID,
		ComputedAt:  now,
		WindowStart: snapshot.WindowStart,
		WindowEnd:   snapshot.WindowEnd,
		Severity:    "warning",
		Title:       "Sustainability risk. Delivery may be propped up by time debt.",
		Rationale: fmt.Sprintf(
			"After-hours commit ratio is %s (threshold: %s) and cycle time slope is "+
				"%s hours/day (threshold: %s), "+
				"suggesting extended hours are masking growing delivery pressure.",
			afterHoursText,
			pythonparity.Repr(afterHoursRatioThreshold),
			slopeText,
			pythonparity.Repr(cycleTimeRisingSlopeThreshold),
		),
		SuccessCriterion: sustainabilityRiskSuccessCriterion,
		Evidence: []EvidenceRef{
			{snapshot.TeamID, "team_metrics_daily", snapshot.WindowStart, snapshot.WindowEnd, "after_hours_commit_ratio", afterHoursEvidence},
			{snapshot.TeamID, "work_item_metrics_daily", snapshot.WindowStart, snapshot.WindowEnd, "cycle_time_p50_hours_slope", slopeEvidence},
		},
	}, nil
}

// evaluateCompoundingRisk is the only rule with two firing paths and the only
// one that can emit "critical".
//
// The FIRST path wins outright (CHAOS-1641): when the persisted composite
// carries severity "elevated" or "high", it fires from that and the legacy
// hotspot proxy is never consulted -- even if the hotspot fields are present
// and would themselves have fired, and even if they are absent. Only a severity
// OUTSIDE that pair (including empty, the mirror of None) falls through.
func evaluateCompoundingRisk(snapshot MetricsSnapshot, now time.Time) (*Recommendation, error) {
	severity := snapshot.CompoundingRiskSeverity
	if severity == "elevated" || severity == "high" {
		return compoundingRiskFromComposite(snapshot, now)
	}
	return compoundingRiskFromHotspotProxy(snapshot, now)
}

func compoundingRiskFromComposite(snapshot MetricsSnapshot, now time.Time) (*Recommendation, error) {
	severity := snapshot.CompoundingRiskSeverity

	// `round(score, 4) if score is not None else 0.0`: an absent score still
	// emits an evidence row, carrying 0.0. That 0.0 is indistinguishable in the
	// stored row from a genuinely-zero composite -- a property of the
	// reference, faithfully reproduced, not an improvement to make here.
	evidence := 0.0
	if snapshot.CompoundingRiskScoreKnown {
		rounded, err := evidenceValue(snapshot.CompoundingRiskScore, 4)
		if err != nil {
			return nil, err
		}
		evidence = rounded
	}

	// The reference picks the rationale with a conditional expression, so the
	// `.3f` conversion is only evaluated on the score-present branch. Same
	// here: FormatFixed is not called at all when the score is absent.
	rationale := fmt.Sprintf("Compounding Risk severity is %s.", severity)
	if snapshot.CompoundingRiskScoreKnown {
		scoreText, err := pythonparity.FormatFixed(snapshot.CompoundingRiskScore, 3)
		if err != nil {
			return nil, err
		}
		rationale = fmt.Sprintf(
			"Compounding Risk score is %s (severity: %s). "+
				"Churn, complexity trend, ownership concentration, and review "+
				"latency are compounding above their tuned thresholds.",
			scoreText, severity,
		)
	}

	recommendationSeverity := "warning"
	if severity == "high" {
		recommendationSeverity = "critical"
	}

	return &Recommendation{
		RuleID:           "compounding-risk",
		TeamID:           snapshot.TeamID,
		OrgID:            snapshot.OrgID,
		ComputedAt:       now,
		WindowStart:      snapshot.WindowStart,
		WindowEnd:        snapshot.WindowEnd,
		Severity:         recommendationSeverity,
		Title:            "Code risk is compounding where change pressure is highest.",
		Rationale:        rationale,
		SuccessCriterion: compoundingRiskSuccessCriterion,
		Evidence: []EvidenceRef{
			{snapshot.TeamID, "compounding_risk_daily", snapshot.WindowStart, snapshot.WindowEnd, "compounding_risk", evidence},
		},
	}, nil
}

// compoundingRiskFromHotspotProxy is the pre-1641 fallback, reached only during
// backfill warmup or where the composite has not been computed. It is live code
// on a fresh deployment, not dead history.
func compoundingRiskFromHotspotProxy(snapshot MetricsSnapshot, now time.Time) (*Recommendation, error) {
	if !snapshot.HotspotComplexityDeltaKnown || !snapshot.HotspotChurnOverlapKnown {
		return nil, nil
	}
	complexityDelta := snapshot.HotspotComplexityDelta
	churnOverlap := snapshot.HotspotChurnOverlap

	if complexityDelta < complexityDeltaThreshold {
		return nil, nil
	}
	if churnOverlap < hotspotChurnOverlapThreshold {
		return nil, nil
	}

	complexityEvidence, err := evidenceValue(complexityDelta, 4)
	if err != nil {
		return nil, err
	}
	overlapEvidence, err := evidenceValue(churnOverlap, 4)
	if err != nil {
		return nil, err
	}
	complexityText, err := pythonparity.FormatFixed(complexityDelta, 3)
	if err != nil {
		return nil, err
	}
	overlapText, err := pythonparity.FormatFixed(churnOverlap, 3)
	if err != nil {
		return nil, err
	}

	// Severity is "warning" on this path even though the composite path can
	// reach "critical": the proxy has no severity signal to map from.
	return &Recommendation{
		RuleID:      "compounding-risk",
		TeamID:      snapshot.TeamID,
		OrgID:       snapshot.OrgID,
		ComputedAt:  now,
		WindowStart: snapshot.WindowStart,
		WindowEnd:   snapshot.WindowEnd,
		Severity:    "warning",
		Title:       "Code risk is compounding where change pressure is highest.",
		Rationale: fmt.Sprintf(
			"Hotspot complexity delta is %s (threshold: %s) and churn-complexity overlap "+
				"is %s (threshold: %s), "+
				"indicating growing technical debt in the most actively-changed files.",
			complexityText,
			pythonparity.Repr(complexityDeltaThreshold),
			overlapText,
			pythonparity.Repr(hotspotChurnOverlapThreshold),
		),
		SuccessCriterion: compoundingRiskSuccessCriterion,
		Evidence: []EvidenceRef{
			{snapshot.TeamID, "file_complexity_snapshots", snapshot.WindowStart, snapshot.WindowEnd, "hotspot_complexity_delta", complexityEvidence},
			{snapshot.TeamID, "file_metrics_daily", snapshot.WindowStart, snapshot.WindowEnd, "hotspot_churn_overlap", overlapEvidence},
		},
	}, nil
}
