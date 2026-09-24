// Pure response-shaping logic -- ports every function in
// api/services/home.py that has no I/O of its own: metric-signal
// construction, severity/confidence scoring, health-state and
// limiting-factor summarisation, and the fixed tiles map. Every
// function here is a direct, verbatim port; see each one's own doc
// comment for its Python source line range.
package home

import (
	"encoding/json"
	"fmt"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// SignalCategory values ported from home.py's SignalCategory Literal
// (services/home.py:61).
const (
	CategoryDelivery   = "delivery"
	CategoryDurability = "durability"
	CategoryWellbeing  = "wellbeing"
	CategoryDynamics   = "dynamics"
	CategoryAI         = "ai"
)

var lowerIsBetter = map[string]bool{
	"cycle_time":          true,
	"review_latency":      true,
	"churn":               true,
	"wip_saturation":      true,
	"blocked_work":        true,
	"change_failure_rate": true,
	"rework_ratio":        true,
	"pr_rework_ratio":     true,
	"compounding_risk":    true,
}

var metricCategories = map[string]string{
	"cycle_time":          CategoryDelivery,
	"review_latency":      CategoryDynamics,
	"throughput":          CategoryDelivery,
	"deploy_freq":         CategoryDelivery,
	"churn":               CategoryDurability,
	"wip_saturation":      CategoryDynamics,
	"blocked_work":        CategoryDelivery,
	"change_failure_rate": CategoryDurability,
	"rework_ratio":        CategoryDurability,
	"pr_rework_ratio":     CategoryDurability,
	"ci_success":          CategoryDurability,
	"compounding_risk":    CategoryDurability,
}

// direction ports _direction (services/home.py:301-306).
func direction(pctChange float64) string {
	if pctChange > 0 {
		return "rose"
	}
	if pctChange < 0 {
		return "fell"
	}
	return "held steady"
}

// formatDeltaWords ports _format_delta (services/home.py:309-310).
func formatDeltaWords(deltaPct float64) string {
	return fmt.Sprintf("%.0f%%", absFloat(deltaPct))
}

// primaryScopeLabel ports _primary_scope_label (services/home.py:313-316).
func primaryScopeLabel(f Filters) string {
	if len(f.Scope.IDs) > 0 {
		return strings.Join(f.Scope.IDs, ",")
	}
	return f.Scope.Level
}

// signalDirection ports _signal_direction (services/home.py:319-324).
func signalDirection(delta float64) string {
	if delta > 1 {
		return "up"
	}
	if delta < -1 {
		return "down"
	}
	return "flat"
}

// priorValue ports _prior_value (services/home.py:327-330).
func priorValue(current, delta float64) (float64, bool) {
	if absFloat(delta+100.0) < 0.0001 {
		return 0, false
	}
	return current / (1.0 + (delta / 100.0)), true
}

// metricImpact ports _metric_impact (services/home.py:333-341).
func metricImpact(metric string, delta float64) float64 {
	dir := signalDirection(delta)
	if dir == "flat" {
		return 0.0
	}
	magnitude := absFloat(delta)
	worsened := (lowerIsBetter[metric] && delta > 0) || (!lowerIsBetter[metric] && delta < 0)
	if worsened {
		return magnitude
	}
	return magnitude * 0.35
}

// severityForImpact ports _severity_for_impact (services/home.py:344-351).
func severityForImpact(impact float64) string {
	if impact >= 60 {
		return "critical"
	}
	if impact >= 35 {
		return "high"
	}
	if impact >= 15 {
		return "medium"
	}
	return "low"
}

// severityRank ports _severity_rank (services/home.py:354-355).
func severityRank(severity string) int {
	switch severity {
	case "critical":
		return 4
	case "high":
		return 3
	case "medium":
		return 2
	case "low":
		return 1
	default:
		return 0
	}
}

// confidenceFromEvidence ports _confidence_from_evidence
// (services/home.py:358-366).
func confidenceFromEvidence(evidenceCount int, coveragePct *float64) string {
	coverage := 0.0
	if coveragePct != nil {
		coverage = *coveragePct
	}
	if evidenceCount >= 7 && coverage >= 75 {
		return "high"
	}
	if evidenceCount >= 2 && coverage >= 40 {
		return "medium"
	}
	return "low"
}

// formatValue ports _format_value (services/home.py:369-373).
func formatValue(value float64, unit string) string {
	suffix := ""
	if unit != "" {
		suffix = " " + unit
	}
	if absFloat(value) >= 100 || value == float64(int64(value)) {
		return fmt.Sprintf("%.0f%s", value, suffix)
	}
	return fmt.Sprintf("%.1f%s", value, suffix)
}

// formatDeltaValue ports _format_delta_value (services/home.py:376-379).
func formatDeltaValue(delta *float64) *string {
	if delta == nil {
		return nil
	}
	s := fmt.Sprintf("%+.0f%%", *delta)
	return &s
}

// evidenceLink ports _evidence_link (services/home.py:382-389).
func evidenceLink(metric string, f Filters) string {
	return fmt.Sprintf(
		"/api/v1/explain?metric=%s&scope_type=%s&scope_id=%s&range_days=%d&compare_days=%d",
		metric, f.Scope.Level, primaryScopeID(f), f.Time.RangeDays, f.Time.CompareDays,
	)
}

// primaryScopeID ports _primary_scope_id (services/home.py:1266-1269).
func primaryScopeID(f Filters) string {
	if len(f.Scope.IDs) > 0 {
		return f.Scope.IDs[0]
	}
	return ""
}

var metricActions = map[string]string{
	"cycle_time":          "Inspect the slowest stage and rebalance active work before adding scope.",
	"review_latency":      "Rebalance reviewer rotation and clear stale review queues.",
	"throughput":          "Review WIP and dependency queues before changing delivery commitments.",
	"deploy_freq":         "Check release blockers and restore the smallest safe deployment path.",
	"churn":               "Inspect hotspots and stabilize rework loops before expanding the change set.",
	"wip_saturation":      "Set a short-term WIP limit and finish active items before starting more.",
	"blocked_work":        "Triage blocked items by owner and unblock the oldest high-impact queue first.",
	"change_failure_rate": "Inspect recent failed changes and tighten pre-release checks around the common failure mode.",
	"rework_ratio":        "Review reopened or rewritten work and pick one root-cause experiment.",
	"ci_success":          "Inspect failing pipelines and restore the most common broken check first.",
	"compounding_risk":    "Inspect the highest-risk scope and reduce the strongest component before adding scope.",
}

const defaultMetricAction = "Inspect supporting evidence and choose one reversible operating experiment."

// actionForMetric ports _action_for_metric (services/home.py:392-409).
func actionForMetric(metric string) string {
	if action, ok := metricActions[metric]; ok {
		return action
	}
	return defaultMetricAction
}

var metricReasons = map[string]string{
	"cycle_time":          "longer cycle time suggests delivery work may spend more time waiting than moving.",
	"review_latency":      "review queues shape how quickly teams can learn from completed work.",
	"throughput":          "throughput movement changes the team's ability to keep commitments credible.",
	"deploy_freq":         "deployment cadence suggests whether finished work can reach users smoothly.",
	"churn":               "higher churn suggests effort may be cycling through rework rather than durable progress.",
	"wip_saturation":      "saturation suggests active work may exceed the team's coordination capacity.",
	"blocked_work":        "blocked time suggests dependencies may be consuming delivery capacity.",
	"change_failure_rate": "failed changes suggest reliability work may be competing with delivery.",
	"rework_ratio":        "rework suggests unclear requirements or fragile implementation paths may be taxing focus.",
	"ci_success":          "CI health suggests whether the delivery path is dependable.",
	"compounding_risk":    "compounding risk combines churn, complexity, ownership, and review pressure into one persisted signal.",
}

var movementWords = map[string]string{"up": "rising", "down": "falling", "flat": "flat"}

// whyForMetric ports _why_for_metric (services/home.py:412-434).
func whyForMetric(metric, label, dir string) string {
	movement := movementWords[dir]
	reason, ok := metricReasons[metric]
	if !ok {
		reason = fmt.Sprintf("%s movement suggests an operating signal to inspect.", label)
	}
	return fmt.Sprintf("%s appears %s; %s", label, movement, reason)
}

// coveragePctFromCoverage ports _coverage_pct_from_coverage
// (services/home.py:437-441).
func coveragePctFromCoverage(coverage map[string]float64) *float64 {
	if len(coverage) == 0 {
		return nil
	}
	sum := 0.0
	for _, v := range coverage {
		sum += v
	}
	avg := sum / float64(len(coverage))
	return &avg
}

// BuildDataConfidence ports build_data_confidence (services/home.py:444-473).
func BuildDataConfidence(coverage map[string]float64, sources map[string]string) DataConfidence {
	coveragePct := coveragePctFromCoverage(coverage)
	var connected, missing []string
	for source, status := range sources {
		if status == "ok" {
			connected = append(connected, source)
		} else {
			missing = append(missing, source)
		}
	}
	sort.Strings(connected)
	sort.Strings(missing)
	if connected == nil {
		connected = []string{}
	}
	if missing == nil {
		missing = []string{}
	}

	var level string
	switch {
	case coveragePct != nil && *coveragePct >= 75 && len(missing) == 0:
		level = "high"
	case coveragePct != nil && *coveragePct >= 40 && len(connected) > 0:
		level = "medium"
	default:
		level = "low"
	}

	caveats := []string{}
	if len(missing) > 0 {
		caveats = append(caveats, "Some source freshness checks are missing or stale.")
	}
	if coveragePct == nil {
		caveats = append(caveats, "Coverage could not be computed from available lineage fields.")
	} else if *coveragePct < 60 {
		caveats = append(caveats, "Coverage appears partial; treat cockpit signals as directional.")
	}

	return DataConfidence{
		Level:            level,
		CoveragePct:      coveragePct,
		ConnectedSources: connected,
		MissingSources:   missing,
		Caveats:          caveats,
	}
}

// BuildMetricSignals ports build_metric_signals (services/home.py:476-511).
func BuildMetricSignals(deltas []MetricDelta, f Filters, dataConfidence DataConfidence) []Signal {
	signals := make([]Signal, 0, len(deltas))
	for _, delta := range deltas {
		dir := signalDirection(delta.DeltaPct)
		evidenceCount := len(delta.Spark)
		impact := metricImpact(delta.Metric, delta.DeltaPct)

		var priorValueStr *string
		if prior, ok := priorValue(delta.Value, delta.DeltaPct); ok {
			s := formatValue(prior, delta.Unit)
			priorValueStr = &s
		}
		deltaPct := delta.DeltaPct
		evidenceRefStr := evidenceLink(delta.Metric, f)

		category := metricCategories[delta.Metric]
		if category == "" {
			category = CategoryDelivery
		}

		signals = append(signals, Signal{
			ID:                fmt.Sprintf("metric:%s", delta.Metric),
			Title:             fmt.Sprintf("%s appears %s", delta.Label, dir),
			Metric:            delta.Metric,
			CurrentValue:      formatValue(delta.Value, delta.Unit),
			PriorValue:        priorValueStr,
			Delta:             formatDeltaValue(&deltaPct),
			Direction:         dir,
			Severity:          severityForImpact(impact),
			Confidence:        confidenceFromEvidence(evidenceCount, dataConfidence.CoveragePct),
			AffectedScope:     primaryScopeLabel(f),
			EvidenceCount:     evidenceCount,
			WhyItMatters:      whyForMetric(delta.Metric, delta.Label, dir),
			RecommendedAction: actionForMetric(delta.Metric),
			EvidenceRef:       &evidenceRefStr,
			Category:          category,
		})
	}
	return RankSignals(signals)
}

// deltaMagnitude ports _rank_signals's own local _delta_magnitude
// (services/home.py:515-521).
func deltaMagnitude(s Signal) float64 {
	if s.Delta == nil || *s.Delta == "" {
		return 0.0
	}
	trimmed := strings.TrimSuffix(*s.Delta, "%")
	v, err := strconv.ParseFloat(trimmed, 64)
	if err != nil {
		return 0.0
	}
	return absFloat(v)
}

// RankSignals ports _rank_signals (services/home.py:514-531): a stable
// descending sort by (severity_rank, delta_magnitude, evidence_count).
// Go's sort.SliceStable preserves input order for equal keys, matching
// Python's sorted(..., reverse=True) stability guarantee.
func RankSignals(signals []Signal) []Signal {
	out := make([]Signal, len(signals))
	copy(out, signals)
	sort.SliceStable(out, func(i, j int) bool {
		si, sj := out[i], out[j]
		ri, rj := severityRank(si.Severity), severityRank(sj.Severity)
		if ri != rj {
			return ri > rj
		}
		mi, mj := deltaMagnitude(si), deltaMagnitude(sj)
		if mi != mj {
			return mi > mj
		}
		return si.EvidenceCount > sj.EvidenceCount
	})
	return out
}

// BuildHealthState ports build_health_state (services/home.py:534-563).
func BuildHealthState(signals []Signal, dataConfidence DataConfidence, asOf *time.Time) HealthState {
	if len(signals) == 0 {
		status := "healthy"
		if dataConfidence.Level == "low" {
			status = "watch"
		}
		return HealthState{
			Status:   status,
			Headline: "Cockpit signals appear sparse",
			Summary:  "Available data suggests watching coverage before making operating changes.",
			AsOf:     (*NaiveDateTime)(asOf),
		}
	}
	top := signals[0]
	var status string
	switch {
	case top.Severity == "critical":
		status = "critical"
	case top.Severity == "high":
		status = "at_risk"
	case top.Severity == "medium" || dataConfidence.Level == "low":
		status = "watch"
	default:
		status = "healthy"
	}
	return HealthState{
		Status:   status,
		Headline: fmt.Sprintf("%s across %s", top.Title, top.AffectedScope),
		Summary:  fmt.Sprintf("The strongest signal suggests %s", top.WhyItMatters),
		AsOf:     (*NaiveDateTime)(asOf),
	}
}

// BuildLimitingFactor ports build_limiting_factor (services/home.py:566-576).
func BuildLimitingFactor(signals []Signal) LimitingFactor {
	if len(signals) == 0 {
		return DefaultLimitingFactor()
	}
	top := signals[0]
	return LimitingFactor{
		Claim:             fmt.Sprintf("%s appears to be the current limiting factor.", top.Title),
		WhyItMatters:      top.WhyItMatters,
		RecommendedAction: top.RecommendedAction,
		Confidence:        top.Confidence,
		EvidenceRef:       top.EvidenceRef,
	}
}

// parseRecommendationEvidence ports _parse_recommendation_evidence
// (services/home.py:579-589): raw is the ClickHouse row's
// latest_evidence_json string (a JSON-encoded list); a malformed or
// non-list-of-objects value degrades to an empty list, matching
// Python's own except/comprehension-filter behaviour.
func parseRecommendationEvidence(raw string) []map[string]any {
	if raw == "" {
		return nil
	}
	var parsed []any
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return nil
	}
	out := make([]map[string]any, 0, len(parsed))
	for _, item := range parsed {
		if m, ok := item.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out
}

// RecommendationRow is one deduped row of the recommendations_daily
// reader (queries_signals.go), matching the fields
// _recommendation_signal (services/home.py:592-633) reads.
type RecommendationRow struct {
	TeamID          string
	RuleID          string
	LatestSeverity  string
	LatestTitle     string
	LatestRationale string
	LatestSuccess   string
	LatestEvidence  string
}

// RecommendationSignal ports _recommendation_signal
// (services/home.py:592-633). Returns ok=false for a row with an empty
// title, matching Python's `if not title: return None`.
func RecommendationSignal(row RecommendationRow, f Filters, dataConfidence DataConfidence) (Signal, bool) {
	title := strings.TrimSpace(row.LatestTitle)
	if title == "" {
		return Signal{}, false
	}
	rawSeverity := row.LatestSeverity
	if rawSeverity == "" {
		rawSeverity = "warning"
	}
	severity := "medium"
	if rawSeverity == "critical" {
		severity = "critical"
	}
	evidence := parseRecommendationEvidence(row.LatestEvidence)
	ruleID := row.RuleID
	if ruleID == "" {
		ruleID = "recommendation"
	}
	teamID := row.TeamID
	if teamID == "" {
		teamID = primaryScopeLabel(f)
	}
	whyItMatters := row.LatestRationale
	if whyItMatters == "" {
		whyItMatters = "A persisted recommendation suggests this operating pattern needs attention."
	}
	recommendedAction := row.LatestSuccess
	if recommendedAction == "" {
		recommendedAction = "Choose one reversible experiment and inspect the evidence trail."
	}
	var evidenceRef *string
	if teamID != "" {
		s := fmt.Sprintf("/api/graphql?query=recommendations&team=%s", teamID)
		evidenceRef = &s
	}
	currentValue := formatValue(float64(len(evidence)), "refs")
	return Signal{
		ID:                fmt.Sprintf("recommendation:%s:%s", ruleID, teamID),
		Title:             title,
		Metric:            ruleID,
		CurrentValue:      currentValue,
		Direction:         "flat",
		Severity:          severity,
		Confidence:        confidenceFromEvidence(len(evidence), dataConfidence.CoveragePct),
		AffectedScope:     teamID,
		EvidenceCount:     len(evidence),
		WhyItMatters:      whyItMatters,
		RecommendedAction: recommendedAction,
		EvidenceRef:       evidenceRef,
		Category:          CategoryDynamics,
	}, true
}

var bareUUIDRe = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// looksLikeUUID ports _looks_like_uuid (services/home.py:642-644).
func looksLikeUUID(value string) bool {
	return bareUUIDRe.MatchString(strings.TrimSpace(value))
}

// RiskRow is one deduped row of the compounding_risk_daily reader
// (queries_signals.go), matching the fields _risk_signal (services/home.py:
// 716-768) reads, plus the resolved display name from
// resolveScopeLabels.
type RiskRow struct {
	Scope            string
	ScopeID          string
	Score            *float64
	Severity         string
	ScopeDisplayName string
}

// RiskSignal ports _risk_signal (services/home.py:716-768).
func RiskSignal(row RiskRow, f Filters, dataConfidence DataConfidence) (Signal, bool) {
	if row.Score == nil {
		return Signal{}, false
	}
	currentValue := *row.Score * 100.0
	rawSeverity := strings.ToLower(row.Severity)
	if rawSeverity == "" {
		rawSeverity = "low"
	}
	severityMap := map[string]string{"high": "high", "elevated": "medium", "low": "low", "unknown": "low"}
	severity, ok := severityMap[rawSeverity]
	if !ok {
		severity = "low"
	}
	scopeID := strings.TrimSpace(row.ScopeID)
	scopeType := strings.TrimSpace(row.Scope)
	if scopeType == "" {
		scopeType = f.Scope.Level
	}
	if scopeType == "" {
		scopeType = "repo"
	}

	// B7: controlled empty/flat state -- absent scope_id suppresses the signal.
	if scopeID == "" {
		return Signal{}, false
	}

	// A8: no bare UUID in any label or headline field.
	entityName := row.ScopeDisplayName
	if entityName == "" || looksLikeUUID(entityName) {
		return Signal{}, false
	}

	affectedScope := scopeType + "s"
	return Signal{
		ID:                fmt.Sprintf("risk:%s:%s", scopeType, scopeID),
		Title:             fmt.Sprintf("Compounding risk appears %s for %s", rawSeverity, entityName),
		Metric:            "compounding_risk",
		CurrentValue:      formatValue(currentValue, "%"),
		Direction:         "flat",
		Severity:          severity,
		Confidence:        confidenceFromEvidence(1, dataConfidence.CoveragePct),
		AffectedScope:     affectedScope,
		EvidenceCount:     1,
		WhyItMatters:      whyForMetric("compounding_risk", "Compounding Risk", "flat"),
		RecommendedAction: actionForMetric("compounding_risk"),
		Category:          CategoryDurability,
		ScopeEntity:       &ScopeEntityRef{ID: scopeID, DisplayName: entityName},
	}, true
}

// SelectConstraint ports _select_constraint (services/home.py:953-963).
func SelectConstraint(deltas []MetricDelta) MetricDelta {
	if len(deltas) == 0 {
		return MetricDelta{Metric: "cycle_time", Label: "Cycle Time", Unit: "days"}
	}
	out := make([]MetricDelta, len(deltas))
	copy(out, deltas)
	sort.SliceStable(out, func(i, j int) bool { return out[i].DeltaPct < out[j].DeltaPct })
	return out[len(out)-1]
}

// tiles ports the fixed tiles map (services/home.py:1187-1208), built
// fresh per call so no caller can mutate a shared value.
// tiles is home.py's tiles dict, in the order Python builds it.
func tiles() pyjson.OrderedMap[Tile] {
	return pyjson.OrderedMapOf(
		pyjson.KeyValue[Tile]{Key: "understand", Value: Tile{Title: "Understand", Subtitle: "Flow stages", Link: "/explore?view=understand"}},
		pyjson.KeyValue[Tile]{Key: "measure", Value: Tile{Title: "Measure", Subtitle: "Coverage & freshness", Link: "/explore?view=measure"}},
		pyjson.KeyValue[Tile]{Key: "align", Value: Tile{Title: "Align", Subtitle: "Investment mix", Link: "/investment"}},
		pyjson.KeyValue[Tile]{Key: "execute", Value: Tile{Title: "Execute", Subtitle: "Top opportunities", Link: "/opportunities"}},
	)
}

func absFloat(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}
