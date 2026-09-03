package remaining

import (
	"fmt"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// The engine, ported from recommendations/engine.py's evaluate_state.
//
// # WHY TOMBSTONES ARE CORRECTNESS, NOT BOOKKEEPING
//
// evaluate_state persists one row per REGISTERED RULE, not one per firing. The
// readers do argMax(fired, computed_at) per (org_id, team_id, rule_id,
// window_end) and keep HAVING latest_fired = true, so without a fired=false row
// at the new window_end a rule that fired yesterday and has since recovered
// keeps surfacing stale guidance (CHAOS-2373). Dropping the tombstones would
// look like a tidy-up and would silently freeze resolved recommendations on
// screen.
//
// # THE TOMBSTONE PATH IS NOT SYMMETRIC WITH THE FIRED PATH
//
// A fired row takes title, severity and success_criterion from the EVALUATOR.
// A tombstone takes them from the registry's static RuleDef. They DISAGREE for
// all five rules, and for two of them the severity is inverted:
//
//	sustainability-risk   fired "warning"   tombstone "critical"
//	compounding-risk      fired "warning"   tombstone "critical"
//	                      (or "critical" when the composite severity is "high")
//
// So a port that reuses the evaluator's values when building a tombstone
// diverges on EVERY non-fired row -- roughly 43% of rows at the rule corpus's
// firing proportions -- while passing every fired-path test. The two sources
// are kept deliberately separate below.

// ruleDefinition mirrors the registry's frozen RuleDef. Only the fields a
// tombstone reads are carried; theme and description are registry-only.
type ruleDefinition struct {
	title            string
	severity         string
	successCriterion string
}

// ruleRegistry mirrors recommendations/registry.py.
//
// These strings are NOT the evaluators' -- compare against
// recommendations_rules.go and the differences are the point. The success
// criteria here even punctuate differently ("within 2 cycles." versus the
// evaluators' "in 2 cycles"), which is exactly the kind of near-miss that a
// hand-copied tombstone would smooth over without anyone noticing.
var ruleRegistry = map[string]ruleDefinition{
	"saturation": {
		title:            "Team Saturation",
		severity:         "warning",
		successCriterion: "WIP trend turns negative OR throughput trend turns positive within 2 cycles.",
	},
	"review-concentration": {
		title:            "Review Concentration Risk",
		severity:         "warning",
		successCriterion: "Reviewer Gini drops below threshold OR review latency p75 drops below threshold within 2 cycles.",
	},
	"thrash": {
		title:            "Thrash Detected",
		severity:         "warning",
		successCriterion: "Churn ratio drops below threshold OR throughput trend turns positive within 2 cycles.",
	},
	"sustainability-risk": {
		title:            "Sustainability Risk",
		severity:         "critical",
		successCriterion: "After-hours ratio drops below threshold AND cycle time trend stabilises within 2 cycles.",
	},
	"compounding-risk": {
		title:            "Compounding Code Risk",
		severity:         "critical",
		successCriterion: "Complexity delta in hotspot files drops below threshold within 2 cycles.",
	},
}

// RecommendationRecord is the row written to recommendations_daily.
type RecommendationRecord struct {
	TeamID           string
	OrgID            string
	RuleID           string
	RuleVersion      string
	WindowStart      time.Time
	WindowEnd        time.Time
	Fired            bool
	Severity         string
	Title            string
	Rationale        string
	SuccessCriterion string
	EvidenceJSON     string
	ComputedAt       time.Time
}

// EvaluateState ports RuleEngine.evaluate_state: one record per registered
// rule, fired or not.
//
// Every record shares ONE computed_at and ONE window_end, so a scheduled run
// replaces the team's whole rule state as a single internally-consistent batch.
// Deriving them per record would let a run straddle a UTC midnight and write
// two window_ends, which the argMax reader would treat as two separate states.
func EvaluateState(
	snapshot MetricsSnapshot, now time.Time, ruleVersion string,
) ([]RecommendationRecord, error) {
	records := make([]RecommendationRecord, 0, len(orderedRuleEvaluators))

	// Iterated in the evaluators' order, not the registry's: the reference
	// loops over its evaluator dict, and that insertion order is what decides
	// the emitted row sequence.
	for _, evaluator := range orderedRuleEvaluators {
		fired, err := evaluator.evaluate(snapshot, now)
		if err != nil {
			return nil, fmt.Errorf("evaluate %s: %w", evaluator.id, err)
		}
		if fired != nil {
			record, recordErr := recommendationToRecord(*fired, ruleVersion)
			if recordErr != nil {
				return nil, fmt.Errorf("record %s: %w", evaluator.id, recordErr)
			}
			records = append(records, record)
			continue
		}
		records = append(records, tombstoneFor(evaluator.id, snapshot, now, ruleVersion))
	}
	return records, nil
}

// tombstoneFor builds the fired=false row from the REGISTRY, never from the
// evaluator. See the file comment for why that distinction is load-bearing.
//
// The reference falls back to ("", "warning", "") when the registry has no
// entry for a rule id -- an empty title and success criterion with a severity
// that is NOT the registry default. Reproduced rather than improved on: a
// missing registry entry is a deployment mismatch, and inventing a title here
// would hide it behind a plausible row.
func tombstoneFor(
	ruleID string, snapshot MetricsSnapshot, now time.Time, ruleVersion string,
) RecommendationRecord {
	definition, known := ruleRegistry[ruleID]
	if !known {
		definition = ruleDefinition{title: "", severity: "warning", successCriterion: ""}
	}
	return RecommendationRecord{
		TeamID:      snapshot.TeamID,
		OrgID:       snapshot.OrgID,
		RuleID:      ruleID,
		RuleVersion: ruleVersion,
		WindowStart: snapshot.WindowStart,
		WindowEnd:   snapshot.WindowEnd,
		Fired:       false,
		Severity:    definition.severity,
		Title:       definition.title,
		// A tombstone has no rationale: nothing fired, so there is nothing to
		// explain. The reference writes an empty string, not a placeholder.
		Rationale:        "",
		SuccessCriterion: definition.successCriterion,
		// The literal "[]", not an encoded empty list. The reference never
		// calls json.dumps on this path (engine.py:223), so there is no
		// encoder to disagree with -- and hard-coding it keeps that true.
		EvidenceJSON: "[]",
		ComputedAt:   now,
	}
}

// recommendationToRecord ports loader.py's recommendation_to_record.
func recommendationToRecord(
	recommendation Recommendation, ruleVersion string,
) (RecommendationRecord, error) {
	evidenceJSON, err := encodeEvidenceJSON(recommendation.Evidence)
	if err != nil {
		return RecommendationRecord{}, err
	}
	return RecommendationRecord{
		TeamID:           recommendation.TeamID,
		OrgID:            recommendation.OrgID,
		RuleID:           recommendation.RuleID,
		RuleVersion:      ruleVersion,
		WindowStart:      recommendation.WindowStart,
		WindowEnd:        recommendation.WindowEnd,
		Fired:            true,
		Severity:         recommendation.Severity,
		Title:            recommendation.Title,
		Rationale:        recommendation.Rationale,
		SuccessCriterion: recommendation.SuccessCriterion,
		EvidenceJSON:     evidenceJSON,
		ComputedAt:       recommendation.ComputedAt,
	}, nil
}

// encodeEvidenceJSON reproduces `json.dumps(evidence_list)` from
// loader.py:448 -- a BARE dumps, with every argument at its default.
//
// That means insertion key order (NOT sorted), ", "/": " separators WITH
// spaces, ensure_ascii escaping, repr-style floats, and bare Infinity /
// -Infinity / NaN tokens which are not valid JSON. Go's encoding/json can
// reproduce none of those four, which is why this goes through
// MarshalPythonJSONInsertionOrder instead.
//
// The key order is the literal at loader.py:425-434 and is contract: the
// reader at api/graphql/resolvers/recommendations.py:115-120 names the same six
// keys in the same sequence.
func encodeEvidenceJSON(evidence []EvidenceRef) (string, error) {
	rows := make([]pythonparity.OrderedObject, 0, len(evidence))
	for _, reference := range evidence {
		rows = append(rows, pythonparity.OrderedObject{
			{Key: "team_id", Value: reference.TeamID},
			{Key: "metric_table", Value: reference.MetricTable},
			{Key: "window_start", Value: reference.WindowStart.Format("2006-01-02")},
			{Key: "window_end", Value: reference.WindowEnd.Format("2006-01-02")},
			{Key: "field", Value: reference.Field},
			{Key: "value", Value: reference.Value},
		})
	}
	encoded, err := pythonparity.MarshalPythonJSONInsertionOrder(rows)
	if err != nil {
		return "", fmt.Errorf("encode evidence_json: %w", err)
	}
	return string(encoded), nil
}
