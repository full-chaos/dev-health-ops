package aianalytics

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/changefailure"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/activeteams"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse/liverow"
)

const (
	reviewLatencyThresholdHours = 24.0
	cycleTimeThresholdHours     = 120.0
	reworkRatioThreshold        = 0.20
	wipCongestionThreshold      = 0.40
	lowThroughputThreshold      = 2.0
	highChurnThreshold          = 0.30
	changeFailureThreshold      = 0.15
	minDataDays                 = 5
)

// flowKind is an improve opportunity kind with its stored spelling.
type flowKind struct {
	enum   model.ImproveOpportunityKind
	stored string
	action string
}

var (
	kindReviewLatency = flowKind{model.ImproveOpportunityKindHighReviewLatency, "high_review_latency", "Reserve a daily review block for PRs waiting longest for first response."}
	kindSlowCycle     = flowKind{model.ImproveOpportunityKindSlowCycleTime, "slow_cycle_time", "Trace the oldest active items to their current waiting state."}
	kindRework        = flowKind{model.ImproveOpportunityKindHighRework, "high_rework", "Compare reopened or rewritten work against its original acceptance criteria."}
	kindWIP           = flowKind{model.ImproveOpportunityKindHighWip, "high_wip", "Set a short-term WIP limit and finish active items before starting more."}
	kindThroughput    = flowKind{model.ImproveOpportunityKindLowThroughput, "low_throughput", "Audit recently completed items for the smallest repeatable delivery pattern."}
	kindChurn         = flowKind{model.ImproveOpportunityKindHighChurn, "high_churn", "Review the files with the largest churn increase for unclear ownership or scope."}
	kindChangeFailure = flowKind{model.ImproveOpportunityKindHighChangeFailure, "high_change_failure", "Review recent failed changes for the earliest detectable signal before release."}
)

// flowRow is one aggregated metric row of an entity.
type flowRow struct {
	EntityID string
	// repo entities
	ReviewP50, ReworkRatio, ChurnRatio, ChangeFailure *float64
	// team entities
	CycleP50, WipCongestion, ItemsCompleted *float64
}

func severity(score float64) string {
	if score >= 0.75 {
		return "high"
	}
	if score >= 0.50 {
		return "medium"
	}
	return "low"
}

// flowMeasure is what a rule compared: the measured value, the rule's constant limit, their unit and
// the side of the limit that fires the rule. The rationale states the same two numbers; here they are
// fields, so no client has to read them out of the sentence.
type flowMeasure struct {
	Value     float64
	Threshold float64
	Unit      model.ImproveOpportunityUnit
	Direction model.ThresholdDirection
}

func above(value, threshold float64, unit model.ImproveOpportunityUnit) flowMeasure {
	return flowMeasure{Value: value, Threshold: threshold, Unit: unit, Direction: model.ThresholdDirectionAbove}
}

func below(value, threshold float64, unit model.ImproveOpportunityUnit) flowMeasure {
	return flowMeasure{Value: value, Threshold: threshold, Unit: unit, Direction: model.ThresholdDirectionBelow}
}

func makeFlow(kind flowKind, entityType, entityID, title, why string, score float64, refs []string, measure flowMeasure) model.ImproveOpportunity {
	clamped := clamp01(score)
	return model.ImproveOpportunity{
		OpportunityID:      stableOpportunityID(kind.stored, entityID, ""),
		Kind:               kind.enum,
		EntityType:         entityType,
		EntityID:           entityID,
		Title:              title,
		Rationale:          why,
		Score:              clamped,
		Severity:           severity(clamped),
		EvidenceRefs:       refs,
		RecommendedAction:  kind.action,
		Value:              measure.Value,
		Threshold:          measure.Threshold,
		Unit:               measure.Unit,
		ThresholdDirection: measure.Direction,
	}
}

type flowRule func(row flowRow, windowDays int) *model.ImproveOpportunity

func days(windowDays int) string { return fmt.Sprintf("%d", windowDays) }

var repoRules = []flowRule{
	func(r flowRow, w int) *model.ImproveOpportunity {
		if r.ReviewP50 == nil || *r.ReviewP50 <= reviewLatencyThresholdHours {
			return nil
		}
		o := makeFlow(kindReviewLatency, "repo", r.EntityID, "High review latency in "+r.EntityID,
			"Median first-review time was "+fixed(*r.ReviewP50, 1)+" h over the last "+days(w)+" days (threshold: "+fixed(reviewLatencyThresholdHours, 0)+" h).",
			scoreRatio(*r.ReviewP50, reviewLatencyThresholdHours),
			[]string{"repo_metrics_daily:pr_first_review_p50_hours:" + r.EntityID},
			above(*r.ReviewP50, reviewLatencyThresholdHours, model.ImproveOpportunityUnitHours))
		return &o
	},
	func(r flowRow, w int) *model.ImproveOpportunity {
		if r.ReworkRatio == nil || *r.ReworkRatio <= reworkRatioThreshold {
			return nil
		}
		o := makeFlow(kindRework, "repo", r.EntityID, "High rework ratio in "+r.EntityID,
			"PR rework ratio was "+pct(*r.ReworkRatio, 0)+" over the last "+days(w)+" days (threshold: "+pct(reworkRatioThreshold, 0)+").",
			scoreRatio(*r.ReworkRatio, reworkRatioThreshold),
			[]string{"repo_metrics_daily:pr_rework_ratio:" + r.EntityID},
			above(*r.ReworkRatio, reworkRatioThreshold, model.ImproveOpportunityUnitRatio))
		return &o
	},
	func(r flowRow, _ int) *model.ImproveOpportunity {
		if r.ChurnRatio == nil || *r.ChurnRatio <= highChurnThreshold {
			return nil
		}
		o := makeFlow(kindChurn, "repo", r.EntityID, "High rework churn in "+r.EntityID,
			"Rework churn ratio was "+pct(*r.ChurnRatio, 0)+" over the last 30 days (threshold: "+pct(highChurnThreshold, 0)+").",
			scoreRatio(*r.ChurnRatio, highChurnThreshold),
			[]string{"repo_metrics_daily:rework_churn_ratio_30d:" + r.EntityID},
			above(*r.ChurnRatio, highChurnThreshold, model.ImproveOpportunityUnitRatio))
		return &o
	},
	func(r flowRow, w int) *model.ImproveOpportunity {
		if r.ChangeFailure == nil || *r.ChangeFailure <= changeFailureThreshold {
			return nil
		}
		o := makeFlow(kindChangeFailure, "repo", r.EntityID, "High change failure rate in "+r.EntityID,
			"Change failure rate was "+pct(*r.ChangeFailure, 0)+" over the last "+days(w)+" days (threshold: "+pct(changeFailureThreshold, 0)+").",
			scoreRatio(*r.ChangeFailure, changeFailureThreshold),
			[]string{changefailure.Table + ":change_failure_rate:" + r.EntityID},
			above(*r.ChangeFailure, changeFailureThreshold, model.ImproveOpportunityUnitRatio))
		return &o
	},
}

var teamRules = []flowRule{
	func(r flowRow, w int) *model.ImproveOpportunity {
		if r.CycleP50 == nil || *r.CycleP50 <= cycleTimeThresholdHours {
			return nil
		}
		o := makeFlow(kindSlowCycle, "team", r.EntityID, "Slow cycle time for "+r.EntityID,
			"Median cycle time was "+fixed(*r.CycleP50, 1)+" h over the last "+days(w)+" days (threshold: "+fixed(cycleTimeThresholdHours, 0)+" h).",
			scoreRatio(*r.CycleP50, cycleTimeThresholdHours),
			[]string{"work_item_metrics_daily:cycle_time_p50_hours:" + r.EntityID},
			above(*r.CycleP50, cycleTimeThresholdHours, model.ImproveOpportunityUnitHours))
		return &o
	},
	func(r flowRow, w int) *model.ImproveOpportunity {
		if r.WipCongestion == nil || *r.WipCongestion <= wipCongestionThreshold {
			return nil
		}
		o := makeFlow(kindWIP, "team", r.EntityID, "High WIP congestion for "+r.EntityID,
			"WIP congestion ratio was "+pct(*r.WipCongestion, 0)+" over the last "+days(w)+" days (threshold: "+pct(wipCongestionThreshold, 0)+").",
			scoreRatio(*r.WipCongestion, wipCongestionThreshold),
			[]string{"work_item_metrics_daily:wip_congestion_ratio:" + r.EntityID},
			above(*r.WipCongestion, wipCongestionThreshold, model.ImproveOpportunityUnitRatio))
		return &o
	},
	func(r flowRow, w int) *model.ImproveOpportunity {
		if r.ItemsCompleted == nil || *r.ItemsCompleted >= lowThroughputThreshold {
			return nil
		}
		gap := pythonparity.Max2(0.0, lowThroughputThreshold-*r.ItemsCompleted)
		o := makeFlow(kindThroughput, "team", r.EntityID, "Low throughput for "+r.EntityID,
			"Only "+fixed(*r.ItemsCompleted, 0)+" items were completed over the last "+days(w)+" days (threshold: "+fixed(lowThroughputThreshold, 0)+").",
			scoreDelta(gap, lowThroughputThreshold),
			[]string{"work_item_metrics_daily:items_completed:" + r.EntityID},
			below(*r.ItemsCompleted, lowThroughputThreshold, model.ImproveOpportunityUnitItems))
		return &o
	},
}

func applyFlowRules(rows []flowRow, rules []flowRule, windowDays int) []model.ImproveOpportunity {
	var out []model.ImproveOpportunity
	for _, row := range rows {
		for _, rule := range rules {
			if o := rule(row, windowDays); o != nil {
				out = append(out, *o)
			}
		}
	}
	return out
}

// repoFlowStatement reads each repository's window averages from
// repo_metrics_daily and its change failure rate from the window's summed
// counts in repo_change_failure_daily (changefailure.WindowRateSQL, CHAOS-8981):
// NULL, and so no opportunity, when the window has no deployment or no
// incident evidence.
var repoFlowStatement = `SELECT
    toString(m.repo_id) AS entity_id,
    m.data_days AS data_days,
    m.pr_first_review_p50_hours AS pr_first_review_p50_hours,
    m.pr_rework_ratio AS pr_rework_ratio,
    m.rework_churn_ratio_30d AS rework_churn_ratio_30d,
    cf.change_failure_rate AS change_failure_rate
FROM (
    SELECT
        repo_id,
        uniqExact(day) AS data_days,
        toNullable(avg(pr_first_review_p50_hours)) AS pr_first_review_p50_hours,
        toNullable(avg(pr_rework_ratio)) AS pr_rework_ratio,
        toNullable(avg(rework_churn_ratio_30d)) AS rework_churn_ratio_30d
    FROM (
        SELECT *
        FROM repo_metrics_daily
        WHERE org_id = {org_id:String}
        ORDER BY computed_at DESC
        LIMIT 1 BY org_id, repo_id, day
    ) AS repo_metrics_daily
    WHERE day >= today() - {window_days:UInt32}@@REPO@@
      AND org_id = {org_id:String}
    GROUP BY repo_id
    HAVING data_days >= 5
) AS m
LEFT JOIN (
    SELECT repo_id, ` + changefailure.WindowRateSQL + ` AS change_failure_rate
    FROM (
        SELECT repo_id, day, ` + strings.Join(changefailure.CountColumns, ", ") + `
        FROM ` + changefailure.Table + `
        WHERE org_id = {org_id:String}
          AND day >= today() - {window_days:UInt32}@@REPO@@
        ORDER BY computed_at DESC
        LIMIT 1 BY org_id, repo_id, day
    )
    GROUP BY repo_id
) AS cf ON m.repo_id = cf.repo_id
ORDER BY data_days DESC
LIMIT 500`

// A retraction row (see package liverow) is left out: it is not a day of data
// of its team, and the 0 it holds in wip_congestion_ratio is not a measured 0.
var teamFlowStatement = `SELECT
    team_id AS entity_id,
    uniqExact(day) AS data_days,
    toNullable(avg(cycle_time_p50_hours)) AS cycle_time_p50_hours,
    toNullable(avg(wip_congestion_ratio)) AS wip_congestion_ratio,
    toNullable(toFloat64(sum(items_completed))) AS items_completed
FROM work_item_metrics_daily FINAL
WHERE day >= today() - {window_days:UInt32}@@TEAM@@
  AND org_id = {org_id:String}
  AND team_id != ''
  AND ` + liverow.Predicate("work_item_metrics_daily", "") + `
GROUP BY team_id
HAVING data_days >= 5
ORDER BY data_days DESC
LIMIT 500`

// loadFlowRows reads the repository and team aggregates. Both reads are
// always issued, and either failing fails the whole load.
func loadFlowRows(ctx context.Context, client QueryClient, orgID, repoID, teamID string, windowDays int) ([]flowRow, []flowRow, error) {
	bindings := []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "window_days", Value: uint32(windowDays)},
	}
	repos, repoErr := loadRepoFlowRows(ctx, client, bindings, repoID)
	teams, teamErr := loadTeamFlowRows(ctx, client, bindings, teamID)
	if repoErr != nil {
		return nil, nil, repoErr
	}
	if teamErr != nil {
		return nil, nil, teamErr
	}
	return repos, teams, nil
}

func loadRepoFlowRows(ctx context.Context, client QueryClient, base []clickhouse.Binding, repoID string) ([]flowRow, error) {
	bindings := append([]clickhouse.Binding(nil), base...)
	statement := strings.ReplaceAll(repoFlowStatement, "@@REPO@@", "")
	if repoID != "" {
		bindings = append(bindings, clickhouse.Binding{Name: "repo_id", Value: repoID})
		statement = strings.ReplaceAll(repoFlowStatement, "@@REPO@@", "\n  AND repo_id = {repo_id:UUID}")
	}
	rs, err := client.Query(ctx, statement, bindings)
	if err != nil {
		return nil, fmt.Errorf("repo metrics query: %w", err)
	}
	defer rs.Close()
	var out []flowRow
	for rs.Next() {
		var (
			r        flowRow
			dataDays uint64
		)
		if err := rs.Scan(&r.EntityID, &dataDays, &r.ReviewP50, &r.ReworkRatio, &r.ChurnRatio, &r.ChangeFailure); err != nil {
			return nil, fmt.Errorf("repo metrics scan: %w", err)
		}
		out = append(out, r)
	}
	if err := rs.Err(); err != nil {
		return nil, fmt.Errorf("repo metrics rows: %w", err)
	}
	return out, nil
}

func loadTeamFlowRows(ctx context.Context, client QueryClient, base []clickhouse.Binding, teamID string) ([]flowRow, error) {
	bindings := append([]clickhouse.Binding(nil), base...)
	// With no team named, the entities are the active teams (and the
	// documented no-team value): a team id that only survives in stored rows
	// (a bare id that was retired for a provider-keyed id) is not a team. A
	// named team is read as named: an id the caller holds may be a retired one.
	statement := strings.ReplaceAll(teamFlowStatement, "@@TEAM@@", "\n  AND "+activeteams.ListablePredicate("team_id"))
	if teamID != "" {
		bindings = append(bindings, clickhouse.Binding{Name: "team_id", Value: teamID})
		statement = strings.ReplaceAll(teamFlowStatement, "@@TEAM@@", "\n  AND team_id = {team_id:String}")
	}
	rs, err := client.Query(ctx, statement, bindings)
	if err != nil {
		return nil, fmt.Errorf("team metrics query: %w", err)
	}
	defer rs.Close()
	var out []flowRow
	for rs.Next() {
		var (
			r        flowRow
			dataDays uint64
		)
		if err := rs.Scan(&r.EntityID, &dataDays, &r.CycleP50, &r.WipCongestion, &r.ItemsCompleted); err != nil {
			return nil, fmt.Errorf("team metrics scan: %w", err)
		}
		out = append(out, r)
	}
	if err := rs.Err(); err != nil {
		return nil, fmt.Errorf("team metrics rows: %w", err)
	}
	return out, nil
}

// boundedFlowLimit keeps a response between 1 and 100 opportunities.
func boundedFlowLimit(limit int) int {
	if limit > 100 {
		return 100
	}
	if limit < 1 {
		return 1
	}
	return limit
}

// boundedWindow keeps the metric window between 1 and 365 days.
func boundedWindow(days int) int {
	if days > 365 {
		return 365
	}
	if days < 1 {
		return 1
	}
	return days
}

// FlowOpportunities answers improveOpportunities. A failed metric read
// answers an empty list, as the detector it ports does, and is logged.
func FlowOpportunities(ctx context.Context, client QueryClient, orgID string, in *model.AIScopeInput, limit, windowDays int) (*model.ImproveOpportunitiesResult, error) {
	if client == nil {
		return nil, fmt.Errorf("aianalytics: clickhouse client is required")
	}
	bounded, window := boundedFlowLimit(limit), boundedWindow(windowDays)
	repoID, teamID := "", ""
	if in != nil {
		if in.RepoID != nil && *in.RepoID != "" {
			if u, err := pythonparity.ParseUUID(*in.RepoID); err == nil {
				repoID = u.String()
			}
		}
		if in.TeamID != nil {
			teamID = *in.TeamID
		}
	}

	opps := []model.ImproveOpportunity{}
	repos, teams, err := loadFlowRows(ctx, client, orgID, repoID, teamID, window)
	if err != nil {
		slog.ErrorContext(ctx, "query_api.ai_analytics.flow_detector_unavailable",
			"operation", "improveOpportunities", "error", err.Error())
	} else {
		opps = append(opps, applyFlowRules(repos, repoRules, window)...)
		opps = append(opps, applyFlowRules(teams, teamRules, window)...)
		sort.SliceStable(opps, func(i, j int) bool { return opps[i].Score > opps[j].Score })
		if len(opps) > bounded {
			opps = opps[:bounded]
		}
		nameFlowOpportunities(ctx, client, orgID, opps)
	}
	return &model.ImproveOpportunitiesResult{
		OrgID: orgID, Opportunities: opps, DetectorReady: true, TotalCount: len(opps),
	}, nil
}

// nameFlowOpportunities fills EntityDisplayName for the returned page: the
// repository full name for a repo entity, the team name for a team entity. A
// name is never needed to decide what is returned, and an entity with no
// stored name keeps nil.
func nameFlowOpportunities(ctx context.Context, client QueryClient, orgID string, opps []model.ImproveOpportunity) {
	if len(opps) == 0 {
		return
	}
	var repoIDs []string
	seen := map[string]bool{}
	for _, o := range opps {
		if o.EntityType == "repo" && !seen[o.EntityID] {
			seen[o.EntityID] = true
			repoIDs = append(repoIDs, o.EntityID)
		}
	}
	catalogue := loadRepoCatalogue(ctx, client, orgID, repoIDs, "improveOpportunities")
	if len(repoIDs) == 0 {
		catalogue = loadTeamNamesOnly(ctx, client, orgID, "improveOpportunities")
	}
	for i := range opps {
		id := opps[i].EntityID
		switch opps[i].EntityType {
		case "repo":
			opps[i].EntityDisplayName = catalogue.repoName(id)
		case "team":
			opps[i].EntityDisplayName = catalogue.teamName(&id)
		}
	}
}
