package aianalytics

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graphqldate"
)

const scopeBreakdownLimit = 10

func validateRange(r model.AIDateRangeInput) error {
	start, end := r.StartDate.Time(), r.EndDate.Time()
	if end.Before(start) {
		return fmt.Errorf("AI analytics date range end_date must be >= start_date (got start=%s, end=%s)",
			start.Format(graphqldate.Layout), end.Format(graphqldate.Layout))
	}
	return nil
}

func missingState(key, title, guidance string) model.AIMissingState {
	return model.AIMissingState{Key: key, Title: title, Guidance: guidance}
}

func filterBuckets(rows []dailyRow, buckets []string) []dailyRow {
	if len(buckets) == 0 {
		return append([]dailyRow(nil), rows...)
	}
	keep := map[string]bool{}
	for _, b := range buckets {
		keep[b] = true
	}
	out := make([]dailyRow, 0, len(rows))
	for _, r := range rows {
		if keep[r.Bucket] {
			out = append(out, r)
		}
	}
	return out
}

func loadRows(ctx context.Context, client QueryClient, orgID string, dr model.AIDateRangeInput, in *model.AIScopeInput) ([]dailyRow, scope, error) {
	if err := validateRange(dr); err != nil {
		return nil, scope{}, err
	}
	sc, err := normalizeScope(ctx, client, orgID, in)
	if err != nil {
		return nil, sc, err
	}
	if sc.unresolved {
		return nil, sc, nil
	}
	rows, err := loadDaily(ctx, client, orgID, dr.StartDate.Time(), dr.EndDate.Time(), sc)
	return rows, sc, err
}

// ---- impact summary -------------------------------------------------------

func aggregateBucketTotals(rows []dailyRow) []model.AIImpactBucketTotals {
	groups, names := groupByBucket(rows)
	out := make([]model.AIImpactBucketTotals, 0, len(names))
	for _, name := range names {
		g := groups[name]
		prsComponent := 0.0
		if v := weightedAvg(pairsOfFloat(g, func(r dailyRow) *float64 { f := r.LevPrs; return &f }, byPrsTotal)); v != nil {
			prsComponent = *v
		}
		lev := &model.AILeverageComponents{
			PrsComponent:       prsComponent,
			CycleTimeComponent: weightedAvg(pairsOf(g, func(r dailyRow) *float64 { return r.LevCycle }, byPrsMerged)),
			ReviewComponent:    weightedAvg(pairsOf(g, func(r dailyRow) *float64 { return r.LevReview }, byPrsTotal)),
			ReworkComponent:    weightedAvg(pairsOf(g, func(r dailyRow) *float64 { return r.LevRework }, byPrsTotal)),
			TestComponent:      weightedAvg(pairsOf(g, func(r dailyRow) *float64 { return r.LevTest }, byPrsTotal)),
			IncidentComponent:  weightedAvg(pairsOf(g, func(r dailyRow) *float64 { return r.LevIncident }, byPrsTotal)),
		}
		out = append(out, model.AIImpactBucketTotals{
			Bucket:                name,
			PrsTotal:              int(sumInt(g, byPrsTotal)),
			PrsMerged:             int(sumInt(g, byPrsMerged)),
			AiAssistedPrRatio:     weightedAvg(pairsOf(g, func(r dailyRow) *float64 { return r.AIAssistedPrRatio }, byPrsTotal)),
			AgentCreatedPrCount:   int(sumInt(g, func(r dailyRow) int64 { return r.AgentCreatedPrs })),
			CycleTimeAvgHours:     weightedAvg(pairsOf(g, func(r dailyRow) *float64 { return r.CycleTimeAvgHours }, byPrsMerged)),
			AiCycleTimeDeltaHours: weightedAvg(pairsOf(g, func(r dailyRow) *float64 { return r.AICycleTimeDelta }, byPrsMerged)),
			AiReviewAmplification: weightedAvg(pairsOf(g, func(r dailyRow) *float64 { return r.AIReviewAmp }, byPrsTotal)),
			ReworkDragRate:        weightedAvg(pairsOf(g, func(r dailyRow) *float64 { return r.ReworkDragRate }, byPrsTotal)),
			RevertRate:            weightedAvg(pairsOf(g, func(r dailyRow) *float64 { return r.RevertRate }, byPrsTotal)),
			IncidentDragRate:      weightedAvg(pairsOf(g, func(r dailyRow) *float64 { return r.IncidentDragRate }, byPrsTotal)),
			TestGapRate:           weightedAvg(pairsOf(g, func(r dailyRow) *float64 { return r.TestGapRate }, byPrsTotal)),
			Leverage:              lev,
		})
	}
	return out
}

// pairsOfFloat is pairsOf for a value that is always present.
func pairsOfFloat(rows []dailyRow, value func(dailyRow) *float64, weight func(dailyRow) int64) []pair {
	return pairsOf(rows, value, weight)
}

func rowToDaily(r dailyRow) model.AIImpactBucketRow {
	return model.AIImpactBucketRow{
		Bucket:                r.Bucket,
		PrsTotal:              int(r.PrsTotal),
		PrsMerged:             int(r.PrsMerged),
		CycleTimeAvgHours:     r.CycleTimeAvgHours,
		ReviewsPerPr:          r.ReviewsPerPr,
		ChangesRequestedPerPr: r.ChangesRequestedPer,
		ReworkPrs:             int(r.ReworkPrs),
		ReworkRate:            r.ReworkDragRate,
		RevertPrs:             int(r.RevertPrs),
		RevertRate:            r.RevertRate,
		IncidentsCount:        int(r.IncidentsCount),
		IncidentRate:          r.IncidentDragRate,
		TestGapPrs:            int(r.TestGapPrs),
		TestGapRate:           r.TestGapRate,
	}
}

// scopeRollups aggregates rows into per-scope AI rollups. Rows with an empty
// scope key are skipped and scopes with no AI-attributed PRs are dropped.
func scopeRollups(rows []dailyRow, key func(dailyRow) string, labels map[string]string) []model.AIImpactScopeRollupRow {
	groups := map[string][]dailyRow{}
	var order []string
	for _, r := range rows {
		k := key(r)
		if k == "" {
			continue
		}
		if _, seen := groups[k]; !seen {
			order = append(order, k)
		}
		groups[k] = append(groups[k], r)
	}
	out := []model.AIImpactScopeRollupRow{}
	for _, id := range order {
		g := groups[id]
		var aiRows, humanRows []dailyRow
		for _, r := range g {
			if aiBuckets[r.Bucket] {
				aiRows = append(aiRows, r)
			}
			if r.Bucket == bucketHuman {
				humanRows = append(humanRows, r)
			}
		}
		aiTotal := sumInt(aiRows, byPrsTotal)
		if aiTotal <= 0 {
			continue
		}
		all := sumInt(g, byPrsTotal)
		aiRework := weightedAvg(pairsOf(aiRows, func(r dailyRow) *float64 { return r.ReworkDragRate }, byPrsTotal))
		humanRework := weightedAvg(pairsOf(humanRows, func(r dailyRow) *float64 { return r.ReworkDragRate }, byPrsTotal))
		label, ok := labels[id]
		if !ok {
			label = id
		}
		out = append(out, model.AIImpactScopeRollupRow{
			ScopeID:           id,
			ScopeLabel:        label,
			AiPrsTotal:        int(aiTotal),
			AiAssistedPrRatio: ratio(float64(aiTotal), float64(all)),
			ReworkRateDelta:   delta(aiRework, humanRework),
		})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].AiPrsTotal != out[j].AiPrsTotal {
			return out[i].AiPrsTotal > out[j].AiPrsTotal
		}
		return out[i].ScopeID < out[j].ScopeID
	})
	if len(out) > scopeBreakdownLimit {
		out = out[:scopeBreakdownLimit]
	}
	return out
}

func loadScopeBreakdowns(ctx context.Context, client QueryClient, orgID string, rows []dailyRow) (repos, teams []model.AIImpactScopeRollupRow) {
	if len(rows) == 0 {
		return []model.AIImpactScopeRollupRow{}, []model.AIImpactScopeRollupRow{}
	}
	repoSet, teamSet := map[string]bool{}, map[string]bool{}
	for _, r := range rows {
		repoSet[r.RepoID] = true
		if r.TeamID != "" {
			teamSet[r.TeamID] = true
		}
	}
	repoIDs, teamIDs := sortedKeys(repoSet), sortedKeys(teamSet)
	repoLabels, err := loadLabels(ctx, client, orgID, "repos", repoIDs)
	if err != nil {
		slog.WarnContext(ctx, "query_api.ai_analytics.labels_unavailable",
			"operation", "aiImpactSummary", "scope", "repo", "error", err.Error())
		repoLabels = map[string]string{}
	}
	teamLabels, err := loadLabels(ctx, client, orgID, "teams", teamIDs)
	if err != nil {
		slog.WarnContext(ctx, "query_api.ai_analytics.labels_unavailable",
			"operation", "aiImpactSummary", "scope", "team", "error", err.Error())
		teamLabels = map[string]string{}
	}
	repos = scopeRollups(rows, func(r dailyRow) string { return r.RepoID }, repoLabels)
	teams = scopeRollups(rows, func(r dailyRow) string { return r.TeamID }, teamLabels)
	return repos, teams
}

func sortedKeys(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for k, present := range set {
		if present {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

// ImpactSummary answers aiImpactSummary.
func ImpactSummary(ctx context.Context, client QueryClient, orgID string, dr model.AIDateRangeInput, in *model.AIScopeInput) (*model.AIImpactSummary, error) {
	if client == nil {
		return nil, fmt.Errorf("aianalytics: clickhouse client is required")
	}
	rows, sc, err := loadRows(ctx, client, orgID, dr, in)
	if err != nil {
		return nil, err
	}
	rows = filterBuckets(rows, sc.buckets)

	daily := make([]model.AIImpactBucketRow, 0, len(rows))
	for _, r := range rows {
		daily = append(daily, rowToDaily(r))
	}
	repoBreakdown, teamBreakdown := loadScopeBreakdowns(ctx, client, orgID, rows)

	total := sumInt(rows, byPrsTotal)
	aiAssisted := sumInt(rows, func(r dailyRow) int64 { return r.AIAssistedPrs })
	unknown := sumInt(rows, func(r dailyRow) int64 { return r.UnknownPrs })

	var computedAt *time.Time
	for _, r := range rows {
		if computedAt == nil || r.ComputedAt.After(*computedAt) {
			t := r.ComputedAt
			computedAt = &t
		}
	}

	missing := []model.AIMissingState{}
	if unknown > 0 {
		missing = append(missing, missingState(
			"unknown_attribution",
			"Unknown attribution needs follow-up",
			"Some PRs could not be attributed to AI-assisted, agent-created, "+
				"AI-reviewed, or human buckets. Treat this as a coverage gap for "+
				"labels, trailers, bot identities, or CI annotations, not as a "+
				"person-level usage signal."))
	}
	if len(rows) > 0 && len(repoBreakdown) == 0 && len(teamBreakdown) == 0 {
		missing = append(missing, missingState(
			"scope_breakdown",
			"No repo or team rollups with AI activity",
			"No repos or teams in this scope have AI-attributed PRs "+
				"in the window, so ranked rollups stay empty. This is a "+
				"no-data state, not a zero-impact signal."))
	}

	return &model.AIImpactSummary{
		OrgID:             orgID,
		StartDate:         dr.StartDate,
		EndDate:           dr.EndDate,
		TotalPrs:          int(total),
		AiAssistedPrs:     int(aiAssisted),
		AgentCreatedPrs:   int(sumInt(rows, func(r dailyRow) int64 { return r.AgentCreatedPrs })),
		HumanPrs:          int(sumInt(rows, func(r dailyRow) int64 { return r.HumanPrs })),
		UnknownPrs:        int(unknown),
		AiAssistedPrRatio: ratio(float64(aiAssisted), float64(total)),
		ByBucket:          aggregateBucketTotals(rows),
		Daily:             daily,
		RepoBreakdown:     repoBreakdown,
		TeamBreakdown:     teamBreakdown,
		MissingStates:     missing,
		DataAvailable:     len(rows) > 0,
		ComputedAt:        computedAt,
	}, nil
}

// ---- comparison -----------------------------------------------------------

func aggregateSide(rows []dailyRow, label string) *model.AIComparisonSide {
	if len(rows) == 0 {
		return &model.AIComparisonSide{Bucket: label}
	}
	return &model.AIComparisonSide{
		Bucket:            label,
		PrsTotal:          int(sumInt(rows, byPrsTotal)),
		PrsMerged:         int(sumInt(rows, byPrsMerged)),
		CycleTimeAvgHours: weightedAvg(pairsOf(rows, func(r dailyRow) *float64 { return r.CycleTimeAvgHours }, byPrsMerged)),
		ReviewsPerPr:      weightedAvg(pairsOf(rows, func(r dailyRow) *float64 { return r.ReviewsPerPr }, byPrsTotal)),
		ReworkRate:        weightedAvg(pairsOf(rows, func(r dailyRow) *float64 { return r.ReworkDragRate }, byPrsTotal)),
		RevertRate:        weightedAvg(pairsOf(rows, func(r dailyRow) *float64 { return r.RevertRate }, byPrsTotal)),
		TestGapRate:       weightedAvg(pairsOf(rows, func(r dailyRow) *float64 { return r.TestGapRate }, byPrsTotal)),
		IncidentRate:      weightedAvg(pairsOf(rows, func(r dailyRow) *float64 { return r.IncidentDragRate }, byPrsTotal)),
	}
}

// Comparison answers aiComparison.
func Comparison(ctx context.Context, client QueryClient, orgID string, dr model.AIDateRangeInput, in *model.AIScopeInput) (*model.AIComparison, error) {
	if client == nil {
		return nil, fmt.Errorf("aianalytics: clickhouse client is required")
	}
	all, _, err := loadRows(ctx, client, orgID, dr, in)
	if err != nil {
		return nil, err
	}
	var aiRows, baseline []dailyRow
	for _, r := range all {
		if aiBuckets[r.Bucket] {
			aiRows = append(aiRows, r)
		}
		if r.Bucket == bucketHuman {
			baseline = append(baseline, r)
		}
	}
	ai := aggregateSide(aiRows, "ai")
	base := aggregateSide(baseline, bucketHuman)
	return &model.AIComparison{
		OrgID:        orgID,
		StartDate:    dr.StartDate,
		EndDate:      dr.EndDate,
		AiSide:       ai,
		BaselineSide: base,
		Delta: &model.AIComparisonDelta{
			CycleTimeDeltaHours: delta(ai.CycleTimeAvgHours, base.CycleTimeAvgHours),
			ReviewsPerPrDelta:   delta(ai.ReviewsPerPr, base.ReviewsPerPr),
			ReworkRateDelta:     delta(ai.ReworkRate, base.ReworkRate),
			RevertRateDelta:     delta(ai.RevertRate, base.RevertRate),
			TestGapRateDelta:    delta(ai.TestGapRate, base.TestGapRate),
			IncidentRateDelta:   delta(ai.IncidentRate, base.IncidentRate),
		},
		DataAvailable: len(aiRows) > 0 || len(baseline) > 0,
	}, nil
}

// ---- review load ----------------------------------------------------------

// engagementSlice accumulates review-engagement inputs for one (bucket, day)
// or one bucket.
type engagementSlice struct {
	latency  []pair
	comments int64
	loc      int64
}

func (s *engagementSlice) add(r engagementRow) {
	s.latency = append(s.latency, pair{value: r.PickupLatency, weight: r.PrsWithFirstReview})
	s.comments = s.comments + r.CommentsTotal
	s.loc = s.loc + r.LocTotal
}

func (s *engagementSlice) pickup() *float64 { return weightedAvg(s.latency) }

func (s *engagementSlice) commentsPerLoc() *float64 {
	if s.loc <= 0 {
		return nil
	}
	v := float64(s.comments) / float64(s.loc)
	return &v
}

func loadEngagementSlices(ctx context.Context, client QueryClient, orgID string, dr model.AIDateRangeInput, sc scope) (map[string]*engagementSlice, map[string]*engagementSlice) {
	byDay, byBucket := map[string]*engagementSlice{}, map[string]*engagementSlice{}
	if sc.workType != "" {
		return byDay, byBucket
	}
	var teamIDs []string
	useIDs := false
	if sc.teamID != "" {
		ids, ok := teamRepoIDs(ctx, client, orgID, sc.teamID, "aiReviewLoad")
		if !ok || len(ids) == 0 {
			return byDay, byBucket
		}
		teamIDs, useIDs = ids, true
	}
	raw, err := loadEngagement(ctx, client, orgID, dr.StartDate.Time(), dr.EndDate.Time(), sc.repoID, teamIDs, useIDs)
	if err != nil {
		slog.WarnContext(ctx, "query_api.ai_analytics.engagement_unavailable",
			"operation", "aiReviewLoad", "error", err.Error())
		return map[string]*engagementSlice{}, map[string]*engagementSlice{}
	}
	for _, r := range raw {
		bucket := r.Bucket
		if bucket == "" {
			bucket = "unknown"
		}
		dayKey := bucket + "\x00" + r.Day
		if byDay[dayKey] == nil {
			byDay[dayKey] = &engagementSlice{}
		}
		byDay[dayKey].add(r)
		if byBucket[bucket] == nil {
			byBucket[bucket] = &engagementSlice{}
		}
		byBucket[bucket].add(r)
	}
	return byDay, byBucket
}

// ReviewLoad answers aiReviewLoad.
func ReviewLoad(ctx context.Context, client QueryClient, orgID string, dr model.AIDateRangeInput, in *model.AIScopeInput) (*model.AIReviewLoadResult, error) {
	if client == nil {
		return nil, fmt.Errorf("aianalytics: clickhouse client is required")
	}
	rows, sc, err := loadRows(ctx, client, orgID, dr, in)
	if err != nil {
		return nil, err
	}
	var byDay, byBucketEng map[string]*engagementSlice
	if sc.unresolved {
		byDay, byBucketEng = map[string]*engagementSlice{}, map[string]*engagementSlice{}
	} else {
		byDay, byBucketEng = loadEngagementSlices(ctx, client, orgID, dr, sc)
	}

	daily := make([]model.AIReviewLoadRow, 0, len(rows))
	for _, r := range rows {
		row := model.AIReviewLoadRow{
			Bucket:                     r.Bucket,
			PrsTotal:                   int(r.PrsTotal),
			ReviewsTotal:               int(reviewTotalForRow(r)),
			ReviewsPerPr:               r.ReviewsPerPr,
			ChangesRequestedPerPr:      r.ChangesRequestedPer,
			ReviewAmplification:        r.AIReviewAmp,
			PostFirstReviewPushesCount: int(r.FollowupCommits),
			PostFirstReviewPushesPerPr: ratio(float64(r.FollowupCommits), float64(r.PrsTotal)),
		}
		if s := byDay[r.Bucket+"\x00"+r.Day]; s != nil {
			row.PickupLatencyHours = s.pickup()
			row.ReviewCommentsPerLoc = s.commentsPerLoc()
		}
		daily = append(daily, row)
	}

	groups, names := groupByBucket(rows)
	byBucket := make([]model.AIReviewLoadRow, 0, len(names))
	for _, name := range names {
		g := groups[name]
		prs := sumInt(g, byPrsTotal)
		var reviews int64
		for _, r := range g {
			reviews = reviews + reviewTotalForRow(r)
		}
		pushes := sumInt(g, func(r dailyRow) int64 { return r.FollowupCommits })
		row := model.AIReviewLoadRow{
			Bucket:                     name,
			PrsTotal:                   int(prs),
			ReviewsTotal:               int(reviews),
			ReviewsPerPr:               weightedAvg(pairsOf(g, func(r dailyRow) *float64 { return r.ReviewsPerPr }, byPrsTotal)),
			ChangesRequestedPerPr:      weightedAvg(pairsOf(g, func(r dailyRow) *float64 { return r.ChangesRequestedPer }, byPrsTotal)),
			ReviewAmplification:        weightedAvg(pairsOf(g, func(r dailyRow) *float64 { return r.AIReviewAmp }, byPrsTotal)),
			PostFirstReviewPushesCount: int(pushes),
			PostFirstReviewPushesPerPr: ratio(float64(pushes), float64(prs)),
		}
		if s := byBucketEng[name]; s != nil {
			row.PickupLatencyHours = s.pickup()
			row.ReviewCommentsPerLoc = s.commentsPerLoc()
		}
		byBucket = append(byBucket, row)
	}

	concentration := &model.AIReviewerConcentrationSummary{DataAvailable: false, ReviewerCount: 0}
	if !sc.unresolved {
		g, n, err := loadReviewerConcentration(ctx, client, orgID, dr.StartDate.Time(), dr.EndDate.Time(), sc)
		if err != nil {
			return nil, err
		}
		concentration = &model.AIReviewerConcentrationSummary{DataAvailable: g != nil, ReviewerCount: n, ReviewerGini: g}
	}

	missing := []model.AIMissingState{}
	if len(rows) > 0 && len(byBucketEng) == 0 {
		missing = append(missing, missingState(
			"review_engagement",
			"Pickup latency and comment density unavailable",
			"Pickup latency and review comments per changed line could "+
				"not be computed for this scope — the raw pull-request "+
				"review timestamps or size fields are missing, or the "+
				"scope (work type, unresolved team) cannot be applied to "+
				"raw PR rows. Treat as no-data, not as zero."))
	}
	if !concentration.DataAvailable {
		missing = append(missing, missingState(
			"reviewer_concentration",
			"Reviewer concentration needs aggregate coverage",
			"Reviewer concentration is only shown as an aggregate "+
				"distribution signal. No reviewer names, rankings, or "+
				"person-level review counts are exposed."))
	}

	return &model.AIReviewLoadResult{
		OrgID:                 orgID,
		StartDate:             dr.StartDate,
		EndDate:               dr.EndDate,
		ByBucket:              byBucket,
		Daily:                 daily,
		ReviewerConcentration: concentration,
		MissingStates:         missing,
		DataAvailable:         len(rows) > 0,
	}, nil
}
