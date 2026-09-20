package aianalytics

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sort"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
)

const (
	maxAttributedPRsPage       = 200
	maxAttributionEvidencePage = 200
)

func finiteOrNil(v *float64) *float64 {
	if v == nil || math.IsNaN(*v) || math.IsInf(*v, 0) {
		return nil
	}
	return v
}

// ---- risk breakdown -------------------------------------------------------

// RiskBreakdown answers aiRiskBreakdown.
func RiskBreakdown(ctx context.Context, client QueryClient, orgID string, dr model.AIDateRangeInput, in *model.AIScopeInput) (*model.AIRiskBreakdownResult, error) {
	if client == nil {
		return nil, fmt.Errorf("aianalytics: clickhouse client is required")
	}
	rows, sc, err := loadRows(ctx, client, orgID, dr, in)
	if err != nil {
		return nil, err
	}
	groups, names := groupByBucket(rows)
	byBucket := make([]model.AIRiskBreakdownRow, 0, len(names))
	for _, name := range names {
		g := groups[name]
		prs := sumInt(g, byPrsTotal)
		rework := sumInt(g, func(r dailyRow) int64 { return r.ReworkPrs })
		revert := sumInt(g, func(r dailyRow) int64 { return r.RevertPrs })
		testGap := sumInt(g, func(r dailyRow) int64 { return r.TestGapPrs })
		incidents := sumInt(g, func(r dailyRow) int64 { return r.IncidentsCount })
		byBucket = append(byBucket, model.AIRiskBreakdownRow{
			Bucket: name, PrsTotal: int(prs),
			ReworkPrs: int(rework), ReworkRate: ratio(float64(rework), float64(prs)),
			RevertPrs: int(revert), RevertRate: ratio(float64(revert), float64(prs)),
			TestGapPrs: int(testGap), TestGapRate: ratio(float64(testGap), float64(prs)),
			IncidentsCount: int(incidents), IncidentRate: ratio(float64(incidents), float64(prs)),
		})
	}

	hotspots, complexity := []model.AIHotspotOverlapRow{}, []model.AIComplexityOverlapRow{}
	if !sc.unresolved && sc.workType == "" {
		hotspots, complexity = loadOverlapRows(ctx, client, orgID, dr, sc)
	}

	missing := []model.AIMissingState{}
	if len(hotspots) == 0 {
		missing = append(missing, missingState(
			"hotspot_overlap",
			"Hotspot overlap has no assessable AI PRs",
			"No AI-attributed PRs in this window could be joined to "+
				"hotspot files — either no AI PRs have commit/file "+
				"linkage yet or no hotspot snapshots exist for the "+
				"window. Treat missing overlap as no data, not as no "+
				"risk."))
	}
	if len(complexity) == 0 {
		missing = append(missing, missingState(
			"complexity_overlap",
			"Complexity overlap has no assessable AI PRs",
			"No AI-attributed PRs in this window could be joined to "+
				"high-complexity files. Drill into PR and Work Graph "+
				"evidence when available; do not infer person-level "+
				"quality from this gap."))
	}
	return &model.AIRiskBreakdownResult{
		OrgID: orgID, StartDate: dr.StartDate, EndDate: dr.EndDate,
		ByBucket: byBucket, HotspotOverlap: hotspots, ComplexityOverlap: complexity,
		MissingStates: missing, DataAvailable: len(rows) > 0,
	}, nil
}

// loadOverlapRows is best effort: an unresolvable team or a failed query
// degrades to no rows, which the caller renders as an explicit missing state.
func loadOverlapRows(ctx context.Context, client QueryClient, orgID string, dr model.AIDateRangeInput, sc scope) ([]model.AIHotspotOverlapRow, []model.AIComplexityOverlapRow) {
	var teamIDs []string
	useIDs := false
	if sc.teamID != "" {
		ids, ok := teamRepoIDs(ctx, client, orgID, sc.teamID, "aiRiskBreakdown")
		if !ok || len(ids) == 0 {
			return []model.AIHotspotOverlapRow{}, []model.AIComplexityOverlapRow{}
		}
		teamIDs, useIDs = ids, true
	}
	start, end := dr.StartDate.Time(), dr.EndDate.Time()

	hotspots := []model.AIHotspotOverlapRow{}
	raw, err := loadOverlap(ctx, client, orgID, start, end, sc.repoID, teamIDs, useIDs, true)
	if err != nil {
		slog.WarnContext(ctx, "query_api.ai_analytics.overlap_unavailable",
			"operation", "aiRiskBreakdown", "overlap", "hotspot", "error", err.Error())
	}
	for _, r := range raw {
		if r.PrsTotal <= 0 {
			continue
		}
		hotspots = append(hotspots, model.AIHotspotOverlapRow{
			Bucket: bucketOrUnknown(r.Bucket), PrsTotal: int(r.PrsTotal),
			PrsTouchingHotspots: int(r.PrsTouching),
			HotspotOverlapRate:  ratio(float64(r.PrsTouching), float64(r.PrsTotal)),
			AvgHotspotRiskScore: finiteOrNil(r.AvgRisk),
		})
	}
	sort.SliceStable(hotspots, func(i, j int) bool { return hotspots[i].Bucket < hotspots[j].Bucket })

	complexity := []model.AIComplexityOverlapRow{}
	raw, err = loadOverlap(ctx, client, orgID, start, end, sc.repoID, teamIDs, useIDs, false)
	if err != nil {
		slog.WarnContext(ctx, "query_api.ai_analytics.overlap_unavailable",
			"operation", "aiRiskBreakdown", "overlap", "complexity", "error", err.Error())
	}
	for _, r := range raw {
		if r.PrsTotal <= 0 {
			continue
		}
		complexity = append(complexity, model.AIComplexityOverlapRow{
			Bucket: bucketOrUnknown(r.Bucket), PrsTotal: int(r.PrsTotal),
			PrsTouchingHighComplexity: int(r.PrsTouching),
			ComplexityOverlapRate:     ratio(float64(r.PrsTouching), float64(r.PrsTotal)),
		})
	}
	sort.SliceStable(complexity, func(i, j int) bool { return complexity[i].Bucket < complexity[j].Bucket })
	return hotspots, complexity
}

func bucketOrUnknown(b string) string {
	if b == "" {
		return "unknown"
	}
	return b
}

// ---- attributed pull requests --------------------------------------------

func clampPage(limit, offset, maxPage int) (int, int) {
	size := limit
	if size > maxPage {
		size = maxPage
	}
	if size < 1 {
		size = 1
	}
	off := offset
	if off < 0 {
		off = 0
	}
	return size, off
}

// AttributedPrs answers aiAttributedPrs.
func AttributedPrs(ctx context.Context, client QueryClient, orgID string, dr model.AIDateRangeInput, in *model.AIScopeInput, limit, offset int) (*model.AiAttributedPrsResult, error) {
	if client == nil {
		return nil, fmt.Errorf("aianalytics: clickhouse client is required")
	}
	if err := validateRange(dr); err != nil {
		return nil, err
	}
	sc, err := normalizeScope(ctx, client, orgID, in)
	if err != nil {
		return nil, err
	}
	empty := &model.AiAttributedPrsResult{
		OrgID: orgID, StartDate: dr.StartDate, EndDate: dr.EndDate, Rows: []model.AiAttributedPr{},
	}
	if sc.unresolved {
		return empty, nil
	}
	pageSize, pageOffset := clampPage(limit, offset, maxAttributedPRsPage)

	var teamIDs []string
	useIDs := false
	if sc.teamID != "" {
		ids, ok := teamRepoIDs(ctx, client, orgID, sc.teamID, "aiAttributedPrs")
		if ok && len(ids) == 0 {
			return empty, nil
		}
		if ok {
			teamIDs, useIDs = ids, true
		}
	}
	raw, err := loadAttributedPRs(ctx, client, orgID, dr.StartDate.Time(), dr.EndDate.Time(), sc.repoID, teamIDs, useIDs, pageSize+1, pageOffset)
	if err != nil {
		return nil, err
	}
	hasMore := len(raw) > pageSize
	page := raw
	if hasMore {
		page = raw[:pageSize]
	}

	distinct := distinctRepoIDs(len(page), func(i int) *string { return &page[i].RepoID })
	teamMap := repoTeamMap(ctx, client, orgID, distinct, "aiAttributedPrs")
	if len(teamMap) > 0 {
		for i := range page {
			page[i].TeamID = teamMap[page[i].RepoID]
		}
	}
	if sc.teamID != "" {
		page = filterAttributed(page, func(r attributedPR) bool { return derefString(r.TeamID) == sc.teamID })
	}
	if sc.workType != "" {
		page = filterAttributed(page, func(r attributedPR) bool { return derefString(r.WorkType) == sc.workType })
	}

	rows := make([]model.AiAttributedPr, 0, len(page))
	for _, r := range page {
		var team *string
		if r.TeamID != nil && *r.TeamID != "" {
			team = r.TeamID
		}
		rows = append(rows, model.AiAttributedPr{
			RepoID: r.RepoID, Number: int(r.Number), Title: r.Title,
			Kind: r.Kind, WorkType: r.WorkType, TeamID: team, MergedAt: r.MergedAt,
		})
	}
	return &model.AiAttributedPrsResult{
		OrgID: orgID, StartDate: dr.StartDate, EndDate: dr.EndDate,
		Rows: rows, Total: len(rows), HasMore: hasMore, DataAvailable: len(rows) > 0,
	}, nil
}

func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func filterAttributed(rows []attributedPR, keep func(attributedPR) bool) []attributedPR {
	out := make([]attributedPR, 0, len(rows))
	for _, r := range rows {
		if keep(r) {
			out = append(out, r)
		}
	}
	return out
}

func distinctRepoIDs(n int, at func(int) *string) []string {
	set := map[string]bool{}
	var order []string
	for i := 0; i < n; i++ {
		id := at(i)
		if id == nil {
			continue
		}
		if !set[*id] {
			set[*id] = true
			order = append(order, *id)
		}
	}
	return order
}

// ---- attribution overview -------------------------------------------------

// AttributionOverview answers aiAttributionOverview.
func AttributionOverview(ctx context.Context, client QueryClient, orgID string, dr model.AIDateRangeInput, in *model.AIAttributionScopeInput, limit, offset int) (*model.AIAttributionOverviewResult, error) {
	if client == nil {
		return nil, fmt.Errorf("aianalytics: clickhouse client is required")
	}
	if err := validateRange(dr); err != nil {
		return nil, err
	}
	var scopeIn *model.AIScopeInput
	if in != nil {
		scopeIn = &model.AIScopeInput{RepoID: in.RepoID, TeamID: in.TeamID, Buckets: in.Buckets}
	}
	sc, err := normalizeScope(ctx, client, orgID, scopeIn)
	if err != nil {
		return nil, err
	}
	empty := &model.AIAttributionOverviewResult{
		OrgID: orgID, StartDate: dr.StartDate, EndDate: dr.EndDate,
		Mix: []model.AIAttributionMixRow{}, Rows: []model.AIAttributionEvidenceRow{},
	}
	if sc.unresolved {
		return empty, nil
	}
	pageSize, pageOffset := clampPage(limit, offset, maxAttributionEvidencePage)

	var teamIDs []string
	useIDs := false
	if sc.teamID != "" {
		ids, ok := teamRepoIDs(ctx, client, orgID, sc.teamID, "aiAttributionOverview")
		if ok && len(ids) == 0 {
			return empty, nil
		}
		if ok {
			teamIDs, useIDs = ids, true
		}
	}
	start, end := dr.StartDate.Time(), dr.EndDate.Time()

	mixRaw, err := loadAttributionMix(ctx, client, orgID, start, end, sc.repoID, teamIDs, useIDs, sc.buckets)
	if err != nil {
		return nil, err
	}
	var total int64
	for _, m := range mixRaw {
		total = total + m.Count
	}
	mix := make([]model.AIAttributionMixRow, 0, len(mixRaw))
	for _, m := range mixRaw {
		share := 0.0
		if total != 0 {
			share = float64(m.Count) / float64(total)
		}
		kind := m.Kind
		if kind == "" {
			kind = "unknown"
		}
		mix = append(mix, model.AIAttributionMixRow{Kind: kind, Count: int(m.Count), Share: share})
	}

	raw, err := loadAttributionEvidence(ctx, client, orgID, start, end, sc.repoID, teamIDs, useIDs, sc.buckets, pageSize+1, pageOffset)
	if err != nil {
		return nil, err
	}
	hasMore := len(raw) > pageSize
	page := raw
	if hasMore {
		page = raw[:pageSize]
	}
	distinct := distinctRepoIDs(len(page), func(i int) *string { return page[i].RepoID })
	teamMap := repoTeamMap(ctx, client, orgID, distinct, "aiAttributionOverview")
	if len(teamMap) > 0 {
		for i := range page {
			if page[i].RepoID != nil {
				page[i].TeamID = teamMap[*page[i].RepoID]
			} else {
				page[i].TeamID = nil
			}
		}
	}
	if sc.teamID != "" {
		kept := page[:0:0]
		for _, r := range page {
			if derefString(r.TeamID) == sc.teamID {
				kept = append(kept, r)
			}
		}
		page = kept
	}
	rows := make([]model.AIAttributionEvidenceRow, 0, len(page))
	for _, r := range page {
		var team *string
		if r.TeamID != nil && *r.TeamID != "" {
			team = r.TeamID
		}
		evidence := r.Evidence
		if evidence == "" {
			evidence = "{}"
		}
		rows = append(rows, model.AIAttributionEvidenceRow{
			SubjectType: r.SubjectType, SubjectID: r.SubjectID, RepoID: r.RepoID,
			Provider: r.Provider, Kind: bucketOrUnknown(r.Kind), Source: r.Source,
			Confidence: float64(r.Confidence), Actor: r.Actor, Evidence: evidence,
			ObservedAt: r.ObservedAt.UTC(), TeamID: team,
		})
	}
	return &model.AIAttributionOverviewResult{
		OrgID: orgID, StartDate: dr.StartDate, EndDate: dr.EndDate,
		Mix: mix, TotalAttributed: int(total), Rows: rows, HasMore: hasMore,
		DataAvailable: len(mix) > 0 || len(rows) > 0,
	}, nil
}
