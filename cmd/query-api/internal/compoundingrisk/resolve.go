package compoundingrisk

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graphqldate"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/teamscope"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

const (
	defaultTrendDays = 30
	scopeRepo        = "repo"
	scopeTeam        = "team"
)

func severityFrom(v string) model.CompoundingRiskSeverity {
	switch strings.ToLower(v) {
	case "low":
		return model.CompoundingRiskSeverityLow
	case "elevated":
		return model.CompoundingRiskSeverityElevated
	case "high":
		return model.CompoundingRiskSeverityHigh
	}
	return model.CompoundingRiskSeverityUnknown
}

func point(scope model.CompoundingRiskScope, day time.Time, scopeID, label string, r storedRow) model.CompoundingRiskPoint {
	return model.CompoundingRiskPoint{
		Day:        graphqldate.New(day),
		Scope:      scope,
		ScopeID:    scopeID,
		ScopeLabel: label,
		Score:      r.score,
		Severity:   severityFrom(r.severity),
		Components: &model.CompoundingRiskComponents{
			ChurnNorm: r.churnNorm, ComplexityNorm: r.complexityNorm, OwnershipNorm: r.ownershipNorm,
			ReviewNorm: r.reviewNorm, ReworkChurn: r.reworkChurn, ComplexityDelta: r.complexityDelta,
			BusFactor: r.busFactor, OwnershipGini: r.ownershipGini, SingleOwnerRatio: r.singleOwnerRatio,
			ReviewLatencyP90h: r.reviewLatencyP90h,
		},
		Weights:     &model.CompoundingRiskWeights{Churn: r.wChurn, Complexity: r.wComplexity, Ownership: r.wOwnership, Review: r.wReview},
		Thresholds:  &model.CompoundingRiskThresholds{Elevated: r.thresholdElevated, High: r.thresholdHigh},
		ComputedAt:  r.computedAt,
		ScopeEntity: &model.CompoundingRiskScopeEntity{ID: scopeID, DisplayName: label},
	}
}

func labelOr(labels map[string]string, id string) string {
	if v, ok := labels[id]; ok {
		return v
	}
	return id
}

// meanOf is the mean of the non-null values in row order, using the same
// compensated float summation Python's sum() applies.
func meanOf(rows []storedRow, pick func(storedRow) *float64) *float64 {
	var vals []float64
	for _, r := range rows {
		if v := pick(r); v != nil {
			vals = append(vals, *v)
		}
	}
	if len(vals) == 0 {
		return nil
	}
	m := pythonparity.Sum(vals) / float64(len(vals))
	return &m
}

// teamPoints derives team points from repo rows: each repo row joins every
// team that owns its repository, and a team's point is the unweighted mean
// of its rows' score and components with weights and thresholds taken from
// its first row and severity re-bucketed from the mean score.
func teamPoints(day time.Time, rows []storedRow, teamsOfRepo map[string][]string, labels map[string]string, teamFilter []string, now time.Time) []model.CompoundingRiskPoint {
	byTeam := map[string][]storedRow{}
	var order []string
	for _, r := range rows {
		for _, team := range teamsOfRepo[r.scopeID] {
			if len(teamFilter) > 0 && !contains(teamFilter, team) {
				continue
			}
			if _, seen := byTeam[team]; !seen {
				order = append(order, team)
			}
			byTeam[team] = append(byTeam[team], r)
		}
	}
	out := make([]model.CompoundingRiskPoint, 0, len(order))
	for _, team := range order {
		rs := byTeam[team]
		first := rs[0]
		avg := meanOf(rs, func(r storedRow) *float64 { return r.score })
		sev := model.CompoundingRiskSeverityUnknown
		switch {
		case avg == nil:
		case *avg >= first.thresholdHigh:
			sev = model.CompoundingRiskSeverityHigh
		case *avg >= first.thresholdElevated:
			sev = model.CompoundingRiskSeverityElevated
		default:
			sev = model.CompoundingRiskSeverityLow
		}
		agg := first
		agg.score = avg
		agg.churnNorm = meanOf(rs, func(r storedRow) *float64 { return r.churnNorm })
		agg.complexityNorm = meanOf(rs, func(r storedRow) *float64 { return r.complexityNorm })
		agg.ownershipNorm = meanOf(rs, func(r storedRow) *float64 { return r.ownershipNorm })
		agg.reviewNorm = meanOf(rs, func(r storedRow) *float64 { return r.reviewNorm })
		agg.reworkChurn = meanOf(rs, func(r storedRow) *float64 { return r.reworkChurn })
		agg.complexityDelta = meanOf(rs, func(r storedRow) *float64 { return r.complexityDelta })
		agg.busFactor = meanOf(rs, func(r storedRow) *float64 { return r.busFactor })
		agg.ownershipGini = meanOf(rs, func(r storedRow) *float64 { return r.ownershipGini })
		agg.singleOwnerRatio = meanOf(rs, func(r storedRow) *float64 { return r.singleOwnerRatio })
		agg.reviewLatencyP90h = meanOf(rs, func(r storedRow) *float64 { return r.reviewLatencyP90h })
		p := point(model.CompoundingRiskScopeTeam, day, team, labelOr(labels, team), agg)
		p.Severity = sev
		if first.computedAt.IsZero() {
			p.ComputedAt = now
		}
		out = append(out, p)
	}
	sort.SliceStable(out, func(i, j int) bool {
		a, b := out[i].Score, out[j].Score
		if (a == nil) != (b == nil) {
			return b == nil
		}
		if a == nil {
			return false
		}
		return *a > *b
	})
	return out
}

func contains(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

// ownedRepos returns the repositories a team owns at asOf, id to full name.
func ownedRepos(ctx context.Context, client QueryClient, orgID, teamID string, asOf time.Time) (map[string]string, error) {
	cond, teamBindings := teamscope.RepoCondition(orgID, "toString(rp.id)", []string{teamID}, asOf)
	out := map[string]string{}
	if cond == "" {
		return out, nil
	}
	bindings := append([]clickhouse.Binding{{Name: "org_id", Value: orgID}}, teamBindings...)
	rs, err := client.Query(ctx, `SELECT toString(rp.id) AS repo_id, rp.repo AS full_name
FROM repos AS rp FINAL
WHERE rp.org_id = {org_id:String}
  AND `+cond, bindings)
	if err != nil {
		return nil, fmt.Errorf("compoundingrisk: owned repos query: %w", err)
	}
	defer rs.Close()
	for rs.Next() {
		var id, name string
		if err := rs.Scan(&id, &name); err != nil {
			return nil, fmt.Errorf("compoundingrisk: owned repos scan: %w", err)
		}
		out[id] = name
	}
	if err := rs.Err(); err != nil {
		return nil, fmt.Errorf("compoundingrisk: owned repos rows: %w", err)
	}
	return out, nil
}

// repoScopeForTeams narrows the repo scope to the repositories the named
// teams own. With no team filter the repo scope is returned as given; with
// one, the result is the sorted owned repository ids, further limited to
// the repo filter (by id or full name) when that is non-empty.
func repoScopeForTeams(ctx context.Context, client QueryClient, orgID string, repoIDs, teamIDs []string, asOf time.Time) (idList, error) {
	if len(teamIDs) == 0 {
		return repoIDs, nil
	}
	owned := map[string]bool{}
	for _, t := range teamIDs {
		repos, err := ownedRepos(ctx, client, orgID, t, asOf)
		if err != nil {
			return nil, err
		}
		for id, name := range repos {
			if len(repoIDs) > 0 && !contains(repoIDs, id) && !contains(repoIDs, name) {
				continue
			}
			owned[id] = true
		}
	}
	out := idList{}
	for id := range owned {
		out = append(out, id)
	}
	sort.Strings(out)
	return out, nil
}

// Resolve answers compoundingRisk for the org.
func Resolve(ctx context.Context, client QueryClient, orgID string, filter *model.CompoundingRiskFilterInput, now time.Time) (*model.CompoundingRiskResult, error) {
	if client == nil {
		return nil, errNoClient
	}
	now = now.UTC()
	breakout, trendDays := model.CompoundingRiskScopeRepo, defaultTrendDays
	var wantDay *time.Time
	var repoIDs, teamIDs []string
	if filter != nil {
		breakout, trendDays = filter.Breakout, filter.TrendDays
		repoIDs, teamIDs = filter.RepoIds, filter.TeamIds
		if filter.Day != nil {
			d := filter.Day.Time()
			wantDay = &d
		}
	}
	if trendDays > maxTrendDays {
		trendDays = maxTrendDays
	}
	if trendDays < 1 {
		trendDays = 1
	}
	endDay := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	startDay := endDay.AddDate(0, 0, -(trendDays - 1))

	empty := func() *model.CompoundingRiskResult {
		return &model.CompoundingRiskResult{OrgID: orgID, Breakout: breakout, Rows: []model.CompoundingRiskPoint{}, Trend: []model.CompoundingRiskTrendPoint{}, GeneratedAt: now}
	}

	var fallbackRepoIDs idList = repoIDs
	var teamFilteredRepoIDs idList
	teamFilteredSet := false
	narrow := func() error {
		ids, err := repoScopeForTeams(ctx, client, orgID, repoIDs, teamIDs, now)
		if err != nil {
			return err
		}
		teamFilteredRepoIDs, teamFilteredSet = ids, true
		fallbackRepoIDs = ids
		return nil
	}

	var day *time.Time
	var err error
	switch {
	case wantDay != nil:
		day = wantDay
	case breakout == model.CompoundingRiskScopeTeam:
		if day, err = latestDay(ctx, client, orgID, scopeTeam, teamIDs, startDay, endDay); err != nil {
			return nil, err
		}
		if day == nil {
			if err = narrow(); err != nil {
				return nil, err
			}
			if day, err = latestDay(ctx, client, orgID, scopeRepo, fallbackRepoIDs, startDay, endDay); err != nil {
				return nil, err
			}
		}
	default:
		if day, err = latestDay(ctx, client, orgID, scopeRepo, repoIDs, startDay, endDay); err != nil {
			return nil, err
		}
	}
	if day == nil {
		return empty(), nil
	}
	d := time.Date(day.Year(), day.Month(), day.Day(), 0, 0, 0, 0, time.UTC)

	var points []model.CompoundingRiskPoint
	if breakout == model.CompoundingRiskScopeRepo {
		rows, err := latestRows(ctx, client, orgID, d, scopeRepo, repoIDs)
		if err != nil {
			return nil, err
		}
		ids := make([]string, 0, len(rows))
		for _, r := range rows {
			ids = append(ids, r.scopeID)
		}
		labels, err := repoLabels(ctx, client, orgID, ids)
		if err != nil {
			return nil, err
		}
		for _, r := range rows {
			points = append(points, point(model.CompoundingRiskScopeRepo, d, r.scopeID, labelOr(labels, r.scopeID), r))
		}
	} else {
		teamRows, err := latestRows(ctx, client, orgID, d, scopeTeam, teamIDs)
		if err != nil {
			return nil, err
		}
		if len(teamRows) > 0 {
			labels, _ := teamLabels(ctx, client, orgID)
			for _, r := range teamRows {
				points = append(points, point(model.CompoundingRiskScopeTeam, d, r.scopeID, labelOr(labels, r.scopeID), r))
			}
		} else {
			if len(teamIDs) > 0 && !teamFilteredSet {
				if err = narrow(); err != nil {
					return nil, err
				}
			}
			repoRows, err := latestRows(ctx, client, orgID, d, scopeRepo, fallbackRepoIDs)
			if err != nil {
				return nil, err
			}
			labels, order := teamLabels(ctx, client, orgID)
			candidates := teamIDs
			if len(candidates) == 0 {
				candidates = order
			}
			teamsOfRepo := map[string][]string{}
			for _, t := range candidates {
				owned, err := ownedRepos(ctx, client, orgID, t, now)
				if err != nil {
					return nil, err
				}
				for id := range owned {
					teamsOfRepo[id] = append(teamsOfRepo[id], t)
				}
			}
			for id := range teamsOfRepo {
				sort.Strings(teamsOfRepo[id])
			}
			points = teamPoints(d, repoRows, teamsOfRepo, labels, teamIDs, now)
		}
	}

	var trendIDs idList
	if breakout == model.CompoundingRiskScopeTeam {
		if len(teamIDs) > 0 && !teamFilteredSet {
			if err = narrow(); err != nil {
				return nil, err
			}
		}
		if len(teamIDs) > 0 {
			trendIDs = teamFilteredRepoIDs
		} else {
			trendIDs = fallbackRepoIDs
		}
	} else {
		trendIDs = repoIDs
	}
	trendRows, err := repoTrend(ctx, client, orgID, d, trendDays, trendIDs)
	if err != nil {
		return nil, err
	}
	trend := make([]model.CompoundingRiskTrendPoint, 0, len(trendRows))
	for _, t := range trendRows {
		trend = append(trend, model.CompoundingRiskTrendPoint{Day: graphqldate.New(t.day), Score: t.avgScore, Severity: model.CompoundingRiskSeverityUnknown})
	}
	if points == nil {
		points = []model.CompoundingRiskPoint{}
	}
	return &model.CompoundingRiskResult{OrgID: orgID, Breakout: breakout, Rows: points, Trend: trend, GeneratedAt: now}, nil
}
