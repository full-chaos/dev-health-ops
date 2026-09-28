package graph

// Translation helpers for the `home` field (CHAOS-6084, CHAOS-7042).
//
// resolve_home (dev_health_ops.api.graphql.resolvers.home) is NOT the
// data source this field is ported from: it was never wired to the real
// build_home_response (api/services/home.py) that GET/POST /api/v1/home
// serves -- it duplicates a much smaller, hand-rolled computation (two
// hardcoded metrics, delta_pct always 0.0, filters argument never read)
// that was never wired to any consumer either (CHAOS-6084 found zero web
// callers of this GraphQL field). Only its AUTHORIZATION convention is
// ported (require_org_id: raise if the envelope carries no org, else
// always use the authorized org, never the orgId argument -- requestOrg
// in orgauth.go already carries that exact contract for WorkGraphEdges).
//
// The DATA comes from home.BuildResponse, the SAME already
// golden-parity-proven builder (against the real build_home_response)
// that backs REST home_route.go -- this field maps only the three
// sub-fields the GraphQL HomeResult type exposes (freshness.
// lastIngestedAt, deltas, reworkThemeAllocation) out of that builder's
// much richer Response. No new query logic is added here.

import (
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/home"
)

// homeFiltersFromGraphQL maps the GraphQL FilterInput to home.Filters.
// FilterInput carries no time range at all (no rangeDays/compareDays/
// startDate/endDate field exists on the GraphQL type), so Time is left
// at its zero value and BuildResponse's own TimeWindow falls back to
// home.DefaultFilters' convention (range_days=14, compare_days=14) --
// see timewindow.Compute. Who/How are not read: home.Filters' own doc
// comment already scopes this package's readers to time/scope/what.
// repos/why.work_category only, matching REST home_route.go's
// homeFiltersFromMap (server/home_route.go) exactly. Scope.Level
// defaults to "org" for a nil/absent filter, mirroring
// homeValidScopeLevels' default and home.DefaultFilters().
func homeFiltersFromGraphQL(in *model.FilterInput) home.Filters {
	f := home.Filters{
		Time:  home.TimeFilter{RangeDays: 14, CompareDays: 14},
		Scope: home.ScopeFilter{Level: "org"},
	}
	if in == nil {
		return f
	}
	if in.Scope != nil {
		if level := scopeLevelToHomeLevel(in.Scope.Level); level != "" {
			f.Scope.Level = level
		}
		f.Scope.IDs = in.Scope.Ids
	}
	if in.What != nil {
		f.What.Repos = in.What.Repos
	}
	if in.Why != nil {
		f.Why.WorkCategory = in.Why.WorkCategory
	}
	return f
}

// scopeLevelToHomeLevel lower-cases the GraphQL enum ("ORG", "TEAM", …)
// to the lowercase Literal string home.Filters/homeValidScopeLevels use
// ("org", "team", …) -- the two are the same five values, only the case
// convention differs between the GraphQL schema and the REST/Pydantic
// surface.
func scopeLevelToHomeLevel(level model.ScopeLevelInput) string {
	switch level {
	case model.ScopeLevelInputOrg:
		return "org"
	case model.ScopeLevelInputTeam:
		return "team"
	case model.ScopeLevelInputRepo:
		return "repo"
	case model.ScopeLevelInputService:
		return "service"
	case model.ScopeLevelInputDeveloper:
		return "developer"
	default:
		return ""
	}
}

// homeResultFromResponse maps home.Response's GraphQL-visible fields to
// model.HomeResult. Freshness.Coverage is mapped too (registered_document_
// field_gate_test.go's populatability gate caught its earlier omission):
// unlike the orphan Python resolve_home this field replaces (which never
// set Freshness.coverage at all, and left it out of the exposed shape
// entirely -- schema.py's old Strawberry construction only ever passed
// last_ingested_at), the real build_home_response's Freshness DOES carry
// a populated coverage -- home.BuildResponse computes it from a real
// ClickHouse read (builder.go's own Response construction) -- so mapping
// it is strictly more complete than parity with the orphan resolver would
// have required, not a divergence from it.
func homeResultFromResponse(resp *home.Response) *model.HomeResult {
	deltas := make([]model.MetricDelta, 0, len(resp.Deltas))
	for _, d := range resp.Deltas {
		deltas = append(deltas, model.MetricDelta{
			Metric:   d.Metric,
			Label:    d.Label,
			Value:    d.Value,
			Unit:     d.Unit,
			DeltaPct: d.DeltaPct,
			Spark:    homeSparkFromResponse(d.Spark),
		})
	}

	allocations := make([]model.ReworkThemeAllocation, 0, len(resp.ReworkThemeAllocation))
	for _, a := range resp.ReworkThemeAllocation {
		allocations = append(allocations, model.ReworkThemeAllocation{
			Theme:         a.Theme,
			Label:         a.Label,
			Allocation:    a.Allocation,
			AllocationPct: a.AllocationPct,
			PrsMerged:     int(a.PRsMerged),
			ChurnLoc:      int(a.ChurnLOC),
		})
	}

	return &model.HomeResult{
		Freshness: &model.Freshness{
			LastIngestedAt: naiveDateTimeToGraphQL(resp.Freshness.LastIngestedAt),
			Coverage: &model.Coverage{
				ReposCoveredPct:          resp.Freshness.Coverage.ReposCoveredPct,
				PrsLinkedToIssuesPct:     resp.Freshness.Coverage.PRsLinkedToIssuesPct,
				IssuesWithCycleStatesPct: resp.Freshness.Coverage.IssuesWithCycleStatesPct,
			},
		},
		Deltas:                deltas,
		ReworkThemeAllocation: allocations,
	}
}

func homeSparkFromResponse(points []home.SparkPoint) []model.SparkPoint {
	if len(points) == 0 {
		return nil
	}
	out := make([]model.SparkPoint, 0, len(points))
	for _, p := range points {
		out = append(out, model.SparkPoint{
			Ts:    pytime.Pydantic(pytime.DateTime{Time: time.Time(p.TS)}),
			Value: p.Value,
		})
	}
	return out
}

// naiveDateTimeToGraphQL formats a *pytime.NaiveDateTime the same way
// its own MarshalJSON does (pytime.Pydantic) rather than duplicating
// that format string here -- nil stays nil, matching schema.py's
// `str(...) if data["freshness"]["last_ingested_at"] else None`.
func naiveDateTimeToGraphQL(at *pytime.NaiveDateTime) *string {
	if at == nil {
		return nil
	}
	s := pytime.Pydantic(pytime.DateTime{Time: time.Time(*at)})
	return &s
}
