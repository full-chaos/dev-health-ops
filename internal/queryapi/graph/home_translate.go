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
// that the GraphQL resolver translates in full. REST home_route.go adapts
// that response to its frozen Python contract. CHAOS-7070 grows the GraphQL
// HomeResult type to expose the FULL Response (every field, not the original
// three) -- no new query logic is added here, only translation.

import (
	"encoding/json"
	"sort"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/home"
)

// homeFiltersFromGraphQL maps the GraphQL FilterInput and HomeWindowInput
// to home.Filters. FilterInput carries no time range (the window is its own
// `window` argument, whose members match REST's filters.time), so Time keeps
// the default (range_days=14, compare_days=14) for every member the caller
// leaves unset, and the day counts and dates reach home.TimeWindow (see
// timewindow.Compute) unchanged, the same computation REST uses. Who/How are
// not read: home.Filters' own doc comment already scopes this package's
// readers to time/scope/what.repos/why.work_category only, matching REST
// home_route.go's homeFiltersFromMap (server/home_route.go) exactly.
// Scope.Level defaults to "org" for a nil/absent filter, mirroring
// homeValidScopeLevels' default and home.DefaultFilters().
func homeFiltersFromGraphQL(in *model.FilterInput, window *model.HomeWindowInput) home.Filters {
	f := home.Filters{
		Time:  home.TimeFilter{RangeDays: 14, CompareDays: 14},
		Scope: home.ScopeFilter{Level: "org"},
	}
	if window != nil {
		if window.RangeDays != nil {
			f.Time.RangeDays = *window.RangeDays
		}
		if window.CompareDays != nil {
			f.Time.CompareDays = *window.CompareDays
		}
		if window.StartDate != nil {
			start := window.StartDate.Time()
			f.Time.StartDate = &start
		}
		if window.EndDate != nil {
			end := window.EndDate.Time()
			f.Time.EndDate = &end
		}
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

// homeResultFromResponse maps every field of home.Response to
// model.HomeResult (CHAOS-7070). Freshness.Coverage is mapped too
// (registered_document_field_gate_test.go's populatability gate caught its
// earlier omission): unlike the orphan Python resolve_home this field
// replaces (which never set Freshness.coverage at all, and left it out of
// the exposed shape entirely -- schema.py's old Strawberry construction
// only ever passed last_ingested_at), the real build_home_response's
// Freshness DOES carry a populated coverage -- home.BuildResponse computes
// it from a real ClickHouse read (builder.go's own Response construction)
// -- so mapping it is strictly more complete than parity with the orphan
// resolver would have required, not a divergence from it.
func homeResultFromResponse(resp *home.Response) *model.HomeResult {
	deltas := make([]model.MetricDelta, 0, len(resp.Deltas))
	for _, d := range resp.Deltas {
		deltas = append(deltas, model.MetricDelta{
			Metric:       d.Metric,
			Label:        d.Label,
			Value:        d.Value,
			Unit:         d.Unit,
			DeltaPct:     d.DeltaPct,
			HasData:      d.HasData,
			HasPriorData: d.HasPriorData,
			Spark:        homeSparkFromResponse(d.Spark),
			RateState:    d.RateState,
			RateCoverage: d.RateCoverage,
			RepoFilterApplied: d.RepoFilterApplied,

			RepoLinkState:          d.RepoLinkState,
			RepoLinkBasis:          repoLinkBasisFromResponse(d.RepoLinkBasis),
			RepoLinkMultiRepoItems: d.RepoLinkMultiRepoItems,
			RepoLinkCoverage:       repoLinkCoverageFromResponse(d.RepoLinkCoverage),
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
			LastIngestedAt:         naiveDateTimeToGraphQL(resp.Freshness.LastIngestedAt),
			LatestSuccessfulSyncAt: microDateTimeToGraphQL(resp.Freshness.LatestSuccessfulSyncAt),
			Sources:                homeFreshnessSourcesFromResponse(resp.Freshness.Sources),
			Coverage: &model.Coverage{
				ReposCoveredPct:          resp.Freshness.Coverage.ReposCoveredPct,
				PrsLinkedToIssuesPct:     resp.Freshness.Coverage.PRsLinkedToIssuesPct,
				IssuesWithCycleStatesPct: resp.Freshness.Coverage.IssuesWithCycleStatesPct,
			},
		},
		Deltas:                deltas,
		ReworkThemeAllocation: allocations,
		Summary:               homeSummaryFromResponse(resp.Summary),
		Tiles:                 homeTilesFromResponse(resp.Tiles),
		Constraint:            homeConstraintFromResponse(resp.Constraint),
		Events:                homeEventsFromResponse(resp.Events),
		HealthState:           homeHealthStateFromResponse(resp.HealthState),
		Signals:               homeSignalsFromResponse(resp.Signals),
		LimitingFactor:        homeLimitingFactorFromResponse(resp.LimitingFactor),
		DataConfidence:        homeDataConfidenceFromResponse(resp.DataConfidence),
		ScopeDataConfidence:   homeScopeDataConfidenceFromResponse(resp.ScopeDataConfidence),
	}
}

// homeFreshnessSourcesFromResponse maps Freshness.Sources (map[string]string
// in home.Response, since Python's WTI sync-source keys are provider names
// discovered at sync time, not a fixed enum) to a deterministically ordered
// list -- sorted by provider name, since a Go map has no order of its own
// and the wire shape must not flap between two resolutions of the same data.
func homeFreshnessSourcesFromResponse(sources map[string]string) []model.HomeFreshnessSource {
	if len(sources) == 0 {
		return []model.HomeFreshnessSource{}
	}
	providers := make([]string, 0, len(sources))
	for provider := range sources {
		providers = append(providers, provider)
	}
	sort.Strings(providers)
	out := make([]model.HomeFreshnessSource, 0, len(providers))
	for _, provider := range providers {
		out = append(out, model.HomeFreshnessSource{Provider: provider, Status: sources[provider]})
	}
	return out
}

func homeSummaryFromResponse(sentences []home.SummarySentence) []model.SummarySentence {
	out := make([]model.SummarySentence, 0, len(sentences))
	for _, s := range sentences {
		out = append(out, model.SummarySentence{ID: s.ID, Text: s.Text, EvidenceLink: s.EvidenceLink})
	}
	return out
}

// homeTilesFromResponse maps HomeResponse.tiles (a Python dict[str, Any],
// carried here as the already-ordered pyjson.OrderedMap[Tile] home.Response
// uses -- see response.go's own Tile doc comment) to a list of key/value
// entries in the SAME insertion order, so GraphQL's list ordering carries
// what a JSON object's key order otherwise only carries by convention.
func homeTilesFromResponse(tiles pyjson.OrderedMap[home.Tile]) []model.HomeTileEntry {
	out := make([]model.HomeTileEntry, 0, tiles.Len())
	for key, tile := range tiles.All() {
		out = append(out, model.HomeTileEntry{
			Key: key,
			Value: &model.HomeTile{
				Title:    tile.Title,
				Subtitle: tile.Subtitle,
				Link:     tile.Link,
			},
		})
	}
	return out
}

func homeConstraintFromResponse(c *home.ConstraintCard) *model.ConstraintCard {
	if c == nil {
		return nil
	}
	evidence := make([]model.ConstraintEvidence, 0, len(c.Evidence))
	for _, e := range c.Evidence {
		evidence = append(evidence, model.ConstraintEvidence{Label: e.Label, Link: e.Link})
	}
	experiments := c.Experiments
	if experiments == nil {
		experiments = []string{}
	}
	return &model.ConstraintCard{
		Title:       c.Title,
		Claim:       c.Claim,
		Evidence:    evidence,
		Experiments: experiments,
	}
}

func homeEventsFromResponse(events []home.EventItem) []model.EventItem {
	out := make([]model.EventItem, 0, len(events))
	for _, e := range events {
		ts := e.TS
		tsString := microDateTimeToGraphQL(&ts)
		out = append(out, model.EventItem{
			Ts:   *tsString,
			Type: e.Type,
			Text: e.Text,
			Link: e.Link,
		})
	}
	return out
}

func homeHealthStateFromResponse(hs home.HealthState) *model.HealthState {
	return &model.HealthState{
		Status:   hs.Status,
		Headline: hs.Headline,
		Summary:  hs.Summary,
		AsOf:     naiveDateTimeToGraphQL(hs.AsOf),
	}
}

func homeSignalsFromResponse(signals []home.Signal) []model.HomeSignal {
	out := make([]model.HomeSignal, 0, len(signals))
	for _, s := range signals {
		var scopeEntity *model.ScopeEntityRef
		if s.ScopeEntity != nil {
			scopeEntity = &model.ScopeEntityRef{ID: s.ScopeEntity.ID, DisplayName: s.ScopeEntity.DisplayName}
		}
		out = append(out, model.HomeSignal{
			ID:                s.ID,
			Title:             s.Title,
			Metric:            s.Metric,
			CurrentValue:      s.CurrentValue,
			PriorValue:        s.PriorValue,
			Delta:             s.Delta,
			Direction:         s.Direction,
			Severity:          s.Severity,
			Confidence:        s.Confidence,
			AffectedScope:     s.AffectedScope,
			EvidenceCount:     s.EvidenceCount,
			WhyItMatters:      s.WhyItMatters,
			RecommendedAction: s.RecommendedAction,
			EvidenceRef:       s.EvidenceRef,
			Category:          s.Category,
			ScopeEntity:       scopeEntity,
			Coverage:          s.Coverage,
			RepoFilterApplied: s.RepoFilterApplied,
			Attribution:       homeSignalAttributionFromResponse(s.Attribution),
		})
	}
	return out
}

func homeSignalAttributionFromResponse(attribution *home.SignalAttribution) *model.SignalAttribution {
	if attribution == nil {
		return nil
	}
	sources := make([]model.SignalAttributionSourceCount, 0, len(attribution.Sources))
	for _, source := range attribution.Sources {
		sources = append(sources, model.SignalAttributionSourceCount{
			Source: model.TeamAttributionSourceFromStored(source.Source),
			Items:  source.Items,
			Share:  source.Share,
		})
	}
	confidence := make([]model.SignalAttributionConfidenceCount, 0, len(attribution.Confidence))
	for _, bucket := range attribution.Confidence {
		confidence = append(confidence, model.SignalAttributionConfidenceCount{
			Confidence: model.TeamAttributionConfidenceFromStored(bucket.Confidence),
			Items:      bucket.Items,
			Share:      bucket.Share,
		})
	}
	return &model.SignalAttribution{
		Items:      attribution.Items,
		Sources:    sources,
		Confidence: confidence,
	}
}

func homeLimitingFactorFromResponse(lf home.LimitingFactor) *model.HomeLimitingFactor {
	return &model.HomeLimitingFactor{
		Claim:             lf.Claim,
		WhyItMatters:      lf.WhyItMatters,
		RecommendedAction: lf.RecommendedAction,
		Confidence:        lf.Confidence,
		EvidenceRef:       lf.EvidenceRef,
	}
}

func homeDataConfidenceFromResponse(dc home.DataConfidence) *model.HomeDataConfidence {
	connected := dc.ConnectedSources
	if connected == nil {
		connected = []string{}
	}
	missing := dc.MissingSources
	if missing == nil {
		missing = []string{}
	}
	caveats := dc.Caveats
	if caveats == nil {
		caveats = []string{}
	}
	return &model.HomeDataConfidence{
		Level:            dc.Level,
		CoveragePct:      dc.CoveragePct,
		ConnectedSources: connected,
		MissingSources:   missing,
		Caveats:          caveats,
	}
}

func homeScopeDataConfidenceFromResponse(dc home.ScopeDataConfidence) *model.HomeScopeDataConfidence {
	caveats := dc.Caveats
	if caveats == nil {
		caveats = []string{}
	}
	return &model.HomeScopeDataConfidence{
		Level:          dc.Level,
		CoveragePct:    dc.CoveragePct,
		LastIngestedAt: naiveDateTimeToGraphQL(dc.LastIngestedAt),
		Caveats:        caveats,
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

// microDateTimeToGraphQL formats a *home.MicroDateTime BY CALLING its own
// MarshalJSON, not by reformatting the time.Time underneath -- unlike
// pytime.NaiveDateTime, MicroDateTime is tz-aware and always appends "Z"
// (naivetime.go's own doc comment), so reusing naiveDateTimeToGraphQL's
// pytime.Pydantic formatting here silently drops it. Calling the type's
// own marshaler keeps this byte-identical to the REST wire shape by
// construction, even if that format ever changes.
func microDateTimeToGraphQL(at *home.MicroDateTime) *string {
	if at == nil {
		return nil
	}
	raw, err := at.MarshalJSON()
	if err != nil {
		return nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil
	}
	return &s
}

func repoLinkBasisFromResponse(b *home.RepoLinkBasis) *model.RepoLinkBasis {
	if b == nil {
		return nil
	}
	return &model.RepoLinkBasis{Native: b.Native, ExplicitText: b.ExplicitText, Heuristic: b.Heuristic}
}

func repoLinkCoverageFromResponse(c *home.RepoLinkCoverage) *model.RepoLinkCoverage {
	if c == nil {
		return nil
	}
	return &model.RepoLinkCoverage{LinkedItems: c.LinkedItems, ItemsInWindow: c.ItemsInWindow}
}
