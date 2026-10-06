package graph

// Translation-correctness tests for home_translate.go (CHAOS-6084 /
// CHAOS-7042 / CHAOS-7070). home.BuildResponse itself is already
// golden-parity-proven against the real Python build_home_response
// (internal/queryapi/home's own golden_test.go et al, re-run as part of
// this change's evidence) -- these tests are NOT a second oracle against
// Python. They exist to pin the NEW code this port adds: the GraphQL
// FilterInput -> home.Filters mapping, and the home.Response ->
// model.HomeResult mapping.
//
// TestHomeResultFromResponse_MapsEveryFieldAgainstTheDomainResponse is the
// CHAOS-7070 oracle. It compares a complete Home domain response with its
// GraphQL translation. REST has a separate frozen-Python adapter because it
// cannot represent GraphQL's no-data flags or nullable constraint. The two
// domain-to-GraphQL shape differences (tiles and freshness.sources change
// from ordered maps to lists) are asserted separately rather than by the
// generic path diff, which compares leaves with the same shape.

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/home"
)

func TestHomeFiltersFromGraphQL_NilInputUsesDefaults(t *testing.T) {
	got := homeFiltersFromGraphQL(nil, nil)
	want := home.Filters{
		Time:  home.TimeFilter{RangeDays: 14, CompareDays: 14},
		Scope: home.ScopeFilter{Level: "org"},
	}
	if got.Time != want.Time {
		t.Errorf("Time = %+v, want %+v", got.Time, want.Time)
	}
	if got.Scope.Level != want.Scope.Level || len(got.Scope.IDs) != 0 {
		t.Errorf("Scope = %+v, want %+v", got.Scope, want.Scope)
	}
}

func TestHomeFiltersFromGraphQL_MapsScopeWhatWhy(t *testing.T) {
	in := &model.FilterInput{
		Scope: &model.ScopeFilterInput{Level: model.ScopeLevelInputTeam, Ids: []string{"team-1"}},
		What:  &model.WhatFilterInput{Repos: []string{"repo-1", "repo-2"}},
		Why:   &model.WhyFilterInput{WorkCategory: []string{"feature_delivery"}},
	}
	got := homeFiltersFromGraphQL(in, nil)

	if got.Scope.Level != "team" {
		t.Errorf("Scope.Level = %q, want %q (lower-cased GraphQL enum)", got.Scope.Level, "team")
	}
	if len(got.Scope.IDs) != 1 || got.Scope.IDs[0] != "team-1" {
		t.Errorf("Scope.IDs = %v, want [team-1]", got.Scope.IDs)
	}
	if len(got.What.Repos) != 2 || got.What.Repos[0] != "repo-1" {
		t.Errorf("What.Repos = %v, want [repo-1 repo-2]", got.What.Repos)
	}
	if len(got.Why.WorkCategory) != 1 || got.Why.WorkCategory[0] != "feature_delivery" {
		t.Errorf("Why.WorkCategory = %v, want [feature_delivery]", got.Why.WorkCategory)
	}
	// Time is never carried by FilterInput -- must still default, same as nil.
	if got.Time.RangeDays != 14 || got.Time.CompareDays != 14 {
		t.Errorf("Time = %+v, want the 14/14 default (FilterInput has no time range field)", got.Time)
	}
}

func TestHomeFiltersFromGraphQL_ScopeWithoutLevelDefaultsToOrg(t *testing.T) {
	// A zero-value ScopeLevelInput ("") is not one of the five valid
	// enum values -- scopeLevelToHomeLevel returns "", and the default
	// "org" must survive rather than being overwritten with an invalid
	// empty level home.BuildResponse's own scope-filter code has no
	// case for.
	in := &model.FilterInput{Scope: &model.ScopeFilterInput{Ids: []string{"x"}}}
	got := homeFiltersFromGraphQL(in, nil)
	if got.Scope.Level != "org" {
		t.Errorf("Scope.Level = %q, want the org default preserved", got.Scope.Level)
	}
}

// TestHomeResultFromResponse_MapsTheOriginalThreeFields pins the three
// sub-fields the GraphQL HomeResult type exposed before CHAOS-7070 grew
// it to the full payload -- see
// TestHomeResultFromResponse_MapsEveryFieldAgainstTheDomainResponse below for
// the full-payload oracle.
func TestHomeResultFromResponse_MapsTheOriginalThreeFields(t *testing.T) {
	ingested := pytime.NaiveDateTime(time.Date(2024, 1, 8, 12, 0, 0, 0, time.UTC))
	resp := &home.Response{
		Freshness: home.Freshness{
			LastIngestedAt: &ingested,
			Coverage: home.Coverage{
				ReposCoveredPct: 80, PRsLinkedToIssuesPct: 60, IssuesWithCycleStatesPct: 40,
			},
		},
		Deltas: []home.MetricDelta{
			{
				Metric: "throughput", Label: "Throughput", Value: 42, Unit: "units", DeltaPct: 12.5, HasData: true,
				Spark: []home.SparkPoint{{TS: pytime.NaiveDateTime(time.Date(2024, 1, 7, 0, 0, 0, 0, time.UTC)), Value: 40}},
			},
		},
		ReworkThemeAllocation: []home.ReworkThemeAllocation{
			{Theme: "feature_delivery", Label: "Feature Delivery", Allocation: 10, AllocationPct: 50, PRsMerged: 3, ChurnLOC: 100},
		},
	}

	got := homeResultFromResponse(resp)

	if got.Freshness == nil || got.Freshness.LastIngestedAt == nil {
		t.Fatal("Freshness.LastIngestedAt must be populated when the Response carries one")
	}
	if want := "2024-01-08T12:00:00"; *got.Freshness.LastIngestedAt != want {
		t.Errorf("Freshness.LastIngestedAt = %q, want %q", *got.Freshness.LastIngestedAt, want)
	}
	if got.Freshness.Coverage == nil {
		t.Fatal("Freshness.Coverage must be populated -- home.BuildResponse always computes it")
	}
	if c := got.Freshness.Coverage; c.ReposCoveredPct != 80 || c.PrsLinkedToIssuesPct != 60 || c.IssuesWithCycleStatesPct != 40 {
		t.Errorf("Freshness.Coverage = %+v, want the mapped Coverage", c)
	}

	if len(got.Deltas) != 1 {
		t.Fatalf("Deltas = %d entries, want 1", len(got.Deltas))
	}
	d := got.Deltas[0]
	if d.Metric != "throughput" || d.Label != "Throughput" || d.Value != 42 || d.Unit != "units" || d.DeltaPct != 12.5 {
		t.Errorf("Deltas[0] = %+v, want the mapped MetricDelta", d)
	}
	if !d.HasData || d.HasPriorData {
		t.Errorf("Deltas[0] data flags = current:%t prior:%t, want current:true prior:false", d.HasData, d.HasPriorData)
	}
	if len(d.Spark) != 1 || d.Spark[0].Value != 40 || d.Spark[0].Ts != "2024-01-07T00:00:00" {
		t.Errorf("Deltas[0].Spark = %+v, want one point at 2024-01-07T00:00:00 value 40", d.Spark)
	}

	if len(got.ReworkThemeAllocation) != 1 {
		t.Fatalf("ReworkThemeAllocation = %d entries, want 1", len(got.ReworkThemeAllocation))
	}
	a := got.ReworkThemeAllocation[0]
	if a.Theme != "feature_delivery" || a.Label != "Feature Delivery" || a.Allocation != 10 || a.AllocationPct != 50 || a.PrsMerged != 3 || a.ChurnLoc != 100 {
		t.Errorf("ReworkThemeAllocation[0] = %+v, want the mapped ReworkThemeAllocation", a)
	}
}

func TestHomeResultFromResponse_NilLastIngestedAtStaysNil(t *testing.T) {
	resp := &home.Response{}
	got := homeResultFromResponse(resp)
	if got.Freshness == nil {
		t.Fatal("Freshness must never be nil")
	}
	if got.Freshness.LastIngestedAt != nil {
		t.Errorf("LastIngestedAt = %v, want nil when the Response carries none", *got.Freshness.LastIngestedAt)
	}
	if got.Deltas == nil {
		t.Error("Deltas must be an empty slice, not nil, matching schema.py's list comprehension over an empty input")
	}
	if got.ReworkThemeAllocation == nil {
		t.Error("ReworkThemeAllocation must be an empty slice, not nil, for the same reason")
	}
	if got.Constraint != nil {
		t.Errorf("Constraint = %+v, want nil when the response has no constraint", got.Constraint)
	}
}

func TestHomeResultFromResponse_NoDataKeepsAbsentConstraintAndDataFlags(t *testing.T) {
	resp := &home.Response{
		Deltas: []home.MetricDelta{
			{Metric: "cycle_time", HasData: false, HasPriorData: true},
		},
		Constraint:  nil,
		HealthState: home.HealthState{Status: "no_data"},
	}

	got := homeResultFromResponse(resp)
	if got.Constraint != nil {
		t.Fatalf("Constraint = %+v, want nil for no_data", got.Constraint)
	}
	if got.HealthState == nil || got.HealthState.Status != "no_data" {
		t.Fatalf("HealthState = %+v, want no_data", got.HealthState)
	}
	if len(got.Deltas) != 1 || got.Deltas[0].HasData || !got.Deltas[0].HasPriorData {
		t.Fatalf("Deltas = %+v, want current=false prior=true", got.Deltas)
	}
}

// TestHomeResultFromResponse_MapsEveryFieldAgainstTheDomainResponse is the
// CHAOS-7070 differential oracle -- see the file header comment.
func TestHomeResultFromResponse_MapsEveryFieldAgainstTheDomainResponse(t *testing.T) {
	ingested := pytime.NaiveDateTime(time.Date(2024, 1, 8, 12, 0, 0, 0, time.UTC))
	synced := home.MicroDateTime(time.Date(2024, 1, 8, 12, 30, 0, 123000, time.UTC))
	asOf := pytime.NaiveDateTime(time.Date(2024, 1, 8, 0, 0, 0, 0, time.UTC))
	priorValue, delta, evidenceRef, lfEvidenceRef := "41", "+1", "ev-1", "ev-2"

	tiles := pyjson.OrderedMapOf(
		pyjson.KeyValue[home.Tile]{Key: "open_prs", Value: home.Tile{Title: "Open PRs", Subtitle: "12", Link: "/prs"}},
		pyjson.KeyValue[home.Tile]{Key: "open_issues", Value: home.Tile{Title: "Open Issues", Subtitle: "7", Link: "/issues"}},
	)

	resp := &home.Response{
		Freshness: home.Freshness{
			LastIngestedAt:         &ingested,
			LatestSuccessfulSyncAt: &synced,
			Sources:                map[string]string{"github": "ok", "jira": "degraded"},
			Coverage:               home.Coverage{ReposCoveredPct: 80, PRsLinkedToIssuesPct: 60, IssuesWithCycleStatesPct: 40},
		},
		Deltas: []home.MetricDelta{
			{Metric: "throughput", Label: "Throughput", Value: 42, Unit: "units", DeltaPct: 12.5, HasData: true, HasPriorData: true,
				Spark: []home.SparkPoint{{TS: pytime.NaiveDateTime(time.Date(2024, 1, 7, 0, 0, 0, 0, time.UTC)), Value: 40}}},
		},
		ReworkThemeAllocation: []home.ReworkThemeAllocation{
			{Theme: "feature_delivery", Label: "Feature Delivery", Allocation: 10, AllocationPct: 50, PRsMerged: 3, ChurnLOC: 100},
		},
		Summary: []home.SummarySentence{
			{ID: "s1", Text: "Throughput is up.", EvidenceLink: "/evidence/s1"},
		},
		Tiles: tiles,
		Constraint: &home.ConstraintCard{
			Title: "Reviewer capacity", Claim: "Reviews are the bottleneck.",
			Evidence:    []home.ConstraintEvidence{{Label: "Review latency", Link: "/evidence/c1"}},
			Experiments: []string{"add-reviewer"},
		},
		Events: []home.EventItem{
			{TS: home.MicroDateTime(time.Date(2024, 1, 8, 9, 0, 0, 0, time.UTC)), Type: "sync", Text: "GitHub synced", Link: "/events/1"},
		},
		HealthState: home.HealthState{Status: "steady", Headline: "Steady", Summary: "Nothing unusual.", AsOf: &asOf},
		Signals: []home.Signal{
			{
				ID: "sig-1", Title: "Throughput dip", Metric: "throughput", CurrentValue: "42", PriorValue: &priorValue,
				Delta: &delta, Direction: "down", Severity: "medium", Confidence: "high", AffectedScope: "org",
				EvidenceCount: 3, WhyItMatters: "Throughput drives delivery.", RecommendedAction: "Investigate backlog.",
				EvidenceRef: &evidenceRef, Category: "feature_delivery",
				ScopeEntity: &home.ScopeEntityRef{ID: "team-1", DisplayName: "Team One"},
			},
			{ID: "sig-2", Title: "No scope", Metric: "wip", CurrentValue: "5", Direction: "flat", Severity: "low",
				Confidence: "low", AffectedScope: "org", EvidenceCount: 0, WhyItMatters: "n/a", RecommendedAction: "n/a",
				Category: "maintenance"},
		},
		LimitingFactor: home.LimitingFactor{
			Claim: "Reviewer capacity limits throughput.", WhyItMatters: "Bounds delivery speed.",
			RecommendedAction: "Add a reviewer.", Confidence: "medium", EvidenceRef: &lfEvidenceRef,
		},
		DataConfidence: home.DataConfidence{
			Level: "high", CoveragePct: floatPtr(72.5),
			ConnectedSources: []string{"github", "jira"}, MissingSources: []string{"linear"}, Caveats: []string{"partial window"},
		},
		ScopeDataConfidence: home.ScopeDataConfidence{
			Level: "medium", CoveragePct: floatPtr(50), LastIngestedAt: &ingested, Caveats: []string{"scope partial"},
		},
	}

	domainJSON, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal domain response: %v", err)
	}
	var domain map[string]any
	if err := json.Unmarshal(domainJSON, &domain); err != nil {
		t.Fatalf("unmarshal domain response: %v", err)
	}

	got := homeResultFromResponse(resp)
	graphqlJSON, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("marshal GraphQL shape: %v", err)
	}
	var gql map[string]any
	if err := json.Unmarshal(graphqlJSON, &gql); err != nil {
		t.Fatalf("unmarshal GraphQL shape: %v", err)
	}

	// Leaf paths with the same shape on both sides -- the domain's snake_case
	// against GraphQL's camelCase equivalent, per the mapping table sent
	// to gwc-web-graphql for CHAOS-7064.
	// This list must cover every leaf field: a hand-picked subset (that missed
	// signals.title/metric/direction/severity/category, two coverage
	// values, and three limiting-factor fields) -- would let a dropped
	// HomeSignal.Title mapping pass unnoticed. It is every leaf field either fixture object
	// (home.Response's top-level Freshness/Summary/Constraint/Events/
	// HealthState/Signals/LimitingFactor/DataConfidence) declares,
	// checked against home/response.go's own json tags on the domain
	// side. freshness.sources and tiles are intentionally absent here --
	// they are the two declared dict->list shape differences, asserted
	// separately below by the code that already existed for them.
	leafPaths := [][2]string{
		{"freshness.last_ingested_at", "freshness.lastIngestedAt"},
		{"freshness.latest_successful_sync_at", "freshness.latestSuccessfulSyncAt"},
		{"freshness.coverage.repos_covered_pct", "freshness.coverage.reposCoveredPct"},
		{"freshness.coverage.prs_linked_to_issues_pct", "freshness.coverage.prsLinkedToIssuesPct"},
		{"freshness.coverage.issues_with_cycle_states_pct", "freshness.coverage.issuesWithCycleStatesPct"},
		{"deltas.0.metric", "deltas.0.metric"},
		{"deltas.0.label", "deltas.0.label"},
		{"deltas.0.value", "deltas.0.value"},
		{"deltas.0.unit", "deltas.0.unit"},
		{"deltas.0.delta_pct", "deltas.0.deltaPct"},
		{"deltas.0.has_data", "deltas.0.hasData"},
		{"deltas.0.has_prior_data", "deltas.0.hasPriorData"},
		{"summary.0.id", "summary.0.id"},
		{"summary.0.text", "summary.0.text"},
		{"summary.0.evidence_link", "summary.0.evidenceLink"},
		{"constraint.title", "constraint.title"},
		{"constraint.claim", "constraint.claim"},
		{"constraint.evidence.0.label", "constraint.evidence.0.label"},
		{"constraint.evidence.0.link", "constraint.evidence.0.link"},
		{"constraint.experiments.0", "constraint.experiments.0"},
		{"events.0.ts", "events.0.ts"},
		{"events.0.type", "events.0.type"},
		{"events.0.text", "events.0.text"},
		{"events.0.link", "events.0.link"},
		{"health_state.status", "healthState.status"},
		{"health_state.headline", "healthState.headline"},
		{"health_state.summary", "healthState.summary"},
		{"health_state.as_of", "healthState.asOf"},
		{"signals.0.id", "signals.0.id"},
		{"signals.0.title", "signals.0.title"},
		{"signals.0.metric", "signals.0.metric"},
		{"signals.0.current_value", "signals.0.currentValue"},
		{"signals.0.prior_value", "signals.0.priorValue"},
		{"signals.0.delta", "signals.0.delta"},
		{"signals.0.direction", "signals.0.direction"},
		{"signals.0.severity", "signals.0.severity"},
		{"signals.0.confidence", "signals.0.confidence"},
		{"signals.0.affected_scope", "signals.0.affectedScope"},
		{"signals.0.evidence_count", "signals.0.evidenceCount"},
		{"signals.0.why_it_matters", "signals.0.whyItMatters"},
		{"signals.0.recommended_action", "signals.0.recommendedAction"},
		{"signals.0.evidence_ref", "signals.0.evidenceRef"},
		{"signals.0.category", "signals.0.category"},
		{"signals.0.scope_entity.id", "signals.0.scopeEntity.id"},
		{"signals.0.scope_entity.display_name", "signals.0.scopeEntity.displayName"},
		{"signals.1.title", "signals.1.title"},
		{"signals.1.metric", "signals.1.metric"},
		{"signals.1.direction", "signals.1.direction"},
		{"signals.1.severity", "signals.1.severity"},
		{"signals.1.category", "signals.1.category"},
		{"signals.1.scope_entity", "signals.1.scopeEntity"},
		{"limiting_factor.claim", "limitingFactor.claim"},
		{"limiting_factor.why_it_matters", "limitingFactor.whyItMatters"},
		{"limiting_factor.recommended_action", "limitingFactor.recommendedAction"},
		{"limiting_factor.confidence", "limitingFactor.confidence"},
		{"limiting_factor.evidence_ref", "limitingFactor.evidenceRef"},
		{"data_confidence.level", "dataConfidence.level"},
		{"data_confidence.coverage_pct", "dataConfidence.coveragePct"},
		{"data_confidence.connected_sources.0", "dataConfidence.connectedSources.0"},
		{"data_confidence.missing_sources.0", "dataConfidence.missingSources.0"},
		{"data_confidence.caveats.0", "dataConfidence.caveats.0"},
		{"scope_data_confidence.level", "scopeDataConfidence.level"},
		{"scope_data_confidence.coverage_pct", "scopeDataConfidence.coveragePct"},
		{"scope_data_confidence.last_ingested_at", "scopeDataConfidence.lastIngestedAt"},
		{"scope_data_confidence.caveats.0", "scopeDataConfidence.caveats.0"},
	}
	for _, pair := range leafPaths {
		restVal, restOK := jsonPathValue(t, domain, pair[0])
		gqlVal, gqlOK := jsonPathValue(t, gql, pair[1])
		if restOK != gqlOK {
			t.Errorf("path %s (domain) / %s (GraphQL): presence mismatch, domain ok=%v GraphQL ok=%v", pair[0], pair[1], restOK, gqlOK)
			continue
		}
		if restOK && restVal != gqlVal {
			t.Errorf("path %s (domain) = %v, path %s (GraphQL) = %v: want equal", pair[0], restVal, pair[1], gqlVal)
		}
	}

	// The two declared shape differences: dict -> ordered list.
	restSources, _ := jsonPathValue(t, domain, "freshness.sources")
	if m, ok := restSources.(map[string]any); !ok || len(m) != 2 {
		t.Fatalf("domain freshness.sources = %v, want a 2-entry object", restSources)
	}
	if len(got.Freshness.Sources) != 2 {
		t.Fatalf("GraphQL freshness.sources = %+v, want 2 entries", got.Freshness.Sources)
	}
	bySource := map[string]string{}
	for _, s := range got.Freshness.Sources {
		bySource[s.Provider] = s.Status
	}
	if bySource["github"] != "ok" || bySource["jira"] != "degraded" {
		t.Errorf("GraphQL freshness.sources = %+v, want github=ok jira=degraded", got.Freshness.Sources)
	}

	restTiles, _ := jsonPathValue(t, domain, "tiles")
	if m, ok := restTiles.(map[string]any); !ok || len(m) != 2 {
		t.Fatalf("domain tiles = %v, want a 2-entry object", restTiles)
	}
	if len(got.Tiles) != 2 {
		t.Fatalf("GraphQL tiles = %+v, want 2 entries", got.Tiles)
	}
	if got.Tiles[0].Key != "open_prs" || got.Tiles[1].Key != "open_issues" {
		t.Errorf("GraphQL tiles order = [%s %s], want [open_prs open_issues] (insertion order)", got.Tiles[0].Key, got.Tiles[1].Key)
	}
	if got.Tiles[0].Value.Title != "Open PRs" || got.Tiles[0].Value.Subtitle != "12" || got.Tiles[0].Value.Link != "/prs" {
		t.Errorf("GraphQL tiles[0].value = %+v, want the mapped HomeTile", got.Tiles[0].Value)
	}
}

// floatPtr is a small helper -- the repo's model structs take *float64
// for DataConfidence.CoveragePct and a literal float64 has no address.
func floatPtr(v float64) *float64 { return &v }

// jsonPathValue walks a dot-separated path (numeric segments index a
// []any) through a decoded JSON value (map[string]any/[]any/scalar),
// returning (value, true) if every segment resolved, or (nil, false) the
// moment one does not -- absent and explicit-null are both "not found"
// here, which is correct for this test: every leaf path above is
// populated by the fixture, so a false means a real translation gap.
func jsonPathValue(t *testing.T, root any, path string) (any, bool) {
	t.Helper()
	cur := root
	for _, segment := range splitPath(path) {
		switch node := cur.(type) {
		case map[string]any:
			v, ok := node[segment]
			if !ok {
				return nil, false
			}
			cur = v
		case []any:
			idx, err := indexOf(segment)
			if err != nil || idx < 0 || idx >= len(node) {
				return nil, false
			}
			cur = node[idx]
		default:
			return nil, false
		}
		if cur == nil {
			return nil, false
		}
	}
	return cur, true
}

func splitPath(path string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(path); i++ {
		if i == len(path) || path[i] == '.' {
			out = append(out, path[start:i])
			start = i + 1
		}
	}
	return out
}

func indexOf(segment string) (int, error) {
	n := 0
	for _, c := range segment {
		if c < '0' || c > '9' {
			return 0, errNotAnIndex
		}
		n = n*10 + int(c-'0')
	}
	return n, nil
}

var errNotAnIndex = errNotAnIndexError{}

type errNotAnIndexError struct{}

func (errNotAnIndexError) Error() string { return "segment is not a numeric index" }

// A requested time window reaches the window computation: 90 days asked,
// 90 days queried (the resolver used to drop it and always query 14).
func TestHomeFiltersFromGraphQL_TimeWindowIsApplied(t *testing.T) {
	now := time.Date(2026, 9, 28, 15, 4, 5, 0, time.UTC)
	days := func(n int) *int { return &n }
	date := func(y int, m time.Month, d int) *graphqldate.Date {
		v := graphqldate.Date(time.Date(y, m, d, 0, 0, 0, 0, time.UTC))
		return &v
	}
	window := func(in *model.FilterInput, w *model.HomeWindowInput) (time.Time, time.Time, time.Time, time.Time) {
		start, end, compareStart, compareEnd, err := home.TimeWindow(homeFiltersFromGraphQL(in, w), now)
		if err != nil {
			t.Fatalf("TimeWindow: %v", err)
		}
		return start, end, compareStart, compareEnd
	}

	// Default path unchanged: no filters, or filters without a time member.
	defStart, defEnd, defCS, defCE := window(nil, nil)
	for name, in := range map[string]*model.FilterInput{"no filters": nil, "empty filters": {}} {
		s, e, cs, ce := window(in, &model.HomeWindowInput{})
		if !s.Equal(defStart) || !e.Equal(defEnd) || !cs.Equal(defCS) || !ce.Equal(defCE) {
			t.Fatalf("%s: window moved off the 14/14 default", name)
		}
	}
	if got := defEnd.Sub(defStart); got != 14*24*time.Hour {
		t.Fatalf("default range = %v, want 14 days", got)
	}

	// 90 days requested -> 90 days queried, comparison window 30 days.
	s, e, cs, ce := window(nil, &model.HomeWindowInput{RangeDays: days(90), CompareDays: days(30)})
	if got := e.Sub(s); got != 90*24*time.Hour {
		t.Fatalf("range = %v, want 90 days", got)
	}
	if got := s.Sub(cs); got != 30*24*time.Hour || !ce.Equal(s) {
		t.Fatalf("compare window = %v ending %v, want 30 days ending at the start %v", got, ce, s)
	}

	// Explicit dates take precedence over the day count, as in REST.
	s, e, _, _ = window(nil, &model.HomeWindowInput{
		RangeDays: days(90), StartDate: date(2026, 1, 1), EndDate: date(2026, 1, 31),
	})
	if !s.Equal(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)) || !e.Equal(time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("explicit dates: window %v..%v, want 2026-01-01..2026-02-01", s, e)
	}
}
