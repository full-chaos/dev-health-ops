// BuildResponse ports build_home_response (services/home.py:966-1231):
// the top-level orchestrator for both GET and POST /api/v1/home. No
// response-level caching is ported (Python's epoch-scoped HOME_CACHE,
// core/cache.py) -- an internal performance optimisation with no effect
// on the wire response for a cache miss, and every other ported REST
// route in this binary (quadrant, sankey, heatmap) carries the same
// documented gap; a cache hit and a cache miss return byte-identical
// JSON in Python, so this is not a parity divergence.
package home

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"
)

// safeFloat ports safe_float (api/utils/numeric.py:22-34): default 0.0
// for a non-finite (NaN/Inf) value.
func safeFloat(v float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0.0
	}
	return v
}

// deltaPct ports delta_pct (api/utils/numeric.py:68-76).
func deltaPct(current, previous float64) float64 {
	if previous == 0 {
		return 0.0
	}
	return (current - previous) / previous * 100.0
}

// sparkPoints ports _spark_points (services/home.py:293-298).
func sparkPoints(rows []dayValueRow, transform func(float64) float64) []SparkPoint {
	points := make([]SparkPoint, 0, len(rows))
	for _, row := range rows {
		value := safeFloat(row.Value)
		points = append(points, SparkPoint{TS: NaiveDateTime(row.Day), Value: safeFloat(transform(value))})
	}
	return points
}

// computeMetricDelta ports _metric_deltas' own _compute_one closure
// (services/home.py:873-948).
func computeMetricDelta(ctx context.Context, client QueryClient, spec metricSpec, startDay, endDay, compareStart, compareEnd time.Time, f Filters, orgID string) (MetricDelta, error) {
	scopeFilter, scopeBindings, err := scopeFilterForMetric(ctx, client, spec.Scope, f, orgID, "team_id", "repo_id")
	if err != nil {
		return MetricDelta{}, err
	}

	var currentValue, previousValue float64
	var series []dayValueRow

	if spec.Metric == "blocked_work" {
		var wg sync.WaitGroup
		var errCur, errPrev error
		wg.Add(2)
		go func() {
			defer wg.Done()
			currentValue, series, errCur = fetchBlockedHours(ctx, client, startDay, endDay, scopeFilter, scopeBindings, orgID)
		}()
		go func() {
			defer wg.Done()
			previousValue, _, errPrev = fetchBlockedHours(ctx, client, compareStart, compareEnd, scopeFilter, scopeBindings, orgID)
		}()
		wg.Wait()
		if errCur != nil {
			return MetricDelta{}, errCur
		}
		if errPrev != nil {
			return MetricDelta{}, errPrev
		}
	} else {
		var wg sync.WaitGroup
		var errCur, errPrev, errSeries error
		wg.Add(3)
		go func() {
			defer wg.Done()
			currentValue, errCur = fetchMetricValue(ctx, client, spec.Table, spec.Column, startDay, endDay, scopeFilter, scopeBindings, spec.Aggregator, orgID)
		}()
		go func() {
			defer wg.Done()
			previousValue, errPrev = fetchMetricValue(ctx, client, spec.Table, spec.Column, compareStart, compareEnd, scopeFilter, scopeBindings, spec.Aggregator, orgID)
		}()
		go func() {
			defer wg.Done()
			series, errSeries = fetchMetricSeries(ctx, client, spec.Table, spec.Column, startDay, endDay, scopeFilter, scopeBindings, spec.Aggregator, orgID)
		}()
		wg.Wait()
		if errCur != nil {
			return MetricDelta{}, errCur
		}
		if errPrev != nil {
			return MetricDelta{}, errPrev
		}
		if errSeries != nil {
			return MetricDelta{}, errSeries
		}
	}

	currentValue = safeFloat(currentValue)
	previousValue = safeFloat(previousValue)
	spark := sparkPoints(series, spec.Transform)
	pctChange := safeFloat(deltaPct(currentValue, previousValue))

	return MetricDelta{
		Metric:   spec.Metric,
		Label:    spec.Label,
		Value:    safeFloat(spec.Transform(currentValue)),
		Unit:     spec.Unit,
		DeltaPct: pctChange,
		Spark:    spark,
	}, nil
}

// computeMetricDeltas ports _metric_deltas (services/home.py:863-950):
// every metric's delta computed concurrently, matching Python's
// asyncio.gather. Order is preserved (indexed, not append-order), so a
// downstream "first max on tie" pick (topDeltaByMagnitude) matches
// Python's own list-order tie-break.
func computeMetricDeltas(ctx context.Context, client QueryClient, f Filters, startDay, endDay, compareStart, compareEnd time.Time, orgID string) ([]MetricDelta, error) {
	out := make([]MetricDelta, len(metrics))
	errs := make([]error, len(metrics))
	var wg sync.WaitGroup
	for i, spec := range metrics {
		wg.Add(1)
		go func(i int, spec metricSpec) {
			defer wg.Done()
			d, err := computeMetricDelta(ctx, client, spec, startDay, endDay, compareStart, compareEnd, f, orgID)
			out[i] = d
			errs[i] = err
		}(i, spec)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// BuildResponse ports build_home_response. now is the instant used for
// TimeWindow's end_date default AND every event timestamp -- production
// wiring passes time.Now().UTC(); a test passes a fixed instant for
// deterministic golden capture (Python's own datetime.now(timezone.utc)
// calls are equally non-deterministic wall-clock reads in both planes,
// so threading now through is a Go-side testability improvement, not a
// parity divergence).
func BuildResponse(ctx context.Context, chClient QueryClient, pgClient PGQueryClient, orgID string, f Filters, now time.Time) (*Response, error) {
	startDay, endDay, compareStart, compareEnd := TimeWindow(f, now)

	var latestSuccessfulSyncAt *time.Time
	if pgClient != nil {
		synced, err := FetchLatestSuccessfulSyncAt(ctx, pgClient, orgID)
		if err != nil {
			return nil, err
		}
		latestSuccessfulSyncAt = synced
	}

	allocationScope := "team"
	if f.Scope.Level == "repo" {
		allocationScope = "repo"
	}
	allocationScopeFilter, allocationScopeBindings, err := scopeFilterForMetric(ctx, chClient, allocationScope, f, orgID, "team_id", "repo_id")
	if err != nil {
		return nil, err
	}
	allocationCategorySQL, allocationCategoryBindings := workCategoryFilter(f)

	var lastIngested *time.Time
	var coverage map[string]float64
	var sources map[string]string
	var deltas []MetricDelta
	var reworkAllocation []ReworkThemeAllocation
	var errIngested, errCoverage, errSources, errDeltas, errRework error

	var wg sync.WaitGroup
	wg.Add(5)
	go func() {
		defer wg.Done()
		lastIngested, errIngested = fetchLastIngestedAt(ctx, chClient, orgID)
	}()
	go func() {
		defer wg.Done()
		coverage, errCoverage = fetchCoverage(ctx, chClient, startDay, endDay, orgID)
	}()
	go func() {
		defer wg.Done()
		sources, errSources = fetchSourceStatuses(ctx, chClient, startDay, orgID)
	}()
	go func() {
		defer wg.Done()
		deltas, errDeltas = computeMetricDeltas(ctx, chClient, f, startDay, endDay, compareStart, compareEnd, orgID)
	}()
	go func() {
		defer wg.Done()
		reworkAllocation, errRework = fetchReworkThemeAllocation(ctx, chClient, startDay, endDay, allocationScopeFilter, allocationScopeBindings, allocationCategorySQL, allocationCategoryBindings, orgID)
	}()
	wg.Wait()
	for _, err := range []error{errIngested, errCoverage, errSources, errDeltas, errRework} {
		if err != nil {
			return nil, err
		}
	}
	if reworkAllocation == nil {
		reworkAllocation = []ReworkThemeAllocation{}
	}

	dataConfidence := BuildDataConfidence(coverage, sources)
	metricSignals := BuildMetricSignals(deltas, f, dataConfidence)

	var recommendationRows []RecommendationRow
	var riskRows []RiskRow
	wg.Add(2)
	go func() {
		defer wg.Done()
		recommendationRows = fetchRecommendationSignals(ctx, chClient, f, startDay, endDay, orgID)
	}()
	go func() {
		defer wg.Done()
		riskRows = fetchRiskSignals(ctx, chClient, f, startDay, endDay, orgID)
	}()
	wg.Wait()

	var recommendationSignals []Signal
	for _, row := range recommendationRows {
		if s, ok := RecommendationSignal(row, f, dataConfidence); ok {
			recommendationSignals = append(recommendationSignals, s)
		}
	}
	var riskSignals []Signal
	for _, row := range riskRows {
		if s, ok := RiskSignal(row, f, dataConfidence); ok {
			riskSignals = append(riskSignals, s)
		}
	}

	allSignals := make([]Signal, 0, len(metricSignals)+len(recommendationSignals)+len(riskSignals))
	allSignals = append(allSignals, metricSignals...)
	allSignals = append(allSignals, recommendationSignals...)
	allSignals = append(allSignals, riskSignals...)
	signals := RankSignals(allSignals)
	if signals == nil {
		signals = []Signal{}
	}

	healthState := BuildHealthState(signals, dataConfidence, lastIngested)
	limitingFactor := BuildLimitingFactor(signals)

	summary := []SummarySentence{}
	topDelta, hasTopDelta := topDeltaByMagnitude(deltas)
	if hasTopDelta {
		scopeFilter, scopeBindings, err := scopeFilterForMetric(ctx, chClient, metricScope(topDelta.Metric), f, orgID, "team_id", "repo_id")
		if err != nil {
			return nil, err
		}
		driverRows, err := fetchMetricDriverDelta(ctx, chClient, metricTable(topDelta.Metric), metricColumn(topDelta.Metric), metricGroup(topDelta.Metric), startDay, endDay, compareStart, compareEnd, scopeFilter, scopeBindings, orgID, 3)
		if err != nil {
			return nil, err
		}
		var driverIDs []string
		for _, row := range driverRows {
			if row.ID != "" {
				driverIDs = append(driverIDs, row.ID)
			}
		}
		driverText := "."
		if len(driverIDs) > 0 {
			driverText = " driven by " + strings.Join(driverIDs, ", ") + "."
		}
		summary = append(summary, SummarySentence{
			ID:           "s1",
			Text:         fmt.Sprintf("%s %s %s%s", topDelta.Label, direction(topDelta.DeltaPct), formatDeltaWords(topDelta.DeltaPct), driverText),
			EvidenceLink: evidenceLink(topDelta.Metric, f),
		})
	}

	constraintMetric := SelectConstraint(deltas)
	constraint := ConstraintCard{
		Title: "This week's constraint: " + constraintMetric.Label,
		Claim: fmt.Sprintf("%s %s %s over the last %d days.",
			constraintMetric.Label, direction(constraintMetric.DeltaPct), formatDeltaWords(constraintMetric.DeltaPct), f.Time.RangeDays),
		Evidence: []ConstraintEvidence{
			{Label: "Drill into " + constraintMetric.Label, Link: evidenceLink(constraintMetric.Metric, f)},
		},
		Experiments: []string{
			"Rebalance reviewer rotation to reduce queueing.",
			"Set WIP limits per team and auto-alert at saturation.",
		},
	}

	events := []EventItem{}
	for _, delta := range deltas {
		if absFloat(delta.DeltaPct) >= 25 {
			eventType := "spike"
			if delta.DeltaPct > 0 {
				eventType = "regression"
			}
			events = append(events, EventItem{
				TS:   MicroDateTime(now),
				Type: eventType,
				Text: fmt.Sprintf("%s shifted %.0f%% over the last %d days.", delta.Label, delta.DeltaPct, f.Time.RangeDays),
				Link: evidenceLink(delta.Metric, f),
			})
		}
	}

	if deltas == nil {
		deltas = []MetricDelta{}
	}

	return &Response{
		Freshness: Freshness{
			LastIngestedAt:         (*NaiveDateTime)(lastIngested),
			LatestSuccessfulSyncAt: (*MicroDateTime)(latestSuccessfulSyncAt),
			Sources:                sources,
			Coverage: Coverage{
				ReposCoveredPct:          coverage["repos_covered_pct"],
				PRsLinkedToIssuesPct:     coverage["prs_linked_to_issues_pct"],
				IssuesWithCycleStatesPct: coverage["issues_with_cycle_states_pct"],
			},
		},
		Deltas:                deltas,
		ReworkThemeAllocation: reworkAllocation,
		Summary:               summary,
		Tiles:                 tiles(),
		Constraint:            constraint,
		Events:                events,
		HealthState:           healthState,
		Signals:               signals,
		LimitingFactor:        limitingFactor,
		DataConfidence:        dataConfidence,
	}, nil
}

// topDeltaByMagnitude ports `max(deltas, key=lambda d: abs(d.delta_pct),
// default=None)` (services/home.py:1102): strict '>' during a
// left-to-right scan keeps the FIRST maximal element on a tie, matching
// Python's max() semantics exactly.
func topDeltaByMagnitude(deltas []MetricDelta) (MetricDelta, bool) {
	if len(deltas) == 0 {
		return MetricDelta{}, false
	}
	best := deltas[0]
	bestMag := absFloat(best.DeltaPct)
	for _, d := range deltas[1:] {
		mag := absFloat(d.DeltaPct)
		if mag > bestMag {
			best = d
			bestMag = mag
		}
	}
	return best, true
}
