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
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/deltarule"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/changefailure"
)

// safeFloat ports safe_float (api/utils/numeric.py:22-34): default 0.0
// for a non-finite (NaN/Inf) value.
func safeFloat(v float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0.0
	}
	return v
}

// sparkPoints ports _spark_points (services/home.py:293-298).
func sparkPoints(rows []dayValueRow, transform func(float64) float64) []SparkPoint {
	points := make([]SparkPoint, 0, len(rows))
	for _, row := range rows {
		value := safeFloat(row.Value)
		points = append(points, SparkPoint{TS: pytime.NaiveDay(row.Day), Value: safeFloat(transform(value))})
	}
	return points
}

// computeMetricDelta ports _metric_deltas' own _compute_one closure
// (services/home.py:873-948).
func computeMetricDelta(ctx context.Context, client QueryClient, spec metricSpec, startDay, endDay, compareStart, compareEnd time.Time, f Filters, orgID string, asOf time.Time) (MetricDelta, error) {
	scopeFilter, scopeBindings, err := scopeFilterForMetric(ctx, client, spec.Scope, f, orgID, "team_id", "repo_id", asOf)
	if err != nil {
		return MetricDelta{}, err
	}

	var currentValue, previousValue float64
	var hasData, hasPriorData bool
	var series []dayValueRow
	var rateState *string

	if spec.Table == changefailure.Table {
		// Change failure rate: the window's summed counts through the one rule
		// (changefailure.Evaluate), so the state goes out with the value and
		// unknown, not applicable and "never counted" are not one empty answer.
		var wg sync.WaitGroup
		var errCur, errPrev, errSeries error
		var current, previous changefailure.View
		wg.Add(3)
		go func() {
			defer wg.Done()
			current, errCur = fetchChangeFailureView(ctx, client, startDay, endDay, scopeFilter, scopeBindings, orgID)
		}()
		go func() {
			defer wg.Done()
			previous, errPrev = fetchChangeFailureView(ctx, client, compareStart, compareEnd, scopeFilter, scopeBindings, orgID)
		}()
		go func() {
			defer wg.Done()
			series, errSeries = fetchMetricSeries(ctx, client, spec.Table, spec.Column, startDay, endDay, scopeFilter, scopeBindings, spec.Aggregator, orgID)
		}()
		wg.Wait()
		for _, err := range []error{errCur, errPrev, errSeries} {
			if err != nil {
				return MetricDelta{}, err
			}
		}
		currentOutcome, previousOutcome := changefailure.Evaluate(current), changefailure.Evaluate(previous)
		if currentOutcome.Value != nil {
			currentValue = *currentOutcome.Value
		}
		if previousOutcome.Value != nil {
			previousValue = *previousOutcome.Value
		}
		hasData = currentOutcome.State == changefailure.StateMeasured
		hasPriorData = previousOutcome.State == changefailure.StateMeasured
		rateState = currentOutcome.StateOrNil()
	} else if spec.Metric == "blocked_work" {
		var wg sync.WaitGroup
		var errCur, errPrev error
		wg.Add(2)
		go func() {
			defer wg.Done()
			currentValue, series, hasData, errCur = fetchBlockedHours(ctx, client, startDay, endDay, scopeFilter, scopeBindings, orgID)
		}()
		go func() {
			defer wg.Done()
			previousValue, _, hasPriorData, errPrev = fetchBlockedHours(ctx, client, compareStart, compareEnd, scopeFilter, scopeBindings, orgID)
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
		var current, previous metricValue
		wg.Add(3)
		go func() {
			defer wg.Done()
			current, errCur = fetchMetricValue(ctx, client, spec.Table, spec.Column, startDay, endDay, scopeFilter, scopeBindings, spec.Aggregator, orgID)
		}()
		go func() {
			defer wg.Done()
			previous, errPrev = fetchMetricValue(ctx, client, spec.Table, spec.Column, compareStart, compareEnd, scopeFilter, scopeBindings, spec.Aggregator, orgID)
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
		currentValue, previousValue = current.Value, previous.Value
		hasData, hasPriorData = current.HasData, previous.HasData
	}

	currentValue = safeFloat(currentValue)
	previousValue = safeFloat(previousValue)
	spark := sparkPoints(series, spec.Transform)
	pctChange := deltarule.Of(currentValue, previousValue, hasData, hasPriorData).Pct
	if pctChange != nil {
		safe := safeFloat(*pctChange)
		pctChange = &safe
	}

	return MetricDelta{
		Metric:       spec.Metric,
		Label:        spec.Label,
		Value:        safeFloat(spec.Transform(currentValue)),
		Unit:         spec.Unit,
		DeltaPct:     pctChange,
		HasData:      hasData,
		HasPriorData: hasPriorData,
		Spark:        spark,
		RateState:    rateState,
	}, nil
}

// computeMetricDeltas ports _metric_deltas (services/home.py:863-950):
// every metric's delta computed concurrently, matching Python's
// asyncio.gather. Order is preserved (indexed, not append-order), so a
// downstream "first max on tie" pick (topDeltaByMagnitude) matches
// Python's own list-order tie-break.
func computeMetricDeltas(ctx context.Context, client QueryClient, f Filters, startDay, endDay, compareStart, compareEnd time.Time, orgID string, asOf time.Time) ([]MetricDelta, error) {
	out := make([]MetricDelta, len(metrics))
	errs := make([]error, len(metrics))
	var wg sync.WaitGroup
	for i, spec := range metrics {
		wg.Add(1)
		go func(i int, spec metricSpec) {
			defer wg.Done()
			d, err := computeMetricDelta(ctx, client, spec, startDay, endDay, compareStart, compareEnd, f, orgID, asOf)
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
	startDay, endDay, compareStart, compareEnd, err := TimeWindow(f, now)
	if err != nil {
		return nil, err
	}

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
	allocationScopeFilter, allocationScopeBindings, err := scopeFilterForMetric(ctx, chClient, allocationScope, f, orgID, "team_id", "repo_id", now)
	if err != nil {
		return nil, err
	}
	allocationCategorySQL, allocationCategoryBindings := workCategoryFilter(f)

	var lastIngested *time.Time
	var coverage Coverage
	var sources map[string]string
	var scopeDataConfidence ScopeDataConfidence
	var deltas []MetricDelta
	var reworkAllocation []ReworkThemeAllocation
	var errIngested, errCoverage, errSources, errScopeConfidence, errDeltas, errRework error

	var wg sync.WaitGroup
	wg.Add(6)
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
		scopeDataConfidence, errScopeConfidence = fetchScopeDataConfidence(ctx, chClient, f, startDay, endDay, orgID, now)
	}()
	go func() {
		defer wg.Done()
		deltas, errDeltas = computeMetricDeltas(ctx, chClient, f, startDay, endDay, compareStart, compareEnd, orgID, now)
	}()
	go func() {
		defer wg.Done()
		reworkAllocation, errRework = fetchReworkThemeAllocation(ctx, chClient, startDay, endDay, allocationScopeFilter, allocationScopeBindings, allocationCategorySQL, allocationCategoryBindings, orgID)
	}()
	wg.Wait()
	for _, err := range []error{errIngested, errCoverage, errSources, errScopeConfidence, errDeltas, errRework} {
		if err != nil {
			return nil, err
		}
	}
	if reworkAllocation == nil {
		reworkAllocation = []ReworkThemeAllocation{}
	}

	dataConfidence := BuildDataConfidence(coverage.ObservedValues(), sources)
	if !hasCurrentMetricData(deltas) {
		return noDataResponse(lastIngested, latestSuccessfulSyncAt, coverage, sources, deltas, reworkAllocation, dataConfidence, scopeDataConfidence), nil
	}
	metricSignals := BuildMetricSignals(deltas, f, dataConfidence)

	var recommendationRows []RecommendationRow
	var riskRows []RiskRow
	var attribution *SignalAttribution
	var errRecommendations, errRisk, errAttribution error
	wg.Add(2)
	go func() {
		defer wg.Done()
		recommendationRows, errRecommendations = fetchRecommendationSignals(ctx, chClient, f, startDay, endDay, orgID)
	}()
	go func() {
		defer wg.Done()
		riskRows, errRisk = fetchRiskSignals(ctx, chClient, f, startDay, endDay, orgID)
	}()
	if hasCurrentWorkItemMetricData(deltas) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			attribution, errAttribution = fetchSignalAttribution(ctx, chClient, f, startDay, endDay, orgID, now)
		}()
	}
	wg.Wait()
	// CHAOS-8186: a failed signal read fails the request. It was answered as
	// "no signals of that kind" with HTTP 200, which a caller cannot tell from
	// a window that has none.
	for _, err := range []error{errRecommendations, errRisk, errAttribution} {
		if err != nil {
			return nil, err
		}
	}
	metricSignals = AttachSignalAttribution(metricSignals, attribution)

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
		scopeFilter, scopeBindings, err := scopeFilterForMetric(ctx, chClient, metricScope(topDelta.Metric), f, orgID, "team_id", "repo_id", now)
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
			Text:         fmt.Sprintf("%s %s%s", topDelta.Label, MoveWords(topDelta), driverText),
			EvidenceLink: evidenceLink(topDelta.Metric, f),
		})
	}

	// A constraint is a claim about a move between two measured values: none
	// is made when no metric has both windows (CHAOS-9063).
	var constraintCard *ConstraintCard
	if constraintMetric, ok := SelectConstraint(deltas); ok {
		constraintCard = &ConstraintCard{
			Title: "This week's constraint: " + constraintMetric.Label,
			Claim: fmt.Sprintf("%s %s over the last %d days.",
				constraintMetric.Label, MoveWords(constraintMetric), f.Time.RangeDays),
			Evidence: []ConstraintEvidence{
				{Label: "Drill into " + constraintMetric.Label, Link: evidenceLink(constraintMetric.Metric, f)},
			},
			Experiments: []string{
				"Rebalance reviewer rotation to reduce queueing.",
				"Set WIP limits per team and auto-alert at saturation.",
			},
		}
	}

	events := []EventItem{}
	for _, delta := range deltas {
		// An event needs a percent that is a statement: a window without a
		// value has none, and a rise from a measured 0 has no percent to
		// threshold (it is named in the sentences and signals instead).
		if pct, ok := delta.Percent(); ok && absFloat(pct) >= 25 {
			eventType := "spike"
			if pct > 0 {
				eventType = "regression"
			}
			events = append(events, EventItem{
				TS:   MicroDateTime(now),
				Type: eventType,
				Text: fmt.Sprintf("%s shifted %.0f%% over the last %d days.", delta.Label, pct, f.Time.RangeDays),
				Link: evidenceLink(delta.Metric, f),
			})
		}
	}

	if deltas == nil {
		deltas = []MetricDelta{}
	}

	return &Response{
		Freshness: Freshness{
			LastIngestedAt:         (*pytime.NaiveDateTime)(lastIngested),
			LatestSuccessfulSyncAt: (*MicroDateTime)(latestSuccessfulSyncAt),
			Sources:                sources,
			Coverage:               coverage,
		},
		Deltas:                deltas,
		ReworkThemeAllocation: reworkAllocation,
		Summary:               summary,
		Tiles:                 tiles(),
		Constraint:            constraintCard,
		Events:                events,
		HealthState:           healthState,
		Signals:               signals,
		LimitingFactor:        limitingFactor,
		DataConfidence:        dataConfidence,
		ScopeDataConfidence:   scopeDataConfidence,
	}, nil
}

func hasCurrentMetricData(deltas []MetricDelta) bool {
	for _, delta := range deltas {
		if delta.HasData {
			return true
		}
	}
	return false
}

// hasCurrentWorkItemMetricData identifies whether the response serves at
// least one metric whose contribution can carry work-item attribution. It
// keeps the extra reader out of an all-repository response and preserves the
// no-data response's zero additional reads.
func hasCurrentWorkItemMetricData(deltas []MetricDelta) bool {
	for _, delta := range deltas {
		if delta.HasData && isWorkItemMetric(delta.Metric) {
			return true
		}
	}
	return false
}

func noDataResponse(lastIngested *time.Time, latestSuccessfulSyncAt *time.Time, coverage Coverage, sources map[string]string, deltas []MetricDelta, reworkAllocation []ReworkThemeAllocation, dataConfidence DataConfidence, scopeDataConfidence ScopeDataConfidence) *Response {
	if deltas == nil {
		deltas = []MetricDelta{}
	}
	if reworkAllocation == nil {
		reworkAllocation = []ReworkThemeAllocation{}
	}
	return &Response{
		Freshness: Freshness{
			LastIngestedAt:         (*pytime.NaiveDateTime)(lastIngested),
			LatestSuccessfulSyncAt: (*MicroDateTime)(latestSuccessfulSyncAt),
			Sources:                sources,
			Coverage:               coverage,
		},
		Deltas:                deltas,
		ReworkThemeAllocation: reworkAllocation,
		Summary:               []SummarySentence{},
		Tiles:                 tiles(),
		Constraint:            nil,
		Events:                []EventItem{},
		HealthState: HealthState{
			Status: "no_data",
			AsOf:   (*pytime.NaiveDateTime)(lastIngested),
		},
		Signals: []Signal{},
		LimitingFactor: LimitingFactor{
			Confidence: "low",
		},
		DataConfidence:      dataConfidence,
		ScopeDataConfidence: scopeDataConfidence,
	}
}

// topDeltaByMagnitude ports `max(deltas, key=lambda d: abs(d.delta_pct),
// default=None)` (services/home.py:1102): strict '>' during a
// left-to-right scan keeps the FIRST maximal element on a tie, matching
// Python's max() semantics exactly.
func topDeltaByMagnitude(deltas []MetricDelta) (MetricDelta, bool) {
	if len(deltas) == 0 {
		return MetricDelta{}, false
	}
	var best MetricDelta
	var bestMag float64
	found := false
	for _, d := range deltas {
		pct, ok := d.Percent()
		if !ok {
			continue
		}
		mag := absFloat(pct)
		if !found || mag > bestMag {
			best = d
			bestMag = mag
			found = true
		}
	}
	if found && bestMag > 0 {
		return best, true
	}
	// No metric has a defined percent other than 0 %. A rise from a measured 0
	// is a move (stated in absolute values) and outranks "held steady": the
	// first such metric is named.
	for _, d := range deltas {
		if d.FromZero() {
			return d, true
		}
	}
	return best, found
}
