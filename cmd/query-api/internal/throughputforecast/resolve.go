package throughputforecast

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
)

// noHistoryForecastID is the literal Python puts in forecast_id when a scope has
// no throughput samples at all.
//
// A sentinel rather than a uuid4, and deliberately so: the no-history payload is
// not a forecast, and a client that stores forecast ids must be able to tell the
// two apart without re-deriving the emptiness test. Reproduced exactly -- the UI
// reads this string.
const noHistoryForecastID = "no-history"

// Resolve ports resolve_throughput_forecast.
//
// orgID is the AUTHORIZED org, never the client-supplied `orgId` argument: the
// Python resolver takes org_id from require_org_id(context) and never reads the
// GraphQL argument at all, so the argument is parsed for wire compatibility and
// then ignored. The caller in schema.resolvers.go is what enforces that.
//
// `now` is injected rather than read here so a test can pin the read windows,
// which are wall-clock derived (utc_today() minus history_weeks) and therefore
// move at UTC midnight. Production passes time.Now.
func Resolve(
	ctx context.Context,
	client QueryClient,
	orgID string,
	input model.ThroughputForecastInput,
	now time.Time,
) (*model.ThroughputForecast, error) {
	started := time.Now()

	// Checked BEFORE any query, exactly where Python checks it: a non-positive
	// window is a client error, and issuing seven reads before rejecting it
	// would bill the org for a request that was never going to answer.
	if input.HistoryWeeks <= 0 {
		return nil, fmt.Errorf("throughputForecast: history_weeks must be positive")
	}

	// Single-team scopes carry the team on the result so the UI can label the
	// scope; multi-team and org-wide ones leave it null. Not a filter -- the
	// filter is the team predicate in every query -- purely a label.
	var resultTeamID *string
	if len(input.TeamIds) == 1 {
		teamID := input.TeamIds[0]
		resultTeamID = &teamID
	}

	history, err := loadThroughputHistory(
		ctx, client, orgID, input.TeamIds, input.WorkScopeID, input.HistoryWeeks, now,
	)
	if err != nil {
		return nil, err
	}

	// A caller-supplied backlog short-circuits the read entirely, so a
	// hypothetical backlog can be forecast against real throughput. Note the
	// nil check is on the POINTER: an explicit 0 is a supplied backlog, not an
	// absent one, and must not fall through to the derived value.
	backlogSize := 0
	if input.BacklogSize != nil {
		backlogSize = *input.BacklogSize
	} else {
		backlogSize, err = loadBacklog(ctx, client, orgID, input.TeamIds, input.WorkScopeID)
		if err != nil {
			return nil, err
		}
	}
	if backlogSize < 0 {
		return nil, fmt.Errorf("throughputForecast: backlog_size must be non-negative")
	}

	// Only fetched for a non-empty backlog: coverage OF an empty backlog is not
	// a meaningful ratio, and Python skips the query rather than reporting a
	// null one. Ordered before the no-history return because Python computes it
	// before that branch -- the empty payload still carries coverage.
	var coverage *estimateCoverage
	if backlogSize > 0 {
		coverage, err = loadEstimateCoverage(ctx, client, orgID, input.TeamIds, input.WorkScopeID)
		if err != nil {
			return nil, err
		}
	}

	if len(history) == 0 {
		windows, err := computeRollingWindows(nil)
		if err != nil {
			return nil, err
		}
		primary, wip, review, incident := computeRiskOverlays(0, 0, 0, 0)
		// A structured payload, NOT null: "no data yet" and "the query failed"
		// are different answers, and a null would make a new team indistinguish-
		// able from a broken one.
		empty := forecastResult{
			teamID:              resultTeamID,
			workScopeID:         input.WorkScopeID,
			backlogSize:         backlogSize,
			historyWeeks:        input.HistoryWeeks,
			rollingWindows:      windows,
			primaryRisk:         primary,
			wipCongestion:       wip,
			reviewBottleneck:    review,
			incidentLoad:        incident,
			insufficientHistory: true,
		}
		// The silently-empty result gets its reason in the log. Without this a
		// scope that has simply never reported is indistinguishable, in the
		// process log, from one whose predicate is wrong.
		slog.WarnContext(ctx, "query_api.throughput_forecast.empty",
			"org_id", orgID,
			"scope", scopeLabel(input),
			"history_weeks", input.HistoryWeeks,
			"backlog_size", backlogSize,
			"reason", "no throughput history in the requested window",
			"duration_ms", time.Since(started).Milliseconds(),
		)
		// staleWip is deliberately NOT loaded on this path: Python passes only
		// estimate_coverage into _result_to_output here, so the field is absent
		// rather than merely empty.
		return toModel(noHistoryForecastID, now, empty, nil, nil, coverage), nil
	}

	currentWIP, averageWIP, err := loadWorkItemOverlay(
		ctx, client, orgID, input.TeamIds, input.WorkScopeID, input.HistoryWeeks, now,
	)
	if err != nil {
		return nil, err
	}
	staleP50, staleP90, err := loadStaleWIP(ctx, client, orgID, input.TeamIds, input.WorkScopeID)
	if err != nil {
		return nil, err
	}
	reviewLatencyHours, err := loadReviewOverlay(ctx, client, orgID, input.HistoryWeeks, now)
	if err != nil {
		return nil, err
	}
	incidentCount, err := loadIncidentOverlay(ctx, client, orgID, input.HistoryWeeks, now)
	if err != nil {
		return nil, err
	}

	result, err := forecastThroughputCapacity(
		history, backlogSize, resultTeamID, input.WorkScopeID, input.HistoryWeeks,
		currentWIP, averageWIP, reviewLatencyHours, incidentCount,
	)
	if err != nil {
		return nil, fmt.Errorf("throughputForecast: %w", err)
	}

	// A forecast with history but no usable rolling window answers "we cannot
	// say", and that is a DIFFERENT emptiness from the one above -- it means no
	// window reached two samples, not that no rows exist. Both need a reason in
	// the log or the two are indistinguishable to an operator.
	if result.p50Weeks == nil {
		slog.WarnContext(ctx, "query_api.throughput_forecast.empty",
			"org_id", orgID,
			"scope", scopeLabel(input),
			"history_weeks", input.HistoryWeeks,
			"history_days", len(history),
			"backlog_size", backlogSize,
			"reason", "no rolling window reached the two-sample minimum",
			"duration_ms", time.Since(started).Milliseconds(),
		)
	}

	slog.InfoContext(ctx, "query_api.throughput_forecast.served",
		"org_id", orgID,
		"scope", scopeLabel(input),
		"history_weeks", input.HistoryWeeks,
		"history_days", len(history),
		"backlog_size", backlogSize,
		"insufficient_history", result.insufficientHistory,
		"primary_risk", result.primaryRisk.kind,
		"duration_ms", time.Since(started).Milliseconds(),
	)

	return toModel(uuid.NewString(), now, result, staleP50, staleP90, coverage), nil
}

// scopeLabel renders the request's scope for a log line.
//
// The team ids are the caller's own selection, already authorized against this
// org by the envelope -- but the count is logged rather than the ids past a
// handful, so a 500-team selection cannot turn one log line into a page of
// output.
func scopeLabel(input model.ThroughputForecastInput) string {
	var parts []string
	switch {
	case len(input.TeamIds) == 0:
		parts = append(parts, "teams=org-wide")
	case len(input.TeamIds) <= 4:
		parts = append(parts, "teams="+strings.Join(input.TeamIds, ","))
	default:
		parts = append(parts, "teams="+strconv.Itoa(len(input.TeamIds))+" selected")
	}
	if input.WorkScopeID != nil && *input.WorkScopeID != "" {
		parts = append(parts, "scope="+*input.WorkScopeID)
	}
	return strings.Join(parts, " ")
}

// toModel maps a kernel result onto the GraphQL type, mirroring
// _result_to_output field for field.
func toModel(
	forecastID string,
	computedAt time.Time,
	result forecastResult,
	staleP50, staleP90 *float64,
	coverage *estimateCoverage,
) *model.ThroughputForecast {
	windows := make([]model.ThroughputRollingWindow, 0, len(result.rollingWindows))
	for _, window := range result.rollingWindows {
		windows = append(windows, model.ThroughputRollingWindow{
			WindowWeeks:          window.windowWeeks,
			MeanWeeklyThroughput: window.meanWeeklyThroughput,
			// The COUNT, not the samples: the distribution itself is never put
			// on the wire, only how much of it there was.
			SampleCount:         len(window.samples),
			InsufficientHistory: window.insufficientHistory,
		})
	}

	forecast := &model.ThroughputForecast{
		ForecastID:          forecastID,
		ComputedAt:          isoFormatUTC(computedAt),
		TeamID:              result.teamID,
		WorkScopeID:         result.workScopeID,
		BacklogSize:         result.backlogSize,
		HistoryWeeks:        result.historyWeeks,
		P50Weeks:            result.p50Weeks,
		P75Weeks:            result.p75Weeks,
		P90Weeks:            result.p90Weeks,
		RollingWindows:      windows,
		PrimaryRisk:         overlayToModel(result.primaryRisk),
		WipCongestion:       overlayToModel(result.wipCongestion),
		ReviewBottleneck:    overlayToModel(result.reviewBottleneck),
		IncidentLoad:        overlayToModel(result.incidentLoad),
		InsufficientHistory: result.insufficientHistory,
	}

	// Absent when BOTH ages are null, exactly as _load_stale_wip returns None
	// there. An object carrying two nulls is a different answer from no object.
	if staleP50 != nil || staleP90 != nil {
		forecast.StaleWip = &model.ThroughputStaleWip{
			P50AgeHours: staleP50,
			P90AgeHours: staleP90,
		}
	}
	if coverage != nil {
		forecast.EstimateCoverage = &model.ThroughputEstimateCoverage{
			Ratio:            coverage.ratio,
			EstimatedCount:   coverage.estimatedCount,
			UnestimatedCount: coverage.unestimatedCount,
			BacklogSize:      coverage.backlogSize,
		}
	}
	return forecast
}

func overlayToModel(overlay riskOverlay) *model.ThroughputRiskOverlay {
	return &model.ThroughputRiskOverlay{
		Kind:      overlay.kind,
		Score:     overlay.score,
		Label:     overlay.label,
		Value:     overlay.value,
		Threshold: overlay.threshold,
		Active:    overlay.active,
	}
}

// isoFormatUTC reproduces datetime.isoformat() for a UTC-aware datetime, which
// is what Python puts in computedAt.
//
// Written out rather than handed to a Go layout string because Python's rule is
// not one Go layout expresses: the fractional part is SIX digits when the
// microsecond is non-zero and ENTIRELY ABSENT when it is zero -- never the
// trailing-zero trimming Go's ".999999" does, which would render a microsecond
// of 123000 as ".123". The offset is spelled "+00:00", not "Z".
//
// computedAt is volatile per call on both sides and can never be compared
// against Python, so this is about the SHAPE a client parses, not about parity.
func isoFormatUTC(moment time.Time) string {
	utc := moment.UTC()
	base := utc.Format("2006-01-02T15:04:05")
	microseconds := utc.Nanosecond() / 1000
	if microseconds != 0 {
		base += fmt.Sprintf(".%06d", microseconds)
	}
	return base + "+00:00"
}
