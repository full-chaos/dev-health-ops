// GET /api/v1/people/{person_id}/metric -- ports
// build_person_metric_response (services/people.py:632-731). Identity
// resolution (resolve.go) and the per-metric config/reads (metricconfig.go/
// metricqueries.go) are shared with GET /api/v1/people/{person_id}/summary
// (summary.go).
package people

import (
	"context"
	"fmt"
	"time"
)

// MetricParams is GET /api/v1/people/{person_id}/metric's already-resolved
// request shape -- the route file owns query-param parsing/validation,
// this package owns the business logic, matching SummaryParams' own
// division of labor.
type MetricParams struct {
	PersonID    string
	Metric      string
	RangeDays   int
	CompareDays int
	// Now stands in for utc_today(), same role as SummaryParams.Now.
	Now time.Time
}

// MetricDefinition ports MetricDefinition (api/models/schemas.py:424-426).
type MetricDefinition struct {
	Description    string `json:"description"`
	Interpretation string `json:"interpretation"`
}

// MetricTimeseriesPoint ports MetricTimeseriesPoint (api/models/
// schemas.py:429-431). Day is formatted "YYYY-MM-DD" (Pydantic's `date`
// field's own `model_dump(mode="json")` shape), matching this binary's
// established plain-string-date convention (cmd/query-api/internal/
// quadrant/response.go's WindowStart/WindowEnd).
type MetricTimeseriesPoint struct {
	Day   string  `json:"day"`
	Value float64 `json:"value"`
}

// MetricBreakdownItem ports MetricBreakdownItem (api/models/schemas.py:
// 434-436).
type MetricBreakdownItem struct {
	Label string  `json:"label"`
	Value float64 `json:"value"`
}

// PersonMetricBreakdowns ports PersonMetricBreakdowns (api/models/
// schemas.py:439-442).
type PersonMetricBreakdowns struct {
	ByRepo     []MetricBreakdownItem `json:"by_repo"`
	ByWorkType []MetricBreakdownItem `json:"by_work_type"`
	ByStage    []MetricBreakdownItem `json:"by_stage"`
}

// DriverStatement ports DriverStatement (api/models/schemas.py:445-447).
type DriverStatement struct {
	Text string `json:"text"`
	Link string `json:"link"`
}

// MetricResponse ports PersonMetricResponse (api/models/schemas.py:
// 450-456).
type MetricResponse struct {
	Metric     string                  `json:"metric"`
	Label      string                  `json:"label"`
	Definition MetricDefinition        `json:"definition"`
	Timeseries []MetricTimeseriesPoint `json:"timeseries"`
	Breakdowns PersonMetricBreakdowns  `json:"breakdowns"`
	Drivers    []DriverStatement       `json:"drivers"`
}

// drilldownLink ports _drilldown_link (services/people.py:325-328).
func drilldownLink(personID, metric string) string {
	if metric == "review_latency" || metric == "churn" {
		return fmt.Sprintf("/api/v1/people/%s/drilldown/prs?metric=%s", personID, metric)
	}
	return fmt.Sprintf("/api/v1/people/%s/drilldown/issues?metric=%s", personID, metric)
}

// driverFromBreakdowns ports _driver_from_breakdowns (services/people.py:
// 374-395): one DriverStatement naming the largest slice of the FIRST
// non-empty breakdown, checked in Python's own dict-literal order
// (by_repo, by_work_type, by_stage, services/people.py:380-384) -- three
// explicit checks, not a range over PersonMetricBreakdowns' fields, so
// this order can never drift with Go's own (unordered) struct field
// iteration.
func driverFromBreakdowns(metric string, breakdowns PersonMetricBreakdowns, personID string) []DriverStatement {
	for _, items := range [][]MetricBreakdownItem{breakdowns.ByRepo, breakdowns.ByWorkType, breakdowns.ByStage} {
		if len(items) == 0 {
			continue
		}
		total := 0.0
		for _, item := range items {
			total += item.Value
		}
		head := items[0]
		var text string
		if total > 0 {
			pct := head.Value / total * 100.0
			text = fmt.Sprintf("%s accounts for %.0f%% of this period.", head.Label, pct)
		} else {
			text = fmt.Sprintf("%s contributes the largest share this period.", head.Label)
		}
		return []DriverStatement{{Text: text, Link: drilldownLink(personID, metric)}}
	}
	return []DriverStatement{}
}

// BuildMetricResponse is the Go port of build_person_metric_response
// (api/services/people.py:632-731). Auth, the outer try/except -> 503
// fallback, and _reject_comparative_params are the CALLER's job (route
// file) -- this function returns a plain Go error for any failure (a
// *RequestError for the typed 400/404, a plain error for anything else),
// never an HTTP status.
func BuildMetricResponse(ctx context.Context, reader *Reader, orgID string, params MetricParams) (MetricResponse, error) {
	if reader == nil {
		return MetricResponse{}, ErrUnavailable
	}

	cfg, ok := personMetricConfig(params.Metric)
	if !ok {
		// `if not config: raise ValueError("metric not supported")`
		// (services/people.py:641-643) -> main.py's own 400 "Metric not
		// supported" (main.py:1111-1118).
		return MetricResponse{}, badRequest("Metric not supported")
	}

	startDay, endDay, _, _ := timeWindow(params.Now, params.RangeDays, params.CompareDays)

	canonical, aliasList, err := resolveIdentityContext(ctx, reader.client, params.PersonID, orgID)
	if err != nil {
		return MetricResponse{}, err
	}
	if canonical == "" {
		return MetricResponse{}, notFound("Person not found")
	}
	identityInputs := identityVariants(canonical, aliasList)

	seriesRows, err := fetchPersonMetricSeries(ctx, reader.client, cfg.Table, cfg.Column, cfg.Aggregator, cfg.IdentityColumn, identityInputs, startDay, endDay, cfg.ExtraWhere, orgID)
	if err != nil {
		return MetricResponse{}, err
	}
	timeseries := make([]MetricTimeseriesPoint, 0, len(seriesRows))
	for _, row := range seriesRows {
		timeseries = append(timeseries, MetricTimeseriesPoint{
			Day:   formatDay(row.Day),
			Value: safeTransform(cfg.Transform, safeFloat(row.Value)),
		})
	}

	breakdowns := PersonMetricBreakdowns{
		ByRepo:     []MetricBreakdownItem{},
		ByWorkType: []MetricBreakdownItem{},
		ByStage:    []MetricBreakdownItem{},
	}
	if cfg.ByRepo != nil {
		rows, err := fetchPersonBreakdown(ctx, reader.client, *cfg.ByRepo, identityInputs, startDay, endDay, orgID)
		if err != nil {
			return MetricResponse{}, err
		}
		breakdowns.ByRepo = toBreakdownItems(rows, cfg.ByRepo.Transform)
	}
	if cfg.ByWorkType != nil {
		rows, err := fetchPersonBreakdown(ctx, reader.client, *cfg.ByWorkType, identityInputs, startDay, endDay, orgID)
		if err != nil {
			return MetricResponse{}, err
		}
		breakdowns.ByWorkType = toBreakdownItems(rows, cfg.ByWorkType.Transform)
	}
	if cfg.ByStage != nil {
		rows, err := fetchPersonBreakdown(ctx, reader.client, *cfg.ByStage, identityInputs, startDay, endDay, orgID)
		if err != nil {
			return MetricResponse{}, err
		}
		breakdowns.ByStage = toBreakdownItems(rows, cfg.ByStage.Transform)
	}

	drivers := driverFromBreakdowns(params.Metric, breakdowns, params.PersonID)

	return MetricResponse{
		Metric:     params.Metric,
		Label:      cfg.Label,
		Definition: MetricDefinition{Description: cfg.Definition.Description, Interpretation: cfg.Definition.Interpretation},
		Timeseries: timeseries,
		Breakdowns: breakdowns,
		Drivers:    drivers,
	}, nil
}

func toBreakdownItems(rows []personBreakdownRow, transform func(float64) float64) []MetricBreakdownItem {
	out := make([]MetricBreakdownItem, 0, len(rows))
	for _, row := range rows {
		out = append(out, MetricBreakdownItem{Label: row.Label, Value: safeTransform(transform, safeFloat(row.Value))})
	}
	return out
}
