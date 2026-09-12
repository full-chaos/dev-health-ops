// Response assembly for GET /api/v1/quadrant -- ports build_quadrant_response
// (api/services/quadrant.py:468-659) for scope_type in {org, team, repo,
// service}. See quadrant.go's package doc comment for the documented
// developer/person scope gap.
package quadrant

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"
)

// Axis is the wire shape of QuadrantAxis (schemas.py:508-511).
type Axis struct {
	Metric string `json:"metric"`
	Label  string `json:"label"`
	Unit   string `json:"unit"`
}

// Axes is the wire shape of QuadrantAxes (schemas.py:514-516).
type Axes struct {
	X Axis `json:"x"`
	Y Axis `json:"y"`
}

// PointTrajectory is the wire shape of QuadrantPointTrajectory
// (schemas.py:519-522).
type PointTrajectory struct {
	X      float64 `json:"x"`
	Y      float64 `json:"y"`
	Window string  `json:"window"`
}

// Point is the wire shape of QuadrantPoint (schemas.py:525-533). Trajectory
// is nil (-> JSON null) when there is one window or fewer, matching Python's
// `_trajectory(ordered) if len(ordered) > 1 else None`.
type Point struct {
	EntityID     string            `json:"entity_id"`
	EntityLabel  string            `json:"entity_label"`
	X            float64           `json:"x"`
	Y            float64           `json:"y"`
	WindowStart  string            `json:"window_start"`
	WindowEnd    string            `json:"window_end"`
	EvidenceLink string            `json:"evidence_link"`
	Trajectory   []PointTrajectory `json:"trajectory"`
}

// Annotation is the wire shape of QuadrantAnnotation (schemas.py:536-540).
// build_quadrant_response never populates this list (annotations: list[
// QuadrantAnnotation] = [] at every return, quadrant.py:658) -- the type
// exists so Response.Annotations can be typed as [] rather than untyped
// nil, matching Python's `[]` (never `null`) on the wire.
type Annotation struct {
	Type        string    `json:"type"`
	Description string    `json:"description"`
	XRange      []float64 `json:"x_range"`
	YRange      []float64 `json:"y_range"`
}

// Response is the wire shape of QuadrantResponse (schemas.py:543-546).
type Response struct {
	Axes        Axes         `json:"axes"`
	Points      []Point      `json:"points"`
	Annotations []Annotation `json:"annotations"`
}

// RequestError carries an HTTP status the way Python's HTTPException does,
// so the route layer can answer the same status code build_quadrant_response
// would raise for the same bad input, without this package importing
// net/http.
type RequestError struct {
	Status  int
	Message string
}

func (e *RequestError) Error() string { return e.Message }

func badRequest(msg string) error     { return &RequestError{Status: 400, Message: msg} }
func notFound(msg string) error       { return &RequestError{Status: 404, Message: msg} }
func notImplemented(msg string) error { return &RequestError{Status: 501, Message: msg} }

// Params is BuildResponse's input -- the query params GET /api/v1/quadrant
// takes (main.py:882-890), minus scope_id (unused for every scope this port
// serves -- see quadrant.go's package doc comment: scope_id only matters for
// the unported person branch).
type Params struct {
	Type      string
	ScopeType string // default "org", matching main.py's own default
	RangeDays int    // default 30
	Bucket    string // default "week"
	StartDate *time.Time
	EndDate   *time.Time
}

// normalizeScope ports _normalize_scope (quadrant.py:306-309).
func normalizeScope(scopeType string) string {
	if scopeType == "developer" || scopeType == "person" {
		return "person"
	}
	return scopeType
}

// groupScope ports _group_scope (quadrant.py:312-319).
func groupScope(scopeType string) string {
	switch scopeType {
	case "org", "team":
		return "team"
	case "repo":
		return "repo"
	case "person":
		return "person"
	default:
		return "team"
	}
}

// normalizeRangeDays ports _normalize_range_days's clamp (quadrant.py:
// 322-327): max(1, min(value, 180)). Python's try/except around int(...)
// only guards a non-numeric query param; the route layer (quadrant_route.go)
// already resolved that case (defaulting to 30, the same fallback
// _normalize_range_days itself uses) before calling BuildResponse, so this
// function only needs the numeric clamp.
func normalizeRangeDays(rangeDays int) int {
	if rangeDays < 1 {
		return 1
	}
	if rangeDays > 180 {
		return 180
	}
	return rangeDays
}

// timeWindow ports time_window (services/filtering.py:78-92), restricted to
// the (start_day, end_day) pair build_quadrant_response actually reads --
// compare_start/compare_end are Python return values this route never uses.
func timeWindow(rangeDays int, startDate, endDate *time.Time) (startDay, endDay time.Time) {
	endDay = time.Now().UTC().Truncate(24 * time.Hour)
	if endDate != nil {
		endDay = *endDate
	}
	endDay = endDay.AddDate(0, 0, 1)

	if startDate != nil {
		startDay = *startDate
		if !startDay.Before(endDay) {
			startDay = endDay.AddDate(0, 0, -1)
		}
		return startDay, endDay
	}
	return endDay.AddDate(0, 0, -rangeDays), endDay
}

// bucketWindowEnd ports _bucket_window_end (quadrant.py:372-375).
func bucketWindowEnd(bucketStart time.Time, bucket string) time.Time {
	if bucket == "month" {
		if bucketStart.Month() == time.December {
			return time.Date(bucketStart.Year()+1, time.January, 1, 0, 0, 0, 0, time.UTC)
		}
		return time.Date(bucketStart.Year(), bucketStart.Month()+1, 1, 0, 0, 0, 0, time.UTC)
	}
	return bucketStart.AddDate(0, 0, 7)
}

func evidenceLink(metric string) string {
	return fmt.Sprintf("/api/v1/explain?metric=%s", metric)
}

type xEntry struct {
	Label string
	X     float64
}

type yWindow struct {
	Label       string
	X           float64
	Y           float64
	WindowStart time.Time
	WindowEnd   time.Time
}

// BuildResponse ports build_quadrant_response (quadrant.py:468-659) for
// group scope "team"/"repo". orgID is the caller's authenticated org
// (authctx.Claims.OrgID at the route layer), matching current_user.org_id
// in main.py's quadrant() view.
func BuildResponse(ctx context.Context, client QueryClient, orgID string, params Params) (*Response, error) {
	definition, ok := QuadrantDefinitions[params.Type]
	if !ok {
		return nil, notFound("Unknown quadrant type")
	}

	normalizedScope := normalizeScope(params.ScopeType)
	switch normalizedScope {
	case "org", "team", "repo", "service":
		// supported below
	case "person":
		return nil, notImplemented("Individual (person/developer) scope quadrants are not yet supported by the Go query-api port of /api/v1/quadrant (CHAOS-5550) -- served by the Python endpoint")
	default:
		return nil, badRequest("Invalid scope filter")
	}

	// CHAOS-2079 churn_throughput forced-repo-grain override, ported
	// verbatim (quadrant.py:493-494).
	if definition.Type == "churn_throughput" && (normalizedScope == "org" || normalizedScope == "team") {
		normalizedScope = "repo"
	}
	scope := groupScope(normalizedScope)

	if params.Bucket != "week" && params.Bucket != "month" {
		return nil, badRequest("Bucket must be week or month")
	}

	rangeDays := normalizeRangeDays(params.RangeDays)
	startDay, endDay := timeWindow(rangeDays, params.StartDate, params.EndDate)

	metricSet := metricsByScope[scope]
	xSpec, ok := metricSet[definition.X.Metric]
	if !ok {
		return nil, badRequest("Metric not supported for scope")
	}
	ySpec, ok := metricSet[definition.Y.Metric]
	if !ok {
		return nil, badRequest("Metric not supported for scope")
	}

	fetch := func(spec MetricSpec) ([]metricRow, error) {
		if definition.Type == "cycle_throughput" && scope == "team" && spec.UsePrimaryTeamAttribution {
			return fetchWorkItemTeamQuadrantMetric(ctx, client, spec.Metric, startDay, endDay, params.Bucket, orgID)
		}
		return fetchQuadrantMetric(ctx, client, spec, startDay, endDay, params.Bucket, orgID)
	}

	xRows, err := fetch(xSpec)
	if err != nil {
		return nil, err
	}
	yRows, err := fetch(ySpec)
	if err != nil {
		return nil, err
	}

	var teamLabels map[string]string
	if scope == "team" {
		seen := map[string]bool{}
		var ids []string
		for _, row := range append(append([]metricRow{}, xRows...), yRows...) {
			id := row.EntityID
			if id != "" && !seen[id] {
				seen[id] = true
				ids = append(ids, id)
			}
		}
		teamLabels = resolveTeamLabels(ctx, client, ids, orgID)
	} else {
		teamLabels = map[string]string{}
	}

	type xKey struct {
		entityID string
		bucket   time.Time
	}
	xMap := map[xKey]xEntry{}
	for _, row := range xRows {
		if row.EntityID == "" {
			continue
		}
		label := row.EntityLabel
		if l, ok := teamLabels[row.EntityID]; ok {
			label = l
		}
		if label == "" {
			label = row.EntityID
		}
		xMap[xKey{row.EntityID, row.Bucket}] = xEntry{Label: label, X: xSpec.Transform(row.Value)}
	}

	entityOrder := make([]string, 0)
	pointsByEntity := map[string][]yWindow{}
	for _, row := range yRows {
		if row.EntityID == "" {
			continue
		}
		label := row.EntityLabel
		if l, ok := teamLabels[row.EntityID]; ok {
			label = l
		}
		xe, ok := xMap[xKey{row.EntityID, row.Bucket}]
		if !ok {
			continue
		}
		if label == "" {
			label = xe.Label
		}
		if _, seen := pointsByEntity[row.EntityID]; !seen {
			entityOrder = append(entityOrder, row.EntityID)
		}
		pointsByEntity[row.EntityID] = append(pointsByEntity[row.EntityID], yWindow{
			Label:       label,
			X:           xe.X,
			Y:           ySpec.Transform(row.Value),
			WindowStart: row.Bucket,
			WindowEnd:   bucketWindowEnd(row.Bucket, params.Bucket),
		})
	}

	points := make([]Point, 0, len(entityOrder))
	for _, entityID := range entityOrder {
		windows := pointsByEntity[entityID]
		if len(windows) == 0 {
			continue
		}
		sort.SliceStable(windows, func(i, j int) bool {
			return windows[i].WindowStart.Before(windows[j].WindowStart)
		})
		latest := windows[len(windows)-1]

		var trajectory []PointTrajectory
		if len(windows) > 1 {
			trajectory = make([]PointTrajectory, 0, len(windows))
			for _, w := range windows {
				trajectory = append(trajectory, PointTrajectory{
					X:      w.X,
					Y:      w.Y,
					Window: w.WindowStart.Format("2006-01-02"),
				})
			}
		}

		points = append(points, Point{
			EntityID:     entityID,
			EntityLabel:  latest.Label,
			X:            latest.X,
			Y:            latest.Y,
			WindowStart:  latest.WindowStart.Format("2006-01-02"),
			WindowEnd:    latest.WindowEnd.Format("2006-01-02"),
			EvidenceLink: evidenceLink(definition.EvidenceMetric),
			Trajectory:   trajectory,
		})
	}

	return &Response{
		Axes: Axes{
			X: Axis{Metric: definition.X.Metric, Label: definition.X.Label, Unit: definition.X.Unit},
			Y: Axis{Metric: definition.Y.Metric, Label: definition.Y.Label, Unit: definition.Y.Unit},
		},
		Points:      points,
		Annotations: []Annotation{},
	}, nil
}

// AsRequestError extracts a *RequestError's status/message, or reports ok=false
// for any other error -- the route layer's 503 fallback (main.py's own
// `except Exception: raise HTTPException(503, "Data unavailable")`).
func AsRequestError(err error) (*RequestError, bool) {
	var reqErr *RequestError
	if errors.As(err, &reqErr) {
		return reqErr, true
	}
	return nil, false
}
