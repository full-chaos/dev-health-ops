// Response assembly for GET /api/v1/heatmap -- ports HeatmapAxes/
// HeatmapCell/HeatmapLegend/HeatmapResponse (api/models/schemas.py:
// 464-484) and the axis-formatting helpers _hour_labels/_weekday_labels/
// _axis_order/_format_axis_value/_totals_by_axis/_axis_values/
// _cells_from_rows (services/heatmap.py:105-192).
package heatmap

import (
	"fmt"
	"sort"
	"time"
)

// Axes is the wire shape of HeatmapAxes (schemas.py:464-467).
type Axes struct {
	X []string `json:"x"`
	Y []string `json:"y"`
}

// Cell is the wire shape of HeatmapCell (schemas.py:469-473).
type Cell struct {
	X     string  `json:"x"`
	Y     string  `json:"y"`
	Value float64 `json:"value"`
}

// Legend is the wire shape of HeatmapLegend (schemas.py:475-478).
type Legend struct {
	Unit  string `json:"unit"`
	Scale string `json:"scale"`
}

// ReviewWaitEvidenceItem is one row of fetch_review_wait_evidence's
// result, as HeatmapResponse.evidence's untyped
// `list[dict[str, Any]]` carries it on the wire for review_wait_density
// (services/heatmap.py:287-300). created_at/first_review_at carry the
// declared DateTime defect -- see package doc comment.
type ReviewWaitEvidenceItem struct {
	RepoID        string    `json:"repo_id"`
	Number        uint32    `json:"number"`
	Title         *string   `json:"title"`
	CreatedAt     time.Time `json:"created_at"`
	FirstReviewAt time.Time `json:"first_review_at"`
}

// IndividualActiveEvidenceItem is one row of
// fetch_individual_active_evidence's result, as HeatmapResponse.evidence
// carries it for active_hours (services/heatmap.py:415-425). AuthorWhen
// carries the declared DateTime defect.
type IndividualActiveEvidenceItem struct {
	Repo        string    `json:"repo"`
	CommitHash  string    `json:"commit_hash"`
	Message     *string   `json:"message"`
	AuthorName  *string   `json:"author_name"`
	AuthorEmail *string   `json:"author_email"`
	AuthorWhen  time.Time `json:"author_when"`
}

// Response is the wire shape of HeatmapResponse (schemas.py:480-484).
// Evidence is nil (JSON null) unless a branch in BuildResponse sets it to
// a (possibly empty, never nil) []ReviewWaitEvidenceItem or
// []IndividualActiveEvidenceItem -- matching Python's `evidence:
// list[dict[str, Any]] | None = None` exactly, including the hotspot_risk
// branch that never sets it (see package doc comment).
type Response struct {
	Axes     Axes   `json:"axes"`
	Cells    []Cell `json:"cells"`
	Legend   Legend `json:"legend"`
	Evidence any    `json:"evidence"`
}

// metricRow is this package's internal generic row shape, ALREADY
// resolved to raw x/y values (int for hour/weekday, time.Time for
// day/week, string for repo/file) plus a float value -- the Go analogue
// of Python's `dict[str, Any]` rows keyed by definition.x_field/
// definition.y_field/"value".
type metricRow struct {
	X, Y  any
	Value float64
}

// hourLabels ports _hour_labels (services/heatmap.py:112-113).
func hourLabels() []string {
	out := make([]string, 24)
	for i := 0; i < 24; i++ {
		out[i] = formatHour(i)
	}
	return out
}

func formatHour(h int) string {
	return fmt.Sprintf("%02d", h)
}

// formatAxisValue ports _format_axis_value (services/heatmap.py:143-160).
// raw is nil for Python's None (returns "").
func formatAxisValue(kind string, raw any) string {
	if raw == nil {
		return ""
	}
	switch kind {
	case "hour":
		if n, ok := raw.(int); ok {
			return formatHour(n)
		}
		return toStr(raw)
	case "weekday":
		n, ok := raw.(int)
		if !ok {
			return toStr(raw)
		}
		if n >= 1 && n <= 7 {
			return weekdayLabels[n-1]
		}
		return toStr(raw)
	case "day", "week":
		if t, ok := raw.(time.Time); ok {
			return t.Format("2006-01-02")
		}
		return toStr(raw)
	default:
		return toStr(raw)
	}
}

func toStr(raw any) string {
	switch v := raw.(type) {
	case string:
		return v
	case int:
		return fmt.Sprintf("%d", v)
	case time.Time:
		return v.Format("2006-01-02")
	default:
		return ""
	}
}

// statusOrder/ageBuckets are unreachable for heatmap (no HEATMAP_METRICS
// entry uses "status" or "age_bucket" as an axis kind), ported anyway to
// keep axisOrder a faithful, total port of _axis_order.
var statusOrder = []string{"backlog", "todo", "in_progress", "in_review", "blocked", "done", "canceled", "unknown"}
var ageBuckets = []string{"0-1d", "1-3d", "3-7d", "7-14d", "14-30d", "30d+"}

// axisOrder ports _axis_order (services/heatmap.py:123-141). totals is
// keyed by the SAME formatted-label strings as values (see package doc
// comment: Python's totals dict is keyed by str(raw), which coincides
// with the formatted label for every kind whose branch actually consults
// totals -- the default/repo/file branch -- so using the formatted label
// consistently changes nothing observable).
//
// axisOrder always returns a non-nil slice, even when values is empty --
// Python's list.sort()/sorted()/list comprehension results are never None,
// so every branch here must marshal to JSON "[]", never "null" (a bare
// `append([]string(nil), empty...)` returns nil in Go, which is the bug
// this shape guards against).
func axisOrder(kind string, values []string, totals map[string]float64) []string {
	seen := map[string]bool{}
	valuesList := make([]string, 0, len(values))
	for _, v := range values {
		if !seen[v] {
			seen[v] = true
			valuesList = append(valuesList, v)
		}
	}

	switch kind {
	case "hour":
		return hourLabels()
	case "weekday":
		filtered := make([]string, 0, len(weekdayLabels))
		presentSet := map[string]bool{}
		for _, v := range valuesList {
			presentSet[v] = true
		}
		for _, label := range weekdayLabels {
			if presentSet[label] {
				filtered = append(filtered, label)
			}
		}
		if len(filtered) == 0 {
			return append([]string{}, weekdayLabels...)
		}
		return filtered
	case "day", "week":
		out := make([]string, len(valuesList))
		copy(out, valuesList)
		sort.Strings(out)
		return out
	case "status":
		filtered := make([]string, 0, len(statusOrder))
		presentSet := map[string]bool{}
		for _, v := range valuesList {
			presentSet[v] = true
		}
		for _, status := range statusOrder {
			if presentSet[status] {
				filtered = append(filtered, status)
			}
		}
		return filtered
	case "age_bucket":
		filtered := make([]string, 0, len(ageBuckets))
		presentSet := map[string]bool{}
		for _, v := range valuesList {
			presentSet[v] = true
		}
		for _, bucket := range ageBuckets {
			if presentSet[bucket] {
				filtered = append(filtered, bucket)
			}
		}
		return filtered
	default:
		// A stable sort by total alone leaves ties ordered by first
		// appearance in valuesList -- the incidental scan order the
		// query returning rows produced, which ClickHouse gives no
		// guarantee over among rows tying on the query's own ORDER BY.
		// Breaking a tie by name, ascending, makes the result a pure
		// function of (name, total): two calls fed the same rows in two
		// different scan orders always agree.
		out := make([]string, len(valuesList))
		copy(out, valuesList)
		sort.Slice(out, func(i, j int) bool {
			if totals[out[i]] != totals[out[j]] {
				return totals[out[i]] > totals[out[j]]
			}
			return out[i] < out[j]
		})
		return out
	}
}

// axisValues ports _axis_values (services/heatmap.py:167-171): isX
// selects definition.x_field/x_axis (true) or y_field/y_axis (false).
func axisValues(rows []metricRow, isX bool, kind string) []string {
	values := make([]string, 0, len(rows))
	totals := map[string]float64{}
	for _, row := range rows {
		var raw any
		if isX {
			raw = row.X
		} else {
			raw = row.Y
		}
		label := formatAxisValue(kind, raw)
		values = append(values, label)
		totals[label] = totals[label] + row.Value
	}
	return axisOrder(kind, values, totals)
}

// cellsFromRows ports _cells_from_rows (services/heatmap.py:174-189).
func cellsFromRows(rows []metricRow, xKind, yKind string) []Cell {
	cells := make([]Cell, 0, len(rows))
	for _, row := range rows {
		xLabel := formatAxisValue(xKind, row.X)
		yLabel := formatAxisValue(yKind, row.Y)
		if xLabel != "" && yLabel != "" {
			cells = append(cells, Cell{X: xLabel, Y: yLabel, Value: row.Value})
		}
	}
	return cells
}
