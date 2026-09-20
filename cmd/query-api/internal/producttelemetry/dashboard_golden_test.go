package producttelemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
)

type goldenQuery struct {
	Query  string            `json:"query"`
	Params map[string]string `json:"params"`
}

type goldenError struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

type goldenCase struct {
	Name          string             `json:"name"`
	Rows          map[string][][]any `json:"rows"`
	Start         string             `json:"start"`
	End           string             `json:"end"`
	Org           string             `json:"org"`
	Client        bool               `json:"client"`
	PythonQueries []goldenQuery      `json:"python_queries"`
	Error         *goldenError       `json:"error"`
	Expected      json.RawMessage    `json:"expected"`
}

// goldenMarkers names the statements by the fragment that tells them apart, in
// the order the Go reader issues them.
var goldenMarkers = []struct{ key, fragment string }{
	{"daily", "toDate(occurred_at)"},
	{"routes", "name = 'page_viewed'"},
	{"features", "name = 'feature_viewed'"},
	{"filters", "name = 'filter_changed'"},
	{"charts", "name = 'chart_interacted'"},
	{"errors", "name = 'client_error'"},
	{"session", "name = 'session_ended'"},
}

func num(v any) json.Number {
	n, _ := v.(json.Number)
	return n
}

func u64(v any) uint64 {
	x, err := strconv.ParseUint(string(num(v)), 10, 64)
	if err != nil {
		panic(err)
	}
	return x
}

func f64(v any) float64 {
	if s, ok := v.(string); ok && s == "NaN" {
		return math.NaN()
	}
	x, err := strconv.ParseFloat(string(num(v)), 64)
	if err != nil {
		panic(err)
	}
	return x
}

// scriptedRowsFor converts one golden row to the typed row the reader scans.
func scriptedRowFor(key string, r []any) []any {
	switch key {
	case "daily":
		d, _ := time.Parse("2006-01-02", r[0].(string))
		return []any{d, u64(r[1])}
	case "routes":
		return []any{nilOrString(r[0]), u64(r[1]), u64(r[2]), u64(r[3])}
	case "features":
		return []any{r[0].(string), r[1].(string), u64(r[2]), u64(r[3])}
	case "filters":
		return []any{r[0].(string), r[1].(string), u64(r[2]), f64(r[3])}
	case "charts":
		return []any{r[0].(string), r[1].(string), r[2].(string), u64(r[3]), u64(r[4])}
	case "errors":
		return []any{nilOrString(r[0]), r[1].(string), r[2].(string), u64(r[3]), u64(r[4])}
	default:
		return []any{f64(r[0]), f64(r[1]), f64(r[2]), f64(r[3]), f64(r[4]), f64(r[5])}
	}
}

func nilOrString(v any) any {
	if s, ok := v.(string); ok {
		return s
	}
	return nil
}

// TestOrgDashboardMatchesTheFrozenGolden runs the Go reader over the requests
// the deleted reference resolver and loader were run over. For every request
// it compares which statements run, the bindings each carries (organisation
// hash, start, end), and the complete answer, including the empty-set
// aggregate, non-finite averages, fractional quantiles and the refusals.
func TestOrgDashboardMatchesTheFrozenGolden(t *testing.T) {
	raw, err := os.ReadFile("testdata/dashboard_golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []goldenCase
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&cases); err != nil {
		t.Fatal(err)
	}
	if len(cases) < 17 {
		t.Fatalf("golden holds %d cases, want at least 17", len(cases))
	}
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			start, _ := time.Parse("2006-01-02", tc.Start)
			end, _ := time.Parse("2006-01-02", tc.End)
			rowsByFragment := map[string][][]any{}
			for _, m := range goldenMarkers {
				for _, r := range tc.Rows[m.key] {
					rowsByFragment[m.fragment] = append(rowsByFragment[m.fragment], scriptedRowFor(m.key, r))
				}
			}
			ch := &scriptedCH{rows: rowsByFragment}

			// The org hash the reference resolver used for this organisation.
			if err := CheckRange(start, end); err != nil {
				if tc.Error == nil || tc.Error.Message != err.Error() {
					t.Fatalf("range error %v, want %+v", err, tc.Error)
				}
				return
			}
			if tc.Error != nil {
				// Refusals decided before the reader runs (missing client, missing
				// organisation) belong to the resolver wiring, pinned by the
				// resolver tests; the reader itself is not reached.
				return
			}
			got, err := (&Reader{ClickHouse: ch}).Org(context.Background(), tc.Org, Range{Start: start, End: end})
			if err != nil {
				t.Fatal(err)
			}

			seen := map[string]goldenQuery{}
			for _, c := range ch.calls {
				for _, m := range goldenMarkers {
					if strings.Contains(c.sql, m.fragment) {
						seen[m.key] = goldenQuery{Query: m.key, Params: map[string]string{
							"org_id_hash": c.bindings["org_id_hash"].(string),
							"start":       c.bindings["start"].(string),
							"end":         c.bindings["end"].(string),
						}}
					}
				}
			}
			var want []goldenQuery
			for _, q := range tc.PythonQueries {
				want = append(want, q)
			}
			var have []goldenQuery
			for _, q := range seen {
				have = append(have, q)
			}
			sort.Slice(have, func(i, j int) bool { return have[i].Query < have[j].Query })
			if !reflect.DeepEqual(have, want) || len(ch.calls) != len(want) {
				t.Fatalf("statements/bindings\n got  %+v (%d calls)\n want %+v", have, len(ch.calls), want)
			}

			gotJSON := roundTrip(t, canonical(got))
			var wantJSON any
			if err := json.Unmarshal(tc.Expected, &wantJSON); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(gotJSON, wantJSON) {
				t.Fatalf("answer\n got  %s\n want %s", tc.Expected, mustJSON(gotJSON))
			}
		})
	}
}

func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func roundTrip(t *testing.T, v any) any {
	t.Helper()
	var out any
	if err := json.Unmarshal([]byte(mustJSON(v)), &out); err != nil {
		t.Fatal(err)
	}
	return out
}

// canonical renders the Go answer in the golden's own shape.
func canonical(d *model.ProductTelemetryDashboardType) map[string]any {
	daily := []map[string]any{}
	for _, x := range d.DailyActiveUsers {
		daily = append(daily, map[string]any{"day": x.Day.String(), "n": x.ActiveAnonymousUsers})
	}
	routes := []map[string]any{}
	for _, x := range d.TopRoutes {
		routes = append(routes, map[string]any{"route": x.RoutePattern, "events": x.Events, "sessions": x.Sessions, "users": x.AnonymousUsers})
	}
	features := []map[string]any{}
	for _, x := range d.FeatureViews {
		features = append(features, map[string]any{"feature": x.Feature, "surface": x.Surface, "views": x.Views, "users": x.AnonymousUsers})
	}
	filters := []map[string]any{}
	for _, x := range d.FilterChanges {
		filters = append(filters, map[string]any{"view": x.View, "key": x.FilterKey, "changes": x.Changes, "avg": x.AvgValueCount})
	}
	charts := []map[string]any{}
	for _, x := range d.ChartInteractions {
		charts = append(charts, map[string]any{"chart": x.Chart, "action": x.Action, "surface": x.Surface, "interactions": x.Interactions, "sessions": x.Sessions})
	}
	errs := []map[string]any{}
	for _, x := range d.ClientErrors {
		errs = append(errs, map[string]any{"route": x.RoutePattern, "boundary": x.Boundary, "cls": x.ErrorClass, "errors": x.Errors, "users": x.AffectedAnonymousUsers})
	}
	s := d.SessionSummary
	return map[string]any{
		"daily": daily, "routes": routes, "features": features, "filters": filters, "charts": charts, "errors": errs,
		"session": map[string]any{"p50": s.P50DurationMs, "p75": s.P75DurationMs, "p90": s.P90DurationMs, "p95": s.P95DurationMs, "pages": s.AvgPagesViewed, "inter": s.AvgInteractions},
	}
}
