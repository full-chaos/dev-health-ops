package testopsrisk

import (
	"context"
	_ "embed"
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
)

//go:embed testdata/python_cells.json
var pythonCells []byte

type fakeRows struct {
	rows [][]any
	i    int
}

func (f *fakeRows) Next() bool   { f.i++; return f.i <= len(f.rows) }
func (f *fakeRows) Err() error   { return nil }
func (f *fakeRows) Close() error { return nil }
func (f *fakeRows) Scan(dest ...any) error {
	for i, v := range f.rows[f.i-1] {
		rv := reflect.ValueOf(dest[i]).Elem()
		if v == nil {
			rv.Set(reflect.Zero(rv.Type()))
			continue
		}
		val := reflect.ValueOf(v)
		if rv.Kind() == reflect.Ptr && val.Kind() != reflect.Ptr {
			p := reflect.New(rv.Type().Elem())
			p.Elem().Set(val)
			rv.Set(p)
			continue
		}
		rv.Set(val)
	}
	return nil
}

type fakeClient struct {
	daily, quadrant [][]any
	statements      []string
	bindings        [][]clickhouse.Binding
}

func (c *fakeClient) Query(_ context.Context, st string, b []clickhouse.Binding) (clickhouse.RowScanner, error) {
	c.statements = append(c.statements, st)
	c.bindings = append(c.bindings, b)
	if strings.Contains(st, "release_daily") {
		return &fakeRows{rows: c.daily}, nil
	}
	return &fakeRows{rows: c.quadrant}, nil
}

func date(s string) graphqldate.Date {
	d, err := graphqldate.Parse(s)
	if err != nil {
		panic(err)
	}
	return d
}

func num(v any) any {
	switch x := v.(type) {
	case nil:
		return nil
	case string:
		if x == "NaN" {
			return math.NaN()
		}
	case float64:
		return x
	}
	return v
}

type cell struct {
	Daily []map[string]any `json:"daily"`
	Quad  []struct {
		Label   string   `json:"repo_label"`
		Success *float64 `json:"pipeline_success_rate"`
		Pass    *float64 `json:"test_pass_rate"`
	} `json:"quadrant"`
	Want struct {
		ReleaseConfidence *float64 `json:"release_confidence"`
		QualityDragHours  *float64 `json:"quality_drag_hours"`
		PipelineStability *float64 `json:"pipeline_stability"`
		Timeseries        [][2]any `json:"timeseries"`
		Breakdown         [][2]any `json:"breakdown"`
		Quadrant          [][3]any `json:"quadrant"`
		ConfidenceSpark   [][2]any `json:"confidence_spark"`
		ConfidenceDelta   *float64 `json:"confidence_delta"`
		DragSpark         [][2]any `json:"drag_spark"`
		DragDelta         *float64 `json:"drag_delta"`
		StabilitySpark    [][2]any `json:"stability_spark"`
		StabilityDelta    *float64 `json:"stability_delta"`
	} `json:"want"`
}

func fp(v any) *float64 {
	v = num(v)
	if v == nil {
		return nil
	}
	f := v.(float64)
	return &f
}

func daily(c cell) [][]any {
	var out [][]any
	for _, r := range c.Daily {
		day, _ := time.Parse("2006-01-02", r["day"].(string))
		row := []any{day}
		for _, k := range []string{"release_confidence", "quality_drag_hours", "failure_rework_hours", "flake_investigation_hours", "queue_wait_hours", "retry_overhead_hours", "pipeline_stability"} {
			if f := fp(r[k]); f != nil {
				row = append(row, *f)
			} else {
				row = append(row, nil)
			}
		}
		out = append(out, row)
	}
	return out
}

func same(a, b *float64) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b || (math.IsNaN(*a) && math.IsNaN(*b))
}

// The expected values were produced by running the Python resolver
// resolve_testops_risk on the same rows (testdata/python_cells.json).
func TestResolve_MatchesPythonCells(t *testing.T) {
	var cells map[string]cell
	if err := json.Unmarshal(pythonCells, &cells); err != nil {
		t.Fatal(err)
	}
	if len(cells) != 8 {
		t.Fatalf("%d cells", len(cells))
	}
	for name, c := range cells {
		t.Run(name, func(t *testing.T) {
			cl := &fakeClient{daily: daily(c)}
			for _, q := range c.Quad {
				cl.quadrant = append(cl.quadrant, []any{q.Label, q.Success, q.Pass})
			}
			got, err := Resolve(context.Background(), cl, "org-1", model.TestOpsRiskInput{StartDate: date("2026-01-01"), EndDate: date("2026-01-31")})
			if err != nil {
				t.Fatal(err)
			}
			w := c.Want
			if !same(got.ReleaseConfidence, w.ReleaseConfidence) || !same(got.QualityDragHours, w.QualityDragHours) || !same(got.PipelineStability, w.PipelineStability) {
				t.Errorf("latest values %v %v %v want %v %v %v", got.ReleaseConfidence, got.QualityDragHours, got.PipelineStability, w.ReleaseConfidence, w.QualityDragHours, w.PipelineStability)
			}
			checkSpark := func(label string, got []model.TestOpsRiskSparkPoint, want [][2]any) {
				if len(got) != len(want) {
					t.Errorf("%s: %d points want %d", label, len(got), len(want))
					return
				}
				for i, p := range got {
					if p.Ts.String() != want[i][0].(string) || !same(&p.Value, fp(want[i][1])) {
						t.Errorf("%s[%d] = %v %v want %v", label, i, p.Ts.String(), p.Value, want[i])
					}
				}
			}
			checkSpark("confidence", got.ConfidenceSpark, w.ConfidenceSpark)
			checkSpark("drag", got.DragSpark, w.DragSpark)
			checkSpark("stability", got.StabilitySpark, w.StabilitySpark)
			if !same(got.ConfidenceDelta, w.ConfidenceDelta) || !same(got.DragDelta, w.DragDelta) || !same(got.StabilityDelta, w.StabilityDelta) {
				t.Errorf("deltas %v %v %v want %v %v %v", got.ConfidenceDelta, got.DragDelta, got.StabilityDelta, w.ConfidenceDelta, w.DragDelta, w.StabilityDelta)
			}
			if len(got.Timeseries) != len(w.Timeseries) {
				t.Fatalf("timeseries %d want %d", len(got.Timeseries), len(w.Timeseries))
			}
			for i, p := range got.Timeseries {
				if p.Date.String() != w.Timeseries[i][0].(string) || !same(&p.RiskScore, fp(w.Timeseries[i][1])) {
					t.Errorf("timeseries[%d] = %v %v want %v", i, p.Date.String(), p.RiskScore, w.Timeseries[i])
				}
			}
			if len(got.QualityDragBreakdown) != len(w.Breakdown) {
				t.Fatalf("breakdown %d want %d", len(got.QualityDragBreakdown), len(w.Breakdown))
			}
			for i, b := range got.QualityDragBreakdown {
				if b.Category != w.Breakdown[i][0].(string) || !same(&b.Hours, fp(w.Breakdown[i][1])) {
					t.Errorf("breakdown[%d] = %v want %v", i, b, w.Breakdown[i])
				}
			}
			if len(got.QuadrantData) != len(w.Quadrant) {
				t.Fatalf("quadrant %d want %d", len(got.QuadrantData), len(w.Quadrant))
			}
			for i, q := range got.QuadrantData {
				if q.ID != w.Quadrant[i][0].(string) || !same(q.PipelineSuccessRate, fp(w.Quadrant[i][1])) || !same(q.TestPassRate, fp(w.Quadrant[i][2])) {
					t.Errorf("quadrant[%d] = %#v want %v", i, q, w.Quadrant[i])
				}
			}
			if got.OrgID != "org-1" || got.Timeseries == nil || got.QualityDragBreakdown == nil || got.QuadrantData == nil || got.ConfidenceSpark == nil || got.DragSpark == nil || got.StabilitySpark == nil {
				t.Errorf("shape: %#v", got)
			}
		})
	}
}

func TestResolve_ScopesEveryStatement(t *testing.T) {
	cl := &fakeClient{}
	if _, err := Resolve(context.Background(), cl, "org-1", model.TestOpsRiskInput{StartDate: date("2026-01-01"), EndDate: date("2026-01-31")}); err != nil {
		t.Fatal(err)
	}
	if len(cl.statements) != 2 {
		t.Fatalf("%d statements", len(cl.statements))
	}
	// Each table read in the daily statement and the quadrant statement is org-scoped;
	// the quadrant's repos join carries the org on its join key.
	if strings.Count(cl.statements[0], "WHERE org_id = {org_id:String}") != 3 {
		t.Error("the daily statement must scope all three tables")
	}
	if !strings.Contains(cl.statements[1], "WHERE org_id = {org_id:String}") || !strings.Contains(cl.statements[1], "repos.org_id = {org_id:String}") {
		t.Error("the quadrant statement must scope the release table and its repos join")
	}
	for i, b := range cl.bindings {
		m := map[string]any{}
		for _, x := range b {
			m[x.Name] = x.Value
		}
		if m["org_id"] != "org-1" || m["start"] != "2026-01-01" || m["end"] != "2026-01-31" {
			t.Errorf("statement %d bindings %#v", i, m)
		}
	}
	if !strings.HasPrefix(strings.TrimSpace(cl.statements[0]), "SELECT") || !strings.Contains(cl.statements[0], "SETTINGS join_use_nulls = 1") || !strings.Contains(cl.statements[1], "LIMIT 50") {
		t.Error("statement shape")
	}
}

func TestClamp01_Table(t *testing.T) {
	for _, c := range []struct{ in, want float64 }{{0.5, 0.5}, {-1, 0}, {2, 1}, {0, 0}, {1, 1}, {math.NaN(), 1}} {
		if got := clamp01(c.in); got != c.want {
			t.Errorf("clamp01(%v) = %v", c.in, got)
		}
	}
}

func TestDelta_Table(t *testing.T) {
	pts := func(v ...float64) []model.TestOpsRiskSparkPoint {
		var out []model.TestOpsRiskSparkPoint
		for _, x := range v {
			out = append(out, model.TestOpsRiskSparkPoint{Value: x})
		}
		return out
	}
	one := 100.0
	neg := -50.0
	for _, c := range []struct {
		in   []model.TestOpsRiskSparkPoint
		want *float64
	}{{nil, nil}, {pts(5), nil}, {pts(0, 5), nil}, {pts(1, 2), &one}, {pts(-2, -3), &neg}, {pts(2, 5, 3), func() *float64 { v := 50.0; return &v }()}} {
		if got := delta(c.in); !same(got, c.want) {
			t.Errorf("delta(%v) = %v want %v", c.in, got, c.want)
		}
	}
}

// A row whose day is absent is skipped, never read as the zero date.
func TestResolve_SkipsRowsWithoutADay(t *testing.T) {
	day, _ := time.Parse("2006-01-02", "2026-01-05")
	cl := &fakeClient{daily: [][]any{
		{nil, 0.5, 1.0, 0.0, 0.0, 0.0, 0.0, 0.5},
		{day, 0.8, nil, nil, nil, nil, nil, nil},
	}}
	got, err := Resolve(context.Background(), cl, "o", model.TestOpsRiskInput{StartDate: date("2026-01-01"), EndDate: date("2026-01-31")})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Timeseries) != 1 || got.Timeseries[0].Date.String() != "2026-01-05" || got.QualityDragHours != nil {
		t.Fatalf("%#v", got)
	}
}
