package security

import (
	"errors"

	"context"
	"encoding/json"
	"github.com/full-chaos/dev-health-go/clickhouse"
	"math"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graphqldate"
)

// The golden holds, for neutral securityOverview requests (every filter shape,
// empty and full result sets, an undefined and a NaN mean, and a failing read
// of each of the four queries), the filter parameters the deleted reference
// resolver bound and the exact answer it built. It is frozen from that
// resolver; nothing here calls Python.
//
// Named differences, not compared here:
//   - the statement text: this resolver scopes the alert side of the join by
//     org as well as the repository side (sa.org_id); the reference scoped the
//     repository side only. The filter values and org are compared.
//   - the reference ran its four reads concurrently, this resolver runs them
//     in order and stops at the first failure; both answer an error.

type secOverviewGoldenCase struct {
	Name       string           `json:"name"`
	FilterSpec map[string]any   `json:"filter_spec"`
	Kpi        []map[string]any `json:"kpi"`
	NaN        bool             `json:"nan"`
	BD         []map[string]any `json:"bd"`
	Top        []map[string]any `json:"top"`
	Trend      []map[string]any `json:"trend"`
	Fail       *string          `json:"fail"`
	Error      bool             `json:"error"`
	PyParams   []map[string]any `json:"python_params"`
	Expected   *struct {
		Kpis struct {
			OpenTotal int      `json:"open_total"`
			Critical  int      `json:"critical"`
			High      int      `json:"high"`
			Mean      *float64 `json:"mean"`
			Delta     int      `json:"delta"`
		} `json:"kpis"`
		Breakdown []struct {
			Severity string `json:"severity"`
			Count    int    `json:"count"`
		} `json:"breakdown"`
		Top []struct {
			RepoID   string  `json:"repo_id"`
			RepoName string  `json:"repo_name"`
			RepoURL  *string `json:"repo_url"`
			Count    int     `json:"count"`
		} `json:"top"`
		Trend []struct {
			Day    string `json:"day"`
			Opened int    `json:"opened"`
			Fixed  int    `json:"fixed"`
		} `json:"trend"`
	} `json:"expected"`
}

func secOverviewFilter(t *testing.T, spec map[string]any) *model.SecurityAlertFilterInput {
	if spec == nil {
		return nil
	}
	f := &model.SecurityAlertFilterInput{}
	strs := func(v any) []string {
		out := []string{}
		for _, x := range v.([]any) {
			out = append(out, x.(string))
		}
		return out
	}
	for k, v := range spec {
		switch k {
		case "open_only":
			f.OpenOnly = v.(bool)
		case "repo_ids":
			f.RepoIds = strs(v)
		case "severities":
			for _, s := range strs(v) {
				f.Severities = append(f.Severities, model.SecuritySeverityInput(s))
			}
		case "sources":
			for _, s := range strs(v) {
				f.Sources = append(f.Sources, model.SecuritySourceInput(s))
			}
		case "states":
			for _, s := range strs(v) {
				f.States = append(f.States, model.SecurityStateInput(s))
			}
		case "since", "until":
			d, err := graphqldate.Parse(v.(string))
			if err != nil {
				t.Fatal(err)
			}
			if k == "since" {
				f.Since = &d
			} else {
				f.Until = &d
			}
		case "search":
			s := v.(string)
			f.Search = &s
		default:
			t.Fatalf("unknown filter %q", k)
		}
	}
	return f
}

func TestSecurityOverviewMatchesTheFrozenGolden(t *testing.T) {
	raw, err := os.ReadFile("testdata/secoverview_golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []secOverviewGoldenCase
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	if len(cases) < 20 {
		t.Fatalf("golden has %d cases", len(cases))
	}
	num := func(v any) uint64 { return uint64(v.(float64)) }
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			var kpi, bd, top, trend [][]any
			for _, r := range c.Kpi {
				var mean any
				if c.NaN {
					mean = math.NaN()
				} else if r["mean_days_to_fix_30d"] != nil {
					mean = r["mean_days_to_fix_30d"].(float64)
				}
				kpi = append(kpi, []any{num(r["open_total"]), num(r["critical"]), num(r["high"]), mean, int64(r["open_delta_30d"].(float64))})
			}
			for _, r := range c.BD {
				bd = append(bd, []any{r["severity"].(string), num(r["count"])})
			}
			for _, r := range c.Top {
				top = append(top, []any{r["repo_id"].(string), r["repo_name"].(string), num(r["count"])})
			}
			for _, r := range c.Trend {
				d, err := time.Parse("2006-01-02", r["day"].(string))
				if err != nil {
					t.Fatal(err)
				}
				trend = append(trend, []any{d, num(r["opened"]), num(r["fixed"])})
			}
			cl := &failingClient{recordingClient: recordingClient{responses: [][][]any{kpi, bd, top, trend}}, failAt: -1}
			if c.Fail != nil {
				cl.failAt = map[string]int{"kpi": 0, "breakdown": 1, "top": 2, "trend": 3}[*c.Fail]
			}
			got, err := ResolveOverview(context.Background(), cl, "org-1", secOverviewFilter(t, c.FilterSpec))
			if c.Error {
				if err == nil {
					t.Fatal("the reference answered an error and this resolver did not")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(cl.bindings) != 4 {
				t.Fatalf("%d reads", len(cl.bindings))
			}
			for i, b := range cl.bindings {
				bound := bindingMap(b)
				want := map[string]any{}
				for k, v := range c.PyParams[i] {
					want[k] = v
				}
				have := map[string]any{}
				for k, v := range bound {
					switch k {
					case "open_states_agg":
						continue
					case "since", "until":
						v = v.(time.Time).Format("2006-01-02 15:04:05") + "+00:00"
					}
					have[k] = v
				}
				// The reference bound the list of open states under a name per
				// query; compare it under the filter name only.
				hb, _ := json.Marshal(have)
				wb, _ := json.Marshal(want)
				if string(hb) != string(wb) {
					t.Errorf("read %d params %s want %s", i, hb, wb)
				}
			}
			e := c.Expected
			k := got.Kpis
			if k.OpenTotal != e.Kpis.OpenTotal || k.Critical != e.Kpis.Critical || k.High != e.Kpis.High || k.OpenDelta30d != e.Kpis.Delta || !reflect.DeepEqual(k.MeanDaysToFix30d, e.Kpis.Mean) {
				t.Errorf("kpis %#v want %#v", k, e.Kpis)
			}
			if len(got.SeverityBreakdown) != len(e.Breakdown) || len(got.TopRepos) != len(e.Top) || len(got.Trend) != len(e.Trend) {
				t.Fatalf("lengths %d/%d/%d", len(got.SeverityBreakdown), len(got.TopRepos), len(got.Trend))
			}
			for i, b := range got.SeverityBreakdown {
				if b.Severity != e.Breakdown[i].Severity || b.Count != e.Breakdown[i].Count {
					t.Errorf("breakdown[%d] %v", i, b)
				}
			}
			for i, r := range got.TopRepos {
				w := e.Top[i]
				if r.RepoID != w.RepoID || r.RepoName != w.RepoName || r.Count != w.Count || !reflect.DeepEqual(r.RepoURL, w.RepoURL) {
					t.Errorf("top[%d] %v", i, r)
				}
			}
			for i, p := range got.Trend {
				w := e.Trend[i]
				if p.Day.String() != w.Day || p.Opened != w.Opened || p.Fixed != w.Fixed {
					t.Errorf("trend[%d] %v", i, p)
				}
			}
		})
	}
}

// failingClient fails the read at position failAt (zero based); -1 never fails.
type failingClient struct {
	recordingClient
	failAt int
	calls  int
}

func (c *failingClient) Query(ctx context.Context, statement string, b []clickhouse.Binding) (clickhouse.RowScanner, error) {
	i := c.calls
	c.calls++
	rs, err := c.recordingClient.Query(ctx, statement, b)
	if i == c.failAt {
		return nil, errors.New("db down")
	}
	return rs, err
}
