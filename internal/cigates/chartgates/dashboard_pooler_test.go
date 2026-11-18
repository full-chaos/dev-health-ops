package chartgates_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const poolerDashboardRel = "deploy/signoz/dashboards/pooler-transaction.json"

type poolerDashboard struct {
	Spec struct {
		Layouts []struct {
			Spec struct {
				Items []struct {
					Content struct {
						Ref string `json:"$ref"`
					} `json:"content"`
				} `json:"items"`
			} `json:"spec"`
		} `json:"layouts"`
		Panels map[string]struct {
			Spec struct {
				Queries []struct {
					Spec struct {
						Plugin struct {
							Spec struct {
								Disabled     bool `json:"disabled"`
								Aggregations []struct {
									MetricName string `json:"metricName"`
								} `json:"aggregations"`
								Filter struct {
									Expression string `json:"expression"`
								} `json:"filter"`
								GroupBy []struct {
									Name          string `json:"name"`
									FieldContext  string `json:"fieldContext"`
									FieldDataType string `json:"fieldDataType"`
								} `json:"groupBy"`
							} `json:"spec"`
						} `json:"plugin"`
					} `json:"spec"`
				} `json:"queries"`
			} `json:"spec"`
		} `json:"panels"`
	} `json:"spec"`
}

// poolerDashboardProblems lists every way the file departs from the four SHOW POOLS series, per pool user, in production.
func poolerDashboardProblems(raw []byte) []string {
	var d poolerDashboard
	if err := json.Unmarshal(raw, &d); err != nil {
		return []string{"unparsable: " + err.Error()}
	}
	want := map[string]string{
		"client-active":  "pgbouncer_pools_client_active_connections",
		"client-waiting": "pgbouncer_pools_client_waiting_connections",
		"server-active":  "pgbouncer_pools_server_active_connections",
		"client-maxwait": "pgbouncer_pools_client_maxwait_seconds",
	}
	var out []string
	if len(d.Spec.Panels) != len(want) {
		out = append(out, "panel count")
	}
	for id, metric := range want {
		p, ok := d.Spec.Panels[id]
		if !ok || len(p.Spec.Queries) != 1 {
			out = append(out, "panel "+id+" missing or not one query")
			continue
		}
		q := p.Spec.Queries[0].Spec.Plugin.Spec
		if q.Disabled {
			out = append(out, "panel "+id+" query disabled")
		}
		if len(q.Aggregations) != 1 || q.Aggregations[0].MetricName != metric {
			out = append(out, "panel "+id+" metric")
		}
		if q.Filter.Expression != "deployment.environment = 'production'" {
			out = append(out, "panel "+id+" filter")
		}
		if len(q.GroupBy) != 1 || q.GroupBy[0].Name != "user" || q.GroupBy[0].FieldContext != "attribute" || q.GroupBy[0].FieldDataType != "string" {
			out = append(out, "panel "+id+" groupBy")
		}
	}
	placed := map[string]int{}
	for _, l := range d.Spec.Layouts {
		for _, it := range l.Spec.Items {
			placed[strings.TrimPrefix(it.Content.Ref, "#/spec/panels/")]++
		}
	}
	for id := range want {
		if placed[id] != 1 {
			out = append(out, "panel "+id+" not placed exactly once in the layout")
		}
	}
	if len(placed) != len(want) {
		out = append(out, "layout places a panel outside the inventory")
	}
	return out
}

func TestPoolerDashboardReadsShowPoolsSeries(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(realRoot(t), poolerDashboardRel))
	if err != nil {
		t.Fatal(err)
	}
	if p := poolerDashboardProblems(raw); len(p) > 0 {
		t.Fatalf("pooler dashboard: %v", p)
	}
	for name, mut := range map[string][2]string{
		"disabled query":       {`"disabled": false`, `"disabled": true`},
		"groupBy fieldContext": {`"fieldContext": "attribute"`, `"fieldContext": "resource"`},
		"missing layout ref":   {`"$ref": "#/spec/panels/client-maxwait"`, `"$ref": "#/spec/panels/client-active"`},
		"wrong metric":         {`pgbouncer_pools_client_waiting_connections`, `pgbouncer_pools_client_waiting_cancel_connections`},
		"filter widened":       {`deployment.environment = 'production'`, `deployment.environment EXISTS`},
	} {
		s := string(raw)
		if !strings.Contains(s, mut[0]) {
			t.Fatalf("%s: mutation anchor %q not in file", name, mut[0])
		}
		if len(poolerDashboardProblems([]byte(strings.Replace(s, mut[0], mut[1], 1)))) == 0 {
			t.Errorf("%s: guard accepted the mutated file", name)
		}
	}
}
