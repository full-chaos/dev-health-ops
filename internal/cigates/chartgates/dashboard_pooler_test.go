package chartgates_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

const poolerDashboardRel = "deploy/signoz/dashboards/pooler-transaction.json"

// The pooler dashboard reads only the four SHOW POOLS series the exporter sidecar emits, per pool user, in production.
func TestPoolerDashboardReadsShowPoolsSeries(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(realRoot(t), poolerDashboardRel))
	if err != nil {
		t.Fatal(err)
	}
	var d struct {
		Spec struct {
			Panels map[string]struct {
				Spec struct {
					Queries []struct {
						Spec struct {
							Plugin struct {
								Spec struct {
									Aggregations []struct {
										MetricName string `json:"metricName"`
									} `json:"aggregations"`
									Filter struct {
										Expression string `json:"expression"`
									} `json:"filter"`
									GroupBy []struct {
										Name string `json:"name"`
									} `json:"groupBy"`
								} `json:"spec"`
							} `json:"plugin"`
						} `json:"spec"`
					} `json:"queries"`
				} `json:"spec"`
			} `json:"panels"`
		} `json:"spec"`
	}
	if err := json.Unmarshal(raw, &d); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"client-active":  "pgbouncer_pools_client_active_connections",
		"client-waiting": "pgbouncer_pools_client_waiting_connections",
		"server-active":  "pgbouncer_pools_server_active_connections",
		"client-maxwait": "pgbouncer_pools_client_maxwait_seconds",
	}
	if len(d.Spec.Panels) != len(want) {
		t.Fatalf("panels = %d, want %d", len(d.Spec.Panels), len(want))
	}
	for id, metric := range want {
		p, ok := d.Spec.Panels[id]
		if !ok || len(p.Spec.Queries) != 1 {
			t.Fatalf("panel %q missing or not one query", id)
		}
		q := p.Spec.Queries[0].Spec.Plugin.Spec
		if len(q.Aggregations) != 1 || q.Aggregations[0].MetricName != metric {
			t.Errorf("panel %q metric = %+v, want %s", id, q.Aggregations, metric)
		}
		if q.Filter.Expression != "deployment.environment = 'production'" {
			t.Errorf("panel %q filter = %q", id, q.Filter.Expression)
		}
		if len(q.GroupBy) != 1 || q.GroupBy[0].Name != "user" {
			t.Errorf("panel %q groupBy = %+v, want [user]", id, q.GroupBy)
		}
	}
}
