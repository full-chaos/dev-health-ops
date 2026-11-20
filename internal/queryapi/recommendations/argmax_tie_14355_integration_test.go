//go:build integration

package recommendations

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chquery"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// Two versions of one recommendations_daily key that share computed_at serve ONE version whatever the scan order.
// CHAOS-14355.
func TestArgMaxTieRecommendations14355(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = inst.Close(context.Background()) }()
	chschema.Apply(ctx, t, inst)
	opts, err := stdclickhouse.ParseDSN(inst.URI)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	client, err := chquery.NewProductionClient(inst.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	const org = "org-14355"
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	recAt := func(title, severity, at string) string {
		return fmt.Sprintf(`INSERT INTO recommendations_daily (team_id, org_id, rule_id, window_start, window_end, fired, severity, title, rationale, success_criterion, evidence_json, computed_at)
            SELECT 'team-a', '%s', 'rule-1', toDate('2026-07-28'), toDate('2026-08-03'), true, '%s', '%s', 'why-%s', 'done-%s', '[]', toDateTime64('%s', 3, 'UTC')`, org, severity, title, title, title, at)
	}
	rec := func(title, severity string) string { return recAt(title, severity, "2026-08-03 12:00:00.000") }
	exec := func(q string) {
		t.Helper()
		if err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	exec(`SYSTEM STOP MERGES recommendations_daily`)
	servedOf := func(versions ...string) []string {
		t.Helper()
		orders := [][]string{versions}
		if len(versions) == 2 {
			orders = append(orders, []string{versions[1], versions[0]})
		}
		seen := map[string]bool{}
		for _, order := range orders {
			for range 3 {
				exec(`TRUNCATE TABLE recommendations_daily`)
				for _, q := range order {
					exec(q)
				}
				got, err := Resolve(ctx, client, org, "team-a", model.WindowInput{Value: 4, Unit: model.WindowUnitWeek}, now)
				if err != nil {
					t.Fatal(err)
				}
				var parts []string
				for _, r := range got {
					parts = append(parts, r.RuleID+" "+string(r.Severity)+" "+r.Title+" "+r.Rationale+" "+r.SuccessCriterion)
				}
				seen[strings.Join(parts, ";")] = true
			}
		}
		served := make([]string, 0, len(seen))
		for v := range seen {
			served = append(served, v)
		}
		sort.Strings(served)
		return served
	}
	t.Run("a tie serves one version, every column from it", func(t *testing.T) {
		a, b := rec("first title", "warning"), rec("second title", "critical")
		refA, refB := servedOf(a), servedOf(b)
		got := servedOf(a, b)
		if len(got) != 1 || (got[0] != refA[0] && got[0] != refB[0]) {
			t.Errorf("same key, same computed_at served %q; want one of %q / %q", got, refA, refB)
		}
	})
	t.Run("a newer computed_at wins over the tiebreak", func(t *testing.T) {
		for _, v := range [][2]string{{"first title", "second title"}, {"second title", "first title"}} {
			want := servedOf(recAt(v[1], "critical", "2026-08-03 12:00:00.001"))
			got := servedOf(recAt(v[0], "warning", "2026-08-03 12:00:00.000"), recAt(v[1], "critical", "2026-08-03 12:00:00.001"))
			if len(got) != 1 || got[0] != want[0] {
				t.Errorf("older %q newer %q: served %q; want %q", v[0], v[1], got, want)
			}
		}
	})
}
