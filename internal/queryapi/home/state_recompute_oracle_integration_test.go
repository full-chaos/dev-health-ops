//go:build integration

package home

import (
	"context"
	"fmt"
	"math"
	"sort"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/linkeditems"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/rowview"
	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse/liverow"
)

// C12 of CHAOS-13088, on the oracle world of CHAOS-9094 (seedOracleWorld): the state-duration recompute of the items
// linked to a repository ceiling against work_item_state_durations_daily as the production executor writes it. A
// recompute over every repository of a world whose items are all linked equals the table, row for row, every day; a
// recompute over repository 1 equals the table of a world that holds only that repository's linked items. The world
// has an open blocker (jira:ABC-2 blocked by gh:a/r#6), so the blocked overlay is part of the comparison.

type stateKey struct{ day, provider, scope, team, status string }

type stateValue struct {
	teamName string
	hours    float64
	items    uint32
}

func readStateTable(ctx context.Context, t *testing.T, client QueryClient, org string, from, to time.Time) map[stateKey]stateValue {
	t.Helper()
	rows, err := client.Query(ctx, `
SELECT toString(day), provider, work_scope_id, team_id, team_name, status, duration_hours, items_touched
FROM work_item_state_durations_daily FINAL
WHERE org_id = {org_id:String} AND day >= {from:Date} AND day < {to:Date}
  AND `+rowview.OrgPredicate("work_item_state_durations_daily", "")+`
  AND `+liverow.Predicate("work_item_state_durations_daily", ""), []dhclickhouse.Binding{
		{Name: "org_id", Value: org}, {Name: "from", Value: from.Format("2006-01-02")}, {Name: "to", Value: to.Format("2006-01-02")},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[stateKey]stateValue{}
	for rows.Next() {
		var key stateKey
		var value stateValue
		if err := rows.Scan(&key.day, &key.provider, &key.scope, &key.team, &value.teamName, &key.status, &value.hours, &value.items); err != nil {
			t.Fatal(err)
		}
		out[key] = value
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func recomputedState(ctx context.Context, t *testing.T, client QueryClient, org string, ids []string, from, to time.Time) map[stateKey]stateValue {
	t.Helper()
	byDay, err := linkeditems.NewStateDaily(linkeditems.Loader{
		Client: client, OrgID: org, RepoIDs: ids, WindowStart: from, WindowEnd: to, Budget: linkeditems.StateBudget,
	}, time.Now().UTC()).Rows(ctx)
	if err != nil {
		t.Fatal(err)
	}
	out := map[stateKey]stateValue{}
	for day, rows := range byDay {
		for _, row := range rows {
			out[stateKey{day.Format("2006-01-02"), row.Provider, row.WorkScopeID, row.TeamID, row.Status}] = stateValue{row.TeamName, row.DurationHours, row.ItemsTouched}
		}
	}
	return out
}

func sameState(t *testing.T, label string, got, want map[stateKey]stateValue) {
	t.Helper()
	var keys []stateKey
	seen := map[stateKey]bool{}
	for key := range got {
		keys, seen[key] = append(keys, key), true
	}
	for key := range want {
		if !seen[key] {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return fmt.Sprint(keys[i]) < fmt.Sprint(keys[j]) })
	for _, key := range keys {
		g, inGot := got[key]
		w, inWant := want[key]
		switch {
		case !inGot:
			t.Errorf("%s: %v is in the daily table (%+v) and not in the recompute", label, key, w)
		case !inWant:
			t.Errorf("%s: %v is in the recompute (%+v) and not in the daily table", label, key, g)
		case g.teamName != w.teamName || g.items != w.items || math.Abs(g.hours-w.hours) > 1e-9:
			t.Errorf("%s: %v recompute %+v, daily table %+v", label, key, g, w)
		}
	}
}

func TestStateRecomputeOfTheLinkedItemsEqualsTheDailyStateTable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	admin, client := newHomeTestClickHouse(ctx, t)

	const orgAll, orgR1 = "state-oracle-all", "state-oracle-r1only"
	from, to := time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC), time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC)

	seedOracleWorld(ctx, t, admin, orgAll, func(oracleItem) bool { return true })
	fillDailyTables(ctx, t, admin, orgAll, from, to)
	seedOracleWorld(ctx, t, admin, orgR1, func(item oracleItem) bool {
		for _, link := range item.links {
			if link.repo == oracleR1 {
				return true
			}
		}
		return false
	})
	fillDailyTables(ctx, t, admin, orgR1, from, to)

	table := readStateTable(ctx, t, client, orgAll, from, to)
	blocked := false
	for key, value := range table {
		if key.status == "blocked" && value.hours > 0 {
			blocked = true
		}
	}
	if len(table) == 0 || !blocked {
		t.Fatalf("the daily table holds %d rows and blocked hours %v: the oracle would not compare the overlay", len(table), blocked)
	}
	sameState(t, "every repository of the world", recomputedState(ctx, t, client, orgAll, []string{oracleR1, oracleR2}, from, to), table)

	// The status list of the filter options under a ceiling (linkeditems.Statuses) is the distinct status of the table
	// when the job ran every day of the items' lives, as it did here.
	statuses, err := linkeditems.Statuses(ctx, linkeditems.Loader{
		Client: client, OrgID: orgAll, RepoIDs: []string{oracleR1, oracleR2},
		WindowStart: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), WindowEnd: to, Budget: linkeditems.StateBudget,
	}, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	tableStatuses := map[string]bool{}
	for key := range table {
		tableStatuses[key.status] = true
	}
	var want []string
	for status := range tableStatuses {
		want = append(want, status)
	}
	sort.Strings(want)
	if fmt.Sprint(statuses) != fmt.Sprint(want) {
		t.Errorf("linked statuses %v, daily table statuses %v", statuses, want)
	}

	one := recomputedState(ctx, t, client, orgAll, []string{oracleR1}, from, to)
	sameState(t, "repository 1 against the world of its linked items", one, readStateTable(ctx, t, client, orgR1, from, to))
	if len(one) == 0 || len(one) >= len(table) {
		t.Errorf("repository 1 has %d rows, the whole world %d: the view narrowed nothing", len(one), len(table))
	}
}
