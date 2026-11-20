package rowview

import (
	"strings"
	"testing"
)

func TestTeamSourcePicksOneViewPerKeyWithoutTheViewColumn(t *testing.T) {
	for table, key := range map[string]string{
		"work_item_metrics_daily": "LIMIT 1 BY work_item_metrics_daily.org_id, work_item_metrics_daily.day, " +
			"work_item_metrics_daily.provider, work_item_metrics_daily.work_scope_id, work_item_metrics_daily.team_id\n",
		"work_item_state_durations_daily": "LIMIT 1 BY work_item_state_durations_daily.org_id, work_item_state_durations_daily.day, " +
			"work_item_state_durations_daily.provider, work_item_state_durations_daily.work_scope_id, " +
			"work_item_state_durations_daily.team_id, work_item_state_durations_daily.status\n",
	} {
		source := TeamSource(table, table+".org_id = {org_id:String}")
		for _, want := range []string{
			"FROM " + table + " FINAL",
			"WHERE (" + table + ".org_id = {org_id:String})",
			table + ".row_view IN ('', 'org', 'team')",
			"multiIf(" + table + ".row_view = 'team', 2, " + table + ".row_view = 'org', 1, 0) DESC",
			key,
		} {
			if !strings.Contains(source, want) {
				t.Errorf("TeamSource(%s) lacks %q:\n%s", table, want, source)
			}
		}
		if strings.Count(source, "LIMIT 1 BY") != 1 {
			t.Errorf("TeamSource(%s) must pick in one LIMIT 1 BY:\n%s", table, source)
		}
	}
}

func TestTeamSourceRefusesATableWithoutTeamRowsAndAnEmptyFilter(t *testing.T) {
	for _, call := range []struct {
		name         string
		table, where string
	}{
		{"person rows", "work_item_user_metrics_daily", "org_id = {org_id:String}"},
		{"table without the view column", "team_metrics_daily", "org_id = {org_id:String}"},
		{"empty filter", "work_item_metrics_daily", " "},
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s: TeamSource(%q, %q) did not panic", call.name, call.table, call.where)
				}
			}()
			_ = TeamSource(call.table, call.where)
		}()
	}
}
