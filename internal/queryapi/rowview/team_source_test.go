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

// The served variants scope their own read by org_id (CHAOS-14376): the organization predicate sits in the FINAL read
// the pick takes its rows from, beside the reader's filter, and the pick is one LIMIT 1 BY. ServedTeamSource applies
// the live-row rule after the pick; ServedTeamPick does not.
func TestServedTeamViewsScopeTheirOwnReadByOrg(t *testing.T) {
	const where = "team_id = {team_id:String}"
	for _, table := range []string{"work_item_metrics_daily", "work_item_state_durations_daily"} {
		for name, source := range map[string]string{"source": ServedTeamSource(table, where), "pick": ServedTeamPick(table, where)} {
			scoped := "FROM " + table + " FINAL WHERE " + table + ".org_id = {org_id:String} AND (" + where + ")"
			if !strings.Contains(source, scoped) {
				t.Errorf("%s %s lacks %q:\n%s", table, name, scoped, source)
			}
			if strings.Count(source, "LIMIT 1 BY") != 1 || strings.Count(source, " FINAL") != 1 {
				t.Errorf("%s %s must read FINAL once and pick in one LIMIT 1 BY:\n%s", table, name, source)
			}
		}
		if live := "AS " + teamSourceAlias + "\n    WHERE "; !strings.Contains(ServedTeamSource(table, where), live) || strings.Contains(ServedTeamPick(table, where), live) {
			t.Errorf("%s: the live-row rule must follow the pick of ServedTeamSource only", table)
		}
	}
}

func TestServedTeamViewsRefuseATableWithoutTeamRowsAndAnEmptyFilter(t *testing.T) {
	for _, call := range []struct {
		name string
		call func()
	}{
		{"source of the person rows", func() { _ = ServedTeamSource("work_item_user_metrics_daily", "1") }},
		{"pick of the person rows", func() { _ = ServedTeamPick("work_item_user_metrics_daily", "1") }},
		{"source with an empty filter", func() { _ = ServedTeamSource("work_item_metrics_daily", " ") }},
		{"pick with an empty filter", func() { _ = ServedTeamPick("work_item_metrics_daily", "") }},
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s did not panic", call.name)
				}
			}()
			call.call()
		}()
	}
}
