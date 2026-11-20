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
// the live-row rule after the pick; ServedTeamPick does not. This is the order "" form.
func TestServedTeamViewsScopeTheirOwnReadByOrg(t *testing.T) {
	const where = "team_id = {team_id:String}"
	for _, table := range []string{"work_item_metrics_daily", "work_item_state_durations_daily"} {
		for name, source := range map[string]string{"source": ServedTeamSource(table, where, ""), "pick": ServedTeamPick(table, where, "")} {
			scoped := "FROM " + table + " FINAL WHERE " + table + ".org_id = {org_id:String} AND (" + where + ")"
			if !strings.Contains(source, scoped) {
				t.Errorf("%s %s lacks %q:\n%s", table, name, scoped, source)
			}
			if strings.Count(source, "LIMIT 1 BY") != 1 || strings.Count(source, " FINAL") != 1 {
				t.Errorf("%s %s must read FINAL once and pick in one LIMIT 1 BY:\n%s", table, name, source)
			}
		}
		if live := "AS " + teamSourceAlias + "\n    WHERE "; !strings.Contains(ServedTeamSource(table, where, ""), live) || strings.Contains(ServedTeamPick(table, where, ""), live) {
			t.Errorf("%s: the live-row rule must follow the pick of ServedTeamSource only", table)
		}
	}
}

// Order "" is the SQL text of the served team view before CHAOS-14405, byte for byte: the FINAL read under TeamPick.
func TestServedTeamViewsWithoutAnOrderKeepTheFinalRead(t *testing.T) {
	const where = "team_id = {team_id:String}"
	for _, table := range []string{"work_item_metrics_daily", "work_item_state_durations_daily"} {
		declared := mustTeamTable(table)
		final := "(SELECT * FROM " + table + " FINAL WHERE " + table + ".org_id = {org_id:String} AND (" + where + ")) AS " + table
		pick := TeamPick(table, final)
		source := "(\n    SELECT * FROM " + pick + " AS " + teamSourceAlias + "\n    WHERE " + declared.LiveRow(teamSourceAlias+".") + "\n)"
		if got := ServedTeamPick(table, where, ""); got != pick {
			t.Errorf("%s pick:\n got  %s\n want %s", table, got, pick)
		}
		if got := ServedTeamSource(table, where, ""); got != source {
			t.Errorf("%s source:\n got  %s\n want %s", table, got, source)
		}
	}
}

// With an order the newest row of each full key, row_view included, is the first by that order, read without FINAL
// and scoped by org_id beside the reader's filter (CHAOS-14405). The view pick on top is the same TeamPick.
func TestServedTeamViewsWithAnOrderPickEachFullKeyByIt(t *testing.T) {
	const where = "team_id = {team_id:String}"
	const order = "(work_item_metrics_daily.computed_at, cityHash64(tuple(work_item_metrics_daily.items_completed)))"
	const table = "work_item_metrics_daily"
	newest := "(SELECT * FROM " + table + " WHERE " + table + ".org_id = {org_id:String} AND (" + where + ") ORDER BY " + order +
		" DESC LIMIT 1 BY " + table + ".org_id, " + table + ".day, " + table + ".provider, " + table + ".work_scope_id, " +
		table + ".team_id, " + table + ".row_view) AS " + table
	if got, want := ServedTeamPick(table, where, order), TeamPick(table, newest); got != want {
		t.Errorf("pick:\n got  %s\n want %s", got, want)
	}
	source := ServedTeamSource(table, where, order)
	if !strings.Contains(source, TeamPick(table, newest)) || strings.Contains(source, " FINAL") {
		t.Errorf("source must pick from the ordered newest rows, without FINAL:\n%s", source)
	}
}

func TestServedTeamViewsRefuseATableWithoutTeamRowsAndAnEmptyFilter(t *testing.T) {
	for _, call := range []struct {
		name string
		call func()
	}{
		{"source of the person rows", func() { _ = ServedTeamSource("work_item_user_metrics_daily", "1", "") }},
		{"pick of the person rows", func() { _ = ServedTeamPick("work_item_user_metrics_daily", "1", "") }},
		{"source with an empty filter", func() { _ = ServedTeamSource("work_item_metrics_daily", " ", "") }},
		{"ordered pick with an empty filter", func() { _ = ServedTeamPick("work_item_metrics_daily", "", "work_item_metrics_daily.computed_at") }},
		{"pick with an empty filter", func() { _ = ServedTeamPick("work_item_metrics_daily", "", "") }},
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
