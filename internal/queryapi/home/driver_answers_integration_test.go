//go:build integration

package home

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// The Home driver of a no-team or team-set request, and of a repository metric in any form, on a real ClickHouse
// (CHAOS-14441, CHAOS-14425): the driver takes the value's own reducer and status filter, and a move from a measured 0
// ranks its drivers by the absolute rise. Each case WANTs the summary driver and the caption; the answer of main
// b737db18f3b2 is in the "main" field of the case (logs l14441-answers-main-1.log 152839e1a104 and, for the
// absent-prior cases, l14441-answers-main-2.log 36346fc641f0). zp-null-cycle runs only with the tuple read of ticket
// 14424 (PR 6772).
//
// Teams Ops and Web. Repositories acme/a and acme/b, both owned by Ops (share 1), so one team [Ops] and the team set
// [Ops, Web] scope the same two repositories. avg and sum differ by design (row counts differ between windows):
//
//	pct-throughput  Ops prior 4 rows x 1 (sum 4, avg 1), current 1 row x 6 (sum 6 +50 %, avg 6 +500 %)
//	                Web prior 1 row x 2, current 1 row x 4 (+100 % both)          sum order Web, Ops; avg order Ops, Web
//	pct-blocked     Ops blocked 4 -> 8 (+100 %); Web blocked 4 -> 6 (+50 %), Web in_progress 1 -> 100
//	                blocked-only sum order Ops, Web; all-status avg order Web (2.5 -> 53), Ops
//	pct-churn       a prior 4 days x 1, current 1 day x 6; b prior 1 day x 2, current 1 day x 4
//	pct-deploy      the same counts as pct-churn          sum order b, a; avg order a, b
//	zp-churn        a 0 -> 5, b 0 -> 2 (absolute order a, b)
//	zp-deploy       a 0 -> 5, b 0 -> 2
//	zp-review       pr_first_review_p50_hours a 0 -> 10, b 0 -> 4 (avg; absolute order a, b)
//	zp-ci           a success 0 / failure 1 -> 1 / 1 (0 -> 0.5); b 0 / 2 -> 3 / 1 (0 -> 0.75): order b, a
//	zp-absent-throughput  Ops measured 0 -> 1; Web no prior row -> 100: a sum counts the absent prior as 0: Web, Ops
//	zp-absent-cycle       Ops measured 0 -> 24 h; Web no prior row -> 48 h: an average needs a measured 0: Ops
//	zp-absent-churn       a measured 0 -> 2; b no prior row -> 5: b, a
//	zp-null-cycle         Ops prior key: an older 0, then a newer NULL; Web measured 0; current Ops 48 h, Web 24 h:
//	                      the newest prior row of Ops is NULL, not a measured 0 (CHAOS-14424's tuple read): Web
const (
	ansOps = "github:acme/ops"
	ansWeb = "github:acme/web"
)

type ansExec interface {
	Exec(context.Context, string, ...any) error
}

func ansTeams(ctx context.Context, t *testing.T, admin ansExec, org string, at time.Time) {
	t.Helper()
	for team, name := range map[string]string{ansOps: "Ops", ansWeb: "Web"} {
		if err := admin.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, repo_patterns, updated_at, org_id, provider, is_active)
VALUES (?, generateUUIDv4(), ?, [], ?, ?, 'github', 1)`, team, name, at, org); err != nil {
			t.Fatal(err)
		}
	}
}

func ansWorkItemRow(ctx context.Context, t *testing.T, admin ansExec, org string, day time.Time, scope, team string, items uint32, at time.Time) {
	t.Helper()
	if err := admin.Exec(ctx, `INSERT INTO work_item_metrics_daily
(org_id, day, provider, work_scope_id, team_id, team_name, items_started, items_completed,
 items_started_unassigned, items_completed_unassigned, wip_count_end_of_day, wip_unassigned_end_of_day,
 cycle_time_p50_hours, cycle_time_p90_hours, lead_time_p50_hours, lead_time_p90_hours,
 wip_age_p50_hours, wip_age_p90_hours, bug_completed_ratio, story_points_completed,
 new_bugs_count, new_items_count, defect_intro_rate, wip_congestion_ratio, predictability_score, computed_at)
VALUES (?, ?, 'github', ?, ?, '', 1, ?, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, ?)`,
		org, day, scope, team, items, at); err != nil {
		t.Fatal(err)
	}
}

// ansCycleRow is a work_item_metrics_daily row with a cycle time (nil = NULL), live by items_started.
func ansCycleRow(ctx context.Context, t *testing.T, admin ansExec, org string, day time.Time, team string, cycle any, at time.Time) {
	t.Helper()
	if err := admin.Exec(ctx, `INSERT INTO work_item_metrics_daily
(org_id, day, provider, work_scope_id, team_id, team_name, items_started, items_completed,
 items_started_unassigned, items_completed_unassigned, wip_count_end_of_day, wip_unassigned_end_of_day,
 cycle_time_p50_hours, cycle_time_p90_hours, lead_time_p50_hours, lead_time_p90_hours,
 wip_age_p50_hours, wip_age_p90_hours, bug_completed_ratio, story_points_completed,
 new_bugs_count, new_items_count, defect_intro_rate, wip_congestion_ratio, predictability_score, computed_at)
VALUES (?, ?, 'github', 'scope-0', ?, '', 1, 0, 0, 0, 1, 0, ?, ?, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, ?)`,
		org, day, team, cycle, cycle, at); err != nil {
		t.Fatal(err)
	}
}

func ansStateRow(ctx context.Context, t *testing.T, admin ansExec, org string, day time.Time, team, status string, hours float64, at time.Time) {
	t.Helper()
	if err := admin.Exec(ctx, `INSERT INTO work_item_state_durations_daily
(day, provider, work_scope_id, team_id, team_name, status, duration_hours, items_touched, computed_at, org_id)
VALUES (?, 'github', 'scope-0', ?, '', ?, ?, 1, ?, ?)`, day, team, status, hours, at, org); err != nil {
		t.Fatal(err)
	}
}

// ansRepos seeds acme/a and acme/b, owned by Ops, and returns their ids.
func ansRepos(ctx context.Context, t *testing.T, admin ansExec, org string, at time.Time) (uuid.UUID, uuid.UUID) {
	t.Helper()
	ids := []uuid.UUID{uuid.New(), uuid.New()}
	ownedFrom := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i, name := range []string{"acme/a", "acme/b"} {
		if err := admin.Exec(ctx, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, 'github', ?, ?, ?)`,
			ids[i], name, org, at, at); err != nil {
			t.Fatal(err)
		}
		if err := admin.Exec(ctx, `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at, share)
VALUES (?, 'github', ?, ?, ?, 'exact', 'inferred', 1, 0, 0, ?, NULL, ?, 1.0)`, org, ansOps, ids[i], name, ownedFrom, ownedFrom); err != nil {
			t.Fatal(err)
		}
	}
	return ids[0], ids[1]
}

func TestHomeDriverAnswersOnClickHouse(t *testing.T) {
	ctx := context.Background()
	admin, client := newHomeTestClickHouse(ctx, t)
	now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	startDay, _, compareStart, _, err := TimeWindow(DefaultFilters(), now)
	if err != nil {
		t.Fatal(err)
	}
	at := startDay.Add(24 * time.Hour)
	priorDays := []time.Time{compareStart, compareStart.AddDate(0, 0, 1), compareStart.AddDate(0, 0, 2), compareStart.AddDate(0, 0, 3)}

	type form struct {
		name string
		ids  []string
	}
	teamForms := []form{{"no-team", nil}, {"team-set", []string{ansOps, ansWeb}}}
	repoForms := []form{{"no-team", nil}, {"one-team", []string{ansOps}}, {"team-set", []string{ansOps, ansWeb}}}

	repoCounts := func(org, table, column string, a, b uuid.UUID, zero bool) {
		insert := func(repo uuid.UUID, day time.Time, v float64) {
			var q string
			switch table {
			case "repo_metrics_daily":
				q = fmt.Sprintf(`INSERT INTO repo_metrics_daily (org_id, repo_id, day, %s, computed_at) VALUES (?, ?, ?, ?, ?)`, column)
				if err := admin.Exec(ctx, q, org, repo, day, v, at); err != nil {
					t.Fatal(err)
				}
			case "deploy_metrics_daily":
				if err := admin.Exec(ctx, `INSERT INTO deploy_metrics_daily (repo_id, day, deployments_count, failed_deployments_count, computed_at, org_id) VALUES (?, ?, ?, 0, ?, ?)`,
					repo, day, uint32(v), at, org); err != nil {
					t.Fatal(err)
				}
			}
		}
		if zero {
			insert(a, compareStart, 0)
			insert(b, compareStart, 0)
			va, vb := 5.0, 2.0
			if column == "pr_first_review_p50_hours" {
				va, vb = 10, 4
			}
			insert(a, startDay, va)
			insert(b, startDay, vb)
			return
		}
		for _, d := range priorDays {
			insert(a, d, 1)
		}
		insert(a, startDay, 6)
		insert(b, compareStart, 2)
		insert(b, startDay, 4)
	}

	// want is the summary's driver list; the caption names the same drivers. main is main's served answer.
	cases := []struct {
		name, metric string
		forms        []form
		want         []string
		main         string
		seed         func(org string)
	}{
		{"pct-throughput", "throughput", teamForms, []string{"Web", "Ops"}, "Ops, Web / Ops and Web", func(org string) {
			for i, d := range priorDays {
				ansWorkItemRow(ctx, t, admin, org, d, fmt.Sprintf("scope-%d", i), ansOps, 1, at)
			}
			ansWorkItemRow(ctx, t, admin, org, startDay, "scope-0", ansOps, 6, at)
			ansWorkItemRow(ctx, t, admin, org, compareStart, "scope-0", ansWeb, 2, at)
			ansWorkItemRow(ctx, t, admin, org, startDay, "scope-0", ansWeb, 4, at)
		}},
		{"pct-blocked", "blocked_work", teamForms, []string{"Ops", "Web"}, "Web, Ops / Web and Ops", func(org string) {
			ansStateRow(ctx, t, admin, org, compareStart, ansOps, "blocked", 4, at)
			ansStateRow(ctx, t, admin, org, startDay, ansOps, "blocked", 8, at)
			ansStateRow(ctx, t, admin, org, compareStart, ansWeb, "blocked", 4, at)
			ansStateRow(ctx, t, admin, org, startDay, ansWeb, "blocked", 6, at)
			ansStateRow(ctx, t, admin, org, compareStart, ansWeb, "in_progress", 1, at)
			ansStateRow(ctx, t, admin, org, startDay, ansWeb, "in_progress", 100, at)
		}},
		{"pct-churn", "churn", repoForms, []string{"acme/b", "acme/a"}, "acme/a, acme/b / acme/a and acme/b", func(org string) {
			a, b := ansRepos(ctx, t, admin, org, at)
			repoCounts(org, "repo_metrics_daily", "total_loc_touched", a, b, false)
		}},
		{"pct-deploy", "deploy_freq", repoForms, []string{"acme/b", "acme/a"}, "acme/a, acme/b / acme/a and acme/b", func(org string) {
			a, b := ansRepos(ctx, t, admin, org, at)
			repoCounts(org, "deploy_metrics_daily", "deployments_count", a, b, false)
		}},
		{"zp-churn", "churn", repoForms, []string{"acme/a", "acme/b"}, "none / none", func(org string) {
			a, b := ansRepos(ctx, t, admin, org, at)
			repoCounts(org, "repo_metrics_daily", "total_loc_touched", a, b, true)
		}},
		{"zp-deploy", "deploy_freq", repoForms, []string{"acme/a", "acme/b"}, "none / none", func(org string) {
			a, b := ansRepos(ctx, t, admin, org, at)
			repoCounts(org, "deploy_metrics_daily", "deployments_count", a, b, true)
		}},
		{"zp-review", "review_latency", repoForms, []string{"acme/a", "acme/b"}, "none / none", func(org string) {
			a, b := ansRepos(ctx, t, admin, org, at)
			repoCounts(org, "repo_metrics_daily", "pr_first_review_p50_hours", a, b, true)
		}},
		{"zp-ci", "ci_success", repoForms, []string{"acme/b", "acme/a"}, "none / none", func(org string) {
			a, b := ansRepos(ctx, t, admin, org, at)
			for _, r := range []struct {
				repo           uuid.UUID
				day            time.Time
				success, fails uint32
			}{{a, compareStart, 0, 1}, {a, startDay, 1, 1}, {b, compareStart, 0, 2}, {b, startDay, 3, 1}} {
				if err := admin.Exec(ctx, `INSERT INTO testops_pipeline_metrics_daily (repo_id, day, pipelines_count, success_count, failure_count, cancelled_count, success_rate, failure_rate, cancel_rate, rerun_rate, computed_at, org_id) VALUES (?, ?, ?, ?, ?, 0, 0, 0, 0, 0, ?, ?)`,
					r.repo, r.day, r.success+r.fails, r.success, r.fails, at, org); err != nil {
					t.Fatal(err)
				}
			}
		}},
		{"zp-absent-throughput", "throughput", teamForms, []string{"Web", "Ops"}, "none / none", func(org string) {
			ansWorkItemRow(ctx, t, admin, org, compareStart, "scope-0", ansOps, 0, at)
			ansWorkItemRow(ctx, t, admin, org, startDay, "scope-0", ansOps, 1, at)
			ansWorkItemRow(ctx, t, admin, org, startDay, "scope-0", ansWeb, 100, at)
		}},
		{"zp-absent-cycle", "cycle_time", teamForms, []string{"Ops"}, "none / none", func(org string) {
			ansCycleRow(ctx, t, admin, org, compareStart, ansOps, 0.0, at)
			ansCycleRow(ctx, t, admin, org, startDay, ansOps, 24.0, at)
			ansCycleRow(ctx, t, admin, org, startDay, ansWeb, 48.0, at)
		}},
		{"zp-absent-churn", "churn", repoForms, []string{"acme/b", "acme/a"}, "none / none", func(org string) {
			a, b := ansRepos(ctx, t, admin, org, at)
			for _, r := range []struct {
				repo uuid.UUID
				day  time.Time
				v    float64
			}{{a, compareStart, 0}, {a, startDay, 2}, {b, startDay, 5}} {
				if err := admin.Exec(ctx, `INSERT INTO repo_metrics_daily (org_id, repo_id, day, total_loc_touched, computed_at) VALUES (?, ?, ?, ?, ?)`,
					org, r.repo, r.day, r.v, at); err != nil {
					t.Fatal(err)
				}
			}
		}},
		{"zp-null-cycle", "cycle_time", teamForms, []string{"Web"}, "not run (needs PR 6772)", func(org string) {
			ansCycleRow(ctx, t, admin, org, compareStart, ansOps, 0.0, at.Add(-time.Hour))
			ansCycleRow(ctx, t, admin, org, compareStart, ansOps, nil, at)
			ansCycleRow(ctx, t, admin, org, compareStart, ansWeb, 0.0, at)
			ansCycleRow(ctx, t, admin, org, startDay, ansOps, 48.0, at)
			ansCycleRow(ctx, t, admin, org, startDay, ansWeb, 24.0, at)
		}},
	}
	// The newer-NULL case needs the tuple read of a Nullable value column (ticket 14424, PR 6772): without it the value
	// read itself skips the NULL and takes the older 0.
	tupleRead := strings.Contains(metricFromClause("work_item_metrics_daily", "cycle_time_p50_hours", "", "a", "b", false), "argMax(tuple(cycle_time_p50_hours)")
	for _, c := range cases {
		if c.name == "zp-null-cycle" && !tupleRead {
			t.Logf("SKIP %s: the value read takes argMax(cycle_time_p50_hours) (ticket 14424 not merged)", c.name)
			continue
		}
		org := "answers-14441-" + c.name
		ansTeams(ctx, t, admin, org, at)
		c.seed(org)
		for _, fm := range c.forms {
			f := DefaultFilters()
			if fm.ids != nil {
				f.Scope = ScopeFilter{Level: "team", IDs: fm.ids}
			}
			resp, err := BuildResponse(ctx, client, nil, org, f, now)
			if err != nil {
				t.Fatal(err)
			}
			d := driverDoubleDelta(resp, c.metric)
			move := "none"
			if pct, ok := d.Percent(); ok {
				move = fmt.Sprintf("pct=%.4g", pct)
			} else if d.FromZero() {
				move = fmt.Sprintf("from-zero value=%g", d.Value)
			}
			summary, caption := "", ""
			if len(resp.Summary) > 0 {
				summary = resp.Summary[0].Text
			}
			for _, s := range resp.Signals {
				if s.ID == "metric:"+c.metric && s.Driver != nil {
					caption = *s.Driver
				}
			}
			t.Logf("ANSWER case=%s metric=%s form=%s move=%s summary=%q caption=%q main=%q", c.name, c.metric, fm.name, move, summary, caption, c.main)
			wantFromZero := strings.HasPrefix(c.name, "zp-")
			if move == "none" || d.FromZero() != wantFromZero {
				t.Errorf("SEED case=%s form=%s: move %s, want from-zero %t: %+v", c.name, fm.name, move, wantFromZero, d)
				continue
			}
			if want := " driven by " + strings.Join(c.want, ", ") + "."; !strings.HasSuffix(summary, want) {
				t.Errorf("DRIVER case=%s form=%s: summary %q, want suffix %q (main: %s)", c.name, fm.name, summary, want, c.main)
			}
			list := c.want[0]
			if len(c.want) > 1 {
				list = strings.Join(c.want[:len(c.want)-1], ", ") + " and " + c.want[len(c.want)-1]
			}
			if want := d.Label + " appears to have moved most in " + list + "."; caption != want {
				t.Errorf("DRIVER case=%s form=%s: caption %q, want %q (main: %s)", c.name, fm.name, caption, want, c.main)
			}
		}
	}
}
