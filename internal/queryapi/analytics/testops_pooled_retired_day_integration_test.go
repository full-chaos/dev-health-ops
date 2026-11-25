//go:build integration

package analytics

import (
	"fmt"
	"strings"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/testopspooled"
)

// CHAOS-14595: a retired repository-day (its newest testops_test_metrics_daily row is a stale-key retraction: retracted 1,
// pooled_samples_stored left at its default 0, the columns of stale_team_keys.go writeStaleKeyZeroRows) is dropped from
// the output by LiveRepoDays, so it must not pull its repository's whole suite history into the raw arm either.
//
// Repository A holds a legacy day (newest row without stored samples, not retracted): the raw arm reads A. Repository B
// holds a long history outside the window, a stored day1 and a day2 that is retired. The pooled read must read the same
// rows whether B's retired day is there or not (within 1,000 rows), and every served TEST_SUITE_DURATION_P95 value (TEAM,
// REPO and ORG series) is the pool of the live days only.

const (
	retiredDayOrg   = "f0f0f0f0-0000-4000-8000-000000014595"
	retiredDayRepoA = "f0f0f0f0-0000-4000-8000-00000014595a"
	retiredDayRepoB = "f0f0f0f0-0000-4000-8000-00000014595b"
)

func TestTestopsPooledSuiteRetiredDayLeavesTheLegacyRawRead(t *testing.T) {
	e := startOwnersEnv(t)
	var version string
	if err := e.admin.QueryRow(e.ctx, `SELECT version()`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	for _, repo := range []string{retiredDayRepoA, retiredDayRepoB} {
		name := "acme/" + repo[len(repo)-1:]
		e.repo(retiredDayOrg, repo, name)
		e.owner(retiredDayOrg, "team-a", repo, name, 1, openFrom(), nil)
	}
	const historyB = 200_000
	e.exec(fmt.Sprintf(`INSERT INTO test_suite_results (repo_id, run_id, suite_id, suite_name, total_count, passed_count, failed_count,
  skipped_count, duration_seconds, started_at, finished_at, org_id, last_synced, report_slot, run_attempt)
SELECT toUUID('%s'), concat('h-', toString(intDiv(number, 10))), concat('hs-', toString(number)), 'pkg', 1, 1, 0, 0,
  toFloat64(number %% 600), toDateTime64('2025-01-01 00:00:00', 3, 'UTC') + intDiv(number, 3), NULL, '%s',
  toDateTime64('2025-12-31 00:00:00', 3, 'UTC'), '', 1
FROM numbers(%d)`, retiredDayRepoB, retiredDayOrg, historyB))
	e.durations(retiredDayOrg, retiredDayRepoA, day1, 10, 20, 30)
	e.durations(retiredDayOrg, retiredDayRepoB, day1, 40, 50)
	e.durations(retiredDayOrg, retiredDayRepoB, day2, 600)
	e.write(retiredDayOrg, day1, retiredDayRepoA, retiredDayRepoB)
	e.write(retiredDayOrg, day2, retiredDayRepoB)
	// A's day1: a newer row without stored samples (written before the writer stored them), not retracted.
	e.exec(`INSERT INTO testops_test_metrics_daily (repo_id, day, org_id, computed_at) VALUES (?, ?, ?, now() + INTERVAL 1 HOUR)`,
		retiredDayRepoA, day1, retiredDayOrg)

	read := func(phase, source string) uint64 {
		t.Helper()
		id := "c14595-" + phase + "-" + uuid.NewString()[:8]
		ctx := stdclickhouse.Context(e.ctx, stdclickhouse.WithQueryID(id))
		var p95 *float64
		var samples uint64
		if err := e.admin.QueryRow(ctx, "SELECT "+testopspooled.P95+", count() FROM "+source+" SETTINGS max_rows_to_read = 0",
			stdclickhouse.Named("org_id", retiredDayOrg), stdclickhouse.Named("start_date", "2026-01-01"), stdclickhouse.Named("end_date", "2026-01-07")).
			Scan(&p95, &samples); err != nil {
			t.Fatalf("%s: %v", phase, err)
		}
		e.exec(`SYSTEM FLUSH LOGS`)
		var rows uint64
		if err := e.admin.QueryRow(e.ctx, `SELECT read_rows FROM system.query_log WHERE query_id = ? AND type = 'QueryFinish'`, id).Scan(&rows); err != nil {
			t.Fatalf("%s query_log: %v", phase, err)
		}
		t.Logf("CELL14595 %s %s: read_rows %d, p95 %v over %d samples (B history %d rows)", version, phase, rows, str(p95), samples, historyB)
		return rows
	}

	expected := func(values ...float64) *float64 {
		t.Helper()
		var p95 float64
		if err := e.admin.QueryRow(e.ctx, `SELECT quantileExactInclusive(0.95)(x) FROM (SELECT arrayJoin(?) AS x)`, values).Scan(&p95); err != nil {
			t.Fatal(err)
		}
		return &p95
	}
	byDay := func(results []model.TimeseriesResult, wantScope model.TimeseriesScope, dimension string) map[string]*float64 {
		out := map[string]*float64{}
		for _, series := range results {
			if series.Scope != wantScope || (dimension != "" && series.DimensionValue != dimension) {
				continue
			}
			for _, bucket := range series.Buckets {
				out[bucket.Date.String()] = bucket.Value
			}
		}
		return out
	}
	day1Text, day2Text := day1.Format(time.DateOnly), day2.Format(time.DateOnly)
	served := func(phase string) {
		t.Helper()
		teams, err := resolveOneTimeseries(e.ctx, e.client, retiredDayOrg, tsInput(model.DimensionInputTeam, model.MeasureInputTestSuiteDurationP95), false, nil, true)
		if err != nil {
			t.Fatalf("%s team series: %v", phase, err)
		}
		scoped := &model.FilterInput{Scope: &model.ScopeFilterInput{Level: model.ScopeLevelInputTeam, Ids: []string{"team-a"}}}
		repos, err := resolveOneTimeseries(e.ctx, e.client, retiredDayOrg, tsInput(model.DimensionInputRepo, model.MeasureInputTestSuiteDurationP95), false, scoped, false)
		if err != nil {
			t.Fatalf("%s repo series: %v", phase, err)
		}
		for _, check := range []struct {
			series string
			values map[string]*float64
			day1   *float64
		}{
			{"ORG", byDay(teams, model.TimeseriesScopeOrg, ""), expected(10, 20, 30, 40, 50)},
			{"TEAM team-a", byDay(teams, model.TimeseriesScopeDimension, "team-a"), expected(10, 20, 30, 40, 50)},
			{"REPO A", byDay(repos, model.TimeseriesScopeDimension, retiredDayRepoA), expected(10, 20, 30)},
			{"REPO B", byDay(repos, model.TimeseriesScopeDimension, retiredDayRepoB), expected(40, 50)},
		} {
			t.Logf("CELL14595 %s %s: day1 %s, day2 %s", phase, check.series, str(check.values[day1Text]), str(check.values[day2Text]))
			if !same(check.values[day1Text], check.day1) {
				t.Errorf("%s %s day1 = %s, want %s", phase, check.series, str(check.values[day1Text]), str(check.day1))
			}
			if check.series != "REPO A" && phase == "retired-present" && check.values[day2Text] != nil {
				t.Errorf("%s %s day2 = %s, want no value: B's day2 is retired", phase, check.series, str(check.values[day2Text]))
			}
		}
	}

	absent := read("retired-absent", testopspooled.SuiteRunsSource)
	// B's day2 retired: the stale-key retraction row (writeStaleKeyZeroRows columns), newer than the stored row.
	e.exec(`INSERT INTO testops_test_metrics_daily (org_id, day, repo_id, computed_at, retracted, team_id, service_id)
VALUES (?, ?, ?, now() + INTERVAL 2 HOUR, 1, NULL, NULL)`, retiredDayOrg, day2, retiredDayRepoB)
	present := read("retired-present", testopspooled.SuiteRunsSource)
	served("retired-present")
	if present > absent+1_000 {
		t.Errorf("%s: with B's retired day the pooled suite read reads %d rows, without it %d; want at most %d more than %d (the retired day must not pull B's %d history rows into the raw arm)",
			version, present, absent, 1_000, absent, historyB)
	}
	// The mutant: the raw arm's legacy set without the retracted term (LegacyRepoDays) reads B's history again.
	live, legacy := testopspooled.LiveLegacyRepoDays("testops_test_metrics_daily"), testopspooled.LegacyRepoDays("testops_test_metrics_daily")
	mutant := strings.ReplaceAll(testopspooled.SuiteRunsSource, live, legacy)
	if mutant == testopspooled.SuiteRunsSource || strings.Count(testopspooled.SuiteRunsSource, live) != 2 {
		t.Fatalf("the mutant changed nothing: SuiteRunsSource holds the live legacy set %d times, want 2", strings.Count(testopspooled.SuiteRunsSource, live))
	}
	if rows := read("retired-present-mutant-without-the-retracted-term", mutant); rows <= absent+1_000 {
		t.Errorf("MUTANT14595 legacy-set-retracted-term red=false: the mutant reads %d rows, want more than %d", rows, absent+1_000)
	} else {
		t.Logf("MUTANT14595 legacy-set-retracted-term red=true: read_rows %d -> %d", present, rows)
	}
}

// CHAOS-14595 r1: a retraction row (retracted 1, marker at its default 0) and a live legacy row (marker 0, retracted 0) of ONE
// repository-day at the SAME computed_at, in both insert orders, each row in its own part (merges stopped). The raw arm's
// legacy set (LiveLegacyRepoDays) and the live gate (LiveRepoDays) must pick the day the same way, and the served value must
// equal the value the source gives with the legacy set of the base (LegacyRepoDays).
func TestTestopsPooledSuiteRetiredDayTiedWithALiveLegacyRow(t *testing.T) {
	e := startOwnersEnv(t)
	e.exec(`SYSTEM STOP MERGES testops_test_metrics_daily`)
	live, legacy := testopspooled.LiveLegacyRepoDays("testops_test_metrics_daily"), testopspooled.LegacyRepoDays("testops_test_metrics_daily")
	baseSource := strings.ReplaceAll(testopspooled.SuiteRunsSource, live, legacy)
	if baseSource == testopspooled.SuiteRunsSource {
		t.Fatal("the base source equals the head source: the legacy set was not replaced")
	}
	const tieAt = "2026-01-08 00:00:00"
	liveRow := func(org, repo string) {
		e.exec(`INSERT INTO testops_test_metrics_daily (repo_id, day, org_id, computed_at) VALUES (?, ?, ?, toDateTime(?, 'UTC'))`, repo, day1, org, tieAt)
	}
	retraction := func(org, repo string) {
		e.exec(`INSERT INTO testops_test_metrics_daily (org_id, day, repo_id, computed_at, retracted, team_id, service_id)
VALUES (?, ?, ?, toDateTime(?, 'UTC'), 1, NULL, NULL)`, org, day1, repo, tieAt)
	}
	for _, order := range []struct {
		name   string
		org    string
		repo   string
		insert func(org, repo string)
	}{
		{"live row then retraction", "f0f0f0f0-0000-4000-8000-0000001459f1", "f0f0f0f0-0000-4000-8000-0000001459c1", func(org, repo string) { liveRow(org, repo); retraction(org, repo) }},
		{"retraction then live row", "f0f0f0f0-0000-4000-8000-0000001459f2", "f0f0f0f0-0000-4000-8000-0000001459c2", func(org, repo string) { retraction(org, repo); liveRow(org, repo) }},
	} {
		t.Run(order.name, func(t *testing.T) {
			e.repo(order.org, order.repo, "acme/tie")
			e.owner(order.org, "team-t", order.repo, "acme/tie", 1, openFrom(), nil)
			e.durations(order.org, order.repo, day1, 10, 20, 30)
			order.insert(order.org, order.repo)
			params := []any{stdclickhouse.Named("org_id", order.org), stdclickhouse.Named("start_date", "2026-01-01"), stdclickhouse.Named("end_date", "2026-01-07")}
			var rows uint64
			if err := e.admin.QueryRow(e.ctx, `SELECT count() FROM testops_test_metrics_daily WHERE org_id = ? AND repo_id = ? AND day = ? AND computed_at = toDateTime(?, 'UTC')`,
				order.org, order.repo, day1, tieAt).Scan(&rows); err != nil {
				t.Fatal(err)
			}
			if rows != 2 {
				t.Fatalf("control: %d stored rows of the day at the tie, want 2 (the retraction and the live row)", rows)
			}
			pick := func(query string) uint64 {
				t.Helper()
				var n uint64
				if err := e.admin.QueryRow(e.ctx, query, params...).Scan(&n); err != nil {
					t.Fatalf("%v\n%s", err, query)
				}
				return n
			}
			for read := 0; read < 5; read++ {
				inLegacySet := pick(`SELECT count() FROM (` + live + `)`)
				inLiveGate := pick(`SELECT count() FROM (SELECT DISTINCT repo_id, day FROM testops_test_metrics_daily
				WHERE org_id = {org_id:String} AND day >= {start_date:Date} AND day < {end_date:Date}) WHERE ` + testopspooled.LiveRepoDays("testops_test_metrics_daily"))
				var head, base *float64
				if err := e.admin.QueryRow(e.ctx, "SELECT "+testopspooled.P95+" FROM "+testopspooled.SuiteRunsSource, params...).Scan(&head); err != nil {
					t.Fatal(err)
				}
				if err := e.admin.QueryRow(e.ctx, "SELECT "+testopspooled.P95+" FROM "+baseSource, params...).Scan(&base); err != nil {
					t.Fatal(err)
				}
				t.Logf("CELL14595 tie %s read %d: LiveLegacyRepoDays %d, LiveRepoDays %d; served p95 head %s, base %s", order.name, read, inLegacySet, inLiveGate, str(head), str(base))
				if inLegacySet != inLiveGate {
					t.Errorf("%s read %d: LiveLegacyRepoDays picks the day %d time(s), LiveRepoDays %d: the two picks differ on the tie", order.name, read, inLegacySet, inLiveGate)
				}
				if !same(head, base) {
					t.Errorf("%s read %d: served p95 %s, the base legacy set serves %s", order.name, read, str(head), str(base))
				}
			}
		})
	}
}

// tieArm is one live row type tied with a retraction row of the same repository-day at one computed_at.
type tieArm struct {
	name    string
	liveRow string // INSERT of the live row; args: repo, day, org, computed_at
}

// checkTieArmAgainstTheLiveGate pins the ruling for a same-second tie of a live row and a retraction row: the served value
// follows the live gate's pick (argMax(retracted, computed_at)). A day the gate serves is read (raw arm or stored samples),
// a day it retires is not read, and the served P95 and median equal the base legacy set's.
func checkTieArmAgainstTheLiveGate(t *testing.T, arm tieArm, orgs, repos [2]string) {
	e := startOwnersEnv(t)
	e.exec(`SYSTEM STOP MERGES testops_test_metrics_daily`)
	live, legacy := testopspooled.LiveLegacyRepoDays("testops_test_metrics_daily"), testopspooled.LegacyRepoDays("testops_test_metrics_daily")
	baseSource := strings.ReplaceAll(testopspooled.SuiteRunsSource, live, legacy)
	if baseSource == testopspooled.SuiteRunsSource {
		t.Fatal("the base source equals the head source: the legacy set was not replaced")
	}
	const tieAt = "2026-01-08 00:00:00"
	liveRow := func(org, repo string) { e.exec(arm.liveRow, repo, day1, org, tieAt) }
	retraction := func(org, repo string) {
		e.exec(`INSERT INTO testops_test_metrics_daily (org_id, day, repo_id, computed_at, retracted, team_id, service_id)
VALUES (?, ?, ?, toDateTime(?, 'UTC'), 1, NULL, NULL)`, org, day1, repo, tieAt)
	}
	var wantP95, wantP50 float64
	if err := e.admin.QueryRow(e.ctx, `SELECT quantileExactInclusive(0.95)(x), quantileExactInclusive(0.5)(x) FROM (SELECT arrayJoin([10., 20., 30.]) AS x)`).
		Scan(&wantP95, &wantP50); err != nil {
		t.Fatal(err)
	}
	for index, order := range []struct {
		name   string
		insert func(org, repo string)
	}{
		{"live row then retraction", func(org, repo string) { liveRow(org, repo); retraction(org, repo) }},
		{"retraction then live row", func(org, repo string) { retraction(org, repo); liveRow(org, repo) }},
	} {
		t.Run(order.name, func(t *testing.T) {
			org, repo := orgs[index], repos[index]
			e.repo(org, repo, "acme/tie")
			e.owner(org, "team-t", repo, "acme/tie", 1, openFrom(), nil)
			e.durations(org, repo, day1, 10, 20, 30)
			order.insert(org, repo)
			params := []any{stdclickhouse.Named("org_id", org), stdclickhouse.Named("start_date", "2026-01-01"), stdclickhouse.Named("end_date", "2026-01-07")}
			var rows uint64
			if err := e.admin.QueryRow(e.ctx, `SELECT count() FROM testops_test_metrics_daily WHERE org_id = ? AND repo_id = ? AND day = ? AND computed_at = toDateTime(?, 'UTC')`,
				org, repo, day1, tieAt).Scan(&rows); err != nil {
				t.Fatal(err)
			}
			if rows != 2 {
				t.Fatalf("control: %d stored rows of the day at the tie, want 2 (the retraction and the live row)", rows)
			}
			count := func(query string) uint64 {
				t.Helper()
				var n uint64
				if err := e.admin.QueryRow(e.ctx, query, params...).Scan(&n); err != nil {
					t.Fatalf("%v\n%s", err, query)
				}
				return n
			}
			var markerStored uint8
			var markerRetracted uint8
			if err := e.admin.QueryRow(e.ctx, `SELECT pooled_samples_stored, retracted FROM `+testopspooled.NewestStoredRows("testops_test_metrics_daily", "retracted"), params...).
				Scan(&markerStored, &markerRetracted); err != nil {
				t.Fatal(err)
			}
			for read := 0; read < 5; read++ {
				admitted := count(`SELECT count() FROM (` + live + `)`)
				gateLive := count(`SELECT count() FROM (SELECT DISTINCT repo_id, day FROM testops_test_metrics_daily
				WHERE org_id = {org_id:String} AND day >= {start_date:Date} AND day < {end_date:Date}) WHERE ` + testopspooled.LiveRepoDays("testops_test_metrics_daily"))
				var headP95, headP50, baseP95, baseP50 *float64
				if err := e.admin.QueryRow(e.ctx, "SELECT "+testopspooled.P95+", "+testopspooled.Median+" FROM "+testopspooled.SuiteRunsSource, params...).Scan(&headP95, &headP50); err != nil {
					t.Fatal(err)
				}
				if err := e.admin.QueryRow(e.ctx, "SELECT "+testopspooled.P95+", "+testopspooled.Median+" FROM "+baseSource, params...).Scan(&baseP95, &baseP50); err != nil {
					t.Fatal(err)
				}
				t.Logf("CELL14595 tie %s %s read %d: marker row (stored %d, retracted %d); raw arm admits %d, live gate %d; served p95 %s / median %s, base %s / %s",
					arm.name, order.name, read, markerStored, markerRetracted, admitted, gateLive, str(headP95), str(headP50), str(baseP95), str(baseP50))
				if gateLive == 0 && (admitted != 0 || headP95 != nil || headP50 != nil) {
					t.Errorf("%s %s read %d: the live gate retires the day, but the raw arm admits it %d time(s) and serves p95 %s / median %s",
						arm.name, order.name, read, admitted, str(headP95), str(headP50))
				}
				if gateLive == 1 && (!same(headP95, &wantP95) || !same(headP50, &wantP50)) {
					t.Errorf("%s %s read %d: the live gate serves the day, but the served p95 %s / median %s, want %v / %v (the day's samples)",
						arm.name, order.name, read, str(headP95), str(headP50), wantP95, wantP50)
				}
				if !same(headP95, baseP95) || !same(headP50, baseP50) {
					t.Errorf("%s %s read %d: served p95 %s / median %s, the base legacy set serves %s / %s",
						arm.name, order.name, read, str(headP95), str(headP50), str(baseP95), str(baseP50))
				}
			}
		})
	}
}

// A live over-cap row (pooled_samples_stored 2: no stored samples, the raw arm reads the day) tied with a retraction row.
func TestTestopsPooledSuiteOverCapRowTiedWithARetraction(t *testing.T) {
	checkTieArmAgainstTheLiveGate(t, tieArm{
		name:    "over-cap",
		liveRow: `INSERT INTO testops_test_metrics_daily (repo_id, day, org_id, computed_at, pooled_samples_stored) VALUES (?, ?, ?, toDateTime(?, 'UTC'), 2)`,
	}, [2]string{"f0f0f0f0-0000-4000-8000-0000001459f3", "f0f0f0f0-0000-4000-8000-0000001459f4"},
		[2]string{"f0f0f0f0-0000-4000-8000-0000001459c3", "f0f0f0f0-0000-4000-8000-0000001459c4"})
}

// A live stored row (pooled_samples_stored 1 with the day's samples 10, 20, 30) tied with a retraction row.
func TestTestopsPooledSuiteStoredRowTiedWithARetraction(t *testing.T) {
	checkTieArmAgainstTheLiveGate(t, tieArm{
		name: "stored",
		liveRow: `INSERT INTO testops_test_metrics_daily (repo_id, day, org_id, computed_at, pooled_service_ids, pooled_durations, pooled_samples_stored)
VALUES (?, ?, ?, toDateTime(?, 'UTC'), [NULL, NULL, NULL], [10., 20., 30.], 1)`,
	}, [2]string{"f0f0f0f0-0000-4000-8000-0000001459f5", "f0f0f0f0-0000-4000-8000-0000001459f6"},
		[2]string{"f0f0f0f0-0000-4000-8000-0000001459c5", "f0f0f0f0-0000-4000-8000-0000001459c6"})
}
