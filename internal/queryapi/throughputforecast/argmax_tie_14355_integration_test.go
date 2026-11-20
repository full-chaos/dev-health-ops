//go:build integration

package throughputforecast

import (
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"
)

// Two versions of one key that share computed_at serve ONE version whatever the scan order. Versions go in as separate
// INSERT statements (same-key rows of ONE block collapse under ReplacingMergeTree) into a table with merges stopped.
// CHAOS-14355.

func optFloat(p *float64) string {
	if p == nil {
		return "NULL"
	}
	return fmt.Sprint(*p)
}

// tieSpec is one read under test: the table, the key columns of key k, the value columns the cells write, how a row of
// the table retires a key, whether the read leaves a retired key out, and the read that serves it.
type tieSpec struct {
	name     string
	table    string
	ms       bool
	key      func(k int) [][2]string
	values   []string
	nullable string
	retire   string // "zeros", "retracted" or "" (no liveness cells)
	filtered bool   // the read leaves a key whose newest row retires it out
	read     func(t *testing.T) string
}

func TestArgMaxTieThroughputForecast14355(t *testing.T) {
	ctx, conn, client := startThroughputForecastSchema(t)
	const org = "org-14355"
	today := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	const weeks = 4
	repo := func(k int) string { return fmt.Sprintf("14355000-0000-4000-8000-00000000000%d", k) }
	day := [2]string{"day", "toDate('2026-08-01')"}
	orgCol := [2]string{"org_id", "'" + org + "'"}
	workKey := func(k int) [][2]string {
		return [][2]string{day, {"provider", "'github'"}, {"work_scope_id", fmt.Sprintf("'scope-%d'", k)}, {"team_id", "'team-a'"}, orgCol}
	}
	workValues := []string{"items_completed", "wip_count_end_of_day", "wip_age_p50_hours", "wip_age_p90_hours", "items_started"}
	repoKey := func(k int) [][2]string { return [][2]string{{"repo_id", "'" + repo(k) + "'"}, day, orgCol} }
	must := func(t *testing.T, err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	// incidentWeeks is the divisor loadIncidentOverlay takes from the server clock, so a cell pins the sum, not the rate.
	incidentWeeks := func(t *testing.T) float64 {
		var w float64
		must(t, conn.QueryRow(ctx, `SELECT toFloat64(greatest(dateDiff('week', toDate('`+startDate(today, weeks)+`'), today()), 1))`).Scan(&w))
		return w
	}
	specs := []tieSpec{
		{name: "loadThroughputHistory", table: "work_item_metrics_daily", key: workKey, values: workValues, retire: "zeros", filtered: true,
			read: func(t *testing.T) string {
				h, err := loadThroughputHistory(ctx, client, org, nil, nil, weeks, today)
				must(t, err)
				return fmt.Sprint(h)
			}},
		{name: "loadWorkItemOverlay", table: "work_item_metrics_daily", key: workKey, values: workValues, retire: "zeros",
			read: func(t *testing.T) string {
				current, average, err := loadWorkItemOverlay(ctx, client, org, nil, nil, weeks, today)
				must(t, err)
				return fmt.Sprintf("current=%v average=%v", current, average)
			}},
		{name: "loadStaleWIP", table: "work_item_metrics_daily", key: workKey, values: workValues, nullable: "wip_age_p50_hours",
			read: func(t *testing.T) string {
				p50, p90, err := loadStaleWIP(ctx, client, org, nil, nil)
				must(t, err)
				return "p50=" + optFloat(p50) + " p90=" + optFloat(p90)
			}},
		{name: "loadBacklog", table: "work_item_metrics_daily", key: workKey, values: workValues, retire: "zeros",
			read: func(t *testing.T) string {
				b, err := loadBacklog(ctx, client, org, nil, nil, weeks, today)
				must(t, err)
				asOf := "nil"
				if b.asOfDay != nil {
					asOf = b.asOfDay.Format("2006-01-02")
				}
				return fmt.Sprintf("size=%d state=%s asOf=%s", b.size, b.state, asOf)
			}},
		{name: "loadEstimateCoverage", table: "estimate_coverage_metrics_daily", ms: true,
			key: func(k int) [][2]string {
				return [][2]string{day, {"provider", "'github'"}, {"work_scope_id", fmt.Sprintf("'scope-%d'", k)}, {"team_id", "'team-a'"}, orgCol}
			},
			values: []string{"estimated_count", "unestimated_count", "backlog_size"},
			read: func(t *testing.T) string {
				e, err := loadEstimateCoverage(ctx, client, org, nil, nil)
				must(t, err)
				return fmt.Sprintf("ratio=%s estimated=%d unestimated=%d backlog=%d grains=%d", optFloat(e.ratio), e.estimatedCount, e.unestimatedCount, e.backlogSize, e.grainCount)
			}},
		{name: "loadReviewOverlay", table: "repo_metrics_daily", key: repoKey,
			values:   []string{"pr_first_review_p50_hours", "prs_merged", "mttr_hours"},
			nullable: "pr_first_review_p50_hours", retire: "retracted", filtered: true,
			read: func(t *testing.T) string {
				v, err := loadReviewOverlay(ctx, client, org, weeks, today)
				must(t, err)
				return fmt.Sprint(v)
			}},
		{name: "loadIncidentOverlay", table: "incident_metrics_daily", key: repoKey,
			values: []string{"incidents_count", "mttr_p50_hours"}, retire: "retracted", filtered: true,
			read: func(t *testing.T) string {
				v, err := loadIncidentOverlay(ctx, client, org, weeks, today)
				must(t, err)
				return fmt.Sprintf("sum=%.6f", v*incidentWeeks(t))
			}},
	}
	for _, c := range specs {
		ts := func(step int) string {
			if c.ms {
				return fmt.Sprintf("toDateTime64('2026-08-01 12:00:00.%03d', 3, 'UTC')", step)
			}
			return fmt.Sprintf("toDateTime('2026-08-01 12:00:%02d', 'UTC')", step)
		}
		const older = -1
		row := func(k, step int, vals map[string]string) string {
			var names, literals []string
			for _, kv := range c.key(k) {
				names, literals = append(names, kv[0]), append(literals, kv[1])
			}
			for _, col := range append(append([]string{}, c.values...), "retracted") {
				if v, ok := vals[col]; ok {
					names, literals = append(names, col), append(literals, v)
				}
			}
			at := ts(step)
			if step == older {
				at = strings.Replace(ts(0), "12:00:00", "11:00:00", 1)
			}
			names, literals = append(names, "computed_at"), append(literals, at)
			return `INSERT INTO ` + c.table + ` (` + strings.Join(names, ", ") + `) SELECT ` + strings.Join(literals, ", ")
		}
		all := func(v string) map[string]string {
			m := map[string]string{}
			for _, col := range c.values {
				m[col] = v
			}
			return m
		}
		retired := func() map[string]string {
			if c.retire == "retracted" {
				return map[string]string{"retracted": "1"}
			}
			return map[string]string{}
		}
		exec := func(t *testing.T, q string) {
			t.Helper()
			if err := conn.Exec(ctx, q); err != nil {
				t.Fatalf("%s: %v", q, err)
			}
		}
		served := func(t *testing.T, statements ...string) []string {
			t.Helper()
			exec(t, `SYSTEM STOP MERGES `+c.table)
			seen := map[string]bool{}
			orders := [][]string{statements}
			if len(statements) == 2 {
				orders = append(orders, []string{statements[1], statements[0]})
			}
			for _, order := range orders {
				for range 3 {
					exec(t, `TRUNCATE TABLE `+c.table)
					for _, q := range order {
						exec(t, q)
					}
					seen[c.read(t)] = true
				}
			}
			out := make([]string, 0, len(seen))
			for v := range seen {
				out = append(out, v)
			}
			sort.Strings(out)
			return out
		}
		only := func(t *testing.T, statements ...string) string {
			t.Helper()
			got := served(t, statements...)
			if len(got) != 1 {
				t.Fatalf("reference read not stable: %v", got)
			}
			return got[0]
		}
		in := func(got []string, allowed ...string) bool {
			if len(got) != 1 {
				return false
			}
			for _, a := range allowed {
				if got[0] == a {
					return true
				}
			}
			return false
		}
		// seeded serves the read over these statements, run once each in order.
		seeded := func(t *testing.T, statements ...string) string {
			t.Helper()
			exec(t, `SYSTEM STOP MERGES `+c.table)
			exec(t, `TRUNCATE TABLE `+c.table)
			for _, q := range statements {
				exec(t, q)
			}
			return c.read(t)
		}
		t.Run(c.name, func(t *testing.T) {
			t.Run("tie serves one version, every column from it", func(t *testing.T) {
				one, two := row(1, 0, all("1")), row(1, 0, all("2"))
				ref1, ref2 := only(t, one), only(t, two)
				if got := served(t, one, two); !in(got, ref1, ref2) {
					t.Errorf("same key, same computed_at served %q; want one of\n%s\n%s", got, ref1, ref2)
				}
			})
			t.Run("a newer computed_at wins over the tiebreak", func(t *testing.T) {
				for _, v := range [][2]string{{"1", "2"}, {"2", "1"}} {
					want := only(t, row(1, 1, all(v[1])))
					if got := served(t, row(1, 0, all(v[0])), row(1, 1, all(v[1]))); !in(got, want) {
						t.Errorf("older %s, newer %s: served %q; want %s", v[0], v[1], got, want)
					}
				}
			})
			if c.nullable != "" {
				t.Run("a NULL of the newest version stays NULL", func(t *testing.T) {
					newest := all("1")
					newest[c.nullable] = "NULL"
					want := only(t, row(1, 1, newest))
					if got := served(t, row(1, 0, all("1.5")), row(1, 1, newest)); !in(got, want) {
						t.Errorf("served %q; want %s", got, want)
					}
				})
			}
			if c.retire == "" {
				return
			}
			t.Run("a tie of a measurement and a retraction row: one of them, never a mix", func(t *testing.T) {
				live, dead := row(1, 0, all("4")), row(1, 0, retired())
				liveRef, deadRef := only(t, live), only(t, dead)
				if c.filtered {
					if empty := seeded(t); deadRef != empty {
						t.Errorf("a key whose only row retires it served %s; want no key (%s)", deadRef, empty)
					}
				}
				if got := served(t, live, dead); !in(got, liveRef, deadRef) {
					t.Errorf("served %q; want the measurement (%s) or the retraction row's read (%s)", got, liveRef, deadRef)
				}
			})
			t.Run("no tie: rows as main serves them, a retired key stays absent", func(t *testing.T) {
				got := seeded(t,
					row(1, older, all("9")), row(1, 0, all("3")), // key 1: the newer version wins
					row(2, older, all("5")), row(2, 0, retired()), // key 2: its newest row retires it
					row(3, 0, all("4")), // key 3: one version
				)
				if want := seeded(t, row(1, 0, all("3")), row(3, 0, all("4"))); got != want {
					t.Errorf("seed served\n%s\nwant (key 2 absent, newest versions)\n%s", got, want)
				}
				if want := noTieWant[c.name]; got != want {
					t.Errorf("seed served\n%s\nwant the base output\n%s", got, want)
				}
			})
			if c.name != "loadBacklog" {
				return
			}
			// The backlog read's other served columns: measured_keys, stale_keys and oldest_day, each through the state and
			// the as-of day it decides.
			atDay := func(k int, d string, step int, vals map[string]string) string {
				return strings.Replace(row(k, step, vals), "toDate('2026-08-01')", "toDate('"+d+"')", 1)
			}
			for name, statements := range map[string][]string{
				"measured_keys: a key whose newest row retires it is not measured": {row(2, older, all("5")), row(2, 0, retired())},
				"stale_keys: a measured key whose newest day is before the window": {atDay(1, "2026-07-01", 0, all("6")), atDay(3, "2026-08-01", 0, all("4"))},
				"oldest_day: the oldest newest day of the measured keys":           {atDay(1, "2026-08-03", 0, all("6")), atDay(3, "2026-08-01", 0, all("4")), atDay(2, "2026-07-20", older, all("5")), atDay(2, "2026-07-20", 0, retired())},
			} {
				t.Run(name, func(t *testing.T) {
					if got, want := seeded(t, statements...), noTieWant["loadBacklog/"+name]; got != want {
						t.Errorf("served %s; want the base output %s", got, want)
					}
				})
			}
		})
	}
}

// noTieWant is what the reads served for the no-tie seeds at base (main b985cef8754b, before slice 3 of CHAOS-14355).
var noTieWant = map[string]string{
	"loadThroughputHistory": "[7]",
	"loadWorkItemOverlay":   "current=7 average=7",
	"loadBacklog":           "size=7 state=MEASURED asOf=2026-08-01",
	"loadReviewOverlay":     "3.5",
	"loadIncidentOverlay":   "sum=7.000000",
	"loadBacklog/measured_keys: a key whose newest row retires it is not measured": "size=0 state=NO_ROWS asOf=nil",
	"loadBacklog/stale_keys: a measured key whose newest day is before the window": "size=10 state=STALE asOf=2026-07-01",
	"loadBacklog/oldest_day: the oldest newest day of the measured keys":           "size=10 state=MEASURED asOf=2026-08-01",
}
