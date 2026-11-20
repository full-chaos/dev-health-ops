//go:build integration

package operatingreview

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/aianalytics"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
)

// Two versions of one key that share computed_at serve ONE version whatever the scan order. Versions go in as separate
// INSERT statements (same-key rows of ONE block collapse under ReplacingMergeTree) into a table with merges stopped.
// CHAOS-14355.

// renderRows prints the rows a fetch returns, pointers dereferenced (nil = NULL), unexported fields included.
func renderRows(rows any) string {
	var value func(v reflect.Value) string
	value = func(v reflect.Value) string {
		switch v.Kind() {
		case reflect.Pointer:
			if v.IsNil() {
				return "NULL"
			}
			return value(v.Elem())
		case reflect.Float32, reflect.Float64:
			return fmt.Sprint(v.Float())
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return fmt.Sprint(v.Int())
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return fmt.Sprint(v.Uint())
		case reflect.String:
			return v.String()
		case reflect.Bool:
			return fmt.Sprint(v.Bool())
		case reflect.Struct:
			parts := make([]string, v.NumField())
			for i := range v.NumField() {
				parts[i] = v.Type().Field(i).Name + "=" + value(v.Field(i))
			}
			return "{" + strings.Join(parts, " ") + "}"
		case reflect.Slice:
			parts := make([]string, v.Len())
			for i := range v.Len() {
				parts[i] = value(v.Index(i))
			}
			return strings.Join(parts, ";")
		}
		return v.Kind().String()
	}
	return value(reflect.ValueOf(rows))
}

// tieSpec is one statement under test: the table, the key columns of key k, the value columns it reads, how a row of
// that table retires a key (none: no live-row rule), and the fetch that serves it.
type tieSpec struct {
	name     string
	table    string
	ms       bool
	key      func(k int) [][2]string
	values   []string
	nullable string
	retire   string // "zeros", "retracted" or "" (no live-row rule)
	fetch    func() (any, error)
}

func TestArgMaxTieOperatingReview14355(t *testing.T) {
	ctx, conn, client := startOperatingReviewSchema(t)
	const org = "org-14355"
	start := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	end := start.AddDate(0, 0, 1)
	repo := func(k int) string { return fmt.Sprintf("14355000-0000-4000-8000-00000000000%d", k) }
	day := [2]string{"day", "toDate('2026-08-01')"}
	orgCol := [2]string{"org_id", "'" + org + "'"}
	specs := []tieSpec{
		{name: "fetchWorkItems", table: "work_item_metrics_daily",
			key: func(k int) [][2]string {
				return [][2]string{day, {"provider", "'github'"}, {"work_scope_id", fmt.Sprintf("'scope-%d'", k)}, {"team_id", "'team-a'"}, orgCol}
			},
			values:   []string{"items_started", "items_completed", "wip_count_end_of_day", "cycle_time_p50_hours", "cycle_time_p90_hours", "wip_age_p50_hours", "wip_age_p90_hours"},
			nullable: "cycle_time_p50_hours", retire: "zeros",
			fetch: func() (any, error) { return fetchWorkItems(ctx, client, org, nil, start, end) }},
		{name: "fetchStateDurations", table: "work_item_state_durations_daily",
			key: func(k int) [][2]string {
				return [][2]string{day, {"provider", "'github'"}, {"work_scope_id", fmt.Sprintf("'scope-%d'", k)}, {"team_id", "'team-a'"}, {"status", "'doing'"}, orgCol}
			},
			values: []string{"duration_hours", "items_touched", "avg_wip"}, retire: "zeros",
			fetch: func() (any, error) { return fetchStateDurations(ctx, client, org, nil, start, end) }},
		{name: "fetchRepoMetrics", table: "repo_metrics_daily",
			key:      func(k int) [][2]string { return [][2]string{{"repo_id", "'" + repo(k) + "'"}, day, orgCol} },
			values:   []string{"prs_merged", "pr_first_review_p50_hours", "single_owner_file_ratio_30d", "code_ownership_gini", "bus_factor", "revert_rate", "mttr_hours"},
			nullable: "mttr_hours", retire: "retracted",
			fetch: func() (any, error) { return fetchRepoMetrics(ctx, client, org, start, end) }},
		{name: "fetchHotspotsAgg", table: "file_hotspot_daily",
			key: func(k int) [][2]string {
				return [][2]string{{"repo_id", "'" + repo(1) + "'"}, day, {"file_path", fmt.Sprintf("'f%d.go'", k)}, orgCol}
			},
			values: []string{"risk_score"},
			fetch:  func() (any, error) { return fetchHotspotsAgg(ctx, client, org, start, end) }},
		{name: "fetchComplexityAgg", table: "repo_complexity_daily",
			key:    func(k int) [][2]string { return [][2]string{{"repo_id", "'" + repo(k) + "'"}, day, orgCol} },
			values: []string{"cyclomatic_per_kloc"},
			fetch:  func() (any, error) { return fetchComplexityAgg(ctx, client, org, start, end) }},
		{name: "fetchDeploymentsAgg", table: "deploy_metrics_daily",
			key:    func(k int) [][2]string { return [][2]string{{"repo_id", "'" + repo(k) + "'"}, day, orgCol} },
			values: []string{"deployments_count", "failed_deployments_count"}, retire: "retracted",
			fetch: func() (any, error) { return fetchDeploymentsAgg(ctx, client, org, start, end) }},
		{name: "fetchIncidentsAgg", table: "incident_metrics_daily",
			key:    func(k int) [][2]string { return [][2]string{{"repo_id", "'" + repo(k) + "'"}, day, orgCol} },
			values: []string{"incidents_count", "mttr_p50_hours"}, nullable: "mttr_p50_hours", retire: "retracted",
			fetch: func() (any, error) { return fetchIncidentsAgg(ctx, client, org, start, end) }},
		{name: "fetchAIImpact", table: "ai_impact_metrics_daily", ms: true,
			key: func(k int) [][2]string {
				return [][2]string{orgCol, {"team_id", "'team-a'"}, {"repo_id", "'" + repo(k) + "'"}, {"work_type", "'pull_request'"}, day, {"attribution_bucket", "'ai_assisted'"}}
			},
			values: []string{"prs_total", "ai_assisted_prs", "agent_created_prs", "human_prs", "unknown_prs", "ai_cycle_time_delta_hours",
				"ai_review_amplification", "rework_drag_rate", "test_gap_rate", "incident_drag_rate"},
			nullable: "ai_cycle_time_delta_hours",
			fetch:    func() (any, error) { return fetchAIImpact(ctx, client, org, nil, start, end) }},
		{name: "fetchAIGovernance", table: "ai_governance_coverage_daily", ms: true,
			key: func(k int) [][2]string {
				return [][2]string{orgCol, {"team_id", "'team-a'"}, {"repo_id", "toUUID('" + repo(k) + "')"}, day}
			},
			values: []string{"ai_artifacts", "declared_artifacts", "human_reviewed_prs", "security_scanned_prs", "in_policy_artifacts"}, retire: "zeros",
			fetch: func() (any, error) { return fetchAIGovernance(ctx, client, org, nil, start, end) }},
	}
	for _, c := range specs {
		ts := func(step int) string {
			if c.ms {
				return fmt.Sprintf("toDateTime64('2026-08-01 12:00:00.%03d', 3, 'UTC')", step)
			}
			return fmt.Sprintf("toDateTime('2026-08-01 12:00:%02d', 'UTC')", step)
		}
		const older = -1
		// row is one INSERT of key k at time step; vals maps a value column to its literal, the others keep their default.
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
		read := func(t *testing.T) string {
			t.Helper()
			rows, err := c.fetch()
			if err != nil {
				t.Fatal(err)
			}
			return renderRows(rows)
		}
		// served runs each insert order of the statements (3 times each) and returns the distinct reads.
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
					seen[read(t)] = true
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
			t.Run("a tie of a measurement and a retraction row: one outcome, never the retraction row", func(t *testing.T) {
				live, dead := row(1, 0, all("4")), row(1, 0, retired())
				exec(t, `TRUNCATE TABLE `+c.table)
				empty := read(t)
				liveRef := only(t, live)
				if got := served(t, live, dead); !in(got, liveRef, empty) {
					t.Errorf("served %q; want the measurement (%s) or no key (%s)", got, liveRef, empty)
				}
			})
			t.Run("no tie: rows as main serves them, a retired key stays absent", func(t *testing.T) {
				exec(t, `SYSTEM STOP MERGES `+c.table)
				exec(t, `TRUNCATE TABLE `+c.table)
				for _, q := range []string{
					row(1, older, all("9")), row(1, 0, all("3")), // key 1: the newer version wins
					row(2, older, all("5")), row(2, 0, retired()), // key 2: its newest row retires it
					row(3, 0, all("4")), // key 3: one version
				} {
					exec(t, q)
				}
				got := read(t)
				exec(t, `TRUNCATE TABLE `+c.table)
				exec(t, row(1, 0, all("3")))
				exec(t, row(3, 0, all("4")))
				if want := read(t); got != want {
					t.Errorf("seed served\n%s\nwant (key 2 absent, newest versions)\n%s", got, want)
				}
				if want := noTieWant[c.name]; got != want {
					t.Errorf("seed served\n%s\nwant the base output\n%s", got, want)
				}
			})
		})
	}
}

// noTieWant is what the statements served for the no-tie seed at base (main 2da190e7b89f, before CHAOS-14355).
var noTieWant = map[string]string{
	"fetchWorkItems":      "{itemsStarted=7 itemsCompleted=7 wipCountEndOfDay=4 cycleTimeP50Hours=3.5 cycleTimeP90Hours=3.5 wipAgeP50Hours=3.5 wipAgeP90Hours=3.5}",
	"fetchStateDurations": "{itemsTouched=7 durationHours=3.5 avgWip=3.5}",
	"fetchRepoMetrics":    "{prsMerged=7 prFirstReviewP50Hours=3.5 singleOwnerFileRatio30d=3.5 codeOwnershipGini=3.5 busFactor=3 revertRate=3.5714285714285716 mttrHours=3.5 storedRows=2}",
	"fetchDeploymentsAgg": "{deploymentsCount=7 failedDeploymentsCount=7 storedRows=2}",
	"fetchIncidentsAgg":   "{incidentsCount=7 mttrP50Hours=3.5 storedRows=2}",
	"fetchAIGovernance": "{aiArtifacts=3 declaredArtifacts=3 humanReviewedPrs=3 securityScannedPrs=3 inPolicyArtifacts=3};" +
		"{aiArtifacts=4 declaredArtifacts=4 humanReviewedPrs=4 securityScannedPrs=4 inPolicyArtifacts=4}",
}

// On a tie of two ai_impact_metrics_daily versions, the operating review (fetchAIImpact) and the AI analytics impact
// summary serve the SAME version: both order by the same hash of the version's 27 value columns. The versions also
// differ in prs_merged, a column only the AI analytics reads, so a hash of fewer columns would split the two pages.
func TestArgMaxTieOperatingReviewAndAIAnalyticsServeOneVersion14355(t *testing.T) {
	ctx, conn, client := startOperatingReviewSchema(t)
	const org = "org-14355-cross"
	const repo = "14355000-0000-4000-8000-0000000000c1"
	start := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	dr := model.AIDateRangeInput{StartDate: graphqldate.New(start), EndDate: graphqldate.New(start)}
	version := func(prs, merged int, delta string) string {
		return fmt.Sprintf(`INSERT INTO ai_impact_metrics_daily (org_id, team_id, repo_id, work_type, day, attribution_bucket,
            prs_total, prs_merged, ai_assisted_prs, ai_cycle_time_delta_hours, computed_at)
            SELECT '%s', 'team-a', '%s', 'pull_request', toDate('2026-08-01'), 'ai_assisted', %d, %d, %d, %s,
            toDateTime64('2026-08-01 12:00:00.000', 3, 'UTC')`, org, repo, prs, merged, prs, delta)
	}
	exec := func(q string) {
		t.Helper()
		if err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	exec(`SYSTEM STOP MERGES ai_impact_metrics_daily`)
	for k := 1; k <= 8; k++ {
		versions := [2]string{version(k, 100+k, fmt.Sprintf("%d.5", k)), version(k+50, 200+k, "NULL")}
		review, analytics := map[float64]bool{}, map[float64]bool{}
		for _, order := range [][2]int{{0, 1}, {1, 0}} {
			for range 3 {
				exec(`TRUNCATE TABLE ai_impact_metrics_daily`)
				for _, i := range order {
					exec(versions[i])
				}
				rows, err := fetchAIImpact(ctx, client, org, nil, start, start.AddDate(0, 0, 1))
				if err != nil {
					t.Fatal(err)
				}
				if len(rows) != 1 {
					t.Fatalf("pair %d: operating review rows %d", k, len(rows))
				}
				review[rows[0].prsTotal] = true
				summary, err := aianalytics.ImpactSummary(ctx, client, org, dr, nil)
				if err != nil {
					t.Fatal(err)
				}
				analytics[summary.TotalPrs] = true
			}
		}
		if len(review) != 1 || len(analytics) != 1 || fmt.Sprint(review) != fmt.Sprint(analytics) {
			t.Errorf("pair %d: operating review served prs_total %v, AI analytics served %v; want one and the same version", k, review, analytics)
		}
	}
}
