//go:build integration

package remaining

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/rowview"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The cells of the recommendations loader's version picks on a computed_at tie
// (CHAOS-14407). Two versions of one key with the same computed_at go in as
// separate INSERTs into tables with merges stopped, in both insert orders, into
// ONE organization (a hash of the whole row holds org_id, so another org per
// order would be another fixture). The cells own their ClickHouse: they truncate
// the tables between orders.

var tieTables = []string{
	"work_item_metrics_daily", "repo_metrics_daily", "user_metrics_daily",
	"team_metrics_daily", "repo_complexity_daily", "file_hotspot_daily",
	"compounding_risk_daily",
}

const (
	tieOrg  = "org-14407"
	tieTeam = "team-14407"
	tieRepo = "14407000-0000-0000-0000-000000000001"
	// tieRepo2 is a second owned repository, for the hotspot read across repositories.
	tieRepo2 = "14407000-0000-0000-0000-000000000002"
)

// tieClock holds the window and the stamps of the cells.
type tieClock struct {
	start, end, day1, day2, at time.Time
}

func newTieClock() tieClock {
	return tieClock{
		start: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
		end:   time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
		day1:  time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC),
		day2:  time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC),
		at:    time.Date(2026, 9, 1, 3, 0, 0, 0, time.UTC),
	}
}

func tieDay(day time.Time) string { return day.Format("2006-01-02") }

func tieStamp(at time.Time) string { return at.Format("2006-01-02 15:04:05") }

type tieEnv struct {
	ctx   context.Context
	conn  driver.Conn
	clock tieClock
}

func startTieEnv(t *testing.T) *tieEnv {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	t.Setenv(operationalOrderingContractEnv, "2")
	chschema.Apply(ctx, t, instance)
	env := &tieEnv{ctx: ctx, conn: openClickHouse(t, ctx, instance), clock: newTieClock()}
	t.Cleanup(func() { _ = env.conn.Close() })
	for _, table := range tieTables {
		env.exec(t, "SYSTEM STOP MERGES "+table)
	}
	return env
}

func (env *tieEnv) exec(t *testing.T, statement string) {
	t.Helper()
	if err := env.conn.Exec(env.ctx, statement); err != nil {
		t.Fatalf("exec: %v\n%s", err, statement)
	}
}

func (env *tieEnv) reset(t *testing.T) {
	t.Helper()
	for _, table := range tieTables {
		env.exec(t, "TRUNCATE TABLE "+table)
	}
}

func (env *tieEnv) loader(t *testing.T) *RecommendationsLoader {
	t.Helper()
	loader, err := NewRecommendationsLoader(env.conn, tieOrg)
	if err != nil {
		t.Fatal(err)
	}
	return loader
}

// tieSite is one pick of the loader.
type tieSite struct {
	name string
	// fixed rows go in first; versions are two versions of one key (or of two keys
	// for a read across keys), stamped at1 and at2. k varies the values.
	fixed    func(c tieClock) []string
	versions func(c tieClock, at1, at2 time.Time, k int) [2]string
	read     func(t *testing.T, env *tieEnv) string
	// served is the version order of the table's served reader, "" for a read
	// across keys or a table whose served readers do not break a tie. marker is a
	// column whose value tells the two versions apart, filter selects the key.
	served, marker, filter string
	// tieWant is the value of a read across keys on a tie: the later day wins.
	tieWant string
}

func ownedRepos() []uuid.UUID {
	return []uuid.UUID{uuid.MustParse(tieRepo)}
}

func workItemRow(c tieClock, completed, wip int, cycle float64, at time.Time) string {
	return fmt.Sprintf(`INSERT INTO work_item_metrics_daily
		(day, provider, work_scope_id, team_id, team_name, items_completed, wip_count_end_of_day,
		 cycle_time_p50_hours, computed_at, org_id, row_view)
		VALUES ('%s', 'jira', 'S1', '%s', '%s', %d, %d, %g, '%s', '%s', '')`,
		tieDay(c.day1), tieTeam, tieTeam, completed, wip, cycle, tieStamp(at), tieOrg)
}

func complexityRow(day time.Time, perKloc float64, at time.Time) string {
	return fmt.Sprintf(`INSERT INTO repo_complexity_daily
		(repo_id, day, loc_total, cyclomatic_total, cyclomatic_per_kloc, computed_at, org_id)
		VALUES ('%s', '%s', 1000, 10, %g, '%s', '%s')`, tieRepo, tieDay(day), perKloc, tieStamp(at), tieOrg)
}

func hotspotRow(repo string, day time.Time, risk float64, at time.Time) string {
	return fmt.Sprintf(`INSERT INTO file_hotspot_daily
		(repo_id, day, file_path, churn_loc_30d, risk_score, computed_at, org_id)
		VALUES ('%s', '%s', 'a.go', 10, %g, '%s', '%s')`, repo, tieDay(day), risk, tieStamp(at), tieOrg)
}

// Served orders (the served reader's text, with its alias replaced by the table name).
const (
	servedRepoMetricsOrder = `(repo_metrics_daily.computed_at, cityHash64(tuple(
                repo_metrics_daily.prs_merged, repo_metrics_daily.pr_first_review_p50_hours,
                repo_metrics_daily.single_owner_file_ratio_30d, repo_metrics_daily.code_ownership_gini,
                repo_metrics_daily.bus_factor, repo_metrics_daily.revert_rate, repo_metrics_daily.mttr_hours,
                repo_metrics_daily.retracted)))`
	servedReviewsOrder    = `(umd.computed_at, cityHash64(tuple(umd.reviews_given, umd.retracted)))`
	servedComplexityOrder = `(repo_complexity_daily.computed_at, cityHash64(tuple(repo_complexity_daily.cyclomatic_per_kloc)))`
	servedHotspotOrder    = `(file_hotspot_daily.computed_at, cityHash64(tuple(file_hotspot_daily.risk_score)))`
)

func tieSites() []tieSite {
	return []tieSite{
		{
			name: "R1_team_source",
			versions: func(c tieClock, at1, at2 time.Time, k int) [2]string {
				return [2]string{workItemRow(c, 1+k, 2+k, 10+float64(k), at1), workItemRow(c, 50+k, 60+k, 30+float64(k), at2)}
			},
			read: func(t *testing.T, env *tieEnv) string {
				c := env.clock
				wip, throughput, err := env.loader(t).loadWIPThroughput(env.ctx, tieTeam, c.start, c.end)
				if err != nil {
					t.Fatal(err)
				}
				_, _, cycle, _, err := env.loader(t).loadSustainabilitySignals(env.ctx, tieTeam, c.start, c.end)
				if err != nil {
					t.Fatal(err)
				}
				return fmt.Sprintf("wip=%v throughput=%v cycle=%v", wip, throughput, cycle)
			},
			served: rowview.WorkItemMetricsNewestOrder, marker: "items_completed", filter: "1",
		},
		{
			name: "R2_rework_ratio",
			versions: func(c tieClock, at1, at2 time.Time, k int) [2]string {
				row := func(rework int, at time.Time) string {
					return fmt.Sprintf(`INSERT INTO repo_metrics_daily
						(repo_id, day, commits_count, prs_merged, prs_merged_reviewed, prs_merged_rework,
						 prs_merged_no_rework_signal, computed_at, org_id)
						VALUES ('%s', '%s', 1, 10, 10, %d, 0, '%s', '%s')`,
						tieRepo, tieDay(c.day1), rework, tieStamp(at), tieOrg)
				}
				return [2]string{row(1, at1), row(5, at2)}
			},
			read: func(t *testing.T, env *tieEnv) string {
				c := env.clock
				value, known, err := env.loader(t).loadReworkRatio(env.ctx, tieTeam, ownedRepos(), c.start, c.end)
				if err != nil {
					t.Fatal(err)
				}
				return fmt.Sprintf("rework=%g known=%v", value, known)
			},
		},
		{
			name: "R3_review_latency",
			versions: func(c tieClock, at1, at2 time.Time, k int) [2]string {
				row := func(merged int, p75 float64, at time.Time) string {
					return fmt.Sprintf(`INSERT INTO repo_metrics_daily
						(repo_id, day, commits_count, prs_merged, pr_cycle_p75_hours, computed_at, org_id)
						VALUES ('%s', '%s', 1, %d, %g, '%s', '%s')`,
						tieRepo, tieDay(c.day1), merged, p75, tieStamp(at), tieOrg)
				}
				return [2]string{row(10+k, 10+float64(k), at1), row(40+k, 30+float64(k), at2)}
			},
			read: func(t *testing.T, env *tieEnv) string {
				c := env.clock
				latency, known, _, _, err := env.loader(t).loadReviewSignals(env.ctx, tieTeam, ownedRepos(), c.start, c.end)
				if err != nil {
					t.Fatal(err)
				}
				return fmt.Sprintf("latency=%g known=%v", latency, known)
			},
			served: servedRepoMetricsOrder, marker: "pr_cycle_p75_hours", filter: "1",
		},
		{
			// Two keys (two days) at one computed_at: the read spans days, so merges
			// never remove the tie. The later day wins.
			tieWant: "latency=10 known=true",
			name:    "R3x_review_latency_two_days",
			versions: func(c tieClock, at1, at2 time.Time, k int) [2]string {
				row := func(day time.Time, p75 float64, at time.Time) string {
					return fmt.Sprintf(`INSERT INTO repo_metrics_daily
						(repo_id, day, commits_count, prs_merged, pr_cycle_p75_hours, computed_at, org_id)
						VALUES ('%s', '%s', 1, 10, %g, '%s', '%s')`,
						tieRepo, tieDay(day), p75, tieStamp(at), tieOrg)
				}
				return [2]string{row(c.day1, 30, at1), row(c.day2, 10, at2)}
			},
			read: func(t *testing.T, env *tieEnv) string {
				c := env.clock
				latency, known, _, _, err := env.loader(t).loadReviewSignals(env.ctx, tieTeam, ownedRepos(), c.start, c.end)
				if err != nil {
					t.Fatal(err)
				}
				return fmt.Sprintf("latency=%g known=%v", latency, known)
			},
		},
		{
			name: "R4_reviewer_gini",
			fixed: func(c tieClock) []string {
				return []string{fmt.Sprintf(`INSERT INTO user_metrics_daily
					(repo_id, day, author_email, commits_count, reviews_given, team_id, computed_at, org_id)
					VALUES ('%s', '%s', 'a@example.test', 1, 10, '', '%s', '%s')`,
					tieRepo, tieDay(c.day1), tieStamp(c.at), tieOrg)}
			},
			versions: func(c tieClock, at1, at2 time.Time, k int) [2]string {
				row := func(reviews, retracted int, at time.Time) string {
					return fmt.Sprintf(`INSERT INTO user_metrics_daily
						(repo_id, day, author_email, commits_count, reviews_given, retracted, team_id, computed_at, org_id)
						VALUES ('%s', '%s', 'b@example.test', 1, %d, %d, '', '%s', '%s')`,
						tieRepo, tieDay(c.day1), reviews, retracted, tieStamp(at), tieOrg)
				}
				// The second version is a retraction row: liveness is judged on the picked version.
				return [2]string{row(10+k, 0, at1), row(30+k, 1, at2)}
			},
			read: func(t *testing.T, env *tieEnv) string {
				c := env.clock
				_, _, gini, known, err := env.loader(t).loadReviewSignals(env.ctx, tieTeam, ownedRepos(), c.start, c.end)
				if err != nil {
					t.Fatal(err)
				}
				return fmt.Sprintf("gini=%.6f known=%v", gini, known)
			},
			served: strings.ReplaceAll(servedReviewsOrder, "umd.", "user_metrics_daily."), marker: "reviews_given",
			filter: "author_email = 'b@example.test'",
		},
		{
			name: "R5_after_hours",
			versions: func(c tieClock, at1, at2 time.Time, k int) [2]string {
				row := func(commits, afterHours int, at time.Time) string {
					return fmt.Sprintf(`INSERT INTO team_metrics_daily
						(day, team_id, team_name, commits_count, after_hours_commits_count, computed_at, org_id, repo_id)
						VALUES ('%s', '%s', '%s', %d, %d, '%s', '%s', '%s')`,
						tieDay(c.day1), tieTeam, tieTeam, commits, afterHours, tieStamp(at), tieOrg, tieRepo)
				}
				// Mixing the columns of the two versions would serve 15/10 or 1/20.
				return [2]string{row(10+k, 1, at1), row(20+k, 15, at2)}
			},
			read: func(t *testing.T, env *tieEnv) string {
				c := env.clock
				ratio, known, _, _, err := env.loader(t).loadSustainabilitySignals(env.ctx, tieTeam, c.start, c.end)
				if err != nil {
					t.Fatal(err)
				}
				return fmt.Sprintf("after_hours=%g known=%v", ratio, known)
			},
			// No served reader of the table breaks a tie: the cell holds the loader to
			// one whole version by its own order.
			served: "(team_metrics_daily.computed_at, cityHash64(" + teamMetricsPick + "))", marker: "commits_count", filter: "1",
		},
		{
			name: "R6_complexity_delta",
			fixed: func(c tieClock) []string {
				return []string{complexityRow(c.day1, 10, c.at)}
			},
			versions: func(c tieClock, at1, at2 time.Time, k int) [2]string {
				return [2]string{complexityRow(c.day2, 12+float64(k), at1), complexityRow(c.day2, 16+float64(k), at2)}
			},
			read: func(t *testing.T, env *tieEnv) string {
				c := env.clock
				delta, known, _, _, err := env.loader(t).loadCompoundingSignals(env.ctx, tieTeam, ownedRepos(), c.start, c.end)
				if err != nil {
					t.Fatal(err)
				}
				return fmt.Sprintf("complexity_delta=%g known=%v", delta, known)
			},
			served: servedComplexityOrder, marker: "cyclomatic_per_kloc", filter: "day = '2026-08-20'",
		},
		{
			name: "R7_hotspot_count",
			fixed: func(c tieClock) []string {
				return []string{complexityRow(c.day1, 10, c.at), complexityRow(c.day2, 15, c.at)}
			},
			versions: func(c tieClock, at1, at2 time.Time, k int) [2]string {
				return [2]string{hotspotRow(tieRepo, c.day2, 0, at1), hotspotRow(tieRepo, c.day2, 0.6+float64(k)/100, at2)}
			},
			read: func(t *testing.T, env *tieEnv) string {
				c := env.clock
				_, _, overlap, known, err := env.loader(t).loadCompoundingSignals(env.ctx, tieTeam, ownedRepos(), c.start, c.end)
				if err != nil {
					t.Fatal(err)
				}
				return fmt.Sprintf("churn_overlap=%g known=%v", overlap, known)
			},
			served: servedHotspotOrder, marker: "risk_score", filter: "1",
		},
		{
			// One file path in two owned repositories on two days (two keys) at one
			// computed_at: the later day wins.
			name:    "R7x_hotspot_two_repos",
			tieWant: "churn_overlap=0.5 known=true",
			fixed: func(c tieClock) []string {
				return []string{complexityRow(c.day1, 10, c.at), complexityRow(c.day2, 15, c.at)}
			},
			versions: func(c tieClock, at1, at2 time.Time, k int) [2]string {
				return [2]string{hotspotRow(tieRepo, c.day2, 0, at1), hotspotRow(tieRepo2, c.day2.AddDate(0, 0, 5), 0.6, at2)}
			},
			read: func(t *testing.T, env *tieEnv) string {
				c := env.clock
				repos := []uuid.UUID{uuid.MustParse(tieRepo), uuid.MustParse(tieRepo2)}
				_, _, overlap, known, err := env.loader(t).loadCompoundingSignals(env.ctx, tieTeam, repos, c.start, c.end)
				if err != nil {
					t.Fatal(err)
				}
				return fmt.Sprintf("churn_overlap=%g known=%v", overlap, known)
			},
		},
		{
			name: "R8_compounding_risk",
			versions: func(c tieClock, at1, at2 time.Time, k int) [2]string {
				row := func(score float64, severity string, at time.Time) string {
					return fmt.Sprintf(`INSERT INTO compounding_risk_daily
						(org_id, day, scope, scope_id, compounding_risk, severity, churn_norm, complexity_norm,
						 ownership_norm, review_norm, w_churn, w_complexity, w_ownership, w_review,
						 threshold_elevated, threshold_high, computed_at)
						VALUES ('%s', '%s', 'team', '%s', %g, '%s', %g, 0.5, 0.5, 0.5, 0.25, 0.25, 0.25, 0.25, 0.5, 0.75, '%s')`,
						tieOrg, tieDay(c.day1), tieTeam, score, severity, score, tieStamp(at))
				}
				return [2]string{row(0.9+float64(k)/1000, "high", at1), row(0.1+float64(k)/1000, "low", at2)}
			},
			read: func(t *testing.T, env *tieEnv) string {
				c := env.clock
				score, known, severity, _, _, _, _, day, err := env.loader(t).loadCompoundingRiskPersisted(env.ctx, tieTeam, c.start, c.end)
				if err != nil {
					t.Fatal(err)
				}
				return fmt.Sprintf("score=%g known=%v severity=%s day=%s", score, known, severity, day)
			},
			served: servedCompoundingRiskOrder(), marker: "compounding_risk", filter: "scope = 'team'",
		},
	}
}

// servedCompoundingRiskOrder is Home's compounding_risk_daily order: computed_at,
// then the loader's compoundingRiskHash (the pin cell holds the hash to Home's text).
func servedCompoundingRiskOrder() string {
	return "(compounding_risk_daily.computed_at, " + compoundingRiskHash + ")"
}

// values seeds the site in both insert orders, 3 times each, and returns the set
// of values the read gave.
func (env *tieEnv) values(t *testing.T, site tieSite, at1, at2 time.Time) map[string]bool {
	t.Helper()
	got := map[string]bool{}
	for order := 0; order < 2; order++ {
		for rep := 0; rep < 3; rep++ {
			env.reset(t)
			env.seed(t, site, nil)
			versions := site.versions(env.clock, at1, at2, 0)
			if order == 1 {
				versions[0], versions[1] = versions[1], versions[0]
			}
			env.exec(t, versions[0])
			env.exec(t, versions[1])
			value := site.read(t, env)
			t.Logf("%s order=%d rep=%d: %s", site.name, order, rep, value)
			got[value] = true
		}
	}
	return got
}

func (env *tieEnv) seed(t *testing.T, site tieSite, versions []string) {
	t.Helper()
	if site.fixed != nil {
		for _, statement := range site.fixed(env.clock) {
			env.exec(t, statement)
		}
	}
	for _, statement := range versions {
		env.exec(t, statement)
	}
}

func sortedValues(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for value := range set {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func (env *tieEnv) servedMarker(t *testing.T, site tieSite, table string) string {
	t.Helper()
	var marker string
	query := fmt.Sprintf("SELECT toString(argMax(%s.%s, %s)) FROM %s WHERE org_id = '%s' AND (%s)",
		table, site.marker, site.served, table, tieOrg, site.filter)
	if err := env.conn.QueryRow(env.ctx, query).Scan(&marker); err != nil {
		t.Fatalf("served pick: %v\n%s", err, query)
	}
	return marker
}

func (env *tieEnv) marker(t *testing.T, site tieSite, table string) string {
	t.Helper()
	var marker string
	query := fmt.Sprintf("SELECT toString(any(%s)) FROM %s WHERE org_id = '%s' AND (%s)", site.marker, table, tieOrg, site.filter)
	if err := env.conn.QueryRow(env.ctx, query).Scan(&marker); err != nil {
		t.Fatalf("marker: %v\n%s", err, query)
	}
	return marker
}

func TestRecommendationsLoaderPicksOneVersionOnATie(t *testing.T) {
	env := startTieEnv(t)
	for _, site := range tieSites() {
		t.Run(site.name, func(t *testing.T) {
			at := env.clock.at
			// Control: the newer version wins in both insert orders.
			control := env.values(t, site, at, at.Add(time.Second))
			if len(control) != 1 {
				t.Errorf("control: the newer version must win in both insert orders: %v", sortedValues(control))
			}
			env.reset(t)
			env.seed(t, site, []string{site.versions(env.clock, at, at.Add(time.Second), 0)[1]})
			if newest := site.read(t, env); !control[newest] {
				t.Errorf("control: got %v, want the newer version alone: %s", sortedValues(control), newest)
			}
			// Tie: one value whatever the insert order.
			tie := env.values(t, site, at, at)
			if len(tie) != 1 {
				t.Errorf("a computed_at tie gives a value that depends on the insert order: %v", sortedValues(tie))
			}
			if site.tieWant != "" && !tie[site.tieWant] {
				t.Errorf("a tie across keys must give the later day's value %s: %v", site.tieWant, sortedValues(tie))
			}
		})
	}
}

// On a tie the loader reads the version the table's served reader picks, and
// every column (and the liveness) of that one version: the tie read equals the
// read of the served version alone. The pairs vary so that each version wins at
// least once.
func TestRecommendationsLoaderReadsTheServedVersionOnATie(t *testing.T) {
	env := startTieEnv(t)
	tableOf := regexp.MustCompile(`INSERT INTO ([a-z_]+)`)
	for _, site := range tieSites() {
		if site.served == "" {
			continue
		}
		t.Run(site.name, func(t *testing.T) {
			at := env.clock.at
			wins := [2]int{}
			for k := 0; k < 12; k++ {
				versions := site.versions(env.clock, at, at, k)
				table := tableOf.FindStringSubmatch(versions[0])[1]
				alone := [2]string{}
				markers := [2]string{}
				for i := range versions {
					env.reset(t)
					env.seed(t, site, []string{versions[i]})
					alone[i], markers[i] = site.read(t, env), env.marker(t, site, table)
				}
				env.reset(t)
				env.seed(t, site, versions[:])
				served, got := env.servedMarker(t, site, table), site.read(t, env)
				winner := 0
				if served == markers[1] {
					winner = 1
				} else if served != markers[0] {
					t.Fatalf("k=%d: the served pick %q is neither version (%q, %q)", k, served, markers[0], markers[1])
				}
				wins[winner]++
				if got != alone[winner] {
					t.Errorf("k=%d: the loader read %s; the served version %d alone reads %s", k, got, winner+1, alone[winner])
				}
			}
			t.Logf("%s: version 1 served %d times, version 2 %d times", site.name, wins[0], wins[1])
			if wins[0] == 0 || wins[1] == 0 {
				t.Fatalf("fixture: each version must win at least once to tell the orders apart: %v", wins)
			}
		})
	}
}

// The served orders the cells hold the loader to are the text of the served
// readers (aliases as written there).
func TestRecommendationsLoaderServedOrdersAreTheServedText(t *testing.T) {
	space := regexp.MustCompile(`\s+`)
	norm := func(text string) string { return space.ReplaceAllString(text, " ") }
	for path, want := range map[string][]string{
		"../../../queryapi/operatingreview/operatingreview.go": {
			strings.TrimSuffix(strings.TrimPrefix(servedRepoMetricsOrder, "("), ")"),
			servedComplexityOrder, servedHotspotOrder,
		},
		"../../../queryapi/aianalytics/loaders.go":           {servedReviewsOrder},
		"../../../queryapi/home/queries_signals.go":          {strings.TrimSuffix(strings.TrimPrefix(compoundingRiskHash, "cityHash64("), ")")},
		"../../../queryapi/throughputforecast/clickhouse.go": {"rowview.ServedTeamSource(\"work_item_metrics_daily\", where, rowview.WorkItemMetricsNewestOrder)"},
	} {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		text := norm(string(raw))
		for _, fragment := range want {
			if !strings.Contains(text, norm(fragment)) {
				t.Errorf("%s does not hold the served order the cells use:\n%s", path, fragment)
			}
		}
	}
}

// latencyRow is one repo_metrics_daily version for the review latency read.
func latencyRow(repo string, day time.Time, merged int, p75 float64, retracted int, at time.Time) string {
	return fmt.Sprintf(`INSERT INTO repo_metrics_daily
		(repo_id, day, commits_count, prs_merged, pr_cycle_p75_hours, retracted, computed_at, org_id)
		VALUES ('%s', '%s', 1, %d, %g, %d, '%s', '%s')`, repo, tieDay(day), merged, p75, retracted, tieStamp(at), tieOrg)
}

func (env *tieEnv) latency(t *testing.T, repos ...string) string {
	t.Helper()
	ids := make([]uuid.UUID, 0, len(repos))
	for _, repo := range repos {
		ids = append(ids, uuid.MustParse(repo))
	}
	c := env.clock
	latency, known, _, _, err := env.loader(t).loadReviewSignals(env.ctx, tieTeam, ids, c.start, c.end)
	if err != nil {
		t.Fatal(err)
	}
	if !known {
		return "absent"
	}
	return fmt.Sprintf("%gh", latency)
}

// The review latency reads a repository-day's picked version, and a picked
// retraction row (retracted = 1) leaves that day out: it is not a measured
// 0 h. The live-row rule runs on the picked version, never before the pick.
func TestRecommendationsLoaderLatencyLeavesRetractedDaysOut(t *testing.T) {
	env := startTieEnv(t)
	c := env.clock
	at, newer := c.at, c.at.Add(time.Second)
	for _, cell := range []struct {
		name  string
		rows  []string
		repos []string
		want  string
	}{
		{"live_zero_is_measured", []string{latencyRow(tieRepo, c.day1, 10, 0, 0, at)}, []string{tieRepo}, "0h"},
		{"retraction_alone", []string{latencyRow(tieRepo, c.day1, 0, 0, 1, at)}, []string{tieRepo}, "absent"},
		{"live_then_newer_retraction", []string{latencyRow(tieRepo, c.day1, 10, 40, 0, at), latencyRow(tieRepo, c.day1, 0, 0, 1, newer)}, []string{tieRepo}, "absent"},
		{"retired_repo_beside_a_live_repo", []string{latencyRow(tieRepo, c.day1, 10, 40, 0, at), latencyRow(tieRepo2, c.day1, 10, 30, 0, at), latencyRow(tieRepo2, c.day1, 0, 0, 1, newer)}, []string{tieRepo, tieRepo2}, "40h"},
		{"live_day_and_retracted_day_one_computed_at", []string{latencyRow(tieRepo, c.day1, 10, 40, 0, at), latencyRow(tieRepo, c.day2, 0, 0, 1, at)}, []string{tieRepo}, "40h"},
		{"older_retraction_then_newer_live", []string{latencyRow(tieRepo, c.day1, 0, 0, 1, at), latencyRow(tieRepo, c.day1, 10, 40, 0, newer)}, []string{tieRepo}, "40h"},
	} {
		t.Run(cell.name, func(t *testing.T) {
			for order := 0; order < 2; order++ {
				env.reset(t)
				rows := append([]string(nil), cell.rows...)
				if order == 1 {
					sort.Sort(sort.Reverse(sort.StringSlice(rows)))
				}
				for _, row := range rows {
					env.exec(t, row)
				}
				if got := env.latency(t, cell.repos...); got != cell.want {
					t.Errorf("order %d: latency = %s, want %s", order, got, cell.want)
				}
			}
		})
	}
	// One key, a live version and a retraction at one computed_at: the latency
	// is the served pick's (absent when the served order picks the retraction),
	// in both insert orders. The pairs vary so that each version wins.
	t.Run("same_key_tie_follows_the_served_pick", func(t *testing.T) {
		wins := map[string]int{}
		for k := 0; k < 12; k++ {
			live, retraction := latencyRow(tieRepo, c.day1, 10+k, 40, 0, at), latencyRow(tieRepo, c.day1, 0, 0, 1, at)
			env.reset(t)
			env.exec(t, live)
			env.exec(t, retraction)
			var servedRetracted uint8
			if err := env.conn.QueryRow(env.ctx, "SELECT argMax(repo_metrics_daily.retracted, "+servedRepoMetricsOrder+
				") FROM repo_metrics_daily WHERE org_id = '"+tieOrg+"'").Scan(&servedRetracted); err != nil {
				t.Fatal(err)
			}
			want := "40h"
			if servedRetracted == 1 {
				want = "absent"
			}
			wins[want]++
			for order, rows := range [][]string{{live, retraction}, {retraction, live}} {
				env.reset(t)
				env.exec(t, rows[0])
				env.exec(t, rows[1])
				if got := env.latency(t, tieRepo); got != want {
					t.Errorf("k=%d order %d: latency = %s, want %s (served pick)", k, order, got, want)
				}
			}
		}
		t.Logf("served pick: %v", wins)
		if wins["absent"] == 0 || wins["40h"] == 0 {
			t.Fatalf("fixture: the served order must pick each version at least once: %v", wins)
		}
	})
}
