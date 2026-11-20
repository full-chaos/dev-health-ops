//go:build integration

package aianalytics

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// Two versions of one key that share computed_at serve ONE version whatever the scan order, every column of a read
// comes from that one version, and a version with a newer computed_at still wins. Versions go in as separate INSERT
// statements (same-key rows of ONE block collapse under ReplacingMergeTree) into a table with merges stopped.
// CHAOS-14355.

// versionsServed runs both insert orders of the versions (3 times each) on a truncated table and returns the distinct
// served values.
func versionsServed(t *testing.T, ctx context.Context, conn chdriver.Conn, table, columns string, versions [2]string, read func() string) []string {
	t.Helper()
	exec(t, ctx, conn, `SYSTEM STOP MERGES %s`, table)
	seen := map[string]bool{}
	for _, order := range [][2]int{{0, 1}, {1, 0}} {
		for range 3 {
			exec(t, ctx, conn, `TRUNCATE TABLE %s`, table)
			for _, i := range order {
				exec(t, ctx, conn, `INSERT INTO %s (%s) %s`, table, columns, versions[i])
			}
			seen[read()] = true
		}
	}
	out := make([]string, 0, len(seen))
	for v := range seen {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

func fp(p *float64) string {
	if p == nil {
		return "NULL"
	}
	return fmt.Sprint(*p)
}

func requireOne(t *testing.T, name string, served []string) {
	t.Helper()
	if len(served) != 1 {
		t.Errorf("%s: same key, same computed_at, served %d values over both insert orders: %s; want one", name, len(served), strings.Join(served, " | "))
	}
}

func requireServed(t *testing.T, name string, served []string, want string) {
	t.Helper()
	if len(served) != 1 || served[0] != want {
		t.Errorf("%s: served %s over both insert orders; want only %s", name, strings.Join(served, " | "), want)
	}
}

// impactColumns are the value columns dailyStatement reads, in its order.
var impactColumns = []string{
	"prs_total", "prs_merged", "ai_assisted_prs", "agent_created_prs", "human_prs", "unknown_prs",
	"ai_assisted_pr_ratio", "cycle_time_avg_hours", "ai_cycle_time_delta_hours", "reviews_per_pr",
	"ai_review_amplification", "changes_requested_per_pr", "rework_prs", "rework_drag_rate",
	"followup_commits_count", "revert_prs", "revert_rate", "incidents_count", "incident_drag_rate",
	"test_gap_prs", "test_gap_rate", "leverage_prs_component", "leverage_cycle_time_component",
	"leverage_review_component", "leverage_rework_component", "leverage_test_component",
	"leverage_incident_component",
}

func dailyValues(r dailyRow) []string {
	f := func(v float64) string { return fmt.Sprint(v) }
	return []string{
		f(r.PrsTotal), f(r.PrsMerged), f(r.AIAssistedPrs), f(r.AgentCreatedPrs), f(r.HumanPrs), f(r.UnknownPrs),
		fp(r.AIAssistedPrRatio), fp(r.CycleTimeAvgHours), fp(r.AICycleTimeDelta), fp(r.ReviewsPerPr),
		fp(r.AIReviewAmp), fp(r.ChangesRequestedPer), f(r.ReworkPrs), fp(r.ReworkDragRate),
		f(r.FollowupCommits), f(r.RevertPrs), fp(r.RevertRate), f(r.IncidentsCount), fp(r.IncidentDragRate),
		f(r.TestGapPrs), fp(r.TestGapRate), f(r.LevPrs), fp(r.LevCycle),
		fp(r.LevReview), fp(r.LevRework), fp(r.LevTest), fp(r.LevIncident),
	}
}

func TestArgMaxTieImpact14355(t *testing.T) {
	ctx, conn, client := startStore(t)
	const table = "ai_impact_metrics_daily"
	const key = `org_id, team_id, repo_id, work_type, day, attribution_bucket`
	keyValues := fmt.Sprintf(`'%s', 'team-a', '%s', 'pull_request', today() - 2, 'ai_assisted'`, org1, rA)
	const at = "2026-08-02 00:00:00.000"
	const newer = "2026-08-02 00:00:00.001"
	// v writes the key with the given columns; the columns not named keep their default (0 or NULL).
	v := func(at string, cols map[string]string) (string, string) {
		names, values := []string{key, "computed_at"}, []string{keyValues, fmt.Sprintf("toDateTime64('%s', 3, 'UTC')", at)}
		for _, c := range impactColumns {
			if val, ok := cols[c]; ok {
				names, values = append(names, c), append(values, val)
			}
		}
		return strings.Join(names, ", "), "SELECT " + strings.Join(values, ", ")
	}
	start, end := time.Now().UTC().AddDate(0, 0, -5), time.Now().UTC().AddDate(0, 0, 1)
	daily := func() []dailyRow {
		rows, err := loadDaily(ctx, client, org1, start, end, scope{})
		if err != nil {
			t.Fatal(err)
		}
		return rows
	}
	readDaily := func() string {
		var parts []string
		for _, r := range daily() {
			parts = append(parts, fmt.Sprintf("prs=%v merged=%v reviews=%s", r.PrsTotal, r.PrsMerged, fp(r.ReviewsPerPr)))
		}
		return strings.Join(parts, ";")
	}
	readOpportunity := func() string {
		rows, err := loadImpactRowsForOpportunities(ctx, client, org1, "", "")
		if err != nil {
			t.Fatal(err)
		}
		var parts []string
		for _, r := range rows {
			parts = append(parts, fmt.Sprintf("prs=%v reviews=%s", r.PrsTotal, fp(r.ReviewsPer)))
		}
		return strings.Join(parts, ";")
	}
	pair := func(a, b map[string]string, atA, atB string) (string, [2]string) {
		colsA, va := v(atA, a)
		colsB, vb := v(atB, b)
		if colsA != colsB {
			t.Fatalf("versions name different columns: %s / %s", colsA, colsB)
		}
		return colsA, [2]string{va, vb}
	}

	t.Run("tie serves one version in both reads", func(t *testing.T) {
		cols, versions := pair(map[string]string{"prs_total": "1", "reviews_per_pr": "1.5"}, map[string]string{"prs_total": "2", "reviews_per_pr": "NULL"}, at, at)
		requireOne(t, "loaders.go dailyStatement", versionsServed(t, ctx, conn, table, cols, versions, readDaily))
		requireOne(t, "ai_opportunities.go loadImpactRowsForOpportunities", versionsServed(t, ctx, conn, table, cols, versions, readOpportunity))
	})

	t.Run("on a tie dailyStatement and the opportunity read serve the same version", func(t *testing.T) {
		for k := 1; k <= 8; k++ {
			cols, versions := pair(
				map[string]string{"prs_total": fmt.Sprint(k), "prs_merged": fmt.Sprint(100 + k), "reviews_per_pr": fmt.Sprintf("%d.5", k)},
				map[string]string{"prs_total": fmt.Sprint(k + 50), "prs_merged": fmt.Sprint(200 + k), "reviews_per_pr": "NULL"},
				at, at)
			d := versionsServed(t, ctx, conn, table, cols, versions, func() string {
				rows := daily()
				if len(rows) != 1 {
					t.Fatalf("daily rows %d", len(rows))
				}
				return fmt.Sprintf("prs=%v reviews=%s", rows[0].PrsTotal, fp(rows[0].ReviewsPerPr))
			})
			o := versionsServed(t, ctx, conn, table, cols, versions, readOpportunity)
			if len(d) != 1 || len(o) != 1 || d[0] != o[0] {
				t.Errorf("pair %d: dailyStatement served %v, the opportunity read served %v; want one and the same version", k, d, o)
			}
		}
	})

	t.Run("every column comes from one version", func(t *testing.T) {
		ones, twos := map[string]string{}, map[string]string{}
		for _, c := range impactColumns {
			ones[c], twos[c] = "1", "2"
		}
		cols, versions := pair(ones, twos, at, at)
		served := versionsServed(t, ctx, conn, table, cols, versions, func() string {
			rows := daily()
			if len(rows) != 1 {
				t.Fatalf("daily rows %d", len(rows))
			}
			return strings.Join(dailyValues(rows[0]), ",")
		})
		for _, s := range served {
			if s != strings.Repeat("1,", 26)+"1" && s != strings.Repeat("2,", 26)+"2" {
				t.Errorf("served a mixed row: %s", s)
			}
		}
		requireOne(t, "every column", served)
		requireOne(t, "opportunity read, every column", versionsServed(t, ctx, conn, table, cols, versions, readOpportunity))
	})

	t.Run("each column is read from its own place in the tuple", func(t *testing.T) {
		distinct := map[string]string{}
		var want []string
		for i, c := range impactColumns {
			distinct[c] = fmt.Sprint(i + 1)
			want = append(want, fmt.Sprint(i+1))
		}
		cols, versions := pair(distinct, distinct, at, at)
		requireServed(t, "column order", versionsServed(t, ctx, conn, table, cols, versions, func() string {
			rows := daily()
			if len(rows) != 1 {
				t.Fatalf("daily rows %d", len(rows))
			}
			return strings.Join(dailyValues(rows[0]), ",")
		}), strings.Join(want, ","))
	})

	t.Run("a newer computed_at wins over the tiebreak", func(t *testing.T) {
		for _, c := range []struct{ older, newer string }{{"1", "2"}, {"2", "1"}} {
			cols, versions := pair(map[string]string{"prs_total": c.older, "reviews_per_pr": "1.5"}, map[string]string{"prs_total": c.newer, "reviews_per_pr": "2.5"}, at, newer)
			requireServed(t, "dailyStatement older "+c.older+" newer "+c.newer, versionsServed(t, ctx, conn, table, cols, versions, readDaily), "prs="+c.newer+" merged=0 reviews=2.5")
			requireServed(t, "opportunity read older "+c.older+" newer "+c.newer, versionsServed(t, ctx, conn, table, cols, versions, readOpportunity), "prs="+c.newer+" reviews=2.5")
		}
	})

	t.Run("a NULL of the newest version stays NULL", func(t *testing.T) {
		cols, versions := pair(map[string]string{"prs_total": "1", "reviews_per_pr": "1.5"}, map[string]string{"prs_total": "1", "reviews_per_pr": "NULL"}, at, newer)
		requireServed(t, "dailyStatement older 1.5 newer NULL", versionsServed(t, ctx, conn, table, cols, versions, readDaily), "prs=1 merged=0 reviews=NULL")
		requireServed(t, "opportunity read older 1.5 newer NULL", versionsServed(t, ctx, conn, table, cols, versions, readOpportunity), "prs=1 reviews=NULL")
		cols, versions = pair(map[string]string{"prs_total": "1", "reviews_per_pr": "NULL"}, map[string]string{"prs_total": "1", "reviews_per_pr": "1.5"}, at, newer)
		requireServed(t, "dailyStatement older NULL newer 1.5", versionsServed(t, ctx, conn, table, cols, versions, readDaily), "prs=1 merged=0 reviews=1.5")
		requireServed(t, "opportunity read older NULL newer 1.5", versionsServed(t, ctx, conn, table, cols, versions, readOpportunity), "prs=1 reviews=1.5")
	})
}

func coverageText(rows []model.AIGovernanceCoverageRow) string {
	text := func(p *string) string {
		if p == nil {
			return "NULL"
		}
		return *p
	}
	var parts []string
	for _, r := range rows {
		parts = append(parts, fmt.Sprintf("%s %s %s ai=%d declared=%d reviewed=%d scanned=%d policy=%d cov=%v/%v/%v/%v",
			r.Day.Time().Format("2006-01-02"), text(r.TeamID), text(r.RepoID), r.AiArtifacts, r.DeclaredArtifacts, r.HumanReviewedPrs,
			r.SecurityScannedPrs, r.InPolicyArtifacts, r.DeclarationCoverage, r.HumanReviewCoverage, r.SecurityScanCoverage, r.InPolicyCoverage))
	}
	return strings.Join(parts, "\n")
}

func TestArgMaxTieGovernanceCoverage14355(t *testing.T) {
	ctx, conn, client := startStore(t)
	const table = "ai_governance_coverage_daily"
	const cols = `org_id, team_id, repo_id, day, ai_artifacts, declared_artifacts, human_reviewed_prs, security_scanned_prs, in_policy_artifacts, computed_at`
	const at = "2026-08-02 00:00:00.000"
	const newer = "2026-08-02 00:00:00.001"
	v := func(at string, counts [5]int) string {
		return fmt.Sprintf(`SELECT '%s', 'team-a', toUUID('%s'), toDate('2026-08-01'), %d, %d, %d, %d, %d, toDateTime64('%s', 3, 'UTC')`,
			org1, rA, counts[0], counts[1], counts[2], counts[3], counts[4], at)
	}
	read := func() string {
		rows, err := loadCoverage(ctx, client, org1, dateRange(), scope{})
		if err != nil {
			t.Fatal(err)
		}
		var parts []string
		for _, r := range rows {
			parts = append(parts, fmt.Sprintf("%d,%d,%d,%d,%d", r.AiArtifacts, r.DeclaredArtifacts, r.HumanReviewedPrs, r.SecurityScannedPrs, r.InPolicyArtifacts))
		}
		return strings.Join(parts, ";")
	}

	t.Run("tie serves one version", func(t *testing.T) {
		requireOne(t, "governance.go coverageStatement", versionsServed(t, ctx, conn, table, cols, [2]string{v(at, [5]int{4, 1, 0, 0, 0}), v(at, [5]int{4, 3, 0, 0, 0})}, read))
	})

	t.Run("every count comes from one version", func(t *testing.T) {
		served := versionsServed(t, ctx, conn, table, cols, [2]string{v(at, [5]int{1, 1, 1, 1, 1}), v(at, [5]int{2, 2, 2, 2, 2})}, read)
		for _, s := range served {
			if s != "1,1,1,1,1" && s != "2,2,2,2,2" {
				t.Errorf("served a mixed row: %s", s)
			}
		}
		requireOne(t, "every count", served)
	})

	t.Run("a tie of two live versions keeps the key", func(t *testing.T) {
		// A pick per column could take ai from the second and declared from the first: all zeros, a retraction row.
		served := versionsServed(t, ctx, conn, table, cols, [2]string{v(at, [5]int{1, 0, 0, 0, 0}), v(at, [5]int{0, 1, 0, 0, 0})}, read)
		requireOne(t, "two live versions", served)
		for _, s := range served {
			if s != "1,0,0,0,0" && s != "0,1,0,0,0" {
				t.Errorf("two live versions served %q; want one of them", s)
			}
		}
	})

	t.Run("a tie of a live version and a row of zeros: liveness and counts from one version", func(t *testing.T) {
		served := versionsServed(t, ctx, conn, table, cols, [2]string{v(at, [5]int{4, 1, 0, 0, 0}), v(at, [5]int{0, 0, 0, 0, 0})}, read)
		requireOne(t, "live + zeros", served)
		for _, s := range served {
			if s != "" && s != "4,1,0,0,0" {
				t.Errorf("served %q; want the live version or no row", s)
			}
		}
	})

	t.Run("a newer computed_at wins over the tiebreak", func(t *testing.T) {
		requireServed(t, "older 1 newer 3", versionsServed(t, ctx, conn, table, cols, [2]string{v(at, [5]int{4, 1, 0, 0, 0}), v(newer, [5]int{4, 3, 0, 0, 0})}, read), "4,3,0,0,0")
		requireServed(t, "older 3 newer 1", versionsServed(t, ctx, conn, table, cols, [2]string{v(at, [5]int{4, 3, 0, 0, 0}), v(newer, [5]int{4, 1, 0, 0, 0})}, read), "4,1,0,0,0")
		requireServed(t, "newer row of zeros retires the key", versionsServed(t, ctx, conn, table, cols, [2]string{v(at, [5]int{4, 3, 0, 0, 0}), v(newer, [5]int{0, 0, 0, 0, 0})}, read), "")
	})

	t.Run("no tie: rows as main serves them", func(t *testing.T) {
		exec(t, ctx, conn, `TRUNCATE TABLE %s`, table)
		for _, s := range []string{
			// (team-a, rA, 08-01): an older version loses.
			fmt.Sprintf(`SELECT '%s', 'team-a', toUUID('%s'), toDate('2026-08-01'), 9, 9, 9, 9, 9, toDateTime64('2026-08-01 00:00:00', 3, 'UTC')`, org1, rA),
			fmt.Sprintf(`SELECT '%s', 'team-a', toUUID('%s'), toDate('2026-08-01'), 4, 2, 1, 3, 4, toDateTime64('2026-08-02 00:00:00', 3, 'UTC')`, org1, rA),
			// (team-a, rA, 08-02): a newer row of zeros retires the key.
			fmt.Sprintf(`SELECT '%s', 'team-a', toUUID('%s'), toDate('2026-08-02'), 5, 5, 5, 5, 5, toDateTime64('2026-08-02 00:00:00', 3, 'UTC')`, org1, rA),
			fmt.Sprintf(`SELECT '%s', 'team-a', toUUID('%s'), toDate('2026-08-02'), 0, 0, 0, 0, 0, toDateTime64('2026-08-03 00:00:00', 3, 'UTC')`, org1, rA),
			// no team, no repository
			fmt.Sprintf(`SELECT '%s', NULL, NULL, toDate('2026-08-03'), 2, 1, 2, 0, 1, toDateTime64('2026-08-03 00:00:00', 3, 'UTC')`, org1),
			// a second team and repository, one version
			fmt.Sprintf(`SELECT '%s', 'team-b', toUUID('%s'), toDate('2026-08-02'), 3, 0, 3, 1, 2, toDateTime64('2026-08-02 00:00:00', 3, 'UTC')`, org1, rB),
			// a key whose only version holds zeros
			fmt.Sprintf(`SELECT '%s', 'team-b', toUUID('%s'), toDate('2026-08-03'), 0, 0, 0, 0, 0, toDateTime64('2026-08-03 00:00:00', 3, 'UTC')`, org1, rB),
			// outside the window, and the other org on the same key
			fmt.Sprintf(`SELECT '%s', 'team-a', toUUID('%s'), toDate('2026-07-31'), 7, 7, 7, 7, 7, toDateTime64('2026-08-01 00:00:00', 3, 'UTC')`, org1, rA),
			fmt.Sprintf(`SELECT '%s', 'team-a', toUUID('%s'), toDate('2026-08-01'), 1000, 1, 1, 1, 1, toDateTime64('2026-08-05 00:00:00', 3, 'UTC')`, org2, rA),
		} {
			exec(t, ctx, conn, `INSERT INTO %s (%s) %s`, table, cols, s)
		}
		team, repo := "team-a", rA
		for _, c := range []struct {
			name string
			sc   scope
			want string
		}{
			{"whole org", scope{}, strings.Join([]string{
				"2026-08-01 team-a " + rA + " ai=4 declared=2 reviewed=1 scanned=3 policy=4 cov=0.5/0.25/0.75/1",
				"2026-08-02 team-b " + rB + " ai=3 declared=0 reviewed=3 scanned=1 policy=2 cov=0/1/0.3333333333333333/0.6666666666666666",
				"2026-08-03 NULL NULL ai=2 declared=1 reviewed=2 scanned=0 policy=1 cov=0.5/1/0/0.5",
			}, "\n")},
			{"team", scope{teamID: team}, "2026-08-01 team-a " + rA + " ai=4 declared=2 reviewed=1 scanned=3 policy=4 cov=0.5/0.25/0.75/1"},
			{"repository", scope{repoID: repo}, "2026-08-01 team-a " + rA + " ai=4 declared=2 reviewed=1 scanned=3 policy=4 cov=0.5/0.25/0.75/1"},
		} {
			rows, err := loadCoverage(ctx, client, org1, dateRange(), c.sc)
			if err != nil {
				t.Fatal(err)
			}
			if got := coverageText(rows); got != c.want {
				t.Errorf("%s:\n got %s\nwant %s", c.name, got, c.want)
			}
		}
	})
}

func TestArgMaxTieReviewerConcentration14355(t *testing.T) {
	ctx, conn, client := startStore(t)
	seedStore(t, ctx, conn)
	const table = "user_metrics_daily"
	const cols = `repo_id, day, author_email, reviews_given, retracted, org_id, computed_at`
	v := func(who string, n, retracted int, at string) string {
		return fmt.Sprintf(`SELECT '%s', toDate('2026-08-01'), '%s', %d, %d, '%s', toDateTime('%s', 'UTC')`, rA, who, n, retracted, org1, at)
	}
	const at = "2026-08-02 00:00:00"
	const newer = "2026-08-02 00:00:01"
	start, _ := time.Parse("2006-01-02", "2026-08-01")
	end, _ := time.Parse("2006-01-02", "2026-08-03")
	read := func() string {
		g, n, err := loadReviewerConcentration(ctx, client, org1, start, end, scope{}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return fmt.Sprintf("gini=%s reviewers=%d", fp(g), n)
	}
	// bob's one version rides in each insert of alice's versions: different keys never collapse.
	both := func(alice string) string { return alice + " UNION ALL " + v("bob@x", 5, 0, at) }

	t.Run("tie serves one version", func(t *testing.T) {
		requireOne(t, "loaders.go concentrationStatement", versionsServed(t, ctx, conn, table, cols, [2]string{both(v("alice@x", 1, 0, at)), both(v("alice@x", 9, 0, at))}, read))
	})

	t.Run("a tie of a measurement and a retraction row: liveness and count from one version", func(t *testing.T) {
		served := versionsServed(t, ctx, conn, table, cols, [2]string{both(v("alice@x", 5, 0, at)), both(v("alice@x", 0, 1, at))}, read)
		requireOne(t, "measurement + retraction", served)
		for _, s := range served {
			// alice counted with 5 (gini 0 over 2) or left out (gini 0 over 1); never counted with the retraction's 0.
			if s != "gini=0 reviewers=2" && s != "gini=0 reviewers=1" {
				t.Errorf("served %q; want alice's measurement or no alice", s)
			}
		}
	})

	t.Run("a newer computed_at wins over the tiebreak", func(t *testing.T) {
		requireServed(t, "older 1 newer 9", versionsServed(t, ctx, conn, table, cols, [2]string{both(v("alice@x", 1, 0, at)), both(v("alice@x", 9, 0, newer))}, read), "gini=0.1428571428571428 reviewers=2")
		requireServed(t, "older 9 newer 1", versionsServed(t, ctx, conn, table, cols, [2]string{both(v("alice@x", 9, 0, at)), both(v("alice@x", 1, 0, newer))}, read), "gini=0.33333333333333326 reviewers=2")
		requireServed(t, "newer retraction row retires alice", versionsServed(t, ctx, conn, table, cols, [2]string{both(v("alice@x", 9, 0, at)), both(v("alice@x", 0, 1, newer))}, read), "gini=0 reviewers=1")
	})

	t.Run("no tie: the value main serves", func(t *testing.T) {
		exec(t, ctx, conn, `TRUNCATE TABLE %s`, table)
		for _, s := range []string{
			v("alice@x", 3, 0, "2026-08-02 00:00:00"),
			v("bob@x", 1, 0, "2026-08-02 00:00:00"),
			v("carol@x", 50, 0, "2026-08-01 00:00:00"),
			v("carol@x", 2, 0, "2026-08-02 00:00:00"),
			v("dave@x", 7, 0, "2026-08-02 00:00:00"),
			v("dave@x", 0, 1, "2026-08-03 00:00:00"),
			fmt.Sprintf(`SELECT '%s', toDate('2026-08-02'), 'alice@x', 4, 0, '%s', toDateTime('2026-08-02 00:00:00', 'UTC')`, rA, org1),
			fmt.Sprintf(`SELECT '%s', toDate('2026-08-01'), 'eve@x', 9999, 0, '%s', toDateTime('2026-08-02 00:00:00', 'UTC')`, rA, org2),
		} {
			exec(t, ctx, conn, `INSERT INTO %s (%s) %s`, table, cols, s)
		}
		if got, want := read(), "gini=0.40000000000000013 reviewers=3"; got != want {
			t.Errorf("got %s, want %s", got, want)
		}
	})
}

func TestArgMaxTieHotspotOverlap14355(t *testing.T) {
	ctx, conn, client := startStore(t)
	seedStore(t, ctx, conn)
	seedGroupB(t, ctx, conn)
	const table = "file_hotspot_daily"
	const cols = `repo_id, day, file_path, risk_score, org_id, computed_at`
	v := func(day string, risk float64, at string) string {
		return fmt.Sprintf(`SELECT '%s', toDate('%s'), 'a.go', %v, '%s', toDateTime('%s', 'UTC')`, rA, day, risk, org1, at)
	}
	const at = "2026-08-02 00:00:00"
	const newer = "2026-08-02 00:00:01"
	start, _ := time.Parse("2006-01-02", "2026-08-01")
	end, _ := time.Parse("2006-01-02", "2026-08-03")
	read := func() string {
		rows, err := loadOverlap(ctx, client, org1, start, end, "", nil, false, true)
		if err != nil {
			t.Fatal(err)
		}
		var parts []string
		for _, r := range rows {
			parts = append(parts, fmt.Sprintf("%s touching=%d risk=%s", r.Bucket, r.PrsTouching, fp(r.AvgRisk)))
		}
		return strings.Join(parts, ";")
	}
	served := func(risk string) string { return "ai_assisted touching=1 risk=" + risk }
	requireOne(t, "loaders_b.go hotspotOverlapTail same day", versionsServed(t, ctx, conn, table, cols, [2]string{v("2026-08-01", 5, at), v("2026-08-01", 7, at)}, read))
	requireServed(t, "two days, one computed_at: newer day (7)", versionsServed(t, ctx, conn, table, cols, [2]string{v("2026-08-01", 5, at), v("2026-08-02", 7, at)}, read), served("7"))
	requireServed(t, "two days, one computed_at: newer day (5)", versionsServed(t, ctx, conn, table, cols, [2]string{v("2026-08-01", 7, at), v("2026-08-02", 5, at)}, read), served("5"))
	requireServed(t, "newer computed_at beats newer day (5)", versionsServed(t, ctx, conn, table, cols, [2]string{v("2026-08-01", 5, newer), v("2026-08-02", 7, at)}, read), served("5"))
	requireServed(t, "newer computed_at beats newer day (7)", versionsServed(t, ctx, conn, table, cols, [2]string{v("2026-08-01", 7, newer), v("2026-08-02", 5, at)}, read), served("7"))
	requireServed(t, "same day, newer computed_at (5)", versionsServed(t, ctx, conn, table, cols, [2]string{v("2026-08-01", 7, at), v("2026-08-01", 5, newer)}, read), served("5"))
	requireServed(t, "same day, newer computed_at (7)", versionsServed(t, ctx, conn, table, cols, [2]string{v("2026-08-01", 5, at), v("2026-08-01", 7, newer)}, read), served("7"))
}

func TestArgMaxTieComplexityOverlap14355(t *testing.T) {
	ctx, conn, client := startStore(t)
	seedStore(t, ctx, conn)
	seedGroupB(t, ctx, conn)
	const table = "file_complexity_snapshots"
	const cols = `repo_id, as_of_day, ref, file_path, high_complexity_functions, org_id, computed_at`
	v := func(day string, high int, at string) string {
		return fmt.Sprintf(`SELECT '%s', toDate('%s'), 'main', 'a.go', %d, '%s', toDateTime('%s', 'UTC')`, rA, day, high, org1, at)
	}
	const at = "2026-08-02 00:00:00"
	const newer = "2026-08-02 00:00:01"
	start, _ := time.Parse("2006-01-02", "2026-08-01")
	end, _ := time.Parse("2006-01-02", "2026-08-03")
	read := func() string {
		rows, err := loadOverlap(ctx, client, org1, start, end, "", nil, false, false)
		if err != nil {
			t.Fatal(err)
		}
		var parts []string
		for _, r := range rows {
			parts = append(parts, fmt.Sprintf("%s touching=%d", r.Bucket, r.PrsTouching))
		}
		return strings.Join(parts, ";")
	}
	const high, low = "ai_assisted touching=1", "ai_assisted touching=0"
	requireOne(t, "loaders_b.go complexityOverlapTail same day", versionsServed(t, ctx, conn, table, cols, [2]string{v("2026-08-01", 2, at), v("2026-08-01", 0, at)}, read))
	requireServed(t, "two days, one computed_at: newer day (0)", versionsServed(t, ctx, conn, table, cols, [2]string{v("2026-08-01", 2, at), v("2026-08-02", 0, at)}, read), low)
	requireServed(t, "two days, one computed_at: newer day (2)", versionsServed(t, ctx, conn, table, cols, [2]string{v("2026-08-01", 0, at), v("2026-08-02", 2, at)}, read), high)
	requireServed(t, "newer computed_at beats newer day (2)", versionsServed(t, ctx, conn, table, cols, [2]string{v("2026-08-01", 2, newer), v("2026-08-02", 0, at)}, read), high)
	requireServed(t, "newer computed_at beats newer day (0)", versionsServed(t, ctx, conn, table, cols, [2]string{v("2026-08-01", 0, newer), v("2026-08-02", 2, at)}, read), low)
	requireServed(t, "same day, newer computed_at (0)", versionsServed(t, ctx, conn, table, cols, [2]string{v("2026-08-01", 2, at), v("2026-08-01", 0, newer)}, read), low)
	requireServed(t, "same day, newer computed_at (2)", versionsServed(t, ctx, conn, table, cols, [2]string{v("2026-08-01", 0, at), v("2026-08-01", 2, newer)}, read), high)
}
