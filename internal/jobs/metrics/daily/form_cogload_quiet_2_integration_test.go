//go:build integration

package daily

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/opfixture"
)

// formCogloadQuiet2 is the synthetic copy of a read-only ClickHouse FORM (four
// statements: stmtR, S0, ORG1, Q1): comment lines removed and the org prefix
// swapped to f0f0f0f0, every date shifted by -252 days. The cells below execute every statement on the migrated
// head schema (opfixture), fire every cost gate, and kill each org / rule mutant. The test
// server is one node, so stmtR proves only that the statement runs and reads 1.
const formCogloadQuiet2 = `
SELECT count() AS replicas FROM clusterAllReplicas('default', system.one);

SELECT table, sum(rows) AS rows, sum(bytes_on_disk) AS bytes, count() AS active_parts, toString(min(min_date)) AS min_day, toString(max(max_date)) AS max_day
FROM system.parts
WHERE active AND database = currentDatabase()
  AND table IN ('user_metrics_daily', 'team_metrics_daily', 'team_cognitive_load_daily', 'daily_metrics_run_marker')
  AND max_date >= toDate('2026-02-16') AND min_date <= toDate('2026-03-17')
GROUP BY table ORDER BY table;

WITH (SELECT throwIf(
  (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table = 'repos') > 5000000
  OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table = 'repos') > 5000000000,
  'STOP-COST-GATE')) AS cost_gate
SELECT uniqExact(org_id) AS orgs FROM repos WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0;

WITH (SELECT throwIf(
  (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('user_metrics_daily', 'team_metrics_daily', 'team_cognitive_load_daily', 'daily_metrics_run_marker') AND max_date >= toDate('2026-02-16') AND min_date <= toDate('2026-03-17')) > 9000000
  OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('user_metrics_daily', 'team_metrics_daily', 'team_cognitive_load_daily', 'daily_metrics_run_marker') AND max_date >= toDate('2026-02-16') AND min_date <= toDate('2026-03-17')) > 4294967296,
  'STOP-COST-GATE')) AS cost_gate
SELECT scope,
       count()                                                                    AS days,
       sum(has_u)                                                                 AS d_umd,
       sum(has_t)                                                                 AS d_tmd,
       sum(has_c)                                                                 AS d_tcl,
       sum(u_live)                                                                AS d_umd_live,
       sum(t_live)                                                                AS d_tmd_live,
       sum(c_live)                                                                AS d_tcl_live,
       countIf(mstate = 'succeeded')                                              AS d_cert,
       countIf(mstate = 'reopened')                                               AS d_reopened,
       countIf(mstate = '')                                                       AS d_nomarker,
       countIf(mstate NOT IN ('', 'succeeded', 'reopened'))                       AS d_badstate,
       countIf(u_live = 1)                                                        AS c_dev,
       countIf(u_live = 1 AND mstate != 'succeeded')                              AS c_dev_uncert,
       countIf(u_live = 0 AND (t_live = 1 OR c_live = 1))                         AS c_teamonly,
       countIf(u_live = 0 AND (t_live = 1 OR c_live = 1) AND mstate = 'succeeded')  AS c_teamonly_cert,
       countIf(u_live = 0 AND (t_live = 1 OR c_live = 1) AND mstate != 'succeeded') AS c_teamonly_uncert,
       countIf(u_live = 0 AND t_live = 1)                                         AS c_tmd_nodev,
       countIf(u_live = 0 AND t_live = 0 AND c_live = 0)                          AS c_norow,
       countIf(u_live = 0 AND t_live = 0 AND c_live = 0 AND mstate = 'succeeded') AS c_norow_cert,
       countIf(u_live = 0 AND t_live = 0 AND c_live = 0 AND mstate = 'reopened')  AS c_norow_reopened,
       countIf(u_live = 0 AND t_live = 0 AND c_live = 0 AND mstate = '')          AS c_norow_nomarker,
       countIf(mstate = 'succeeded' AND mfin < toDateTime64(addDays(day, 1), 3, 'UTC')) AS c_cert_before_day_end,
       countIf(has_u = 1 AND u_live = 0)                                          AS c_umd_retracted_only,
       countIf(has_t = 1 AND t_live = 0)                                          AS c_tmd_retracted_only,
       countIf(has_c = 1 AND c_live = 0)                                          AS c_tcl_retracted_only,
       countIf(c_live = 1 AND c_auth = 0)                                         AS c_tcl_authorless,
       greatest(max(u_orgs), max(t_orgs), max(c_orgs), max(m_orgs))              AS orgs_max
FROM (
    SELECT s.day                     AS day,
           ifNull(u.has_u, 0)        AS has_u,
           ifNull(u.u_live, 0)       AS u_live,
           ifNull(u.orgs, 0)         AS u_orgs,
           ifNull(t.has_t, 0)        AS has_t,
           ifNull(t.t_live, 0)       AS t_live,
           ifNull(t.orgs, 0)         AS t_orgs,
           ifNull(c.has_c, 0)        AS has_c,
           ifNull(c.c_live, 0)       AS c_live,
           ifNull(c.c_auth, 0)       AS c_auth,
           ifNull(c.orgs, 0)         AS c_orgs,
           ifNull(m.mstate, '')      AS mstate,
           ifNull(m.mfin, toDateTime64(0, 3, 'UTC')) AS mfin,
           ifNull(m.orgs, 0)         AS m_orgs
    FROM (SELECT addDays(toDate('2026-02-16'), number) AS day FROM numbers(30)) AS s
    LEFT JOIN (
        SELECT day, toUInt8(1) AS has_u, toUInt8(countIf(live) > 0) AS u_live, uniqExact(org_id) AS orgs
        FROM (
            SELECT org_id, day, repo_id, author_email, argMax(user_metrics_daily.retracted, user_metrics_daily.computed_at) = 0 AS live
            FROM user_metrics_daily
            WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0 AND day >= toDate('2026-02-16') AND day <= toDate('2026-03-17')
            GROUP BY org_id, day, repo_id, author_email
        )
        GROUP BY day
    ) AS u ON u.day = s.day
    LEFT JOIN (
        SELECT day, toUInt8(1) AS has_t, toUInt8(countIf(measured) > 0) AS t_live, uniqExact(org_id) AS orgs
        FROM (
            SELECT org_id, day, team_id, max(live) = 1 AS measured
            FROM (
                SELECT org_id, day, team_id, repo_id, live
                FROM (
                    SELECT org_id, day, team_id, repo_id,
                           (newest.commits_count != 0 OR newest.after_hours_commits_count != 0 OR newest.weekend_commits_count != 0
                            OR newest.after_hours_commit_ratio != 0 OR newest.weekend_commit_ratio != 0) AS live,
                           countIf(repo_id != '') OVER (PARTITION BY org_id, day, team_id) AS real_repo_count
                    FROM (
                        SELECT org_id, day, team_id, repo_id,
                               argMax(tuple(team_metrics_daily.commits_count, team_metrics_daily.after_hours_commits_count,
                                   team_metrics_daily.weekend_commits_count, team_metrics_daily.after_hours_commit_ratio,
                                   team_metrics_daily.weekend_commit_ratio),
                                   (team_metrics_daily.computed_at, cityHash64(tuple(team_metrics_daily.commits_count,
                                   team_metrics_daily.after_hours_commits_count, team_metrics_daily.weekend_commits_count,
                                   team_metrics_daily.after_hours_commit_ratio, team_metrics_daily.weekend_commit_ratio)))) AS picked,
                               picked.1 AS commits_count,
                               picked.2 AS after_hours_commits_count,
                               picked.3 AS weekend_commits_count,
                               picked.4 AS after_hours_commit_ratio,
                               picked.5 AS weekend_commit_ratio
                        FROM team_metrics_daily
                        WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0 AND day >= toDate('2026-02-16') AND day <= toDate('2026-03-17')
                        GROUP BY org_id, day, team_id, repo_id
                    ) AS newest
                )
                WHERE repo_id != '' OR real_repo_count = 0
            )
            GROUP BY org_id, day, team_id
        )
        GROUP BY day
    ) AS t ON t.day = s.day
    LEFT JOIN (
        SELECT day, toUInt8(1) AS has_c, toUInt8(countIf(live) > 0) AS c_live, toUInt8(countIf(live AND sac > 0) > 0) AS c_auth,
               uniqExact(org_id) AS orgs
        FROM (
            SELECT org_id, team_id, day,
                   argMax(team_cognitive_load_daily.sample_author_count, team_cognitive_load_daily.computed_at) AS sac,
                   (argMax(team_cognitive_load_daily.pr_interruption_load, team_cognitive_load_daily.computed_at) != 0
                    OR argMax(team_cognitive_load_daily.context_spread_count, team_cognitive_load_daily.computed_at) != 0
                    OR argMax(team_cognitive_load_daily.review_request_load, team_cognitive_load_daily.computed_at) != 0
                    OR argMax(team_cognitive_load_daily.contributing_repo_count, team_cognitive_load_daily.computed_at) != 0
                    OR argMax(team_cognitive_load_daily.sample_author_count, team_cognitive_load_daily.computed_at) != 0
                    OR isNotNull(tupleElement(argMax(tuple(team_cognitive_load_daily.after_hours_commit_ratio), team_cognitive_load_daily.computed_at), 1))
                    OR isNotNull(tupleElement(argMax(tuple(team_cognitive_load_daily.weekend_commit_ratio), team_cognitive_load_daily.computed_at), 1))) AS live
            FROM team_cognitive_load_daily
            WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0 AND day >= toDate('2026-02-16') AND day <= toDate('2026-03-17')
            GROUP BY org_id, team_id, day
        )
        GROUP BY day
    ) AS c ON c.day = s.day
    LEFT JOIN (
        SELECT target_day AS day,
               argMax(state, (version, state = 'reopened'))        AS mstate,
               argMax(finalized_at, (version, state = 'reopened')) AS mfin,
               uniqExact(org_id)                                   AS orgs
        FROM daily_metrics_run_marker
        WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0 AND target_day >= toDate('2026-02-16') AND target_day <= toDate('2026-03-17')
        GROUP BY target_day
    ) AS m ON m.day = s.day
    WHERE cost_gate = 0
)
ARRAY JOIN ['all', if(toDayOfWeek(day) >= 6, 'weekend', 'weekday'), concat('dow', toString(toDayOfWeek(day)))] AS scope
WHERE cost_gate = 0
GROUP BY scope
ORDER BY scope;
`

const formCogloadQuiet2SHA256 = "408a5fdb5e96f3bd0f34ca0018d923f12abf5ca98396371c31fa0c190e8af991"

const (
	fcqOrg      = "f0f0f0f0-0000-4000-8000-000000000001"
	fcqOrgTwin  = "f0f0f0f0-0000-4000-8000-000000000002"
	fcqOther    = "f1f1f1f1-0000-4000-8000-000000000001"
	fcqRepo     = "0a0a0a0a-0000-4000-8000-000000000001"
	fcqOtherRep = "0b0b0b0b-0000-4000-8000-000000000001"
	fcqFarRows  = 200000
)

// fcqWindowStart is the first day of the window of the synthetic copy (a Monday).
var fcqWindowStart = time.Date(2026, 2, 16, 0, 0, 0, 0, time.UTC)

var fcqColumns = []string{"days", "d_umd", "d_tmd", "d_tcl", "d_umd_live", "d_tmd_live", "d_tcl_live",
	"d_cert", "d_reopened", "d_nomarker", "d_badstate", "c_dev", "c_dev_uncert", "c_teamonly", "c_teamonly_cert",
	"c_teamonly_uncert", "c_tmd_nodev", "c_norow", "c_norow_cert", "c_norow_reopened", "c_norow_nomarker",
	"c_cert_before_day_end", "c_umd_retracted_only", "c_tmd_retracted_only", "c_tcl_retracted_only",
	"c_tcl_authorless", "orgs_max"}

// fcqWant is the Q1 result for the fixture of seedFormCogloadQuiet2, by hand.
// Days by offset from the window start (offset 0 is a Monday). 0: live
// developer, team and tcl rows, certified before the day ended. 1: a live
// team_metrics row only, certified after the day. 2: tcl only, its newest row
// live by a ratio alone and authorless. 3: nothing, certified. 4: nothing,
// succeeded and reopened at one version. 5: a developer key whose newer
// retraction row sits in another part than its older live row. 6: a
// retraction-only developer row, reopened then succeeded. 7: a live developer
// row, no marker. 8: a team whose repository key's newest row is zeros, plus a
// live row with no repository that the repo_id layer drops. 9: a tcl team whose
// newest row is zeros with no ratio. Every other day: nothing.
var fcqWant = map[string][]uint64{
	//         days umd tmd tcl ul tl cl crt re nm bad dev du to tc tu tnd nr nc nre nnm bef ur tr cr aut org
	"all":     {30, 4, 3, 3, 2, 2, 2, 4, 1, 25, 0, 2, 1, 2, 1, 1, 1, 26, 2, 1, 23, 1, 2, 1, 1, 1, 1},
	"weekday": {22, 2, 3, 3, 2, 2, 2, 3, 1, 18, 0, 2, 1, 2, 1, 1, 1, 18, 1, 1, 16, 1, 0, 1, 1, 1, 1},
	"weekend": {8, 2, 0, 0, 0, 0, 0, 1, 0, 7, 0, 0, 0, 0, 0, 0, 0, 8, 1, 0, 7, 0, 2, 0, 0, 0, 1},
	"dow1":    {5, 2, 1, 1, 2, 1, 1, 1, 0, 4, 0, 2, 1, 0, 0, 0, 0, 3, 0, 0, 3, 1, 0, 0, 0, 0, 1},
	"dow2":    {5, 0, 2, 0, 0, 1, 0, 1, 0, 4, 0, 0, 0, 1, 1, 0, 1, 4, 0, 0, 4, 0, 0, 1, 0, 0, 1},
	"dow3":    {4, 0, 0, 2, 0, 0, 1, 0, 0, 4, 0, 0, 0, 1, 0, 1, 0, 3, 0, 0, 3, 0, 0, 0, 1, 1, 1},
	"dow4":    {4, 0, 0, 0, 0, 0, 0, 1, 0, 3, 0, 0, 0, 0, 0, 0, 0, 4, 1, 0, 3, 0, 0, 0, 0, 0, 1},
	"dow5":    {4, 0, 0, 0, 0, 0, 0, 0, 1, 3, 0, 0, 0, 0, 0, 0, 0, 4, 0, 1, 3, 0, 0, 0, 0, 0, 1},
	"dow6":    {4, 1, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 4, 0, 1, 0, 0, 0, 1},
	"dow7":    {4, 1, 0, 0, 0, 0, 0, 1, 0, 3, 0, 0, 0, 0, 0, 0, 0, 4, 1, 0, 3, 0, 1, 0, 0, 0, 1},
}

var fcqTables = []string{"user_metrics_daily", "team_metrics_daily", "team_cognitive_load_daily", "daily_metrics_run_marker"}

func formCogloadQuiet2Statements(t *testing.T) []string {
	t.Helper()
	sum := sha256.Sum256([]byte(strings.TrimPrefix(formCogloadQuiet2, "\n")))
	if got := hex.EncodeToString(sum[:]); got != formCogloadQuiet2SHA256 {
		t.Fatalf("synthetic FORM sha256 = %s, want %s", got, formCogloadQuiet2SHA256)
	}
	var statements []string
	for _, part := range strings.Split(formCogloadQuiet2, ";\n") {
		if part = strings.TrimSpace(part); part != "" {
			statements = append(statements, part)
		}
	}
	if len(statements) != 4 {
		t.Fatalf("synthetic FORM holds %d statements, want 4", len(statements))
	}
	return statements
}

func seedFormCogloadQuiet2(ctx context.Context, t *testing.T, admin stdclickhouse.Conn) {
	t.Helper()
	exec := func(statement string, args ...any) { fcqExec(ctx, t, admin, statement, args...) }
	// Parts stay unmerged, so a key's older and newer rows are read as two rows.
	for _, table := range fcqTables {
		exec(`SYSTEM STOP MERGES ` + table)
	}
	day := func(offset int) time.Time { return fcqWindowStart.AddDate(0, 0, offset) }
	older, newer := time.Now().UTC().Add(-time.Hour).Truncate(time.Second), time.Now().UTC().Truncate(time.Second)
	exec(`INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES
		(toUUID(?), 'acme/api', 'github', ?, now64(3), now64(3)), (toUUID(?), 'acme/web', 'github', ?, now64(3), now64(3))`,
		fcqRepo, fcqOrg, fcqOtherRep, fcqOther)
	umd := func(org, repo string, d time.Time, retracted uint8, at time.Time) {
		exec(`INSERT INTO user_metrics_daily (org_id, repo_id, author_email, day, retracted, computed_at)
			VALUES (?, toUUID(?), 'dev-a@example.com', ?, ?, ?)`, org, repo, d, retracted, at)
	}
	tmd := func(org, repo string, d time.Time, commits float64, at time.Time) {
		exec(`INSERT INTO team_metrics_daily (org_id, team_id, repo_id, day, commits_count, computed_at)
			VALUES (?, 'team-a', ?, ?, ?, ?)`, org, repo, d, commits, at)
	}
	tcl := func(org string, d time.Time, load float64, repos, authors uint32, afterHours *float64, at time.Time) {
		exec(`INSERT INTO team_cognitive_load_daily (org_id, team_id, day, pr_interruption_load, contributing_repo_count,
			sample_author_count, after_hours_commit_ratio, weekend_commit_ratio, computed_at)
			VALUES (?, 'team-a', ?, ?, ?, ?, ?, NULL, ?)`, org, d, load, repos, authors, afterHours, at)
	}
	marker := func(org string, d time.Time, state string, version uint64, finalized time.Time) {
		exec(`INSERT INTO daily_metrics_run_marker (org_id, target_day, generation, state, finalized_at, version)
			VALUES (?, ?, 'fixed-schedule:daily_metrics_fanout:fixture', ?, ?, ?)`, org, d, state, finalized, version)
	}
	half := 0.5
	quarter := 0.25
	d01, d02, d03, d04, d05 := day(0), day(1), day(2), day(3), day(4)
	d06, d07, d08, d09, d10 := day(5), day(6), day(7), day(8), day(9)
	umd(fcqOrg, fcqRepo, d01, 0, newer)
	tmd(fcqOrg, "r1", d01, 3, newer)
	tcl(fcqOrg, d01, 0, 0, 0, nil, older)
	tcl(fcqOrg, d01, 1, 1, 2, &half, newer)
	marker(fcqOrg, d01, "succeeded", 100, d01.Add(20*time.Hour))
	tmd(fcqOrg, "r1", d02, 1, newer)
	marker(fcqOrg, d02, "succeeded", 110, d02.Add(26*time.Hour))
	tcl(fcqOrg, d03, 1, 1, 3, &half, older)
	tcl(fcqOrg, d03, 0, 0, 0, &quarter, newer)
	marker(fcqOrg, d04, "succeeded", 120, d04.Add(27*time.Hour))
	marker(fcqOrg, d05, "succeeded", 200, d05.Add(26*time.Hour))
	marker(fcqOrg, d05, "reopened", 200, d05.Add(26*time.Hour))
	umd(fcqOrg, fcqRepo, d06, 0, older)
	umd(fcqOrg, fcqRepo, d06, 1, newer)
	umd(fcqOrg, fcqRepo, d07, 1, newer)
	marker(fcqOrg, d07, "reopened", 300, d07.Add(24*time.Hour))
	marker(fcqOrg, d07, "succeeded", 301, d07.Add(25*time.Hour))
	umd(fcqOrg, fcqRepo, d08, 0, newer)
	tmd(fcqOrg, "r1", d09, 2, older)
	tmd(fcqOrg, "r1", d09, 0, newer)
	tmd(fcqOrg, "", d09, 5, newer)
	tcl(fcqOrg, d10, 1, 1, 1, &half, older)
	tcl(fcqOrg, d10, 0, 0, 0, nil, newer)
	// Outside the window, the same org: never counted.
	for _, d := range []time.Time{day(-1), day(30)} {
		umd(fcqOrg, fcqRepo, d, 0, newer)
		tmd(fcqOrg, "r1", d, 1, newer)
		tcl(fcqOrg, d, 1, 1, 1, &half, newer)
		marker(fcqOrg, d, "succeeded", 400, d.Add(2*time.Hour))
	}
	// Another org with every class live on every window day: never counted.
	for offset := 0; offset < 30; offset++ {
		d := d01.AddDate(0, 0, offset)
		umd(fcqOther, fcqOtherRep, d, 0, newer)
		tmd(fcqOther, "r9", d, 1, newer)
		tcl(fcqOther, d, 1, 1, 1, &half, newer)
		marker(fcqOther, d, "succeeded", 500, d.Add(2*time.Hour))
	}
	// Far outside the window (partition 202512): the partition minmax index
	// must keep the Q1 read off these parts.
	exec(`INSERT INTO user_metrics_daily (org_id, repo_id, author_email, day, retracted, computed_at)
		SELECT ?, toUUID(?), concat('far-', toString(number), '@example.com'), toDate('2025-12-06'), 0, now()
		FROM numbers(?)`, fcqOrg, fcqRepo, fcqFarRows)
}

func fcqQuery(ctx context.Context, admin stdclickhouse.Conn, statement string) (map[string][]uint64, error) {
	rows, err := admin.Query(ctx, statement)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := map[string][]uint64{}
	for rows.Next() {
		var scope string
		values := make([]uint64, len(fcqColumns))
		dest := []any{&scope}
		for i := range values {
			dest = append(dest, &values[i])
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}
		result[scope] = values
	}
	return result, rows.Err()
}

func fcqScalar(ctx context.Context, t *testing.T, admin stdclickhouse.Conn, statement string) uint64 {
	t.Helper()
	var value uint64
	if err := admin.QueryRow(ctx, statement).Scan(&value); err != nil {
		t.Fatalf("%v\n%s", err, statement)
	}
	return value
}

func fcqMutate(t *testing.T, statement, old, replacement string) string {
	t.Helper()
	mutated := strings.Replace(statement, old, replacement, 1)
	if mutated == statement {
		t.Fatalf("mutant anchor not found: %q", old)
	}
	return mutated
}

// fcqReadRows runs one statement under a query id and returns its query_log
// type and read_rows.
func fcqReadRows(ctx context.Context, t *testing.T, admin stdclickhouse.Conn, statement string) (string, uint64, error) {
	t.Helper()
	queryID := fmt.Sprintf("fcq-%d", time.Now().UnixNano())
	rows, runErr := admin.Query(stdclickhouse.Context(ctx, stdclickhouse.WithQueryID(queryID)), statement)
	if runErr == nil {
		for rows.Next() {
		}
		runErr = rows.Err()
		rows.Close()
	}
	if err := admin.Exec(ctx, `SYSTEM FLUSH LOGS`); err != nil {
		t.Fatal(err)
	}
	var kind string
	var readRows uint64
	if err := admin.QueryRow(ctx, `SELECT toString(type), read_rows FROM system.query_log
		WHERE query_id = ? AND type != 'QueryStart' ORDER BY event_time_microseconds DESC LIMIT 1`, queryID).Scan(&kind, &readRows); err != nil {
		t.Fatalf("query_log of %s: %v", queryID, err)
	}
	return kind, readRows, runErr
}

func fcqAssertGateFires(ctx context.Context, t *testing.T, admin stdclickhouse.Conn, statement, capLiteral string, parts uint64) {
	t.Helper()
	fired := fcqMutate(t, statement, capLiteral, "> 0")
	kind, readRows, err := fcqReadRows(ctx, t, admin, fired)
	var exception *stdclickhouse.Exception
	if err == nil || !errors.As(err, &exception) || exception.Code != 395 || !strings.Contains(err.Error(), "STOP-COST-GATE") {
		t.Fatalf("gate with cap 0: err = %v, want code 395 STOP-COST-GATE", err)
	}
	if kind != "ExceptionBeforeStart" || readRows > 2*parts+1 {
		t.Fatalf("gate fire: query_log type %s read_rows %d, want ExceptionBeforeStart and <= 2P+1 = %d", kind, readRows, 2*parts+1)
	}
}

// fcqExec runs one seeding statement and fails the test on error.
func fcqExec(ctx context.Context, t *testing.T, admin stdclickhouse.Conn, statement string, args ...any) {
	t.Helper()
	if err := admin.Exec(ctx, statement, args...); err != nil {
		t.Fatalf("seed: %v\n%s", err, statement)
	}
}

// fcqDropOrgFilter removes the org prefix filter of the read of one table.
func fcqDropOrgFilter(t *testing.T, statement, table string) string {
	t.Helper()
	at := strings.Index(statement, "FROM "+table+"\n")
	if at < 0 {
		t.Fatalf("no FROM %s", table)
	}
	const filter = "startsWith(org_id, 'f0f0f0f0') AND "
	rel := strings.Index(statement[at:], filter)
	if rel < 0 {
		t.Fatalf("no org filter after FROM %s", table)
	}
	return statement[:at+rel] + statement[at+rel+len(filter):]
}

func TestFormCogloadQuiet2(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	statements := formCogloadQuiet2Statements(t)
	stmtR, s0, org1, q1 := statements[0], statements[1], statements[2], statements[3]
	_, admin := opfixture.Start(ctx, t)
	seedFormCogloadQuiet2(ctx, t, admin)
	parts := fcqScalar(ctx, t, admin, `SELECT count() FROM system.parts`)

	t.Run("stmtR", func(t *testing.T) {
		if got := fcqScalar(ctx, t, admin, stmtR); got != 1 {
			t.Fatalf("replicas = %d, want 1", got)
		}
	})

	s0Tables := func(t *testing.T, statement string) map[string]uint64 {
		rows, err := admin.Query(ctx, statement)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		got := map[string]uint64{}
		for rows.Next() {
			var table, minDay, maxDay string
			var count, bytes, active uint64
			if err := rows.Scan(&table, &count, &bytes, &active, &minDay, &maxDay); err != nil {
				t.Fatal(err)
			}
			if count > 0 {
				got[table] = count
			}
			if minDay > "2026-03-17" || maxDay < "2026-02-16" {
				t.Fatalf("S0 %s holds parts outside the window: %s .. %s", table, minDay, maxDay)
			}
		}
		return got
	}
	t.Run("S0", func(t *testing.T) {
		got := s0Tables(t, s0)
		for _, table := range fcqTables {
			if got[table] == 0 {
				t.Fatalf("S0 rows = %v, want rows > 0 for %s", got, table)
			}
		}
		if len(got) != len(fcqTables) {
			t.Fatalf("S0 tables = %v, want exactly the 4 read tables", got)
		}
	})
	t.Run("S0 mutant wrong table name", func(t *testing.T) {
		got := s0Tables(t, fcqMutate(t, s0, "'team_metrics_daily'", "'team_metric_daily'"))
		if _, ok := got["team_metrics_daily"]; ok || len(got) != len(fcqTables)-1 {
			t.Fatalf("S0 with a wrong name = %v, want the team_metrics_daily row missing", got)
		}
	})

	t.Run("ORG1", func(t *testing.T) {
		if got := fcqScalar(ctx, t, admin, org1); got != 1 {
			t.Fatalf("orgs = %d, want 1", got)
		}
	})

	t.Run("Q1 classes", func(t *testing.T) {
		got, err := fcqQuery(ctx, admin, q1)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, fcqWant) {
			for scope, want := range fcqWant {
				if !reflect.DeepEqual(got[scope], want) {
					t.Errorf("scope %s:\n got %v\nwant %v\ncols %v", scope, got[scope], want, fcqColumns)
				}
			}
			t.Fatalf("Q1 = %v", got)
		}
	})

	t.Run("Q1 pass path reads only the window parts", func(t *testing.T) {
		window := fcqScalar(ctx, t, admin, `SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase()
			AND table IN ('user_metrics_daily', 'team_metrics_daily', 'team_cognitive_load_daily', 'daily_metrics_run_marker')
			AND max_date >= toDate('2026-02-16') AND min_date <= toDate('2026-03-17')`)
		kind, readRows, err := fcqReadRows(ctx, t, admin, q1)
		if err != nil {
			t.Fatal(err)
		}
		// Six cost_gate references, each counted as its own pair of system.parts reads.
		bound := window + 12*parts + 30 + 1
		t.Logf("Q1 read_rows %d, window rows %d, parts %d, bound %d", readRows, window, parts, bound)
		if kind != "QueryFinish" || readRows > bound || readRows >= fcqFarRows {
			t.Fatalf("Q1 %s read_rows %d, want <= %d (window parts + 12P + 31) and < %d (the far partition unread)", kind, readRows, bound, fcqFarRows)
		}
	})

	t.Run("Q1 gate fires", func(t *testing.T) { fcqAssertGateFires(ctx, t, admin, q1, "> 9000000", parts) })
	t.Run("ORG1 gate fires", func(t *testing.T) { fcqAssertGateFires(ctx, t, admin, org1, "> 5000000", parts) })

	killed := func(t *testing.T, statement string) {
		t.Helper()
		got, err := fcqQuery(ctx, admin, statement)
		if err != nil {
			t.Fatal(err)
		}
		if reflect.DeepEqual(got, fcqWant) {
			t.Fatalf("mutant survived: the fixture does not tell it from the FORM")
		}
	}
	for _, table := range fcqTables {
		t.Run("Q1 mutant no org prefix on "+table, func(t *testing.T) { killed(t, fcqDropOrgFilter(t, q1, table)) })
	}
	rules := map[string][2]string{
		"Q1 mutant developer days from raw rows": {
			"argMax(user_metrics_daily.retracted, user_metrics_daily.computed_at) = 0 AS live", "1 AS live"},
		"Q1 mutant developer live from any version (merge state)": {
			"argMax(user_metrics_daily.retracted, user_metrics_daily.computed_at) = 0 AS live", "max(user_metrics_daily.retracted = 0) AS live"},
		"Q1 mutant team days from raw rows": {
			"(newest.commits_count != 0 OR", "(1 OR"},
		"Q1 mutant team rows without the repo_id layer": {
			"WHERE repo_id != '' OR real_repo_count = 0", "WHERE 1"},
		"Q1 mutant tcl days from raw rows": {
			"(argMax(team_cognitive_load_daily.pr_interruption_load", "(1 OR argMax(team_cognitive_load_daily.pr_interruption_load"},
		"Q1 mutant tcl live without the nullable ratios": {
			"OR isNotNull(tupleElement(argMax(tuple(team_cognitive_load_daily.after_hours_commit_ratio)",
			"OR 0 AND isNotNull(tupleElement(argMax(tuple(team_cognitive_load_daily.after_hours_commit_ratio)"},
		"Q1 mutant authors of any row, not the newest": {
			"argMax(team_cognitive_load_daily.sample_author_count, team_cognitive_load_daily.computed_at) AS sac",
			"max(team_cognitive_load_daily.sample_author_count) AS sac"},
		"Q1 mutant tie goes to succeeded": {
			"argMax(state, (version, state = 'reopened'))", "argMax(state, (version, state = 'succeeded'))"},
		"Q1 mutant window shifted a day": {
			"addDays(toDate('2026-02-16'), number)", "addDays(toDate('2026-02-15'), number)"},
		"Q1 mutant day end one day late": {
			"mfin < toDateTime64(addDays(day, 1)", "mfin < toDateTime64(addDays(day, 2)"},
	}
	for name, rule := range rules {
		t.Run(name, func(t *testing.T) { killed(t, fcqMutate(t, q1, rule[0], rule[1])) })
	}

	t.Run("ORG1 mutant no org prefix", func(t *testing.T) {
		if got := fcqScalar(ctx, t, admin, fcqMutate(t, org1, "WHERE startsWith(org_id, 'f0f0f0f0') AND ", "WHERE ")); got == 1 {
			t.Fatalf("orgs = %d without the prefix, want > 1", got)
		}
	})

	// Last: a second org under the prefix. ORG1 reads 2 (its STOP) and Q1 orgs_max 2.
	t.Run("a prefix that names two orgs stops ORG1 and shows in Q1 orgs_max", func(t *testing.T) {
		fcqExec(ctx, t, admin, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced)
			VALUES (generateUUIDv4(), 'acme/twin', 'github', ?, now64(3), now64(3))`, fcqOrgTwin)
		fcqExec(ctx, t, admin, `INSERT INTO user_metrics_daily (org_id, repo_id, author_email, day, retracted, computed_at)
			VALUES (?, toUUID(?), 'dev-b@example.com', toDate('2026-02-16'), 0, now())`, fcqOrgTwin, fcqRepo)
		if got := fcqScalar(ctx, t, admin, org1); got != 2 {
			t.Fatalf("orgs = %d, want 2 (STOP)", got)
		}
		got, err := fcqQuery(ctx, admin, q1)
		if err != nil {
			t.Fatal(err)
		}
		if orgs := got["all"][len(fcqColumns)-1]; orgs != 2 {
			t.Fatalf("Q1 orgs_max = %d, want 2", orgs)
		}
	})
}
