//go:build integration

package chmigrate_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/testopspooled"
)

// DO NOT MERGE. Throwaway exec proof of a prod read form (FORM-3NN-POOL-2). pool3nnForm is the form file with ONE swap
// (the app org prefix c6a38355 -> the synthetic f0f0f0f0, public repo); its sha256 is pinned. Synthetic rows only.
const (
	pool3nnOrg   = "f0f0f0f0-0000-4000-8000-000000000000"
	pool3nnOther = "b0000000-0000-4000-8000-000000000000"
)

const pool3nnFormSHA256 = "153075ca034e87540c45617b90a028c8697d582849e8b14975396d6c52d42757"

const pool3nnForm = `-- FORM-3NN-POOL-2 (ClickHouse, prod, read-only). acp-train39, 2026-11-24. query-api roll PRE read (acp LINE-1). STEP number TBD.
-- Replaces FORM-333-POOL-1.sql 74ade4e92522 (its B = 7 x suite rows + 3 x daily rows is the pre-fix statement; not valid at the fix).
-- What: the row counts that bound ONE pooled TEST_SUITE_DURATION_P95 statement of the FIXED query-api build (ops #7008,
--   CHAOS-14591; cites at the PR head 557a2ad42bde (2026-11-24T15:07Z), NOT FINAL: re-cite at the squash). The statement runs
--   with max_rows_to_read = TestopsPooledMaxRowsToRead 5,000,000, read_overflow_mode 'throw' (analytics/orgpooled.go:80, :87-93).
-- Reads of the statement at 557a2ad42bde (testopspooled.SuiteRunsSource, testopspooled.go:117-138; suiteRawRowsRuled :145-160):
--   test_suite_results FINAL x 1 (:156): repo_id IN (the repos of LegacyRepoDays) (:158), key (org_id, repo_id, ...): it reads
--     the granules of the repos with a legacy repo-day in the window, their WHOLE history (no day key) = L. The window functions
--     (:148-149, :154-155) run on that one read. An empty repo set reads nothing.
--   testops_test_metrics_daily x 4: NewestStoredRows :126, LegacyRepoDays :133, LiveRepoDays :137, LegacyRepoDays :158; each <= D.
--   (Superseded head 17ecff098907: 7 non-FINAL run-set reads of L + 7 FINAL run-set reads + 10 daily reads; its own CI cell was
--   RED: Go run 45417447668 job 138528524025, "CELL14259 legacy head: read_rows 18900056 = 7.00 x the 2700003 suite rows", served
--   refused code 158 = ClickHouse 26.7 does not build equal IN sets once. stmt4 / stmt5 run_set_* columns = INFO for that shape.)
--   CI at 557a2ad42bde (read 15:27Z): Go run 45421919369 success; the head cell runs in job 138544326786 (shard-packages-16, SHARD-TEST
--   line; a pass prints no CELL14259 line, so no read_rows number is seen); Go quality 45421918902 in_progress.
-- BOUND (pm computes on the Mac from the outputs; acp rules). For each window bucket b in {all, 90, 30} (legacy day >= today() - N):
--   D  = S0 rows(testops_test_metrics_daily) (all orgs: a daily read reads whole granules, which can hold another org's rows; >= stmt2 org_rows).
--   Lb = 8192 x stmt5 legacy_repo_blocks_b (the granules of the legacy repos). If stmt5 errors: Lb = stmt4 legacy_repo_rows_b + 16384 x stmt3 legacy_repos_b.
--   B_b = Lb + 4 x D.
--   WANT B_all <= 4,000,000 (80 % of 5,000,000) = PASS. Else report B of all 3 buckets: acp rules; B_b >= 5,000,000 for a window that a
--   caller sends = the build refuses the measure = STOP (no START). A squash SQL that differs from 557a2ad42bde in testopspooled.go /
--   fragments.go = new factors = a new form -N. The CI cell BOUNDCHECK proves read_rows of the pin's own SuiteRunsSource <= B_b on its rows.
-- Form: lane-scratch/pm-guard/FORM-TEMPLATE-3.md 6b9e73a3c45a (CH shape, one statement per call, self-gating); form of
--   acp-train35 q33/FORM-333-POOL-1.sql 74ade4e92522.
-- Tables at the chain head (baseline head.json + chain 171): test_suite_results ReplacingMergeTree(last_synced) ORDER BY
--   (org_id, repo_id, run_id, suite_id), index_granularity 8192; testops_test_metrics_daily ReplacingMergeTree(computed_at)
--   PARTITION BY toYYYYMM(day) ORDER BY (org_id, repo_id, day), index_granularity 8192; pooled_samples_stored (161).
-- Legacy repo-day = the pin's LegacyRepoDays rule: the newest row of the repo-day (ORDER BY computed_at DESC, cityHash64(tuple(*))
--   DESC LIMIT 1 BY org_id, repo_id, day) has pooled_samples_stored != 1. Run set (INFO) = the 17ecff098907 legacySuiteRuns rule: a
--   run with a suite row (any version) whose (repo_id, toDate(coalesce(started_at, finished_at))) is a legacy repo-day.
-- No placeholder. The file sha12 acp names is of THIS file.
-- WORK MATH (each gated statement holds its cost gate; a gate reads system.parts twice (2P) + system.one (1)):
--   stmt1 reads test_suite_results x 1; stmt2 and stmt3 read testops_test_metrics_daily x 1; stmt4 and stmt5 read
--   test_suite_results x 2 + testops_test_metrics_daily x 2. Caps: stmt1-3 20,000,000 rows / 4,000,000,000 bytes on the
--   statement table; stmt4-5 9,000,000 rows / 2,000,000,000 bytes on BOTH tables together (2 reads each: <= 18,000,000 + 2P + 1).
--   No FINAL, no CTE, no scalar subquery on data. S0 and stmt0 read system tables only (no gate).
-- Output: counts only (stmt0: the server version string). No ids, no times, no payload text.

-- S0 (system.parts only). WANT 2 rows (test_suite_results, testops_test_metrics_daily), each rows > 0 and active_parts > 0 (a missing row = wrong name: STOP).
SELECT table AS t, sum(rows) AS rows, sum(bytes_on_disk) AS bytes, count() AS active_parts
FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('test_suite_results', 'testops_test_metrics_daily')
GROUP BY table ORDER BY table;

-- stmt0 ClickHouse server version (system.one only). WANT 1 row. INFO: acp compares it with the CI version of the cells.
SELECT version() AS ch_version;

-- stmt1 ORG-1 on test_suite_results. WANT orgs = 1, org_rows > 0 (org_rows <= S0 rows).
WITH (SELECT throwIf(
  (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('test_suite_results')) > 20000000
  OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('test_suite_results')) > 4000000000,
  'STOP-COST-GATE')) AS cost_gate
SELECT uniqExact(org_id) AS orgs, count() AS org_rows
FROM test_suite_results
WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0;

-- stmt2 ORG-1 on testops_test_metrics_daily. WANT orgs = 1, org_rows > 0 (org_rows <= S0 rows).
WITH (SELECT throwIf(
  (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('testops_test_metrics_daily')) > 20000000
  OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('testops_test_metrics_daily')) > 4000000000,
  'STOP-COST-GATE')) AS cost_gate
SELECT uniqExact(org_id) AS orgs, count() AS org_rows
FROM testops_test_metrics_daily
WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0;

-- stmt3 ORG-1 legacy repo-days (newest row per repo-day). WANT 1 row; legacy_repo_days <= repo_days; each _90 / _30 <= its all value.
WITH (SELECT throwIf(
  (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('testops_test_metrics_daily')) > 20000000
  OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('testops_test_metrics_daily')) > 4000000000,
  'STOP-COST-GATE')) AS cost_gate
SELECT count() AS repo_days,
  countIf(pooled_samples_stored != 1) AS legacy_repo_days,
  uniqExactIf(repo_id, pooled_samples_stored != 1) AS legacy_repos,
  countIf(pooled_samples_stored != 1 AND day >= today() - 90) AS legacy_repo_days_90,
  uniqExactIf(repo_id, pooled_samples_stored != 1 AND day >= today() - 90) AS legacy_repos_90,
  countIf(pooled_samples_stored != 1 AND day >= today() - 30) AS legacy_repo_days_30,
  uniqExactIf(repo_id, pooled_samples_stored != 1 AND day >= today() - 30) AS legacy_repos_30
FROM (
  SELECT org_id, repo_id, day, pooled_samples_stored
  FROM testops_test_metrics_daily
  WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0
  ORDER BY computed_at DESC, cityHash64(tuple(*)) DESC
  LIMIT 1 BY org_id, repo_id, day
);

-- stmt4 ORG-1 suite rows of the legacy repos (L) and of the run set (R, INFO), per bucket. WANT 1 row; org_rows = stmt1 org_rows (+ rows synced between the two calls); run_set_rows <= legacy_repo_rows <= org_rows; each _90 / _30 <= its all value.
WITH (SELECT throwIf(
  (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('test_suite_results', 'testops_test_metrics_daily')) > 9000000
  OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('test_suite_results', 'testops_test_metrics_daily')) > 2000000000,
  'STOP-COST-GATE')) AS cost_gate
SELECT count() AS org_rows,
  countIf(lr.repo_last > toDate(0)) AS legacy_repo_rows,
  countIf(lr.repo_last >= today() - 90) AS legacy_repo_rows_90,
  countIf(lr.repo_last >= today() - 30) AS legacy_repo_rows_30,
  countIf(rs.run_last > toDate(0)) AS run_set_rows,
  countIf(rs.run_last >= today() - 90) AS run_set_rows_90,
  countIf(rs.run_last >= today() - 30) AS run_set_rows_30,
  uniqExactIf((s.repo_id, s.run_id), rs.run_last > toDate(0)) AS run_set_runs
FROM test_suite_results AS s
LEFT JOIN (
  SELECT repo_id, max(day) AS repo_last
  FROM (
    SELECT org_id, repo_id, day, pooled_samples_stored
    FROM testops_test_metrics_daily
    WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0
    ORDER BY computed_at DESC, cityHash64(tuple(*)) DESC
    LIMIT 1 BY org_id, repo_id, day
  )
  WHERE pooled_samples_stored != 1
  GROUP BY repo_id
) AS lr ON s.repo_id = lr.repo_id
LEFT JOIN (
  SELECT repo_id, run_id, max(toDate(coalesce(started_at, finished_at))) AS run_last
  FROM test_suite_results
  WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0
    AND (repo_id, toDate(coalesce(started_at, finished_at))) IN (
      SELECT repo_id, day FROM (
        SELECT org_id, repo_id, day, pooled_samples_stored
        FROM testops_test_metrics_daily
        WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0
        ORDER BY computed_at DESC, cityHash64(tuple(*)) DESC
        LIMIT 1 BY org_id, repo_id, day
      ) WHERE pooled_samples_stored != 1)
  GROUP BY repo_id, run_id
) AS rs ON s.repo_id = rs.repo_id AND s.run_id = rs.run_id
WHERE startsWith(s.org_id, 'f0f0f0f0') AND cost_gate = 0;

-- stmt5 ORG-1 granules (8192-row blocks of a part) that hold a legacy-repo row or a run-set row, per bucket. WANT 1 row; 8192 x each blocks value >= the matching stmt4 rows value; run_set_blocks_b <= legacy_repo_blocks_b. An error here = report the exact text; B uses the stmt4 fallback (acp rules).
WITH (SELECT throwIf(
  (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('test_suite_results', 'testops_test_metrics_daily')) > 9000000
  OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('test_suite_results', 'testops_test_metrics_daily')) > 2000000000,
  'STOP-COST-GATE')) AS cost_gate
SELECT uniqExactIf((s._part, intDiv(s._part_offset, 8192)), lr.repo_last > toDate(0)) AS legacy_repo_blocks,
  uniqExactIf((s._part, intDiv(s._part_offset, 8192)), lr.repo_last >= today() - 90) AS legacy_repo_blocks_90,
  uniqExactIf((s._part, intDiv(s._part_offset, 8192)), lr.repo_last >= today() - 30) AS legacy_repo_blocks_30,
  uniqExactIf((s._part, intDiv(s._part_offset, 8192)), rs.run_last > toDate(0)) AS run_set_blocks,
  uniqExactIf((s._part, intDiv(s._part_offset, 8192)), rs.run_last >= today() - 90) AS run_set_blocks_90,
  uniqExactIf((s._part, intDiv(s._part_offset, 8192)), rs.run_last >= today() - 30) AS run_set_blocks_30
FROM test_suite_results AS s
LEFT JOIN (
  SELECT repo_id, max(day) AS repo_last
  FROM (
    SELECT org_id, repo_id, day, pooled_samples_stored
    FROM testops_test_metrics_daily
    WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0
    ORDER BY computed_at DESC, cityHash64(tuple(*)) DESC
    LIMIT 1 BY org_id, repo_id, day
  )
  WHERE pooled_samples_stored != 1
  GROUP BY repo_id
) AS lr ON s.repo_id = lr.repo_id
LEFT JOIN (
  SELECT repo_id, run_id, max(toDate(coalesce(started_at, finished_at))) AS run_last
  FROM test_suite_results
  WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0
    AND (repo_id, toDate(coalesce(started_at, finished_at))) IN (
      SELECT repo_id, day FROM (
        SELECT org_id, repo_id, day, pooled_samples_stored
        FROM testops_test_metrics_daily
        WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0
        ORDER BY computed_at DESC, cityHash64(tuple(*)) DESC
        LIMIT 1 BY org_id, repo_id, day
      ) WHERE pooled_samples_stored != 1)
  GROUP BY repo_id, run_id
) AS rs ON s.repo_id = rs.repo_id AND s.run_id = rs.run_id
WHERE startsWith(s.org_id, 'f0f0f0f0') AND cost_gate = 0;
`

type pool3nnStatement struct{ label, text string }

func pool3nnSplit(body string) []pool3nnStatement {
	var out []pool3nnStatement
	var chunk []string
	for _, line := range strings.Split(body, "\n") {
		chunk = append(chunk, line)
		if !strings.HasSuffix(strings.TrimSpace(chmigrate.StripLineComments(line)), ";") {
			continue
		}
		text := strings.TrimSuffix(strings.TrimRight(strings.Join(chunk, "\n"), " \t\n"), ";")
		label := ""
		for _, l := range chunk {
			if t := strings.TrimSpace(l); strings.HasPrefix(t, "-- S0") || strings.HasPrefix(t, "-- stmt") {
				label = strings.Fields(strings.TrimPrefix(t, "-- "))[0]
				break
			}
		}
		out = append(out, pool3nnStatement{label: label, text: text})
		chunk = nil
	}
	return out
}

func pool3nnRender(v any) string {
	value := reflect.ValueOf(v)
	for value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return "NULL"
		}
		value = value.Elem()
	}
	return fmt.Sprint(value.Interface())
}

func pool3nnRun(ctx context.Context, conn driver.Conn, text, comment string) ([]map[string]string, []string, error) {
	qctx := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"log_comment": comment}))
	rows, err := conn.Query(qctx, text)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	columns := rows.Columns()
	types := rows.ColumnTypes()
	var typeNames []string
	for _, ct := range types {
		typeNames = append(typeNames, ct.DatabaseTypeName())
	}
	var got []map[string]string
	for rows.Next() {
		cells := make([]any, len(types))
		for i, ct := range types {
			cells[i] = reflect.New(ct.ScanType()).Interface()
		}
		if err := rows.Scan(cells...); err != nil {
			return nil, nil, err
		}
		row := map[string]string{}
		for i, name := range columns {
			row[name] = pool3nnRender(cells[i])
		}
		got = append(got, row)
	}
	return got, typeNames, rows.Err()
}

func pool3nnLog(ctx context.Context, t *testing.T, conn driver.Conn, comment string) ([]string, uint64) {
	t.Helper()
	rows, err := conn.Query(ctx, `SELECT toString(type), read_rows FROM system.query_log
		WHERE log_comment = ? AND type != 'QueryStart' ORDER BY event_time_microseconds`, comment)
	if err != nil {
		t.Fatalf("query log: %v", err)
	}
	defer rows.Close()
	var types []string
	var total uint64
	for rows.Next() {
		var typ string
		var read uint64
		if err := rows.Scan(&typ, &read); err != nil {
			t.Fatalf("query log scan: %v", err)
		}
		types = append(types, typ)
		total += read
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return types, total
}

// The fixture, built here and recomputed here (never read back from ClickHouse for a WANT). Day offsets count back from the
// server's today(). Daily rows: A legacy -100..-91 + stored -10..-1; B legacy -20..-11 + a NEWER stored row on -15 (so -15 is
// not legacy); C stored -5..-1; D legacy -60..-56; X (other org) legacy -25..-21. Suite rows: 3 runs x 100 suites per day on
// A -200..-150 (no daily row) + -100..-91 + -10..-1, B -20..-11, C -5..-1, D -60..-56, X -25..-21; one INSERT per block.
type pool3nnDaily struct {
	org, repo  string
	off        int
	stored     int
	laterHours int
}

type pool3nnBlock struct {
	org, repo, tag string
	from, to       int
}

const (
	pool3nnRepoA = "00000000-0000-4000-8000-00000000000a"
	pool3nnRepoB = "00000000-0000-4000-8000-00000000000b"
	pool3nnRepoC = "00000000-0000-4000-8000-00000000000c"
	pool3nnRepoD = "00000000-0000-4000-8000-00000000000d"
	pool3nnRepoX = "00000000-0000-4000-8000-0000000000b9"
	pool3nnRuns  = 3
	pool3nnSuite = 100
)

func pool3nnFixture() ([]pool3nnDaily, []pool3nnBlock) {
	var daily []pool3nnDaily
	add := func(org, repo string, from, to, stored int) {
		for off := from; off >= to; off-- {
			daily = append(daily, pool3nnDaily{org: org, repo: repo, off: off, stored: stored})
		}
	}
	add(pool3nnOrg, pool3nnRepoA, 100, 91, 0)
	add(pool3nnOrg, pool3nnRepoA, 10, 1, 1)
	add(pool3nnOrg, pool3nnRepoB, 20, 11, 0)
	daily = append(daily, pool3nnDaily{org: pool3nnOrg, repo: pool3nnRepoB, off: 15, stored: 1, laterHours: 1})
	add(pool3nnOrg, pool3nnRepoC, 5, 1, 1)
	add(pool3nnOrg, pool3nnRepoD, 60, 56, 0)
	add(pool3nnOther, pool3nnRepoX, 25, 21, 0)
	blocks := []pool3nnBlock{
		{pool3nnOrg, pool3nnRepoA, "a", 200, 150},
		{pool3nnOrg, pool3nnRepoA, "a", 100, 91},
		{pool3nnOrg, pool3nnRepoA, "a", 10, 1},
		{pool3nnOrg, pool3nnRepoB, "b", 20, 11},
		{pool3nnOrg, pool3nnRepoC, "c", 5, 1},
		{pool3nnOrg, pool3nnRepoD, "d", 60, 56},
		{pool3nnOther, pool3nnRepoX, "x", 25, 21},
	}
	return daily, blocks
}

// pool3nnExpect recomputes every count WANT of stmt1-stmt4 for the org prefix org from the fixture.
func pool3nnExpect(daily []pool3nnDaily, blocks []pool3nnBlock, orgPrefix string) map[string]map[string]string {
	type key struct {
		repo string
		off  int
	}
	newest := map[key]pool3nnDaily{}
	dailyRows := 0
	for _, d := range daily {
		if !strings.HasPrefix(d.org, orgPrefix) {
			continue
		}
		dailyRows++
		k := key{d.repo, d.off}
		if old, ok := newest[k]; !ok || d.laterHours > old.laterHours {
			newest[k] = d
		}
	}
	legacy := map[key]bool{}
	repoLast := map[string]int{}
	legacyDays := [3]int{}
	legacyRepos := [3]map[string]bool{{}, {}, {}}
	limits := [3]int{1 << 30, 90, 30}
	for k, d := range newest {
		if d.stored == 1 {
			continue
		}
		legacy[k] = true
		if last, ok := repoLast[k.repo]; !ok || k.off < last {
			repoLast[k.repo] = k.off
		}
		for i, limit := range limits {
			if k.off <= limit {
				legacyDays[i]++
				legacyRepos[i][k.repo] = true
			}
		}
	}
	suiteRows := 0
	legacyRepoRows := [3]int{}
	runSetRows := [3]int{}
	runSetRuns := 0
	for _, b := range blocks {
		if !strings.HasPrefix(b.org, orgPrefix) {
			continue
		}
		for off := b.from; off >= b.to; off-- {
			rows := pool3nnRuns * pool3nnSuite
			suiteRows += rows
			last, isLegacyRepo := repoLast[b.repo]
			for i, limit := range limits {
				if isLegacyRepo && last <= limit {
					legacyRepoRows[i] += rows
				}
				if legacy[key{b.repo, off}] && off <= limit {
					runSetRows[i] += rows
				}
			}
			if legacy[key{b.repo, off}] {
				runSetRuns += pool3nnRuns
			}
		}
	}
	orgs := "1"
	s := func(n int) string { return fmt.Sprint(n) }
	return map[string]map[string]string{
		"stmt1": {"orgs": orgs, "org_rows": s(suiteRows)},
		"stmt2": {"orgs": orgs, "org_rows": s(dailyRows)},
		"stmt3": {"repo_days": s(len(newest)), "legacy_repo_days": s(legacyDays[0]), "legacy_repos": s(len(legacyRepos[0])),
			"legacy_repo_days_90": s(legacyDays[1]), "legacy_repos_90": s(len(legacyRepos[1])),
			"legacy_repo_days_30": s(legacyDays[2]), "legacy_repos_30": s(len(legacyRepos[2]))},
		"stmt4": {"org_rows": s(suiteRows), "legacy_repo_rows": s(legacyRepoRows[0]), "legacy_repo_rows_90": s(legacyRepoRows[1]),
			"legacy_repo_rows_30": s(legacyRepoRows[2]), "run_set_rows": s(runSetRows[0]), "run_set_rows_90": s(runSetRows[1]),
			"run_set_rows_30": s(runSetRows[2]), "run_set_runs": s(runSetRuns)},
	}
}

func pool3nnUint(t *testing.T, row map[string]string, name string) uint64 {
	t.Helper()
	var n uint64
	if _, err := fmt.Sscan(row[name], &n); err != nil {
		t.Fatalf("%s = %q: %v", name, row[name], err)
	}
	return n
}

func TestForm3nnPoolExecutesAtTheChainHead(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	sum := sha256.Sum256([]byte(pool3nnForm))
	if got := hex.EncodeToString(sum[:]); got != pool3nnFormSHA256 {
		t.Fatalf("form sha256 = %s, want %s", got, pool3nnFormSHA256)
	}
	statements := pool3nnSplit(pool3nnForm)
	byLabel := map[string]string{}
	labels := make([]string, 0, len(statements))
	for _, s := range statements {
		labels = append(labels, s.label)
		byLabel[s.label] = s.text
	}
	if strings.Join(labels, ",") != "S0,stmt0,stmt1,stmt2,stmt3,stmt4,stmt5" {
		t.Fatalf("form statements = %v", labels)
	}
	// Shape: the caps, the gate table list, the org rule on every data read.
	both := "table IN ('test_suite_results', 'testops_test_metrics_daily')"
	shape := map[string]struct {
		rows, bytes, list string
		org, suite, daily int
	}{
		"stmt1": {"> 20000000", "> 4000000000", "table IN ('test_suite_results')", 1, 1, 0},
		"stmt2": {"> 20000000", "> 4000000000", "table IN ('testops_test_metrics_daily')", 1, 0, 1},
		"stmt3": {"> 20000000", "> 4000000000", "table IN ('testops_test_metrics_daily')", 1, 0, 1},
		"stmt4": {"> 9000000", "> 2000000000", both, 4, 2, 2},
		"stmt5": {"> 9000000", "> 2000000000", both, 4, 2, 2},
	}
	for _, s := range statements {
		want, gated := shape[s.label]
		if !gated {
			if strings.Contains(s.text, "STOP-COST-GATE") || strings.Contains(s.text, "FROM test_suite_results") || strings.Contains(s.text, "FROM testops_test_metrics_daily") {
				t.Fatalf("%s: a system statement holds a gate or a data read", s.label)
			}
			continue
		}
		for lit, n := range map[string]int{want.rows: 1, want.bytes: 1, "'STOP-COST-GATE'": 1, want.list: 2, "'f0f0f0f0'": want.org,
			"FROM test_suite_results": want.suite, "FROM testops_test_metrics_daily": want.daily} {
			if got := strings.Count(s.text, lit); got != n {
				t.Fatalf("%s: %q found %d times, want %d", s.label, lit, got, n)
			}
		}
		if got := strings.Count(s.text, "startsWith("); got != want.org {
			t.Fatalf("%s: startsWith found %d times, want %d", s.label, got, want.org)
		}
		if got := strings.Count(s.text, "cost_gate = 0"); got != want.org {
			t.Fatalf("%s: cost_gate = 0 found %d times, want %d (one per data read)", s.label, got, want.org)
		}
	}

	scratch := scratchDatabase(t)
	conn := openDatabase(t, scratch.URI, "")
	db, _, err := chmigrate.NewConnDB(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := chmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	result, err := chmigrate.Upgrade(ctx, db, baseline, chain)
	if err != nil {
		t.Fatalf("real chain: %v", err)
	}
	if result.Head < "171_" {
		t.Fatalf("schema head %q, want 171 or later", result.Head)
	}
	var version string
	var today time.Time
	if err := conn.QueryRow(ctx, "SELECT version(), today()").Scan(&version, &today); err != nil {
		t.Fatal(err)
	}
	t.Logf("schema head %s, clickhouse %s, today %s", result.Head, version, today.Format("2006-01-02"))
	day := func(off int) string { return today.AddDate(0, 0, -off).Format("2006-01-02") }

	daily, blocks := pool3nnFixture()
	for _, stmt := range []string{"SYSTEM STOP MERGES test_suite_results", "SYSTEM STOP MERGES testops_test_metrics_daily"} {
		if err := conn.Exec(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}
	// Two INSERTs: a ReplacingMergeTree collapses same-key rows inside one inserted block, so the newer row of a repo-day goes in
	// its own part (merges are stopped), as a later writer run does.
	for _, later := range []bool{false, true} {
		var values []string
		for _, d := range daily {
			if (d.laterHours > 0) != later {
				continue
			}
			values = append(values, fmt.Sprintf("(toUUID('%s'), toDate('%s'), '%s', toDateTime('%s 10:00:00', 'UTC') + %d, %d)",
				d.repo, day(d.off), d.org, day(0), 3600*d.laterHours, d.stored))
		}
		if err := conn.Exec(ctx, "INSERT INTO testops_test_metrics_daily (repo_id, day, org_id, computed_at, pooled_samples_stored) VALUES "+
			strings.Join(values, ", ")); err != nil {
			t.Fatalf("daily fixture (later %v): %v", later, err)
		}
	}
	perDay := pool3nnRuns * pool3nnSuite
	for _, b := range blocks {
		insert := fmt.Sprintf(`INSERT INTO test_suite_results (repo_id, run_id, suite_id, suite_name, org_id, last_synced, started_at, duration_seconds)
SELECT toUUID('%s'), concat('%s-', toString(%d - intDiv(number, %d)), '-', toString(intDiv(number %% %d, %d))), concat('s-', toString(number %% %d)), 'pkg', '%s',
  toDateTime64('%s 09:00:00', 3, 'UTC'), toDateTime64(toDate('%s') - (%d - intDiv(number, %d)), 3, 'UTC') + 3600, toFloat64(number %% 600)
FROM numbers(%d)`, b.repo, b.tag, b.from, perDay, perDay, pool3nnSuite, pool3nnSuite, b.org, day(0), day(0), b.from, perDay, (b.from-b.to+1)*perDay)
		if err := conn.Exec(ctx, insert); err != nil {
			t.Fatalf("suite fixture %s %d..%d: %v", b.tag, b.from, b.to, err)
		}
	}
	size := map[string][2]uint64{}
	for _, table := range []string{"test_suite_results", "testops_test_metrics_daily"} {
		var rowsN, partsN uint64
		if err := conn.QueryRow(ctx, `SELECT sum(rows), count() FROM system.parts
			WHERE active AND database = currentDatabase() AND table = ?`, table).Scan(&rowsN, &partsN); err != nil {
			t.Fatal(err)
		}
		size[table] = [2]uint64{rowsN, partsN}
		t.Logf("fixture: %s rows=%d active_parts=%d", table, rowsN, partsN)
	}
	suiteAll := 0
	for _, b := range blocks {
		suiteAll += (b.from - b.to + 1) * perDay
	}
	if size["test_suite_results"][0] != uint64(suiteAll) || size["testops_test_metrics_daily"][0] != uint64(len(daily)) {
		t.Fatalf("fixture rows %v, want suite %d / daily %d", size, suiteAll, len(daily))
	}
	t.Logf("fixture: suite parts %d for %d INSERTs (merges stopped)", size["test_suite_results"][1], len(blocks))
	parts := size["test_suite_results"][1] + size["testops_test_metrics_daily"][1]

	want := pool3nnExpect(daily, blocks, "f0f0f0f0")
	wantOther := pool3nnExpect(daily, blocks, "b0000000")
	t.Logf("WANT (Go recompute) %v", want)
	expect := func(phase, label string, got, want map[string]string) {
		t.Helper()
		for name, value := range want {
			if got[name] != value {
				t.Errorf("%s %s: %s = %s, want %s", phase, label, name, got[name], value)
			}
		}
	}
	wantTypes := map[string][]string{
		"S0":    {"String", "UInt64", "UInt64", "UInt64"},
		"stmt0": {"String"},
		"stmt1": {"UInt64", "UInt64"},
		"stmt2": {"UInt64", "UInt64"},
		"stmt3": {"UInt64", "UInt64", "UInt64", "UInt64", "UInt64", "UInt64", "UInt64"},
		"stmt4": {"UInt64", "UInt64", "UInt64", "UInt64", "UInt64", "UInt64", "UInt64", "UInt64"},
		"stmt5": {"UInt64", "UInt64", "UInt64", "UInt64", "UInt64", "UInt64"},
	}
	out := map[string]map[string]string{}
	passComments := map[string]string{}
	for _, label := range labels {
		passComments[label] = fmt.Sprintf("pool3nn-pass-%s-%d", label, time.Now().UnixNano())
		got, types, err := pool3nnRun(ctx, conn, byLabel[label], passComments[label])
		if err != nil {
			t.Fatalf("PASS %s: SQL error: %v", label, err)
		}
		if strings.Join(types, ",") != strings.Join(wantTypes[label], ",") {
			t.Errorf("OUTPUT %s: column types %v, want %v", label, types, wantTypes[label])
		}
		wantRows := 1
		if label == "S0" {
			wantRows = 2
		}
		if len(got) != wantRows {
			t.Fatalf("PASS %s: %d rows, want %d", label, len(got), wantRows)
		}
		t.Logf("PASS %s ROWS %v", label, got)
		out[label] = got[0]
		if label == "S0" {
			if got[0]["t"] != "test_suite_results" || got[0]["rows"] != fmt.Sprint(suiteAll) || got[1]["t"] != "testops_test_metrics_daily" || got[1]["rows"] != fmt.Sprint(len(daily)) {
				t.Errorf("PASS S0: %v", got)
			}
			continue
		}
		expect("PASS", label, got[0], want[label])
	}
	if out["stmt0"]["ch_version"] != version {
		t.Errorf("stmt0 version %q, want %q", out["stmt0"]["ch_version"], version)
	}
	// stmt5 bounds: 8192 x blocks covers the rows, the run set sits inside the legacy repos, each bucket inside "all".
	for _, b := range []string{"", "_90", "_30"} {
		lb, rb := pool3nnUint(t, out["stmt5"], "legacy_repo_blocks"+b), pool3nnUint(t, out["stmt5"], "run_set_blocks"+b)
		lr, rr := pool3nnUint(t, out["stmt4"], "legacy_repo_rows"+b), pool3nnUint(t, out["stmt4"], "run_set_rows"+b)
		if 8192*lb < lr || 8192*rb < rr || rb > lb || (rr > 0) != (rb > 0) {
			t.Errorf("stmt5%s: blocks legacy %d run set %d vs rows legacy %d run set %d", b, lb, rb, lr, rr)
		}
	}

	// BOUND B = L + 4 x D of the form header, from the outputs, and BOUNDCHECK: the pin's own SuiteRunsSource read_rows on the same rows.
	d := size["testops_test_metrics_daily"][0]
	if s0d := pool3nnUint(t, out["stmt2"], "org_rows"); s0d > d {
		t.Fatalf("stmt2 org_rows %d over the daily table rows %d", s0d, d)
	}
	bound := func(b string) uint64 { return 8192*pool3nnUint(t, out["stmt5"], "legacy_repo_blocks"+b) + 4*d }
	windows := []struct {
		name, bucket string
		start        int
	}{{"all", "", 365}, {"90", "_90", 90}, {"30", "_30", 30}}
	readPin := func(name string, start int) uint64 {
		t.Helper()
		comment := fmt.Sprintf("pool3nn-pin-%s-%d", name, time.Now().UnixNano())
		qctx := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"log_comment": comment}))
		var n uint64
		if err := conn.QueryRow(qctx, "SELECT count() FROM "+testopspooled.SuiteRunsSource, clickhouse.Named("org_id", pool3nnOrg),
			clickhouse.Named("start_date", day(start)), clickhouse.Named("end_date", day(-1))).Scan(&n); err != nil {
			t.Fatalf("pin read %s: %v", name, err)
		}
		if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
			t.Fatal(err)
		}
		types, read := pool3nnLog(ctx, t, conn, comment)
		if len(types) != 1 || types[0] != "QueryFinish" {
			t.Fatalf("pin read %s: query_log types %v", name, types)
		}
		t.Logf("BOUNDCHECK pin read %s: %d source rows, read_rows %d", name, n, read)
		return read
	}
	for _, w := range windows {
		bw := bound(w.bucket)
		read := readPin(w.name, w.start)
		t.Logf("BOUNDCHECK window %s: read_rows %d, B %d (L %d, D %d)", w.name, read, bw, bw-4*d, d)
		if read > bw {
			t.Errorf("BOUNDCHECK window %s: the pin statement read %d rows over B = %d", w.name, read, bw)
		}
		if read < pool3nnUint(t, out["stmt4"], "legacy_repo_rows"+w.bucket) {
			t.Errorf("BOUNDCHECK window %s: read_rows %d under the legacy repo rows %s: the form L does not match the pin read", w.name, read, out["stmt4"]["legacy_repo_rows"+w.bucket])
		}
	}
	if read := readPin("stored-only", 10); read > 4*d+2*8192 {
		t.Errorf("BOUNDCHECK stored-only window: read_rows %d over 4 x D + 2 x 8192 = %d", read, 4*d+2*8192)
	}

	// Mutants. Each is a test copy of one statement with ONE change; each must change the output to its recomputed WANT.
	type mutant struct {
		label, name string
		from, to    string
		want        map[string]string
	}
	newestFlip := func(m map[string]string, extra map[string]string) map[string]string {
		c := map[string]string{}
		for k, v := range m {
			c[k] = v
		}
		for k, v := range extra {
			c[k] = v
		}
		return c
	}
	mutants := []mutant{
		{"stmt1", "org: prefix of another org", "'f0f0f0f0'", "'b0000000'", wantOther["stmt1"]},
		{"stmt1", "rule: no org filter", "startsWith(org_id, 'f0f0f0f0') AND ", "", map[string]string{"orgs": "2", "org_rows": fmt.Sprint(suiteAll)}},
		{"stmt2", "org: prefix of another org", "'f0f0f0f0'", "'b0000000'", wantOther["stmt2"]},
		{"stmt2", "rule: no org filter", "startsWith(org_id, 'f0f0f0f0') AND ", "", map[string]string{"orgs": "2", "org_rows": fmt.Sprint(len(daily))}},
		{"stmt3", "org: prefix of another org", "'f0f0f0f0'", "'b0000000'", wantOther["stmt3"]},
		{"stmt3", "rule: oldest row instead of the newest", "ORDER BY computed_at DESC", "ORDER BY computed_at ASC",
			newestFlip(want["stmt3"], map[string]string{"legacy_repo_days": fmt.Sprint(pool3nnUint(t, want["stmt3"], "legacy_repo_days") + 1),
				"legacy_repo_days_90": fmt.Sprint(pool3nnUint(t, want["stmt3"], "legacy_repo_days_90") + 1),
				"legacy_repo_days_30": fmt.Sprint(pool3nnUint(t, want["stmt3"], "legacy_repo_days_30") + 1)})},
		{"stmt4", "org: prefix of another org", "'f0f0f0f0'", "'b0000000'", wantOther["stmt4"]},
		{"stmt4", "rule: oldest row instead of the newest", "ORDER BY computed_at DESC", "ORDER BY computed_at ASC",
			newestFlip(want["stmt4"], map[string]string{"run_set_rows": fmt.Sprint(pool3nnUint(t, want["stmt4"], "run_set_rows") + uint64(perDay)),
				"run_set_rows_90": fmt.Sprint(pool3nnUint(t, want["stmt4"], "run_set_rows_90") + uint64(perDay)),
				"run_set_rows_30": fmt.Sprint(pool3nnUint(t, want["stmt4"], "run_set_rows_30") + uint64(perDay)),
				"run_set_runs":    fmt.Sprint(pool3nnUint(t, want["stmt4"], "run_set_runs") + pool3nnRuns)})},
		{"stmt4", "rule: run set NOT IN the legacy days", "    AND (repo_id, toDate(coalesce(started_at, finished_at))) IN (", "    AND 1 = 1 AND (repo_id, toDate(coalesce(started_at, finished_at))) NOT IN (", nil},
		{"stmt5", "org: prefix of another org", "'f0f0f0f0'", "'b0000000'", nil},
	}
	for _, m := range mutants {
		text := byLabel[m.label]
		mutated := strings.ReplaceAll(text, m.from, m.to)
		if mutated == text {
			t.Fatalf("MUTANT %s %s: nothing to mutate", m.label, m.name)
		}
		got, _, err := pool3nnRun(ctx, conn, mutated, fmt.Sprintf("pool3nn-mut-%s-%d", m.label, time.Now().UnixNano()))
		if err != nil || len(got) != 1 {
			t.Fatalf("MUTANT %s %s: err=%v rows=%d", m.label, m.name, err, len(got))
		}
		t.Logf("MUTANT %s %s ROW %v", m.label, m.name, got[0])
		if m.want != nil {
			expect("MUTANT", m.label+" "+m.name, got[0], m.want)
		}
		if reflect.DeepEqual(got[0], out[m.label]) {
			t.Errorf("MUTANT %s %s: output equals the clean output", m.label, m.name)
		}
	}
	t.Logf("mutants run: %d", len(mutants))

	// PASS-path read_rows: the data reads of the statement + its gate (2P + 1 per scalar evaluation; logged).
	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatalf("the query log is not readable by this user: %v", err)
	}
	ts, td := size["test_suite_results"][0], size["testops_test_metrics_daily"][0]
	maxRead := map[string]uint64{"stmt1": ts, "stmt2": td, "stmt3": td, "stmt4": 2*ts + 2*td, "stmt5": 2*ts + 2*td}
	for _, label := range []string{"stmt1", "stmt2", "stmt3", "stmt4", "stmt5"} {
		types, read := pool3nnLog(ctx, t, conn, passComments[label])
		t.Logf("PASS %s query_log types=%v read_rows=%d (Ts=%d Td=%d P=%d)", label, types, read, ts, td, parts)
		if len(types) != 1 || types[0] != "QueryFinish" {
			t.Errorf("PASS %s: query_log types %v, want [QueryFinish]", label, types)
		}
		if limit := maxRead[label] + 5*(2*parts+1); read > limit {
			t.Errorf("PASS %s: query_log read_rows %d over the data reads + 5 gate evaluations = %d", label, read, limit)
		}
	}

	// Gate fires: each cap literal lowered to 0 in a test copy. WANT code 395 STOP-COST-GATE before any table read.
	type fired struct{ label, cap, comment string }
	var fires []fired
	gated := []string{"stmt1", "stmt2", "stmt3", "stmt4", "stmt5"}
	sort.Strings(gated)
	for _, label := range gated {
		for _, literal := range []string{shape[label].rows, shape[label].bytes} {
			lowered := strings.Replace(byLabel[label], literal, "> 0", 1)
			comment := fmt.Sprintf("pool3nn-gate-%s-%s-%d", label, strings.TrimPrefix(literal, "> "), time.Now().UnixNano())
			_, _, err := pool3nnRun(ctx, conn, lowered, comment)
			var ex *clickhouse.Exception
			if err == nil || !errors.As(err, &ex) || ex.Code != 395 || !strings.Contains(ex.Message, "STOP-COST-GATE") {
				t.Errorf("GATE %s %s lowered to 0: err = %v, want code 395 STOP-COST-GATE", label, literal, err)
				continue
			}
			t.Logf("GATE %s %s lowered to 0: fired code=%d", label, literal, ex.Code)
			fires = append(fires, fired{label, literal, comment})
		}
	}
	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatalf("the query log is not readable by this user: %v", err)
	}
	for _, f := range fires {
		types, read := pool3nnLog(ctx, t, conn, f.comment)
		t.Logf("GATE %s %s query_log types=%v read_rows=%d bound=%d", f.label, f.cap, types, read, 2*parts+1)
		if len(types) != 1 || types[0] != "ExceptionBeforeStart" {
			t.Errorf("GATE %s %s: query_log types %v, want [ExceptionBeforeStart]", f.label, f.cap, types)
		}
		if read > 2*parts+1 {
			t.Errorf("GATE %s %s: read_rows %d over 2 x P + 1 = %d", f.label, f.cap, read, 2*parts+1)
		}
	}
	if len(fires) != 10 {
		t.Errorf("gates fired %d times, want 10 (5 statements x 2 caps)", len(fires))
	}
}
