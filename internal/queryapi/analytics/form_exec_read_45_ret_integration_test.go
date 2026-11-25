//go:build integration

package analytics

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

// DO NOT MERGE. Throwaway exec proof of a prod read FORM (FORM-45-RET-1). ret45Form is the form file text in its token form
// (<ORG1_8HEX> stays in it); its sha256 is pinned. The token is swapped for a synthetic prefix the way the operator's sed does.
// Synthetic rows only.
const (
	ret45OrgPrefix   = "a1b2c3d4"
	ret45Org         = "a1b2c3d4-0000-4000-8000-00000000000a"
	ret45OtherOrg    = "e5f6a7b8-0000-4000-8000-00000000000b"
	ret45MutantToken = "d0d0d0d0"
	ret45FormSHA256  = "c02a39b2e5cfb47e9ae3603fcef72fdec45244ff2753fa1ee4a97a47e793bcf0"
)

const ret45Form = `-- FORM-45-RET-1 (ClickHouse, prod, read-only). acp-train45, 2026-11-25. DRAFT, NOT EXECUTED, NOT READ BY ACP. query-api roll sq45-a PRE read (rider CHAOS-14595, #7045 35d78984a59a).
--   What: today's count of the repo-days that the fix takes out of the pooled TEST_SUITE_DURATION_P95 raw arm = legacy repo-days (newest row pooled_samples_stored != 1,
--   testopspooled.LegacyRepoDays) whose rows at the newest computed_at are ALL retractions (LiveLegacyRepoDays HAVING tupleElement(max((computed_at, 1 - retracted)), 2) = 0).
--   WANT use: retired_legacy_repo_days = 0 (all 3 buckets) = the fix changes no served answer on prod today = the rider prod arm is NOT EXERCISED by data, decided BEFORE START (not after).
--   > 0 = the fix removes those days from the raw read: the post-roll arm is then FORM-45-QL-1 stmt0 (new-text rows) and a served-equality note (acp rules).
-- Cites at cd2fbc1f543b: internal/queryapi/testopspooled/testopspooled.go LegacyRepoDays :58-60, LiveLegacyRepoDays :62-70 (new), SuiteRunsSource :140, suiteRawRowsRuled :168.
-- Prior fact: P10 stmt3 of STEP 336 (lane-scratch/acp-rec336 RECORD-336-sq39-a-1.md): legacy_repo_days = 0 (all / 90 / 30), org_rows = 274 daily rows.
-- Org rule: startsWith(org_id, '<ORG-1 prefix, 8 hex, read from the P10 FORM of STEP 336>') + ORG-1 WANT 1 (stmt2). Mutant: the org literal changed to a 8-hex that no org holds -> stmt2 orgs = 0.
-- WORK MATH: stmt2 / stmt3 read testops_test_metrics_daily x 2 (the outer read + the HAVING subquery) <= 2 x 9,000,000 + 2P + 1 < 20,000,000. Caps: 9,000,000 rows / 2,000,000,000 bytes (the table has about 274 rows today).
-- No SET / SETTINGS / EXPLAIN. Output: counts only.

-- S0 (system.parts only). WANT 1 row, rows > 0, active_parts > 0.
SELECT table AS t, sum(rows) AS rows, sum(bytes_on_disk) AS bytes, count() AS active_parts
FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('testops_test_metrics_daily')
GROUP BY table ORDER BY table;

-- stmtR replicas (1 row per replica). WANT replicas = 1, else STOP.
SELECT count() AS replicas FROM clusterAllReplicas('default', system.one);

-- stmt2 ORG-1 on testops_test_metrics_daily. WANT orgs = 1, org_rows > 0 (org_rows <= S0 rows). A control > 0.
WITH (SELECT throwIf(
  (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('testops_test_metrics_daily')) > 9000000
  OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('testops_test_metrics_daily')) > 2000000000,
  'STOP-COST-GATE')) AS cost_gate
SELECT uniqExact(org_id) AS orgs, count() AS org_rows, countIf(retracted = 1) AS org_retracted_rows
FROM testops_test_metrics_daily
WHERE startsWith(org_id, '<ORG1_8HEX>') AND cost_gate = 0;

-- stmt3 ORG-1 legacy repo-days and the retired subset the fix drops, per bucket. WANT 1 row; retired_* <= legacy_*; each _90 / _30 <= its all value; retired_legacy_repo_days = 0 = NOT EXERCISED by data.
WITH (SELECT throwIf(
  (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('testops_test_metrics_daily')) > 9000000
  OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('testops_test_metrics_daily')) > 2000000000,
  'STOP-COST-GATE')) AS cost_gate
SELECT count() AS repo_days,
  countIf(pooled_samples_stored != 1) AS legacy_repo_days,
  countIf(pooled_samples_stored != 1 AND (repo_id, day) IN (
    SELECT repo_id, day FROM testops_test_metrics_daily WHERE startsWith(org_id, '<ORG1_8HEX>')
    GROUP BY repo_id, day HAVING tupleElement(max((computed_at, 1 - retracted)), 2) = 0)) AS retired_legacy_repo_days,
  countIf(pooled_samples_stored != 1 AND day >= today() - 90 AND (repo_id, day) IN (
    SELECT repo_id, day FROM testops_test_metrics_daily WHERE startsWith(org_id, '<ORG1_8HEX>')
    GROUP BY repo_id, day HAVING tupleElement(max((computed_at, 1 - retracted)), 2) = 0)) AS retired_legacy_repo_days_90,
  countIf(pooled_samples_stored != 1 AND day >= today() - 30 AND (repo_id, day) IN (
    SELECT repo_id, day FROM testops_test_metrics_daily WHERE startsWith(org_id, '<ORG1_8HEX>')
    GROUP BY repo_id, day HAVING tupleElement(max((computed_at, 1 - retracted)), 2) = 0)) AS retired_legacy_repo_days_30
FROM (
  SELECT org_id, repo_id, day, pooled_samples_stored
  FROM testops_test_metrics_daily
  WHERE startsWith(org_id, '<ORG1_8HEX>') AND cost_gate = 0
  ORDER BY computed_at DESC, cityHash64(tuple(*)) DESC
  LIMIT 1 BY org_id, repo_id, day
);
`

type ret45Statement struct {
	label string
	lines []string // the lines after the marker line up to and including the line that ends in ";"
}

func (s ret45Statement) text() string { return strings.Join(s.lines, "\n") }

// ret45Cut cuts the statements with the awk rule: a marker line starts with "-- S0 " or "-- stmt"; the statement is the lines
// after the marker until a line ending in ";" (that line included).
func ret45Cut(body string) []ret45Statement {
	var out []ret45Statement
	var cur *ret45Statement
	for _, line := range strings.Split(body, "\n") {
		if cur == nil {
			if strings.HasPrefix(line, "-- S0 ") || strings.HasPrefix(line, "-- stmt") {
				cur = &ret45Statement{label: strings.Fields(strings.TrimPrefix(line, "-- "))[0]}
			}
			continue
		}
		cur.lines = append(cur.lines, line)
		if strings.HasSuffix(strings.TrimRight(line, " \t"), ";") {
			out = append(out, *cur)
			cur = nil
		}
	}
	return out
}

// ret45Exec is the text a client sends: the cut without its final ";".
func ret45Exec(text string) string { return strings.TrimSuffix(strings.TrimRight(text, " \t\n"), ";") }

func ret45Swap(text, prefix string) string { return strings.ReplaceAll(text, "<ORG1_8HEX>", prefix) }

// ret45Mutate applies one string replace and fails unless it hit exactly want times (a mutant is never a silent no-op).
func ret45Mutate(t *testing.T, text, old, repl string, want int) string {
	t.Helper()
	if got := strings.Count(text, old); got != want {
		t.Fatalf("mutant: %q found %d times, want %d", old, got, want)
	}
	return strings.ReplaceAll(text, old, repl)
}

func ret45Render(v any) string {
	value := reflect.ValueOf(v)
	for value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return "NULL"
		}
		value = value.Elem()
	}
	return fmt.Sprint(value.Interface())
}

func ret45Run(ctx context.Context, conn driver.Conn, text string, opts ...stdclickhouse.QueryOption) ([]map[string]string, []string, error) {
	rows, err := conn.Query(stdclickhouse.Context(ctx, opts...), text)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	columns := rows.Columns()
	types := rows.ColumnTypes()
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
			row[name] = ret45Render(cells[i])
		}
		got = append(got, row)
	}
	return got, columns, rows.Err()
}

// The fixture. One repo-day = the rows written for (org, repo, day offset back from the server's today()). A row is
// (hour offset of computed_at, pooled_samples_stored, retracted). Rows of one repo-day with the same hour offset are a tie.
type ret45Row struct {
	hour    int
	stored  uint8
	retired uint8
}

type ret45Day struct {
	org  string
	repo int
	off  int
	rows []ret45Row
}

func ret45Fixture() []ret45Day {
	a := ret45Org
	b := ret45OtherOrg
	return []ret45Day{
		// (a) LIVE legacy.
		{a, 1, 10, []ret45Row{{0, 0, 0}}},
		{a, 1, 20, []ret45Row{{0, 0, 0}, {1, 2, 0}}},
		// (b) RETIRED legacy: the newest rows are all retractions. Window edges: 5 (30), 50 (90 only), 120 (all only), 8, 15 (stored 2),
		// 30 / 31 and 90 / 91 (the window boundaries), 1 (the window-mutant day).
		{a, 2, 5, []ret45Row{{0, 0, 0}, {1, 0, 1}}},
		{a, 2, 50, []ret45Row{{0, 0, 0}, {1, 0, 1}}},
		{a, 2, 120, []ret45Row{{0, 0, 0}, {1, 0, 1}}},
		{a, 3, 8, []ret45Row{{0, 0, 1}}},
		{a, 5, 15, []ret45Row{{0, 2, 1}}},
		{a, 5, 30, []ret45Row{{0, 0, 1}}},
		{a, 5, 31, []ret45Row{{0, 0, 1}}},
		{a, 5, 90, []ret45Row{{0, 0, 1}}},
		{a, 5, 91, []ret45Row{{0, 0, 1}}},
		{a, 2, 1, []ret45Row{{0, 0, 1}}},
		// (c) MIXED same-computed_at tie of a live row and a retraction: kept, not retired.
		{a, 3, 12, []ret45Row{{1, 0, 0}, {1, 0, 1}}},
		{a, 3, 70, []ret45Row{{0, 0, 0}, {1, 3, 0}, {1, 0, 1}}},
		// (d) non-legacy (pooled_samples_stored = 1): a live one, and one whose newest row is a retraction.
		{a, 4, 3, []ret45Row{{0, 1, 0}}},
		{a, 4, 4, []ret45Row{{0, 1, 0}, {1, 1, 1}}},
		// a day measured again after its marker: live.
		{a, 4, 6, []ret45Row{{0, 0, 0}, {1, 0, 1}, {2, 0, 0}}},
		// (f) the second org: the same repo id as org A's repo 1 with a NEWER retraction, and its own days.
		{b, 1, 10, []ret45Row{{5, 0, 1}}},
		{b, 6, 5, []ret45Row{{0, 0, 1}}},
		{b, 6, 6, []ret45Row{{0, 0, 0}}},
		{b, 6, 40, []ret45Row{{0, 0, 1}}},
	}
}

func ret45Repo(n int) uuid.UUID {
	return uuid.MustParse(fmt.Sprintf("45000000-0000-4000-8000-%012d", n))
}

type ret45Want struct {
	orgs, orgRows, orgRetracted                     int
	repoDays, legacy, retired, retired90, retired30 int
}

// ret45Recompute is the independent Go recompute of the stmt2 / stmt3 answers for the org prefix. Per repo-day it takes the
// newest computed_at; the legacy flag is pooled_samples_stored != 1 of the row there (a tie must agree, or the fixture is
// ambiguous); the repo-day is retired when EVERY row at the newest computed_at is a retraction. complement = rule mutant A
// (HAVING = 1 instead of = 0); storedEq1 = rule mutant B (the legacy predicate of the retired count is = 1); window90 = the
// _90 column's day limit.
func ret45Recompute(fixture []ret45Day, prefix string, complement, storedEq1 bool, window90 int) ret45Want {
	var w ret45Want
	orgs := map[string]bool{}
	for _, d := range fixture {
		if !strings.HasPrefix(d.org, prefix) {
			continue
		}
		orgs[d.org] = true
		maxHour := -1
		for _, r := range d.rows {
			w.orgRows++
			w.orgRetracted += int(r.retired)
			if r.hour > maxHour {
				maxHour = r.hour
			}
		}
		allRetired := true
		legacySeen := map[bool]bool{}
		for _, r := range d.rows {
			if r.hour != maxHour {
				continue
			}
			legacySeen[r.stored != 1] = true
			if r.retired != 1 {
				allRetired = false
			}
		}
		if len(legacySeen) != 1 {
			panic("ret45 fixture: a tie with disagreeing legacy flags")
		}
		legacy := legacySeen[true]
		w.repoDays++
		if legacy {
			w.legacy++
		}
		inSet := allRetired
		if complement {
			inSet = !allRetired
		}
		pred := legacy
		if storedEq1 {
			pred = !legacy
		}
		if pred && inSet {
			w.retired++
			if d.off <= window90 {
				w.retired90++
			}
			if d.off <= 30 {
				w.retired30++
			}
		}
	}
	w.orgs = len(orgs)
	return w
}

func ret45Num(t *testing.T, row map[string]string, name string) int {
	t.Helper()
	var n int
	if _, err := fmt.Sscan(row[name], &n); err != nil {
		t.Fatalf("%s = %q: %v", name, row[name], err)
	}
	return n
}

func TestForm45RetExecutesAtTheChainHead(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	admin, _ := crossorg.Start(ctx, t)

	statements := ret45Cut(ret45Form)
	byLabel := map[string]ret45Statement{}
	var labels []string
	for _, s := range statements {
		labels = append(labels, s.label)
		byLabel[s.label] = s
	}
	cell := func(name string, fn func(t *testing.T)) {
		t.Run(name, func(t *testing.T) {
			fn(t)
			t.Logf("CELL45RET %s PASS", name)
		})
	}
	run := func(t *testing.T, label, prefix string, opts ...stdclickhouse.QueryOption) ([]map[string]string, []string, error) {
		t.Helper()
		return ret45Run(ctx, admin, ret45Exec(ret45Swap(byLabel[label].text(), prefix)), opts...)
	}
	mustRow := func(t *testing.T, label, text string) (map[string]string, []string) {
		t.Helper()
		rows, cols, err := ret45Run(ctx, admin, ret45Exec(text))
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		if len(rows) != 1 {
			t.Fatalf("%s: %d rows, want 1", label, len(rows))
		}
		return rows[0], cols
	}

	// Seed (before any statement runs). Merges stopped; one INSERT per row so a tie stays two rows in two parts.
	var today time.Time
	if err := admin.QueryRow(ctx, "SELECT today()").Scan(&today); err != nil {
		t.Fatal(err)
	}
	if err := admin.Exec(ctx, "SYSTEM STOP MERGES testops_test_metrics_daily"); err != nil {
		t.Fatal(err)
	}
	fixture := ret45Fixture()
	seeded := 0
	for _, d := range fixture {
		day := today.AddDate(0, 0, -d.off)
		for _, r := range d.rows {
			at := time.Date(today.Year(), today.Month(), today.Day(), 10, 0, 0, 0, time.UTC).Add(time.Duration(r.hour) * time.Hour)
			if err := admin.Exec(ctx, `INSERT INTO testops_test_metrics_daily (repo_id, day, team_id, org_id, computed_at, pooled_samples_stored, retracted)
VALUES (?, ?, 'team-45', ?, ?, ?, ?)`, ret45Repo(d.repo), day, d.org, at, r.stored, r.retired); err != nil {
				t.Fatalf("seed: %v", err)
			}
			seeded++
		}
	}
	base := ret45Recompute(fixture, ret45OrgPrefix, false, false, 90)
	// Hand-checked constants (counted by eye from ret45Fixture, org A): 17 repo-days, 27 rows, 14 retraction rows, 15 legacy
	// repo-days (all but the two stored = 1 days), 10 retired legacy (<= 90 days: 8, <= 30 days: 5).
	if base != (ret45Want{orgs: 1, orgRows: 27, orgRetracted: 14, repoDays: 17, legacy: 15, retired: 10, retired90: 8, retired30: 5}) {
		t.Fatalf("Go recompute %+v differs from the hand-checked constants", base)
	}

	cell("pin", func(t *testing.T) {
		sum := sha256.Sum256([]byte(ret45Form))
		if got := hex.EncodeToString(sum[:]); got != ret45FormSHA256 {
			t.Fatalf("form sha256 = %s, want %s", got, ret45FormSHA256)
		}
		if strings.Join(labels, ",") != "S0,stmtR,stmt2,stmt3" {
			t.Fatalf("form statements = %v", labels)
		}
		for _, s := range statements {
			semis := 0
			for _, line := range s.lines {
				if strings.HasPrefix(line, "--") {
					t.Fatalf("%s: a cut line starts with --: %q", s.label, line)
				}
				if strings.HasSuffix(strings.TrimRight(line, " \t"), ";") {
					semis++
				}
			}
			if semis != 1 || !strings.HasSuffix(strings.TrimRight(s.lines[len(s.lines)-1], " \t"), ";") {
				t.Fatalf("%s: %d ';' lines, want exactly one and it last", s.label, semis)
			}
			t.Logf("cut %s: %d lines", s.label, len(s.lines))
		}
		if got := strings.Count(ret45Form, "<ORG1_8HEX>"); got != 5 {
			t.Fatalf("token <ORG1_8HEX> found %d times in the form, want 5", got)
		}
	})

	cell("shape", func(t *testing.T) {
		banned := regexp.MustCompile(`(?i)\b(SET|SETTINGS|EXPLAIN)\b`)
		for _, s := range statements {
			if m := banned.FindString(s.text()); m != "" {
				t.Fatalf("%s holds %q", s.label, m)
			}
		}
		want := map[string]string{
			"S0":    "t,rows,bytes,active_parts",
			"stmtR": "replicas",
			"stmt2": "orgs,org_rows,org_retracted_rows",
			"stmt3": "repo_days,legacy_repo_days,retired_legacy_repo_days,retired_legacy_repo_days_90,retired_legacy_repo_days_30",
		}
		for label, cols := range want {
			_, got, err := run(t, label, ret45OrgPrefix)
			if err != nil {
				t.Fatalf("%s: %v", label, err)
			}
			if strings.Join(got, ",") != cols {
				t.Fatalf("%s columns = %v, want %s", label, got, cols)
			}
		}
	})

	cell("S0", func(t *testing.T) {
		rows, _, err := run(t, "S0", ret45OrgPrefix)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 1 || rows[0]["t"] != "testops_test_metrics_daily" {
			t.Fatalf("S0 rows = %v, want 1 row of testops_test_metrics_daily", rows)
		}
		if n := ret45Num(t, rows[0], "rows"); n < seeded {
			t.Fatalf("S0 rows = %d, want >= %d seeded", n, seeded)
		}
		if ret45Num(t, rows[0], "active_parts") <= 0 || ret45Num(t, rows[0], "bytes") <= 0 {
			t.Fatalf("S0 = %v, want active_parts > 0 and bytes > 0", rows[0])
		}
	})

	cell("stmtR", func(t *testing.T) {
		rows, _, err := run(t, "stmtR", ret45OrgPrefix)
		if err != nil || len(rows) != 1 || rows[0]["replicas"] != "1" {
			t.Fatalf("stmtR = %v, %v, want replicas = 1", rows, err)
		}
	})

	check2 := func(t *testing.T, row map[string]string, w ret45Want) {
		t.Helper()
		if got := (ret45Want{orgs: ret45Num(t, row, "orgs"), orgRows: ret45Num(t, row, "org_rows"), orgRetracted: ret45Num(t, row, "org_retracted_rows")}); got.orgs != w.orgs || got.orgRows != w.orgRows || got.orgRetracted != w.orgRetracted {
			t.Fatalf("stmt2 = %v, want orgs %d org_rows %d org_retracted_rows %d", row, w.orgs, w.orgRows, w.orgRetracted)
		}
	}
	check3 := func(t *testing.T, row map[string]string, w ret45Want) {
		t.Helper()
		got := ret45Want{repoDays: ret45Num(t, row, "repo_days"), legacy: ret45Num(t, row, "legacy_repo_days"), retired: ret45Num(t, row, "retired_legacy_repo_days"),
			retired90: ret45Num(t, row, "retired_legacy_repo_days_90"), retired30: ret45Num(t, row, "retired_legacy_repo_days_30")}
		if got.repoDays != w.repoDays || got.legacy != w.legacy || got.retired != w.retired || got.retired90 != w.retired90 || got.retired30 != w.retired30 {
			t.Fatalf("stmt3 = %v, want repo_days %d legacy %d retired %d retired_90 %d retired_30 %d", row, w.repoDays, w.legacy, w.retired, w.retired90, w.retired30)
		}
	}

	cell("stmt2", func(t *testing.T) {
		row, _ := mustRow(t, "stmt2", ret45Swap(byLabel["stmt2"].text(), ret45OrgPrefix))
		check2(t, row, base)
		if other := ret45Recompute(fixture, "e5f6a7b8", false, false, 90); other.orgRows == base.orgRows {
			t.Fatalf("the other org holds %d rows like org A: the control cannot tell them apart", other.orgRows)
		}
	})

	cell("stmt3", func(t *testing.T) {
		row, _ := mustRow(t, "stmt3", ret45Swap(byLabel["stmt3"].text(), ret45OrgPrefix))
		check3(t, row, base)
		// The three buckets are ordered and the retired count never passes the legacy count.
		if !(base.retired30 <= base.retired90 && base.retired90 <= base.retired && base.retired <= base.legacy && base.legacy <= base.repoDays) {
			t.Fatalf("expected numbers out of order: %+v", base)
		}
	})

	cell("mutant_org", func(t *testing.T) {
		row2, _ := mustRow(t, "stmt2", ret45Swap(byLabel["stmt2"].text(), ret45MutantToken))
		if row2["orgs"] != "0" || row2["org_rows"] != "0" || row2["org_retracted_rows"] != "0" {
			t.Fatalf("org mutant stmt2 = %v, want all 0", row2)
		}
		row3, _ := mustRow(t, "stmt3", ret45Swap(byLabel["stmt3"].text(), ret45MutantToken))
		for name, v := range row3 {
			if v != "0" {
				t.Fatalf("org mutant stmt3 %s = %s, want 0 (%v)", name, v, row3)
			}
		}
	})

	stmt3 := ret45Swap(byLabel["stmt3"].text(), ret45OrgPrefix)
	cell("mutant_rule_a", func(t *testing.T) {
		// The HAVING sits in the three retired columns (all / 90 / 30): all three flip to = 1 (3 hits), so each column is the
		// complement of the live-newest set.
		text := ret45Mutate(t, stmt3, "HAVING tupleElement(max((computed_at, 1 - retracted)), 2) = 0", "HAVING tupleElement(max((computed_at, 1 - retracted)), 2) = 1", 3)
		row, _ := mustRow(t, "stmt3", text)
		want := ret45Recompute(fixture, ret45OrgPrefix, true, false, 90)
		check3(t, row, want)
		if want.retired == base.retired {
			t.Fatalf("rule mutant A does not move the retired count (%d)", want.retired)
		}
	})

	cell("mutant_rule_b", func(t *testing.T) {
		text := ret45Mutate(t, stmt3, "countIf(pooled_samples_stored != 1 AND (repo_id, day) IN (", "countIf(pooled_samples_stored = 1 AND (repo_id, day) IN (", 1)
		row, _ := mustRow(t, "stmt3", text)
		want := ret45Recompute(fixture, ret45OrgPrefix, false, true, 90)
		if ret45Num(t, row, "retired_legacy_repo_days") != want.retired || want.retired == base.retired {
			t.Fatalf("rule mutant B retired = %s, want %d (not %d)", row["retired_legacy_repo_days"], want.retired, base.retired)
		}
		// The other columns keep their rule.
		if ret45Num(t, row, "legacy_repo_days") != base.legacy || ret45Num(t, row, "retired_legacy_repo_days_90") != base.retired90 {
			t.Fatalf("rule mutant B moved a column it does not touch: %v", row)
		}
	})

	cell("mutant_window", func(t *testing.T) {
		text := ret45Mutate(t, stmt3, "day >= today() - 90", "day >= today() - 1", 1)
		row, _ := mustRow(t, "stmt3", text)
		want := ret45Recompute(fixture, ret45OrgPrefix, false, false, 1)
		if ret45Num(t, row, "retired_legacy_repo_days_90") != want.retired90 || want.retired90 >= base.retired90 {
			t.Fatalf("window mutant _90 = %s, want %d (< %d)", row["retired_legacy_repo_days_90"], want.retired90, base.retired90)
		}
		if ret45Num(t, row, "retired_legacy_repo_days") != base.retired || ret45Num(t, row, "retired_legacy_repo_days_30") != base.retired30 {
			t.Fatalf("window mutant moved a column it does not touch: %v", row)
		}
	})

	cell("gate", func(t *testing.T) {
		for _, label := range []string{"stmt2", "stmt3"} {
			real := ret45Swap(byLabel[label].text(), ret45OrgPrefix)
			if _, _, err := ret45Run(ctx, admin, ret45Exec(real)); err != nil {
				t.Fatalf("%s control (real caps): %v", label, err)
			}
			capped := ret45Mutate(t, real, "> 9000000", "> 0", 1)
			_, _, err := ret45Run(ctx, admin, ret45Exec(capped))
			var ex *stdclickhouse.Exception
			if !errors.As(err, &ex) || ex.Code != 395 || !strings.Contains(ex.Message, "STOP-COST-GATE") {
				t.Fatalf("%s at cap 0: err = %v, want ClickHouse code 395 with STOP-COST-GATE", label, err)
			}
			t.Logf("%s at cap 0: code %d", label, ex.Code)
		}
	})

	cell("readbudget", func(t *testing.T) {
		queryID := "ret45-" + uuid.NewString()
		if _, _, err := run(t, "stmt3", ret45OrgPrefix, stdclickhouse.WithQueryID(queryID)); err != nil {
			t.Fatal(err)
		}
		if err := admin.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
			t.Fatal(err)
		}
		var readRows uint64
		if err := admin.QueryRow(ctx, `SELECT read_rows FROM system.query_log WHERE query_id = ? AND type = 'QueryFinish'`, queryID).Scan(&readRows); err != nil {
			t.Fatalf("query_log: %v", err)
		}
		var systemParts uint64
		if err := admin.QueryRow(ctx, "SELECT count() FROM system.parts").Scan(&systemParts); err != nil {
			t.Fatal(err)
		}
		t.Logf("stmt3 read_rows = %d, seeded = %d, system.parts rows = %d", readRows, seeded, systemParts)
		if readRows >= 20000000 {
			t.Fatalf("stmt3 read_rows = %d, want < 20000000", readRows)
		}
		// Slack: the two gate reads of system.parts (every active part, upper bound) + system.one (1).
		slack := 2*systemParts + 1
		if limit := uint64(2*seeded) + slack; readRows > limit {
			t.Errorf("stmt3 read_rows = %d > 2 x %d seeded + slack %d = %d (the form assumes 2 reads of the table)", readRows, seeded, slack, limit)
		}
	})

}
