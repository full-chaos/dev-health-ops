//go:build integration

package backfillrun

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// DO NOT MERGE. This file executes FORM-14190-DAILY-4.sql in one READ ONLY transaction on a fresh database at the head
// chain. The text below is the .sql with its org prefix replaced by {{ORG_PREFIX}} and its sync run prefix by
// {{RUN_PREFIX}}, nothing else (its sha256 is checked); the cell puts synthetic values in at run time and checks that
// every org-scoped FROM got the prefix. With synthetic daily runs it wants the pass set to pass every WANT, each mutant
// row set to fail its WANT, and the catalog and index gate to fire (pm-guard FORM-TEMPLATE-3, PostgreSQL form).
const (
	formExecDaily14190SHA256  = "761a4581087e5fec305c7f45007c2659e41d95a4d39b83123cb1955fc7c7f9b6"
	formExecDaily14190OrgSlot = "{{ORG_PREFIX}}"
	formExecDaily14190RunSlot = "{{RUN_PREFIX}}"
	formExecDaily14190SQL     = `-- FORM-14190-DAILY-4 (lane-14190c, 2026-11-23, ops pin a7febdf50002). PostgreSQL prod, READ ONLY, CHAOS-14190.
-- The daily runs of the backfill (sync run prefix {{RUN_PREFIX}}, T0 2026-11-23 14:50:56 UTC) are terminal. Run it AFTER
-- FORM-14190-CH-1 (its pending touched days before 2026-11-09 = 0). The fan-out writes at most 46 runs under
-- 'post-sync:' || sync_run_id (D2); the older touched days are run by the touched-day drain or other syncs under other
-- generations: D4-2 reads, per key (day, repository) before 2026-11-09, the NEWEST OWNER run (the newest post-sync or
-- drain run that lists the key; a run of every repository lists every key of its day), as the drain does
-- (internal/jobs/metrics/daily/touched_drain.go:170-240). Replaces FORM-14190-DAILY-3 (0d1b523b995c) per acp
-- READ-FORM-14190-CH1-DAILY3-1 (cc82c215f379): P1-A (D4 -> D4-2), P3-D (D2 last_day WANT). WANT text and work math:
-- FORM-14190-DAILY-4.md. Shape: pm-guard/FORM-TEMPLATE-3.md (sha12 6b9e73a3c45a), PostgreSQL
-- form (b). Run line: psql -X -v ON_ERROR_STOP=1 -f FORM-14190-DAILY-4.sql (on the primary; one transaction).
-- Done run = status IN ('succeeded', 'no_repositories') AND finalization_status = 'succeeded'.
-- Failed run = status IN ('failed', 'canceled') (terminal). Open run = neither: pending, running, and running with
-- finalization 'failed' (a released finalize that ClaimFinalize takes again; INFO retrying).
-- Output: counts, days and UTC minutes only. Org prefix {{ORG_PREFIX}} on every FROM with org_id: a uuid range (it lets the
-- unique index uq_daily_metrics_run_generation (org_id, target_day, generation) bound the scan) and starts_with.

BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;

-- G0 catalog cost gate (reads pg_catalog only). WANT cost_gate_ok = 0. Caps (reltuples / heap + TOAST bytes):
-- organizations 100000 / 268435456, daily_metrics_runs 1000000 / 1073741824, daily_metrics_partitions 5000000 /
-- 4294967296. Indexes uq_daily_metrics_run_generation (daily_metrics_runs) and uq_daily_metrics_partition_ordinal
-- (daily_metrics_partitions, run_id first) must both be found, indisvalid AND indisready. ORG-1: organizations with the prefix = 1. A table or the
-- index missing, a table never analyzed (reltuples -1), over a cap, the index not valid or not ready, or orgs != 1 =
-- the named cast error STOP-DAILY-G0.
WITH bs AS (SELECT current_setting('block_size')::bigint AS block_size),
rel AS (
    SELECT c.relname::text AS relname, c.reltuples::bigint AS reltuples,
           c.relpages::bigint * bs.block_size + COALESCE(t.relpages::bigint, 0) * bs.block_size AS bytes
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    CROSS JOIN bs
    LEFT JOIN pg_catalog.pg_class t ON t.oid = c.reltoastrelid
    WHERE n.nspname = 'public' AND c.relkind = 'r' AND c.relname IN ('organizations', 'daily_metrics_runs', 'daily_metrics_partitions')
),
caps (relname, max_rows, max_bytes) AS (
    VALUES ('organizations', 100000::bigint, 268435456::bigint),
           ('daily_metrics_runs', 1000000::bigint, 1073741824::bigint),
           ('daily_metrics_partitions', 5000000::bigint, 4294967296::bigint)
),
idx AS (
    SELECT count(*) AS found, bool_and(i.indisvalid AND i.indisready) AS usable
    FROM pg_catalog.pg_index i
    JOIN pg_catalog.pg_class ic ON ic.oid = i.indexrelid
    JOIN pg_catalog.pg_class tc ON tc.oid = i.indrelid
    JOIN pg_catalog.pg_namespace n ON n.oid = ic.relnamespace
    WHERE n.nspname = 'public'
      AND ((ic.relname = 'uq_daily_metrics_run_generation' AND tc.relname = 'daily_metrics_runs')
           OR (ic.relname = 'uq_daily_metrics_partition_ordinal' AND tc.relname = 'daily_metrics_partitions'))
),
orgs AS (
    SELECT count(*) AS n FROM public.organizations o WHERE starts_with(o.id::text, '{{ORG_PREFIX}}')
),
checked AS (
    SELECT count(rel.relname) AS tables,
           bool_and(rel.reltuples >= 0 AND rel.reltuples <= caps.max_rows AND rel.bytes <= caps.max_bytes) AS within,
           string_agg(caps.relname || '=' || COALESCE(rel.reltuples::text, 'missing') || '/'
                      || COALESCE(rel.bytes::text, 'missing'), ' ' ORDER BY caps.relname) AS sizes
    FROM caps LEFT JOIN rel ON rel.relname = caps.relname
)
SELECT CASE WHEN checked.tables = 3 AND checked.within AND idx.found = 2 AND idx.usable AND orgs.n = 1
            THEN 0
            ELSE ('STOP-DAILY-G0 ' || checked.sizes || ' indexes found=' || idx.found::text
                  || ' usable=' || COALESCE(idx.usable::text, 'missing') || ' orgs=' || orgs.n::text)::int
       END AS cost_gate_ok
FROM checked CROSS JOIN idx CROSS JOIN orgs;

-- C0 INFO the sizes the gate read (pg_catalog only), for the work math. WANT 3 rows.
SELECT c.relname, c.reltuples::bigint AS reltuples, c.relpages::bigint AS heap_pages
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relkind = 'r' AND c.relname IN ('organizations', 'daily_metrics_runs', 'daily_metrics_partitions')
ORDER BY c.relname;

-- S0 ORG-1, INFO (the STOP is in G0: orgs = 1 in its CASE).
SELECT count(*) AS orgs
FROM public.organizations o
WHERE starts_with(o.id::text, '{{ORG_PREFIX}}');

-- D1 INFO the org's daily runs created since T0, by generation class x status x finalization_status: runs, first and
-- last target day, first and last created_at minute (UTC). Classes: this_backfill ('post-sync:' + the run prefix {{RUN_PREFIX}}), post_sync_other, touched_drain,
-- fixed_schedule, manual_daily, ext_recompute, other.
SELECT CASE WHEN r.generation LIKE 'post-sync:{{RUN_PREFIX}}%' THEN 'this_backfill'
            WHEN r.generation LIKE 'post-sync:%' THEN 'post_sync_other'
            WHEN r.generation LIKE 'touched-drain:%' THEN 'touched_drain'
            WHEN r.generation LIKE 'fixed-schedule:%' THEN 'fixed_schedule'
            WHEN r.generation LIKE 'manual-daily:%' THEN 'manual_daily'
            WHEN r.generation LIKE 'ext-recompute:%' THEN 'ext_recompute'
            ELSE 'other' END AS generation_class,
       r.status, r.finalization_status, count(*) AS runs,
       min(r.target_day) AS first_day, max(r.target_day) AS last_day,
       to_char(min(r.created_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI') AS first_created_minute,
       to_char(max(r.created_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI') AS last_created_minute
FROM public.daily_metrics_runs r
WHERE r.org_id >= '{{ORG_PREFIX}}-0000-0000-0000-000000000000'::uuid AND r.org_id <= '{{ORG_PREFIX}}-ffff-ffff-ffff-ffffffffffff'::uuid
  AND starts_with(r.org_id::text, '{{ORG_PREFIX}}') AND r.created_at >= '2026-11-23 14:50:56+00'::timestamptz
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;

-- D2 the backfill's own daily runs (generation 'post-sync:' + the run prefix {{RUN_PREFIX}}). WANT total > 0 (control: the
-- fan-out happened), total <= 46 (1 + 14 window days + 31 touched days; more = the model is wrong = STOP), open = 0,
-- failed = 0, last_day = 2026-11-23 (the fan-out's target day: it fixes the 2026-11-09 bound of CH-1 K1 and D4-2). INFO done, retrying, no_repositories, blocked, first and last day, first and last created_at minute,
-- last update minute (UTC).
SELECT count(*) AS total,
       count(*) FILTER (WHERE r.status IN ('succeeded', 'no_repositories') AND r.finalization_status = 'succeeded') AS done,
       count(*) FILTER (WHERE NOT (r.status IN ('succeeded', 'no_repositories') AND r.finalization_status = 'succeeded')
                          AND r.status NOT IN ('failed', 'canceled')) AS open,
       count(*) FILTER (WHERE r.status IN ('failed', 'canceled')) AS failed,
       count(*) FILTER (WHERE r.status = 'running' AND r.finalization_status = 'failed') AS retrying,
       count(*) FILTER (WHERE r.status = 'no_repositories') AS no_repositories,
       count(*) FILTER (WHERE r.blocked_at IS NOT NULL) AS blocked,
       min(r.target_day) AS first_day, max(r.target_day) AS last_day,
       COALESCE(to_char(min(r.created_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI'), '') AS first_created_minute,
       COALESCE(to_char(max(r.created_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI'), '') AS last_created_minute,
       COALESCE(to_char(max(r.updated_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI'), '') AS last_update_minute
FROM public.daily_metrics_runs r
WHERE r.org_id >= '{{ORG_PREFIX}}-0000-0000-0000-000000000000'::uuid AND r.org_id <= '{{ORG_PREFIX}}-ffff-ffff-ffff-ffffffffffff'::uuid
  AND starts_with(r.org_id::text, '{{ORG_PREFIX}}') AND r.created_at >= '2026-11-23 14:50:56+00'::timestamptz
  AND r.generation LIKE 'post-sync:{{RUN_PREFIX}}%';

-- D3 INFO every other daily run of the org created since T0 (not the backfill's generation): total, open, failed.
-- Not a WANT: hourly syncs and the nightly fan-out keep adding runs; an open one there does not hold the read-back.
SELECT count(*) AS total,
       count(*) FILTER (WHERE NOT (r.status IN ('succeeded', 'no_repositories') AND r.finalization_status = 'succeeded')
                          AND r.status NOT IN ('failed', 'canceled')) AS open,
       count(*) FILTER (WHERE r.status IN ('failed', 'canceled')) AS failed
FROM public.daily_metrics_runs r
WHERE r.org_id >= '{{ORG_PREFIX}}-0000-0000-0000-000000000000'::uuid AND r.org_id <= '{{ORG_PREFIX}}-ffff-ffff-ffff-ffffffffffff'::uuid
  AND starts_with(r.org_id::text, '{{ORG_PREFIX}}') AND r.created_at >= '2026-11-23 14:50:56+00'::timestamptz
  AND r.generation NOT LIKE 'post-sync:{{RUN_PREFIX}}%';

-- D4-2 the keys (day, repository) of days before 2026-11-09 (outside the fan-out's window runs). Marking runs = runs of
-- the org created since T0 with generation 'post-sync:%' or 'touched-drain:%' (the only runs whose start marks keys
-- dispatched, touched_drain.go:147-150). A run lists the repositories of its daily_metrics_partitions.repo_ids; a run
-- of every repository (full_org) lists every key of its day (key repository '*' = the day itself). The OWNER of a key
-- = the newest (created_at, id) marking run of its day that lists it. WANT keys > 0, days >= 161 (every day
-- 2026-06-01..2026-11-08 is a backfill window day) and keys_owner_not_done = 0. INFO owner open / failed / retrying,
-- full-org days, the first and last day.
WITH m AS (
    SELECT r.id, r.target_day, r.full_org, r.created_at,
           (r.status IN ('succeeded', 'no_repositories') AND r.finalization_status = 'succeeded') AS done,
           (r.status IN ('failed', 'canceled')) AS failed,
           (r.status = 'running' AND r.finalization_status = 'failed') AS retrying
    FROM public.daily_metrics_runs r
    WHERE r.org_id >= '{{ORG_PREFIX}}-0000-0000-0000-000000000000'::uuid AND r.org_id <= '{{ORG_PREFIX}}-ffff-ffff-ffff-ffffffffffff'::uuid
      AND starts_with(r.org_id::text, '{{ORG_PREFIX}}') AND r.created_at >= '2026-11-23 14:50:56+00'::timestamptz
      AND r.target_day < '2026-11-09'::date
      AND (r.generation LIKE 'post-sync:%' OR r.generation LIKE 'touched-drain:%')
),
listed AS (
    SELECT DISTINCT m.id AS run_id, m.target_day AS day, x.repo AS repo
    FROM m
    JOIN public.daily_metrics_partitions part ON part.run_id = m.id
    CROSS JOIN LATERAL json_array_elements_text(part.repo_ids) AS x(repo)
),
key_set AS (
    SELECT DISTINCT day, repo FROM listed
    UNION
    SELECT DISTINCT target_day, '*' FROM m WHERE m.full_org
),
owners AS (
    SELECT DISTINCT ON (k.day, k.repo) k.day, k.repo, m.done, m.failed, m.retrying
    FROM key_set k
    JOIN m ON m.target_day = k.day
    LEFT JOIN listed l ON l.run_id = m.id AND l.repo = k.repo
    WHERE m.full_org OR l.run_id IS NOT NULL
    ORDER BY k.day, k.repo, m.created_at DESC, m.id DESC
)
SELECT count(*) AS keys,
       count(DISTINCT day) AS days,
       count(*) FILTER (WHERE NOT done) AS keys_owner_not_done,
       count(*) FILTER (WHERE NOT done AND NOT failed) AS keys_owner_open,
       count(*) FILTER (WHERE failed) AS keys_owner_failed,
       count(*) FILTER (WHERE retrying) AS keys_owner_retrying,
       count(DISTINCT day) FILTER (WHERE repo = '*') AS full_org_days,
       min(day) AS first_day,
       max(day) AS last_day
FROM owners;

ROLLBACK;
`
	formExecDaily14190OrgPrefix = "f0f0f0f0"
	formExecDaily14190RunPrefix = "b1b1b1b1"
)

type formExecDaily14190Piece struct {
	id, text string
}

// formExecDaily14190SQLLines is the SQL of a piece without its comment lines.
func formExecDaily14190SQLLines(text string) string {
	var out strings.Builder
	for _, line := range strings.Split(text, "\n") {
		if !strings.HasPrefix(line, "--") {
			out.WriteString(line + "\n")
		}
	}
	return out.String()
}

// formExecDaily14190Pieces splits the FORM as psql runs it: a statement ends at a SQL line (not a "--" comment line)
// that ends with ";". A data statement is named by its first comment line "-- <id> "; BEGIN and ROLLBACK by their word.
func formExecDaily14190Pieces(t *testing.T, text string) []formExecDaily14190Piece {
	t.Helper()
	statementID := regexp.MustCompile(`^(S0|C0|G0|D[1-3]|D4-2)$`)
	var pieces []formExecDaily14190Piece
	seen := map[string]bool{}
	var current []string
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, "--") || !strings.HasSuffix(strings.TrimRight(line, " "), ";") {
			current = append(current, line)
			continue
		}
		current = append(current, strings.TrimSuffix(strings.TrimRight(line, " "), ";"))
		statement := strings.TrimSpace(strings.Join(current, "\n"))
		current = nil
		id := ""
		for _, commentLine := range strings.Split(statement, "\n") {
			if fields := strings.Fields(commentLine); strings.HasPrefix(commentLine, "--") && id == "" && len(fields) > 1 && statementID.MatchString(fields[1]) {
				id = fields[1]
			}
		}
		sqlText := strings.TrimSpace(formExecDaily14190SQLLines(statement))
		switch {
		case strings.HasPrefix(sqlText, "BEGIN "):
			id = "BEGIN"
		case sqlText == "ROLLBACK":
			id = "ROLLBACK"
		}
		if id == "" || seen[id] {
			t.Fatalf("a piece with no id, or id %q twice:\n%s", id, statement)
		}
		seen[id] = true
		pieces = append(pieces, formExecDaily14190Piece{id: id, text: statement})
	}
	if strings.TrimSpace(formExecDaily14190SQLLines(strings.Join(current, "\n"))) != "" {
		t.Fatalf("SQL after the last statement:\n%s", strings.Join(current, "\n"))
	}
	return pieces
}

// formExecDaily14190Banned names what a read FORM must not hold: a SET statement (SET at the start of a line or after
// ";", any case) and a write or plan keyword as a whole word.
func formExecDaily14190Banned(sqlText string) []string {
	setStatement := regexp.MustCompile(`(?im)(^|;)\s*SET\s`)
	keyword := regexp.MustCompile(`(?i)\b(INSERT|UPDATE|DELETE|EXPLAIN|COPY|TRUNCATE|ALTER|CREATE|DROP)\b`)
	var found []string
	if setStatement.MatchString(sqlText) {
		found = append(found, "SET statement")
	}
	return append(found, keyword.FindAllString(sqlText, -1)...)
}

type formExecDaily14190Result struct {
	columns []string
	rows    [][]string
}

func formExecDaily14190Query(ctx context.Context, conn *pgx.Conn, statement string) (formExecDaily14190Result, error) {
	rows, err := conn.Query(ctx, statement)
	if err != nil {
		return formExecDaily14190Result{}, err
	}
	defer rows.Close()
	var result formExecDaily14190Result
	for _, field := range rows.FieldDescriptions() {
		result.columns = append(result.columns, field.Name)
	}
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return formExecDaily14190Result{}, err
		}
		row := make([]string, len(values))
		for index, value := range values {
			row[index] = fmt.Sprint(value)
		}
		result.rows = append(result.rows, row)
	}
	return result, rows.Err()
}

// formExecDaily14190Run runs the pieces in order in one transaction, as pm runs the file; replaceID/from/to change one
// statement in a test copy. It returns the results by id, or the first error (the transaction is then rolled back).
func formExecDaily14190Run(ctx context.Context, t *testing.T, conn *pgx.Conn, pieces []formExecDaily14190Piece, replaceID, from, to string) (map[string]formExecDaily14190Result, error) {
	t.Helper()
	results := map[string]formExecDaily14190Result{}
	for _, piece := range pieces {
		text := piece.text
		if piece.id == replaceID {
			if strings.Count(text, from) != 1 {
				t.Fatalf("%s holds %q %d time(s), want 1", piece.id, from, strings.Count(text, from))
			}
			text = strings.Replace(text, from, to, 1)
		}
		if piece.id == "BEGIN" || piece.id == "ROLLBACK" {
			if _, err := conn.Exec(ctx, text); err != nil {
				t.Fatalf("%s: %v", piece.id, err)
			}
			continue
		}
		result, err := formExecDaily14190Query(ctx, conn, text)
		if err != nil {
			if _, rollbackErr := conn.Exec(ctx, "ROLLBACK"); rollbackErr != nil {
				t.Fatalf("ROLLBACK after %s: %v", piece.id, rollbackErr)
			}
			return results, fmt.Errorf("%s: %w", piece.id, err)
		}
		for index, row := range result.rows {
			t.Logf("FORM-14190-DAILY-4 %s row %d %v %v", piece.id, index, result.columns, row)
		}
		results[piece.id] = result
	}
	return results, nil
}

func formExecDaily14190Cell(t *testing.T, label string, result formExecDaily14190Result, row int, column string) string {
	t.Helper()
	if row >= len(result.rows) {
		t.Fatalf("%s: %d row(s), want a row %d", label, len(result.rows), row)
	}
	for index, name := range result.columns {
		if name == column {
			return result.rows[row][index]
		}
	}
	t.Fatalf("%s: no column %s in %v", label, column, result.columns)
	return ""
}

func formExecDaily14190Want(t *testing.T, label string, result formExecDaily14190Result, row int, want map[string]string) {
	t.Helper()
	for column, value := range want {
		got := formExecDaily14190Cell(t, label, result, row, column)
		if prefix, isPrefix := strings.CutSuffix(value, "*"); isPrefix {
			if !strings.HasPrefix(got, prefix) {
				t.Errorf("%s row %d %s = %q, want prefix %q", label, row, column, got, prefix)
			}
			continue
		}
		if got != value {
			t.Errorf("%s row %d %s = %q, want %q", label, row, column, got, value)
		}
	}
}

// TestFormExecDaily14190ReadsTheBackfillDailyRuns. Org f0f0f0f0-... holds the backfill's daily runs (generation
// 'post-sync:' + a run id of prefix b1b1b1b1): two succeeded and one no_repositories run after T0, one pending run of
// the same generation BEFORE T0 (out of the window), and an open daily run of another sync after T0 (D3 INFO). Org
// e0e0e0e0-... holds an open and a failed run of the same generation and must not count (the org filter mutants).
func TestFormExecDaily14190ReadsTheBackfillDailyRuns(t *testing.T) {
	ctx := context.Background()
	instance, admin := startDatabase(t)
	uri := freshDatabase(t, instance, admin)
	conn, err := pgx.Connect(ctx, uri)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)

	sum := sha256.Sum256([]byte(formExecDaily14190SQL))
	if got := hex.EncodeToString(sum[:]); got != formExecDaily14190SHA256 {
		t.Fatalf("sha256 of the embedded text = %s, want %s (FORM-14190-DAILY-4.sql with %s and %s)", got, formExecDaily14190SHA256,
			formExecDaily14190OrgSlot, formExecDaily14190RunSlot)
	}
	pieces := formExecDaily14190Pieces(t, formExecDaily14190SQL)
	var ids []string
	for index, piece := range pieces {
		ids = append(ids, piece.id)
		sqlLines := formExecDaily14190SQLLines(piece.text)
		orgFroms := strings.Count(sqlLines, "FROM public.daily_metrics_runs") + strings.Count(sqlLines, "FROM public.organizations")
		filters := strings.Count(sqlLines, "starts_with(")
		ranges := strings.Count(sqlLines, formExecDaily14190OrgSlot+"-0000-0000-0000-000000000000'::uuid") +
			strings.Count(sqlLines, formExecDaily14190OrgSlot+"-ffff-ffff-ffff-ffffffffffff'::uuid")
		slots := strings.Count(sqlLines, formExecDaily14190OrgSlot)
		if filters != orgFroms || slots != filters+ranges {
			t.Errorf("%s: %d org-scoped FROM, %d starts_with, %d range bounds, %d org slots", piece.id, orgFroms, filters, ranges, slots)
		}
		if strings.Contains(sqlLines, "FROM public.daily_metrics_runs") && ranges != 2*strings.Count(sqlLines, "FROM public.daily_metrics_runs") {
			t.Errorf("%s: a daily_metrics_runs read without both range bounds", piece.id)
		}
		wantRunSlots := map[string]int{"D1": 1, "D2": 1, "D3": 1}[piece.id]
		if got := strings.Count(sqlLines, formExecDaily14190RunSlot); got != wantRunSlots {
			t.Errorf("%s: %d run prefix slot(s), want %d", piece.id, got, wantRunSlots)
		}
		pieces[index].text = strings.ReplaceAll(strings.ReplaceAll(piece.text, formExecDaily14190OrgSlot, formExecDaily14190OrgPrefix),
			formExecDaily14190RunSlot, formExecDaily14190RunPrefix)
		if strings.Contains(pieces[index].text, "{{") {
			t.Errorf("%s: a placeholder is left", piece.id)
		}
		if found := formExecDaily14190Banned(formExecDaily14190SQLLines(pieces[index].text)); len(found) > 0 {
			t.Errorf("%s holds %v", piece.id, found)
		}
	}
	// The ban can fail: a real SET statement (at the start of a line or after ";", any case) and a write are caught; a
	// name that only contains the letters (key_set, OFFSET) is not.
	for text, want := range map[string]bool{
		"SET statement_timeout = 1;\nSELECT 1":                      true,
		"SELECT 1; set work_mem = '1MB'":                            true,
		"  Set LOCAL lock_timeout = 0":                              true,
		"UPDATE t SET a = 1":                                        true,
		"WITH key_set AS (SELECT 1) SELECT * FROM key_set OFFSET 0": false,
	} {
		if got := len(formExecDaily14190Banned(text)) > 0; got != want {
			t.Errorf("ban on %q = %v, want %v", text, got, want)
		}
	}
	if got := strings.Join(ids, ","); got != "BEGIN,G0,C0,S0,D1,D2,D3,D4-2,ROLLBACK" {
		t.Fatalf("pieces = %s", got)
	}

	const (
		org, other = formExecDaily14190OrgPrefix + "-1419-4000-8000-0000000000a1", "e0e0e0e0-1419-4000-8000-0000000000a2"
		afterT0    = "2026-11-23 15:00:00+00"
		beforeT0   = "2026-11-23 14:00:00+00"
	)
	generation := "post-sync:" + formExecDaily14190RunPrefix + "-1419-4000-8000-000000000001"
	otherGeneration := "post-sync:c2c2c2c2-1419-4000-8000-000000000002"
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := conn.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	for _, o := range []string{org, other} {
		exec(`INSERT INTO organizations (id, slug, name, tier, is_active, created_at, updated_at)
VALUES ($1::uuid, 'org-' || $1::text, 'org', 'enterprise', true, now(), now())`, o)
	}
	daily := func(n int, orgID, gen, day, status, finalization, createdAt string) {
		t.Helper()
		var token, lease any
		if finalization == "running" {
			token, lease = fmt.Sprintf("00000000-1419-4000-8000-%012d", n), "2026-11-23 16:00:00+00"
		}
		exec(`INSERT INTO daily_metrics_runs (id, org_id, target_day, generation, status, finalization_status, finalization_claim_token,
finalization_lease_expires_at, created_at, updated_at)
VALUES ($1::uuid, $2::uuid, $3::date, $4, $5, $6, $7::uuid, $8::timestamptz, $9::timestamptz, $9::timestamptz)`,
			fmt.Sprintf("0da11700-1419-4000-8000-%012d", n), orgID, day, gen, status, finalization, token, lease, createdAt)
	}
	repoA, repoB := "aaaaaaaa-1419-4000-8000-00000000000a", "bbbbbbbb-1419-4000-8000-00000000000b"
	// part lists the repositories of run n (one daily_metrics_partitions row); fullOrg marks run n a run of every repository.
	part := func(n int, repos ...string) {
		t.Helper()
		list := `["` + strings.Join(repos, `","`) + `"]`
		exec(`INSERT INTO daily_metrics_partitions (id, run_id, ordinal, repo_ids, status, created_at, updated_at)
VALUES ($1::uuid, $2::uuid, 0, $3::json, 'succeeded', now(), now())`,
			fmt.Sprintf("0da1170f-1419-4000-8000-%012d", n), fmt.Sprintf("0da11700-1419-4000-8000-%012d", n), list)
	}
	fullOrg := func(n int) {
		t.Helper()
		exec(`UPDATE daily_metrics_runs SET full_org = true WHERE id = $1::uuid`, fmt.Sprintf("0da11700-1419-4000-8000-%012d", n))
	}
	remove := func(n int) {
		t.Helper()
		exec(`DELETE FROM daily_metrics_runs WHERE id = $1::uuid`, fmt.Sprintf("0da11700-1419-4000-8000-%012d", n))
	}
	daily(1, org, generation, "2026-11-23", "succeeded", "succeeded", afterT0)
	daily(2, org, generation, "2026-11-22", "succeeded", "succeeded", afterT0)
	daily(3, org, generation, "2026-06-10", "no_repositories", "succeeded", afterT0)
	daily(4, org, generation, "2026-11-21", "pending", "pending", beforeT0)
	daily(5, org, otherGeneration, "2026-11-23", "pending", "pending", afterT0)
	daily(6, other, generation, "2026-11-23", "running", "pending", afterT0)
	daily(12, other, generation, "2026-11-22", "failed", "failed", afterT0)
	// The backlog: days before 2026-11-09 run by the touched-day drain, done; 2026-07-02 has an older failed run and a
	// newer done run (the newest wins).
	drain := "touched-drain:e:" + "d3d3d3d3-1419-4000-8000-000000000003"
	daily(13, org, drain, "2026-07-01", "succeeded", "succeeded", afterT0)
	daily(14, org, drain+"-a", "2026-07-02", "failed", "failed", afterT0)
	daily(15, org, drain, "2026-07-02", "succeeded", "succeeded", "2026-11-23 15:30:00+00")
	part(13, repoA)
	part(14, repoA)
	part(15, repoA)
	fullOrg(3)
	// Every backfill window day before 2026-11-09 (2026-06-01..2026-11-08, 161 days) has a done drain run of repo A
	// created after T0 (14:55), older than the runs above.
	bulk := func(day int) {
		t.Helper()
		n := 200 + day
		daily(n, org, drain+"-bulk", mustDay(t, "2026-06-01").AddDate(0, 0, day).Format("2006-01-02"), "succeeded", "succeeded", "2026-11-23 14:55:00+00")
		part(n, repoA)
	}
	for day := range 161 {
		bulk(day)
	}
	// The other org: an open drain run owning (2026-07-15, A), created after T0 (the D4-2 org mutant).
	daily(18, other, drain, "2026-07-15", "running", "pending", afterT0)
	part(18, repoA)
	exec(`ANALYZE organizations`)
	exec(`ANALYZE daily_metrics_runs`)
	exec(`ANALYZE daily_metrics_partitions`)

	read := func(label string) map[string]formExecDaily14190Result {
		t.Helper()
		results, err := formExecDaily14190Run(ctx, t, conn, pieces, "", "", "")
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		return results
	}
	wantPass := func(results map[string]formExecDaily14190Result) bool {
		cell := func(id, column string) string { return formExecDaily14190Cell(t, id, results[id], 0, column) }
		var total int
		if _, err := fmt.Sscan(cell("D2", "total"), &total); err != nil {
			t.Fatalf("D2 total: %v", err)
		}
		var days int
		if _, err := fmt.Sscan(cell("D4-2", "days"), &days); err != nil {
			t.Fatalf("D4-2 days: %v", err)
		}
		return cell("G0", "cost_gate_ok") == "0" && cell("S0", "orgs") == "1" && total > 0 && total <= 46 &&
			cell("D2", "open") == "0" && cell("D2", "failed") == "0" && strings.HasPrefix(cell("D2", "last_day"), "2026-11-23") &&
			cell("D4-2", "keys") != "0" && days >= 161 && cell("D4-2", "keys_owner_not_done") == "0"
	}

	// The pass set: every WANT holds.
	results := read("pass")
	formExecDaily14190Want(t, "pass G0", results["G0"], 0, map[string]string{"cost_gate_ok": "0"})
	if got := len(results["C0"].rows); got != 3 {
		t.Errorf("pass C0: %d rows, want 3", got)
	}
	formExecDaily14190Want(t, "pass S0", results["S0"], 0, map[string]string{"orgs": "1"})
	formExecDaily14190Want(t, "pass D2", results["D2"], 0, map[string]string{"total": "3", "done": "3", "open": "0", "failed": "0",
		"retrying": "0", "no_repositories": "1", "blocked": "0", "first_day": "2026-06-10*", "last_day": "2026-11-23*",
		"first_created_minute": "2026-11-23 15:00", "last_created_minute": "2026-11-23 15:00"})
	formExecDaily14190Want(t, "pass D3", results["D3"], 0, map[string]string{"total": "165", "open": "1", "failed": "1"})
	formExecDaily14190Want(t, "pass D4-2", results["D4-2"], 0, map[string]string{"keys": "162", "days": "161", "keys_owner_not_done": "0",
		"keys_owner_open": "0", "keys_owner_failed": "0", "keys_owner_retrying": "0", "full_org_days": "1",
		"first_day": "2026-06-01*", "last_day": "2026-11-08*"})
	if got := len(results["D1"].rows); got != 5 {
		t.Fatalf("pass D1: %d rows, want 5", got)
	}
	formExecDaily14190Want(t, "pass D1", results["D1"], 0, map[string]string{"generation_class": "post_sync_other", "status": "pending", "finalization_status": "pending", "runs": "1"})
	formExecDaily14190Want(t, "pass D1", results["D1"], 1, map[string]string{"generation_class": "this_backfill", "status": "no_repositories", "finalization_status": "succeeded", "runs": "1"})
	formExecDaily14190Want(t, "pass D1", results["D1"], 2, map[string]string{"generation_class": "this_backfill", "status": "succeeded", "finalization_status": "succeeded", "runs": "2"})
	formExecDaily14190Want(t, "pass D1", results["D1"], 3, map[string]string{"generation_class": "touched_drain", "status": "failed", "finalization_status": "failed", "runs": "1"})
	formExecDaily14190Want(t, "pass D1", results["D1"], 4, map[string]string{"generation_class": "touched_drain", "status": "succeeded", "finalization_status": "succeeded", "runs": "163",
		"first_created_minute": "2026-11-23 14:55", "last_created_minute": "2026-11-23 15:30"})
	for row := range 3 {
		formExecDaily14190Want(t, "pass D1", results["D1"], row, map[string]string{"first_created_minute": "2026-11-23 15:00", "last_created_minute": "2026-11-23 15:00"})
	}
	if !wantPass(results) {
		t.Errorf("pass set: a WANT failed")
	}

	// The org filter: the other org's open and failed runs of the same generation are kept out of D2 by the org filter.
	// The uuid range and starts_with are the same filter (the range bounds are the prefix's first and last uuid), so
	// either one alone still passes; without both the other org's runs count and the WANTs fail.
	orgFilter := "WHERE r.org_id >= '" + formExecDaily14190OrgPrefix + "-0000-0000-0000-000000000000'::uuid AND r.org_id <= '" +
		formExecDaily14190OrgPrefix + "-ffff-ffff-ffff-ffffffffffff'::uuid\n  AND starts_with(r.org_id::text, '" + formExecDaily14190OrgPrefix + "') AND "
	for _, mutant := range []struct {
		name, to   string
		killed     bool
		open, fail string
	}{
		{"no-org-filter", "WHERE ", true, "1", "1"},
		{"range-only", "WHERE r.org_id >= '" + formExecDaily14190OrgPrefix + "-0000-0000-0000-000000000000'::uuid AND r.org_id <= '" +
			formExecDaily14190OrgPrefix + "-ffff-ffff-ffff-ffffffffffff'::uuid AND ", false, "0", "0"},
		{"starts-with-only", "WHERE starts_with(r.org_id::text, '" + formExecDaily14190OrgPrefix + "') AND ", false, "0", "0"},
	} {
		results, err := formExecDaily14190Run(ctx, t, conn, pieces, "D2", orgFilter, mutant.to)
		if err != nil {
			t.Fatalf("MUTANT %s: %v", mutant.name, err)
		}
		formExecDaily14190Want(t, "MUTANT "+mutant.name+" D2", results["D2"], 0, map[string]string{"open": mutant.open, "failed": mutant.fail})
		if passed := wantPass(results); passed == mutant.killed {
			t.Errorf("MUTANT %s: WANTs passed = %v, want %v", mutant.name, passed, !mutant.killed)
		}
	}

	// The gate fires: each cap literal lowered to 0 in a test copy, and the index marked not valid (superuser UPDATE of
	// pg_index in this test database). WANT the named cast error STOP-DAILY-G0 (22P02) before any later statement.
	fire := func(label, from, to string) {
		t.Helper()
		replaceID := "G0"
		if from == "" {
			replaceID = ""
		}
		results, err := formExecDaily14190Run(ctx, t, conn, pieces, replaceID, from, to)
		var pgErr *pgconn.PgError
		if err == nil || !errors.As(err, &pgErr) || pgErr.Code != "22P02" || !strings.Contains(pgErr.Message, "STOP-DAILY-G0") {
			t.Errorf("GATE %s: err = %v, want 22P02 STOP-DAILY-G0", label, err)
			return
		}
		if len(results) != 0 {
			t.Errorf("GATE %s: statements ran before the gate: %v", label, results)
		}
		t.Logf("GATE %s: fired: %s", label, pgErr.Message)
	}
	// Mutant row sets: each must fail its WANT.
	daily(7, org, generation, "2026-11-20", "running", "pending", afterT0)
	results = read("open")
	formExecDaily14190Want(t, "open D2", results["D2"], 0, map[string]string{"total": "4", "open": "1", "failed": "0"})
	remove(7)
	daily(8, org, generation, "2026-11-19", "running", "running", afterT0)
	results = read("finalizing")
	formExecDaily14190Want(t, "finalizing D2", results["D2"], 0, map[string]string{"open": "1"})
	remove(8)
	daily(9, org, generation, "2026-11-18", "failed", "failed", afterT0)
	results = read("failed")
	formExecDaily14190Want(t, "failed D2", results["D2"], 0, map[string]string{"failed": "1", "open": "0"})
	remove(9)
	daily(10, org, generation, "2026-11-17", "running", "failed", afterT0)
	results = read("finalize-failed")
	formExecDaily14190Want(t, "finalize-failed D2", results["D2"], 0, map[string]string{"failed": "0", "open": "1", "retrying": "1"})
	remove(10)
	for n := 100; n < 144; n++ {
		daily(n, org, generation, fmt.Sprintf("2026-%02d-%02d", 8+(n-100)/28, 1+(n-100)%28), "succeeded", "succeeded", afterT0)
	}
	results = read("over 46")
	formExecDaily14190Want(t, "over 46 D2", results["D2"], 0, map[string]string{"total": "47", "open": "0", "failed": "0"})
	if wantPass(results) {
		t.Errorf("over 46: the D2 bound passed")
	}
	for n := 100; n < 144; n++ {
		remove(n)
	}
	daily(16, org, drain+"-b", "2026-07-01", "pending", "pending", "2026-11-23 15:45:00+00")
	part(16, repoA)
	results = read("drain open")
	formExecDaily14190Want(t, "drain open D2", results["D2"], 0, map[string]string{"total": "3", "open": "0", "failed": "0"})
	formExecDaily14190Want(t, "drain open D4-2", results["D4-2"], 0, map[string]string{"keys_owner_not_done": "1", "keys_owner_open": "1"})
	if wantPass(results) {
		t.Errorf("drain open: the D4-2 WANT passed")
	}
	remove(16)
	daily(17, org, drain+"-c", "2026-07-02", "running", "failed", "2026-11-23 15:50:00+00")
	part(17, repoA)
	results = read("drain retrying")
	formExecDaily14190Want(t, "drain retrying D4-2", results["D4-2"], 0, map[string]string{"keys_owner_not_done": "1", "keys_owner_retrying": "1", "keys_owner_failed": "0"})
	remove(17)
	// Owner per key, not newest run per day (acp P1-A): 2026-08-15 gets an older run of repo A and a NEWER run of repo B
	// only (or of every repository). The DAILY-3 D4 rule read only the newest run of the day.
	ownerCase := func(label, statusA, finalA string, reposB []string, full, wantNotDone, wantPassed bool) {
		t.Helper()
		daily(19, org, drain+"-x", "2026-08-15", statusA, finalA, "2026-11-23 15:40:00+00")
		part(19, repoA)
		daily(20, org, otherGeneration, "2026-08-15", "succeeded", "succeeded", "2026-11-23 15:45:00+00")
		if full {
			fullOrg(20)
		} else {
			part(20, reposB...)
		}
		results := read(label)
		want := "0"
		if wantNotDone {
			want = "1"
		}
		formExecDaily14190Want(t, label+" D4-2", results["D4-2"], 0, map[string]string{"keys_owner_not_done": want})
		if got := wantPass(results); got != wantPassed {
			t.Errorf("%s: WANTs passed = %v, want %v", label, got, wantPassed)
		}
		remove(20)
		remove(19)
	}
	ownerCase("open owner A, newer done B", "running", "pending", []string{repoB}, false, true, false)
	ownerCase("failed owner A, newer done B", "failed", "failed", []string{repoB}, false, true, false)
	ownerCase("failed A healed by newer done A", "failed", "failed", []string{repoA}, false, false, true)
	ownerCase("open A, newer done full-org run", "running", "pending", nil, true, false, true)
	results, err = formExecDaily14190Run(ctx, t, conn, pieces, "D4-2", "AND r.target_day < '2026-11-09'::date", "AND r.target_day < '2026-01-01'::date")
	if err != nil {
		t.Fatalf("D4-2 no rows: %v", err)
	}
	formExecDaily14190Want(t, "D4-2 no rows", results["D4-2"], 0, map[string]string{"keys": "0", "days": "0"})
	if wantPass(results) {
		t.Errorf("D4-2 no rows: the control passed")
	}
	// One backfill day without a run since T0: days 160 < 161.
	remove(200 + 152)
	results = read("160 days")
	formExecDaily14190Want(t, "160 days D4-2", results["D4-2"], 0, map[string]string{"days": "160", "keys_owner_not_done": "0"})
	if wantPass(results) {
		t.Errorf("160 days: the D4-2 days WANT passed")
	}
	bulk(152)
	// The fan-out's target day missing from D2: last_day 2026-11-22.
	remove(1)
	results = read("no target day")
	formExecDaily14190Want(t, "no target day D2", results["D2"], 0, map[string]string{"last_day": "2026-11-22*"})
	if wantPass(results) {
		t.Errorf("no target day: the D2 last_day WANT passed")
	}
	daily(1, org, generation, "2026-11-23", "succeeded", "succeeded", afterT0)
	// D4-2 org filter (acp P2-A): the other org's open owner of (2026-07-15, A) is newer than this org's run of the key.
	d42Filter := "WHERE r.org_id >= '" + formExecDaily14190OrgPrefix + "-0000-0000-0000-000000000000'::uuid AND r.org_id <= '" +
		formExecDaily14190OrgPrefix + "-ffff-ffff-ffff-ffffffffffff'::uuid\n      AND starts_with(r.org_id::text, '" + formExecDaily14190OrgPrefix + "') AND "
	for _, mutant := range []struct {
		name, to string
		killed   bool
		notDone  string
	}{
		{"D4-2 no-org-filter", "WHERE ", true, "1"},
		{"D4-2 range-only", "WHERE r.org_id >= '" + formExecDaily14190OrgPrefix + "-0000-0000-0000-000000000000'::uuid AND r.org_id <= '" +
			formExecDaily14190OrgPrefix + "-ffff-ffff-ffff-ffffffffffff'::uuid AND ", false, "0"},
		{"D4-2 starts-with-only", "WHERE starts_with(r.org_id::text, '" + formExecDaily14190OrgPrefix + "') AND ", false, "0"},
	} {
		results, err := formExecDaily14190Run(ctx, t, conn, pieces, "D4-2", d42Filter, mutant.to)
		if err != nil {
			t.Fatalf("MUTANT %s: %v", mutant.name, err)
		}
		formExecDaily14190Want(t, "MUTANT "+mutant.name, results["D4-2"], 0, map[string]string{"keys_owner_not_done": mutant.notDone})
		if passed := wantPass(results); passed == mutant.killed {
			t.Errorf("MUTANT %s: WANTs passed = %v, want %v", mutant.name, passed, !mutant.killed)
		}
	}
	exec(`UPDATE daily_metrics_runs SET generation = 'moved:' || generation WHERE generation = $1`, generation)
	results = read("no rows")
	formExecDaily14190Want(t, "no rows D2", results["D2"], 0, map[string]string{"total": "0"})
	if wantPass(results) {
		t.Errorf("no rows: the control passed")
	}
	exec(`UPDATE daily_metrics_runs SET generation = $1 WHERE generation = 'moved:' || $1`, generation)
	exec(`INSERT INTO organizations (id, slug, name, tier, is_active, created_at, updated_at)
VALUES ($1::uuid, 'org-second', 'org', 'enterprise', true, now(), now())`, formExecDaily14190OrgPrefix+"-1419-4000-8000-0000000000a9")
	fire("two orgs with the prefix", "", "")
	exec(`DELETE FROM organizations WHERE id = $1::uuid`, formExecDaily14190OrgPrefix+"-1419-4000-8000-0000000000a9")
	results = read("restored")
	if !wantPass(results) {
		t.Fatalf("restored pass set: a WANT failed")
	}

	for _, literal := range []string{"100000::bigint", "268435456::bigint", "1000000::bigint", "1073741824::bigint", "5000000::bigint", "4294967296::bigint"} {
		fire(literal+" -> 0", literal, "0::bigint")
	}
	exec(`UPDATE pg_index SET indisvalid = false WHERE indexrelid = 'public.uq_daily_metrics_run_generation'::regclass`)
	fire("index not valid", "", "")
	exec(`UPDATE pg_index SET indisvalid = true WHERE indexrelid = 'public.uq_daily_metrics_run_generation'::regclass`)
	exec(`UPDATE pg_index SET indisready = false WHERE indexrelid = 'public.uq_daily_metrics_run_generation'::regclass`)
	fire("index not ready", "", "")
	exec(`UPDATE pg_index SET indisready = true WHERE indexrelid = 'public.uq_daily_metrics_run_generation'::regclass`)
	fire("index missing", "ic.relname = 'uq_daily_metrics_run_generation'", "ic.relname = 'uq_missing_index'")
	fire("partition index missing", "ic.relname = 'uq_daily_metrics_partition_ordinal'", "ic.relname = 'uq_missing_index'")
	fire("table missing", "c.relname IN ('organizations', 'daily_metrics_runs', 'daily_metrics_partitions')",
		"c.relname IN ('organizations', 'daily_metrics_runs', 'daily_metrics_partitions_gone')")
	exec(`UPDATE pg_class SET reltuples = -1 WHERE oid = 'public.daily_metrics_runs'::regclass`)
	fire("never analyzed", "", "")
	exec(`ANALYZE daily_metrics_runs`)
	results = read("index restored")
	if !wantPass(results) {
		t.Fatalf("index restored: a WANT failed")
	}
}
