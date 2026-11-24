//go:build integration

package joboutbox_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

const formWk38SHA256 = "aaefe58f49bb64fe98519dc1cb42b79ea69f201cd07c69553f4561b1bf9d6650"

const formWk38 = `-- FORM-wk38-OUTBOX-2 (acp-train38, 2026-11-24): pre-roll cost read for the workers roll that carries ops #6850 (CHAOS-14511), STEP 335.
-- -2 = FORM-wk38-OUTBOX-1.sql f1055788cfcb (frozen) with the acp rule: a cost gate INSIDE EVERY data statement (each stops itself), ORG-1 stated.
-- Why: acp READ-DELTA-6850-2 cea87c7ca576 :27: the #6850 live outbox arm (staleFinalizeLiveJobSQL) reads ALL pending/claimed outbox rows of
-- every kind once per statement (through ix_worker_job_outbox_due). This FORM names that live count; it also names the finalize row count
-- (the rows the finalize-outbox-index hook indexes) and the table / index sizes the CONCURRENTLY build scans.
-- Run: psql -X -v ON_ERROR_STOP=1 -f FORM-wk38-OUTBOX-2.sql   (no SET in the file; pm keeps the session timeout; pm may send '\timing on' first)
-- ORG-1: N/A. worker_job_outbox has no org column and no foreign key to organizations (ops 97c147e9 internal/pgmigrate/baseline/head.json,
-- CREATE TABLE public.worker_job_outbox): the org is only inside args (job envelope), which this FORM never reads except in stmt5's
-- job_kind filter. Named all-org exception: every count is all-org. The output holds counts, sizes and booleans only (no id, no args).
-- Schema cites (same file): ix_worker_job_outbox_due btree (status, next_attempt_at, scheduled_at, created_at) WHERE status IN
-- ('pending','claimed'); ix_worker_job_outbox_terminal btree (status, delivered_at, updated_at) WHERE status IN ('delivered','dead').
-- COST GATES (every data statement carries its own; a failed gate = an int cast error whose text starts STOP-): heap cap 629145600 bytes
-- (600 MiB; prod 474.8 MiB at READ-DELTA-6850-1); due index cap 104857600 (100 MiB); terminal index cap 209715200 (200 MiB). stmt2 / stmt3 /
-- stmt4: a one-time filter (uncorrelated scalar subquery = InitPlan, run before any row is read) + a LIMIT; stmt5: CASE, whose THEN branch
-- (the heap count, an InitPlan) runs only when the size test passes. Each statement <= 2,000,001 rows (TEMPLATE-3: <= 20 M).
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;

-- stmt0 G0 catalog gate (reads no data table; sizes by pg_relation_size, not relpages). WANT cost_gate_ok = 0.
WITH t AS (SELECT to_regclass('public.worker_job_outbox') AS rel,
                  to_regclass('public.ix_worker_job_outbox_due') AS due,
                  to_regclass('public.ix_worker_job_outbox_terminal') AS term),
facts AS (
  SELECT (rel IS NOT NULL) AS has_table, (due IS NOT NULL) AS has_due, (term IS NOT NULL) AS has_term,
         CASE WHEN rel IS NULL THEN NULL ELSE pg_catalog.pg_relation_size(rel) END AS heap_bytes,
         CASE WHEN due IS NULL THEN NULL ELSE pg_catalog.pg_relation_size(due) END AS due_bytes,
         CASE WHEN term IS NULL THEN NULL ELSE pg_catalog.pg_relation_size(term) END AS term_bytes,
         (SELECT count(*) FROM pg_catalog.pg_index i WHERE i.indexrelid = due AND i.indisvalid AND i.indisready) AS due_valid
  FROM t)
SELECT CASE
         WHEN has_table AND has_due AND has_term AND due_valid = 1
          AND heap_bytes <= 629145600 -- cap S0-heap
          AND due_bytes <= 104857600 -- cap S0-due
          AND term_bytes <= 209715200 -- cap S0-term
         THEN 0
         ELSE ('STOP-G0 table=' || COALESCE(has_table::text, 'null') || ' due=' || COALESCE(has_due::text, 'null')
               || ' term=' || COALESCE(has_term::text, 'null') || ' due_valid=' || COALESCE(due_valid::text, 'null')
               || ' heap_bytes=' || COALESCE(heap_bytes::text, 'null') || ' due_bytes=' || COALESCE(due_bytes::text, 'null')
               || ' term_bytes=' || COALESCE(term_bytes::text, 'null'))::int
       END AS cost_gate_ok
FROM facts;

-- stmt1 INFO, catalog only (no data table read, no gate needed): the finalize index state before the roll.
-- WANT finalize_index_present = f (0167 not applied, the hook not run yet).
SELECT (to_regclass('public.ix_worker_job_outbox_finalize_domain_id') IS NOT NULL) AS finalize_index_present;

-- stmt2 the live pending/claimed backlog of EVERY kind (= the rows the #6850 live outbox arm reads once per survey statement).
-- Gate: due index and heap under their caps, else STOP-S2. WANT pending_claimed <= 200000 (200001 = the LIMIT cap was hit: STOP, report).
SELECT count(*) AS pending_claimed
FROM (SELECT 1 FROM public.worker_job_outbox AS o
      WHERE (SELECT CASE WHEN pg_catalog.pg_relation_size('public.ix_worker_job_outbox_due'::regclass) <= 104857600 -- cap S2-due
                          AND pg_catalog.pg_relation_size('public.worker_job_outbox'::regclass) <= 629145600 -- cap S2-heap
                         THEN 0 ELSE ('STOP-S2 due_bytes=' || pg_catalog.pg_relation_size('public.ix_worker_job_outbox_due'::regclass)
                                      || ' heap_bytes=' || pg_catalog.pg_relation_size('public.worker_job_outbox'::regclass))::int END) = 0
        AND o.status IN ('pending', 'claimed')
      LIMIT 200001) AS s;

-- stmt3 INFO: the same backlog split, finalize kind or not, by status (the arm keeps only the finalize rows after the read).
-- Gate: as stmt2, else STOP-S3.
SELECT (s.job_kind = 'metrics.daily_finalize') AS finalize_kind, s.status, count(*) AS n
FROM (SELECT o.job_kind, o.status FROM public.worker_job_outbox AS o
      WHERE (SELECT CASE WHEN pg_catalog.pg_relation_size('public.ix_worker_job_outbox_due'::regclass) <= 104857600 -- cap S3-due
                          AND pg_catalog.pg_relation_size('public.worker_job_outbox'::regclass) <= 629145600 -- cap S3-heap
                         THEN 0 ELSE ('STOP-S3 due_bytes=' || pg_catalog.pg_relation_size('public.ix_worker_job_outbox_due'::regclass)
                                      || ' heap_bytes=' || pg_catalog.pg_relation_size('public.worker_job_outbox'::regclass))::int END) = 0
        AND o.status IN ('pending', 'claimed')
      LIMIT 200001) AS s
GROUP BY 1, 2 ORDER BY 1, 2;

-- stmt4 control (can fail): the status literals match real rows. Gate: terminal index and heap under their caps, else STOP-S4.
-- WANT terminal_rows > 0 (0 = the literals or the table are wrong: STOP).
SELECT count(*) AS terminal_rows
FROM (SELECT 1 FROM public.worker_job_outbox AS o
      WHERE (SELECT CASE WHEN pg_catalog.pg_relation_size('public.ix_worker_job_outbox_terminal'::regclass) <= 209715200 -- cap S4-term
                          AND pg_catalog.pg_relation_size('public.worker_job_outbox'::regclass) <= 629145600 -- cap S4-heap
                         THEN 0 ELSE ('STOP-S4 term_bytes=' || pg_catalog.pg_relation_size('public.ix_worker_job_outbox_terminal'::regclass)
                                      || ' heap_bytes=' || pg_catalog.pg_relation_size('public.worker_job_outbox'::regclass))::int END) = 0
        AND o.status IN ('delivered', 'dead')
      LIMIT 1000) AS s;

-- stmt5 INFO: the finalize rows the hook indexes (one heap read: no index serves job_kind = 'metrics.daily_finalize' before the roll).
-- Gate: CASE; the heap count runs only when the heap is under its cap, else STOP-S5. WANT an integer (READ-DELTA-6850-1: about 49k of 482k).
SELECT CASE WHEN pg_catalog.pg_relation_size('public.worker_job_outbox'::regclass) <= 629145600 -- cap S5-heap
            THEN (SELECT count(*) FROM (SELECT 1 FROM public.worker_job_outbox AS o
                                        WHERE o.job_kind = 'metrics.daily_finalize' LIMIT 2000001) AS s)
            ELSE ('STOP-S5 heap_bytes=' || pg_catalog.pg_relation_size('public.worker_job_outbox'::regclass))::int
       END AS finalize_rows;

ROLLBACK;
`

// DO NOT MERGE. Throwaway exec proof of a prod read form (FORM-wk38-OUTBOX-2, STEP 335 pre-roll outbox cost read). formWk38 is
// the form file byte for byte; its sha256 is pinned. Synthetic rows only (no org column on the table; ids use the f0f0f0f0 prefix).
func TestFormWk38OutboxExecutes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	sum := sha256.Sum256([]byte(formWk38))
	if got := hex.EncodeToString(sum[:]); got != formWk38SHA256 {
		t.Fatalf("form sha256 = %s, want %s", got, formWk38SHA256)
	}
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	pgschema.Apply(ctx, t, pool)
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, query, args...); err != nil {
			t.Fatal(err)
		}
	}
	seed := func(kind, status string, n int) {
		t.Helper()
		exec(`INSERT INTO public.worker_job_outbox
    (id, dedupe_key, job_kind, contract_version, args, payload_hash, queue, priority, max_attempts, scheduled_at, status,
     claim_token, claimed_at, claim_expires_at, attempt_count, next_attempt_at, river_job_id, delivered_at, created_at, updated_at)
SELECT gen_random_uuid(), 'formwk38:' || $1::text || ':' || $2::text || ':' || g, $1::varchar, 1,
       json_build_object('contract_version', 1, 'domain', json_build_object('type', 'daily_metrics_run', 'id', 'f0f0f0f0-0000-4000-8000-' || lpad(g::text, 12, '0'))),
       'sha256:' || repeat('0', 64), 'metrics', 2, 4, now(), $2::varchar,
       CASE WHEN $2::text = 'claimed' THEN gen_random_uuid() END, CASE WHEN $2::text = 'claimed' THEN now() END,
       CASE WHEN $2::text = 'claimed' THEN now() + interval '5 minutes' END, 0, now(),
       CASE WHEN $2::text = 'delivered' THEN 4242 + g END, CASE WHEN $2::text = 'delivered' THEN now() END, now(), now()
FROM generate_series(1, $3::int) AS g`, kind, status, n)
	}
	const fin, other = "metrics.daily_finalize", "metrics.daily_partition"
	seed(fin, "pending", 3)
	seed(fin, "claimed", 2)
	seed(other, "pending", 4)
	seed(other, "claimed", 1)
	seed(fin, "delivered", 5)
	seed(fin, "dead", 1)
	seed(other, "delivered", 6)
	// results: 0 BEGIN, 1 stmt0, 2 stmt1, 3 stmt2, 4 stmt3, 5 stmt4, 6 stmt5, 7 ROLLBACK.
	wantSplit := [][]string{{"f", "claimed", "1"}, {"f", "pending", "4"}, {"t", "claimed", "2"}, {"t", "pending", "3"}}
	check := func(name, index string) {
		t.Helper()
		got := runWk38Form(t, ctx, pool, formWk38, "")
		if len(got) != 8 || got[1][0][0] != "0" || got[2][0][0] != index || got[3][0][0] != "10" ||
			!reflect.DeepEqual(got[4], wantSplit) || got[5][0][0] != "12" || got[6][0][0] != "11" {
			t.Fatalf("%s: FORM = %v, want stmt0 0, stmt1 %s, stmt2 10, stmt3 %v, stmt4 12, stmt5 11", name, got, index, wantSplit)
		}
		t.Logf("CELL %s PASS: stmt0 0, stmt1 %s, stmt2 10, stmt3 %v, stmt4 12, stmt5 11", name, index, wantSplit)
	}
	// Prod before the roll = revision 0166: the 0167 finalize index is absent.
	exec("DROP INDEX public.ix_worker_job_outbox_finalize_domain_id")
	check("index_absent_0166", "f")
	exec(`CREATE INDEX ix_worker_job_outbox_finalize_domain_id ON public.worker_job_outbox ((args #>> '{domain,id}')) WHERE job_kind = 'metrics.daily_finalize'`)
	check("index_present_0167", "t")

	// Red cells: every cap set below the fixture size stops its own statement with its STOP text.
	for _, red := range []struct{ cap, stop string }{
		{"<= 629145600 -- cap S0-heap", "STOP-G0"}, {"<= 104857600 -- cap S0-due", "STOP-G0"}, {"<= 209715200 -- cap S0-term", "STOP-G0"},
		{"<= 104857600 -- cap S2-due", "STOP-S2"}, {"<= 629145600 -- cap S2-heap", "STOP-S2"},
		{"<= 104857600 -- cap S3-due", "STOP-S3"}, {"<= 629145600 -- cap S3-heap", "STOP-S3"},
		{"<= 209715200 -- cap S4-term", "STOP-S4"}, {"<= 629145600 -- cap S4-heap", "STOP-S4"},
		{"<= 629145600 -- cap S5-heap", "STOP-S5"},
	} {
		if strings.Count(formWk38, red.cap) != 1 {
			t.Fatalf("cap %q: not exactly once in the form", red.cap)
		}
		runWk38Form(t, ctx, pool, strings.Replace(formWk38, red.cap, "<= 0 -- red", 1), red.stop)
		t.Logf("CELL red %q PASS: %s", red.cap, red.stop)
	}
	// The gates run BEFORE the read: with the cap below the size and the STOP turned into a plain value, the outbox scan node never runs.
	stmt2 := between(t, formWk38, "SELECT count(*) AS pending_claimed", "LIMIT 200001) AS s;")
	gated2 := regexp.MustCompile(`(?s)ELSE \('STOP-S2.*?\)::int END`).ReplaceAllString(strings.Replace(stmt2, "<= 104857600 -- cap S2-due", "<= 0 -- red", 1), "ELSE 1 END")
	if gated2 == stmt2 || strings.Contains(gated2, "STOP-S2") {
		t.Fatalf("stmt2 gate mutation did not apply")
	}
	if loops := outboxScanLoops(t, ctx, pool, gated2); loops != 0 {
		t.Fatalf("stmt2 with a failed gate: outbox scan loops = %d, want 0 (the one-time filter runs first)", loops)
	}
	if loops := outboxScanLoops(t, ctx, pool, stmt2); loops < 1 {
		t.Fatalf("stmt2 as written: outbox scan loops = %d, want >= 1 (the control can fail)", loops)
	}
	t.Logf("CELL stmt2-gate-first PASS: failed gate = outbox scan loops 0; as written >= 1")
	stmt5 := between(t, formWk38, "SELECT CASE WHEN pg_catalog.pg_relation_size('public.worker_job_outbox'::regclass) <= 629145600 -- cap S5-heap", "END AS finalize_rows;")
	gated5 := regexp.MustCompile(`(?s)ELSE \('STOP-S5.*?\)::int`).ReplaceAllString(strings.Replace(stmt5, "<= 629145600 -- cap S5-heap", "<= 0 -- red", 1), "ELSE -1")
	if gated5 == stmt5 || strings.Contains(gated5, "STOP-S5") {
		t.Fatalf("stmt5 gate mutation did not apply")
	}
	if loops := outboxScanLoops(t, ctx, pool, gated5); loops != 0 {
		t.Fatalf("stmt5 with a failed gate: outbox scan loops = %d, want 0 (the THEN InitPlan is not run)", loops)
	}
	if loops := outboxScanLoops(t, ctx, pool, stmt5); loops < 1 {
		t.Fatalf("stmt5 as written: outbox scan loops = %d, want >= 1", loops)
	}
	t.Logf("CELL stmt5-case-initplan PASS: failed gate = outbox scan loops 0; as written >= 1")
	// Control: stmt4 can answer 0 (a wrong status literal).
	if got := runWk38Form(t, ctx, pool, strings.Replace(formWk38, "AND o.status IN ('delivered', 'dead')", "AND o.status IN ('deliveredx', 'deadx')", 1), "")[5][0][0]; got != "0" {
		t.Fatalf("stmt4 with wrong literals = %s, want 0 (the control can fail)", got)
	}
	t.Logf("CELL stmt4-control-can-fail PASS")
	// The LIMIT cap: a backlog above 200,000 rows answers exactly 200001 (= the STOP value of stmt2).
	seed("formwk38.bulk", "pending", 200000)
	if got := runWk38Form(t, ctx, pool, formWk38, ""); got[3][0][0] != "200001" || got[1][0][0] != "0" {
		t.Fatalf("over-cap backlog: stmt0 %v stmt2 %v, want 0 and 200001", got[1], got[3])
	}
	t.Logf("CELL stmt2-limit-cap PASS: 200010 pending/claimed rows answer 200001")
}

func between(t *testing.T, s, start, end string) string {
	t.Helper()
	i := strings.Index(s, start)
	if i < 0 {
		t.Fatalf("no %q in the form", start)
	}
	j := strings.Index(s[i:], end)
	if j < 0 {
		t.Fatalf("no %q after %q", end, start)
	}
	return s[i : i+j+len(end)]
}

// outboxScanLoops runs EXPLAIN (ANALYZE, FORMAT JSON) of one statement (read-only transaction, rolled back) and returns the
// summed Actual Loops of every plan node that reads worker_job_outbox or one of its indexes.
func outboxScanLoops(t *testing.T, ctx context.Context, pool *pgxpool.Pool, stmt string) int {
	t.Helper()
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var raw string
	if err := tx.QueryRow(ctx, "EXPLAIN (ANALYZE, FORMAT JSON) "+strings.TrimSuffix(strings.TrimSpace(stmt), ";")).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var plans []map[string]any
	if err := json.Unmarshal([]byte(raw), &plans); err != nil {
		t.Fatal(err)
	}
	total, nodes := 0, 0
	var walk func(n map[string]any)
	walk = func(n map[string]any) {
		rel, _ := n["Relation Name"].(string)
		ix, _ := n["Index Name"].(string)
		if rel == "worker_job_outbox" || strings.HasPrefix(ix, "ix_worker_job_outbox") {
			nodes++
			if l, ok := n["Actual Loops"].(float64); ok {
				total += int(l)
			}
		}
		if kids, ok := n["Plans"].([]any); ok {
			for _, k := range kids {
				if m, ok := k.(map[string]any); ok {
					walk(m)
				}
			}
		}
	}
	for _, p := range plans {
		if m, ok := p["Plan"].(map[string]any); ok {
			walk(m)
		}
	}
	if nodes == 0 {
		t.Fatalf("no worker_job_outbox node in the plan: %s", raw)
	}
	t.Logf("plan outbox nodes %d loops %d", nodes, total)
	return total
}

// runWk38Form sends the FORM in one simple-query call, as psql -f does. With wantErr set it wants the call to fail with
// that text; else it returns each statement's rows as text.
func runWk38Form(t *testing.T, ctx context.Context, pool *pgxpool.Pool, form, wantErr string) [][][]string {
	t.Helper()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	results, err := conn.Conn().PgConn().Exec(ctx, form).ReadAll()
	if err == nil {
		for _, r := range results {
			if r.Err != nil {
				err = r.Err
				break
			}
		}
	}
	if wantErr != "" {
		if err == nil || !strings.Contains(err.Error(), wantErr) {
			t.Fatalf("FORM error = %v, want %q", err, wantErr)
		}
		_, _ = conn.Conn().PgConn().Exec(context.WithoutCancel(ctx), "ROLLBACK").ReadAll()
		return nil
	}
	if err != nil {
		t.Fatalf("FORM: %v", err)
	}
	out := [][][]string{}
	for _, result := range results {
		rows := [][]string{}
		for _, row := range result.Rows {
			cells := []string{}
			for _, cell := range row {
				cells = append(cells, string(cell))
			}
			rows = append(rows, cells)
		}
		out = append(out, rows)
	}
	t.Logf("FORM results: %v", out)
	return out
}
