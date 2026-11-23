//go:build integration

package pgmigrate_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
)

// Throwaway exec proof of two prod read forms: not for merge. Each form constant is its form file byte for byte
// (the cells assert the sha256); the cells fill the placeholders by plain replacement, as the run scripts do.

const rfRunsFormSHA256 = "1588c0843bd90a120a707b8088f06d82b626c6a42f1e6700b9d599d0d68ac1bf"

const rfRunsForm = `-- RUNS-13902-2: run state of ONE pass 1 chunk of CARD-13902-recompute-3 (30f972dfa124) and the org's open daily runs.
-- Postgres, database devhealth, read only. Lane l13902-runforms (lead fcsup), 2026-11-23 UTC. New form written from the code
-- (RUNS-14171-2 08891da2be4c is lost with bigboy). Shape: FORM-TEMPLATE-3 (6b9e73a3c45a, PM-RULES 8e04e2613b6f) (b).
-- -2 = RUNS-13902-1 (0902170eae56, never read) + the runs-read.sh column contract + card run matched by its rerun tag (no T0).
-- Prod: NOT EXECUTED. CI exec: EXEC-PROOF-13902-RUNFORMS-1.md. Uses, WANTs, work math: RUNS-13902-2.md.
-- Run: psql -X -q -A -F'|' -v ON_ERROR_STOP=1 (header ON, no -t), the filled file on stdin. One transaction, ends ROLLBACK.
-- No SET (statement_timeout comes from PGOPTIONS), no write. Output: counts, days, booleans.
-- Fill (runs-read.sh fill_form; every placeholder, comments too): {{FROM_DAY}} {{TO_DAY}} = the chunk (YYYY-MM-DD; the P4
--   read: the run range); {{RERUN_TAG}} = the chunk's --rerun-tag (recomp-13902-13891-<chunk FROM>; ValidRerunTag
--   [A-Za-z0-9._-], 1-32, manual_run.go:39, so it cannot close the quote).
-- Code: git show a7febdf50002:<path>; paths under internal/jobs/metrics/daily/ unless named. Tables + columns:
--   internal/pgmigrate/baseline/head.json (no diff a7febdf50002..origin/main 404d0a49429d under internal/pgmigrate).
-- CARD RUN = the run daily-start made for (org, day, tag): generation = 'manual-daily:' (postgres.go:93) + the first 16 hex
--   of sha256(org + '|' + day + '|' + <sorted repo ids joined by ','> + '|rerun:' + tag) (manual_run.go:85-96); daily-start
--   passes the canonical lower-case org uuid (internal/workersctl/main.go:1011, :3748-3763, :2754-2759), the day as
--   YYYY-MM-DD (:1091-1092) and no repo id (no --repo-id = deferred discovery, so the repo part is empty). A finalize-redrive
--   appends '#finalize-redrive:<nonce>' (redrive.go:25-29): the form compares the first 29 characters.
--   AND full_org (postgres.go:310: true when the start has no repo id).
-- Run literals: insert status 'pending' + finalization 'pending' postgres.go:310, :496; status 'running' :822; terminal
--   status 'failed' (+ finalization 'failed') :956, :1674-1676, dispatch_fail.go:72; finalize success: finalization
--   'succeeded', finalized_at, status 'succeeded' ('no_repositories' with no partition) :1581-1583 with :1556-1559, and
--   :1716-1718; a retryable finalize release keeps status 'running' with finalization 'failed' (:1633 -> :1716-1718).
--   blocked_at: blocked.go:327. 'canceled': no Go writer at the pin.
-- Partition literals: insert 'pending' postgres.go:354, :997; claim 'running' :1120; 'succeeded' :1203; retryable 'failed'
--   :1264, :1288 -> :1314; 'failed_permanent' :1406.
-- OPEN = status 'pending' or 'running' (a retryable finalize included), or status 'succeeded' with finalization
--   'pending'/'running' (no writer makes it; counted to be safe).
-- Cost: starts_with on org_id::text uses no index: RUNS and DAY read daily_metrics_runs once; PART reads it once plus
--   daily_metrics_partitions at most once. G0 (catalog only) caps both tables before any data row (work math in the md).

BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;

-- G0 catalog gate (reads no data table). WANT cost_gate_ok 0. Else the cast error 'STOP-G0 ...' aborts the transaction and
-- ON_ERROR_STOP ends the file: STOP. STOP when: a table is missing, never analyzed (reltuples -1), over a cap, or the
-- partition index is missing / invalid / partial.
SELECT 'G0' AS stmt,
       CASE WHEN g.tables_found = 2 AND g.min_est_rows >= 0
                 AND g.runs_rows <= 500000 AND g.runs_bytes <= 536870912
                 AND g.parts_rows <= 1000000 AND g.parts_bytes <= 1073741824
                 AND g.part_index_ok = 1
            THEN 0
            ELSE ('STOP-G0 tables=' || g.tables_found || ' runs=' || g.runs_rows || '/' || g.runs_bytes
                  || ' partitions=' || g.parts_rows || '/' || g.parts_bytes
                  || ' partition_index=' || g.part_index_ok)::int
       END AS cost_gate_ok
FROM (SELECT count(*) AS tables_found,
             min(rel.est_rows) AS min_est_rows,
             COALESCE(max(rel.est_rows) FILTER (WHERE rel.rel_name = 'daily_metrics_runs'), -1) AS runs_rows,
             COALESCE(max(rel.est_bytes) FILTER (WHERE rel.rel_name = 'daily_metrics_runs'), -1) AS runs_bytes,
             COALESCE(max(rel.est_rows) FILTER (WHERE rel.rel_name = 'daily_metrics_partitions'), -1) AS parts_rows,
             COALESCE(max(rel.est_bytes) FILTER (WHERE rel.rel_name = 'daily_metrics_partitions'), -1) AS parts_bytes,
             (SELECT count(*) FROM pg_catalog.pg_index AS ix
                JOIN pg_catalog.pg_class AS ixc ON ixc.oid = ix.indexrelid
                JOIN pg_catalog.pg_namespace AS ixn ON ixn.oid = ixc.relnamespace
              WHERE ixn.nspname = 'public' AND ixc.relname = 'ix_daily_metrics_partition_run_status'
                AND ix.indisvalid AND ix.indisready AND ix.indpred IS NULL) AS part_index_ok
      FROM (SELECT c.relname AS rel_name, c.reltuples::bigint AS est_rows,
                   (c.relpages::bigint + COALESCE(tc.relpages::bigint, 0)) * current_setting('block_size')::bigint AS est_bytes
            FROM pg_catalog.pg_class AS c
              JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
              LEFT JOIN pg_catalog.pg_class AS tc ON tc.oid = c.reltoastrelid
            WHERE n.nspname = 'public' AND c.relkind = 'r'
              AND c.relname IN ('daily_metrics_runs', 'daily_metrics_partitions')) AS rel) AS g;

-- RUNS (the runs-read.sh row). orgs = ORG-1 (WANT 1; also the control: 0 = the read sees nothing). runs / succeeded /
-- open / failed = card runs of the chunk; org_open = every open daily run of the org, any day, any generation.
-- Chunk-done WANT: runs = range_days = days, succeeded = runs, open 0, failed 0, org_open 0, blocked 0, other_end 0.
-- failed > 0 or blocked > 0 = STOP. Control ctrl_range_runs (every run of the org in the chunk; the nightly runs) WANT > 0.
SELECT 'RUNS' AS stmt,
       count(DISTINCT x.org_id) AS orgs,
       count(*) FILTER (WHERE x.is_card) AS runs,
       count(*) FILTER (WHERE x.is_card AND x.status = 'succeeded' AND x.finalization_status = 'succeeded') AS succeeded,
       count(*) FILTER (WHERE x.is_card AND x.is_open) AS open,
       count(*) FILTER (WHERE x.is_card AND x.status = 'failed') AS failed,
       count(*) FILTER (WHERE x.is_open) AS org_open,
       count(DISTINCT x.target_day) FILTER (WHERE x.is_card) AS days,
       (DATE '{{TO_DAY}}' - DATE '{{FROM_DAY}}' + 1) AS range_days,
       count(*) FILTER (WHERE x.is_card AND x.finalization_status = 'succeeded'
                          AND x.finalized_at IS NOT NULL) AS finalized,
       count(*) FILTER (WHERE x.is_card AND x.status = 'running' AND x.finalization_status = 'failed') AS finalize_retry,
       count(*) FILTER (WHERE x.is_card AND x.blocked_at IS NOT NULL) AS blocked,
       count(*) FILTER (WHERE x.is_card AND x.status IN ('no_repositories', 'canceled')) AS other_end,
       count(*) FILTER (WHERE x.is_open AND NOT x.is_card) AS other_open,
       min(x.target_day) FILTER (WHERE x.is_open) AS org_open_first_day,
       count(*) FILTER (WHERE x.in_range) AS ctrl_range_runs
FROM (SELECT r.org_id, r.target_day, r.status, r.finalization_status, r.finalized_at, r.blocked_at,
             (r.target_day BETWEEN DATE '{{FROM_DAY}}' AND DATE '{{TO_DAY}}') AS in_range,
             (r.target_day BETWEEN DATE '{{FROM_DAY}}' AND DATE '{{TO_DAY}}' AND r.full_org
              AND left(r.generation, 29) = 'manual-daily:' || left(encode(sha256(convert_to(
                    r.org_id::text || '|' || to_char(r.target_day, 'YYYY-MM-DD') || '||rerun:' || '{{RERUN_TAG}}',
                    'UTF8')), 'hex'), 16)) AS is_card,
             (r.status IN ('pending', 'running')
              OR (r.status = 'succeeded' AND r.finalization_status IN ('pending', 'running'))) AS is_open
      FROM public.daily_metrics_runs AS r
      WHERE starts_with(r.org_id::text, 'c6a38355')) AS x;

-- PART: the partitions of the card runs of the chunk. Control ctrl_range_partitions (every partition of every run of the
-- org in the chunk) WANT > 0. Chunk-done WANT: card_partitions > 0, part_open 0, part_failed 0, part_failed_permanent 0.
-- part_failed_permanent > 0 = STOP; part_failed (retryable) > 0 = not done, read again.
SELECT 'PART' AS stmt,
       count(*) FILTER (WHERE y.is_card) AS card_partitions,
       count(*) FILTER (WHERE y.is_card AND y.part_state = 'succeeded') AS part_succeeded,
       count(*) FILTER (WHERE y.is_card AND y.part_state IN ('pending', 'running')) AS part_open,
       count(*) FILTER (WHERE y.is_card AND y.part_state = 'failed') AS part_failed,
       count(*) FILTER (WHERE y.is_card AND y.part_state = 'failed_permanent') AS part_failed_permanent,
       count(*) AS ctrl_range_partitions
FROM (SELECT p.status AS part_state,
             (r.full_org
              AND left(r.generation, 29) = 'manual-daily:' || left(encode(sha256(convert_to(
                    r.org_id::text || '|' || to_char(r.target_day, 'YYYY-MM-DD') || '||rerun:' || '{{RERUN_TAG}}',
                    'UTF8')), 'hex'), 16)) AS is_card
      FROM public.daily_metrics_runs AS r
        JOIN public.daily_metrics_partitions AS p ON p.run_id = r.id
      WHERE starts_with(r.org_id::text, 'c6a38355')
        AND r.target_day BETWEEN DATE '{{FROM_DAY}}' AND DATE '{{TO_DAY}}') AS y;

-- DAY: one row per day of the chunk (started and not started days; the already_covered STOP reads this list).
-- Chunk-done WANT: every row d_card_runs 1, d_card_done 1. Control d_ctrl_runs (every run of the org that day) WANT > 0.
SELECT 'DAY' AS stmt,
       to_char(DATE '{{FROM_DAY}}' + s.day_offset, 'YYYY-MM-DD') AS list_day,
       COALESCE(w.day_card_runs, 0) AS d_card_runs,
       COALESCE(w.day_card_done, 0) AS d_card_done,
       COALESCE(w.day_card_open, 0) AS d_card_open,
       COALESCE(w.day_card_failed, 0) AS d_card_failed,
       COALESCE(w.day_runs_all, 0) AS d_ctrl_runs
FROM generate_series(0, DATE '{{TO_DAY}}' - DATE '{{FROM_DAY}}') AS s(day_offset)
  LEFT JOIN (SELECT v.target_day AS run_day,
                    count(*) FILTER (WHERE v.is_card) AS day_card_runs,
                    count(*) FILTER (WHERE v.is_card AND v.status = 'succeeded'
                                       AND v.finalization_status = 'succeeded') AS day_card_done,
                    count(*) FILTER (WHERE v.is_card AND v.is_open) AS day_card_open,
                    count(*) FILTER (WHERE v.is_card AND v.status = 'failed') AS day_card_failed,
                    count(*) AS day_runs_all
             FROM (SELECT r.target_day, r.status, r.finalization_status,
                          (r.full_org
                           AND left(r.generation, 29) = 'manual-daily:' || left(encode(sha256(convert_to(
                                 r.org_id::text || '|' || to_char(r.target_day, 'YYYY-MM-DD') || '||rerun:'
                                 || '{{RERUN_TAG}}', 'UTF8')), 'hex'), 16)) AS is_card,
                          (r.status IN ('pending', 'running')
                           OR (r.status = 'succeeded' AND r.finalization_status IN ('pending', 'running'))) AS is_open
                   FROM public.daily_metrics_runs AS r
                   WHERE starts_with(r.org_id::text, 'c6a38355')
                     AND r.target_day BETWEEN DATE '{{FROM_DAY}}' AND DATE '{{TO_DAY}}') AS v
             GROUP BY v.target_day) AS w ON w.run_day = DATE '{{FROM_DAY}}' + s.day_offset
ORDER BY s.day_offset;

ROLLBACK;
`

const rfFinalizeDayFormSHA256 = "5b9388b50be6e2051a2a32013c8bdf5c4c2b0e8f6a717662752c53bcc1ae9676"

const rfFinalizeDayForm = `-- FINALIZE-DAY-13902-2: finalize state of ONE day after one pass 2 finalize-redrive call of CARD-13902-recompute-3
-- (30f972dfa124). Postgres, database devhealth, read only. Lane l13902-runforms (lead fcsup), 2026-11-23 UTC. New form written
-- from the code (FINALIZE-DAY-14171-2 f35247fd15cf is lost with bigboy). Shape: FORM-TEMPLATE-3 (6b9e73a3c45a, PM-RULES
-- 8e04e2613b6f) (b). -2 = FINALIZE-DAY-13902-1 (b2a0a62763e6, never read) + the finalize-pass2.sh column contract, one
-- data statement. Prod: NOT EXECUTED. CI exec: EXEC-PROOF-13902-RUNFORMS-1.md. Uses, WANTs, work math: FINALIZE-DAY-13902-2.md.
-- Run: psql -X -q -A -F'|' -v ON_ERROR_STOP=1 (header ON, no -t), the filled file on stdin. One transaction, ends ROLLBACK.
-- No SET (statement_timeout comes from PGOPTIONS), no write. Output: counts and booleans.
-- Fill (finalize-pass2.sh fill_form; every placeholder, comments too): {{DAY}} = the day of the call (YYYY-MM-DD);
--   {{TS_UTC}} = date -u '+%Y-%m-%d %H:%M:%S' on the pm host just BEFORE that call.
-- Code: git show a7febdf50002:<path>, paths under internal/jobs/metrics/daily/. Tables + columns:
--   internal/pgmigrate/baseline/head.json (no diff a7febdf50002..origin/main 404d0a49429d under internal/pgmigrate).
-- One call (RedriveFinalizeForRange redrive.go:620, redriveOneFinalizeForRange :754): per day the newest (updated_at)
--   eligible run whose partitions all succeeded; a status 'succeeded' run is reset to status 'running' + finalization
--   'pending', updated_at = now, generation + '#finalize-redrive:<nonce>' (redrive.go:897-901, :25-29; finalized_at is
--   kept); a status 'running' run with a failed or expired finalize gets a fresh generation (:927-929). Then the finalize
--   job: success = finalization 'succeeded', finalized_at = now, status 'succeeded' (postgres.go:1581-1583 with
--   :1556-1559, :1716-1718); terminal failure = status 'failed' + finalization 'failed' (postgres.go:1674-1676); a
--   retryable release keeps status 'running' with finalization 'failed' (postgres.go:1633 -> :1716-1718).
--   updated_at and finalized_at are the worker clock (store.now()).
-- Clock: TS_UTC is the pm host clock. A pm clock ahead of the worker by more than the call takes gives finalized_since 0 =
--   STOP (safe direction). OPEN = the RUNS-13902-2 predicate.
-- Cost: starts_with on org_id::text uses no index: FIN reads daily_metrics_runs once. G0 (catalog only) caps it first.

BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;

-- G0 catalog gate (reads no data table). WANT cost_gate_ok 0. Else the cast error 'STOP-G0 ...' aborts the transaction and
-- ON_ERROR_STOP ends the file: STOP. STOP when: the table is missing, never analyzed (reltuples -1), or over a cap.
SELECT 'G0' AS stmt,
       CASE WHEN g.tables_found = 1 AND g.min_est_rows >= 0
                 AND g.runs_rows <= 500000 AND g.runs_bytes <= 536870912
            THEN 0
            ELSE ('STOP-G0 tables=' || g.tables_found || ' runs=' || g.runs_rows || '/' || g.runs_bytes)::int
       END AS cost_gate_ok
FROM (SELECT count(*) AS tables_found,
             min(rel.est_rows) AS min_est_rows,
             COALESCE(max(rel.est_rows) FILTER (WHERE rel.rel_name = 'daily_metrics_runs'), -1) AS runs_rows,
             COALESCE(max(rel.est_bytes) FILTER (WHERE rel.rel_name = 'daily_metrics_runs'), -1) AS runs_bytes
      FROM (SELECT c.relname AS rel_name, c.reltuples::bigint AS est_rows,
                   (c.relpages::bigint + COALESCE(tc.relpages::bigint, 0)) * current_setting('block_size')::bigint AS est_bytes
            FROM pg_catalog.pg_class AS c
              JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
              LEFT JOIN pg_catalog.pg_class AS tc ON tc.oid = c.reltoastrelid
            WHERE n.nspname = 'public' AND c.relkind = 'r'
              AND c.relname IN ('daily_metrics_runs')) AS rel) AS g;

-- FIN (the finalize-pass2.sh row). orgs = ORG-1 over every run of the org (WANT 1; also a control). Day DAY:
-- WANT failed_since 0 (> 0 = STOP); open_since 0 (> 0 = read again, at most 20 reads, then STOP); then finalized_since >= 1
-- (0 = STOP). Control ctrl_day_runs (every run of the org that day) WANT > 0; ts_not_future WANT t.
-- INFO open_now, finalize_retry_since, finalized_since_card (a redriven 'manual-daily:' full-org run).
SELECT 'FIN' AS stmt,
       count(DISTINCT f.org_id) AS orgs,
       count(*) FILTER (WHERE f.on_day AND f.finalization_status = 'succeeded'
                          AND f.finalized_at >= TIMESTAMPTZ '{{TS_UTC}}+00') AS finalized_since,
       count(*) FILTER (WHERE f.on_day AND f.status = 'failed'
                          AND f.updated_at >= TIMESTAMPTZ '{{TS_UTC}}+00') AS failed_since,
       count(*) FILTER (WHERE f.on_day AND f.is_open AND f.updated_at >= TIMESTAMPTZ '{{TS_UTC}}+00') AS open_since,
       count(*) FILTER (WHERE f.on_day AND f.is_open) AS open_now,
       count(*) FILTER (WHERE f.on_day AND f.status = 'running' AND f.finalization_status = 'failed'
                          AND f.updated_at >= TIMESTAMPTZ '{{TS_UTC}}+00') AS finalize_retry_since,
       count(*) FILTER (WHERE f.on_day AND f.finalization_status = 'succeeded'
                          AND f.finalized_at >= TIMESTAMPTZ '{{TS_UTC}}+00'
                          AND f.generation LIKE 'manual-daily:%#finalize-redrive:%' AND f.full_org) AS finalized_since_card,
       count(*) FILTER (WHERE f.on_day) AS ctrl_day_runs,
       (TIMESTAMPTZ '{{TS_UTC}}+00' <= now()) AS ts_not_future
FROM (SELECT r.org_id, r.status, r.finalization_status, r.finalized_at, r.updated_at, r.generation, r.full_org,
             (r.target_day = DATE '{{DAY}}') AS on_day,
             (r.status IN ('pending', 'running')
              OR (r.status = 'succeeded' AND r.finalization_status IN ('pending', 'running'))) AS is_open
      FROM public.daily_metrics_runs AS r
      WHERE starts_with(r.org_id::text, 'c6a38355')) AS f;

ROLLBACK;
`

const (
	rfOrgA  = "c6a38355-0000-4000-8000-00000000000a"
	rfOrgA2 = "c6a38355-0000-4000-8000-0000000000a2"
	rfOrgB  = "0b0b0b0b-0000-4000-8000-00000000000b"
	rfTag   = "recomp-13902-13891-2026-11-10"
	// Independent vectors (Python hashlib over the manual_run.go seed "org|day||rerun:tag", first 16 hex).
	rfVector1110 = "manual-daily:c8b04d32b318e06d"
	rfVector1111 = "manual-daily:56a3c9aaed12561e"
)

type rfRow map[string]string

type rfRun struct {
	org, day, generation, status, finalization string
	fullOrg, blocked                           bool
	created, updated                           time.Time
	finalized                                  *time.Time
	partitions                                 []string
}

type rfScan struct{ seqScan, seqRead, idxFetch, inserted int64 }

func rfAt(t time.Time) *time.Time { return &t }

// rfGeneration is the manual daily-start generation of internal/jobs/metrics/daily/manual_run.go:85-96 for a run with
// no repository id (deferred discovery).
func rfGeneration(org, day, tag string, repos ...string) string {
	sort.Strings(repos)
	sum := sha256.Sum256([]byte(org + "|" + day + "|" + strings.Join(repos, ",") + "|rerun:" + tag))
	return "manual-daily:" + hex.EncodeToString(sum[:])[:16]
}

// rfDatabase is a scratch database on the package's shared server with the pgmigrate baseline and chain at the head.
func rfDatabase(t *testing.T) (string, *pgx.Conn) {
	t.Helper()
	instance := sharedInstance(t)
	admin := connect(t, instance.URI)
	uri := databaseURI(t, instance.URI, scratchDatabase(t, admin))
	conn := connect(t, uri)
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if _, err := pgmigrate.Upgrade(ctx, conn, baseline, chain); err != nil {
		t.Fatalf("apply the schema: %v", err)
	}
	var version string
	if err := conn.QueryRow(ctx, `SHOW server_version`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("schema at the head: baseline + %d chain files, last %s; server_version %s", len(chain), chain[len(chain)-1].Name, version)
	return uri, conn
}

func rfExec(t *testing.T, conn *pgx.Conn, sql string, args ...any) int64 {
	t.Helper()
	tag, err := conn.Exec(context.Background(), sql, args...)
	if err != nil {
		t.Fatalf("setup %q: %v", sql, err)
	}
	return tag.RowsAffected()
}

func rfInsertRun(t *testing.T, conn *pgx.Conn, run rfRun) string {
	t.Helper()
	var id string
	if err := conn.QueryRow(context.Background(), `
INSERT INTO public.daily_metrics_runs
    (id, org_id, target_day, generation, status, finalization_status, finalized_at, created_at, updated_at,
     blocked_at, blocked_reason, full_org)
VALUES (gen_random_uuid(), $1::uuid, $2::date, $3, $4, $5, $6, $7, $8,
        CASE WHEN $9::boolean THEN $8::timestamptz END, CASE WHEN $9::boolean THEN 'synthetic' END, $10)
RETURNING id::text`,
		run.org, run.day, run.generation, run.status, run.finalization, run.finalized, run.created, run.updated,
		run.blocked, run.fullOrg).Scan(&id); err != nil {
		t.Fatalf("insert run %s %s: %v", run.day, run.generation, err)
	}
	for ordinal, status := range run.partitions {
		rfExec(t, conn, `
INSERT INTO public.daily_metrics_partitions
    (id, run_id, ordinal, repo_ids, status, attempt_count, created_at, updated_at, failure_reason)
VALUES (gen_random_uuid(), $1::uuid, $2, '[]'::json, $3::varchar, 1, $4, $4,
        CASE WHEN $3::varchar IN ('failed', 'failed_permanent') THEN 'synthetic' END)`,
			id, ordinal, status, run.updated)
	}
	return id
}

func rfSetRun(t *testing.T, conn *pgx.Conn, id, status, finalization string, updated time.Time, finalized *time.Time) {
	t.Helper()
	if n := rfExec(t, conn, `
UPDATE public.daily_metrics_runs SET status = $2, finalization_status = $3, updated_at = $4, finalized_at = $5
WHERE id = $1::uuid`, id, status, finalization, updated, finalized); n != 1 {
		t.Fatalf("set run %s: %d rows", id, n)
	}
}

func rfSetPartition(t *testing.T, conn *pgx.Conn, runID string, ordinal int, status string) {
	t.Helper()
	if n := rfExec(t, conn, `
UPDATE public.daily_metrics_partitions
SET status = $3::varchar, failure_reason = CASE WHEN $3::varchar IN ('failed', 'failed_permanent') THEN 'synthetic' END
WHERE run_id = $1::uuid AND ordinal = $2`, runID, ordinal, status); n != 1 {
		t.Fatalf("set partition %s/%d: %d rows", runID, ordinal, n)
	}
}

func rfAnalyze(t *testing.T, conn *pgx.Conn) {
	t.Helper()
	rfExec(t, conn, `ANALYZE public.daily_metrics_runs, public.daily_metrics_partitions`)
}

func rfEstRows(t *testing.T, conn *pgx.Conn) string {
	t.Helper()
	var text string
	if err := conn.QueryRow(context.Background(), `
SELECT string_agg(c.relname || '=' || c.reltuples::bigint, ' ' ORDER BY c.relname) FROM pg_catalog.pg_class AS c
WHERE c.relnamespace = 'public'::regnamespace AND c.relname IN ('daily_metrics_runs', 'daily_metrics_partitions')`).Scan(&text); err != nil {
		t.Fatal(err)
	}
	return text
}

// rfScans reads the cumulative table statistics on a fresh session (a fresh stats snapshot).
func rfScans(t *testing.T, uri string) map[string]rfScan {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, uri)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	rows, err := conn.Query(ctx, `
SELECT relname, COALESCE(seq_scan, 0), COALESCE(seq_tup_read, 0), COALESCE(idx_tup_fetch, 0), n_tup_ins
FROM pg_catalog.pg_stat_user_tables
WHERE schemaname = 'public' AND relname IN ('daily_metrics_runs', 'daily_metrics_partitions')`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]rfScan{}
	for rows.Next() {
		var name string
		var s rfScan
		if err := rows.Scan(&name, &s.seqScan, &s.seqRead, &s.idxFetch, &s.inserted); err != nil {
			t.Fatal(err)
		}
		out[name] = s
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// rfWaitScans polls the statistics until done says the flush landed (backends flush at exit or when idle).
func rfWaitScans(t *testing.T, uri, what string, done func(map[string]rfScan) bool) map[string]rfScan {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for {
		s := rfScans(t, uri)
		if done(s) {
			return s
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: statistics did not reach the expected state: %+v", what, s)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func rfCheckForm(t *testing.T, name, form, wantSHA string) {
	t.Helper()
	sum := sha256.Sum256([]byte(form))
	if got := hex.EncodeToString(sum[:]); got != wantSHA {
		t.Fatalf("%s: sha256 %s, want %s (the constant is not the form file)", name, got, wantSHA)
	}
	if strings.Count(form, "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;") != 1 || !strings.HasSuffix(form, "\nROLLBACK;\n") {
		t.Fatalf("%s: not one REPEATABLE READ READ ONLY transaction that ends ROLLBACK", name)
	}
	for _, line := range strings.Split(form, "\n") {
		code := strings.ToUpper(strings.TrimSpace(line))
		if strings.HasPrefix(code, "--") {
			continue
		}
		for _, banned := range []string{"SET ", "SET\t", "INSERT ", "UPDATE ", "DELETE ", "EXPLAIN", "COPY ", "CREATE ", "DROP ", "ALTER "} {
			if strings.HasPrefix(code, banned) || strings.Contains(code, " "+banned) {
				t.Fatalf("%s: banned word %q in %q", name, banned, line)
			}
		}
	}
	t.Logf("%s sha256 %s (%d bytes), starts_with org filters %d", name, wantSHA, len(form), strings.Count(form, "starts_with("))
}

func rfFill(t *testing.T, form string, values map[string]string) string {
	t.Helper()
	for key, value := range values {
		if !strings.Contains(form, "{{"+key+"}}") {
			t.Fatalf("placeholder %s not in the form", key)
		}
		form = strings.ReplaceAll(form, "{{"+key+"}}", value)
	}
	if strings.Contains(form, "{{") {
		t.Fatal("a placeholder is left after the fill")
	}
	return form
}

// rfRunForm sends the filled form as psql -f does in effect: every statement in order, in one session; the first error
// ends the rest (simple query protocol = ON_ERROR_STOP). It returns the rows by statement label; the session ends on return.
func rfRunForm(t *testing.T, uri, scenario, sql string) (map[string][]rfRow, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	conn, err := pgx.Connect(ctx, uri)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	results, err := conn.PgConn().Exec(ctx, sql).ReadAll()
	out := map[string][]rfRow{}
	for _, result := range results {
		header := make([]string, 0, len(result.FieldDescriptions))
		for _, field := range result.FieldDescriptions {
			header = append(header, field.Name)
		}
		if len(header) > 0 {
			t.Logf("%s HEADER %s", scenario, strings.Join(header, "|"))
		}
		for _, raw := range result.Rows {
			row := rfRow{}
			values := make([]string, 0, len(raw))
			for i, name := range header {
				value := ""
				if raw[i] != nil {
					value = string(raw[i])
				}
				row[name] = value
				values = append(values, value)
			}
			t.Logf("%s ROW %s", scenario, strings.Join(values, "|"))
			out[row["stmt"]] = append(out[row["stmt"]], row)
		}
	}
	if err != nil {
		t.Logf("%s ERROR %v", scenario, err)
	}
	return out, err
}

func rfRead(t *testing.T, uri, scenario, sql string, labels ...string) map[string][]rfRow {
	t.Helper()
	rows, err := rfRunForm(t, uri, scenario, sql)
	if err != nil {
		t.Fatalf("%s: %v", scenario, err)
	}
	for _, label := range labels {
		if label != "DAY" && len(rows[label]) != 1 {
			t.Fatalf("%s: %d %s rows, want 1", scenario, len(rows[label]), label)
		}
	}
	if g := rows["G0"][0]["cost_gate_ok"]; g != "0" {
		t.Fatalf("%s: G0 cost_gate_ok %s, want 0", scenario, g)
	}
	return rows
}

func rfWant(t *testing.T, scenario string, row rfRow, want map[string]string) {
	t.Helper()
	for column, value := range want {
		got, ok := row[column]
		if !ok {
			t.Errorf("%s: no column %s", scenario, column)
			continue
		}
		if got != value {
			t.Errorf("%s: %s = %q, want %q", scenario, column, got, value)
		}
	}
}

// rfContract checks the script parse contract: the row has every named column as a non-negative integer.
func rfContract(t *testing.T, scenario string, row rfRow, columns ...string) {
	t.Helper()
	for _, column := range columns {
		value, ok := row[column]
		if !ok || value == "" || strings.Trim(value, "0123456789") != "" {
			t.Errorf("%s: contract column %s = %q, want a non-negative integer", scenario, column, value)
		}
	}
}

// rfFire lowers one gate input in the filled form (one exact text, once) and WANTs the named STOP before any row.
func rfFire(t *testing.T, uri, scenario, sql, from, to string) {
	t.Helper()
	if strings.Count(sql, from) != 1 {
		t.Fatalf("%s: %q is in the form %d times, want 1", scenario, from, strings.Count(sql, from))
	}
	rows, err := rfRunForm(t, uri, scenario, strings.Replace(sql, from, to, 1))
	if err == nil || !strings.Contains(err.Error(), "STOP-G0") {
		t.Errorf("%s: err %v, want the STOP-G0 cast error", scenario, err)
		return
	}
	if len(rows) != 0 {
		t.Errorf("%s: %d row labels returned after the gate fired, want 0", scenario, len(rows))
	}
	t.Logf("GATE %s fired: %v", scenario, err)
}

// rfNeverAnalyzedThenEmpty: on the fresh head the gate STOPs (reltuples -1); after ANALYZE of the empty tables the form
// runs and its controls read 0 (each control can fail).
func rfNeverAnalyzedThenEmpty(t *testing.T, uri string, conn *pgx.Conn, form, label string, zero map[string]string) {
	t.Helper()
	t.Logf("fresh head reltuples: %s", rfEstRows(t, conn))
	if rows, err := rfRunForm(t, uri, "GATE never-analyzed", form); err == nil || !strings.Contains(err.Error(), "STOP-G0") || len(rows) != 0 {
		t.Errorf("never analyzed: err %v rows %d, want STOP-G0 and no row", err, len(rows))
	}
	rfAnalyze(t, conn)
	est := rfEstRows(t, conn)
	t.Logf("empty analyzed reltuples: %s", est)
	if strings.Contains(est, "=-1") {
		t.Logf("ANALYZE of an empty table left reltuples -1: empty-control read not run")
		return
	}
	rows := rfRead(t, uri, "empty-controls", form, "G0", label)
	rfWant(t, "empty-controls "+label, rows[label][0], zero)
}

func TestRunsReadFormGivesTheChunkStateAndStopsOnItsGate(t *testing.T) {
	rfCheckForm(t, "RUNS-13902-2", rfRunsForm, rfRunsFormSHA256)
	if got := rfGeneration(rfOrgA, "2026-11-10", rfTag); got != rfVector1110 {
		t.Fatalf("generation vector: %s, want %s", got, rfVector1110)
	}
	if got := rfGeneration(rfOrgA, "2026-11-11", rfTag); got != rfVector1111 {
		t.Fatalf("generation vector: %s, want %s", got, rfVector1111)
	}
	uri, conn := rfDatabase(t)
	t0 := time.Date(2026, 11, 20, 5, 0, 0, 0, time.UTC)
	chunk := map[string]string{"FROM_DAY": "2026-11-10", "TO_DAY": "2026-11-12", "RERUN_TAG": rfTag}
	form := rfFill(t, rfRunsForm, chunk)
	rfNeverAnalyzedThenEmpty(t, uri, conn, form, "RUNS", map[string]string{"orgs": "0", "ctrl_range_runs": "0", "runs": "0"})

	runsIn, partsIn := int64(0), int64(0)
	insert := func(run rfRun) string {
		runsIn++
		partsIn += int64(len(run.partitions))
		return rfInsertRun(t, conn, run)
	}
	done := func(day string, h int) (time.Time, time.Time) {
		d, _ := time.Parse("2006-01-02", day)
		return d.Add(time.Duration(24+h) * time.Hour), d.Add(time.Duration(25+h) * time.Hour)
	}
	nightly := map[string]string{}
	for _, day := range []string{"2026-11-09", "2026-11-10", "2026-11-11", "2026-11-12", "2026-11-13"} {
		created, finished := done(day, 2)
		nightly[day] = insert(rfRun{org: rfOrgA, day: day, generation: "fixed-schedule:daily_metrics_fanout:rf-" + day,
			status: "succeeded", finalization: "succeeded", fullOrg: true, created: created, updated: finished,
			finalized: rfAt(finished), partitions: []string{"succeeded", "succeeded"}})
	}
	// Same day, other tag; and the same tag with a repository id (repo-scoped, not full_org): neither is a card run.
	oldManual := time.Date(2026, 11, 15, 1, 0, 0, 0, time.UTC)
	insert(rfRun{org: rfOrgA, day: "2026-11-11", generation: rfGeneration(rfOrgA, "2026-11-11", "recomp-old"), status: "succeeded",
		finalization: "succeeded", fullOrg: true, created: oldManual, updated: oldManual, finalized: rfAt(oldManual),
		partitions: []string{"succeeded"}})
	insert(rfRun{org: rfOrgA, day: "2026-11-11", generation: rfGeneration(rfOrgA, "2026-11-11", rfTag, "11111111-1111-4111-8111-111111111111"),
		status: "succeeded", finalization: "succeeded", fullOrg: false, created: t0.Add(30 * time.Minute), updated: t0.Add(40 * time.Minute),
		finalized: rfAt(t0.Add(40 * time.Minute)), partitions: []string{"succeeded"}})
	cardRun := func(day, tag, suffix string) rfRun {
		return rfRun{org: rfOrgA, day: day, generation: rfGeneration(rfOrgA, day, tag) + suffix,
			status: "succeeded", finalization: "succeeded", fullOrg: true, created: t0.Add(time.Hour),
			updated: t0.Add(2 * time.Hour), finalized: rfAt(t0.Add(2 * time.Hour)), partitions: []string{"succeeded", "succeeded"}}
	}
	card := map[string]string{}
	card["2026-11-10"] = insert(cardRun("2026-11-10", rfTag, ""))
	card["2026-11-11"] = insert(cardRun("2026-11-11", rfTag, ""))
	// A finalize-redrive appends its marker: still the card run.
	card["2026-11-12"] = insert(cardRun("2026-11-12", rfTag, "#finalize-redrive:rfnonce1"))
	// The previous chunk (other tag, other day).
	insert(cardRun("2026-11-13", "recomp-13902-13891-2026-11-13", ""))
	// Org B: the same tag and day shape, open, with a failed_permanent partition: out of the org prefix.
	insert(rfRun{org: rfOrgB, day: "2026-11-10", generation: rfGeneration(rfOrgB, "2026-11-10", rfTag), status: "running",
		finalization: "pending", fullOrg: true, created: t0.Add(time.Hour), updated: t0.Add(time.Hour),
		partitions: []string{"failed_permanent"}})
	rfAnalyze(t, conn)
	t.Logf("seeded reltuples: %s", rfEstRows(t, conn))

	labels := []string{"G0", "RUNS", "PART", "DAY"}
	dayWant := func(scenario string, rows map[string][]rfRow, want map[string]map[string]string) {
		t.Helper()
		if len(rows["DAY"]) != len(want) {
			t.Errorf("%s: %d DAY rows, want %d", scenario, len(rows["DAY"]), len(want))
		}
		for _, row := range rows["DAY"] {
			if w, ok := want[row["list_day"]]; ok {
				rfWant(t, scenario+" DAY "+row["list_day"], row, w)
			} else {
				t.Errorf("%s: unexpected DAY row %s", scenario, row["list_day"])
			}
		}
	}
	doneDay := func(ctrl string) map[string]string {
		return map[string]string{"d_card_runs": "1", "d_card_done": "1", "d_card_open": "0", "d_card_failed": "0", "d_ctrl_runs": ctrl}
	}

	// PASS path with the scan count: wait until the seed session's statistics landed, read, wait for the read's.
	before := rfWaitScans(t, uri, "seed flush", func(s map[string]rfScan) bool {
		return s["daily_metrics_runs"].inserted == runsIn && s["daily_metrics_partitions"].inserted == partsIn
	})
	rows := rfRead(t, uri, "RUNS pass", form, labels...)
	after := rfWaitScans(t, uri, "pass read flush", func(s map[string]rfScan) bool {
		return s["daily_metrics_runs"].seqScan >= before["daily_metrics_runs"].seqScan+2
	})
	runScans := after["daily_metrics_runs"].seqScan - before["daily_metrics_runs"].seqScan
	runRead := after["daily_metrics_runs"].seqRead - before["daily_metrics_runs"].seqRead
	runIdx := after["daily_metrics_runs"].idxFetch - before["daily_metrics_runs"].idxFetch
	partRead := after["daily_metrics_partitions"].seqRead - before["daily_metrics_partitions"].seqRead +
		after["daily_metrics_partitions"].idxFetch - before["daily_metrics_partitions"].idxFetch
	t.Logf("PASS-PATH RUNS: table rows runs=%d partitions=%d; runs seq_scan +%d seq_tup_read +%d idx_tup_fetch +%d; partitions read +%d; math: RUNS 1 x runs, PART <= 1 x runs + 1 x partitions (+ <= 1 runs probe per partition), DAY 1 x runs",
		runsIn, partsIn, runScans, runRead, runIdx, partRead)
	if runScans < 2 || runScans > 3 || runRead != runScans*runsIn || runIdx > partsIn || partRead > partsIn || partRead < 6 {
		t.Errorf("pass-path scan count: runs scans %d (want 2..3) read %d (want scans x %d) idx %d (want <= %d), partitions read %d (want 6..%d)",
			runScans, runRead, runsIn, runIdx, partsIn, partRead, partsIn)
	}
	var vector string
	if err := conn.QueryRow(context.Background(), `SELECT generation FROM public.daily_metrics_runs WHERE id = $1::uuid`, card["2026-11-10"]).Scan(&vector); err != nil || vector != rfVector1110 {
		t.Fatalf("stored card generation %q (%v), want %s", vector, err, rfVector1110)
	}
	rfContract(t, "RUNS pass", rows["RUNS"][0], "orgs", "runs", "succeeded", "open", "failed", "org_open")
	rfWant(t, "RUNS pass RUNS", rows["RUNS"][0], map[string]string{"orgs": "1", "runs": "3", "succeeded": "3", "open": "0",
		"failed": "0", "org_open": "0", "days": "3", "range_days": "3", "finalized": "3", "finalize_retry": "0", "blocked": "0",
		"other_end": "0", "other_open": "0", "org_open_first_day": "", "ctrl_range_runs": "8"})
	rfWant(t, "RUNS pass PART", rows["PART"][0], map[string]string{"card_partitions": "6", "part_succeeded": "6",
		"part_open": "0", "part_failed": "0", "part_failed_permanent": "0", "ctrl_range_partitions": "14"})
	dayWant("RUNS pass", rows, map[string]map[string]string{"2026-11-10": doneDay("2"), "2026-11-11": doneDay("4"), "2026-11-12": doneDay("2")})

	// FAIL: a card run still open (finalize pending, one partition pending): WAIT.
	rfSetRun(t, conn, card["2026-11-11"], "running", "pending", t0.Add(3*time.Hour), rfAt(t0.Add(2*time.Hour)))
	rfSetPartition(t, conn, card["2026-11-11"], 1, "pending")
	rows = rfRead(t, uri, "RUNS open", form, labels...)
	rfWant(t, "RUNS open RUNS", rows["RUNS"][0], map[string]string{"runs": "3", "succeeded": "2", "open": "1", "failed": "0",
		"org_open": "1", "other_open": "0", "org_open_first_day": "2026-11-11"})
	rfWant(t, "RUNS open PART", rows["PART"][0], map[string]string{"part_open": "1", "part_succeeded": "5"})
	dayWant("RUNS open", rows, map[string]map[string]string{"2026-11-10": doneDay("2"),
		"2026-11-11": {"d_card_runs": "1", "d_card_done": "0", "d_card_open": "1", "d_card_failed": "0", "d_ctrl_runs": "4"},
		"2026-11-12": doneDay("2")})
	rfSetRun(t, conn, card["2026-11-11"], "succeeded", "succeeded", t0.Add(2*time.Hour), rfAt(t0.Add(2*time.Hour)))
	rfSetPartition(t, conn, card["2026-11-11"], 1, "succeeded")

	// FAIL: a card run failed with a failed_permanent partition: STOP.
	rfSetRun(t, conn, card["2026-11-12"], "failed", "failed", t0.Add(3*time.Hour), rfAt(t0.Add(3*time.Hour)))
	rfSetPartition(t, conn, card["2026-11-12"], 0, "failed_permanent")
	rows = rfRead(t, uri, "RUNS failed", form, labels...)
	rfWant(t, "RUNS failed RUNS", rows["RUNS"][0], map[string]string{"failed": "1", "succeeded": "2", "open": "0", "org_open": "0"})
	rfWant(t, "RUNS failed PART", rows["PART"][0], map[string]string{"part_failed_permanent": "1", "part_succeeded": "5"})
	dayWant("RUNS failed", rows, map[string]map[string]string{"2026-11-10": doneDay("2"), "2026-11-11": doneDay("4"),
		"2026-11-12": {"d_card_runs": "1", "d_card_done": "0", "d_card_open": "0", "d_card_failed": "1", "d_ctrl_runs": "2"}})
	rfSetRun(t, conn, card["2026-11-12"], "succeeded", "succeeded", t0.Add(2*time.Hour), rfAt(t0.Add(2*time.Hour)))
	rfSetPartition(t, conn, card["2026-11-12"], 0, "succeeded")

	// FAIL: a retryable finalize release (status running, finalization failed) and a retryable failed partition: WAIT.
	rfSetRun(t, conn, card["2026-11-10"], "running", "failed", t0.Add(3*time.Hour), rfAt(t0.Add(2*time.Hour)))
	rfSetPartition(t, conn, card["2026-11-10"], 1, "failed")
	rows = rfRead(t, uri, "RUNS retry", form, labels...)
	rfWant(t, "RUNS retry RUNS", rows["RUNS"][0], map[string]string{"finalize_retry": "1", "open": "1", "failed": "0", "succeeded": "2", "org_open": "1"})
	rfWant(t, "RUNS retry PART", rows["PART"][0], map[string]string{"part_failed": "1", "part_failed_permanent": "0"})
	rfSetRun(t, conn, card["2026-11-10"], "succeeded", "succeeded", t0.Add(2*time.Hour), rfAt(t0.Add(2*time.Hour)))
	rfSetPartition(t, conn, card["2026-11-10"], 1, "succeeded")

	// FAIL: a blocked card run: STOP.
	rfExec(t, conn, `UPDATE public.daily_metrics_runs SET blocked_at = $2, blocked_reason = 'synthetic' WHERE id = $1::uuid`, card["2026-11-10"], t0.Add(4*time.Hour))
	rows = rfRead(t, uri, "RUNS blocked", form, labels...)
	rfWant(t, "RUNS blocked RUNS", rows["RUNS"][0], map[string]string{"blocked": "1"})
	rfExec(t, conn, `UPDATE public.daily_metrics_runs SET blocked_at = NULL, blocked_reason = NULL WHERE id = $1::uuid`, card["2026-11-10"])

	// FAIL: a day of the chunk with no card run (partial chunk after already_covered): the DAY list names it.
	rfExec(t, conn, `DELETE FROM public.daily_metrics_runs WHERE id = $1::uuid`, card["2026-11-11"])
	rows = rfRead(t, uri, "RUNS missing", form, labels...)
	rfWant(t, "RUNS missing RUNS", rows["RUNS"][0], map[string]string{"runs": "2", "days": "2", "range_days": "3", "ctrl_range_runs": "7"})
	dayWant("RUNS missing", rows, map[string]map[string]string{"2026-11-10": doneDay("2"),
		"2026-11-11": {"d_card_runs": "0", "d_card_done": "0", "d_card_open": "0", "d_card_failed": "0", "d_ctrl_runs": "3"},
		"2026-11-12": doneDay("2")})
	card["2026-11-11"] = rfInsertRun(t, conn, cardRun("2026-11-11", rfTag, ""))

	// FAIL (P4): another daily run of the org open, outside the chunk, then inside it.
	_, finished13 := done("2026-11-13", 2)
	rfSetRun(t, conn, nightly["2026-11-13"], "running", "pending", finished13, nil)
	rows = rfRead(t, uri, "RUNS other-open", form, labels...)
	rfWant(t, "RUNS other-open RUNS", rows["RUNS"][0], map[string]string{"org_open": "1", "other_open": "1", "open": "0", "org_open_first_day": "2026-11-13"})
	rfSetRun(t, conn, nightly["2026-11-13"], "succeeded", "succeeded", finished13, rfAt(finished13))
	_, finished10 := done("2026-11-10", 2)
	rfSetRun(t, conn, nightly["2026-11-10"], "pending", "pending", finished10, nil)
	rows = rfRead(t, uri, "RUNS other-open-in-range", form, labels...)
	rfWant(t, "RUNS other-open-in-range RUNS", rows["RUNS"][0], map[string]string{"org_open": "1", "other_open": "1", "open": "0", "org_open_first_day": "2026-11-10"})
	rfSetRun(t, conn, nightly["2026-11-10"], "succeeded", "succeeded", finished10, rfAt(finished10))

	// FAIL (ORG-1): a second org with the same 8-char prefix.
	a2 := rfInsertRun(t, conn, rfRun{org: rfOrgA2, day: "2026-11-10", generation: "fixed-schedule:daily_metrics_fanout:rf-a2",
		status: "succeeded", finalization: "succeeded", fullOrg: true, created: t0, updated: t0, finalized: rfAt(t0)})
	rows = rfRead(t, uri, "RUNS org-1", form, labels...)
	rfWant(t, "RUNS org-1 RUNS", rows["RUNS"][0], map[string]string{"orgs": "2"})
	rfExec(t, conn, `DELETE FROM public.daily_metrics_runs WHERE id = $1::uuid`, a2)

	// FAIL (controls): a range with no run of the org.
	empty := rfFill(t, rfRunsForm, map[string]string{"FROM_DAY": "2026-10-01", "TO_DAY": "2026-10-02", "RERUN_TAG": "recomp-13902-13891-2026-10-01"})
	rows = rfRead(t, uri, "RUNS ctrl-empty-range", empty, labels...)
	rfWant(t, "RUNS ctrl-empty-range RUNS", rows["RUNS"][0], map[string]string{"ctrl_range_runs": "0", "runs": "0", "range_days": "2", "orgs": "1"})
	rfWant(t, "RUNS ctrl-empty-range PART", rows["PART"][0], map[string]string{"ctrl_range_partitions": "0"})
	dayWant("RUNS ctrl-empty-range", rows, map[string]map[string]string{
		"2026-10-01": {"d_card_runs": "0", "d_ctrl_runs": "0"}, "2026-10-02": {"d_card_runs": "0", "d_ctrl_runs": "0"}})

	// FAIL: the right range with another chunk's tag: no card run (the tag decides).
	wrongTag := rfFill(t, rfRunsForm, map[string]string{"FROM_DAY": "2026-11-10", "TO_DAY": "2026-11-12", "RERUN_TAG": "recomp-13902-13891-2026-11-13"})
	rows = rfRead(t, uri, "RUNS wrong-tag", wrongTag, labels...)
	rfWant(t, "RUNS wrong-tag RUNS", rows["RUNS"][0], map[string]string{"runs": "0", "ctrl_range_runs": "8"})

	// P4 read before step 1 (runs-read.sh P4: the run range, tag recomp-13902-13891-P4): no card run, nothing open.
	p4 := rfFill(t, rfRunsForm, map[string]string{"FROM_DAY": "2026-11-09", "TO_DAY": "2026-11-13", "RERUN_TAG": "recomp-13902-13891-P4"})
	rows = rfRead(t, uri, "RUNS p4", p4, labels...)
	rfContract(t, "RUNS p4", rows["RUNS"][0], "orgs", "runs", "succeeded", "open", "failed", "org_open")
	rfWant(t, "RUNS p4 RUNS", rows["RUNS"][0], map[string]string{"orgs": "1", "runs": "0", "org_open": "0", "range_days": "5", "ctrl_range_runs": "11"})

	// The pass read again after the restores: every value back.
	rows = rfRead(t, uri, "RUNS pass-again", form, labels...)
	rfWant(t, "RUNS pass-again RUNS", rows["RUNS"][0], map[string]string{"orgs": "1", "runs": "3", "succeeded": "3", "open": "0",
		"failed": "0", "org_open": "0", "finalized": "3", "ctrl_range_runs": "8"})

	// Gate fires: each cap set to 0, the index name and a table name changed. Data present and analyzed.
	for _, fire := range [][3]string{
		{"runs rows", "g.runs_rows <= 500000", "g.runs_rows <= 0"},
		{"runs bytes", "g.runs_bytes <= 536870912", "g.runs_bytes <= 0"},
		{"partitions rows", "g.parts_rows <= 1000000", "g.parts_rows <= 0"},
		{"partitions bytes", "g.parts_bytes <= 1073741824", "g.parts_bytes <= 0"},
		{"partition index", "'ix_daily_metrics_partition_run_status'", "'ix_rf_no_such_index'"},
		{"table missing", "('daily_metrics_runs', 'daily_metrics_partitions')) AS rel", "('daily_metrics_runs', 'rf_no_such_table')) AS rel"},
	} {
		rfFire(t, uri, "GATE RUNS "+fire[0], form, fire[1], fire[2])
	}
}

func TestFinalizeDayReadFormGivesTheDayStateAndStopsOnItsGate(t *testing.T) {
	rfCheckForm(t, "FINALIZE-DAY-13902-2", rfFinalizeDayForm, rfFinalizeDayFormSHA256)
	uri, conn := rfDatabase(t)
	ts := time.Date(2026, 11, 20, 6, 0, 0, 0, time.UTC)
	form := rfFill(t, rfFinalizeDayForm, map[string]string{"DAY": "2026-11-11", "TS_UTC": "2026-11-20 06:00:00"})
	rfNeverAnalyzedThenEmpty(t, uri, conn, form, "FIN", map[string]string{"orgs": "0", "ctrl_day_runs": "0", "finalized_since": "0"})

	runsIn := int64(0)
	insert := func(run rfRun) string {
		runsIn++
		return rfInsertRun(t, conn, run)
	}
	nightlyDone := time.Date(2026, 11, 12, 3, 0, 0, 0, time.UTC)
	nightly := insert(rfRun{org: rfOrgA, day: "2026-11-11", generation: "fixed-schedule:daily_metrics_fanout:rf-1111",
		status: "succeeded", finalization: "succeeded", fullOrg: true, created: nightlyDone.Add(-time.Hour), updated: nightlyDone,
		finalized: rfAt(nightlyDone)})
	insert(rfRun{org: rfOrgA, day: "2026-11-12", generation: "fixed-schedule:daily_metrics_fanout:rf-1112",
		status: "succeeded", finalization: "succeeded", fullOrg: true, created: nightlyDone, updated: nightlyDone,
		finalized: rfAt(nightlyDone)})
	pass1 := ts.Add(-30 * time.Minute)
	card := insert(rfRun{org: rfOrgA, day: "2026-11-11", generation: rfVector1111 + "#finalize-redrive:rfnonce1",
		status: "succeeded", finalization: "succeeded", fullOrg: true, created: ts.Add(-50 * time.Minute),
		updated: ts.Add(5 * time.Minute), finalized: rfAt(ts.Add(5 * time.Minute))})
	insert(rfRun{org: rfOrgB, day: "2026-11-11", generation: rfGeneration(rfOrgB, "2026-11-11", rfTag), status: "failed",
		finalization: "failed", fullOrg: true, created: ts, updated: ts.Add(time.Minute), finalized: rfAt(ts.Add(time.Minute))})
	rfAnalyze(t, conn)
	t.Logf("seeded reltuples: %s", rfEstRows(t, conn))
	labels := []string{"G0", "FIN"}
	restore := func() {
		rfSetRun(t, conn, card, "succeeded", "succeeded", ts.Add(5*time.Minute), rfAt(ts.Add(5*time.Minute)))
		rfSetRun(t, conn, nightly, "succeeded", "succeeded", nightlyDone, rfAt(nightlyDone))
	}

	// PASS path with the scan count.
	before := rfWaitScans(t, uri, "seed flush", func(s map[string]rfScan) bool { return s["daily_metrics_runs"].inserted == runsIn })
	rows := rfRead(t, uri, "FIN pass", form, labels...)
	after := rfWaitScans(t, uri, "pass read flush", func(s map[string]rfScan) bool {
		return s["daily_metrics_runs"].seqScan >= before["daily_metrics_runs"].seqScan+1
	})
	runScans := after["daily_metrics_runs"].seqScan - before["daily_metrics_runs"].seqScan
	runRead := after["daily_metrics_runs"].seqRead - before["daily_metrics_runs"].seqRead
	t.Logf("PASS-PATH FIN: runs rows=%d; runs seq_scan +%d seq_tup_read +%d (math 1 x %d = %d)", runsIn, runScans, runRead, runsIn, runsIn)
	if runScans != 1 || runRead != runsIn {
		t.Errorf("pass-path scan count: runs scans %d read %d, want 1 and %d", runScans, runRead, runsIn)
	}
	rfContract(t, "FIN pass", rows["FIN"][0], "orgs", "finalized_since", "failed_since", "open_since")
	rfWant(t, "FIN pass FIN", rows["FIN"][0], map[string]string{"orgs": "1", "finalized_since": "1", "failed_since": "0", "open_since": "0",
		"open_now": "0", "finalize_retry_since": "0", "finalized_since_card": "1", "ctrl_day_runs": "2", "ts_not_future": "t"})

	// FAIL: reset by the redrive, finalize not done yet (finalized_at still the pass 1 time): read again.
	rfSetRun(t, conn, card, "running", "pending", ts.Add(time.Minute), rfAt(pass1))
	rows = rfRead(t, uri, "FIN open", form, labels...)
	rfWant(t, "FIN open FIN", rows["FIN"][0], map[string]string{"finalized_since": "0", "open_since": "1", "open_now": "1", "failed_since": "0"})
	restore()

	// FAIL: terminal finalize failure: STOP.
	rfSetRun(t, conn, card, "failed", "failed", ts.Add(7*time.Minute), rfAt(ts.Add(7*time.Minute)))
	rows = rfRead(t, uri, "FIN failed", form, labels...)
	rfWant(t, "FIN failed FIN", rows["FIN"][0], map[string]string{"failed_since": "1", "finalized_since": "0", "open_since": "0"})
	restore()

	// FAIL: retryable finalize release: open, not failed.
	rfSetRun(t, conn, card, "running", "failed", ts.Add(7*time.Minute), rfAt(pass1))
	rows = rfRead(t, uri, "FIN retry", form, labels...)
	rfWant(t, "FIN retry FIN", rows["FIN"][0], map[string]string{"finalize_retry_since": "1", "open_since": "1", "failed_since": "0", "finalized_since": "0"})
	restore()

	// FAIL: the call changed nothing (no run touched after TS): finalized_since 0 = STOP.
	rfSetRun(t, conn, card, "succeeded", "succeeded", pass1, rfAt(pass1))
	rows = rfRead(t, uri, "FIN untouched", form, labels...)
	rfWant(t, "FIN untouched FIN", rows["FIN"][0], map[string]string{"finalized_since": "0", "open_since": "0", "failed_since": "0", "finalized_since_card": "0"})
	// INFO: another run of the day finalized after TS, the card run not: finalized_since 1, finalized_since_card 0.
	rfSetRun(t, conn, nightly, "succeeded", "succeeded", ts.Add(5*time.Minute), rfAt(ts.Add(5*time.Minute)))
	rows = rfRead(t, uri, "FIN not-card", form, labels...)
	rfWant(t, "FIN not-card FIN", rows["FIN"][0], map[string]string{"finalized_since": "1", "finalized_since_card": "0"})
	restore()

	// FAIL (ORG-1): a second org with the same 8-char prefix.
	a2 := rfInsertRun(t, conn, rfRun{org: rfOrgA2, day: "2026-11-12", generation: "fixed-schedule:daily_metrics_fanout:rf-a2",
		status: "succeeded", finalization: "succeeded", fullOrg: true, created: ts, updated: ts, finalized: rfAt(ts)})
	rows = rfRead(t, uri, "FIN org-1", form, labels...)
	rfWant(t, "FIN org-1 FIN", rows["FIN"][0], map[string]string{"orgs": "2"})
	rfExec(t, conn, `DELETE FROM public.daily_metrics_runs WHERE id = $1::uuid`, a2)

	// FAIL (control): a day with no run of the org.
	empty := rfFill(t, rfFinalizeDayForm, map[string]string{"DAY": "2026-10-01", "TS_UTC": "2026-11-20 06:00:00"})
	rows = rfRead(t, uri, "FIN ctrl-empty-day", empty, labels...)
	rfWant(t, "FIN ctrl-empty-day FIN", rows["FIN"][0], map[string]string{"ctrl_day_runs": "0", "finalized_since": "0", "orgs": "1"})
	// FAIL: a TS in the future.
	future := rfFill(t, rfFinalizeDayForm, map[string]string{"DAY": "2026-11-11", "TS_UTC": "2099-01-01 00:00:00"})
	rows = rfRead(t, uri, "FIN ts-future", future, labels...)
	rfWant(t, "FIN ts-future FIN", rows["FIN"][0], map[string]string{"ts_not_future": "f", "finalized_since": "0"})

	rows = rfRead(t, uri, "FIN pass-again", form, labels...)
	rfWant(t, "FIN pass-again FIN", rows["FIN"][0], map[string]string{"orgs": "1", "finalized_since": "1", "failed_since": "0", "open_since": "0", "finalized_since_card": "1", "ctrl_day_runs": "2"})

	for _, fire := range [][3]string{
		{"runs rows", "g.runs_rows <= 500000", "g.runs_rows <= 0"},
		{"runs bytes", "g.runs_bytes <= 536870912", "g.runs_bytes <= 0"},
		{"table missing", "IN ('daily_metrics_runs')) AS rel", "IN ('rf_no_such_table')) AS rel"},
	} {
		rfFire(t, uri, "GATE FIN "+fire[0], form, fire[1], fire[2])
	}
}
