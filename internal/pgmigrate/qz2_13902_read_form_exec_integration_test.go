//go:build integration

package pgmigrate_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
)

// Throwaway exec proof of the prod read form FORM-QZ2-13902-2 (CHAOS-13902, Q-z2): not for merge. qz2Form is the form
// file with its org prefix (8 hex) replaced by the synthetic "f0f0f0f0"; its sha256 is pinned. The cell runs the text the
// way pm does (psql -X -A -F'|' -v ON_ERROR_STOP=1 -f -, the form on stdin, PGOPTIONS statement_timeout=30000) against a
// scratch database at the repo chain head, with a synthetic org f0 and a foreign org b0 in every table the form reads.
const qz2SHA256 = "e15adb9919df7d193babbb545d7e3a8b8b1234a4399143417135cd422b395d2e"

const qz2Form = `-- FORM-QZ2-13902-2: Q-z2 of CARD-13902-recompute-13 (z): which sync window set the raw floor 2025-11-01 of git_commits.
-- Postgres, database devhealth, READ ONLY. Lane l13902 (lead fcdev), 2026-11-24. Prod: NOT EXECUTED. CI exec: OPEN (cell
--   internal/pgmigrate/qz2_13902_read_form_exec_integration_test.go). acp FORM read: OPEN. md: FORM-QZ2-13902-2.md.
-- -2 = FORM-QZ2-13902-1 (fc0a43549fed) + the acp fixes of READ-CARD-13902-10-1 (Q2-P2-1 ORG-1 in G0, Q2-P2-2 no
--   sub-second timestamps, Q2-P2-3 caps + work math in the md, Q2-P3-1 'incidents' in the dataset list) + stmt S0 (catalog
--   sizes, INFO) + BACKFILL as aggregates (no row list).
-- Shape: FORM-TEMPLATE-3 (b): one transaction REPEATABLE READ READ ONLY, G0 catalog gate first, ends ROLLBACK; no SET
--   (statement_timeout comes from PGOPTIONS); no write. Output = counts, text keys, UTC days, UTC times cut to the second.
-- Run: pm's PG read line: psql -X -A -F'|' -v ON_ERROR_STOP=1 -f -, this file on stdin. No placeholder.
-- Org scope: the org prefix is in 4 places, each a starts_with: G0 (organizations), UNITS, BACKFILL, FIRST-RUN.
-- Tables + columns: internal/pgmigrate/baseline/head.json at 3207ac6258c0 (chain 0163..0166 alter none of them):
--   organizations (id uuid), sync_run_units (org_id text, dataset_key varchar, mode varchar, since_at timestamptz,
--   created_at timestamptz), sync_runs (org_id text, created_at timestamptz), backfill_jobs (org_id varchar, status varchar,
--   since_date date, before_date date, created_at timestamptz).
-- Dataset keys: internal/scheduler/sync/planner.go:248-253 at 3207ac6258c0 ('incidents' at :249 gitlab, :250 jira, :253
--   pagerduty). Modes: planner.go:19-21 ('incremental', 'full_resync', 'backfill'). A unit's window start = since_at
--   (providersync/inprocess.go:431; github_commits_route.go:104-105 uses it as the since parameter).
-- Question: which unit (dataset, mode, created second) first fetched with a window start at or before 2025-11-01 00:00 UTC,
--   and whether any window started earlier. WANTs + reading: FORM-QZ2-13902-2.md.
-- Cost: starts_with uses no index: UNITS, BACKFILL, FIRST-RUN each read their one table at most once. G0 (catalog + the
--   organizations count) caps every table before any data statement; a STOP text names every size. Work math: the md.

BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;

-- G0 catalog gate. WANT cost_gate_ok 0. Else the cast error 'STOP-G0 ...' aborts the transaction and ON_ERROR_STOP ends
-- the file: STOP (no later statement runs). STOP when: orgs != 1, a table missing, never analyzed (reltuples -1), or a
-- table over its row or byte cap. Each cap literal is in this statement only, once.
SELECT 'G0' AS stmt,
       CASE WHEN g.orgs = 1 AND g.tables_found = 4 AND g.min_est_rows >= 0
                 AND g.org_rows <= 40000 AND g.org_bytes <= 67108864
                 AND g.units_rows <= 6000000 AND g.units_bytes <= 1610612736
                 AND g.runs_rows <= 3000000 AND g.runs_bytes <= 536870912
                 AND g.backfill_rows <= 250000 AND g.backfill_bytes <= 134217728
            THEN 0
            ELSE concat('STOP-G0 orgs=', g.orgs, ' tables=', g.tables_found, ' min_rows=', g.min_est_rows,
                        ' organizations=', g.org_rows, '/', g.org_bytes, ' units=', g.units_rows, '/', g.units_bytes,
                        ' runs=', g.runs_rows, '/', g.runs_bytes,
                        ' backfill=', g.backfill_rows, '/', g.backfill_bytes)::int
       END AS cost_gate_ok
FROM (SELECT (SELECT count(*) FROM public.organizations AS o WHERE starts_with(o.id::text, 'f0f0f0f0')) AS orgs,
             count(*) AS tables_found,
             min(rel.est_rows) AS min_est_rows,
             COALESCE(max(rel.est_rows) FILTER (WHERE rel.rel_name = 'organizations'), -1) AS org_rows,
             COALESCE(max(rel.est_bytes) FILTER (WHERE rel.rel_name = 'organizations'), -1) AS org_bytes,
             COALESCE(max(rel.est_rows) FILTER (WHERE rel.rel_name = 'sync_run_units'), -1) AS units_rows,
             COALESCE(max(rel.est_bytes) FILTER (WHERE rel.rel_name = 'sync_run_units'), -1) AS units_bytes,
             COALESCE(max(rel.est_rows) FILTER (WHERE rel.rel_name = 'sync_runs'), -1) AS runs_rows,
             COALESCE(max(rel.est_bytes) FILTER (WHERE rel.rel_name = 'sync_runs'), -1) AS runs_bytes,
             COALESCE(max(rel.est_rows) FILTER (WHERE rel.rel_name = 'backfill_jobs'), -1) AS backfill_rows,
             COALESCE(max(rel.est_bytes) FILTER (WHERE rel.rel_name = 'backfill_jobs'), -1) AS backfill_bytes
      FROM (SELECT c.relname AS rel_name, c.reltuples::bigint AS est_rows,
                   (c.relpages::bigint + COALESCE(tc.relpages::bigint, 0)) * current_setting('block_size')::bigint AS est_bytes
            FROM pg_catalog.pg_class AS c
              JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
              LEFT JOIN pg_catalog.pg_class AS tc ON tc.oid = c.reltoastrelid
            WHERE n.nspname = 'public' AND c.relkind = 'r'
              AND c.relname IN ('organizations', 'sync_run_units', 'sync_runs', 'backfill_jobs')) AS rel) AS g;

-- S0 (INFO, catalog only): the size of each read table, one row per table: estimated rows (reltuples), heap pages, TOAST
-- pages, est_bytes = (heap + TOAST pages) x block_size (the value G0 caps). The md's work math reads these on the first run.
SELECT 'S0' AS stmt, c.relname AS table_name, c.reltuples::bigint AS est_rows, c.relpages AS heap_pages,
       COALESCE(tc.relpages, 0) AS toast_pages,
       (c.relpages::bigint + COALESCE(tc.relpages::bigint, 0)) * current_setting('block_size')::bigint AS est_bytes
FROM pg_catalog.pg_class AS c
  JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
  LEFT JOIN pg_catalog.pg_class AS tc ON tc.oid = c.reltoastrelid
WHERE n.nspname = 'public' AND c.relkind = 'r'
  AND c.relname IN ('backfill_jobs', 'organizations', 'sync_runs', 'sync_run_units')
ORDER BY c.relname COLLATE "C";

-- UNITS: per (dataset_key, mode) of the org's raw-feeding datasets, plus one '<all>' row (control: WANT org_units > 0).
-- Times are UTC, cut to the second (to_char, no rounding).
SELECT 'UNITS' AS stmt,
       CASE WHEN GROUPING(u.dataset_key) = 1 THEN '<all>' ELSE u.dataset_key END AS dataset_key,
       CASE WHEN GROUPING(u.mode) = 1 THEN '<all>' ELSE u.mode END AS mode,
       count(*) AS org_units,
       to_char(min(u.created_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') AS first_created,
       to_char(min(u.since_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') AS min_since,
       count(*) FILTER (WHERE u.since_at IS NULL) AS null_since,
       count(*) FILTER (WHERE u.since_at < TIMESTAMPTZ '2025-11-01 00:00:00+00') AS since_before_floor,
       count(*) FILTER (WHERE u.since_at = TIMESTAMPTZ '2025-11-01 00:00:00+00') AS since_at_floor,
       to_char((min(u.created_at) FILTER (WHERE u.since_at <= TIMESTAMPTZ '2025-11-01 00:00:00+00')) AT TIME ZONE 'UTC',
               'YYYY-MM-DD HH24:MI:SS') AS first_created_at_or_before_floor
FROM public.sync_run_units AS u
WHERE starts_with(u.org_id, 'f0f0f0f0')
  AND u.dataset_key IN ('commits', 'commit-stats', 'prs', 'pr-reviews', 'cicd', 'tests', 'deployments', 'work-items', 'incidents')
GROUP BY GROUPING SETS ((), (u.dataset_key, u.mode))
ORDER BY GROUPING(u.dataset_key) DESC, u.dataset_key COLLATE "C", u.mode COLLATE "C";

-- BACKFILL: the org's backfill jobs as aggregates (no ids, no row list): one '<all>' row, then one row per status.
-- Days are YYYY-MM-DD; first_created_day is UTC.
SELECT 'BACKFILL' AS stmt,
       CASE WHEN GROUPING(b.status) = 1 THEN '<all>' ELSE b.status END AS status,
       count(*) AS jobs,
       to_char(min(b.since_date), 'YYYY-MM-DD') AS min_since_date,
       to_char(max(b.since_date), 'YYYY-MM-DD') AS max_since_date,
       to_char(min(b.before_date), 'YYYY-MM-DD') AS min_before_date,
       to_char(max(b.before_date), 'YYYY-MM-DD') AS max_before_date,
       count(*) FILTER (WHERE b.since_date <= DATE '2025-11-01') AS since_on_or_before_floor,
       to_char(min(b.created_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD') AS first_created_day
FROM public.backfill_jobs AS b
WHERE starts_with(b.org_id, 'f0f0f0f0')
GROUP BY GROUPING SETS ((), (b.status))
ORDER BY GROUPING(b.status) DESC, b.status COLLATE "C";

-- FIRST-RUN: the org's sync runs (count) and the first one (UTC, cut to the second).
SELECT 'FIRST-RUN' AS stmt, count(*) AS org_runs,
       to_char(min(r.created_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') AS first_run_created
FROM public.sync_runs AS r
WHERE starts_with(r.org_id, 'f0f0f0f0');

ROLLBACK;
`

const (
	qz2Org        = "f0f0f0f0-0000-4000-8000-000000000000"
	qz2OrgTwin    = "f0f0f0f0-0000-4000-8000-000000000001"
	qz2ForeignOrg = "b0000000-0000-4000-8000-000000000000"
)

// qz2Fixture: org f0 (the form's org) and the foreign org b0, each with an integration, a source and a sync config (FK
// parents), sync runs, sync run units and backfill jobs. Units: since_at before / at / after 2025-11-01 00:00Z and NULL;
// dataset keys in the form's list and out of it ('files'); modes incremental / full_resync / backfill (planner.go:19-21).
const qz2Fixture = `
INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at) VALUES
  ('f0f0f0f0-0000-4000-8000-000000000000', 'qz2-f0', 'QZ2 F0', 'community', 'stripe', true, now(), now()),
  ('b0000000-0000-4000-8000-000000000000', 'qz2-b0', 'QZ2 B0', 'community', 'stripe', true, now(), now());
INSERT INTO integrations (id, org_id, provider, name, config, is_active, created_at, updated_at) VALUES
  ('10000000-0000-4000-8000-0000000000f0', 'f0f0f0f0-0000-4000-8000-000000000000', 'github', 'qz2 f0', '{}', true, now(), now()),
  ('10000000-0000-4000-8000-0000000000b0', 'b0000000-0000-4000-8000-000000000000', 'github', 'qz2 b0', '{}', true, now(), now());
INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata,
    is_enabled, discovered_at, last_seen_at) VALUES
  ('20000000-0000-4000-8000-0000000000f0', 'f0f0f0f0-0000-4000-8000-000000000000', '10000000-0000-4000-8000-0000000000f0',
   'github', 'repository', 'qz2-f0/repo', 'repo', 'qz2-f0/repo', '{}', true, now(), now()),
  ('20000000-0000-4000-8000-0000000000b0', 'b0000000-0000-4000-8000-000000000000', '10000000-0000-4000-8000-0000000000b0',
   'github', 'repository', 'qz2-b0/repo', 'repo', 'qz2-b0/repo', '{}', true, now(), now());
INSERT INTO sync_configurations (id, org_id, name, provider, created_at, updated_at) VALUES
  ('50000000-0000-4000-8000-0000000000f0', 'f0f0f0f0-0000-4000-8000-000000000000', 'qz2 f0', 'github', now(), now()),
  ('50000000-0000-4000-8000-0000000000b0', 'b0000000-0000-4000-8000-000000000000', 'qz2 b0', 'github', now(), now());
INSERT INTO sync_runs (id, org_id, integration_id, triggered_by, mode, status, total_units, completed_units, failed_units,
    created_at) VALUES
  ('30000000-0000-4000-8000-000000000001', 'f0f0f0f0-0000-4000-8000-000000000000', '10000000-0000-4000-8000-0000000000f0',
   'schedule', 'incremental', 'success', 5, 5, 0, '2025-12-01 06:00:00.123456+00'),
  ('30000000-0000-4000-8000-000000000002', 'f0f0f0f0-0000-4000-8000-000000000000', '10000000-0000-4000-8000-0000000000f0',
   'manual', 'backfill', 'success', 2, 2, 0, '2026-01-05 00:00:00+00'),
  ('30000000-0000-4000-8000-0000000000b0', 'b0000000-0000-4000-8000-000000000000', '10000000-0000-4000-8000-0000000000b0',
   'schedule', 'incremental', 'success', 3, 3, 0, '2025-10-01 00:00:00+00');
INSERT INTO sync_run_units (id, org_id, sync_run_id, integration_id, source_id, provider, dataset_key, cost_class, mode,
    since_at, before_at, status, attempts, created_at, updated_at) VALUES
  ('40000000-0000-4000-8000-000000000001', 'f0f0f0f0-0000-4000-8000-000000000000', '30000000-0000-4000-8000-000000000001',
   '10000000-0000-4000-8000-0000000000f0', '20000000-0000-4000-8000-0000000000f0', 'github', 'commits', 'standard',
   'incremental', '2025-11-01 00:00:00+00', '2025-12-01 06:00:00+00', 'success', 1, '2025-12-01 06:00:01.5+00', now()),
  ('40000000-0000-4000-8000-000000000002', 'f0f0f0f0-0000-4000-8000-000000000000', '30000000-0000-4000-8000-000000000002',
   '10000000-0000-4000-8000-0000000000f0', '20000000-0000-4000-8000-0000000000f0', 'github', 'commits', 'standard',
   'incremental', '2025-11-25 00:00:00+00', '2026-01-05 00:00:00+00', 'success', 1, '2026-01-05 00:00:00+00', now()),
  ('40000000-0000-4000-8000-000000000003', 'f0f0f0f0-0000-4000-8000-000000000000', '30000000-0000-4000-8000-000000000002',
   '10000000-0000-4000-8000-0000000000f0', '20000000-0000-4000-8000-0000000000f0', 'github', 'commits', 'standard',
   'backfill', '2025-10-15 00:00:00+00', '2025-11-01 00:00:00+00', 'success', 1, '2026-01-05 00:00:02.999999+00', now()),
  ('40000000-0000-4000-8000-000000000004', 'f0f0f0f0-0000-4000-8000-000000000000', '30000000-0000-4000-8000-000000000001',
   '10000000-0000-4000-8000-0000000000f0', '20000000-0000-4000-8000-0000000000f0', 'github', 'commit-stats', 'heavy',
   'incremental', NULL, NULL, 'success', 1, '2025-12-01 06:00:02+00', now()),
  ('40000000-0000-4000-8000-000000000005', 'f0f0f0f0-0000-4000-8000-000000000000', '30000000-0000-4000-8000-000000000001',
   '10000000-0000-4000-8000-0000000000f0', '20000000-0000-4000-8000-0000000000f0', 'github', 'prs', 'standard',
   'full_resync', '2025-11-01 00:00:00+00', '2025-12-01 06:00:00+00', 'success', 1, '2025-12-01 06:00:03+00', now()),
  ('40000000-0000-4000-8000-000000000006', 'f0f0f0f0-0000-4000-8000-000000000000', '30000000-0000-4000-8000-000000000001',
   '10000000-0000-4000-8000-0000000000f0', '20000000-0000-4000-8000-0000000000f0', 'github', 'files', 'heavy',
   'incremental', '2025-10-01 00:00:00+00', '2025-12-01 06:00:00+00', 'success', 1, '2025-12-01 06:00:04+00', now()),
  ('40000000-0000-4000-8000-000000000007', 'f0f0f0f0-0000-4000-8000-000000000000', '30000000-0000-4000-8000-000000000001',
   '10000000-0000-4000-8000-0000000000f0', '20000000-0000-4000-8000-0000000000f0', 'jira', 'incidents', 'light',
   'incremental', '2025-11-20 00:00:00+00', '2025-12-01 06:00:00+00', 'success', 1, '2025-12-01 06:00:05+00', now()),
  ('40000000-0000-4000-8000-0000000000b8', 'b0000000-0000-4000-8000-000000000000', '30000000-0000-4000-8000-0000000000b0',
   '10000000-0000-4000-8000-0000000000b0', '20000000-0000-4000-8000-0000000000b0', 'github', 'commits', 'standard',
   'incremental', '2025-09-01 00:00:00+00', '2025-10-01 00:00:00+00', 'success', 1, '2025-10-01 00:00:01+00', now()),
  ('40000000-0000-4000-8000-0000000000b9', 'b0000000-0000-4000-8000-000000000000', '30000000-0000-4000-8000-0000000000b0',
   '10000000-0000-4000-8000-0000000000b0', '20000000-0000-4000-8000-0000000000b0', 'github', 'prs', 'standard',
   'backfill', '2025-11-01 00:00:00+00', '2025-11-15 00:00:00+00', 'success', 1, '2025-10-01 00:00:02+00', now()),
  ('40000000-0000-4000-8000-0000000000ba', 'b0000000-0000-4000-8000-000000000000', '30000000-0000-4000-8000-0000000000b0',
   '10000000-0000-4000-8000-0000000000b0', '20000000-0000-4000-8000-0000000000b0', 'github', 'files', 'heavy',
   'incremental', NULL, NULL, 'success', 1, '2025-10-01 00:00:03+00', now());
INSERT INTO backfill_jobs (id, org_id, sync_config_id, status, since_date, before_date, total_chunks, completed_chunks,
    failed_chunks, created_at, updated_at) VALUES
  ('60000000-0000-4000-8000-000000000001', 'f0f0f0f0-0000-4000-8000-000000000000', '50000000-0000-4000-8000-0000000000f0',
   'completed', '2025-11-01', '2025-12-01', 1, 1, 0, '2025-12-01 05:00:00.5+00', now()),
  ('60000000-0000-4000-8000-000000000002', 'f0f0f0f0-0000-4000-8000-000000000000', '50000000-0000-4000-8000-0000000000f0',
   'pending', '2025-12-15', '2026-01-01', 0, 0, 0, '2026-01-02 00:00:00+00', now()),
  ('60000000-0000-4000-8000-000000000003', 'f0f0f0f0-0000-4000-8000-000000000000', '50000000-0000-4000-8000-0000000000f0',
   'completed', '2025-10-01', '2025-11-01', 1, 1, 0, '2026-01-03 00:00:00+00', now()),
  ('60000000-0000-4000-8000-0000000000b4', 'b0000000-0000-4000-8000-000000000000', '50000000-0000-4000-8000-0000000000b0',
   'failed', '2025-06-01', '2025-07-01', 1, 0, 1, '2025-07-02 00:00:00+00', now());
`

const qz2Tables = "organizations, sync_run_units, sync_runs, backfill_jobs"

// qz2Result is one psql run: the exit code, the non-empty stdout lines, stderr.
type qz2Result struct {
	rc     int
	lines  []string
	stderr string
}

// qz2Psql runs form the way pm does: psql -X -A -F'|' -v ON_ERROR_STOP=1 -f -, the form on stdin, PGOPTIONS
// statement_timeout=30000. Every line goes to t.Logf (the junit system-out). The DSN is never logged.
func qz2Psql(t *testing.T, psql, uri, name, form string) qz2Result {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, psql, "-X", "-A", "-F|", "-v", "ON_ERROR_STOP=1", "-f", "-", "-d", uri)
	cmd.Env = append(os.Environ(), "PGOPTIONS=-c statement_timeout=30000")
	cmd.Stdin = strings.NewReader(form)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	rc := 0
	if err := cmd.Run(); err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			t.Fatalf("%s: run psql: %v", name, err)
		}
		rc = exitErr.ExitCode()
	}
	var lines []string
	for _, line := range strings.Split(stdout.String(), "\n") {
		if line = strings.TrimRight(line, "\r"); line != "" {
			lines = append(lines, line)
		}
	}
	t.Logf("%s: psql rc=%d form sha256=%s", name, rc, qz2Sum(form))
	for _, line := range lines {
		t.Logf("%s: out %s", name, line)
	}
	for _, line := range strings.Split(stderr.String(), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			t.Logf("%s: err %s", name, line)
		}
	}
	return qz2Result{rc: rc, lines: lines, stderr: stderr.String()}
}

func qz2Sum(text string) string {
	sum := sha256.Sum256([]byte(text))
	return hex.EncodeToString(sum[:])
}

// qz2Mutate replaces the one place of old in form by replacement; a missing or repeated old is a Fatal (the mutant
// would test nothing, or more than its one place).
func qz2Mutate(t *testing.T, form, old, replacement string) string {
	t.Helper()
	if n := strings.Count(form, old); n != 1 {
		t.Fatalf("mutant text %q is in the form %d times, want 1", old, n)
	}
	return strings.Replace(form, old, replacement, 1)
}

func qz2Footer(rows int) string {
	if rows == 1 {
		return "(1 row)"
	}
	return fmt.Sprintf("(%d rows)", rows)
}

// qz2Block is one result: the header, the rows, the row-count footer.
func qz2Block(header string, rows ...string) []string {
	return append(append([]string{header}, rows...), qz2Footer(len(rows)))
}

const (
	qz2UnitsHeader    = "stmt|dataset_key|mode|org_units|first_created|min_since|null_since|since_before_floor|since_at_floor|first_created_at_or_before_floor"
	qz2BackfillHeader = "stmt|status|jobs|min_since_date|max_since_date|min_before_date|max_before_date|since_on_or_before_floor|first_created_day"
	qz2RunsHeader     = "stmt|org_runs|first_run_created"
)

func qz2G0Pass() []string { return qz2Block("stmt|cost_gate_ok", "G0|0") }

func qz2S0() []string {
	return qz2Block("stmt|table_name|est_rows|heap_pages|toast_pages|est_bytes",
		"S0|backfill_jobs|4|1|0|8192",
		"S0|organizations|2|1|0|8192",
		"S0|sync_run_units|10|1|0|8192",
		"S0|sync_runs|3|1|0|8192")
}

func qz2UnitsPass() []string {
	return qz2Block(qz2UnitsHeader,
		"UNITS|<all>|<all>|6|2025-12-01 06:00:01|2025-10-15 00:00:00|1|1|2|2025-12-01 06:00:01",
		"UNITS|commit-stats|incremental|1|2025-12-01 06:00:02||1|0|0|",
		"UNITS|commits|backfill|1|2026-01-05 00:00:02|2025-10-15 00:00:00|0|1|0|2026-01-05 00:00:02",
		"UNITS|commits|incremental|2|2025-12-01 06:00:01|2025-11-01 00:00:00|0|0|1|2025-12-01 06:00:01",
		"UNITS|incidents|incremental|1|2025-12-01 06:00:05|2025-11-20 00:00:00|0|0|0|",
		"UNITS|prs|full_resync|1|2025-12-01 06:00:03|2025-11-01 00:00:00|0|0|1|2025-12-01 06:00:03")
}

func qz2BackfillPass() []string {
	return qz2Block(qz2BackfillHeader,
		"BACKFILL|<all>|3|2025-10-01|2025-12-15|2025-11-01|2026-01-01|2|2025-12-01",
		"BACKFILL|completed|2|2025-10-01|2025-11-01|2025-11-01|2025-12-01|2|2025-12-01",
		"BACKFILL|pending|1|2025-12-15|2025-12-15|2026-01-01|2026-01-01|0|2026-01-02")
}

func qz2RunsPass() []string { return qz2Block(qz2RunsHeader, "FIRST-RUN|2|2025-12-01 06:00:00") }

// qz2Transcript is the whole stdout of a run that passes G0.
func qz2Transcript(units, backfill, runs []string) []string {
	out := []string{"BEGIN"}
	for _, block := range [][]string{qz2G0Pass(), qz2S0(), units, backfill, runs} {
		out = append(out, block...)
	}
	return append(out, "ROLLBACK")
}

func qz2WantLines(t *testing.T, name string, got qz2Result, want []string) {
	t.Helper()
	if got.rc != 0 {
		t.Fatalf("%s: psql rc=%d, want 0; stderr: %s", name, got.rc, got.stderr)
	}
	if !reflect.DeepEqual(got.lines, want) {
		t.Fatalf("%s: output\n%s\nwant\n%s", name, strings.Join(got.lines, "\n"), strings.Join(want, "\n"))
	}
	if last := got.lines[len(got.lines)-1]; last != "ROLLBACK" {
		t.Fatalf("%s: transcript ends %q, want ROLLBACK", name, last)
	}
}

// qz2WantStop: psql rc != 0, stderr names STOP-G0 and every fragment, and stdout is BEGIN only (no statement after G0
// printed a line).
func qz2WantStop(t *testing.T, name string, got qz2Result, fragments ...string) {
	t.Helper()
	if got.rc == 0 {
		t.Fatalf("%s: psql rc=0, want != 0 (STOP-G0)", name)
	}
	for _, fragment := range append([]string{"STOP-G0"}, fragments...) {
		if !strings.Contains(got.stderr, fragment) {
			t.Fatalf("%s: stderr lacks %q: %s", name, fragment, got.stderr)
		}
	}
	if !reflect.DeepEqual(got.lines, []string{"BEGIN"}) {
		t.Fatalf("%s: stdout %q, want only BEGIN (no line of a later statement)", name, got.lines)
	}
}

func TestQZ213902ReadFormExec(t *testing.T) {
	if sum := qz2Sum(qz2Form); sum != qz2SHA256 {
		t.Fatalf("qz2Form sha256 = %s, want %s (the pinned swapped text)", sum, qz2SHA256)
	}
	if n := strings.Count(qz2Form, "'f0f0f0f0'"); n != 4 {
		t.Fatalf("qz2Form holds the org prefix %d times, want 4 (G0, UNITS, BACKFILL, FIRST-RUN)", n)
	}
	psql, err := exec.LookPath("psql")
	if err != nil {
		t.Fatalf("psql is not on PATH of this runner (%v): the cell runs the form with pm's psql line and cannot run without it", err)
	}
	if version, err := exec.Command(psql, "--version").Output(); err == nil {
		t.Logf("client: %s", strings.TrimSpace(string(version)))
	}

	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	d := newDownInstance(t)
	uri := d.at(t, len(chain))
	conn := connect(t, uri)
	ctx := context.Background()
	t.Logf("schema: baseline heads %v + %d chain files, head revision %s (%s)", baseline.Heads, len(chain),
		chain[len(chain)-1].Revision, chain[len(chain)-1].Name)
	var server string
	if err := conn.QueryRow(ctx, "SELECT current_setting('server_version')").Scan(&server); err != nil {
		t.Fatal(err)
	}
	t.Logf("server: PostgreSQL %s", server)

	if _, err := conn.Exec(ctx, qz2Fixture); err != nil {
		t.Fatalf("fixture: %v", err)
	}

	// Before ANALYZE: every read table is never analyzed (reltuples -1), so G0 must STOP.
	var neverAnalyzed int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'public' AND c.relkind = 'r' AND c.reltuples < 0
		  AND c.relname IN ('organizations', 'sync_run_units', 'sync_runs', 'backfill_jobs')`).Scan(&neverAnalyzed); err != nil {
		t.Fatal(err)
	}
	if neverAnalyzed != 4 {
		t.Fatalf("before ANALYZE %d of the 4 read tables have reltuples -1, want 4 (the fire cell needs a never-analyzed table)", neverAnalyzed)
	}
	t.Run("G0 fires on reltuples -1", func(t *testing.T) {
		qz2WantStop(t, "fire-reltuples", qz2Psql(t, psql, uri, "fire-reltuples", qz2Form), "min_rows=-1")
	})

	if _, err := conn.Exec(ctx, "ANALYZE "+qz2Tables); err != nil {
		t.Fatalf("analyze: %v", err)
	}

	t.Run("PASS", func(t *testing.T) {
		qz2WantLines(t, "pass", qz2Psql(t, psql, uri, "pass", qz2Form), qz2Transcript(qz2UnitsPass(), qz2BackfillPass(), qz2RunsPass()))
	})

	t.Run("org mutant G0", func(t *testing.T) {
		form := qz2Mutate(t, qz2Form, "starts_with(o.id::text, 'f0f0f0f0')", "starts_with(o.id::text, '')")
		qz2WantStop(t, "org-g0", qz2Psql(t, psql, uri, "org-g0", form), "orgs=2")
	})
	t.Run("org mutant UNITS", func(t *testing.T) {
		form := qz2Mutate(t, qz2Form, "starts_with(u.org_id, 'f0f0f0f0')", "starts_with(u.org_id, '')")
		units := qz2Block(qz2UnitsHeader,
			"UNITS|<all>|<all>|8|2025-10-01 00:00:01|2025-09-01 00:00:00|1|2|3|2025-10-01 00:00:01",
			"UNITS|commit-stats|incremental|1|2025-12-01 06:00:02||1|0|0|",
			"UNITS|commits|backfill|1|2026-01-05 00:00:02|2025-10-15 00:00:00|0|1|0|2026-01-05 00:00:02",
			"UNITS|commits|incremental|3|2025-10-01 00:00:01|2025-09-01 00:00:00|0|1|1|2025-10-01 00:00:01",
			"UNITS|incidents|incremental|1|2025-12-01 06:00:05|2025-11-20 00:00:00|0|0|0|",
			"UNITS|prs|backfill|1|2025-10-01 00:00:02|2025-11-01 00:00:00|0|0|1|2025-10-01 00:00:02",
			"UNITS|prs|full_resync|1|2025-12-01 06:00:03|2025-11-01 00:00:00|0|0|1|2025-12-01 06:00:03")
		qz2WantLines(t, "org-units", qz2Psql(t, psql, uri, "org-units", form), qz2Transcript(units, qz2BackfillPass(), qz2RunsPass()))
	})
	t.Run("org mutant BACKFILL", func(t *testing.T) {
		form := qz2Mutate(t, qz2Form, "starts_with(b.org_id, 'f0f0f0f0')", "starts_with(b.org_id, '')")
		backfill := qz2Block(qz2BackfillHeader,
			"BACKFILL|<all>|4|2025-06-01|2025-12-15|2025-07-01|2026-01-01|3|2025-07-02",
			"BACKFILL|completed|2|2025-10-01|2025-11-01|2025-11-01|2025-12-01|2|2025-12-01",
			"BACKFILL|failed|1|2025-06-01|2025-06-01|2025-07-01|2025-07-01|1|2025-07-02",
			"BACKFILL|pending|1|2025-12-15|2025-12-15|2026-01-01|2026-01-01|0|2026-01-02")
		qz2WantLines(t, "org-backfill", qz2Psql(t, psql, uri, "org-backfill", form), qz2Transcript(qz2UnitsPass(), backfill, qz2RunsPass()))
	})
	t.Run("org mutant FIRST-RUN", func(t *testing.T) {
		form := qz2Mutate(t, qz2Form, "starts_with(r.org_id, 'f0f0f0f0')", "starts_with(r.org_id, '')")
		runs := qz2Block(qz2RunsHeader, "FIRST-RUN|3|2025-10-01 00:00:00")
		qz2WantLines(t, "org-runs", qz2Psql(t, psql, uri, "org-runs", form), qz2Transcript(qz2UnitsPass(), qz2BackfillPass(), runs))
	})

	t.Run("rule mutant since_before_floor < to <=", func(t *testing.T) {
		form := qz2Mutate(t, qz2Form, "u.since_at < TIMESTAMPTZ", "u.since_at <= TIMESTAMPTZ")
		units := qz2Block(qz2UnitsHeader,
			"UNITS|<all>|<all>|6|2025-12-01 06:00:01|2025-10-15 00:00:00|1|3|2|2025-12-01 06:00:01",
			"UNITS|commit-stats|incremental|1|2025-12-01 06:00:02||1|0|0|",
			"UNITS|commits|backfill|1|2026-01-05 00:00:02|2025-10-15 00:00:00|0|1|0|2026-01-05 00:00:02",
			"UNITS|commits|incremental|2|2025-12-01 06:00:01|2025-11-01 00:00:00|0|1|1|2025-12-01 06:00:01",
			"UNITS|incidents|incremental|1|2025-12-01 06:00:05|2025-11-20 00:00:00|0|0|0|",
			"UNITS|prs|full_resync|1|2025-12-01 06:00:03|2025-11-01 00:00:00|0|1|1|2025-12-01 06:00:03")
		qz2WantLines(t, "rule-lt", qz2Psql(t, psql, uri, "rule-lt", form), qz2Transcript(units, qz2BackfillPass(), qz2RunsPass()))
	})
	t.Run("rule mutant dataset list without commits", func(t *testing.T) {
		form := qz2Mutate(t, qz2Form, "('commits', 'commit-stats',", "('commit-stats',")
		units := qz2Block(qz2UnitsHeader,
			"UNITS|<all>|<all>|3|2025-12-01 06:00:02|2025-11-01 00:00:00|1|0|1|2025-12-01 06:00:03",
			"UNITS|commit-stats|incremental|1|2025-12-01 06:00:02||1|0|0|",
			"UNITS|incidents|incremental|1|2025-12-01 06:00:05|2025-11-20 00:00:00|0|0|0|",
			"UNITS|prs|full_resync|1|2025-12-01 06:00:03|2025-11-01 00:00:00|0|0|1|2025-12-01 06:00:03")
		qz2WantLines(t, "rule-list-commits", qz2Psql(t, psql, uri, "rule-list-commits", form), qz2Transcript(units, qz2BackfillPass(), qz2RunsPass()))
	})
	t.Run("rule mutant dataset list without incidents", func(t *testing.T) {
		form := qz2Mutate(t, qz2Form, ", 'incidents')", ")")
		units := qz2Block(qz2UnitsHeader,
			"UNITS|<all>|<all>|5|2025-12-01 06:00:01|2025-10-15 00:00:00|1|1|2|2025-12-01 06:00:01",
			"UNITS|commit-stats|incremental|1|2025-12-01 06:00:02||1|0|0|",
			"UNITS|commits|backfill|1|2026-01-05 00:00:02|2025-10-15 00:00:00|0|1|0|2026-01-05 00:00:02",
			"UNITS|commits|incremental|2|2025-12-01 06:00:01|2025-11-01 00:00:00|0|0|1|2025-12-01 06:00:01",
			"UNITS|prs|full_resync|1|2025-12-01 06:00:03|2025-11-01 00:00:00|0|0|1|2025-12-01 06:00:03")
		qz2WantLines(t, "rule-list-incidents", qz2Psql(t, psql, uri, "rule-list-incidents", form), qz2Transcript(units, qz2BackfillPass(), qz2RunsPass()))
	})

	t.Run("G0 fires on a missing table", func(t *testing.T) {
		form := qz2Mutate(t, qz2Form, "('organizations', 'sync_run_units', 'sync_runs', 'backfill_jobs')",
			"('organizations', 'sync_run_units', 'sync_runs', 'backfill_jobz')")
		qz2WantStop(t, "fire-table", qz2Psql(t, psql, uri, "fire-table", form), "tables=3", "backfill=-1/-1")
	})
	for _, limit := range []struct{ name, literal string }{
		{"organizations rows", "<= 40000 "},
		{"organizations bytes", "<= 67108864\n"},
		{"units rows", "<= 6000000 "},
		{"units bytes", "<= 1610612736\n"},
		{"runs rows", "<= 3000000 "},
		{"runs bytes", "<= 536870912\n"},
		{"backfill rows", "<= 250000 "},
		{"backfill bytes", "<= 134217728\n"},
	} {
		t.Run("G0 fires on cap "+limit.name, func(t *testing.T) {
			lowered := strings.Replace(limit.literal, strings.TrimSpace(strings.TrimPrefix(limit.literal, "<= ")), "0", 1)
			form := qz2Mutate(t, qz2Form, limit.literal, lowered)
			name := "fire-cap-" + strings.ReplaceAll(limit.name, " ", "-")
			qz2WantStop(t, name, qz2Psql(t, psql, uri, name, form), "orgs=1 tables=4")
		})
	}
	t.Run("G0 fires on orgs 2", func(t *testing.T) {
		if _, err := conn.Exec(ctx, `INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
			VALUES ($1, 'qz2-f0-twin', 'QZ2 F0 twin', 'community', 'stripe', true, now(), now())`, qz2OrgTwin); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if _, err := conn.Exec(ctx, "DELETE FROM organizations WHERE id = $1", qz2OrgTwin); err != nil {
				t.Errorf("remove the twin org: %v", err)
			}
		}()
		qz2WantStop(t, "fire-orgs", qz2Psql(t, psql, uri, "fire-orgs", qz2Form), "orgs=2")
	})

	t.Run("write mutant fails read-only", func(t *testing.T) {
		form := qz2Mutate(t, qz2Form, "AS rel) AS g;\n",
			"AS rel) AS g;\nUPDATE public.organizations SET name = name WHERE id = '"+qz2Org+"';\n")
		got := qz2Psql(t, psql, uri, "write", form)
		if got.rc == 0 {
			t.Fatal("write mutant: psql rc=0, want != 0")
		}
		if !strings.Contains(got.stderr, "cannot execute UPDATE in a read-only transaction") {
			t.Fatalf("write mutant: stderr lacks the read-only error: %s", got.stderr)
		}
		want := append([]string{"BEGIN"}, qz2G0Pass()...)
		if !reflect.DeepEqual(got.lines, want) {
			t.Fatalf("write mutant: stdout %q, want %q (G0 then nothing)", got.lines, want)
		}
	})

	var foreign int
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM organizations WHERE id = $1", qz2ForeignOrg).Scan(&foreign); err != nil || foreign != 1 {
		t.Fatalf("foreign org rows = %d (%v), want 1 (the fixture is unchanged by every run)", foreign, err)
	}
}
