//go:build integration

package backfillrun

import (
	"bytes"
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

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

// DO NOT MERGE. This file executes FORM-14190-G0-3.sql in one READ ONLY transaction on a fresh database at the head
// chain. The text below is the .sql with its org prefix replaced by {{ORG_PREFIX}} and its config id by {{CONFIG_ID}},
// nothing else (its sha256 is checked); the cell puts synthetic values in at run time and checks that every
// starts_with of every statement got the prefix and the config statement got the id once.
// With synthetic sync runs it wants: a backfill Linear run open = G0a > 0, a scheduled or manual one = G0b > 0, a run of
// the wave-2 driver's shape only in G0w, and the catalog gate fires when a cap is crossed (pm-guard FORM-TEMPLATE-3,
// PostgreSQL form).
const (
	formExecG03x14190SHA256      = "f5f0e945cbcfe724a1fab3d1bcaa5fc9c553466564520c9e238df347926d5775"
	formExecG03x14190Placeholder = "{{ORG_PREFIX}}"
	formExecG03x14190ConfigSlot  = "{{CONFIG_ID}}"
	formExecG03x14190SQL         = `-- FORM-14190-G0-3 (lane-14190c, 2026-11-23, ops pin a7febdf50002). PostgreSQL prod, READ ONLY, CHAOS-14190.
-- The T0 proof of CARD-14190-BACKFILL-1 P4 as acp ruled it (READ-CARD-14190-1 P2-1, P2-2; CARD-ADDENDUM-14190-2 RL1,
-- RL2; pm Q1 ruling (i), CARD-ADDENDUM-14190-3): the card's config row is in org {{ORG_PREFIX}}, and open sync runs of
-- that org with Linear units are counted by TRIGGER. A backfill one open = STOP; a scheduled
-- or manual one open = WAIT for its end, then run this file once more. Runs of the wave-2 GitHub backfill driver's
-- shape are listed apart (INFO). Replaces FORM-14190-G0-2 (a5de9d047a37). WANT text and work math: FORM-14190-G0-3.md.
-- Shape: pm-guard/FORM-TEMPLATE-3.md (sha12 6b9e73a3c45a), PostgreSQL form (b). Run line, in the same sitting just
-- before the card's command: psql -X -v ON_ERROR_STOP=1 -f FORM-14190-G0-3.sql (on the primary; one transaction).
-- Open run = sync_runs.status NOT IN ('success', 'partial_failed', 'failed') (terminal set at the pin:
-- internal/syncdispatchruntime/native_finalize_sync_run.go:118-120). Open unit = status NOT IN ('success', 'failed')
-- (:115-116). Trigger = sync_runs.triggered_by: 'schedule' | 'manual' | 'backfill' at the pin (FORM-14190-G0-3.md).
-- Output: counts, UTC minutes and 8-char run prefixes only. Org prefix {{ORG_PREFIX}} by starts_with on every FROM with org_id.

BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;

-- G0 catalog cost gate (reads pg_catalog only). WANT cost_gate_ok = 0. Caps (reltuples / heap + TOAST bytes):
-- organizations 100000 / 268435456, sync_configurations 200000 / 536870912, sync_runs 1000000 / 1073741824,
-- sync_run_units 5000000 / 4294967296.
-- A table missing, never analyzed (reltuples -1) or over a cap = the named cast error STOP-G0 (ON_ERROR_STOP ends the file).
WITH bs AS (SELECT current_setting('block_size')::bigint AS block_size),
rel AS (
    SELECT c.relname::text AS relname, c.reltuples::bigint AS reltuples,
           c.relpages::bigint * bs.block_size + COALESCE(t.relpages::bigint, 0) * bs.block_size AS bytes
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    CROSS JOIN bs
    LEFT JOIN pg_catalog.pg_class t ON t.oid = c.reltoastrelid
    WHERE n.nspname = 'public' AND c.relkind = 'r' AND c.relname IN ('organizations', 'sync_configurations', 'sync_runs', 'sync_run_units')
),
caps (relname, max_rows, max_bytes) AS (
    VALUES ('organizations', 100000::bigint, 268435456::bigint),
           ('sync_configurations', 200000::bigint, 536870912::bigint),
           ('sync_runs', 1000000::bigint, 1073741824::bigint),
           ('sync_run_units', 5000000::bigint, 4294967296::bigint)
)
SELECT CASE WHEN count(rel.relname) = 4
             AND bool_and(rel.reltuples >= 0 AND rel.reltuples <= caps.max_rows AND rel.bytes <= caps.max_bytes)
            THEN 0
            ELSE ('STOP-G0 ' || string_agg(caps.relname || '=' || COALESCE(rel.reltuples::text, 'missing') || '/'
                                           || COALESCE(rel.bytes::text, 'missing'), ' ' ORDER BY caps.relname))::int
       END AS cost_gate_ok
FROM caps LEFT JOIN rel ON rel.relname = caps.relname;

-- C0 INFO the sizes the gate read (pg_catalog only), for the work math. WANT 4 rows.
SELECT c.relname, c.reltuples::bigint AS reltuples, c.relpages::bigint AS heap_pages
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relkind = 'r' AND c.relname IN ('organizations', 'sync_configurations', 'sync_runs', 'sync_run_units')
ORDER BY c.relname;

-- S0 ORG-1. WANT orgs = 1.
SELECT count(*) AS orgs
FROM public.organizations o
WHERE starts_with(o.id::text, '{{ORG_PREFIX}}');

-- S1 the card's sync configuration (--config-id {{CONFIG_ID}}; the verb takes the org from this
-- row when --org is absent). WANT config_rows = 1 (the id, in the org prefix) and config_shape_ok = 1 (Linear, active,
-- planner-managed, a parent: no parent_id, no source_id).
SELECT count(*) AS config_rows,
       count(*) FILTER (WHERE lower(c.provider) = 'linear' AND c.is_active AND c.planner_managed
                        AND c.parent_id IS NULL AND c.source_id IS NULL) AS config_shape_ok
FROM public.sync_configurations c
WHERE c.id = '{{CONFIG_ID}}'::uuid AND starts_with(c.org_id, '{{ORG_PREFIX}}');

-- G0a open Linear BACKFILL runs of the org (triggered_by 'backfill' or mode 'backfill'). WANT backfill_open = 0;
-- > 0 = STOP (no T0).
SELECT count(*) AS backfill_open,
       COALESCE(sum(u.units_open), 0)::bigint AS backfill_units_open,
       COALESCE(to_char(max(r.created_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI'), '') AS backfill_last_minute
FROM public.sync_runs r
JOIN LATERAL (
    SELECT count(*) AS units, count(*) FILTER (WHERE x.status NOT IN ('success', 'failed')) AS units_open
    FROM public.sync_run_units x
    WHERE x.sync_run_id = r.id AND starts_with(x.org_id, '{{ORG_PREFIX}}') AND x.provider = 'linear'
) u ON u.units > 0
WHERE starts_with(r.org_id, '{{ORG_PREFIX}}') AND r.status NOT IN ('success', 'partial_failed', 'failed')
  AND (r.triggered_by = 'backfill' OR r.mode = 'backfill');

-- G0b open Linear runs that are not a backfill, by trigger. WANT schedule_open = 0 and manual_open = 0; > 0 = WAIT for
-- the run to end, then run this file once more. other_trigger_open WANT 0; > 0 = STOP (a trigger the pin does not write).
SELECT count(*) FILTER (WHERE r.triggered_by = 'schedule') AS schedule_open,
       count(*) FILTER (WHERE r.triggered_by = 'manual') AS manual_open,
       count(*) FILTER (WHERE r.triggered_by NOT IN ('schedule', 'manual')) AS other_trigger_open,
       COALESCE(sum(u.units_open), 0)::bigint AS units_open,
       COALESCE(to_char(max(r.created_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI'), '') AS last_minute
FROM public.sync_runs r
JOIN LATERAL (
    SELECT count(*) AS units, count(*) FILTER (WHERE x.status NOT IN ('success', 'failed')) AS units_open
    FROM public.sync_run_units x
    WHERE x.sync_run_id = r.id AND starts_with(x.org_id, '{{ORG_PREFIX}}') AND x.provider = 'linear'
) u ON u.units > 0
WHERE starts_with(r.org_id, '{{ORG_PREFIX}}') AND r.status NOT IN ('success', 'partial_failed', 'failed')
  AND r.triggered_by <> 'backfill' AND r.mode <> 'backfill';

-- G0w INFO open runs of the wave-2 driver's shape, apart: mode and triggered_by 'backfill', units provider github with
-- dataset_key cicd. Not a stop (acp: the driver may run). The prefixes match the driver's START prefixes.
SELECT count(*) AS wave2_shape_open,
       COALESCE(sum(u.units_open), 0)::bigint AS wave2_shape_units_open,
       COALESCE(string_agg(left(r.id::text, 8) || ':' || r.status, ' ' ORDER BY r.created_at, r.id), '') AS wave2_shape_runs
FROM public.sync_runs r
JOIN LATERAL (
    SELECT count(*) AS units, count(*) FILTER (WHERE x.status NOT IN ('success', 'failed')) AS units_open
    FROM public.sync_run_units x
    WHERE x.sync_run_id = r.id AND starts_with(x.org_id, '{{ORG_PREFIX}}') AND x.provider = 'github' AND x.dataset_key = 'cicd'
) u ON u.units > 0
WHERE starts_with(r.org_id, '{{ORG_PREFIX}}') AND r.status NOT IN ('success', 'partial_failed', 'failed')
  AND r.mode = 'backfill' AND r.triggered_by = 'backfill';

-- C1 control: terminal runs of the org with Linear units, capped at 10 (early exit). WANT terminal_linear_runs > 0.
SELECT count(*) AS terminal_linear_runs
FROM (
    SELECT 1
    FROM public.sync_runs r
    WHERE starts_with(r.org_id, '{{ORG_PREFIX}}') AND r.status IN ('success', 'partial_failed', 'failed')
      AND EXISTS (SELECT 1 FROM public.sync_run_units x
                  WHERE x.sync_run_id = r.id AND starts_with(x.org_id, '{{ORG_PREFIX}}') AND x.provider = 'linear')
    LIMIT 10
) AS s;

ROLLBACK;
`
)

type formExecG03x14190Piece struct {
	id, text string
}

// formExecG03x14190Pieces splits the FORM as psql runs it: the pieces between ";\n" that hold SQL. A data statement is
// named by its first comment line "-- <id> "; BEGIN and ROLLBACK by their own word.
func formExecG03x14190Pieces(t *testing.T, text string) []formExecG03x14190Piece {
	t.Helper()
	statementID := regexp.MustCompile(`^(S[01]|C[01]|G0[abw]?)$`)
	var pieces []formExecG03x14190Piece
	seen := map[string]bool{}
	for _, raw := range strings.Split(text, ";\n") {
		statement := strings.TrimSpace(raw)
		id, sqlText := "", ""
		for _, line := range strings.Split(statement, "\n") {
			if strings.HasPrefix(line, "--") {
				if fields := strings.Fields(line); id == "" && len(fields) > 1 && statementID.MatchString(fields[1]) {
					id = fields[1]
				}
				continue
			}
			sqlText += line + "\n"
		}
		sqlText = strings.TrimSpace(sqlText)
		if sqlText == "" {
			continue
		}
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
		pieces = append(pieces, formExecG03x14190Piece{id: id, text: statement})
	}
	return pieces
}

// formExecG03x14190SQLLines is the SQL of a piece without its comment lines.
func formExecG03x14190SQLLines(text string) string {
	var out strings.Builder
	for _, line := range strings.Split(text, "\n") {
		if !strings.HasPrefix(line, "--") {
			out.WriteString(line + "\n")
		}
	}
	return out.String()
}

type formExecG03x14190Result struct {
	columns []string
	rows    [][]string
}

func formExecG03x14190Query(ctx context.Context, conn *pgx.Conn, statement string) (formExecG03x14190Result, error) {
	rows, err := conn.Query(ctx, statement)
	if err != nil {
		return formExecG03x14190Result{}, err
	}
	defer rows.Close()
	var result formExecG03x14190Result
	for _, field := range rows.FieldDescriptions() {
		result.columns = append(result.columns, field.Name)
	}
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return formExecG03x14190Result{}, err
		}
		row := make([]string, len(values))
		for index, value := range values {
			row[index] = fmt.Sprint(value)
		}
		result.rows = append(result.rows, row)
	}
	return result, rows.Err()
}

// formExecG03x14190Run runs the pieces in order in one transaction, as pm runs the file. replaceID/from/to change one
// statement in a test copy. It returns the results by id, or the first error (then the transaction is rolled back, as
// ON_ERROR_STOP ends the file).
func formExecG03x14190Run(ctx context.Context, t *testing.T, conn *pgx.Conn, pieces []formExecG03x14190Piece, replaceID, from, to string) (map[string]formExecG03x14190Result, error) {
	t.Helper()
	results := map[string]formExecG03x14190Result{}
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
		result, err := formExecG03x14190Query(ctx, conn, text)
		if err != nil {
			if _, rollbackErr := conn.Exec(ctx, "ROLLBACK"); rollbackErr != nil {
				t.Fatalf("ROLLBACK after %s: %v", piece.id, rollbackErr)
			}
			return results, fmt.Errorf("%s: %w", piece.id, err)
		}
		for index, row := range result.rows {
			t.Logf("FORM-14190-G0-3 %s row %d %v %v", piece.id, index, result.columns, row)
		}
		results[piece.id] = result
	}
	return results, nil
}

func formExecG03x14190Cell(t *testing.T, label string, results map[string]formExecG03x14190Result, id, column string) string {
	t.Helper()
	result, ok := results[id]
	if !ok || len(result.rows) != 1 {
		t.Fatalf("%s %s: want 1 row, got %v", label, id, result.rows)
	}
	for index, name := range result.columns {
		if name == column {
			return result.rows[0][index]
		}
	}
	t.Fatalf("%s %s: no column %s in %v", label, id, column, result.columns)
	return ""
}

func formExecG03x14190Want(t *testing.T, label string, results map[string]formExecG03x14190Result, id string, want map[string]string) {
	t.Helper()
	minute := regexp.MustCompile(`^\d{4}-\d\d-\d\d \d\d:\d\d$`)
	for column, value := range want {
		got := formExecG03x14190Cell(t, label, results, id, column)
		if value == "<minute>" {
			if !minute.MatchString(got) {
				t.Errorf("%s %s %s = %q, want a UTC minute", label, id, column, got)
			}
			continue
		}
		if got != value {
			t.Errorf("%s %s %s = %q, want %q", label, id, column, got, value)
		}
	}
}

// TestFormExecG02x14190CountsOpenLinearRunsByTrigger. Synthetic org prefix f1419aaa. Phase 1: Linear runs that ended
// (success by schedule, partial_failed by a manual Sync now), an open wave-2-shaped GitHub backfill (cicd), an open
// GitHub incremental run; another org (e1419aaa) holds an open Linear backfill. Then, one at a time: an open scheduled
// Linear run (WAIT), an open manual one (WAIT), open Linear backfills (STOP: one by trigger, one by mode only), an open
// Linear run with a trigger the pin does not write (STOP).
func TestFormExecG02x14190CountsOpenLinearRunsByTrigger(t *testing.T) {
	ctx := context.Background()
	instance, admin := startDatabase(t)
	uri := freshDatabase(t, instance, admin)
	conn, err := pgx.Connect(ctx, uri)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)

	sum := sha256.Sum256([]byte(formExecG03x14190SQL))
	if got := hex.EncodeToString(sum[:]); got != formExecG03x14190SHA256 {
		t.Fatalf("sha256 of the embedded text = %s, want %s (FORM-14190-G0-3.sql with the org prefix as %s, the config id as %s)", got, formExecG03x14190SHA256, formExecG03x14190Placeholder, formExecG03x14190ConfigSlot)
	}
	const prefix = "f1419aaa"
	configID, otherConfigID := "f1419aaa-1419-4000-8000-0000000000c1", "f1419aaa-1419-4000-8000-0000000000c2"
	pieces := formExecG03x14190Pieces(t, formExecG03x14190SQL)
	var ids []string
	for index, piece := range pieces {
		ids = append(ids, piece.id)
		sqlLines := formExecG03x14190SQLLines(piece.text)
		placeholders, filters := strings.Count(sqlLines, formExecG03x14190Placeholder), strings.Count(sqlLines, "starts_with(")
		if placeholders != filters {
			t.Errorf("%s: %d org prefix(es) for %d starts_with filter(s)", piece.id, placeholders, filters)
		}
		switch piece.id {
		case "S0", "S1", "G0a", "G0b", "G0w", "C1":
			if placeholders == 0 {
				t.Errorf("%s: no org prefix", piece.id)
			}
		default:
			if placeholders != 0 {
				t.Errorf("%s: an org prefix in a statement that reads no org table", piece.id)
			}
		}
		wantSlots := 0
		if piece.id == "S1" {
			wantSlots = 1
		}
		if got := strings.Count(sqlLines, formExecG03x14190ConfigSlot); got != wantSlots {
			t.Errorf("%s: %d config id slot(s), want %d", piece.id, got, wantSlots)
		}
		pieces[index].text = strings.ReplaceAll(strings.ReplaceAll(piece.text, formExecG03x14190Placeholder, prefix), formExecG03x14190ConfigSlot, configID)
		if strings.Contains(pieces[index].text, "{{") {
			t.Errorf("%s: a placeholder is left", piece.id)
		}
		upper := strings.ToUpper(formExecG03x14190SQLLines(piece.text))
		for _, banned := range []string{"SET ", "INSERT ", "UPDATE ", "DELETE ", "EXPLAIN", "COPY "} {
			if strings.Contains(upper, banned) {
				t.Errorf("%s holds %q", piece.id, banned)
			}
		}
	}
	if got := strings.Join(ids, ","); got != "BEGIN,G0,C0,S0,S1,G0a,G0b,G0w,C1,ROLLBACK" {
		t.Fatalf("pieces = %s", got)
	}

	const org, other = prefix + "-1419-4000-8000-0000000000e1", "e1419aaa-1419-4000-8000-0000000000e2"
	id := func(kind, n int) string { return fmt.Sprintf("%08x-1419-4000-8000-%012d", kind, n) }
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
	for _, integration := range []struct {
		n             int
		orgID, vendor string
	}{{1, org, "linear"}, {2, org, "github"}, {3, other, "linear"}} {
		exec(`INSERT INTO integrations (id, org_id, provider, name, config, is_active, created_at, updated_at)
VALUES ($1::uuid, $2, $3, 'fx', '{}'::json, true, now(), now())`, id(0x1a, integration.n), integration.orgID, integration.vendor)
		exec(`INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata, is_enabled, discovered_at, last_seen_at)
VALUES ($1::uuid, $2, $3::uuid, $4, 'team', 'FX', 'FX', 'FX', '{}'::json, true, now(), now())`, id(0x5a, integration.n), integration.orgID, id(0x1a, integration.n), integration.vendor)
	}
	// The card's config (a planner-managed Linear parent of the org) and a config of the other org (S1 must not take it).
	for _, config := range []struct {
		id, orgID   string
		integration int
	}{{configID, org, 1}, {otherConfigID, other, 3}} {
		exec(`INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets, sync_options, is_active, planner_managed, integration_id, created_at, updated_at)
VALUES ($1::uuid, $2, 'linear-fx', 'linear', '["work-items"]'::json, '{}'::json, true, true, $3::uuid, now(), now())`, config.id, config.orgID, id(0x1a, config.integration))
	}
	run := func(n, integration int, orgID, triggeredBy, mode, status string, units ...[3]string) string {
		runID := id(0x5c00+n, n)
		exec(`INSERT INTO sync_runs (id, org_id, integration_id, triggered_by, mode, status, total_units, completed_units, failed_units, created_at)
VALUES ($1::uuid, $2, $3::uuid, $4, $5, $6, $7, 0, 0, now())`, runID, orgID, id(0x1a, integration), triggeredBy, mode, status, len(units))
		for index, unit := range units {
			exec(`INSERT INTO sync_run_units (id, org_id, sync_run_id, integration_id, source_id, provider, dataset_key, cost_class, mode, status, attempts, created_at, updated_at)
VALUES ($1::uuid, $2, $3::uuid, $4::uuid, $5::uuid, $6, $7, 'medium', $8, $9, 0, now(), now())`,
				id(0x0e, n*100+index), orgID, runID, id(0x1a, integration), id(0x5a, integration), unit[0], unit[1], mode, unit[2])
		}
		return runID[:8]
	}
	linear := func(status string, count int) [][3]string {
		units := make([][3]string, count)
		for index := range units {
			units[index] = [3]string{"linear", "work-items", status}
		}
		return units
	}
	run(1, 1, org, "schedule", "incremental", "success", linear("success", 2)...)
	run(2, 1, org, "manual", "incremental", "partial_failed", [3]string{"linear", "work-items", "success"}, [3]string{"linear", "work-items", "failed"})
	wave2 := run(3, 2, org, "backfill", "backfill", "running", [3]string{"github", "cicd", "running"}, [3]string{"github", "cicd", "success"})
	run(4, 2, org, "schedule", "incremental", "dispatching", [3]string{"github", "prs", "planned"})
	run(5, 3, other, "backfill", "backfill", "running", linear("running", 1)...)
	exec(`ANALYZE organizations`)
	exec(`ANALYZE sync_configurations`)
	exec(`ANALYZE sync_runs`)
	exec(`ANALYZE sync_run_units`)

	read := func(label string) map[string]formExecG03x14190Result {
		t.Helper()
		results, err := formExecG03x14190Run(ctx, t, conn, pieces, "", "", "")
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		return results
	}
	// Phase 1: no open Linear run of the org. Every WANT holds; the wave-2-shaped run is only in G0w.
	results := read("phase1")
	formExecG03x14190Want(t, "phase1", results, "G0", map[string]string{"cost_gate_ok": "0"})
	if got := len(results["C0"].rows); got != 4 {
		t.Errorf("phase1 C0: %d rows, want 4", got)
	}
	formExecG03x14190Want(t, "phase1", results, "S0", map[string]string{"orgs": "1"})
	formExecG03x14190Want(t, "phase1", results, "S1", map[string]string{"config_rows": "1", "config_shape_ok": "1"})
	formExecG03x14190Want(t, "phase1", results, "G0a", map[string]string{"backfill_open": "0", "backfill_units_open": "0", "backfill_last_minute": ""})
	formExecG03x14190Want(t, "phase1", results, "G0b", map[string]string{"schedule_open": "0", "manual_open": "0", "other_trigger_open": "0", "units_open": "0", "last_minute": ""})
	formExecG03x14190Want(t, "phase1", results, "G0w", map[string]string{"wave2_shape_open": "1", "wave2_shape_units_open": "1", "wave2_shape_runs": wave2 + ":running"})
	formExecG03x14190Want(t, "phase1", results, "C1", map[string]string{"terminal_linear_runs": "2"})

	// Mutation cells on the phase 1 rows: each must change its value.
	for _, mutant := range []struct {
		name, id, from, to, column, killed string
	}{
		// partial_failed is terminal: without it the manual partial_failed Linear run reads as open.
		{"terminal-set-without-partial-failed", "G0b", "r.status NOT IN ('success', 'partial_failed', 'failed')", "r.status NOT IN ('success', 'failed')", "manual_open", "0"},
		// The provider filter decides G0a: on github it takes the wave-2-shaped backfill.
		{"g0a-provider-github", "G0a", "x.provider = 'linear'", "x.provider = 'github'", "backfill_open", "0"},
		// The org filter of the runs is read: on the other org's prefix G0a takes its open Linear backfill.
		{"g0a-other-org", "G0a", "WHERE starts_with(r.org_id, '" + prefix + "')", "WHERE starts_with(r.org_id, 'e1419aaa')", "backfill_open", "0"},
		// S1 reads the org prefix of the config row: the other org's config is not taken.
		{"s1-other-org-config", "S1", "'" + configID + "'::uuid", "'" + otherConfigID + "'::uuid", "config_rows", "1"},
		// S1 reads the shape: a source-pinned child is not the card's config.
		{"s1-shape-planner-managed", "S1", "c.planner_managed", "NOT c.planner_managed", "config_shape_ok", "1"},
		// The control can fail: with no terminal status it finds nothing.
		{"control-no-terminal", "C1", "r.status IN ('success', 'partial_failed', 'failed')", "r.status IN ('none')", "terminal_linear_runs", "2"},
	} {
		text := mutant.to
		if mutant.name == "g0a-other-org" {
			// The other org's units carry its own prefix too: swap both filters of the statement.
			for index, piece := range pieces {
				if piece.id == "G0a" {
					mutated := pieces[index]
					mutated.text = strings.ReplaceAll(piece.text, prefix, "e1419aaa")
					copied := append([]formExecG03x14190Piece(nil), pieces...)
					copied[index] = mutated
					results, err := formExecG03x14190Run(ctx, t, conn, copied, "", "", "")
					if err != nil {
						t.Fatalf("MUTANT %s: %v", mutant.name, err)
					}
					got := formExecG03x14190Cell(t, "MUTANT "+mutant.name, results, mutant.id, mutant.column)
					t.Logf("MUTANT %s: %s %s = %s (unmutated %s)", mutant.name, mutant.id, mutant.column, got, mutant.killed)
					if got == mutant.killed {
						t.Errorf("MUTANT %s SURVIVED", mutant.name)
					}
				}
			}
			continue
		}
		results, err := formExecG03x14190Run(ctx, t, conn, pieces, mutant.id, mutant.from, text)
		if err != nil {
			t.Fatalf("MUTANT %s: %v", mutant.name, err)
		}
		got := formExecG03x14190Cell(t, "MUTANT "+mutant.name, results, mutant.id, mutant.column)
		t.Logf("MUTANT %s: %s %s = %s (unmutated %s)", mutant.name, mutant.id, mutant.column, got, mutant.killed)
		if got == mutant.killed {
			t.Errorf("MUTANT %s SURVIVED", mutant.name)
		}
	}

	// The gate fires: each cap literal lowered to 0 in a test copy. WANT the named cast error STOP-G0 (SQLSTATE 22P02),
	// raised by G0 before any later statement runs.
	for _, literal := range []string{"100000::bigint", "268435456::bigint", "200000::bigint", "536870912::bigint", "1000000::bigint", "1073741824::bigint", "5000000::bigint", "4294967296::bigint"} {
		results, err := formExecG03x14190Run(ctx, t, conn, pieces, "G0", literal, "0::bigint")
		var pgErr *pgconn.PgError
		if err == nil || !errors.As(err, &pgErr) || pgErr.Code != "22P02" || !strings.Contains(pgErr.Message, "STOP-G0") {
			t.Errorf("GATE %s lowered to 0: err = %v, want 22P02 STOP-G0", literal, err)
			continue
		}
		if len(results) != 0 {
			t.Errorf("GATE %s: statements ran before the gate: %v", literal, results)
		}
		t.Logf("GATE %s lowered to 0: fired: %s", literal, pgErr.Message)
	}

	// Phase 2: an open SCHEDULED Linear run = WAIT (G0b), not a STOP (G0a).
	run(6, 1, org, "schedule", "incremental", "running", [3]string{"linear", "work-items", "running"}, [3]string{"linear", "work-items", "success"})
	results = read("phase2")
	formExecG03x14190Want(t, "phase2", results, "G0a", map[string]string{"backfill_open": "0"})
	formExecG03x14190Want(t, "phase2", results, "G0b", map[string]string{"schedule_open": "1", "manual_open": "0", "other_trigger_open": "0", "units_open": "1", "last_minute": "<minute>"})

	// Phase 3: an open MANUAL Linear run (Sync now or a webhook request) = WAIT too.
	run(7, 1, org, "manual", "incremental", "dispatching", linear("planned", 1)...)
	results = read("phase3")
	formExecG03x14190Want(t, "phase3", results, "G0a", map[string]string{"backfill_open": "0"})
	formExecG03x14190Want(t, "phase3", results, "G0b", map[string]string{"schedule_open": "1", "manual_open": "1", "units_open": "2"})

	// Phase 4: open Linear BACKFILLS = STOP: the card's own run (13 planned units) and a manual run in backfill mode.
	run(8, 1, org, "backfill", "backfill", "dispatching", linear("planned", 13)...)
	run(9, 1, org, "manual", "backfill", "planned", linear("planned", 1)...)
	results = read("phase4")
	formExecG03x14190Want(t, "phase4", results, "G0a", map[string]string{"backfill_open": "2", "backfill_units_open": "14", "backfill_last_minute": "<minute>"})
	formExecG03x14190Want(t, "phase4", results, "G0b", map[string]string{"schedule_open": "1", "manual_open": "1", "other_trigger_open": "0"})
	formExecG03x14190Want(t, "phase4", results, "G0w", map[string]string{"wave2_shape_open": "1"})
	// The mode arm is read: on the trigger alone the manual backfill-mode run drops out of G0a.
	results, err = formExecG03x14190Run(ctx, t, conn, pieces, "G0a", "(r.triggered_by = 'backfill' OR r.mode = 'backfill')", "r.triggered_by = 'backfill'")
	if err != nil {
		t.Fatalf("MUTANT g0a-without-mode-arm: %v", err)
	}
	if got := formExecG03x14190Cell(t, "MUTANT g0a-without-mode-arm", results, "G0a", "backfill_open"); got == "2" {
		t.Errorf("MUTANT g0a-without-mode-arm SURVIVED: backfill_open %s", got)
	}

	// Phase 5: an open Linear run with a trigger the pin does not write = STOP (other_trigger_open).
	run(10, 1, org, "api", "incremental", "running", linear("running", 1)...)
	results = read("phase5")
	formExecG03x14190Want(t, "phase5", results, "G0b", map[string]string{"schedule_open": "1", "manual_open": "1", "other_trigger_open": "1"})
}

// TestCardArgvChaos14190UnknownConfigWritesNothing: the card's argv shape (no --org) with a config id that has no row
// fails with exit 1 and "Sync configuration not found" (command.go:219-225, LoadConfig nil on no row) and writes
// nothing: no occurrence, no trigger, no scheduled job (the writes are in Mint, after LoadConfig and Validate, in the
// same transaction, which the failure rolls back).
func TestCardArgvChaos14190UnknownConfigWritesNothing(t *testing.T) {
	ctx := context.Background()
	instance, admin := startDatabase(t)
	uri := freshDatabase(t, instance, admin)
	conn, err := pgx.Connect(ctx, uri)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	count := func() string {
		t.Helper()
		var occurrences, triggers, jobs int
		if err := conn.QueryRow(ctx, `SELECT (SELECT count(*) FROM scheduled_sync_occurrences), (SELECT count(*) FROM sync_manual_triggers),
(SELECT count(*) FROM scheduled_jobs)`).Scan(&occurrences, &triggers, &jobs); err != nil {
			t.Fatal(err)
		}
		return fmt.Sprintf("occurrences=%d triggers=%d jobs=%d", occurrences, triggers, jobs)
	}
	before := count()
	env := map[string]string{"MIGRATION_DATABASE_URI": uri}
	lookup := func(key string) (string, bool) { value, ok := env[key]; return value, ok }
	var stdout, stderr bytes.Buffer
	code := runVerb(ctx, cli.Env{Args: []string{"--config-id", "f1419aaa-1419-4000-8000-00000000dead", "--dataset-keys", "work-items", "--since", "2026-06-01"},
		Lookup: lookup, Stdout: &stdout, Stderr: &stderr})
	t.Logf("exit %d\nstdout: %s\nstderr: %s", code, stdout.String(), stderr.String())
	if code != cli.ExitFailure || stdout.Len() != 0 || !strings.Contains(stderr.String(), "Sync configuration not found: f1419aaa-1419-4000-8000-00000000dead") {
		t.Fatalf("unknown config: exit %d, stdout %q, want exit %d and the not-found line", code, stdout.String(), cli.ExitFailure)
	}
	if after := count(); after != before {
		t.Fatalf("an unknown config wrote rows: before %s, after %s", before, after)
	}
}
