//go:build integration

package joboutbox

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// EXEC PROOF ONLY (never merged): FORM-14090-S0-3 and FORM-14511-RECHECK-3 on Postgres at the migrated schema (with
// revision 0167's partial outbox index). The SQL files are the FORMs with the one org prefix literal replaced by the
// fixture prefix 'f0f0f0f0'. Pass paths, each G0 STOP fired once, each control shown failing.
func TestFormsS02AndRecheck2Execute(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	admin := startStaleFinalizeAdmin(t, ctx, "form14511")
	read := func(name string) string {
		raw, err := os.ReadFile("testdata/form14511/" + name)
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	s02, recheck := read("FORM-14090-S0-3.sql"), read("FORM-14511-RECHECK-3.sql")
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := admin.Exec(ctx, query, args...); err != nil {
			t.Fatal(err)
		}
	}
	old := time.Now().UTC().Add(-72 * time.Hour)
	const org = "f0f0f0f0-0000-4000-8000-000000000002"
	exec(`INSERT INTO public.organizations (id, slug, name) VALUES ($1::uuid, 'form-14511', 'form-14511')`, org)
	run := func(id string, day int) {
		t.Helper()
		exec(`INSERT INTO public.daily_metrics_runs (id, org_id, target_day, generation, status, finalization_status, created_at, updated_at)
VALUES ($1::uuid, $2::uuid, DATE '2026-03-01' + $3::int, 'form14511:' || $1, 'running', 'pending', $4, $4)`, id, org, day, old)
	}
	partition := func(runID, status string) {
		t.Helper()
		exec(`INSERT INTO public.daily_metrics_partitions (id, run_id, ordinal, repo_ids, status, attempt_count, completed_at, created_at, updated_at)
VALUES (gen_random_uuid(), $1::uuid, 0, '[]'::jsonb, $2, 1, $3, $3, $3)`, runID, status, old)
	}
	const stuck = "f0f0f0f0-0000-4000-8000-0000000000f1"
	run(stuck, 0)
	partition(stuck, "failed")
	exec(`INSERT INTO public.worker_job_outbox
    (id, dedupe_key, job_kind, contract_version, args, payload_hash, queue, priority, max_attempts, scheduled_at,
     status, attempt_count, next_attempt_at, river_job_id, delivered_at, created_at, updated_at)
VALUES (gen_random_uuid(), 'metrics.daily_finalize:' || $1, 'metrics.daily_finalize', 1,
       json_build_object('contract_version', 1, 'domain', json_build_object('type', 'daily_metrics_run', 'id', $1::text)),
       'sha256:' || repeat('0', 64), 'metrics', 2, 4, $2, 'delivered', 1, $2, 4242, $2, $2, $2)`, stuck, old)
	exec("ANALYZE public.daily_metrics_runs, public.daily_metrics_partitions, public.worker_job_outbox")

	// FORM-14090-S0-2: BEGIN, G0, PREPARE, 3 x EXECUTE, S2, DEALLOCATE, ROLLBACK.
	got := runFormScript(t, ctx, admin, s02, "")
	if got[1][0][0] != "0" || got[3][0][0] != "f" || got[4][0][0] != "f" || got[5][0][0] != "f" {
		t.Fatalf("S0-2 = %v, want G0 0 and statement 0 false three times", got)
	}
	if s2 := got[6][0]; s2[0] == "0" || s2[2] != "0" {
		t.Fatalf("S0-2 S2 = %v, want runs_total > 0 and base_rows 0 (= exists false)", s2)
	}
	const base = "f0f0f0f0-0000-4000-8000-0000000000f2"
	run(base, 1)
	partition(base, "succeeded")
	got = runFormScript(t, ctx, admin, s02, "")
	if got[3][0][0] != "t" || got[6][0][2] != "1" {
		t.Fatalf("S0-2 with one base run = %v, want exists true and base_rows 1 (both can change)", got)
	}
	exec("DELETE FROM public.daily_metrics_partitions WHERE run_id = $1::uuid", base)
	exec("DELETE FROM public.daily_metrics_runs WHERE id = $1::uuid", base)
	runFormScript(t, ctx, admin, strings.Replace(s02, "runs <= 200000", "runs <= 0", 1), "STOP-G0")
	runFormScript(t, ctx, admin, strings.Replace(s02, "partitions <= 500000", "partitions <= 0", 1), "STOP-G0")
	runFormScript(t, ctx, admin, strings.Replace(s02, "runs_bytes <= 104857600", "runs_bytes <= 0", 1), "STOP-G0")
	runFormScript(t, ctx, admin, strings.Replace(s02, "partitions_bytes <= 209715200", "partitions_bytes <= 0", 1), "STOP-G0")
	runFormScript(t, ctx, admin, strings.Replace(s02, "AND c.relname IN ('daily_metrics_runs', 'daily_metrics_partitions')", "AND c.relname IN ('daily_metrics_runs', 'daily_metrics_partitionsx')", 1), "partitions=null")
	runFormScript(t, ctx, admin, strings.ReplaceAll(s02, "'f0f0f0f0'", "'e0e0e0e0'"), "STOP-G0 orgs=0")
	if got := runFormScript(t, ctx, admin, strings.Replace(s02, "count(*) AS runs_total", "count(*) FILTER (WHERE false) AS runs_total", 1), "")[6][0][0]; got != "0" {
		t.Fatalf("S0-2 S2 under a mutation = %v, want runs_total 0 (the control can fail)", got)
	}

	// FORM-14511-RECHECK-2: BEGIN, G0, PREPARE, PREPARE, S1, S2, S3, S4, S5, DEALLOCATE, DEALLOCATE, ROLLBACK.
	got = runFormScript(t, ctx, admin, recheck, "")
	if got[1][0][0] != "0" || got[4][0][0] != "f" || got[5][0][0] == "0" || got[6][0][0] != "f" || got[7][0][0] != "f" || got[8][0][0] == "0" {
		t.Fatalf("RECHECK-2 = %v, want G0 0, S1 false, S2 > 0, S3 false (the newest run's finalize row is delivered and old), S4 false, S5 > 0", got)
	}
	exec(`UPDATE public.worker_job_outbox SET status = 'pending', river_job_id = NULL, delivered_at = NULL WHERE dedupe_key = $1`,
		"metrics.daily_finalize:"+stuck)
	if got := runFormScript(t, ctx, admin, recheck, "")[6][0][0]; got != "t" {
		t.Fatalf("RECHECK-2 S3 with an unrelayed finalize row = %v, want true (the re-check can answer true)", got)
	}
	runFormScript(t, ctx, admin, strings.Replace(recheck, "outbox <= 2000000", "outbox <= 0", 1), "STOP-G0")
	runFormScript(t, ctx, admin, strings.Replace(recheck, "runs_bytes <= 104857600", "runs_bytes <= 0", 1), "STOP-G0")
	runFormScript(t, ctx, admin, strings.Replace(recheck, "partitions_bytes <= 209715200", "partitions_bytes <= 0", 1), "STOP-G0")
	runFormScript(t, ctx, admin, strings.Replace(recheck, "outbox_bytes <= 2147483648", "outbox_bytes <= 0", 1), "STOP-G0")
	runFormScript(t, ctx, admin, strings.ReplaceAll(recheck, "'f0f0f0f0'", "'e0e0e0e0'"), "STOP-G0 orgs=0")
	if got := runFormScript(t, ctx, admin, strings.Replace(recheck, "WHERE outbox.job_kind = 'metrics.daily_finalize';", "WHERE outbox.job_kind = 'metrics.daily_finalizex';", 1), "")[8][0][0]; got != "0" {
		t.Fatalf("RECHECK-2 S5 under a wrong literal = %v, want 0 (the control can fail)", got)
	}
	exec("DROP INDEX public.ix_worker_job_outbox_finalize_domain_id")
	runFormScript(t, ctx, admin, recheck, "indexes_ok=1")
}

// runFormScript sends a FORM in one simple-query call, as psql -f does. With wantErr set it wants the call to fail with
// that text; else it returns each statement's rows as text.
func runFormScript(t *testing.T, ctx context.Context, admin *pgxpool.Pool, form, wantErr string) [][][]string {
	t.Helper()
	conn, err := admin.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	results, err := conn.Conn().PgConn().Exec(ctx, form).ReadAll()
	if err == nil {
		for _, result := range results {
			if result.Err != nil {
				err = result.Err
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
