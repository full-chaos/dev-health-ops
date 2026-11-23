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

// EXEC PROOF ONLY (never merged): FORM-13863-SELECT-1 on Postgres at the migrated schema. The SQL is the FORM file with
// the one org prefix literal replaced by the fixture prefix 'f0f0f0f0' (testdata). It runs the PASS path on a fixture
// of the 13863 shape (8 running + finalize-pending runs, each with one 'failed' partition with no reason; one run with
// only a recent partition; one succeeded run), fires each G0 STOP once, and shows that each control can fail.
func TestFormSelect13863Executes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	admin := startStaleFinalizeAdmin(t, ctx, "form13863")
	raw, err := os.ReadFile("testdata/form13863/FORM-13863-SELECT-1.sql")
	if err != nil {
		t.Fatal(err)
	}
	form := string(raw)
	old := time.Now().UTC().Add(-72 * time.Hour)
	const org = "f0f0f0f0-0000-4000-8000-000000000001"
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := admin.Exec(ctx, query, args...); err != nil {
			t.Fatal(err)
		}
	}
	exec(`INSERT INTO public.organizations (id, slug, name) VALUES ($1::uuid, 'form-13863', 'form-13863')`, org)
	run := func(id, status, finalization string, day int, at time.Time) {
		t.Helper()
		exec(`INSERT INTO public.daily_metrics_runs (id, org_id, target_day, generation, status, finalization_status, created_at, updated_at)
VALUES ($1::uuid, $2::uuid, DATE '2026-09-17' + $3::int, 'form13863:' || $1, $4, $5, $6, $6)`, id, org, day, status, finalization, at)
	}
	partition := func(runID string, ordinal int, status string, at time.Time) {
		t.Helper()
		exec(`INSERT INTO public.daily_metrics_partitions (id, run_id, ordinal, repo_ids, status, attempt_count, completed_at, created_at, updated_at)
VALUES (gen_random_uuid(), $1::uuid, $2, '[]'::jsonb, $3, 1, $4, $4, $4)`, runID, ordinal, status, at)
	}
	for i := 0; i < 8; i++ {
		id := "f0f0f0f0-0000-4000-8000-0000000000a" + string(rune('0'+i))
		run(id, "running", "pending", i, old)
		partition(id, 0, "succeeded", old)
		partition(id, 1, "failed", old)
	}
	const recentOnly = "f0f0f0f0-0000-4000-8000-0000000000b1"
	run(recentOnly, "running", "pending", 20, old)
	partition(recentOnly, 0, "succeeded", time.Now().UTC().Add(-time.Hour))
	run("f0f0f0f0-0000-4000-8000-0000000000c1", "succeeded", "succeeded", 21, old)
	exec("ANALYZE public.daily_metrics_runs, public.daily_metrics_partitions")

	results := runForm(t, ctx, admin, form, "")
	if got := results[1]; len(got) != 1 || got[0][0] != "0" {
		t.Fatalf("G0 = %v, want cost_gate_ok 0", got)
	}
	if got := results[2]; len(got) != 1 || got[0][0] == "0" {
		t.Fatalf("S1 = %v, want running_runs > 0", got)
	}
	s2 := results[3]
	statusArm, recent := 0, 0
	for _, row := range s2 {
		// run8, target_day, app_org, status_arm, recent_only, no_partition, in_base, partitions, failed, failed_permanent
		if row[3] == "t" {
			statusArm++
			if row[2] != "t" || row[6] != "f" || row[9] != "0" || row[8] != "1" {
				t.Fatalf("S2 status-arm row %v, want app_org t, in_base f, failed 1, failed_permanent 0", row)
			}
		}
		if row[4] == "t" {
			recent++
		}
	}
	if len(s2) != 9 || statusArm != 8 || recent != 1 {
		t.Fatalf("S2 = %v, want 9 rows: 8 status-arm, 1 recent-only", s2)
	}
	if got := results[4]; len(got) != 1 || got[0][0] != "0" || got[0][1] != "8" {
		t.Fatalf("S3 = %v, want stage_base 0, status_arm_runs 8", got)
	}

	// Each control can fail.
	if got := runForm(t, ctx, admin, strings.Replace(form, "WHERE run.status = 'running';", "WHERE run.status = 'runningx';", 1), "")[2]; got[0][0] != "0" {
		t.Fatalf("S1 with a wrong literal = %v, want 0 (the control can fail)", got)
	}
	const base = "f0f0f0f0-0000-4000-8000-0000000000d1"
	run(base, "running", "pending", 22, old)
	partition(base, 0, "succeeded", old)
	if got := runForm(t, ctx, admin, form, "")[4]; got[0][0] != "1" {
		t.Fatalf("S3 with one base run = %v, want stage_base 1 (the WANT can fail)", got)
	}
	exec("DELETE FROM public.daily_metrics_partitions WHERE run_id = $1::uuid", base)
	exec("DELETE FROM public.daily_metrics_runs WHERE id = $1::uuid", base)

	// Each G0 STOP fires once.
	runForm(t, ctx, admin, strings.Replace(form, "runs <= 200000", "runs <= 1", 1), "STOP-G0")
	runForm(t, ctx, admin, strings.Replace(form, "partitions <= 500000", "partitions <= 1", 1), "STOP-G0")
	runForm(t, ctx, admin, strings.ReplaceAll(form, "'f0f0f0f0'", "'e0e0e0e0'"), "STOP-G0 orgs=0")
	exec("DROP INDEX public.ix_daily_metrics_partition_run_status")
	runForm(t, ctx, admin, form, "index_ok=0")
}

// runForm sends the FORM in one simple-query call, as psql -f does. With wantErr set it wants the call to fail with
// that text; else it returns each statement's rows as text.
func runForm(t *testing.T, ctx context.Context, admin *pgxpool.Pool, form, wantErr string) [][][]string {
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
