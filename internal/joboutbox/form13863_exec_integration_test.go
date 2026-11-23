//go:build integration

package joboutbox

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// EXEC PROOF ONLY (never merged): FORM-13863-SELECT-2 on Postgres at the migrated schema. The SQL is the FORM file with
// the one org prefix literal replaced by the fixture prefix 'f0f0f0f0' (testdata; its sha256 is pinned below). It runs the
// FORM's WANT on the 13863 shape (8 app-org 'running' + finalize-pending runs in 2026-09-17..2026-11-14, each with one
// 'failed' partition): PASS; the same fixture plus one recent-only run: STOP (the case the hold guards); fires every G0
// STOP once (unanalyzed, tables, org, index, rows and bytes caps) and shows each control failing.
func TestFormSelect13863Executes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	admin := startStaleFinalizeAdmin(t, ctx, "form13863")
	raw, err := os.ReadFile("testdata/form13863/FORM-13863-SELECT-2.sql")
	if err != nil {
		t.Fatal(err)
	}
	if sum := sha256.Sum256(raw); hex.EncodeToString(sum[:]) != "a1a2619656445376fa8e4ee0928b5d531b042e7cfb09791405a8ba01989abdab" {
		t.Fatalf("testdata sha256 %x, want the FORM-13863-SELECT-2 copy this proof was written for", sum)
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
	// Distinct 8-char prefixes, as random prod ids have.
	stuck := []string{
		"1a000001-0000-4000-8000-000000000001", "2b000002-0000-4000-8000-000000000002", "3c000003-0000-4000-8000-000000000003",
		"4d000004-0000-4000-8000-000000000004", "5e000005-0000-4000-8000-000000000005", "6f000006-0000-4000-8000-000000000006",
		"7a000007-0000-4000-8000-000000000007", "8b000008-0000-4000-8000-000000000008",
	}
	for i, id := range stuck {
		run(id, "running", "pending", i*8, old)
		partition(id, 0, "succeeded", old)
		partition(id, 1, "failed", old)
	}
	const recentOnly = "9c000009-0000-4000-8000-000000000009"
	run(recentOnly, "running", "pending", 3, old)
	partition(recentOnly, 0, "succeeded", time.Now().UTC().Add(-time.Hour))
	run("0d00000a-0000-4000-8000-00000000000a", "succeeded", "succeeded", 70, old)

	// Before ANALYZE: reltuples -1 = STOP.
	runForm(t, ctx, admin, form, "unanalyzed=")
	exec("ANALYZE public.daily_metrics_runs, public.daily_metrics_partitions")

	// 8 stuck + 1 recent-only: the FORM's WANT must STOP (the drain redrove one run an hour ago).
	results := runForm(t, ctx, admin, form, "")
	if results[1][0][0] != "0" || results[2][0][0] == "0" {
		t.Fatalf("G0/S1 = %v / %v, want 0 and > 0", results[1], results[2])
	}
	if stops := want13863(results[3], results[4][0]); len(stops) == 0 {
		t.Fatalf("8 stuck + 1 recent-only: WANT passed, want a STOP (S2 %v, S3 %v)", results[3], results[4])
	} else {
		t.Logf("8 stuck + 1 recent-only: STOP %v", stops)
	}
	exec("DELETE FROM public.daily_metrics_partitions WHERE run_id = $1::uuid", recentOnly)
	exec("DELETE FROM public.daily_metrics_runs WHERE id = $1::uuid", recentOnly)

	// The 13863 shape alone: PASS.
	results = runForm(t, ctx, admin, form, "")
	if stops := want13863(results[3], results[4][0]); len(stops) != 0 {
		t.Fatalf("the 13863 shape: STOP %v, want PASS (S2 %v, S3 %v)", stops, results[3], results[4])
	}

	// Each control and WANT can fail.
	if got := runForm(t, ctx, admin, strings.Replace(form, "WHERE run.status = 'running';", "WHERE run.status = 'runningx';", 1), "")[2]; got[0][0] != "0" {
		t.Fatalf("S1 with a wrong literal = %v, want 0", got)
	}
	const base = "ae00000b-0000-4000-8000-00000000000b"
	run(base, "running", "pending", 71, old)
	partition(base, 0, "succeeded", old)
	results = runForm(t, ctx, admin, form, "")
	if results[4][0][2] != "1" || len(want13863(results[3], results[4][0])) == 0 {
		t.Fatalf("one base run: S3 %v, want stage_base 1 and a STOP", results[4])
	}
	exec("DELETE FROM public.daily_metrics_partitions WHERE run_id = $1::uuid", base)
	exec("DELETE FROM public.daily_metrics_runs WHERE id = $1::uuid", base)
	exec(`UPDATE public.daily_metrics_partitions SET status = 'succeeded' WHERE run_id = $1::uuid AND status = 'failed'`, stuck[0])
	results = runForm(t, ctx, admin, form, "")
	if len(want13863(results[3], results[4][0])) == 0 {
		t.Fatalf("7 stuck: WANT passed, want a STOP (S2 %v)", results[3])
	}
	exec(`UPDATE public.daily_metrics_partitions SET status = 'failed' WHERE run_id = $1::uuid AND ordinal = 1`, stuck[0])

	// Each G0 STOP fires once.
	runForm(t, ctx, admin, strings.Replace(form, "runs <= 200000", "runs <= 1", 1), "STOP-G0")
	runForm(t, ctx, admin, strings.Replace(form, "runs_bytes <= 104857600", "runs_bytes <= 0", 1), "STOP-G0")
	runForm(t, ctx, admin, strings.Replace(form, "partitions <= 500000", "partitions <= 1", 1), "STOP-G0")
	runForm(t, ctx, admin, strings.Replace(form, "partitions_bytes <= 209715200", "partitions_bytes <= 0", 1), "STOP-G0")
	runForm(t, ctx, admin, strings.Replace(form, "AND c.relname IN ('daily_metrics_runs', 'daily_metrics_partitions')", "AND c.relname IN ('daily_metrics_runs', 'daily_metrics_partitionsx')", 1), "tables=1")
	runForm(t, ctx, admin, strings.ReplaceAll(form, "'f0f0f0f0'", "'e0e0e0e0'"), "STOP-G0 orgs=0")
	exec("DROP INDEX public.ix_daily_metrics_partition_run_status")
	exec("CREATE INDEX ix_daily_metrics_partition_run_status ON public.daily_metrics_partitions (status, run_id)")
	runForm(t, ctx, admin, form, "index_ok=0")
}

// want13863 is the FORM's WANT: every S2 row status_arm; exactly 8 app-org rows in 2026-09-17..2026-11-14 with failed >= 1;
// S3 stage_base 0, running_pending = the S2 rows, status_arm_runs = running_pending. It returns the misses (STOP lines).
func want13863(s2 [][]string, s3 []string) []string {
	// S2: run8, target_day, app_org, past_horizon, status_arm, recent_only, no_partition, in_base, partitions, failed, failed_permanent
	var stops []string
	card := 0
	for _, row := range s2 {
		if row[4] != "t" {
			stops = append(stops, "row "+row[0]+" "+row[1]+" not status_arm (recent_only "+row[5]+", in_base "+row[7]+")")
		}
		failed, _ := strconv.Atoi(row[9])
		if row[2] == "t" && row[1] >= "2026-09-17" && row[1] <= "2026-11-14" && failed >= 1 {
			card++
		}
	}
	if card != 8 {
		stops = append(stops, "card rows "+strconv.Itoa(card)+", want 8")
	}
	// S3: running_pending, running_pending_past_horizon, stage_base, status_arm_runs
	if s3[2] != "0" {
		stops = append(stops, "stage_base "+s3[2])
	}
	if s3[0] != strconv.Itoa(len(s2)) || s3[3] != s3[0] {
		stops = append(stops, "running_pending "+s3[0]+" vs S2 rows "+strconv.Itoa(len(s2))+", status_arm_runs "+s3[3])
	}
	return stops
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
