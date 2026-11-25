//go:build integration

package providersync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// DO NOT MERGE. This file executes a prod read FORM (the pre / post read of one backfill re-walk) byte for byte with every
// prod value and every comment text swapped to a synthetic one (the sha256 below is the sha256 of the synthetic copy), on the
// migrated Postgres schema, the way psql -f runs it (one simple-protocol call, the first error stops the rest). It checks the
// pre shape (no re-walk), the post shape (one finished backfill run of the source on the day), the re-run gate's bounds
// (another day, the day before, another source, a run before the floor, a regular run), each open unit status, a failed unit,
// the G0 cases (byte caps, TOAST, org), read-only and the control. The card's grep lines run on the result rendered as psql
// -A -F"|" prints it (grep from the runner's PATH, so the card's BRE back reference is executed as written).
const formExecRewalkPGSHA256 = "5f33d8759123904de5a60dbfffbd5315b96a4f60316513ac46f25c047b28f28a"

const formExecRewalkPGSQL = ``

const (
	formExecRewalkPGOrg      = "f0f0f0f0-0000-4000-8000-000000000001"
	formExecRewalkPGOrg2     = "f0f0f0f0-0000-4000-8000-000000000009"
	formExecRewalkPGSource   = "0c0c0c0c-0000-4000-8000-000000000001"
	formExecRewalkPGOtherSrc = "0d0d0d0d-0000-4000-8000-000000000002"
)

type formExecRewalkPGResult struct {
	columns []string
	rows    [][]string
}

func formExecRewalkPGRun(ctx context.Context, pool *pgxpool.Pool, text string) ([]formExecRewalkPGResult, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()
	results, err := conn.Conn().PgConn().Exec(ctx, text).ReadAll()
	if err != nil {
		return nil, err
	}
	var out []formExecRewalkPGResult
	for _, result := range results {
		if result.Err != nil {
			return nil, result.Err
		}
		set := formExecRewalkPGResult{}
		for _, field := range result.FieldDescriptions {
			set.columns = append(set.columns, field.Name)
		}
		for _, raw := range result.Rows {
			row := make([]string, len(raw))
			for i, cell := range raw {
				row[i] = string(cell)
			}
			set.rows = append(set.rows, row)
		}
		out = append(out, set)
	}
	return out, nil
}

// formExecRewalkPGCells is one statement's single row by column name: results are BEGIN, G0, stmt0, stmt1, stmt2, stmt3, ROLLBACK.
func formExecRewalkPGCells(t *testing.T, results []formExecRewalkPGResult, index int, label string) map[string]string {
	t.Helper()
	if len(results) != 7 {
		t.Fatalf("%s: %d result sets, want 7 (BEGIN, G0, stmt0, stmt1, stmt2, stmt3, ROLLBACK)", label, len(results))
	}
	set := results[index]
	if len(set.rows) != 1 {
		t.Fatalf("%s: result %d has %d rows, want 1", label, index, len(set.rows))
	}
	cells := map[string]string{}
	for i, name := range set.columns {
		cells[name] = set.rows[0][i]
	}
	t.Logf("%s result %d %v", label, index, cells)
	return cells
}

func formExecRewalkPGWant(t *testing.T, label string, got, want map[string]string) {
	t.Helper()
	for column, value := range want {
		if have, ok := got[column]; !ok || have != value {
			t.Errorf("%s: %s = %q, want %q (row %v)", label, column, have, value, got)
		}
	}
}

// formExecRewalkPGRender renders the result sets the way psql -X -A -F"|" prints a file run: BEGIN, then per statement the
// header, the rows (NULL empty) and "(N row[s])", then ROLLBACK.
func formExecRewalkPGRender(results []formExecRewalkPGResult) string {
	var out strings.Builder
	out.WriteString("BEGIN\n")
	for _, set := range results[1 : len(results)-1] {
		out.WriteString(strings.Join(set.columns, "|") + "\n")
		for _, row := range set.rows {
			out.WriteString(strings.Join(row, "|") + "\n")
		}
		if len(set.rows) == 1 {
			out.WriteString("(1 row)\n")
		} else {
			out.WriteString("(" + strconv.Itoa(len(set.rows)) + " rows)\n")
		}
	}
	out.WriteString("ROLLBACK\n")
	return out.String()
}

// formExecRewalkPGGrep runs `grep <args>` on text and returns its stdout (the count the card's WANT line compares).
func formExecRewalkPGGrep(t *testing.T, text string, args ...string) string {
	t.Helper()
	command := exec.Command("grep", args...)
	command.Stdin = strings.NewReader(text)
	out, _ := command.Output()
	return strings.TrimSpace(string(out))
}

func TestFormExecRewalkPG(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	sum := sha256.Sum256([]byte(formExecRewalkPGSQL))
	if got := hex.EncodeToString(sum[:]); got != formExecRewalkPGSHA256 {
		t.Fatalf("sha256 of the embedded text = %s, want %s (the synthetic copy)", got, formExecRewalkPGSHA256)
	}
	for index, line := range strings.Split(formExecRewalkPGSQL, "\n") {
		if strings.HasPrefix(line, "--") && strings.Contains(line, ";") {
			t.Errorf("comment line %d holds a ';': %s", index+1, line)
		}
	}
	if !strings.HasPrefix(formExecRewalkPGSQL, "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;\n") || !strings.HasSuffix(formExecRewalkPGSQL, "\nROLLBACK;\n") {
		t.Fatalf("the FORM is not one READ ONLY transaction that ends in ROLLBACK")
	}
	for _, banned := range []string{"\nSET ", " SET LOCAL", "INSERT ", "UPDATE ", "DELETE "} {
		if strings.Contains(strings.ToUpper(formExecRewalkPGSQL), strings.ToUpper(banned)) {
			t.Errorf("the FORM holds %q", banned)
		}
	}

	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = instance.Close(context.Background()) }()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createProviderSyncFixture(t, ctx, pool)
	seedProviderSyncFixture(t, ctx, pool)
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	exec(`INSERT INTO public.organizations (id, slug, name) VALUES ($1, 'rewalk-form', 'rewalk-form')`, formExecRewalkPGOrg)
	for _, source := range []struct{ id, external string }{{formExecRewalkPGSource, "fxorg/rewalk-repo"}, {formExecRewalkPGOtherSrc, "fxorg/other-repo"}} {
		exec(`INSERT INTO public.integration_sources
		  (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata, is_enabled, discovered_at, last_seen_at)
		  VALUES ($1, $2, $3, 'github', 'repository', $4, $4, $4, '{}', TRUE, NOW(), NOW())`, source.id, formExecRewalkPGOrg, firstIntegrationID, source.external)
	}
	runN := 0
	// run writes one sync run of the org with one unit of the source over [since, before] (the verb stores --before D as the end of
	// D - 1), created at created.
	run := func(triggeredBy, runStatus, unitStatus, source, created, since, before string) string {
		t.Helper()
		runN++
		id := "f0f0f0f0-0000-4000-8000-0000000001" + strconv.FormatInt(int64(runN)+10, 10)
		exec(`INSERT INTO public.sync_runs (id, org_id, integration_id, triggered_by, mode, status, total_units, completed_units, failed_units, created_at, completed_at)
		  VALUES ($1, $2, $3, $4, 'backfill', $5, 1, 1, 0, $6::timestamptz, $6::timestamptz + interval '20 minutes')`,
			id, formExecRewalkPGOrg, firstIntegrationID, triggeredBy, runStatus, created)
		exec(`INSERT INTO public.sync_run_units (id, org_id, sync_run_id, integration_id, source_id, provider, dataset_key, cost_class, mode,
		  since_at, before_at, status, attempts, created_at, updated_at, processor_flags)
		  VALUES ($1, $2, $3, $4, $5, 'github', 'cicd', 'medium', 'backfill', $6::timestamptz, $7::timestamptz, $8, 1, $9::timestamptz, $9::timestamptz,
		  '{"family_dataset_cicd":true,"family_dataset_tests":true}')`,
			"f0f0f0f0-0000-4000-8000-0000000002"+strconv.FormatInt(int64(runN)+10, 10), formExecRewalkPGOrg, id, firstIntegrationID, source, since, before, unitStatus, created)
		return id
	}
	const day, dayEnd = "2026-03-09 00:00:00+00", "2026-03-09 23:59:59.999999+00"
	// The org's history before the card: a regular run of the source on the day (not a backfill) and an older backfill of the day
	// (created before the floor). Neither is a re-walk of this card.
	run("schedule", "success", "success", formExecRewalkPGSource, "2026-03-16 08:00:00+00", day, dayEnd)
	run("backfill", "success", "success", formExecRewalkPGSource, "2026-03-14 10:00:00+00", day, dayEnd)

	runForm := func(label, text string) []formExecRewalkPGResult {
		t.Helper()
		results, err := formExecRewalkPGRun(ctx, pool, text)
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		return results
	}
	// cardGreps runs the card's P0 (pre) or P1 (post) grep lines on the rendered out, each WANT as the card writes it.
	cardGreps := func(label string, results []formExecRewalkPGResult, post bool) {
		t.Helper()
		out := formExecRewalkPGRender(results)
		lines := [][]string{
			{"1", "-cx", "BEGIN"},
			{"1", "-cx", "cost_gate_ok|orgs|runs_bytes|units_bytes"},
			{"1", "-cx", "0|1|[0-9][0-9]*|[0-9][0-9]*"},
			{"1", "-cx", "open_units|open_backfill_runs_org"},
			{"1", "-cx", "0|[0-9][0-9]*"},
			{"1", "-cx", "runs|runs_open|runs_success|runs_partial_failed|runs_failed|units|units_open|units_success|units_failed|cicd_units|first_since|last_before|last_completed_at"},
			{"1", "-cx", "ROLLBACK"},
			{"0", "-cE", "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"},
		}
		if post {
			lines = append(lines, []string{"1", "-c", `^1|0|1|0|0|\([1-9][0-9]*\)|0|\1|0|\1|`})
		} else {
			lines = append(lines, []string{"1", "-cx", "0|0|0|0|0|0|0|0|0|0|||"})
		}
		for _, line := range lines {
			if got := formExecRewalkPGGrep(t, out, line[1:]...); got != line[0] {
				t.Errorf("%s card grep %v: %s, want %s\n%s", label, line[1:], got, line[0], out)
			}
		}
		t.Logf("GREP-CHECK %s post=%v: %d lines checked", label, post, len(lines))
	}
	zero := map[string]string{"runs": "0", "runs_open": "0", "runs_success": "0", "runs_partial_failed": "0", "runs_failed": "0",
		"units": "0", "units_open": "0", "units_success": "0", "units_failed": "0", "cicd_units": "0", "first_since": "", "last_before": "", "last_completed_at": ""}
	pre := func(label string) {
		t.Helper()
		results := runForm(label, formExecRewalkPGSQL)
		formExecRewalkPGWant(t, label+" G0", formExecRewalkPGCells(t, results, 1, label+" G0"), map[string]string{"cost_gate_ok": "0", "orgs": "1"})
		formExecRewalkPGWant(t, label+" stmt0", formExecRewalkPGCells(t, results, 2, label+" stmt0"), map[string]string{"orgs": "1"})
		formExecRewalkPGWant(t, label+" stmt1", formExecRewalkPGCells(t, results, 3, label+" stmt1"), map[string]string{"open_units": "0", "open_backfill_runs_org": "0"})
		formExecRewalkPGWant(t, label+" stmt2", formExecRewalkPGCells(t, results, 4, label+" stmt2"), zero)
		if n, err := strconv.Atoi(formExecRewalkPGCells(t, results, 5, label+" stmt3")["org_runs"]); err != nil || n < 1 {
			t.Errorf("%s stmt3 org_runs %v, want > 0", label, n)
		}
		cardGreps(label, results, false)
	}

	t.Run("pre: no re-walk yet (a regular run and an older backfill do not count)", func(t *testing.T) { pre(t.Name()) })

	t.Run("gate bounds: the next day, the day before, another source do not count", func(t *testing.T) {
		run("backfill", "success", "success", formExecRewalkPGSource, "2026-03-16 09:00:00+00", "2026-03-10 00:00:00+00", "2026-03-10 23:59:59.999999+00")
		run("backfill", "success", "success", formExecRewalkPGOtherSrc, "2026-03-16 09:00:00+00", day, dayEnd)
		run("backfill", "success", "success", formExecRewalkPGSource, "2026-03-16 09:00:00+00", "2026-03-08 00:00:00+00", "2026-03-08 23:59:59.999999+00")
		pre(t.Name())
	})

	for _, status := range []string{"planned", "dispatching", "running", "retrying"} {
		t.Run("open unit of the source ("+status+"): stmt1 reads it (a walk runs on the source)", func(t *testing.T) {
			id := run("schedule", "running", status, formExecRewalkPGSource, "2026-03-16 09:10:00+00", "2026-03-15 00:00:00+00", "2026-03-15 23:59:59.999999+00")
			results := runForm(t.Name(), formExecRewalkPGSQL)
			formExecRewalkPGWant(t, t.Name(), formExecRewalkPGCells(t, results, 3, "stmt1"), map[string]string{"open_units": "1", "open_backfill_runs_org": "0"})
			exec(`UPDATE public.sync_run_units SET status = 'success' WHERE sync_run_id = $1`, id)
			exec(`UPDATE public.sync_runs SET status = 'success' WHERE id = $1`, id)
		})
	}

	t.Run("a failed re-walk: runs_failed 1, units_failed 1, the post grep misses", func(t *testing.T) {
		id := run("backfill", "failed", "failed", formExecRewalkPGSource, "2026-03-16 09:15:00+00", day, dayEnd)
		results := runForm(t.Name(), formExecRewalkPGSQL)
		formExecRewalkPGWant(t, t.Name(), formExecRewalkPGCells(t, results, 4, "stmt2"), map[string]string{
			"runs": "1", "runs_open": "0", "runs_success": "0", "runs_failed": "1", "units": "1", "units_failed": "1", "units_success": "0"})
		if got := formExecRewalkPGGrep(t, formExecRewalkPGRender(results), "-c", `^1|0|1|0|0|\([1-9][0-9]*\)|0|\1|0|\1|`); got != "0" {
			t.Errorf("the card's post grep reads %s on a failed run, want 0", got)
		}
		exec(`DELETE FROM public.sync_run_units WHERE sync_run_id = $1`, id)
		exec(`DELETE FROM public.sync_runs WHERE id = $1`, id)
	})

	t.Run("the re-walk running: the gate counts it open", func(t *testing.T) {
		id := run("backfill", "running", "dispatching", formExecRewalkPGSource, "2026-03-16 09:20:00+00", day, dayEnd)
		results := runForm(t.Name(), formExecRewalkPGSQL)
		formExecRewalkPGWant(t, t.Name(), formExecRewalkPGCells(t, results, 3, "stmt1"), map[string]string{"open_units": "1", "open_backfill_runs_org": "1"})
		formExecRewalkPGWant(t, t.Name(), formExecRewalkPGCells(t, results, 4, "stmt2"), map[string]string{
			"runs": "1", "runs_open": "1", "runs_success": "0", "units": "1", "units_open": "1", "units_success": "0", "cicd_units": "1"})
		exec(`UPDATE public.sync_run_units SET status = 'success' WHERE sync_run_id = $1`, id)
		exec(`UPDATE public.sync_runs SET status = 'success' WHERE id = $1`, id)
	})

	t.Run("post: one finished re-walk of the source on the day", func(t *testing.T) {
		results := runForm(t.Name(), formExecRewalkPGSQL)
		formExecRewalkPGWant(t, t.Name(), formExecRewalkPGCells(t, results, 3, "stmt1"), map[string]string{"open_units": "0", "open_backfill_runs_org": "0"})
		cells := formExecRewalkPGCells(t, results, 4, "stmt2")
		formExecRewalkPGWant(t, t.Name(), cells, map[string]string{"runs": "1", "runs_open": "0", "runs_success": "1", "runs_partial_failed": "0",
			"runs_failed": "0", "units": "1", "units_open": "0", "units_success": "1", "units_failed": "0", "cicd_units": "1"})
		if !strings.HasPrefix(cells["first_since"], "2026-03-09 00:00:00") || !strings.HasPrefix(cells["last_before"], "2026-03-09 23:59:59.999999") {
			t.Errorf("post window %s .. %s, want the day 2026-03-09", cells["first_since"], cells["last_before"])
		}
		cardGreps(t.Name(), results, true)
	})

	t.Run("a second re-walk: the gate reads 2 runs (a second sitting STOPs at its pre-read)", func(t *testing.T) {
		run("backfill", "success", "success", formExecRewalkPGSource, "2026-03-16 10:00:00+00", day, dayEnd)
		formExecRewalkPGWant(t, t.Name(), formExecRewalkPGCells(t, runForm(t.Name(), formExecRewalkPGSQL), 4, "stmt2"), map[string]string{"runs": "2", "units": "2"})
	})

	t.Run("org mutant: a second org with the prefix stops G0 (orgs=2)", func(t *testing.T) {
		exec(`INSERT INTO public.organizations (id, slug, name) VALUES ($1, 'rewalk-form-two', 'rewalk-form-two')`, formExecRewalkPGOrg2)
		_, err := formExecRewalkPGRun(ctx, pool, formExecRewalkPGSQL)
		if err == nil || !strings.Contains(err.Error(), "STOP-G0 orgs=2") {
			t.Errorf("err = %v, want STOP-G0 orgs=2", err)
		}
		exec(`DELETE FROM public.organizations WHERE id = $1`, formExecRewalkPGOrg2)
	})
	t.Run("org mutant: an org prefix matching nothing stops G0 (orgs=0)", func(t *testing.T) {
		text := strings.Replace(formExecRewalkPGSQL, "starts_with(o.id::text, 'f0f0f0f0')", "starts_with(o.id::text, '00000000')", 1)
		_, err := formExecRewalkPGRun(ctx, pool, text)
		if err == nil || !strings.Contains(err.Error(), "STOP-G0 orgs=0") {
			t.Errorf("err = %v, want STOP-G0 orgs=0", err)
		}
	})
	for _, mutant := range []struct{ name, from string }{
		{"organizations bytes cap 0", "'organizations') <= 67108864"},
		{"sync_runs bytes cap 0", "'sync_runs') <= 1073741824"},
		{"sync_run_units bytes cap 0", "'sync_run_units') <= 4294967296"},
	} {
		t.Run("G0 fires: "+mutant.name, func(t *testing.T) {
			if got := strings.Count(formExecRewalkPGSQL, mutant.from); got != 1 {
				t.Fatalf("cap literal %q %d time(s), want 1", mutant.from, got)
			}
			to := mutant.from[:strings.LastIndex(mutant.from, " ")+1] + "0"
			_, err := formExecRewalkPGRun(ctx, pool, strings.Replace(formExecRewalkPGSQL, mutant.from, to, 1))
			if err == nil || !strings.Contains(err.Error(), "STOP-G0 orgs=1") || !strings.Contains(err.Error(), "bytes=") {
				t.Errorf("err = %v, want STOP-G0 with the bytes text", err)
			}
		})
	}
	t.Run("G0 counts TOAST: a units cap at the heap size stops once TOAST holds bytes", func(t *testing.T) {
		exec(`UPDATE public.sync_run_units SET result = json_build_object('blob', (SELECT string_agg(md5(i::text), '') FROM generate_series(1, 30000) AS i))
		  WHERE org_id = $1`, formExecRewalkPGOrg)
		var heap, toast int64
		if err := pool.QueryRow(ctx, `SELECT pg_relation_size(c.oid), CASE WHEN c.reltoastrelid = 0 THEN 0 ELSE pg_relation_size(c.reltoastrelid) END
			FROM pg_class c WHERE c.oid = 'public.sync_run_units'::regclass`).Scan(&heap, &toast); err != nil {
			t.Fatal(err)
		}
		t.Logf("sync_run_units heap %d bytes, TOAST %d bytes", heap, toast)
		if toast <= 0 {
			t.Fatalf("TOAST holds %d bytes, want > 0 for this case", toast)
		}
		capLiteral := "'sync_run_units') <= 4294967296"
		results := runForm(t.Name()+" cap heap+TOAST", strings.Replace(formExecRewalkPGSQL, capLiteral, "'sync_run_units') <= "+strconv.FormatInt(heap+toast, 10), 1))
		formExecRewalkPGWant(t, t.Name(), formExecRewalkPGCells(t, results, 1, "G0"), map[string]string{"cost_gate_ok": "0", "units_bytes": strconv.FormatInt(heap+toast, 10)})
		_, err := formExecRewalkPGRun(ctx, pool, strings.Replace(formExecRewalkPGSQL, capLiteral, "'sync_run_units') <= "+strconv.FormatInt(heap, 10), 1))
		if err == nil || !strings.Contains(err.Error(), "STOP-G0") || !strings.Contains(err.Error(), "sync_run_units:"+strconv.FormatInt(heap, 10)+"+") {
			t.Errorf("err = %v, want STOP-G0 naming sync_run_units:%d+<toast>", err, heap)
		}
	})
	t.Run("control mutant: another org prefix in stmt3 reads org_runs 0", func(t *testing.T) {
		text := strings.Replace(formExecRewalkPGSQL, "FROM public.sync_runs r\nWHERE starts_with(r.org_id, 'f0f0f0f0');", "FROM public.sync_runs r\nWHERE starts_with(r.org_id, '00000000');", 1)
		if text == formExecRewalkPGSQL {
			t.Fatal("control text not found")
		}
		formExecRewalkPGWant(t, t.Name(), formExecRewalkPGCells(t, runForm(t.Name(), text), 5, "stmt3"), map[string]string{"org_runs": "0"})
	})
	t.Run("read only: a write inside the transaction is refused", func(t *testing.T) {
		text := strings.Replace(formExecRewalkPGSQL, "ROLLBACK;", "DELETE FROM public.sync_runs;\nROLLBACK;", 1)
		_, err := formExecRewalkPGRun(ctx, pool, text)
		if err == nil || !strings.Contains(err.Error(), "read-only") {
			t.Errorf("err = %v, want a read-only transaction error", err)
		}
	})
}
