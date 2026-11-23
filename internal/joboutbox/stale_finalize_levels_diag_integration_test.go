//go:build integration

package joboutbox

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// DIAGNOSIS ONLY (never merged): which survey statement ends the levels cell at 1000 live runs. It logs a timestamp
// for every survey statement, then EXPLAIN (ANALYZE, BUFFERS) of statement 0, A and B with JIT off and no
// statement_timeout, at 1000 live runs straight after the bulk load, after VACUUM (ANALYZE) river.river_job, and at
// 1001 runs. It asserts nothing.
func TestDiagnoseStaleFinalizeLevelsAtTheCap(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = instance.Close(context.Background()) }()
	admin := openIntegrationPool(t, ctx, instance.URI)
	defer admin.Close()
	createStrandSchema(t, ctx, admin)
	domainRole, err := containers.RoleName("sfdiag_domain", instance)
	if err != nil {
		t.Fatal(err)
	}
	queueRole, err := containers.RoleName("sfdiag_queue", instance)
	if err != nil {
		t.Fatal(err)
	}
	createStrandRoles(t, ctx, admin, domainRole, queueRole)
	if _, err := riverstore.ApplyPinnedMigrations(ctx, admin, riverstore.MigrationOptions{
		Schema: "river", DomainRole: domainRole, QueueRole: queueRole,
	}); err != nil {
		t.Fatal(err)
	}
	old := time.Now().UTC().Add(-72 * time.Hour)
	seed := func(firstDay, n int, at time.Time) {
		t.Helper()
		started := time.Now()
		if _, err := admin.Exec(ctx, `
WITH run AS (
    INSERT INTO public.daily_metrics_runs (id, org_id, target_day, generation, status, finalization_status, created_at, updated_at)
    SELECT gen_random_uuid(), '00000000-0000-4000-8000-0000000d1a90'::uuid, DATE '2020-01-01' + n, 'live-level:' || n, 'running', 'pending', $3, $3
    FROM generate_series($1::int, $1::int + $2::int - 1) AS n
    RETURNING id
), partition AS (
    INSERT INTO public.daily_metrics_partitions (id, run_id, ordinal, repo_ids, status, attempt_count, completed_at, created_at, updated_at)
    SELECT gen_random_uuid(), run.id, 0, '[]'::jsonb, 'succeeded', 1, $3, $3, $3 FROM run
)
INSERT INTO river.river_job (args, attempt, created_at, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at)
SELECT jsonb_build_object('domain', jsonb_build_object('type', 'daily_metrics_run', 'id', run.id::text)),
       0, $3, NULL, 'metrics.daily_finalize', 4, '{}'::jsonb, 2, 'metrics', 'scheduled', $3 + interval '30 days'
FROM run`, firstDay, n, at); err != nil {
			t.Fatal(err)
		}
		t.Logf("DIAG seeded %d live runs in %s", n, time.Since(started))
	}
	jobTable := `"river"."river_job"`
	candidatesSQL := fmt.Sprintf(staleFinalizeCandidatesSQLTemplate, jobTable)
	levelsSQL := fmt.Sprintf(staleFinalizeLevelsSQLTemplate, jobTable)
	execution := regexp.MustCompile(`Execution Time: ([0-9.]+) ms`)
	buffers := regexp.MustCompile(`Buffers: shared hit=(\d+)(?: read=(\d+))?`)
	diagnose := func(state string) {
		t.Helper()
		cutoff := time.Now().UTC().Add(-StaleFinalizeHorizon)
		// 1. The production survey, every statement timestamped.
		start := time.Now()
		begin := func(ctx context.Context) (pgx.Tx, error) {
			tx, err := admin.Begin(ctx)
			if err != nil {
				return nil, err
			}
			return &timedTx{Tx: tx, t: t, state: state, start: start}, nil
		}
		survey := surveyStaleFinalize(ctx, begin, staleFinalizeBaseExistsSQL, candidatesSQL, levelsSQL, cutoff, 10, nil)
		t.Logf("DIAG %s survey: %s total, %d candidates, levels %+v, failures %+v", state, time.Since(start), len(survey.candidates), survey.levels, survey.failures)
		// 2. Each statement under EXPLAIN (ANALYZE, BUFFERS), JIT off, no statement_timeout.
		for _, statement := range []struct {
			name, query string
			args        []any
		}{
			{"statement 0", staleFinalizeBaseExistsSQL, []any{cutoff}},
			{"statement A", candidatesSQL, []any{cutoff, 10, nil, nil, nil}},
			{"statement B", levelsSQL, []any{cutoff, staleFinalizeLevelCap + 1}},
		} {
			tx, err := admin.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			for _, setup := range []string{"SET TRANSACTION READ ONLY", staleFinalizeLockTimeout, staleFinalizeJITOff} {
				if _, err := tx.Exec(ctx, setup); err != nil {
					t.Fatal(err)
				}
			}
			rows, err := tx.Query(ctx, "EXPLAIN (ANALYZE, BUFFERS) "+statement.query, statement.args...)
			if err != nil {
				t.Fatalf("EXPLAIN %s: %v", statement.name, err)
			}
			var plan strings.Builder
			for rows.Next() {
				var line string
				if err := rows.Scan(&line); err != nil {
					t.Fatal(err)
				}
				plan.WriteString(line + "\n")
			}
			rows.Close()
			_ = tx.Rollback(ctx)
			ms, hit, read := "", "", "0"
			if m := execution.FindStringSubmatch(plan.String()); m != nil {
				ms = m[1]
			}
			if m := buffers.FindStringSubmatch(plan.String()); m != nil {
				hit = m[1]
				if m[2] != "" {
					read = m[2]
				}
			}
			t.Logf("DIAG %s %s: exec_ms=%s shared_hit=%s shared_read=%s\n%s", state, statement.name, ms, hit, read, plan.String())
		}
	}
	seed(1, 1000, old)
	diagnose("1000 runs, straight after the bulk load")
	if _, err := admin.Exec(ctx, "VACUUM (ANALYZE) river.river_job"); err != nil {
		t.Fatal(err)
	}
	diagnose("1000 runs, after VACUUM (ANALYZE) river.river_job")
	seed(1001, 1, old.Add(time.Second))
	diagnose("1001 runs")
}

// timedTx logs the start offset, duration and error of every statement the survey sends.
type timedTx struct {
	pgx.Tx
	t     *testing.T
	state string
	start time.Time
}

func (tx *timedTx) log(sql string, began time.Time, err error) {
	first := strings.TrimSpace(sql)
	if i := strings.IndexByte(first, '\n'); i > 0 {
		first = first[:i]
	}
	tx.t.Logf("DIAG %s stmt at +%s took %s err=%v: %.80s", tx.state, began.Sub(tx.start), time.Since(began), err, first)
}

func (tx *timedTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	began := time.Now()
	tag, err := tx.Tx.Exec(ctx, sql, args...)
	tx.log(sql, began, err)
	return tag, err
}

func (tx *timedTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	began := time.Now()
	rows, err := tx.Tx.Query(ctx, sql, args...)
	if err != nil {
		tx.log(sql, began, err)
		return rows, err
	}
	return &timedRows{Rows: rows, done: func(err error) { tx.log(sql, began, err) }}, nil
}

func (tx *timedTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	began := time.Now()
	return timedRow{row: tx.Tx.QueryRow(ctx, sql, args...), done: func(err error) { tx.log(sql, began, err) }}
}

type timedRows struct {
	pgx.Rows
	done   func(error)
	logged bool
}

func (rows *timedRows) Close() {
	rows.Rows.Close()
	if !rows.logged {
		rows.logged = true
		rows.done(rows.Rows.Err())
	}
}

type timedRow struct {
	row  pgx.Row
	done func(error)
}

func (row timedRow) Scan(dest ...any) error {
	err := row.row.Scan(dest...)
	row.done(err)
	return err
}
