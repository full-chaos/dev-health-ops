//go:build integration

package syncdispatchruntime

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime/synclog"
)

// TestPostSyncCommitsAfterAWakeOfItsOwnRunReleasesTheOutboxRow: the post_sync pass gets the fence's bounded retry
// (CHAOS-14273). A wake of the same run holds the post_sync outbox row (a real row lock: the wake upsert, open in a
// transaction on the second pool) when the pass reaches its commit snapshot; the wake commits right after the first
// 55P03. The pass reads the snapshot again (the wake left the row 'pending', unclaimed, same attempt: decideCommit
// accepts it) and commits, with one fan-out outcome that is not an error and no failure or give-up line.
//
// RED CONTROL (today): postSyncCommitAllowed reads the snapshot once, on the pass transaction itself; the 55P03 aborts
// it and Fanout returns ErrPostSyncUnavailable (stage commit_check, SQLSTATE 55P03), so River burns the attempt.
func TestPostSyncCommitsAfterAWakeOfItsOwnRunReleasesTheOutboxRow(t *testing.T) {
	logs := &lockedLogBuffer{}
	observer := &recordingFanoutObserver{}
	kind := postSyncFenceKind(postSyncPassOptions{logs: logs, observer: observer})
	withCommitLockPools(t, func(ctx context.Context, pool, other *pgxpool.Pool, tracer *commitLockTracer) {
		kind.seed(t, ctx, other)
		if _, err := other.Exec(ctx, `UPDATE sync_dispatch_outbox SET attempts = 1 WHERE id = $1`, kind.outbox); err != nil {
			t.Fatal(err)
		}
		holder, err := other.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = holder.Rollback(context.Background()) }()
		tag, err := holder.Exec(ctx, rearmOutboxSQL, uuid.NewString(), kind.org, kind.run, kind.kind)
		if err != nil {
			t.Fatal(err)
		}
		if tag.RowsAffected() != 1 {
			t.Fatalf("own-run wake upsert rows = %d, want 1 (the row is not held)", tag.RowsAffected())
		}
		var released error
		tracer.onBusy = func() { released = holder.Commit(ctx) }

		err = kind.pass(t, ctx, pool, 1)
		if busy := tracer.busyReads(); busy != 1 {
			t.Fatalf("commit snapshot 55P03 reads = %d, want 1 (the wake must hold the row when the pass reaches it)", busy)
		}
		if released != nil {
			t.Fatalf("release the wake: %v", released)
		}
		if err != nil || !kind.wrote(t, ctx, other) {
			t.Fatalf("post_sync pass after the wake released the row: err=%v wrote=%t, want nil/true (a bounded re-read, not a River retry)",
				err, kind.wrote(t, ctx, other))
		}
		if len(observer.outcomes) != 1 || observer.outcomes[0] == jobruntime.PostSyncFanoutOutcomeError {
			t.Fatalf("fan-out outcomes %v, want exactly one, not error", observer.outcomes)
		}
		for _, line := range logs.lines() {
			if strings.Contains(line, "msg=post_sync_fanout.failed") || strings.Contains(line, "msg=sync_dispatch_fence.commit_snapshot_busy") {
				t.Fatalf("a released row logged a failure or give-up line:\n%s", strings.Join(logs.lines(), "\n"))
			}
		}
	})
}

// seedHeldPostSyncRow seeds the post_sync fence world at attempt 1 and runs the wake upsert of its row in an open
// transaction on other: the row stays held until the returned transaction ends.
func seedHeldPostSyncRow(t *testing.T, ctx context.Context, other *pgxpool.Pool, kind fenceKind) pgx.Tx {
	t.Helper()
	kind.seed(t, ctx, other)
	if _, err := other.Exec(ctx, `UPDATE sync_dispatch_outbox SET attempts = 1 WHERE id = $1`, kind.outbox); err != nil {
		t.Fatal(err)
	}
	holder, err := other.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tag, err := holder.Exec(ctx, rearmOutboxSQL, uuid.NewString(), kind.org, kind.run, kind.kind)
	if err != nil || tag.RowsAffected() != 1 {
		_ = holder.Rollback(context.Background())
		t.Fatalf("own-run wake upsert: rows=%d err=%v, want 1 row held", tag.RowsAffected(), err)
	}
	return holder
}

func postSyncFenceArgs() PostSyncArgs {
	return PostSyncArgs{TransportArgs: TransportArgs{
		Version: ContractVersionV1, OrgID: postSyncFenceOrg, RunID: postSyncFenceRun,
		DispatchOutbox: postSyncFenceOutbox, RouteGeneration: postSyncRouteGeneration, DeliveryAttempt: 1,
	}}
}

// TestPostSyncGivesUpOnAnOutboxRowHeldPastTheRetryBound: the same-run wake never commits. The pass reads the snapshot
// once per backoff step (7 reads, each in a savepoint) and returns ErrPostSyncUnavailable at commit_check with the last
// 55P03 in its chain: one WARN give-up line for kind post_sync, the counter at 1, exactly one ERROR
// post_sync_fanout.failed line, one error outcome and nothing committed. No statement of the pass ever met an aborted
// transaction (25P02), and a gate that gave up leaves its transaction usable: the savepoints took every 55P03.
func TestPostSyncGivesUpOnAnOutboxRowHeldPastTheRetryBound(t *testing.T) {
	reader := installBusyMeter(t)
	logs := &lockedLogBuffer{}
	observer := &recordingFanoutObserver{}
	kind := postSyncFenceKind(postSyncPassOptions{logs: logs, observer: observer})
	withCommitLockPools(t, func(ctx context.Context, pool, other *pgxpool.Pool, tracer *commitLockTracer) {
		holder := seedHeldPostSyncRow(t, ctx, other, kind)
		defer func() { _ = holder.Rollback(context.Background()) }()
		installFakeSnapshotClock(t)

		passCtx, cancel := context.WithTimeout(ctx, busyPassBound)
		defer cancel()
		err := kind.pass(t, passCtx, pool, 1)

		var pgErr *pgconn.PgError
		if !errors.Is(err, ErrPostSyncUnavailable) || postSyncStageOf(err) != stageCommitCheck || !errors.As(err, &pgErr) || pgErr.Code != "55P03" {
			t.Fatalf("pass on a row held past the bound: %v (stage %q), want ErrPostSyncUnavailable at commit_check over SQLSTATE 55P03", err, postSyncStageOf(err))
		}
		if want := len(commitSnapshotLockBackoff) + 1; tracer.busyReads() != want {
			t.Fatalf("commit snapshot 55P03 reads = %d, want %d (one per backoff step and the first)", tracer.busyReads(), want)
		}
		var failed, warns []string
		for _, line := range logs.lines() {
			if strings.Contains(line, "msg=post_sync_fanout.failed") {
				failed = append(failed, line)
			}
			if strings.Contains(line, "msg=sync_dispatch_fence.commit_snapshot_busy") {
				warns = append(warns, line)
			}
		}
		if len(failed) != 1 || !strings.Contains(failed[0], "level=ERROR") || !strings.Contains(failed[0], "reason=commit_check ") ||
			!strings.Contains(failed[0], "error.code=55P03") {
			t.Fatalf("failure lines = %q, want exactly one ERROR naming reason=commit_check and error.code=55P03", failed)
		}
		if len(warns) != 1 || !strings.Contains(warns[0], "level=WARN") || !strings.Contains(warns[0], "outbox_kind="+outboxKindPostSync+" ") ||
			!strings.Contains(warns[0], "reason="+commitSnapshotBusyReason+" ") || !strings.Contains(warns[0], "tries=7 ") ||
			!strings.Contains(warns[0], "sync_run_id="+postSyncFenceRun) {
			t.Fatalf("give-up lines = %q, want one WARN naming the run, outbox_kind=post_sync, the reason and tries=7", warns)
		}
		counted := busyCounts(t, reader)
		if want := outboxKindPostSync + "/" + commitSnapshotBusyReason; counted[want] != 1 || len(counted) != 1 {
			t.Fatalf("%s = %v, want exactly {%s: 1}", FenceCommitSnapshotBusyCounter, counted, want)
		}
		if len(observer.outcomes) != 1 || observer.outcomes[0] != jobruntime.PostSyncFanoutOutcomeError {
			t.Fatalf("fan-out outcomes %v, want exactly [error]", observer.outcomes)
		}
		if kind.wrote(t, ctx, other) {
			t.Fatal("the pass committed while the row was held")
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(context.Background()) }()
		gate := &NativePostSyncService{logger: synclog.New(slog.New(slog.NewTextHandler(io.Discard, nil)))}
		if allowed, gateErr := gate.postSyncCommitAllowed(passCtx, tx, postSyncFenceArgs()); allowed || !errors.Is(gateErr, ErrPostSyncUnavailable) {
			t.Fatalf("gate on the held row: allowed=%t err=%v, want ErrPostSyncUnavailable", allowed, gateErr)
		}
		var one int
		if err := tx.QueryRow(ctx, `SELECT 1`).Scan(&one); err != nil || one != 1 {
			t.Fatalf("the gate's transaction after its give-up: %v, want usable (no 25P02: every 55P03 hit a savepoint)", err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatalf("roll back the gate's transaction: %v", err)
		}
		if aborted := tracer.abortedStatements(); aborted != 0 {
			t.Fatalf("%d statements failed with SQLSTATE 25P02 (in failed transaction), want 0", aborted)
		}
	})
}

// countingPostSyncDaily is the marker daily writer that counts its stagings.
type countingPostSyncDaily struct {
	markerDaily
	calls *int
}

func (writer countingPostSyncDaily) StartRunTx(ctx context.Context, tx pgx.Tx, plan PostSyncPlan, prerequisite string) (string, string, error) {
	*writer.calls++
	return writer.markerDaily.StartRunTx(ctx, tx, plan, prerequisite)
}

// TestPostSyncRetryStagesTheFanoutOnce: the bounded re-read repeats only the commit snapshot. When the wake commits on
// the first 55P03, the pass commits with the daily writer staged exactly once, one marker row per family and exactly
// one published outcome: no second fan-out from the retry.
func TestPostSyncRetryStagesTheFanoutOnce(t *testing.T) {
	kind := postSyncFenceKind(postSyncPassOptions{})
	withCommitLockPools(t, func(ctx context.Context, pool, other *pgxpool.Pool, tracer *commitLockTracer) {
		holder := seedHeldPostSyncRow(t, ctx, other, kind)
		defer func() { _ = holder.Rollback(context.Background()) }()
		var released error
		tracer.onBusy = func() { released = holder.Commit(ctx) }
		calls := 0
		observer := &recordingFanoutObserver{}
		service, err := NewNativePostSyncService(pool, countingPostSyncDaily{calls: &calls}, markerRemaining{}, markerWorkGraph{},
			markerTeam{}, markerTeamRepoOwnership{}, synclog.New(slog.New(slog.NewTextHandler(io.Discard, nil))))
		if err != nil {
			t.Fatal(err)
		}
		service.now = func() time.Time { return time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC) }
		service.SetFanoutObserver(observer)

		err = service.Fanout(ctx, postSyncFenceArgs())
		if busy := tracer.busyReads(); busy != 1 || released != nil {
			t.Fatalf("commit snapshot 55P03 reads = %d (release %v), want 1 and a clean release", busy, released)
		}
		if err != nil || calls != 1 {
			t.Fatalf("pass after the wake released the row: err=%v daily stagings=%d, want nil/1", err, calls)
		}
		if len(observer.outcomes) != 1 || observer.outcomes[0] != jobruntime.PostSyncFanoutOutcomePublished {
			t.Fatalf("fan-out outcomes %v, want exactly [published]", observer.outcomes)
		}
		rows, err := other.Query(ctx, `SELECT kind, count(*) FROM post_sync_markers WHERE sync_run_id = $1 GROUP BY kind`, postSyncFenceRun)
		if err != nil {
			t.Fatal(err)
		}
		perKind, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (int64, error) {
			var family string
			var count int64
			if err := row.Scan(&family, &count); err != nil {
				return 0, err
			}
			if count != 1 {
				t.Errorf("marker rows of %s = %d, want 1", family, count)
			}
			if family == "daily" {
				return 1, nil
			}
			return 0, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		daily := int64(0)
		for _, hit := range perKind {
			daily += hit
		}
		if daily != 1 {
			t.Fatalf("daily marker families = %d, want 1 (the daily fan-out committed once)", daily)
		}
	})
}
