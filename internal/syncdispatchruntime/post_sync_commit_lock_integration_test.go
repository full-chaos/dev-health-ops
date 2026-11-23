//go:build integration

package syncdispatchruntime

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
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
