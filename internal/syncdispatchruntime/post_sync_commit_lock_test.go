package syncdispatchruntime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime/synclog"
)

// untimedLogger is a text logger with no time attribute, so a cell can compare whole lines.
func untimedLogger(w io.Writer) *synclog.Logger {
	return synclog.New(slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{
		ReplaceAttr: func(groups []string, attr slog.Attr) slog.Attr {
			if len(groups) == 0 && attr.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return attr
		},
	})))
}

const (
	commitLockUnitOrg = "00000000-0000-4000-8000-0000000000a1"
	commitLockUnitRun = "00000000-0000-4000-8000-000000000001"
)

func commitLockUnitTransport() TransportArgs {
	return TransportArgs{
		Version: ContractVersionV1, OrgID: commitLockUnitOrg, RunID: commitLockUnitRun,
		DispatchOutbox: "00000000-0000-4000-8000-000000000002", RouteGeneration: 1, DeliveryAttempt: 1,
	}
}

// busyWarnLine is the exact give-up WARN of one outbox kind and reason after 7 reads in the 315 ms window, of run runID, its
// cause the error of type errType with SQLSTATE 55P03.
func busyWarnLine(runID, kind, reason, errType string) string {
	return fmt.Sprintf("level=WARN msg=sync_dispatch_fence.commit_snapshot_busy sync_run_id=%s outbox_kind=%s reason=%s tries=7 lock_wait_budget=315ms error.class=postgres error.type=%s error.code=55P03\n",
		runID, kind, reason, errType)
}

// TestPostSyncCommitGateGivesUpWithItsStageAndTheLockError: the post_sync commit gate reads a held row (or route
// barrier) once per backoff step, each read in its own savepoint, then returns ErrPostSyncUnavailable at commit_check
// with the sentinel text and the last 55P03 in its chain, one exact WARN for kind post_sync and the counter at 1.
func TestPostSyncCommitGateGivesUpWithItsStageAndTheLockError(t *testing.T) {
	for _, c := range []struct {
		name, reason, errType string
		tx                    func() *snapshotTx
	}{
		{"row held", commitSnapshotBusyReason, "*pgconn.PgError", func() *snapshotTx { return &snapshotTx{result: lockNotAvailable} }},
		{"route barrier held", commitBarrierBusyReason, "*fmt.wrapErrors", func() *snapshotTx {
			return &snapshotTx{result: lockNotAvailable, barrier: lockNotAvailable}
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			reader := installBusyMeter(t)
			clock := installFakeSnapshotClock(t)
			var logs bytes.Buffer
			service := &NativePostSyncService{logger: untimedLogger(&logs)}
			tx := c.tx()
			allowed, err := service.postSyncCommitAllowed(context.Background(), tx, PostSyncArgs{commitLockUnitTransport()})
			if want := len(commitSnapshotLockBackoff) + 1; tx.savepoints != want || tx.rollbacks != want || clock.sleepCount() != want-1 || tx.commit != 0 || tx.released != 0 {
				t.Fatalf("savepoints=%d rollbacks=%d waits=%d commits=%d released=%d; want %d savepoints, each rolled back, %d waits, nothing committed",
					tx.savepoints, tx.rollbacks, clock.sleepCount(), tx.commit, tx.released, want, want-1)
			}
			var pgErr *pgconn.PgError
			if allowed || !errors.Is(err, ErrPostSyncUnavailable) || err.Error() != ErrPostSyncUnavailable.Error() ||
				postSyncStageOf(err) != stageCommitCheck || !errors.As(err, &pgErr) || pgErr.Code != "55P03" {
				t.Fatalf("gate = %t, %v (stage %q); want refused with ErrPostSyncUnavailable (sentinel text) at commit_check over SQLSTATE 55P03",
					allowed, err, postSyncStageOf(err))
			}
			if got, want := logs.String(), busyWarnLine(commitLockUnitRun, outboxKindPostSync, c.reason, c.errType); got != want {
				t.Fatalf("log =\n%q\nwant exactly\n%q", got, want)
			}
			counted := busyCounts(t, reader)
			if want := outboxKindPostSync + "/" + c.reason; counted[want] != 1 || len(counted) != 1 {
				t.Fatalf("%s = %v, want exactly {%s: 1}", FenceCommitSnapshotBusyCounter, counted, want)
			}
		})
	}
}

// TestPostSyncCommitGateReadsAgainAfterAReleasedRow: a row held at read 1 and free at read 2 is accepted: one savepoint
// rolled back, the second released, no WARN and no counter.
func TestPostSyncCommitGateReadsAgainAfterAReleasedRow(t *testing.T) {
	reader := installBusyMeter(t)
	installFakeSnapshotClock(t)
	var logs bytes.Buffer
	service := &NativePostSyncService{logger: untimedLogger(&logs)}
	tx := &snapshotTx{result: func(read int) error {
		if read == 1 {
			return &pgconn.PgError{Code: "55P03"}
		}
		return nil
	}}
	allowed, err := service.postSyncCommitAllowed(context.Background(), tx, PostSyncArgs{commitLockUnitTransport()})
	if err != nil || !allowed || tx.reads != 2 || tx.rollbacks != 1 || tx.released != 1 {
		t.Fatalf("gate = %t, %v (reads %d, rollbacks %d, released %d); want allowed after 2 reads", allowed, err, tx.reads, tx.rollbacks, tx.released)
	}
	if logs.Len() != 0 || len(busyCounts(t, reader)) != 0 {
		t.Fatalf("a released row reported busy: %v\n%s", busyCounts(t, reader), logs.String())
	}
}

// TestPostSyncCommitGateDoesNotRetryOtherErrors: a read error that is not 55P03 is ErrPostSyncUnavailable at
// commit_check over that error after ONE read, with no WARN and no counter (what the single read returned before).
func TestPostSyncCommitGateDoesNotRetryOtherErrors(t *testing.T) {
	reader := installBusyMeter(t)
	installFakeSnapshotClock(t)
	var logs bytes.Buffer
	service := &NativePostSyncService{logger: untimedLogger(&logs)}
	tx := &snapshotTx{result: func(int) error { return &pgconn.PgError{Code: "40001"} }}
	allowed, err := service.postSyncCommitAllowed(context.Background(), tx, PostSyncArgs{commitLockUnitTransport()})
	var pgErr *pgconn.PgError
	if allowed || tx.reads != 1 || postSyncStageOf(err) != stageCommitCheck || err.Error() != ErrPostSyncUnavailable.Error() ||
		!errors.As(err, &pgErr) || pgErr.Code != "40001" {
		t.Fatalf("gate = %t, %v (reads %d); want refused after 1 read with ErrPostSyncUnavailable at commit_check over 40001", allowed, err, tx.reads)
	}
	if logs.Len() != 0 || len(busyCounts(t, reader)) != 0 {
		t.Fatalf("a non-lock error reported busy: %v\n%s", busyCounts(t, reader), logs.String())
	}
}

// TestFencesWithoutAFailureFuncKeepTheirErrors: dispatch, finalize and reference discovery set no failure func. Their
// give-up is still the fmt wrap of the sentinel naming the reason (no Postgres error in the chain), a non-lock error
// is the sentinel value itself, and the WARN line is the same line.
func TestFencesWithoutAFailureFuncKeepTheirErrors(t *testing.T) {
	transport := commitLockUnitTransport()
	for _, c := range []struct {
		kind     string
		sentinel error
		args     Args
	}{
		{outboxKindDispatchSyncRun, ErrDispatchSyncRunUnavailable, DispatchSyncRunArgs{transport}},
		{outboxKindFinalizeSyncRun, ErrFinalizeSyncRunUnavailable, FinalizeSyncRunArgs{transport}},
		{outboxKindReferenceDiscovery, ErrReferenceDiscoveryUnavailable, ReferenceDiscoveryArgs{transport}},
	} {
		t.Run(c.kind, func(t *testing.T) {
			installBusyMeter(t)
			installFakeSnapshotClock(t)
			var logs bytes.Buffer
			fence := &transportFence{args: c.args, kind: c.kind, unavailable: c.sentinel, logger: untimedLogger(&logs)}
			_, err := fence.lockCommitSnapshot(context.Background(), &snapshotTx{result: lockNotAvailable})
			want := c.sentinel.Error() + ": commit snapshot: " + commitSnapshotBusyReason + " (SQLSTATE 55P03 after 7 reads in a 315ms window)"
			var pgErr *pgconn.PgError
			if err == nil || err.Error() != want || fmt.Sprintf("%T", err) != "*fmt.wrapError" || errors.Unwrap(err) != c.sentinel || errors.As(err, &pgErr) {
				t.Fatalf("give-up = %T %v; want *fmt.wrapError %q over the sentinel alone", err, err, want)
			}
			if got, want := logs.String(), busyWarnLine(commitLockUnitRun, c.kind, commitSnapshotBusyReason, "*pgconn.PgError"); got != want {
				t.Fatalf("log =\n%q\nwant exactly\n%q", got, want)
			}
			_, err = fence.lockCommitSnapshot(context.Background(), &snapshotTx{result: func(int) error { return &pgconn.PgError{Code: "40001"} }})
			if err != c.sentinel {
				t.Fatalf("non-lock read error = %T %v, want the sentinel value %v itself", err, err, c.sentinel)
			}
		})
	}
}

// TestPostSyncCommitGateJudgesTheRetriedRead: decideCommit judges the snapshot of the read that succeeded, not the
// delivery the pass started with. Read 1 meets the held row (55P03); the holder bumps the row's attempts to 2 (a newer
// delivery owns it) and releases; read 2 returns that row. The gate refuses delivery attempt 1 exactly as a fresh read of
// the same row does (false, nil error, one INFO post_sync_fanout.delivery_not_current line with
// reason=delivery_attempt_mismatch), and the bound was not hit, so there is no WARN and no busy count.
func TestPostSyncCommitGateJudgesTheRetriedRead(t *testing.T) {
	args := PostSyncArgs{commitLockUnitTransport()}
	if allowed, reason := decideCommit(commitSnapshot{found: true, attempts: 2}, args.RouteGeneration(), args.Attempt()); allowed || reason != "delivery_attempt_mismatch" {
		t.Fatalf("decideCommit on a row at attempts 2 for delivery attempt 1 = %t, %q; want false, delivery_attempt_mismatch", allowed, reason)
	}
	gate := func(t *testing.T, tx *snapshotTx) (bool, error, string, map[string]int64) {
		t.Helper()
		reader := installBusyMeter(t)
		installFakeSnapshotClock(t)
		var logs bytes.Buffer
		service := &NativePostSyncService{logger: untimedLogger(&logs)}
		allowed, err := service.postSyncCommitAllowed(context.Background(), tx, args)
		return allowed, err, logs.String(), busyCounts(t, reader)
	}
	fresh := &snapshotTx{result: func(int) error { return nil }, attempts: func(int) int64 { return 2 }}
	freshAllowed, freshErr, freshLog, _ := gate(t, fresh)
	if freshAllowed || freshErr != nil || fresh.reads != 1 {
		t.Fatalf("fresh read of the superseded row = %t, %v (reads %d); want refused with no error after 1 read", freshAllowed, freshErr, fresh.reads)
	}
	retried := &snapshotTx{
		result: func(read int) error {
			if read == 1 {
				return &pgconn.PgError{Code: "55P03"}
			}
			return nil
		},
		attempts: func(read int) int64 { return 2 },
	}
	allowed, err, log, counted := gate(t, retried)
	if allowed || err != nil || retried.reads != 2 || retried.rollbacks != 1 || retried.released != 1 || retried.commit != 0 {
		t.Fatalf("retried read of the superseded row = %t, %v (reads %d, rollbacks %d, released %d, commits %d); want refused with no error after 2 reads",
			allowed, err, retried.reads, retried.rollbacks, retried.released, retried.commit)
	}
	want := "level=INFO msg=post_sync_fanout.delivery_not_current org_id=" + commitLockUnitOrg + " sync_run_id=" + commitLockUnitRun +
		" reason=delivery_attempt_mismatch delivery_route_generation=1 current_route_generation=1\n"
	if freshLog != want || log != freshLog {
		t.Fatalf("refusal log after the retry =\n%q\nfresh read =\n%q\nwant both exactly\n%q", log, freshLog, want)
	}
	if len(counted) != 0 {
		t.Fatalf("%s = %v after a retry inside the bound, want none", FenceCommitSnapshotBusyCounter, counted)
	}
}
