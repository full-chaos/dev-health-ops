package syncdispatchruntime

import (
	"context"
	"errors"
	"hash/fnv"
	"math/bits"
	"strconv"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime/synclog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// scheduleRedispatch ports _schedule_redispatch verbatim, INCLUDING its
// exception-swallowing. Python's bare `except Exception:` logs
// dispatch_sync_run.redispatch_rearm_failed and never re-raises -- every
// call site in Dispatch() calls this as a fire-and-forget re-arm, and the
// caller-side wrappers Python leaves around some of those calls are
// unreachable dead code precisely BECAUSE this function structurally
// cannot fail its caller. Ported deliberately as a function with no error
// return, not smoothed over as an implementation detail: a Go caller must
// not add its own error handling assuming this can fail the pass.
//
// Opens its OWN transaction rather than accepting the caller's tx --
// matching Python's own separate `with get_postgres_session_sync() as
// session:` block, which is a DIFFERENT session than whatever transaction
// the caller used to reach its own terminal state (Dispatch()'s main
// claim/route/enqueue session has already closed by every call site that
// reaches this).
// redispatchCountdown is _schedule_redispatch's own default wakeup delay,
// named so a caller that must compare against it (CHAOS-4605's
// deferred-outside-snapshot arm in dispatch_sync_run) reads the SAME env var
// with the SAME default rather than keeping a second copy of the number.
func redispatchCountdown() time.Duration {
	return time.Duration(budgetEnvInt("SYNC_DISPATCH_REDISPATCH_COUNTDOWN", 60)) * time.Second
}

// dueNowRearmAt is the wakeup rule for a pass that has work eligible NOW which
// it nonetheless could not queue -- concurrency-capped units, and (CHAOS-4605)
// units left unclaimed because they became claimable after the guard snapshot.
// That work wants the default countdown: it is not deferred, it is waiting for
// capacity or for the next pass's decision.
//
// The trap is that a pass can hold BOTH kinds at once, and scheduleRedispatch
// writes exactly ONE wakeup whose second statement overwrites a still-pending
// row for this kind UNCONDITIONALLY. So arming the bare countdown while a
// budget/cooldown deferral is already pending EARLIER pushes that earlier
// wakeup back by up to the whole countdown. Codex found this twice, in the two
// arms independently: round 1 P2 in the riverQueued > 0 snapshot-deferral arm
// (a wakeup at now+5s moved to now+60s), and round 2 P2 in the riverQueued == 0
// tail, where `counts.dispatchable > 0` armed the bare countdown and dropped
// `counts.nextDeferredAt` entirely -- the latter reachable on origin/main too,
// via concurrency-capped units, and made far more reachable by CHAOS-4605's own
// allow-list, which is what leaves a unit dispatchable instead of claiming it.
//
// Taking the earlier of the two is therefore not a preference, it is the only
// choice that cannot delay either claimant. Waking early costs one cheap pass;
// waking late strands work that was already due. Named, shared by both arms and
// tested on its own because the RULE is what regressed, and a regression back to
// "always the countdown" is invisible at a call site.
func dueNowRearmAt(nextDeferredAt *time.Time, now time.Time) *time.Time {
	return dueNowRearmAtAfter(nextDeferredAt, now, redispatchCountdown())
}

// dueNowRearmAtAfter is dueNowRearmAt with the countdown given, so the idle
// back-off (CHAOS-8459) lengthens only the countdown and never a deferral.
func dueNowRearmAtAfter(nextDeferredAt *time.Time, now time.Time, countdown time.Duration) *time.Time {
	return earlierOf(nextDeferredAt, now.Add(countdown))
}

// redispatchBackoffMaxDefaultSeconds bounds the idle back-off of the re-arm
// (CHAOS-8459). A unit's terminal commit wakes the pass at once (CHAOS-10013),
// so the countdown only bounds a lost wake and may grow while nothing ends.
const redispatchBackoffMaxDefaultSeconds = 300

// redispatchBackoffJitter is the half-width of the seeded spread (20 percent).
const redispatchBackoffJitter = 0.2

// redispatchCountdownMax is the ceiling of the back-off. A value below the
// configured base is clamped to the base, so a bad setting turns the back-off
// off and never makes the re-arm faster than the base.
func redispatchCountdownMax(base time.Duration) time.Duration {
	limit := time.Duration(budgetEnvInt("SYNC_DISPATCH_REDISPATCH_COUNTDOWN_MAX", redispatchBackoffMaxDefaultSeconds)) * time.Second
	if limit < base {
		return base
	}
	return limit
}

// backoffCountdown grows the re-arm delay with the time since the run last
// made progress: base * 2^k where k is the bit length of idle/base, so the
// delay roughly doubles each time the idle time doubles. The spread is seeded
// by the run id and level, so a given run gets the same value for the same
// level (no test or log flakiness) while different runs do not re-arm in step.
// The result never goes below base or above limit.
func backoffCountdown(base, limit, idle time.Duration, syncRunID string) (time.Duration, int) {
	if base <= 0 || idle < base {
		return base, 0
	}
	level := bits.Len64(uint64(idle / base))
	if level > 20 {
		level = 20
	}
	raw := base
	for step := 0; step < level && raw < limit; step++ {
		raw *= 2
	}
	if raw > limit {
		raw = limit
	}
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(syncRunID + ":" + strconv.Itoa(level)))
	unit := float64(hash.Sum32())/float64(^uint32(0))*2 - 1
	jittered := raw + time.Duration(float64(raw)*redispatchBackoffJitter*unit)
	if jittered < base {
		jittered = base
	}
	if jittered > limit {
		jittered = limit
	}
	return jittered, level
}

// lastRunProgressAt is the newest end of a unit of the run (success or
// failed), else the oldest unit creation. A running unit's heartbeat is not
// progress: a long chunked unit must not keep the re-arm at the base.
func lastRunProgressAt(ctx context.Context, pool *pgxpool.Pool, syncRunID string) (*time.Time, error) {
	var at *time.Time
	err := pool.QueryRow(ctx, `
SELECT COALESCE(MAX(updated_at) FILTER (WHERE status IN ($2, $3)), MIN(created_at))
FROM public.sync_run_units
WHERE sync_run_id = $1::uuid`, syncRunID, syncRunUnitStatusSuccess, syncRunUnitStatusFailed).Scan(&at)
	if err != nil {
		return nil, err
	}
	return at, nil
}

// rearmBackoff returns the countdown for a pass that claimed nothing and left
// due-now work behind, plus log attributes when it grew. A failed read gives
// the base: the back-off is an optimisation and must not delay a run.
func rearmBackoff(ctx context.Context, pool *pgxpool.Pool, syncRunID string, now time.Time) (time.Duration, []synclog.Attr) {
	base := redispatchCountdown()
	progressAt, err := lastRunProgressAt(ctx, pool, syncRunID)
	if err != nil || progressAt == nil {
		return base, nil
	}
	return countdownForIdle(base, redispatchCountdownMax(base), now.Sub(*progressAt), syncRunID)
}

func countdownForIdle(base, limit, idle time.Duration, syncRunID string) (time.Duration, []synclog.Attr) {
	countdown, level := backoffCountdown(base, limit, idle, syncRunID)
	if level == 0 {
		return base, nil
	}
	return countdown, []synclog.Attr{
		synclog.Count(synclog.KeyIdleSeconds, int(idle.Seconds())),
		synclog.Count(synclog.KeyBackoffSeconds, int(countdown.Seconds())),
		synclog.Count(synclog.KeyBackoffLevel, level),
	}
}

func scheduleRedispatch(ctx context.Context, pool *pgxpool.Pool, logger *synclog.Logger, syncRunID string, availableAt *time.Time, now time.Time, extra ...synclog.Attr) {
	countdown := redispatchCountdown()
	redispatchAt := now.Add(countdown)
	if availableAt != nil {
		redispatchAt = *availableAt
	}

	fail := func(err error) {
		logger.Warn(ctx, synclog.MsgDispatchSyncRunRedispatchRearmFailed, synclog.Run(synclog.ParseID(syncRunID)), synclog.Failure(err))
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		fail(err)
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rearmedByOther, err := dispatchWakeupAlreadyPending(ctx, tx, syncRunID)
	if err != nil {
		fail(err)
		return
	}
	if err := upsertDiscoveryOutboxWakeup(ctx, tx, "", syncRunID, outboxKindDispatchSyncRun, redispatchAt); err != nil {
		fail(err)
		return
	}
	// The second write Python's own _schedule_redispatch does on top of the
	// LEAST-semantics upsert above: a still-pending, not-yet-claimed row for
	// THIS kind gets its available_at set to the newly-computed redispatchAt
	// even when that is later. It exists for the pass's OWN row: the relay
	// left it 'dispatched' with the available_at it was armed for, which is
	// in the past, so the upsert's LEAST would keep that past instant and wake
	// the run again on the very next relay tick instead of after the
	// countdown.
	//
	// CHAOS-10013: it is skipped when the row was ALREADY 'pending' before
	// this pass re-armed it. The pass's own delivery is 'dispatched' (the
	// transport fence at the top of Dispatch refuses any other state), so a
	// pending row here was re-armed by another writer while this pass ran --
	// a unit leaving its slot (syncrunrollup.WakeDispatch), reference
	// discovery succeeding, or the materializer. Its instant is a real due
	// time, and overwriting it with now + countdown is how a unit that
	// finished mid-pass lost its wake and the bucket sat idle for the whole
	// countdown. The upsert's LEAST keeps the earlier of the two, which is the
	// same earlier-wins rule dueNowRearmAt states for the pass's own
	// arguments.
	if !rearmedByOther {
		if _, err := tx.Exec(ctx, `
UPDATE public.sync_dispatch_outbox
SET available_at = $2, updated_at = $3
WHERE sync_run_id = $1::uuid AND kind = $4 AND status = 'pending' AND claim_token IS NULL`,
			syncRunID, redispatchAt, now, outboxKindDispatchSyncRun); err != nil {
			fail(err)
			return
		}
	}
	if err := tx.Commit(ctx); err != nil {
		fail(err)
		return
	}
	attrs := append([]synclog.Attr{synclog.Run(synclog.ParseID(syncRunID)), synclog.Count(synclog.KeyCountdown, int(countdown.Seconds())), synclog.Instant(synclog.KeyAvailableAt, redispatchAt), synclog.Flag(synclog.KeyKeptEarlierWakeup, rearmedByOther)}, extra...)
	logger.Info(ctx, synclog.MsgDispatchSyncRunRedispatchRearmed, attrs...)
}

// dispatchWakeupAlreadyPending locks the run's dispatch_sync_run outbox row
// and reports whether it is already 'pending'. The lock holds the row until
// scheduleRedispatch commits, so a concurrent writer cannot slip a wake in
// between this read and the overwrite it decides on. No row is not pending.
func dispatchWakeupAlreadyPending(ctx context.Context, tx pgx.Tx, syncRunID string) (bool, error) {
	var status string
	err := tx.QueryRow(ctx, `
SELECT status FROM public.sync_dispatch_outbox
WHERE sync_run_id = $1::uuid AND kind = $2
FOR UPDATE`, syncRunID, outboxKindDispatchSyncRun).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return status == "pending", nil
}
