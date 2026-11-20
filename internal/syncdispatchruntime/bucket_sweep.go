package syncdispatchruntime

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
	"github.com/full-chaos/dev-health-ops/internal/syncrunrollup"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// The bucket sweep (CHAOS-14250). One sync.bucket_sweep job per (org, provider, cost class) bucket hands the
// bucket's free concurrency slots to its oldest eligible waiting runs. A slot release (a unit end, a reconciler
// repair, a pass that left granted slots unused) bumps the bucket's generation and enqueues it; the River uniqueness of
// the kind collapses concurrent releases into one pending sweep; the sweep re-arms itself (a snooze) while the bucket
// has an eligible waiter, and a run capped by the bucket parks its own dispatch row. Passes per bucket per hour are
// therefore bounded by the sweeps, one safety re-arm plus one per release, whatever the queue depth.
//
// The sweep reserves each slot it hands out (sync_dispatch_bucket_reservations), inside its bucket-locked
// transaction: a pass of another run counts the reservation as a used slot, and the reserved run's own pass retires
// it. A reservation older than admissionLivenessBound is void (a run whose pass never runs stops holding the slot).

// The insert policy of sync.bucket_sweep; it equals the kind's registry row (contracts/jobs/v1/registry.json).
const (
	bucketSweepPriority    = 2
	bucketSweepMaxAttempts = 5
)

// bucketSweepUniqueStates are the River states in which a bucket's sweep blocks another insert: the four River
// requires (available, pending, running, scheduled) and retryable. completed, cancelled and discarded are left out,
// so a finished sweep never blocks the next one.
var bucketSweepUniqueStates = []rivertype.JobState{
	rivertype.JobStateAvailable,
	rivertype.JobStatePending,
	rivertype.JobStateRetryable,
	rivertype.JobStateRunning,
	rivertype.JobStateScheduled,
}

// BucketSweepJobArgs is the River argument type of sync.bucket_sweep: the registry envelope with the bucket as its
// payload. The payload fields carry the River uniqueness tags; the envelope fields are derived from them.
type BucketSweepJobArgs struct {
	Version       int                            `json:"contract_version"`
	OrgID         string                         `json:"organization_id"`
	CorrelationID string                         `json:"correlation_id"`
	Idempotency   string                         `json:"idempotency_key"`
	Domain        jobcontract.DomainLink         `json:"domain"`
	Payload       jobcontract.BucketSweepPayload `json:"payload"`
}

func (BucketSweepJobArgs) Kind() string { return jobcontract.KindBucketSweep }

// NewBucketSweepJobArgs builds the one argument value of a bucket.
func NewBucketSweepJobArgs(orgID, provider, costClass string) BucketSweepJobArgs {
	key := orgID + ":" + provider + ":" + costClass
	return BucketSweepJobArgs{
		Version:       jobcontract.ContractVersionV1,
		OrgID:         orgID,
		CorrelationID: "bucket-sweep:" + key,
		Idempotency:   jobcontract.KindBucketSweep + ":" + key,
		Domain:        jobcontract.DomainLink{Type: "sync_bucket", ID: orgID},
		Payload:       jobcontract.BucketSweepPayload{OrgID: orgID, Provider: provider, CostClass: costClass},
	}
}

func (args BucketSweepJobArgs) valid() error {
	if args != NewBucketSweepJobArgs(args.Payload.OrgID, args.Payload.Provider, args.Payload.CostClass) ||
		!uuidPattern.MatchString(args.OrgID) ||
		!slices.Contains(jobcontract.BucketSweepProviders, args.Payload.Provider) ||
		!slices.Contains(jobcontract.BucketSweepCostClasses, args.Payload.CostClass) {
		return ErrInvalidReference
	}
	return nil
}

// BucketSweepInserter is the part of the River client the enqueuer uses; *river.Client satisfies it.
type BucketSweepInserter interface {
	Insert(ctx context.Context, args river.JobArgs, opts *river.InsertOpts) (*rivertype.JobInsertResult, error)
	JobRetry(ctx context.Context, id int64) (*rivertype.JobRow, error)
}

// RiverBucketSweepEnqueuer inserts sync.bucket_sweep jobs directly with the River client, with the kind's own
// uniqueness. It does not go through worker_job_outbox: the relay dedupes on the outbox dedupe key and on the
// idempotency key across every River state (joboutbox inserter), so a per-bucket key there would block every sweep
// after the first delivered one, and only a direct insert returns the River job a duplicate can be made due with.
type RiverBucketSweepEnqueuer struct {
	client BucketSweepInserter
}

// NewRiverBucketSweepEnqueuer wraps a River client (insert-only is enough) on the queue role's pool.
func NewRiverBucketSweepEnqueuer(client BucketSweepInserter) (*RiverBucketSweepEnqueuer, error) {
	if !present(client) {
		return nil, ErrWorkerRegistration
	}
	return &RiverBucketSweepEnqueuer{client: client}, nil
}

// BucketSweepInsertOpts is the insert policy of one sweep.
func BucketSweepInsertOpts() *river.InsertOpts {
	return &river.InsertOpts{
		Queue:       syncdispatchcontract.RiverQueue,
		Priority:    bucketSweepPriority,
		MaxAttempts: bucketSweepMaxAttempts,
		UniqueOpts: river.UniqueOpts{
			ByArgs:  true,
			ByState: bucketSweepUniqueStates,
		},
	}
}

// EnqueueBucketSweep inserts the bucket's sweep, due now. A sweep already waiting on its safety re-arm (scheduled) or
// on a retry (retryable) is made due now instead (River JobRetry); one that is available, pending or running stays as
// it is: it will read the bucket after the caller's commit, or has read it already and re-checks once before it ends.
func (enqueuer *RiverBucketSweepEnqueuer) EnqueueBucketSweep(ctx context.Context, orgID, provider, costClass string) error {
	return enqueuer.insert(ctx, orgID, provider, costClass, true)
}

// EnsureBucketSweep inserts the bucket's sweep, due now, unless the bucket already has one; a pending sweep keeps its
// due time.
func (enqueuer *RiverBucketSweepEnqueuer) EnsureBucketSweep(ctx context.Context, orgID, provider, costClass string) error {
	return enqueuer.insert(ctx, orgID, provider, costClass, false)
}

func (enqueuer *RiverBucketSweepEnqueuer) insert(ctx context.Context, orgID, provider, costClass string, dueNow bool) error {
	if enqueuer == nil || !present(enqueuer.client) {
		return ErrWorkerRegistration
	}
	args := NewBucketSweepJobArgs(orgID, provider, costClass)
	if err := args.valid(); err != nil {
		return err
	}
	result, err := enqueuer.client.Insert(ctx, args, BucketSweepInsertOpts())
	if err != nil {
		return fmt.Errorf("%w: insert bucket sweep", ErrRiverInsert)
	}
	if result == nil || result.Job == nil {
		return fmt.Errorf("%w: missing bucket sweep job", ErrInsertRejected)
	}
	if dueNow && result.UniqueSkippedAsDuplicate &&
		(result.Job.State == rivertype.JobStateScheduled || result.Job.State == rivertype.JobStateRetryable) {
		if _, err := enqueuer.client.JobRetry(ctx, result.Job.ID); err != nil {
			return fmt.Errorf("%w: make bucket sweep due", ErrRiverInsert)
		}
	}
	return nil
}

// bucketSweepResult is what one sweep did: the runs it woke, whether the bucket still has an eligible waiter, and
// whether a wake source signalled the bucket while the sweep ran (its generation moved).
type bucketSweepResult struct {
	woken    []string
	waiting  bool
	signaled bool
}

// eligibleWaiterPredicateSQL is the one eligibility rule of a bucket's waiter (CHAOS-14250 r1 class 3): an open run
// whose reference discovery succeeded, with a planned or due retrying unit in the bucket. It reads the run as `run`
// and the bucket as $2 (provider), $3 (cost class), the due instant as $5. Each probe is a scalar subquery that stops at
// its first row (ix_sync_run_units_run_bucket_waiting, uq_sync_run_reference_discoveries_run), tested IS NOT NULL. As
// EXISTS the planner turns a probe into a join; as a boolean expression it estimates it at 0.5 % of the rows. Either
// way it hashes or scans every open run of the class and sorts them, which reads the whole queue. IS NOT NULL is
// estimated at 99.5 %, so the ordered walk with the LIMIT is the plan.
const eligibleWaiterPredicateSQL = `run.status NOT IN ('success', 'partial_failed', 'failed')
    AND (
        SELECT 1 FROM public.sync_run_units AS unit
        WHERE unit.sync_run_id = run.id AND unit.provider = $2 AND unit.cost_class = $3
            AND unit.status IN ('planned', 'retrying')
            AND (unit.status = 'planned' OR unit.available_at <= $5)
        LIMIT 1
    ) IS NOT NULL
    AND (
        SELECT 1 FROM public.sync_run_reference_discoveries AS discovery
        WHERE discovery.sync_run_id = run.id AND discovery.status = 'success'
        LIMIT 1
    ) IS NOT NULL`

// bucketHeadsSQL lists the eligible waiters of one class of a bucket, oldest first. It walks the org's open runs of
// the class in (created_at, id) order on ix_sync_runs_org_class_active_order and probes each one, so it reads about
// as many runs as it returns plus the ineligible ones in front of them, never the whole waiting queue. The class is
// compared as an integer ($4: 1 backfill, 0 not): the planner rewrites a boolean compared with false into
// NOT (a OR b), then into two inequalities, which no longer match the index column.
const bucketHeadsSQL = `
SELECT run.id::text
FROM public.sync_runs AS run
WHERE run.org_id = $1
    AND (run.triggered_by = 'backfill' OR run.mode = 'backfill')::integer = $4::integer
    AND ` + eligibleWaiterPredicateSQL + `
ORDER BY run.created_at, run.id
LIMIT $6`

// readBucketHeads reads up to limit eligible waiters of one class, oldest first, leaving out the runs in skip (the
// bucket's reserved runs, at most its cap): it reads limit+len(skip) heads and drops them here.
func readBucketHeads(ctx context.Context, tx pgx.Tx, bucket dispatchBucket, backfill bool, now time.Time, limit int, skip map[string]bool) ([]string, error) {
	if limit <= 0 {
		return nil, nil
	}
	class := 0
	if backfill {
		class = 1
	}
	rows, err := tx.Query(ctx, bucketHeadsSQL, bucket.orgID, bucket.provider, bucket.costClass, class, now, limit+len(skip))
	if err != nil {
		return nil, fmt.Errorf("%w: read bucket heads: %w", ErrDiscoveryTransientFailure, err)
	}
	defer rows.Close()
	var heads []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("%w: scan bucket head: %w", ErrDiscoveryTransientFailure, err)
		}
		if !skip[id] && len(heads) < limit {
			heads = append(heads, id)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: iterate bucket heads: %w", ErrDiscoveryTransientFailure, err)
	}
	return heads, nil
}

// reservedRunsSQL lists the runs that hold a reservation in the bucket (at most its cap, after the expiry).
const reservedRunsSQL = `
SELECT sync_run_id::text FROM public.sync_dispatch_bucket_reservations
WHERE org_id = $1 AND provider = $2 AND cost_class = $3`

func readReservedRuns(ctx context.Context, tx pgx.Tx, bucket dispatchBucket) (map[string]bool, error) {
	rows, err := tx.Query(ctx, reservedRunsSQL, bucket.orgID, bucket.provider, bucket.costClass)
	if err != nil {
		return nil, fmt.Errorf("%w: read reserved runs: %w", ErrDiscoveryTransientFailure, err)
	}
	defer rows.Close()
	reserved := map[string]bool{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("%w: scan reserved run: %w", ErrDiscoveryTransientFailure, err)
		}
		reserved[id] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: iterate reserved runs: %w", ErrDiscoveryTransientFailure, err)
	}
	return reserved, nil
}

// insertBucketReservationSQL reserves one slot of the bucket for a run the sweep picked.
const insertBucketReservationSQL = `
INSERT INTO public.sync_dispatch_bucket_reservations (org_id, provider, cost_class, sync_run_id, backfill, reserved_at)
VALUES ($1, $2, $3, $4::uuid, $5, $6)`

// expireBucketReservationsSQL drops the bucket's void reservations: older than the liveness bound ($5 seconds).
const expireBucketReservationsSQL = `
DELETE FROM public.sync_dispatch_bucket_reservations
WHERE org_id = $1 AND provider = $2 AND cost_class = $3
    AND reserved_at <= $4::timestamptz - make_interval(secs => $5::double precision)`

// runBucketSweep is one sweep of a bucket. Under the bucket's advisory lock (the guard's lock, so a pass and the sweep
// read one state) it reads the bucket's generation, counts the free slots (the cap less the active units and the live
// reservations), and picks, oldest first, up to the backfill share (CHAOS-14211) of backfill waiters and the rest of
// the free slots of non-backfill waiters; it reserves each pick before the commit. After the commit each picked run
// gets its dispatch wakeup as its own statement, so no other run's outbox row is held across a commit (acp 14265
// condition 5). Last, it acknowledges the generation it read; if a wake source bumped it meanwhile, the result says
// signaled and the caller runs the sweep again.
func runBucketSweep(ctx context.Context, pool *pgxpool.Pool, bucket dispatchBucket, now func() time.Time) (bucketSweepResult, error) {
	started := now()
	woken, waiting, generation, err := sweepBucketOnce(ctx, pool, bucket, started)
	if err != nil {
		return bucketSweepResult{}, err
	}
	for _, runID := range woken {
		syncrunrollup.WakeDispatch(ctx, pool, runID, started, syncrunrollup.DispatchWakePathBucketSweep)
	}
	signaled, err := syncrunrollup.AckBucketGeneration(ctx, pool, bucket.orgID, bucket.provider, bucket.costClass, generation)
	if err != nil {
		return bucketSweepResult{woken: woken, waiting: waiting}, fmt.Errorf("%w: %w", ErrDiscoveryTransientFailure, err)
	}
	return bucketSweepResult{woken: woken, waiting: waiting, signaled: signaled}, nil
}

func sweepBucketOnce(ctx context.Context, pool *pgxpool.Pool, bucket dispatchBucket, now time.Time) ([]string, bool, int64, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, false, 0, fmt.Errorf("%w: begin bucket sweep: %w", ErrDiscoveryTransientFailure, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	woken, waiting, generation, err := pickBucketSlots(ctx, tx, bucket, now)
	if err != nil {
		return nil, false, 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, 0, fmt.Errorf("%w: commit bucket sweep: %w", ErrDiscoveryTransientFailure, err)
	}
	return woken, waiting, generation, nil
}

// pickBucketSlots is the sweep's transaction body: under the bucket lock it reads the generation, picks the heads and
// reserves their slots, so the reservations commit with the lock's release.
func pickBucketSlots(ctx context.Context, tx pgx.Tx, bucket dispatchBucket, now time.Time) ([]string, bool, int64, error) {
	if err := acquireBucketAdvisoryLocks(ctx, tx, []dispatchBucket{bucket}); err != nil {
		return nil, false, 0, err
	}
	generation, err := syncrunrollup.ReadBucketGeneration(ctx, tx, bucket.orgID, bucket.provider, bucket.costClass)
	if err != nil {
		return nil, false, 0, fmt.Errorf("%w: %w", ErrDiscoveryTransientFailure, err)
	}
	if _, err := tx.Exec(ctx, expireBucketReservationsSQL, bucket.orgID, bucket.provider, bucket.costClass, now,
		admissionLivenessBound().Seconds()); err != nil {
		return nil, false, 0, fmt.Errorf("%w: expire bucket reservations: %w", ErrDiscoveryTransientFailure, err)
	}
	active, activeBackfill, err := countActiveBucketUnits(ctx, tx, bucket, now.Add(-staleDispatchSeconds()), now)
	if err != nil {
		return nil, false, 0, err
	}
	reserved, reservedBackfill, err := countBucketReservations(ctx, tx, bucket, "", now)
	if err != nil {
		return nil, false, 0, err
	}
	capValue := concurrencyCapForCostClass(bucket.costClass)
	free := max(0, capValue-active-reserved)
	backfillUsable := free
	if capValue >= 2 {
		backfillUsable = min(free, max(0, capValue-1-activeBackfill-reservedBackfill))
	}
	reservedRuns, err := readReservedRuns(ctx, tx, bucket)
	if err != nil {
		return nil, false, 0, err
	}
	backfillHeads, err := readBucketHeads(ctx, tx, bucket, true, now, backfillUsable, reservedRuns)
	if err != nil {
		return nil, false, 0, err
	}
	otherHeads, err := readBucketHeads(ctx, tx, bucket, false, now, free-len(backfillHeads), reservedRuns)
	if err != nil {
		return nil, false, 0, err
	}
	for _, pick := range []struct {
		heads    []string
		backfill bool
	}{{backfillHeads, true}, {otherHeads, false}} {
		for _, runID := range pick.heads {
			if _, err := tx.Exec(ctx, insertBucketReservationSQL, bucket.orgID, bucket.provider, bucket.costClass, runID,
				pick.backfill, now); err != nil {
				return nil, false, 0, fmt.Errorf("%w: reserve bucket slot: %w", ErrDiscoveryTransientFailure, err)
			}
		}
	}
	woken := append(backfillHeads, otherHeads...)
	waiting := len(woken) > 0
	for _, backfill := range []bool{true, false} {
		if waiting {
			break
		}
		heads, err := readBucketHeads(ctx, tx, bucket, backfill, now, 1, nil)
		if err != nil {
			return nil, false, 0, err
		}
		waiting = len(heads) > 0
	}
	return woken, waiting, generation, nil
}

type bucketSweepWorker struct {
	river.WorkerDefaults[BucketSweepJobArgs]
	pool *pgxpool.Pool
	now  func() time.Time
	taps handlerTaps
}

// Middleware feeds the claim-liveness taps (WithHandlerObserver).
func (worker *bucketSweepWorker) Middleware(job *rivertype.JobRow) []rivertype.WorkerMiddleware {
	if worker == nil {
		return nil
	}
	return worker.taps.middleware(job)
}

// Work runs one sweep. A bucket signalled during the sweep runs it again at once (JobSnooze(0): an insert would be
// dropped as a duplicate of this running job); otherwise it re-arms with a snooze (no attempt is used) while the
// bucket has an eligible waiter.
func (worker *bucketSweepWorker) Work(ctx context.Context, job *river.Job[BucketSweepJobArgs]) (err error) {
	if worker == nil || worker.pool == nil || job == nil {
		return ErrWorkerRegistration
	}
	if err := job.Args.valid(); err != nil {
		return river.JobCancel(err)
	}
	var span oteltrace.Span
	ctx, span = spanForCoordinatorJob(ctx, job.Args.Kind(), "", "")
	completed := false
	defer finishCoordinatorWork(span, &err, &completed)
	bucket := dispatchBucket{orgID: job.Args.Payload.OrgID, provider: job.Args.Payload.Provider, costClass: job.Args.Payload.CostClass}
	result, err := runBucketSweep(ctx, worker.pool, bucket, worker.now)
	completed = true
	if err != nil {
		return err
	}
	if result.signaled {
		return river.JobSnooze(0)
	}
	if result.waiting {
		return river.JobSnooze(cappedRearmMax())
	}
	return nil
}

// RegisterBucketSweepWorker registers the sync.bucket_sweep worker on the domain pool. The caller reports the kind's
// handler spec, as for the other registry kinds this runtime hosts.
func RegisterBucketSweepWorker(workers *river.Workers, pool *pgxpool.Pool, options ...WorkerOption) error {
	if workers == nil || pool == nil {
		return ErrWorkerRegistration
	}
	taps := handlerTaps{observer: resolveWorkerOptions(options).observer}
	if river.AddWorkerSafely(workers, &bucketSweepWorker{pool: pool, now: func() time.Time { return time.Now().UTC() }, taps: taps}) != nil {
		return ErrWorkerRegistration
	}
	return nil
}
