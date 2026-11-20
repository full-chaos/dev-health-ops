package syncdispatchruntime

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/syncbucket"
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
	nextAt   time.Time
}

// runBucketSweep is one sweep of a bucket. Under the bucket's advisory lock (the guard's lock, so a pass and the sweep
// read one state) it reads the bucket's generation and reserves the bucket's free slots for its oldest eligible
// waiters (syncbucket.ReserveHeads: the cap less the active units and the live reservations, the backfill share first).
// After the commit each reserved run gets its dispatch wakeup as its own statement, so no other run's outbox row is
// held across a commit (acp 14265 condition 5). Last, it acknowledges the generation it read; if a wake source bumped
// it meanwhile, the result says signaled and the caller runs the sweep again. nextAt is when the bucket's oldest live
// reservation expires (zero: none), so a reserved run that never claims is picked again then.
func runBucketSweep(ctx context.Context, pool *pgxpool.Pool, bucket dispatchBucket, now func() time.Time) (bucketSweepResult, error) {
	started := now()
	woken, waiting, generation, nextAt, err := sweepBucketOnce(ctx, pool, bucket, started)
	if err != nil {
		return bucketSweepResult{}, err
	}
	syncrunrollup.WakeReserved(ctx, pool, woken, started, syncrunrollup.DispatchWakePathBucketSweep)
	signaled, err := syncrunrollup.AckBucketGeneration(ctx, pool, bucket.orgID, bucket.provider, bucket.costClass, generation)
	if err != nil {
		return bucketSweepResult{woken: woken, waiting: waiting, nextAt: nextAt}, fmt.Errorf("%w: %w", ErrDiscoveryTransientFailure, err)
	}
	return bucketSweepResult{woken: woken, waiting: waiting, signaled: signaled, nextAt: nextAt}, nil
}

func sweepBucketOnce(ctx context.Context, pool *pgxpool.Pool, bucket dispatchBucket, now time.Time) ([]string, bool, int64, time.Time, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, false, 0, time.Time{}, fmt.Errorf("%w: begin bucket sweep: %w", ErrDiscoveryTransientFailure, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	woken, waiting, generation, nextAt, err := pickBucketSlots(ctx, tx, bucket, now)
	if err != nil {
		return nil, false, 0, time.Time{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, 0, time.Time{}, fmt.Errorf("%w: commit bucket sweep: %w", ErrDiscoveryTransientFailure, err)
	}
	return woken, waiting, generation, nextAt, nil
}

// pickBucketSlots is the sweep's transaction body: the bucket lock first, then the generation and the reservations.
func pickBucketSlots(ctx context.Context, tx pgx.Tx, bucket dispatchBucket, now time.Time) ([]string, bool, int64, time.Time, error) {
	if err := acquireBucketAdvisoryLocks(ctx, tx, []dispatchBucket{bucket}); err != nil {
		return nil, false, 0, time.Time{}, err
	}
	generation, err := syncrunrollup.ReadBucketGeneration(ctx, tx, bucket.orgID, bucket.provider, bucket.costClass)
	if err != nil {
		return nil, false, 0, time.Time{}, fmt.Errorf("%w: %w", ErrDiscoveryTransientFailure, err)
	}
	heads, err := syncbucket.ReserveHeads(ctx, tx, bucket.syncBucket(), now, "")
	if err != nil {
		return nil, false, 0, time.Time{}, fmt.Errorf("%w: %w", ErrDiscoveryTransientFailure, err)
	}
	nextAt, _, err := syncbucket.EarliestReservationExpiry(ctx, tx, bucket.syncBucket())
	if err != nil {
		return nil, false, 0, time.Time{}, fmt.Errorf("%w: %w", ErrDiscoveryTransientFailure, err)
	}
	return heads.Reserved, heads.StillWaiting, generation, nextAt, nil
}

// sweepRearm is the sweep's snooze while the bucket has a waiter: SYNC_DISPATCH_CAPPED_REARM_MAX, or sooner when a
// reservation expires first (one second after it), so a head that never claims is picked again by eligibility.
func sweepRearm(now, nextAt time.Time) time.Duration {
	rearm := cappedRearmMax()
	if !nextAt.IsZero() {
		rearm = min(rearm, max(time.Second, nextAt.Sub(now)+time.Second))
	}
	return rearm
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
		return river.JobSnooze(sweepRearm(worker.now(), result.nextAt))
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
