package syncdispatchruntime

import (
	"context"
	"fmt"
	"regexp"
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
// repair, a pass that left granted slots unused) enqueues it; the River uniqueness of the kind collapses concurrent
// releases into one pending sweep; the sweep re-arms itself (a snooze) while the bucket has an eligible waiter, and a
// run capped by the bucket arms no timer of its own. Passes per bucket per hour are therefore bounded by the sweeps,
// one safety re-arm plus one per release, whatever the queue depth.

// bucketSweepWokenKey is the mark the sweep writes into sync_runs.result of a run it picked: the Unix time of the pick.
// The older-run hold-back (CHAOS-8880) reads it as a liveness sign, so another run's pass leaves the slot to the run.
const bucketSweepWokenKey = "bucket_sweep_woken_epoch"

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

// bucketLabelPattern bounds a provider or cost class label (the payload schema's pattern).
var bucketLabelPattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,31}$`)

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
		!bucketLabelPattern.MatchString(args.Payload.Provider) || !bucketLabelPattern.MatchString(args.Payload.CostClass) {
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

// bucketSweepResult is what one sweep did: the runs it woke, and whether the bucket still has an eligible waiter.
type bucketSweepResult struct {
	woken   []string
	waiting bool
}

// bucketHeadsSQL lists the eligible waiters of one class of a bucket, oldest first: open runs of the org whose
// reference discovery succeeded, with a planned or due retrying unit in the bucket. Every eligibility filter is in the
// WHERE, none behind a LIMIT (CHAOS-14250 r1b P1-2); a run blocked before the guard or whose units are deferred into the
// future is not eligible. The bucket's waiting units come from ix_sync_run_units_bucket_status_lease (org, provider,
// cost class, status), the same read as the guard's older-run read, so other buckets' backlogs are never read.
const bucketHeadsSQL = `
WITH waiting AS MATERIALIZED (
    SELECT DISTINCT unit.sync_run_id
    FROM public.sync_run_units AS unit
    WHERE unit.org_id = $1 AND unit.provider = $2 AND unit.cost_class = $3
        AND (unit.status = 'planned' OR (unit.status = 'retrying' AND unit.available_at <= $5))
)
SELECT run.id::text
FROM waiting
JOIN public.sync_runs AS run ON run.id = waiting.sync_run_id
WHERE run.status NOT IN ('success', 'partial_failed', 'failed')
    AND run.org_id = $1
    AND (run.triggered_by = 'backfill' OR run.mode = 'backfill') = $4::boolean
    AND EXISTS (
        SELECT 1 FROM public.sync_run_reference_discoveries AS discovery
        WHERE discovery.sync_run_id = run.id AND discovery.status = 'success'
    )
ORDER BY run.created_at, run.id
LIMIT $6`

func readBucketHeads(ctx context.Context, tx pgx.Tx, bucket dispatchBucket, backfill bool, now time.Time, limit int) ([]string, error) {
	if limit <= 0 {
		return nil, nil
	}
	rows, err := tx.Query(ctx, bucketHeadsSQL, bucket.orgID, bucket.provider, bucket.costClass, backfill, now, limit)
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
		heads = append(heads, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: iterate bucket heads: %w", ErrDiscoveryTransientFailure, err)
	}
	return heads, nil
}

// markBucketSweepWokenSQL writes the sweep's mark into sync_runs.result of an open run, merged into the JSON object
// the column holds (a run whose result is not an object keeps it untouched and gets no mark). finalize overwrites
// result at the terminal write, so a finished run carries no mark.
const markBucketSweepWokenSQL = `
UPDATE public.sync_runs
SET result = (COALESCE(result::jsonb, '{}'::jsonb) || jsonb_build_object('` + bucketSweepWokenKey + `', $2::double precision))::json
WHERE id = $1::uuid
    AND status NOT IN ('success', 'partial_failed', 'failed')
    AND (result IS NULL OR jsonb_typeof(result::jsonb) = 'object')`

// runBucketSweep is one sweep of a bucket. Under the bucket's advisory lock (the guard's lock, so a pass and the sweep
// read one state) it counts the free slots and picks, oldest first, up to the backfill share (CHAOS-14211) of backfill
// waiters and the rest of the free slots of non-backfill waiters (the class rule of CHAOS-14250 D). After the commit,
// each picked run gets the mark and its dispatch wakeup, each as its own statement, so no other run's outbox row is
// held across a commit (acp 14265 condition 5). If a unit of the bucket ended during the sweep (its release found this
// sweep running, so it enqueued none), it sweeps once more; the runs it already woke hold their slots in that round.
func runBucketSweep(ctx context.Context, pool *pgxpool.Pool, bucket dispatchBucket, now func() time.Time) (bucketSweepResult, error) {
	var result bucketSweepResult
	for round := 0; round < 2; round++ {
		started := now()
		woken, waiting, err := sweepBucketOnce(ctx, pool, bucket, started, result.woken)
		if err != nil {
			return result, err
		}
		result.woken = append(result.woken, woken...)
		result.waiting = waiting
		for _, runID := range woken {
			if _, err := pool.Exec(ctx, markBucketSweepWokenSQL, runID, float64(started.UnixMicro())/1e6); err != nil {
				return result, fmt.Errorf("%w: mark swept run: %w", ErrDiscoveryTransientFailure, err)
			}
			syncrunrollup.WakeDispatch(ctx, pool, runID, started, syncrunrollup.DispatchWakePathBucketSweep)
		}
		released, err := slotReleasedSince(ctx, pool, bucket, started)
		if err != nil || !released {
			return result, err
		}
	}
	return result, nil
}

func sweepBucketOnce(ctx context.Context, pool *pgxpool.Pool, bucket dispatchBucket, now time.Time, already []string) ([]string, bool, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("%w: begin bucket sweep: %w", ErrDiscoveryTransientFailure, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := acquireBucketAdvisoryLocks(ctx, tx, []dispatchBucket{bucket}); err != nil {
		return nil, false, err
	}
	active, activeBackfill, err := countActiveBucketUnits(ctx, tx, bucket, now.Add(-staleDispatchSeconds()), now)
	if err != nil {
		return nil, false, err
	}
	capValue := concurrencyCapForCostClass(bucket.costClass)
	free := max(0, capValue-active-len(already))
	backfillUsable := free
	if capValue >= 2 {
		backfillUsable = min(free, max(0, capValue-1-activeBackfill))
	}
	pick := func(backfill bool, limit int) ([]string, error) {
		if limit <= 0 {
			return nil, nil
		}
		heads, err := readBucketHeads(ctx, tx, bucket, backfill, now, limit+len(already))
		if err != nil {
			return nil, err
		}
		heads = slices.DeleteFunc(heads, func(id string) bool { return slices.Contains(already, id) })
		return heads[:min(limit, len(heads))], nil
	}
	backfillHeads, err := pick(true, backfillUsable)
	if err != nil {
		return nil, false, err
	}
	otherHeads, err := pick(false, free-len(backfillHeads))
	if err != nil {
		return nil, false, err
	}
	woken := append(backfillHeads, otherHeads...)
	waiting := len(woken) > 0
	for _, backfill := range []bool{true, false} {
		if waiting {
			break
		}
		heads, err := readBucketHeads(ctx, tx, bucket, backfill, now, 1)
		if err != nil {
			return nil, false, err
		}
		waiting = len(heads) > 0
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, fmt.Errorf("%w: commit bucket sweep: %w", ErrDiscoveryTransientFailure, err)
	}
	return woken, waiting, nil
}

// slotReleasedSinceSQL is whether a unit of the bucket ended at or after since: the release that may have found this
// sweep running and so did not enqueue another (ix_sync_run_units_bucket_status_lease).
const slotReleasedSinceSQL = `
SELECT EXISTS (
    SELECT 1 FROM public.sync_run_units AS unit
    WHERE unit.org_id = $1 AND unit.provider = $2 AND unit.cost_class = $3
        AND unit.status IN ('success', 'failed') AND unit.updated_at >= $4
)`

func slotReleasedSince(ctx context.Context, pool *pgxpool.Pool, bucket dispatchBucket, since time.Time) (bool, error) {
	var released bool
	if err := pool.QueryRow(ctx, slotReleasedSinceSQL, bucket.orgID, bucket.provider, bucket.costClass, since).Scan(&released); err != nil {
		return false, fmt.Errorf("%w: read bucket releases: %w", ErrDiscoveryTransientFailure, err)
	}
	return released, nil
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

// Work runs one sweep and re-arms it with a snooze (no attempt is used) while the bucket has an eligible waiter.
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
