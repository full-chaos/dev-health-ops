//go:build integration

package joboutbox

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river/rivertype"
)

// This file stands up its own container and its own schema rather than
// extending the relay integration harness. That harness has no daily-metrics
// or work-graph tables and carries a role/grant setup that would have to be
// widened for every test in it, not just these; widening a shared fixture to
// admit one new reader is how a least-privilege posture quietly stops meaning
// anything.
const (
	strandQueueRole      = "strand_queue_runtime"
	strandDomainRole     = "strand_domain_runtime"
	strandQueuePassword  = "strand_queue_password"
	strandDomainPassword = "strand_domain_password"
)

type strandFixture struct {
	admin    *pgxpool.Pool
	relay    *Relay
	registry *jobruntime.Registry
	orgID    string
	// nextOutbox hands out distinct outbox ids. Deriving one from the domain
	// id or the dedupe key looks tidy and is wrong: the bounded-pass case
	// seeds five partitions whose keys are all the same length and whose ids
	// differ only in the last digits, so any such scheme collides on the
	// primary key and the test fails for a reason that has nothing to do with
	// the repair.
	nextOutbox int
}

func TestStrandRepairAgainstLivePostgres(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	}()

	admin := openIntegrationPool(t, ctx, instance.URI)
	defer admin.Close()
	createStrandSchema(t, ctx, admin)
	createStrandRoles(t, ctx, admin)
	if _, err := riverstore.ApplyPinnedMigrations(ctx, admin, riverstore.MigrationOptions{
		Schema:     "river",
		DomainRole: strandDomainRole,
		QueueRole:  strandQueueRole,
	}); err != nil {
		t.Fatal(err)
	}
	// NOTHING is granted by hand. Every privilege the repair needs comes from
	// ApplyPinnedMigrations, which is the authoritative path -- it REVOKEs ALL
	// on the queue role and re-grants an explicit list. Hand-granting here
	// would mask exactly the defect that shipped in the first draft: grants
	// that existed only in the provisioning script and were erased by this
	// migration before the reconciler ever started.

	queue := openIntegrationPool(t, ctx,
		integrationRoleURI(t, instance.URI, strandQueueRole, strandQueuePassword))
	defer queue.Close()
	domain := openIntegrationPool(t, ctx,
		integrationRoleURI(t, instance.URI, strandDomainRole, strandDomainPassword))
	defer domain.Close()

	// The pool split is a security boundary, not an implementation detail, so
	// it is asserted rather than assumed: the queue role must be unable to read
	// execution state even though the repair it powers depends on that state.
	if _, err := queue.Exec(ctx, "SELECT id FROM public.worker_job_runs"); err == nil {
		t.Fatal("the queue-control role can read worker_job_runs; the pool split is not real")
	}
	if _, err := domain.Exec(ctx, "SELECT id FROM public.worker_job_runs"); err != nil {
		t.Fatalf("the domain role cannot read worker_job_runs: %v", err)
	}

	registry, err := jobruntime.Load(filepath.Join("..", "..", "contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	repository, err := NewRepository(queue)
	if err != nil {
		t.Fatal(err)
	}
	inserter, err := NewRiverInserter(queue, "river", registry)
	if err != nil {
		t.Fatal(err)
	}
	relay, err := NewRelay(repository, inserter, DefaultRelayConfig())
	if err != nil {
		t.Fatal(err)
	}
	repair, err := NewStrandRepair(queue, domain, "river")
	if err != nil {
		t.Fatal(err)
	}
	fixture := &strandFixture{
		admin: admin, relay: relay, registry: registry, orgID: integrationUUID(7001),
	}

	t.Run("a stranded partition is rearmed and its dead delivery deleted", func(t *testing.T) {
		resetStrandTables(t, ctx, admin)
		now := time.Now().UTC().Truncate(time.Microsecond)
		partitionID := integrationUUID(1)
		runID := integrationUUID(2)
		seedDailyRun(t, ctx, admin, fixture.orgID, runID, "running", "pending", nil)
		seedDailyPartition(t, ctx, admin, fixture.orgID, partitionID, runID, "running", ptr(now.Add(-time.Hour)))
		outboxID := deliverDailyPartition(t, ctx, fixture, partitionID, now)
		jobID := riverJobFor(t, ctx, admin, outboxID)
		makeJobTerminal(t, ctx, admin, jobID, "completed", now.Add(-2*time.Hour))

		result, err := repair.Step(ctx, now, 10)
		if err != nil {
			t.Fatal(err)
		}
		if result.Rearmed != 1 || result.SkippedJobLive != 0 {
			t.Fatalf("Step() = %+v, want 1 rearmed and 0 skipped", result)
		}
		assertOutboxRearmed(t, ctx, admin, outboxID)
		if riverJobExists(t, ctx, admin, jobID) {
			t.Fatal("the dead River delivery was not deleted; an old job and a new attempt " +
				"can now be runnable together")
		}
	})

	// Adversarial constraint 1: a naive re-enqueue is a silent no-op, because
	// the producer publishes ON CONFLICT (dedupe_key) DO NOTHING and reports an
	// existing delivered row as success. The rearm must therefore reset the
	// ROW, so the ordinary relay mints a genuinely new delivery under the
	// unchanged key.
	t.Run("the rearmed row yields a fresh delivery under the same dedupe key", func(t *testing.T) {
		resetStrandTables(t, ctx, admin)
		now := time.Now().UTC().Truncate(time.Microsecond)
		partitionID := integrationUUID(3)
		runID := integrationUUID(4)
		seedDailyRun(t, ctx, admin, fixture.orgID, runID, "running", "pending", nil)
		seedDailyPartition(t, ctx, admin, fixture.orgID, partitionID, runID, "running", ptr(now.Add(-time.Hour)))
		outboxID := deliverDailyPartition(t, ctx, fixture, partitionID, now)
		firstJob := riverJobFor(t, ctx, admin, outboxID)
		makeJobTerminal(t, ctx, admin, firstJob, "completed", now.Add(-2*time.Hour))

		if _, err := repair.Step(ctx, now, 10); err != nil {
			t.Fatal(err)
		}
		if _, err := relay.Step(ctx, now.Add(time.Second), 10); err != nil {
			t.Fatal(err)
		}
		secondJob := riverJobFor(t, ctx, admin, outboxID)
		if secondJob == firstJob {
			t.Fatalf("the relay reused River job %d; the dedupe key defeated the rearm", firstJob)
		}
		var dedupeCount int
		if err := admin.QueryRow(ctx,
			"SELECT count(*) FROM public.worker_job_outbox WHERE dedupe_key = $1",
			"metrics.daily_partition:"+partitionID).Scan(&dedupeCount); err != nil {
			t.Fatal(err)
		}
		if dedupeCount != 1 {
			t.Fatalf("dedupe key rows = %d, want 1: the rearm must reset the row, not add one",
				dedupeCount)
		}
	})

	// The structural Stale exclusion (CHAOS-4025). A job River may still
	// rescue is non-terminal by definition, so it is refused -- and the
	// refusal is COUNTED, because a skip count climbing while nothing is
	// rearmed is the signature of a rescuer that has stopped running.
	t.Run("a delivery River may still run is skipped and counted", func(t *testing.T) {
		for _, state := range []string{"running", "available", "retryable", "scheduled"} {
			t.Run(state, func(t *testing.T) {
				resetStrandTables(t, ctx, admin)
				now := time.Now().UTC().Truncate(time.Microsecond)
				partitionID := integrationUUID(5)
				runID := integrationUUID(6)
				seedDailyRun(t, ctx, admin, fixture.orgID, runID, "running", "pending", nil)
				seedDailyPartition(t, ctx, admin, fixture.orgID, partitionID, runID, "running", ptr(now.Add(-time.Hour)))
				outboxID := deliverDailyPartition(t, ctx, fixture, partitionID, now)
				jobID := riverJobFor(t, ctx, admin, outboxID)
				setJobState(t, ctx, admin, jobID, state)

				result, err := repair.Step(ctx, now, 10)
				if err != nil {
					t.Fatal(err)
				}
				if result.Rearmed != 0 || result.SkippedJobLive != 1 ||
					result.SkippedClaimLive != 0 {
					t.Fatalf("Step() = %+v, want 0 rearmed and exactly 1 live-job skip", result)
				}
				assertOutboxStillDelivered(t, ctx, admin, outboxID, jobID)
				if !riverJobExists(t, ctx, admin, jobID) {
					t.Fatalf("a %s job was deleted; River was still going to run it", state)
				}
			})
		}
	})

	// The lease-proxy refusal. A live domain lease stands in for a live
	// idempotency lease, so a row whose owner has just renewed must not be
	// touched.
	t.Run("a live domain lease is refused", func(t *testing.T) {
		resetStrandTables(t, ctx, admin)
		now := time.Now().UTC().Truncate(time.Microsecond)
		partitionID := integrationUUID(7)
		runID := integrationUUID(8)
		seedDailyRun(t, ctx, admin, fixture.orgID, runID, "running", "pending", nil)
		seedDailyPartition(t, ctx, admin, fixture.orgID, partitionID, runID, "running", ptr(now.Add(5*time.Minute)))
		outboxID := deliverDailyPartition(t, ctx, fixture, partitionID, now)
		makeJobTerminal(t, ctx, admin, riverJobFor(t, ctx, admin, outboxID), "completed", now.Add(-2*time.Hour))

		result, err := repair.Step(ctx, now, 10)
		if err != nil {
			t.Fatal(err)
		}
		if result.Rearmed != 0 || result.SkippedJobLive != 0 {
			t.Fatalf("Step() = %+v, want an untouched row", result)
		}
	})

	// The execution-state check, which replaced the lease proxy. These three
	// cases are the whole reason the repair reads worker_job_runs on a second
	// pool: the proxy could not distinguish them, and two of the three end in a
	// manufactured strand if rearmed.
	t.Run("idempotency claim state decides eligibility", func(t *testing.T) {
		for _, testCase := range []struct {
			name        string
			status      string
			leaseOffset time.Duration
			hasLease    bool
			wantRearmed int
			wantLive    int
			wantSettled int
		}{
			{
				// A live claim is the dangerous one. Begin returns
				// ClaimAlreadyComplete, so a rearmed job is ACKed without ever
				// running and the domain row stays unfinished (CHAOS-3998).
				name: "a live claim is refused", status: "running",
				leaseOffset: 5 * time.Minute, hasLease: true, wantLive: 1,
			},
			{
				// A settled claim is equally futile to rearm, and the lease
				// proxy could not see this case at all -- a settled row has no
				// lease to reason about.
				name: "a succeeded claim is refused", status: "succeeded", wantSettled: 1,
			},
			{
				name: "a terminal claim is refused", status: "terminal", wantSettled: 1,
			},
			{
				// An expired claim lease is what the stranded production rows
				// actually look like: Begin takes the row over and the handler
				// runs.
				name: "an expired claim is reclaimed", status: "running",
				leaseOffset: -time.Hour, hasLease: true, wantRearmed: 1,
			},
			{
				name: "a retryable claim is reclaimed", status: "retryable", wantRearmed: 1,
			},
		} {
			t.Run(testCase.name, func(t *testing.T) {
				resetStrandTables(t, ctx, admin)
				now := time.Now().UTC().Truncate(time.Microsecond)
				partitionID := integrationUUID(70)
				runID := integrationUUID(71)
				seedDailyRun(t, ctx, admin, fixture.orgID, runID, "running", "pending", nil)
				seedDailyPartition(t, ctx, admin, fixture.orgID, partitionID, runID, "running", ptr(now.Add(-time.Hour)))
				outboxID := deliverDailyPartition(t, ctx, fixture, partitionID, now)
				makeJobTerminal(t, ctx, admin, riverJobFor(t, ctx, admin, outboxID), "completed", now.Add(-2*time.Hour))

				var lease *time.Time
				if testCase.hasLease {
					lease = ptr(now.Add(testCase.leaseOffset))
				}
				seedJobRun(t, ctx, admin, "metrics.daily_partition",
					"metrics.daily_partition:"+partitionID, fixture.orgID, partitionID,
					testCase.status, lease)

				result, err := repair.Step(ctx, now, 10)
				if err != nil {
					t.Fatal(err)
				}
				if result.Rearmed != testCase.wantRearmed ||
					result.SkippedClaimLive != testCase.wantLive ||
					result.SkippedClaimSettled != testCase.wantSettled {
					t.Fatalf("Step() = %+v, want %d rearmed / %d live / %d settled", result,
						testCase.wantRearmed, testCase.wantLive, testCase.wantSettled)
				}
			})
		}
	})

	// A candidate with NO claim row at all is safe: the first delivery creates
	// it. This is the control proving the cases above turn on the claim row and
	// not on some other difference in the fixture.
	t.Run("a candidate with no claim row is rearmed", func(t *testing.T) {
		resetStrandTables(t, ctx, admin)
		now := time.Now().UTC().Truncate(time.Microsecond)
		partitionID := integrationUUID(72)
		runID := integrationUUID(73)
		seedDailyRun(t, ctx, admin, fixture.orgID, runID, "running", "pending", nil)
		seedDailyPartition(t, ctx, admin, fixture.orgID, partitionID, runID, "running", ptr(now.Add(-time.Hour)))
		outboxID := deliverDailyPartition(t, ctx, fixture, partitionID, now)
		makeJobTerminal(t, ctx, admin, riverJobFor(t, ctx, admin, outboxID), "completed", now.Add(-2*time.Hour))

		result, err := repair.Step(ctx, now, 10)
		if err != nil {
			t.Fatal(err)
		}
		if result.Rearmed != 1 || result.SkippedClaimLive != 0 || result.SkippedClaimSettled != 0 {
			t.Fatalf("Step() = %+v, want 1 rearmed with no claim refusals", result)
		}
	})

	// Adversarial constraint 4: never re-drive a finalizer while the partition
	// layer still owns the run. ClaimFinalize would refuse it, so the job
	// could only no-op.
	t.Run("a finalizer is refused while a sibling partition is unfinished", func(t *testing.T) {
		resetStrandTables(t, ctx, admin)
		now := time.Now().UTC().Truncate(time.Microsecond)
		runID := integrationUUID(10)
		seedDailyRun(t, ctx, admin, fixture.orgID, runID, "running", "running", ptr(now.Add(-time.Hour)))
		seedDailyPartition(t, ctx, admin, fixture.orgID, integrationUUID(11), runID, "succeeded", nil)
		seedDailyPartition(t, ctx, admin, fixture.orgID, integrationUUID(12), runID, "running", ptr(now.Add(-time.Hour)))
		outboxID := deliverDailyFinalize(t, ctx, fixture, runID, now)
		makeJobTerminal(t, ctx, admin, riverJobFor(t, ctx, admin, outboxID), "completed", now.Add(-2*time.Hour))

		result, err := repair.Step(ctx, now, 10)
		if err != nil {
			t.Fatal(err)
		}
		if result.Rearmed != 0 {
			t.Fatalf("Step() = %+v, want the finalizer left to the partition layer", result)
		}

		// Once every sibling has succeeded the same row becomes eligible.
		if _, err := admin.Exec(ctx,
			"UPDATE public.daily_metrics_partitions SET status='succeeded', lease_expires_at=NULL WHERE run_id=$1",
			runID); err != nil {
			t.Fatal(err)
		}
		result, err = repair.Step(ctx, now, 10)
		if err != nil {
			t.Fatal(err)
		}
		if result.Rearmed != 1 {
			t.Fatalf("Step() = %+v, want 1 rearmed once all partitions succeeded", result)
		}
	})

	// The finalize branches that production can actually reach. Migration 0057
	// constrains daily_metrics_runs so that a 'running' finalization ALWAYS
	// carries a token and lease, and 'pending'/'failed' never carry either --
	// so the reachable cases are exactly these three. The predicate still
	// mirrors classifyLease's non-NULL requirement defensively, but this suite
	// no longer claims to test a row the schema forbids.
	t.Run("finalize eligibility follows ClaimFinalize", func(t *testing.T) {
		for _, testCase := range []struct {
			name         string
			finalization string
			hasLease     bool
			leaseOffset  time.Duration
			wantRearmed  int
		}{
			{
				name: "running with an expired lease is reclaimed", finalization: "running",
				hasLease: true, leaseOffset: -time.Hour, wantRearmed: 1,
			},
			{
				name: "running with a live lease is refused", finalization: "running",
				hasLease: true, leaseOffset: 10 * time.Minute, wantRearmed: 0,
			},
			{
				// Reachable and previously UNTESTED: deleting the
				// pending/failed branch would have passed the old suite.
				name: "pending is reclaimed", finalization: "pending", wantRearmed: 1,
			},
			{
				name: "failed is reclaimed", finalization: "failed", wantRearmed: 1,
			},
			{
				name: "succeeded is refused", finalization: "succeeded", wantRearmed: 0,
			},
		} {
			t.Run(testCase.name, func(t *testing.T) {
				resetStrandTables(t, ctx, admin)
				now := time.Now().UTC().Truncate(time.Microsecond)
				runID := integrationUUID(80)
				var lease *time.Time
				if testCase.hasLease {
					lease = ptr(now.Add(testCase.leaseOffset))
				}
				seedDailyRun(t, ctx, admin, fixture.orgID, runID, "running", testCase.finalization, lease)
				outboxID := deliverDailyFinalize(t, ctx, fixture, runID, now)
				makeJobTerminal(t, ctx, admin, riverJobFor(t, ctx, admin, outboxID), "completed", now.Add(-2*time.Hour))

				result, err := repair.Step(ctx, now, 10)
				if err != nil {
					t.Fatal(err)
				}
				if result.Rearmed != testCase.wantRearmed {
					t.Fatalf("Step() = %+v, want %d rearmed", result, testCase.wantRearmed)
				}
			})
		}
	})

	// codex review 2026-08-20: the domain joins carried no organization
	// predicate, so a contract-valid envelope naming another tenant's UUID
	// could be rearmed across the tenant boundary.
	t.Run("a domain row belonging to another organization does not match", func(t *testing.T) {
		resetStrandTables(t, ctx, admin)
		now := time.Now().UTC().Truncate(time.Microsecond)
		partitionID := integrationUUID(60)
		runID := integrationUUID(61)
		otherOrg := integrationUUID(7002)
		// The domain rows belong to a DIFFERENT organization than the envelope
		// the outbox row carries.
		seedDailyRun(t, ctx, admin, otherOrg, runID, "running", "pending", nil)
		seedDailyPartition(t, ctx, admin, otherOrg, partitionID, runID, "running", ptr(now.Add(-time.Hour)))
		outboxID := deliverDailyPartition(t, ctx, fixture, partitionID, now)
		makeJobTerminal(t, ctx, admin, riverJobFor(t, ctx, admin, outboxID), "completed", now.Add(-2*time.Hour))

		result, err := repair.Step(ctx, now, 10)
		if err != nil {
			t.Fatal(err)
		}
		if result.Rearmed != 0 {
			t.Fatalf("Step() = %+v, want no cross-tenant rearm", result)
		}
		assertOutboxStillDelivered(t, ctx, admin, outboxID, riverJobFor(t, ctx, admin, outboxID))
	})

	t.Run("work graph requests", func(t *testing.T) {
		for _, testCase := range []struct {
			name         string
			state        string
			leaseExpires *time.Time
			wantRearmed  int
		}{
			{"running with an expired lease is rearmed", "running", ptr(time.Now().UTC().Add(-time.Hour)), 1},
			{"pending and never claimed is rearmed", "pending", nil, 1},
			{"a live lease is refused", "running", ptr(time.Now().UTC().Add(10 * time.Minute)), 0},
			// CHAOS-3999 owns the abandonment contract for ambiguous heads,
			// and Claim refuses them anyway, so a rearm could only mint a
			// no-op job.
			{"ambiguous is left to the abandonment contract", "ambiguous", nil, 0},
			{"succeeded is refused", "succeeded", nil, 0},
			{"failed is refused", "failed", nil, 0},
			{"canceled is refused", "canceled", nil, 0},
		} {
			t.Run(testCase.name, func(t *testing.T) {
				resetStrandTables(t, ctx, admin)
				now := time.Now().UTC().Truncate(time.Microsecond)
				requestID := integrationUUID(20)
				seedWorkGraphRequest(t, ctx, admin, fixture.orgID, requestID,
					jobcontract.KindWorkGraphBuild, testCase.state, testCase.leaseExpires)
				outboxID := deliverWorkGraphBuild(t, ctx, fixture, requestID, now)
				makeJobTerminal(t, ctx, admin, riverJobFor(t, ctx, admin, outboxID), "completed", now.Add(-2*time.Hour))

				result, err := repair.Step(ctx, now, 10)
				if err != nil {
					t.Fatal(err)
				}
				if result.Rearmed != testCase.wantRearmed {
					t.Fatalf("Step() = %+v, want %d rearmed", result, testCase.wantRearmed)
				}
			})
		}
	})

	// A request row is bound by kind as well as id, exactly as
	// PostgresStore.Claim keys it. Matching on id alone would let a strand in
	// one kind rearm the delivery of another.
	t.Run("a request of a different kind does not match", func(t *testing.T) {
		resetStrandTables(t, ctx, admin)
		now := time.Now().UTC().Truncate(time.Microsecond)
		requestID := integrationUUID(21)
		seedWorkGraphRequest(t, ctx, admin, fixture.orgID, requestID,
			jobcontract.KindInvestmentMaterialize, "pending", nil)
		outboxID := deliverWorkGraphBuild(t, ctx, fixture, requestID, now)
		makeJobTerminal(t, ctx, admin, riverJobFor(t, ctx, admin, outboxID), "completed", now.Add(-2*time.Hour))

		result, err := repair.Step(ctx, now, 10)
		if err != nil {
			t.Fatal(err)
		}
		if result.Rearmed != 0 {
			t.Fatalf("Step() = %+v, want no match across kinds", result)
		}
	})

	t.Run("a pass is bounded and takes the oldest deliveries first", func(t *testing.T) {
		resetStrandTables(t, ctx, admin)
		now := time.Now().UTC().Truncate(time.Microsecond)
		runID := integrationUUID(30)
		seedDailyRun(t, ctx, admin, fixture.orgID, runID, "running", "pending", nil)
		ordered := make([]string, 0, 5)
		for index := range 5 {
			partitionID := integrationUUID(31 + index)
			seedDailyPartition(t, ctx, admin, fixture.orgID, partitionID, runID, "running", ptr(now.Add(-time.Hour)))
			outboxID := deliverDailyPartition(t, ctx, fixture, partitionID, now)
			makeJobTerminal(t, ctx, admin, riverJobFor(t, ctx, admin, outboxID), "completed", now.Add(-2*time.Hour))
			// Stagger delivered_at so oldest-first is observable at all.
			if _, err := admin.Exec(ctx,
				"UPDATE public.worker_job_outbox SET delivered_at=$2 WHERE id=$1",
				outboxID, now.Add(time.Duration(index)*time.Minute)); err != nil {
				t.Fatal(err)
			}
			ordered = append(ordered, outboxID)
		}
		result, err := repair.Step(ctx, now, 2)
		if err != nil {
			t.Fatal(err)
		}
		if result.Rearmed != 2 {
			t.Fatalf("Step(limit=2) = %+v, want exactly 2 rearmed", result)
		}
		// codex review 2026-08-20: asserting only the COUNT was tautological --
		// reversing ORDER BY and rearming the two NEWEST would have passed.
		// Assert WHICH rows moved.
		for index, outboxID := range ordered {
			var status string
			if err := admin.QueryRow(ctx,
				"SELECT status FROM public.worker_job_outbox WHERE id=$1", outboxID).Scan(&status); err != nil {
				t.Fatal(err)
			}
			want := "delivered"
			if index < 2 {
				want = "pending"
			}
			if status != want {
				t.Fatalf("outbox row %d (delivered_at +%dm) is %q, want %q: the pass did not take "+
					"the oldest deliveries first", index, index, status, want)
			}
		}
	})

	// The post-delete re-check. Between the predicate that selected a terminal
	// job and the delete that removes it, River could in principle move that
	// job back to a runnable state; deleting it then would destroy work that
	// was about to run. The window is too narrow to hit by scheduling, so it
	// is driven directly: the real query and the real transaction are used,
	// and only the delete is replaced by one that reports a job which is no
	// longer terminal.
	t.Run("a delivery that turned runnable before the delete is not removed", func(t *testing.T) {
		resetStrandTables(t, ctx, admin)
		now := time.Now().UTC().Truncate(time.Microsecond)
		partitionID := integrationUUID(40)
		runID := integrationUUID(41)
		seedDailyRun(t, ctx, admin, fixture.orgID, runID, "running", "pending", nil)
		seedDailyPartition(t, ctx, admin, fixture.orgID, partitionID, runID, "running", ptr(now.Add(-time.Hour)))
		outboxID := deliverDailyPartition(t, ctx, fixture, partitionID, now)
		jobID := riverJobFor(t, ctx, admin, outboxID)
		makeJobTerminal(t, ctx, admin, jobID, "completed", now.Add(-2*time.Hour))

		racing, err := NewStrandRepair(queue, domain, "river")
		if err != nil {
			t.Fatal(err)
		}
		racing.client = resurrectingDelete{id: jobID}
		if _, err := racing.Step(ctx, now, 10); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("Step() error = %v, want ErrUnavailable when the deleted job was not terminal", err)
		}
		assertOutboxStillDelivered(t, ctx, admin, outboxID, jobID)
		if !riverJobExists(t, ctx, admin, jobID) {
			t.Fatal("the transaction was committed after a non-terminal delete")
		}

		// Control: the same repair with the real delete does rearm the row, so
		// the refusal above is the re-check firing and not the fixture simply
		// failing to match the predicate.
		control, err := NewStrandRepair(queue, domain, "river")
		if err != nil {
			t.Fatal(err)
		}
		result, err := control.Step(ctx, now, 10)
		if err != nil || result.Rearmed != 1 {
			t.Fatalf("control Step() = %+v, %v; want 1 rearmed", result, err)
		}
	})

	// The blocker, made executable. Without the CHAOS-3997 grants the repair
	// cannot read the domain row, and the operator must be able to tell that
	// from a database outage.
	t.Run("a missing grant reports itself rather than looking unavailable", func(t *testing.T) {
		resetStrandTables(t, ctx, admin)
		now := time.Now().UTC().Truncate(time.Microsecond)
		if _, err := admin.Exec(ctx,
			"REVOKE SELECT ON TABLE public.daily_metrics_partitions FROM "+strandQueueRole); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if _, err := admin.Exec(ctx,
				"GRANT SELECT ON TABLE public.daily_metrics_partitions TO "+strandQueueRole); err != nil {
				t.Fatal(err)
			}
		}()
		_, err := repair.Step(ctx, now, 10)
		if !errors.Is(err, ErrNotAuthorized) {
			t.Fatalf("Step() error = %v, want ErrNotAuthorized", err)
		}
		if errors.Is(err, ErrUnavailable) {
			t.Fatal("a denied statement must not be reported as an unavailable database")
		}
	})
}

func ptr(value time.Time) *time.Time { return &value }

// seedJobRun writes the execution-state row the repair reads on the domain
// pool. Keyed on (job_kind, idempotency_key) exactly as PostgresIdempotency
// keys it, where idempotency_key is the outbox dedupe_key.
func seedJobRun(
	t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	jobKind, idempotencyKey, orgID, domainID, status string, leaseExpires *time.Time,
) {
	t.Helper()
	var claimToken *string
	if leaseExpires != nil {
		token := integrationUUID(9500)
		claimToken = &token
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.worker_job_runs (
			id, job_kind, idempotency_key, org_id, domain_type, domain_id,
			status, claim_token, lease_expires_at
		) VALUES ($1, $2, $3, $4, 'daily_metrics_partition', $5, $6, $7, $8)`,
		integrationUUID(9600), jobKind, idempotencyKey, orgID, domainID,
		status, claimToken, leaseExpires); err != nil {
		t.Fatal(err)
	}
}

// resurrectingDelete reports a successful delete of a job that is no longer
// terminal, which is what River would report if the job had been made runnable
// again between the predicate and the delete.
type resurrectingDelete struct{ id int64 }

func (stub resurrectingDelete) JobDeleteTx(context.Context, pgx.Tx, int64) (*rivertype.JobRow, error) {
	return &rivertype.JobRow{ID: stub.id, State: rivertype.JobStateAvailable}, nil
}

func createStrandRoles(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	for _, statement := range []string{
		"CREATE ROLE " + strandDomainRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + strandDomainPassword + "'",
		"CREATE ROLE " + strandQueueRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + strandQueuePassword + "'",
	} {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
}

func createStrandSchema(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	// daily_metrics_* and work_graph_execution_requests carry only the columns
	// the repair's predicates read, plus the constraints that make an invalid
	// fixture impossible to write -- notably the work-graph CHECK that forces a
	// non-running request to hold a NULL claim token and lease, which is what
	// makes the "pending" row unable to carry a lease at all.
	_, err := pool.Exec(ctx, `
		CREATE TABLE public.daily_metrics_runs (
			id uuid PRIMARY KEY,
			org_id uuid NOT NULL,
			status text NOT NULL,
			finalization_status text NOT NULL DEFAULT 'pending',
			finalization_claim_token uuid NULL,
			finalization_lease_expires_at timestamptz NULL,
			finalized_at timestamptz NULL,
			-- Copied verbatim from alembic 0057. Production forbids a 'running'
			-- finalization without a token AND lease, and forbids any other
			-- status from holding either. Without this constraint the fixture
			-- can express rows production cannot, and a test built on an
			-- impossible row proves nothing about production.
			CONSTRAINT daily_metrics_runs_finalization_claim CHECK (
				(finalization_status = 'running' AND finalization_claim_token IS NOT NULL
					AND finalization_lease_expires_at IS NOT NULL)
				OR (finalization_status <> 'running' AND finalization_claim_token IS NULL
					AND finalization_lease_expires_at IS NULL)
			)
		);
		CREATE TABLE public.daily_metrics_partitions (
			id uuid PRIMARY KEY,
			org_id uuid NOT NULL,
			run_id uuid NOT NULL REFERENCES public.daily_metrics_runs(id),
			status text NOT NULL,
			claim_token uuid NULL,
			lease_expires_at timestamptz NULL,
			attempt_count integer NOT NULL DEFAULT 0
		);
		CREATE TABLE public.work_graph_execution_requests (
			id uuid PRIMARY KEY,
			org_id uuid NOT NULL,
			kind text NOT NULL CHECK (kind IN (
				'workgraph.build', 'investment.materialize', 'investment.dispatch',
				'investment.chunk', 'investment.finalize'
			)),
			state text NOT NULL DEFAULT 'pending' CHECK (state IN (
				'pending', 'running', 'succeeded', 'failed', 'ambiguous', 'canceled'
			)),
			claim_token uuid NULL,
			lease_expires_at timestamptz NULL,
			attempt_count integer NOT NULL DEFAULT 0,
			CHECK ((state = 'running' AND claim_token IS NOT NULL AND lease_expires_at IS NOT NULL)
				OR (state <> 'running' AND claim_token IS NULL AND lease_expires_at IS NULL))
		);
		CREATE TABLE public.worker_job_outbox (
			id uuid PRIMARY KEY,
			dedupe_key varchar(256) NOT NULL UNIQUE,
			job_kind varchar(96) NOT NULL,
			contract_version integer NOT NULL,
			args json NOT NULL,
			payload_hash varchar(71) NOT NULL,
			queue varchar(96) NOT NULL,
			priority smallint NOT NULL,
			max_attempts smallint NOT NULL,
			scheduled_at timestamptz NOT NULL,
			status varchar(16) NOT NULL,
			claim_token uuid,
			claimed_at timestamptz,
			claim_expires_at timestamptz,
			attempt_count integer NOT NULL,
			first_attempt_at timestamptz,
			last_attempt_at timestamptz,
			next_attempt_at timestamptz NOT NULL,
			last_error_code varchar(64),
			last_error_detail varchar(256),
			last_error_at timestamptz,
			river_job_id bigint UNIQUE,
			delivered_at timestamptz,
			prerequisite_completion_key text NULL,
			created_at timestamptz NOT NULL,
			updated_at timestamptz NOT NULL,
			CONSTRAINT worker_job_outbox_status CHECK (status IN ('pending','claimed','delivered','dead')),
			CONSTRAINT worker_job_outbox_claim CHECK (
				(status='claimed' AND claim_token IS NOT NULL AND claimed_at IS NOT NULL AND claim_expires_at IS NOT NULL)
				OR (status<>'claimed' AND claim_token IS NULL AND claimed_at IS NULL AND claim_expires_at IS NULL)
			),
			CONSTRAINT worker_job_outbox_delivery CHECK (
				(status='delivered' AND river_job_id IS NOT NULL AND delivered_at IS NOT NULL)
				OR (status<>'delivered' AND river_job_id IS NULL AND delivered_at IS NULL)
			)
		);
		CREATE TABLE public.worker_job_completion_fences (
			completion_key text PRIMARY KEY,
			completed_at timestamptz NOT NULL DEFAULT statement_timestamp()
		);
		CREATE TABLE public.worker_job_runs (
			id uuid PRIMARY KEY,
			job_kind text NOT NULL,
			idempotency_key text NOT NULL,
			org_id uuid NULL,
			domain_type text NOT NULL,
			domain_id uuid NOT NULL,
			status text NOT NULL,
			claim_token uuid NULL,
			lease_expires_at timestamptz NULL,
			attempt_count integer NOT NULL DEFAULT 1,
			UNIQUE (job_kind, idempotency_key)
		)`)
	if err != nil {
		t.Fatal(err)
	}
}

func resetStrandTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `TRUNCATE
		public.worker_job_outbox, public.worker_job_completion_fences,
		public.daily_metrics_partitions, public.daily_metrics_runs,
		public.work_graph_execution_requests, public.worker_job_runs,
		river.river_job RESTART IDENTITY`); err != nil {
		t.Fatal(err)
	}
}

func seedDailyRun(
	t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	orgID, runID, status, finalizationStatus string, finalizationLease *time.Time,
) {
	t.Helper()
	var claimToken *string
	if finalizationLease != nil {
		token := integrationUUID(9700)
		claimToken = &token
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.daily_metrics_runs (
			id, org_id, status, finalization_status,
			finalization_claim_token, finalization_lease_expires_at
		) VALUES ($1, $2, $3, $4, $5, $6)`,
		runID, orgID, status, finalizationStatus, claimToken, finalizationLease); err != nil {
		t.Fatal(err)
	}
}

func seedDailyPartition(
	t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	orgID, partitionID, runID, status string, leaseExpires *time.Time,
) {
	t.Helper()
	var claimToken *string
	if leaseExpires != nil {
		token := integrationUUID(9000)
		claimToken = &token
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.daily_metrics_partitions (id, org_id, run_id, status, claim_token, lease_expires_at)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		partitionID, orgID, runID, status, claimToken, leaseExpires); err != nil {
		t.Fatal(err)
	}
}

func seedWorkGraphRequest(
	t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	orgID, requestID, kind, state string, leaseExpires *time.Time,
) {
	t.Helper()
	var claimToken *string
	if state == "running" {
		token := integrationUUID(9001)
		claimToken = &token
	} else {
		leaseExpires = nil
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.work_graph_execution_requests (id, org_id, kind, state, claim_token, lease_expires_at)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		requestID, orgID, kind, state, claimToken, leaseExpires); err != nil {
		t.Fatal(err)
	}
}

// deliverDailyPartition seeds a pending outbox row and drives the real relay
// until it is delivered, so every fixture starts from a delivery the
// production path actually produced rather than one hand-written to match the
// predicate.
func deliverDailyPartition(t *testing.T, ctx context.Context, fixture *strandFixture, partitionID string, now time.Time) string {
	t.Helper()
	return deliverStrandSeed(t, ctx, fixture, now, jobcontract.KindDailyMetricsPartition,
		"metrics.daily_partition:"+partitionID, "daily_metrics_partition", partitionID,
		jobcontract.DailyMetricsPartitionPayload{PartitionID: partitionID})
}

func deliverDailyFinalize(t *testing.T, ctx context.Context, fixture *strandFixture, runID string, now time.Time) string {
	t.Helper()
	return deliverStrandSeed(t, ctx, fixture, now, jobcontract.KindDailyMetricsFinalize,
		"metrics.daily_finalize:"+runID, "daily_metrics_run", runID,
		jobcontract.DailyMetricsFinalizePayload{RunID: runID})
}

func deliverWorkGraphBuild(t *testing.T, ctx context.Context, fixture *strandFixture, requestID string, now time.Time) string {
	t.Helper()
	return deliverStrandSeed(t, ctx, fixture, now, jobcontract.KindWorkGraphBuild,
		"workgraph.build:"+requestID, "work_graph_request", requestID,
		jobcontract.WorkGraphBuildPayload{RequestID: requestID})
}

func deliverStrandSeed(
	t *testing.T, ctx context.Context, fixture *strandFixture, now time.Time,
	kind, dedupeKey, domainType, domainID string, payload any,
) string {
	t.Helper()
	envelope := jobcontract.Envelope{
		ContractVersion: 1,
		CorrelationID:   "strand-integration-" + domainID,
		IdempotencyKey:  dedupeKey,
		OrganizationID:  &fixture.orgID,
		Domain:          jobcontract.DomainLink{Type: domainType, ID: domainID},
		Payload:         payload,
	}
	args, err := jobcontract.MarshalCanonical(envelope)
	if err != nil {
		t.Fatal(err)
	}
	fixture.nextOutbox++
	outboxID := integrationUUID(5000 + fixture.nextOutbox)
	// Queue, priority and max attempts are read from the registry rather than
	// written by hand. The inserter rejects a stored row whose policy differs
	// from its descriptor by even one field, and it does so as a TERMINAL
	// 'policy_rejected' -- so a hand-picked value does not fail loudly, it
	// quietly marks the fixture row dead and leaves the assertions below
	// testing nothing.
	descriptor, ok := fixture.registry.Descriptor(kind)
	if !ok {
		t.Fatalf("no registry descriptor for %s", kind)
	}
	seedOutbox(t, ctx, fixture.admin, outboxSeed{
		ID:          outboxID,
		DedupeKey:   dedupeKey,
		Kind:        kind,
		Version:     1,
		Args:        args,
		PayloadHash: canonicalHash(args),
		Queue:       descriptor.Queue,
		Priority:    descriptor.Priority,
		MaxAttempts: descriptor.MaxAttempts,
		ScheduledAt: now.Add(-time.Minute),
	})
	if _, err := fixture.relay.Step(ctx, now, 10); err != nil {
		t.Fatal(err)
	}
	var status, errorCode, errorDetail string
	if err := fixture.admin.QueryRow(ctx, `
		SELECT status, coalesce(last_error_code, ''), coalesce(last_error_detail, '')
		FROM public.worker_job_outbox WHERE id=$1`, outboxID).
		Scan(&status, &errorCode, &errorDetail); err != nil {
		t.Fatal(err)
	}
	if status != "delivered" {
		t.Fatalf("fixture outbox row is %q (%s: %s), want delivered: the relay did not produce a "+
			"delivery and every assertion below would be vacuous", status, errorCode, errorDetail)
	}
	return outboxID
}

func riverJobFor(t *testing.T, ctx context.Context, pool *pgxpool.Pool, outboxID string) int64 {
	t.Helper()
	var jobID *int64
	if err := pool.QueryRow(ctx,
		"SELECT river_job_id FROM public.worker_job_outbox WHERE id=$1", outboxID).Scan(&jobID); err != nil {
		t.Fatal(err)
	}
	if jobID == nil {
		t.Fatal("outbox row carries no River delivery")
	}
	return *jobID
}

func makeJobTerminal(t *testing.T, ctx context.Context, pool *pgxpool.Pool, jobID int64, state string, finalizedAt time.Time) {
	t.Helper()
	command, err := pool.Exec(ctx,
		"UPDATE river.river_job SET state=$2::river.river_job_state, finalized_at=$3 WHERE id=$1",
		jobID, state, finalizedAt)
	if err != nil {
		t.Fatal(err)
	}
	if command.RowsAffected() != 1 {
		t.Fatalf("no River job %d to make terminal", jobID)
	}
}

func setJobState(t *testing.T, ctx context.Context, pool *pgxpool.Pool, jobID int64, state string) {
	t.Helper()
	command, err := pool.Exec(ctx,
		"UPDATE river.river_job SET state=$2::river.river_job_state, finalized_at=NULL WHERE id=$1",
		jobID, state)
	if err != nil {
		t.Fatal(err)
	}
	if command.RowsAffected() != 1 {
		t.Fatalf("no River job %d to restate", jobID)
	}
}

func riverJobExists(t *testing.T, ctx context.Context, pool *pgxpool.Pool, jobID int64) bool {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM river.river_job WHERE id=$1", jobID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count > 0
}

func assertOutboxRearmed(t *testing.T, ctx context.Context, pool *pgxpool.Pool, outboxID string) {
	t.Helper()
	var status, errorCode string
	var riverJobID *int64
	var deliveredAt, claimExpiresAt *time.Time
	if err := pool.QueryRow(ctx, `
		SELECT status, coalesce(last_error_code, ''), river_job_id, delivered_at, claim_expires_at
		FROM public.worker_job_outbox WHERE id=$1`, outboxID).
		Scan(&status, &errorCode, &riverJobID, &deliveredAt, &claimExpiresAt); err != nil {
		t.Fatal(err)
	}
	if status != "pending" || riverJobID != nil || deliveredAt != nil || claimExpiresAt != nil {
		t.Fatalf("rearmed row = status %q, river_job_id %v, delivered_at %v, claim_expires_at %v",
			status, riverJobID, deliveredAt, claimExpiresAt)
	}
	if errorCode != strandRecoveryCode {
		t.Fatalf("rearmed row error code = %q, want %q", errorCode, strandRecoveryCode)
	}
}

func assertOutboxStillDelivered(t *testing.T, ctx context.Context, pool *pgxpool.Pool, outboxID string, jobID int64) {
	t.Helper()
	var status string
	var riverJobID *int64
	if err := pool.QueryRow(ctx,
		"SELECT status, river_job_id FROM public.worker_job_outbox WHERE id=$1", outboxID).
		Scan(&status, &riverJobID); err != nil {
		t.Fatal(err)
	}
	if status != "delivered" || riverJobID == nil || *riverJobID != jobID {
		t.Fatalf("row = status %q, river_job_id %v; want an untouched delivery of job %d",
			status, riverJobID, jobID)
	}
}
