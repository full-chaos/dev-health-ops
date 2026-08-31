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
	// CREATE ROLE is cluster-scoped, not database-scoped -- a scratch
	// database does not isolate it (CHAOS-4661). Deriving both role names
	// from this call's own database identity is what makes two successive
	// runs, and two concurrent lanes, collision-free.
	strandDomainRole, err := containers.RoleName("strand_domain_runtime", instance)
	if err != nil {
		t.Fatal(err)
	}
	defer containers.DropRole(admin, strandDomainRole, t.Logf)
	strandQueueRole, err := containers.RoleName("strand_queue_runtime", instance)
	if err != nil {
		t.Fatal(err)
	}
	defer containers.DropRole(admin, strandQueueRole, t.Logf)
	createStrandRoles(t, ctx, admin, strandDomainRole, strandQueueRole)
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
		seedDailyPartition(t, ctx, admin, partitionID, runID, "running", ptr(now.Add(-time.Hour)))
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
		seedDailyPartition(t, ctx, admin, partitionID, runID, "running", ptr(now.Add(-time.Hour)))
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
				seedDailyPartition(t, ctx, admin, partitionID, runID, "running", ptr(now.Add(-time.Hour)))
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
		seedDailyPartition(t, ctx, admin, partitionID, runID, "running", ptr(now.Add(5*time.Minute)))
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
				seedDailyPartition(t, ctx, admin, partitionID, runID, "running", ptr(now.Add(-time.Hour)))
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
		seedDailyPartition(t, ctx, admin, partitionID, runID, "running", ptr(now.Add(-time.Hour)))
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
		seedDailyPartition(t, ctx, admin, integrationUUID(11), runID, "succeeded", nil)
		seedDailyPartition(t, ctx, admin, integrationUUID(12), runID, "running", ptr(now.Add(-time.Hour)))
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
			"UPDATE public.daily_metrics_partitions SET status='succeeded', claim_token=NULL, lease_expires_at=NULL WHERE run_id=$1",
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
		seedDailyPartition(t, ctx, admin, partitionID, runID, "running", ptr(now.Add(-time.Hour)))
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
		// codex review round 3: the previous version of this test staggered
		// delivered_at in the SAME order the outbox ids are generated
		// (integrationUUID counts upward), so ORDER BY outbox.id alone would
		// have produced the identical result and passed. The timestamps are now
		// assigned in REVERSE of id order, so id-ordering and delivery-ordering
		// disagree and only the correct one satisfies the assertions.
		const seeded = 5
		byDeliveredAt := make([]string, seeded)
		for index := range seeded {
			partitionID := integrationUUID(31 + index)
			seedDailyPartition(t, ctx, admin, partitionID, runID, "running", ptr(now.Add(-time.Hour)))
			outboxID := deliverDailyPartition(t, ctx, fixture, partitionID, now)
			makeJobTerminal(t, ctx, admin, riverJobFor(t, ctx, admin, outboxID), "completed", now.Add(-2*time.Hour))
			age := seeded - 1 - index
			if _, err := admin.Exec(ctx,
				"UPDATE public.worker_job_outbox SET delivered_at=$2 WHERE id=$1",
				outboxID, now.Add(time.Duration(age)*time.Minute)); err != nil {
				t.Fatal(err)
			}
			// Position in delivered_at order, oldest first: the LAST row
			// created carries the OLDEST delivered_at.
			byDeliveredAt[age] = outboxID
		}
		result, err := repair.Step(ctx, now, 2)
		if err != nil {
			t.Fatal(err)
		}
		if result.Rearmed != 2 {
			t.Fatalf("Step(limit=2) = %+v, want exactly 2 rearmed", result)
		}
		// Assert WHICH rows moved, in delivered_at order. Because id order is the
		// reverse of delivery order here, a pass that ordered by id would move
		// the wrong two and fail.
		for position, outboxID := range byDeliveredAt {
			var status string
			if err := admin.QueryRow(ctx,
				"SELECT status FROM public.worker_job_outbox WHERE id=$1", outboxID).Scan(&status); err != nil {
				t.Fatal(err)
			}
			want := "delivered"
			if position < 2 {
				want = "pending"
			}
			if status != want {
				t.Fatalf("the row at delivered_at position %d (id %s) is %q, want %q: the pass did "+
					"not take the oldest deliveries first", position, outboxID, status, want)
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
		seedDailyPartition(t, ctx, admin, partitionID, runID, "running", ptr(now.Add(-time.Hour)))
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
	// finished_at follows ck_worker_job_run_claim_state from alembic 0052: a
	// 'running' row has a claim and no finish time, every other status has a
	// finish time and no claim. An expired lease is still a lease, so the
	// reclaimable 'running' fixture stays valid.
	var finishedAt *time.Time
	if status != "running" {
		finished := time.Now().UTC().Truncate(time.Microsecond)
		finishedAt = &finished
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.worker_job_runs (
			id, job_kind, idempotency_key, org_id, domain_type, domain_id,
			status, claim_token, lease_expires_at, attempt_count,
			started_at, finished_at, created_at, updated_at
		) VALUES ($1, $2, $3, $4, 'daily_metrics_partition', $5, $6, $7, $8, 1,
			statement_timestamp(), $9, statement_timestamp(), statement_timestamp())`,
		integrationUUID(9600), jobKind, idempotencyKey, orgID, domainID,
		status, claimToken, leaseExpires, finishedAt); err != nil {
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

func createStrandRoles(t *testing.T, ctx context.Context, pool *pgxpool.Pool, domainRole, queueRole string) {
	t.Helper()
	for _, statement := range []string{
		"CREATE ROLE " + domainRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + strandDomainPassword + "'",
		"CREATE ROLE " + queueRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + strandQueuePassword + "'",
	} {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
}

func createStrandSchema(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	// Every table is DERIVED FROM THE ALEMBIC MIGRATIONS, column for column and
	// constraint for constraint, not hand-written to match the repair's
	// predicates. The first draft of this file invented an org_id column on
	// daily_metrics_partitions that production does not have, so the suite
	// stayed green while the shipped query crash-looped the prod reconciler on
	// `column partition.org_id does not exist`. The per-table authorities:
	//
	//   - daily_metrics_runs / daily_metrics_partitions: alembic 0057, with the
	//     'no_repositories' status widened in by 0095.
	//   - work_graph_execution_requests: alembic 0060, including its
	//     terminal-immutability trigger.
	//   - worker_job_outbox: alembic 0046, plus 0063's
	//     prerequisite_completion_key column.
	//   - worker_job_completion_fences: alembic 0063.
	//   - worker_job_runs: alembic 0052.
	_, err := pool.Exec(ctx, `
		CREATE TABLE public.daily_metrics_runs (
			id uuid PRIMARY KEY,
			org_id uuid NOT NULL,
			target_day date NOT NULL,
			generation varchar(64) NOT NULL,
			status varchar(16) NOT NULL DEFAULT 'pending',
			finalization_status varchar(16) NOT NULL DEFAULT 'pending',
			finalization_claim_token uuid NULL,
			finalization_lease_expires_at timestamptz NULL,
			finalized_at timestamptz NULL,
			created_at timestamptz NOT NULL,
			updated_at timestamptz NOT NULL,
			CONSTRAINT ck_daily_metrics_run_status CHECK (
				status IN ('pending', 'running', 'succeeded', 'failed', 'canceled', 'no_repositories')
			),
			CONSTRAINT ck_daily_metrics_finalize_status CHECK (
				finalization_status IN ('pending', 'running', 'succeeded', 'failed')
			),
			CONSTRAINT ck_daily_metrics_finalize_lease CHECK (
				(finalization_status = 'running' AND finalization_claim_token IS NOT NULL
					AND finalization_lease_expires_at IS NOT NULL)
				OR (finalization_status <> 'running' AND finalization_claim_token IS NULL
					AND finalization_lease_expires_at IS NULL)
			),
			CONSTRAINT uq_daily_metrics_run_generation UNIQUE (org_id, target_day, generation)
		);
		CREATE TABLE public.daily_metrics_partitions (
			id uuid PRIMARY KEY,
			run_id uuid NOT NULL REFERENCES public.daily_metrics_runs(id) ON DELETE CASCADE,
			ordinal integer NOT NULL,
			repo_ids json NOT NULL,
			status varchar(16) NOT NULL DEFAULT 'pending',
			claim_token uuid NULL,
			lease_expires_at timestamptz NULL,
			attempt_count integer NOT NULL DEFAULT 0,
			completed_at timestamptz NULL,
			created_at timestamptz NOT NULL,
			updated_at timestamptz NOT NULL,
			CONSTRAINT ck_daily_metrics_partition_ordinal CHECK (ordinal >= 0),
			CONSTRAINT ck_daily_metrics_partition_status CHECK (
				status IN ('pending', 'running', 'succeeded', 'failed')
			),
			CONSTRAINT ck_daily_metrics_partition_attempts CHECK (attempt_count >= 0),
			CONSTRAINT ck_daily_metrics_partition_lease CHECK (
				(status = 'running' AND claim_token IS NOT NULL AND lease_expires_at IS NOT NULL)
				OR (status <> 'running' AND claim_token IS NULL AND lease_expires_at IS NULL)
			),
			CONSTRAINT uq_daily_metrics_partition_ordinal UNIQUE (run_id, ordinal)
		);
		CREATE INDEX ix_daily_metrics_partition_reclaim
			ON public.daily_metrics_partitions (status, lease_expires_at);
		CREATE INDEX ix_daily_metrics_partition_run_status
			ON public.daily_metrics_partitions (run_id, status);
		CREATE TABLE public.work_graph_execution_requests (
			id uuid PRIMARY KEY,
			org_id uuid NOT NULL,
			kind text NOT NULL CHECK (kind IN (
				'workgraph.build', 'investment.materialize', 'investment.dispatch',
				'investment.chunk', 'investment.finalize'
			)),
			scope jsonb NOT NULL,
			model_ref text NULL CHECK (model_ref IS NULL OR length(model_ref) <= 128),
			prompt_ref text NULL CHECK (prompt_ref IS NULL OR length(prompt_ref) <= 128),
			llm_concurrency integer NOT NULL CHECK (llm_concurrency BETWEEN 1 AND 16),
			spend_limit_microunits bigint NOT NULL CHECK (spend_limit_microunits >= 0),
			correlation_id text NOT NULL CHECK (length(correlation_id) BETWEEN 1 AND 128),
			idempotency_key text NOT NULL UNIQUE CHECK (length(idempotency_key) BETWEEN 1 AND 256),
			state text NOT NULL DEFAULT 'pending' CHECK (state IN (
				'pending', 'running', 'succeeded', 'failed', 'ambiguous', 'canceled'
			)),
			claim_token uuid NULL,
			lease_expires_at timestamptz NULL,
			attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
			created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
			updated_at timestamptz NOT NULL DEFAULT statement_timestamp(),
			CHECK ((state = 'running' AND claim_token IS NOT NULL AND lease_expires_at IS NOT NULL)
				OR (state <> 'running' AND claim_token IS NULL AND lease_expires_at IS NULL))
		);
		CREATE INDEX ix_work_graph_execution_claim
			ON public.work_graph_execution_requests (kind, state, lease_expires_at);
		CREATE OR REPLACE FUNCTION forbid_work_graph_terminal_mutation()
		RETURNS trigger AS $trigger$
		BEGIN
			IF OLD.state IN ('succeeded', 'failed', 'canceled') THEN
				RAISE EXCEPTION 'terminal work graph execution request is immutable';
			END IF;
			RETURN NEW;
		END;
		$trigger$ LANGUAGE plpgsql;
		CREATE TRIGGER work_graph_execution_terminal_immutable
		BEFORE UPDATE ON public.work_graph_execution_requests
		FOR EACH ROW EXECUTE FUNCTION forbid_work_graph_terminal_mutation();
		CREATE TABLE public.worker_job_outbox (
			id uuid PRIMARY KEY,
			dedupe_key varchar(256) NOT NULL,
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
			river_job_id bigint,
			delivered_at timestamptz,
			created_at timestamptz NOT NULL,
			updated_at timestamptz NOT NULL,
			prerequisite_completion_key text NULL CHECK (
				prerequisite_completion_key IS NULL
				OR (
					length(prerequisite_completion_key) BETWEEN 1 AND 256
					AND prerequisite_completion_key ~ '^[a-z][a-z0-9_]{0,95}:[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
				)
			),
			CONSTRAINT ck_worker_job_outbox_status CHECK (status IN ('pending', 'claimed', 'delivered', 'dead')),
			CONSTRAINT ck_worker_job_outbox_contract_version CHECK (contract_version > 0),
			CONSTRAINT ck_worker_job_outbox_priority CHECK (priority BETWEEN 1 AND 4),
			CONSTRAINT ck_worker_job_outbox_max_attempts CHECK (max_attempts BETWEEN 1 AND 25),
			CONSTRAINT ck_worker_job_outbox_attempt_count CHECK (attempt_count >= 0),
			CONSTRAINT ck_worker_job_outbox_payload_hash CHECK (
				length(payload_hash) = 71 AND payload_hash LIKE 'sha256:%'
			),
			CONSTRAINT ck_worker_job_outbox_args_size CHECK (length(CAST(args AS TEXT)) <= 16384),
			CONSTRAINT ck_worker_job_outbox_claim_state CHECK (
				(status = 'claimed' AND claim_token IS NOT NULL AND claimed_at IS NOT NULL AND claim_expires_at IS NOT NULL)
				OR (status <> 'claimed' AND claim_token IS NULL AND claimed_at IS NULL AND claim_expires_at IS NULL)
			),
			CONSTRAINT ck_worker_job_outbox_delivery_state CHECK (
				(status = 'delivered' AND river_job_id IS NOT NULL AND delivered_at IS NOT NULL)
				OR (status <> 'delivered' AND river_job_id IS NULL AND delivered_at IS NULL)
			),
			CONSTRAINT ck_worker_job_outbox_error_state CHECK (
				(last_error_code IS NULL AND last_error_detail IS NULL AND last_error_at IS NULL)
				OR (last_error_code IS NOT NULL AND last_error_detail IS NOT NULL AND last_error_at IS NOT NULL)
			),
			CONSTRAINT uq_worker_job_outbox_dedupe_key UNIQUE (dedupe_key),
			CONSTRAINT uq_worker_job_outbox_river_job_id UNIQUE (river_job_id)
		);
		CREATE INDEX ix_worker_job_outbox_due
			ON public.worker_job_outbox (status, next_attempt_at, scheduled_at, created_at)
			WHERE status IN ('pending', 'claimed');
		CREATE INDEX ix_worker_job_outbox_claim_expiry
			ON public.worker_job_outbox (claim_expires_at)
			WHERE status = 'claimed';
		CREATE INDEX ix_worker_job_outbox_terminal
			ON public.worker_job_outbox (status, delivered_at, updated_at)
			WHERE status IN ('delivered', 'dead');
		CREATE INDEX ix_worker_job_outbox_prerequisite
			ON public.worker_job_outbox (prerequisite_completion_key)
			WHERE prerequisite_completion_key IS NOT NULL;
		CREATE TABLE public.worker_job_completion_fences (
			completion_key text PRIMARY KEY
				CHECK (
					length(completion_key) BETWEEN 1 AND 256
					AND completion_key ~ '^[a-z][a-z0-9_]{0,95}:[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
				),
			completed_at timestamptz NOT NULL DEFAULT statement_timestamp()
		);
		CREATE TABLE public.worker_job_runs (
			id uuid PRIMARY KEY,
			job_kind varchar(96) NOT NULL,
			idempotency_key varchar(256) NOT NULL,
			org_id uuid NULL,
			domain_type varchar(64) NOT NULL,
			domain_id uuid NOT NULL,
			status varchar(16) NOT NULL,
			claim_token uuid NULL,
			lease_expires_at timestamptz NULL,
			attempt_count integer NOT NULL,
			started_at timestamptz NOT NULL,
			finished_at timestamptz NULL,
			result varchar(16) NULL,
			error_category varchar(32) NULL,
			created_at timestamptz NOT NULL,
			updated_at timestamptz NOT NULL,
			CONSTRAINT ck_worker_job_run_status CHECK (
				status IN ('running', 'retryable', 'succeeded', 'terminal')
			),
			CONSTRAINT ck_worker_job_run_attempt_count CHECK (attempt_count >= 1),
			CONSTRAINT ck_worker_job_run_claim_state CHECK (
				(status = 'running' AND claim_token IS NOT NULL AND lease_expires_at IS NOT NULL AND finished_at IS NULL)
				OR (status <> 'running' AND claim_token IS NULL AND lease_expires_at IS NULL AND finished_at IS NOT NULL)
			),
			CONSTRAINT ck_worker_job_run_result_state CHECK (
				(result IS NULL AND error_category IS NULL)
				OR (result IS NOT NULL AND error_category IS NOT NULL)
			),
			CONSTRAINT uq_worker_job_run_key UNIQUE (job_kind, idempotency_key)
		);
		CREATE INDEX ix_worker_job_run_reclaim
			ON public.worker_job_runs (status, lease_expires_at)`)
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
	// The generation is derived from the run id so uq_daily_metrics_run_generation
	// can never collide across fixtures that share the org and target day. It is
	// passed as its own parameter rather than reusing $1: a parameter bound to a
	// uuid column in one place and a varchar column in another is exactly the
	// "inconsistent types deduced" parse failure this hotfix documents.
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.daily_metrics_runs (
			id, org_id, target_day, generation, status, finalization_status,
			finalization_claim_token, finalization_lease_expires_at,
			created_at, updated_at
		) VALUES ($1, $2, CURRENT_DATE, $7, $3, $4, $5, $6,
			statement_timestamp(), statement_timestamp())`,
		runID, orgID, status, finalizationStatus, claimToken, finalizationLease,
		runID); err != nil {
		t.Fatal(err)
	}
}

// seedDailyPartition takes no organization: daily_metrics_partitions has no
// org_id column in production, a partition's org is reachable ONLY through its
// run_id foreign key. The first draft of this fixture invented that column,
// which is exactly how the suite stayed green against a schema production does
// not have. The ordinal is assigned as the next free slot for the run, which
// keeps uq_daily_metrics_partition_ordinal satisfied without every caller
// numbering its partitions by hand.
func seedDailyPartition(
	t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	partitionID, runID, status string, leaseExpires *time.Time,
) {
	t.Helper()
	var claimToken *string
	if leaseExpires != nil {
		token := integrationUUID(9000)
		claimToken = &token
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.daily_metrics_partitions (
			id, run_id, ordinal, repo_ids, status, claim_token, lease_expires_at,
			created_at, updated_at
		)
		SELECT $1, $2, COALESCE(MAX(sibling.ordinal) + 1, 0), '[]'::json, $3, $4, $5,
			statement_timestamp(), statement_timestamp()
		FROM public.daily_metrics_partitions AS sibling
		WHERE sibling.run_id = $2`,
		partitionID, runID, status, claimToken, leaseExpires); err != nil {
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
	// scope, llm_concurrency, spend_limit_microunits, correlation_id and
	// idempotency_key are NOT NULL without defaults in alembic 0060, so the
	// fixture must supply them. The idempotency key is kind-prefixed exactly as
	// the work-graph publisher builds its dedupe keys, and stays unique across
	// the cross-kind fixture because the kind is part of it. The request id is
	// passed again as $7 for the text columns rather than reusing $1: a
	// parameter bound to a uuid column in one place and a text column in
	// another is exactly the "inconsistent types deduced" parse failure this
	// hotfix documents.
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.work_graph_execution_requests (
			id, org_id, kind, scope, llm_concurrency, spend_limit_microunits,
			correlation_id, idempotency_key, state, claim_token, lease_expires_at
		) VALUES ($1, $2, $3, '{}'::jsonb, 1, 0, $7, $3 || ':' || $7, $4, $5, $6)`,
		requestID, orgID, kind, state, claimToken, leaseExpires,
		requestID); err != nil {
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
