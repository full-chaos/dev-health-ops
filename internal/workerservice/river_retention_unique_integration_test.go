//go:build integration

package workerservice

import (
	"context"
	"errors"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

const (
	retentionUniqueRolePassword = "retention_unique_password"
	// The prune_worker_job_terminal default (scheduler/fixed/producers.go:369-395),
	// not imported: a test import of scheduler/fixed would tie this package's
	// test selection to every scheduler change.
	retentionUniqueOutboxRetention = 30 * 24 * time.Hour
)

type retentionUniqueKind struct {
	name     string
	kind     string
	envelope jobcontract.Envelope
}

// retentionUniqueKinds are the outbox kinds whose idempotency key can recur
// across runs. Each envelope carries that producer's key shape.
func retentionUniqueKinds() []retentionUniqueKind {
	org := "00000000-0000-4000-8000-000000000009"
	return []retentionUniqueKind{
		{
			// remaining/publisher.go:103; the partition id is deterministic per
			// request (remaining/postgres.go:1319-1332), so a repeated request
			// republishes it.
			name: "C1 remaining partition",
			kind: jobcontract.KindRemainingDORA,
			envelope: jobcontract.Envelope{
				ContractVersion: jobcontract.ContractVersionV1,
				OrganizationID:  &org,
				CorrelationID:   "remaining:00000000-0000-4000-8000-000000000101",
				IdempotencyKey:  "remaining:partition:00000000-0000-4000-8000-000000000102",
				Domain:          jobcontract.DomainLink{Type: "remaining_metric_partition", ID: "00000000-0000-4000-8000-000000000102"},
				Payload:         jobcontract.NewRemainingMetricsPartitionPayload(jobcontract.KindRemainingDORA, "00000000-0000-4000-8000-000000000102"),
			},
		},
		{
			// workersctl/trigger_workgraph.go:183,194.
			name: "C2 manual workgraph build",
			kind: jobcontract.KindWorkGraphBuild,
			envelope: jobcontract.Envelope{
				ContractVersion: jobcontract.ContractVersionV1,
				OrganizationID:  &org,
				CorrelationID:   "manual-trigger:workgraph.build:" + org + ":2026-10-01:2026-10-31",
				IdempotencyKey:  "manual-trigger:workgraph.build:" + org + ":2026-10-01:2026-10-31",
				Domain:          jobcontract.DomainLink{Type: "work_graph_request", ID: "00000000-0000-4000-8000-000000000103"},
				Payload:         jobcontract.WorkGraphBuildPayload{RequestID: "00000000-0000-4000-8000-000000000103"},
			},
		},
		{
			// workersctl/trigger_investment.go:159-176.
			name: "C3 manual investment materialize",
			kind: jobcontract.KindInvestmentMaterialize,
			envelope: jobcontract.Envelope{
				ContractVersion: jobcontract.ContractVersionV1,
				OrganizationID:  &org,
				CorrelationID:   "manual-trigger:investment.materialize:" + org + ":2026-10-01:2026-10-31",
				IdempotencyKey:  "manual-trigger:investment.materialize:" + org + ":2026-10-01:2026-10-31",
				Domain:          jobcontract.DomainLink{Type: "investment_request", ID: "00000000-0000-4000-8000-000000000104"},
				Payload:         jobcontract.InvestmentMaterializePayload{RequestID: "00000000-0000-4000-8000-000000000104"},
			},
		},
		{
			// externalrecompute/enqueue.go:125 with the replay correlation of
			// replay.go:382-383. The request row does not stop the publish:
			// workgraph/publisher.go:173-186 publishes again on a duplicate row.
			name: "C4 external recompute replay",
			kind: jobcontract.KindInvestmentMaterialize,
			envelope: jobcontract.Envelope{
				ContractVersion: jobcontract.ContractVersionV1,
				OrganizationID:  &org,
				CorrelationID:   "ext-recompute:00000000-0000-5000-8000-000000000105",
				IdempotencyKey:  "ext-recompute:00000000-0000-5000-8000-000000000105:investment.materialize",
				Domain:          jobcontract.DomainLink{Type: "investment_request", ID: "00000000-0000-4000-8000-000000000105"},
				Payload:         jobcontract.InvestmentMaterializePayload{RequestID: "00000000-0000-4000-8000-000000000105"},
			},
		},
		{
			// api/webhookintake/delivery.go:121-131.
			name: "C5 webhook delivery",
			kind: jobcontract.KindWebhookDelivery,
			envelope: jobcontract.Envelope{
				ContractVersion: jobcontract.ContractVersionV1,
				CorrelationID:   "webhook-delivery:00000000-0000-4000-8000-000000000106",
				IdempotencyKey:  "webhook:4f1c0e6a0f3b6a2d9e7c5b3a1f0e9d8c7b6a5f4e3d2c1b0a9f8e7d6c5b4a3f2e",
				Domain:          jobcontract.DomainLink{Type: "webhook_delivery", ID: "00000000-0000-4000-8000-000000000106"},
				Payload:         jobcontract.WebhookDeliveryPayload{DeliveryID: "00000000-0000-4000-8000-000000000106"},
			},
		},
		{
			// api/billing/subscriptionhooks.go:332-398.
			name: "C6 billing notification",
			kind: jobcontract.KindBillingNotification,
			envelope: jobcontract.Envelope{
				ContractVersion: jobcontract.ContractVersionV1,
				OrganizationID:  &org,
				CorrelationID:   "billing-notification:00000000-0000-4000-8000-000000000107",
				IdempotencyKey:  "billing:invoice_receipt:" + org + ":evt_0001",
				Domain:          jobcontract.DomainLink{Type: "billing_notification", ID: "00000000-0000-4000-8000-000000000107"},
				Payload:         jobcontract.BillingNotificationPayload{NotificationID: "00000000-0000-4000-8000-000000000107"},
			},
		},
	}
}

// TestRiverRetentionAgainstOutboxPruneForStableKeys runs the shipped default
// River retention against the outbox prune. A key is published and
// delivered, its River job finalizes after a given lag, the 30-day
// prune_worker_job_terminal cutoff removes the outbox row one hour past it,
// and the River job cleaner runs at the worker client's configured
// retention. The same key is then published again.
//
// Cancelled keeps River's 24h default, so its row is always gone and the
// repeat runs as a new job. Discarded stays 29 days: with lag <= 1 day the
// row is gone and the repeat runs as a new job; with lag > 1 day + the prune
// slack the row still holds the key and the repeat dedupes into it. That
// discarded-only residual is pinned so a change to it is seen.
func TestRiverRetentionAgainstOutboxPruneForStableKeys(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	t.Cleanup(cancel)

	instance := pgschema.Start(ctx, t)
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	domainRole, err := containers.RoleName("retention_unique_domain", instance)
	if err != nil {
		t.Fatal(err)
	}
	queueRole, err := containers.RoleName("retention_unique_queue", instance)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { containers.DropRole(admin, domainRole, t.Logf) })
	t.Cleanup(func() { containers.DropRole(admin, queueRole, t.Logf) })
	for _, role := range []string{domainRole, queueRole} {
		if _, err := admin.Exec(ctx, "CREATE ROLE "+role+" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE "+
			"NOREPLICATION NOBYPASSRLS PASSWORD '"+retentionUniqueRolePassword+"'"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := riverstore.ApplyPinnedMigrations(ctx, admin, riverstore.MigrationOptions{
		Schema: "river", DomainRole: domainRole, QueueRole: queueRole,
	}); err != nil {
		t.Fatal(err)
	}
	domainPool := retentionUniqueRolePool(t, ctx, instance.URI, domainRole)
	queuePool := retentionUniqueRolePool(t, ctx, instance.URI, queueRole)

	registry, err := jobruntime.Load(filepath.Join("..", "..", "contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	producer, err := joboutbox.NewProducer(domainPool, registry)
	if err != nil {
		t.Fatal(err)
	}
	repository, err := joboutbox.NewRepository(queuePool)
	if err != nil {
		t.Fatal(err)
	}
	inserter, err := joboutbox.NewRiverInserter(queuePool, "river", registry)
	if err != nil {
		t.Fatal(err)
	}
	clientConfig := shippedRiverClientConfig(loadRiverRetentionConfig(t, map[string]string{}))
	cleaner := riverpgxv5.New(queuePool).GetExecutor()

	relay := func(t *testing.T) int64 {
		t.Helper()
		now := time.Now()
		claims, err := repository.ClaimDue(ctx, now, 1, 30*time.Second)
		if err != nil || len(claims) != 1 {
			t.Fatalf("ClaimDue() = %d claims, %v; want 1", len(claims), err)
		}
		jobID, err := repository.Dispatch(ctx, claims[0], now, inserter.Insert)
		if err != nil {
			t.Fatalf("Dispatch() error = %v", err)
		}
		return jobID
	}

	for _, kind := range retentionUniqueKinds() {
		for _, tc := range []struct {
			state      string
			name       string
			lag        time.Duration
			wantNewJob bool
		}{
			{state: "cancelled", name: "lag 1d: repeat makes a new job", lag: 24 * time.Hour, wantNewJob: true},
			{state: "cancelled", name: "lag 1d+2h: repeat makes a new job", lag: 26 * time.Hour, wantNewJob: true},
			{state: "discarded", name: "lag 1d: repeat makes a new job", lag: 24 * time.Hour, wantNewJob: true},
			{state: "discarded", name: "PIN residual lag 1d+2h: repeat dedupes into the finalized job", lag: 26 * time.Hour, wantNewJob: false},
		} {
			state := tc.state
			t.Run(kind.name+"/"+state+"/"+tc.name, func(t *testing.T) {
				if _, err := admin.Exec(ctx, "TRUNCATE public.worker_job_outbox, "+
					"public.worker_job_delivery_abandonments, public.worker_job_completion_fences, "+
					"river.river_job RESTART IDENTITY CASCADE"); err != nil {
					t.Fatal(err)
				}
				if err := producer.PublishStandalone(ctx, kind.kind, kind.envelope); err != nil {
					t.Fatalf("first publish: %v", err)
				}
				firstJob := relay(t)

				// While the outbox row exists, the outbox itself refuses the repeat.
				if err := producer.PublishStandalone(ctx, kind.kind, kind.envelope); !errors.Is(err, joboutbox.ErrDeliveryAlreadyTerminal) {
					t.Fatalf("repeat before the prune = %v, want ErrDeliveryAlreadyTerminal", err)
				}

				cutoff := time.Now().UTC().Truncate(time.Second).Add(-retentionUniqueOutboxRetention)
				delivered := cutoff.Add(-time.Hour)
				outboxID := joboutbox.OutboxRowID(kind.envelope.IdempotencyKey).String()
				if _, err := admin.Exec(ctx, `UPDATE public.worker_job_outbox
						SET created_at = $2, scheduled_at = $2, next_attempt_at = $2,
						    delivered_at = $2, updated_at = $2
						WHERE id = $1::uuid`, outboxID, delivered); err != nil {
					t.Fatal(err)
				}
				if _, err := admin.Exec(ctx, `UPDATE river.river_job
						SET state = $2::river.river_job_state, attempt = 1, created_at = $3,
						    scheduled_at = $3, attempted_at = $3, finalized_at = $4
						WHERE id = $1`, firstJob, state, delivered, delivered.Add(tc.lag)); err != nil {
					t.Fatal(err)
				}

				pruned, err := repository.DeleteTerminalBefore(ctx, cutoff, 500)
				if err != nil || pruned != 1 {
					t.Fatalf("outbox prune deleted %d rows, %v; want 1", pruned, err)
				}
				cleanedAt := time.Now()
				if _, err := cleaner.JobDeleteBefore(ctx, &riverdriver.JobDeleteBeforeParams{
					CancelledDoDelete:           true,
					CancelledFinalizedAtHorizon: cleanedAt.Add(-clientConfig.CancelledJobRetentionPeriod),
					CompletedDoDelete:           true,
					CompletedFinalizedAtHorizon: cleanedAt.Add(-clientConfig.CompletedJobRetentionPeriod),
					DiscardedDoDelete:           true,
					DiscardedFinalizedAtHorizon: cleanedAt.Add(-clientConfig.DiscardedJobRetentionPeriod),
					Max:                         1000,
					Schema:                      "river",
				}); err != nil {
					t.Fatal(err)
				}

				if err := producer.PublishStandalone(ctx, kind.kind, kind.envelope); err != nil {
					t.Fatalf("repeat after the prune: %v", err)
				}
				repeatJob := relay(t)
				var jobs int
				if err := admin.QueryRow(ctx, "SELECT count(*) FROM river.river_job").Scan(&jobs); err != nil {
					t.Fatal(err)
				}
				if tc.wantNewJob {
					if repeatJob == firstJob || jobs != 1 {
						t.Fatalf("repeat job %d (first %d, %d River rows); want a new job and the finalized row cleaned",
							repeatJob, firstJob, jobs)
					}
					return
				}
				if repeatJob != firstJob || jobs != 1 {
					t.Fatalf("repeat job %d (first %d, %d River rows); want the repeat deduped into the finalized job",
						repeatJob, firstJob, jobs)
				}
			})
		}
	}
}

func retentionUniqueRolePool(t *testing.T, ctx context.Context, raw, role string) *pgxpool.Pool {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	parsed.User = url.UserPassword(role, retentionUniqueRolePassword)
	pool, err := pgxpool.New(ctx, parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}
