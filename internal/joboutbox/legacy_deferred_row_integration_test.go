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
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestExecutablePublishAcceptsTheLegacyDeferredRowForTheSameKind covers the
// migration hazard CHAOS-3946's fix creates: state persisted by the OLD code
// that the NEW code must still read.
//
// Before the cutover sync.team_autoimport was celery-routed, so
// teamAutoimportPostSyncWriter.PublishTx's unconditional PublishDeferred was
// legal and SUCCEEDED, writing rows into public.worker_job_outbox keyed
// "post-sync:<sync_run_id>:sync.team_autoimport". Those rows can still exist.
// The fix now publishes the SAME dedupe key through the executable path, and
// Producer.publish's ON CONFLICT (dedupe_key) DO NOTHING branch re-reads the
// existing row and compares job_kind, contract_version, payload_hash and
// prerequisite_completion_key -- returning ErrContractRejected on any
// mismatch. If the route change altered any of those, every affected sync run
// would be permanently rejected with dedupe_key_conflicts_with_existing_row
// and its team-autoimport handoff would never be relayed.
//
// So this test performs the actual migration: it publishes the envelope the
// pre-cutover code published, under a celery-routed registry, then publishes
// the identical envelope through the post-cutover executable path under the
// checked-in river registry, and asserts the second publish is accepted and
// leaves exactly one unchanged row.
//
// Deferred-ness is deliberately NOT a column on the row -- the relay resolves
// it per-kind from the registry (deferredRelayKinds) -- so the legacy row also
// becomes relay-eligible once the route flips, which is what finally drains it.
func TestExecutablePublishAcceptsTheLegacyDeferredRowForTheSameKind(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
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
	pool := openIntegrationPool(t, ctx, instance.URI)
	defer pool.Close()
	createOutboxSchema(t, ctx, pool)

	production, err := jobruntime.Load(filepath.Join("..", "..", "contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	// The pre-cutover contract: go_implemented / celery / celery is the only
	// shape descriptorAllowsPublish accepts on the deferred path.
	beforeCutover := integrationRouteRegistry(production, map[string]string{
		jobcontract.KindTeamAutoimport: "celery",
	})
	// The checked-in contract today.
	afterCutover := integrationRouteRegistry(production, map[string]string{
		jobcontract.KindTeamAutoimport: "river",
	})

	const (
		organizationID = "3f2a6c18-3d5c-4a3e-9f2b-1c7d5e9a4b60"
		syncRunID      = "5b8e1d47-2c9a-4f61-8d3e-7a2b6c4f0d19"
	)
	organization := organizationID
	envelope := jobcontract.Envelope{
		ContractVersion: jobcontract.ContractVersionV1,
		OrganizationID:  &organization,
		CorrelationID:   "post-sync:" + syncRunID,
		IdempotencyKey:  "post-sync:" + syncRunID + ":" + jobcontract.KindTeamAutoimport,
		Domain:          jobcontract.DomainLink{Type: "sync_run", ID: syncRunID},
		Payload:         jobcontract.TeamAutoimportPayload{SyncRunID: syncRunID},
	}

	// 1. The pre-cutover code path, which succeeded and left a durable row.
	legacyProducer, err := NewProducer(pool, beforeCutover)
	if err != nil {
		t.Fatal(err)
	}
	publishInTx(t, ctx, pool, func(tx pgx.Tx) error {
		return legacyProducer.PublishDeferred(ctx, tx, jobcontract.KindTeamAutoimport, envelope)
	})
	legacyKind, legacyVersion, legacyHash, legacyPrerequisite := readOutboxRow(t, ctx, pool, envelope.IdempotencyKey)
	if legacyKind != jobcontract.KindTeamAutoimport {
		t.Fatalf("the pre-cutover publish did not persist a row: kind=%q", legacyKind)
	}

	// 2. The post-cutover code path, publishing the identical envelope.
	currentProducer, err := NewProducer(pool, afterCutover)
	if err != nil {
		t.Fatal(err)
	}
	publishInTx(t, ctx, pool, func(tx pgx.Tx) error {
		return currentProducer.Publish(ctx, tx, jobcontract.KindTeamAutoimport, envelope)
	})

	// 3. The legacy row is accepted, not conflicted, and not duplicated.
	var rows int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM public.worker_job_outbox WHERE dedupe_key=$1`,
		envelope.IdempotencyKey,
	).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("outbox rows for the sync run = %d, want exactly 1", rows)
	}
	kind, version, hash, prerequisite := readOutboxRow(t, ctx, pool, envelope.IdempotencyKey)
	if kind != legacyKind || version != legacyVersion ||
		hash != legacyHash || prerequisite != legacyPrerequisite {
		t.Fatalf(
			"the cutover changed the persisted row identity: "+
				"kind %q->%q version %d->%d hash %q->%q prerequisite %q->%q. "+
				"Every pre-cutover sync run would be rejected with "+
				"dedupe_key_conflicts_with_existing_row.",
			legacyKind, kind, legacyVersion, version,
			legacyHash, hash, legacyPrerequisite, prerequisite,
		)
	}
}

func readOutboxRow(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	dedupeKey string,
) (string, int, string, string) {
	t.Helper()
	var kind, hash, prerequisite string
	var version int
	if err := pool.QueryRow(ctx, `
SELECT job_kind, contract_version, payload_hash, COALESCE(prerequisite_completion_key, '')
FROM public.worker_job_outbox WHERE dedupe_key = $1`, dedupeKey).
		Scan(&kind, &version, &hash, &prerequisite); err != nil {
		t.Fatal(err)
	}
	return kind, version, hash, prerequisite
}

// publishInTx runs one publish in its own committed transaction, which is how
// the post-sync fanout reaches the producer.
func publishInTx(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	publish func(pgx.Tx) error,
) {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := publish(tx); err != nil {
		t.Fatalf("publish rejected: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

// TestPublishStandaloneUsesTheExecutableRoute pins by EFFECT what
// publish_route_agreement_test.go's table can only assume.
//
// That guard skips this package (the publish seams are defined here, so
// PublishStandalone's internal delegation to Publish is an implementation, not
// a consumer choosing a route) and hard-codes "PublishStandalone selects the
// executable route". If the delegation were switched to PublishDeferred the
// guard would stay green while daily.PostgresPublisher.PublishPartition -- its
// only caller, at publisher.go:110 -- silently stopped publishing the
// River-routed metrics.daily_partition. An assumption a guard depends on has to
// be asserted somewhere, so it is asserted here.
//
// Both directions are pinned: a Celery-routed kind must be REFUSED (only the
// executable route rejects it), and the checked-in River route must be
// accepted. Asserting only the second half would pass a delegation that
// published everything deferred.
func TestPublishStandaloneUsesTheExecutableRoute(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
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
	pool := openIntegrationPool(t, ctx, instance.URI)
	defer pool.Close()
	createOutboxSchema(t, ctx, pool)

	production, err := jobruntime.Load(filepath.Join("..", "..", "contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	const partitionID = "9c4b1f7e-6a2d-4c58-b1e3-0f7a5d8c2b64"
	organization := "7a1c9e42-5b3d-4f06-8c27-9d1b4e6a3f58"
	envelope := jobcontract.Envelope{
		ContractVersion: jobcontract.ContractVersionV1,
		OrganizationID:  &organization,
		CorrelationID:   "daily:" + partitionID,
		IdempotencyKey:  "metrics.daily_partition:" + partitionID,
		Domain: jobcontract.DomainLink{
			Type: "daily_metrics_partition",
			ID:   partitionID,
		},
		Payload: jobcontract.DailyMetricsPartitionPayload{PartitionID: partitionID},
	}

	celeryRouted, err := NewProducer(pool, integrationRouteRegistry(production, map[string]string{
		jobcontract.KindDailyMetricsPartition: "celery",
	}))
	if err != nil {
		t.Fatal(err)
	}
	err = celeryRouted.PublishStandalone(ctx, jobcontract.KindDailyMetricsPartition, envelope)
	if !errors.Is(err, ErrPolicyRejected) {
		t.Fatalf("PublishStandalone on a Celery-routed kind = %v, want ErrPolicyRejected: "+
			"it must take the executable route, which a Celery route refuses", err)
	}

	riverRouted, err := NewProducer(pool, integrationRouteRegistry(production, map[string]string{
		jobcontract.KindDailyMetricsPartition: "river",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := riverRouted.PublishStandalone(ctx, jobcontract.KindDailyMetricsPartition, envelope); err != nil {
		t.Fatalf("PublishStandalone on the checked-in River route = %v, want it accepted", err)
	}
	var rows int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM public.worker_job_outbox WHERE dedupe_key=$1`,
		envelope.IdempotencyKey,
	).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("outbox rows = %d, want exactly 1", rows)
	}
}
