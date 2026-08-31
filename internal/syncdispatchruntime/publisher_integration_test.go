//go:build integration

package syncdispatchruntime

import (
	"context"
	"net/url"
	"testing"
	"time"

	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

const (
	integrationDomainPassword = "sync_dispatch_runtime_domain_password"
	integrationQueuePassword  = "sync_dispatch_runtime_queue_password"
)

// TestPublisherInsertTxPostgres verifies the bounded publisher against River's
// real pgx InsertTx API under the queue-control role. The domain tables exist
// only because the checked-in River migration validates their names; this test
// neither grants nor uses domain-table access from the publisher transaction.
func TestPublisherInsertTxPostgres(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
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

	// CREATE ROLE is cluster-scoped, not database-scoped -- a scratch
	// database does not isolate it (CHAOS-4661). Deriving both role names
	// from this call's own database identity is what makes two successive
	// runs, and two concurrent lanes, collision-free.
	dbName, err := containers.DatabaseName(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	integrationDomainRole, err := containers.RoleName("sync_dispatch_runtime_domain", instance)
	if err != nil {
		t.Fatal(err)
	}
	integrationQueueRole, err := containers.RoleName("sync_dispatch_runtime_queue", instance)
	if err != nil {
		t.Fatal(err)
	}

	adminPool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer adminPool.Close()
	for _, statement := range []string{
		"REVOKE CREATE ON SCHEMA public FROM PUBLIC",
		"CREATE ROLE " + integrationDomainRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + integrationDomainPassword + "'",
		"CREATE ROLE " + integrationQueueRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + integrationQueuePassword + "'",
		"GRANT CONNECT ON DATABASE " + dbName + " TO " + integrationDomainRole + ", " + integrationQueueRole,
		"CREATE TABLE public.worker_job_outbox (id uuid PRIMARY KEY, state text NOT NULL)",
		"CREATE TABLE public.sync_dispatch_outbox (id uuid PRIMARY KEY, state text NOT NULL)",
		"CREATE TABLE public.sync_dispatch_transport_routes (kind text PRIMARY KEY, generation bigint NOT NULL)",
	} {
		if _, err := adminPool.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := riverstore.ApplyPinnedMigrations(ctx, adminPool, riverstore.MigrationOptions{
		Schema: "river", DomainRole: integrationDomainRole, QueueRole: integrationQueueRole,
	}); err != nil {
		t.Fatal(err)
	}

	queuePool, err := pgxpool.New(ctx, integrationRoleURI(t, instance.URI, integrationQueueRole))
	if err != nil {
		t.Fatal(err)
	}
	defer queuePool.Close()
	client, err := river.NewClient(riverpgxv5.New(queuePool), &river.Config{Schema: "river"})
	if err != nil {
		t.Fatal(err)
	}
	publisher, err := NewPublisher(client, PublisherOptions{Queue: "sync", MaxAttempts: 5})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := queuePool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	jobID, err := publisher.Publish(ctx, tx, Claim{
		OutboxID: testOutbox, Kind: "dispatch_sync_run", RouteGeneration: 11,
		DeliveryAttempt: 1,
	}, testReference())
	if err != nil {
		t.Fatal(err)
	}
	if jobID == "" {
		t.Fatal("publisher returned an empty River job id")
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	var encoded []byte
	if err := adminPool.QueryRow(ctx, "SELECT args FROM river.river_job WHERE id = $1", jobID).Scan(&encoded); err != nil {
		t.Fatal(err)
	}
	args, err := Convert(Claim{
		OutboxID: testOutbox, Kind: "dispatch_sync_run", RouteGeneration: 11,
		DeliveryAttempt: 1,
	}, testReference())
	if err != nil {
		t.Fatal(err)
	}
	if !matchesReturnedArgs(encoded, args) {
		t.Fatal("stored River arguments do not match the exact v1 argument shape")
	}

	// A handler may successfully acknowledge a temporary semantic block (for
	// example dispatch waiting for reference discovery), after which the same
	// durable outbox row is re-armed. Same-attempt ambiguity must deduplicate,
	// but the next durable delivery attempt must create executable work rather
	// than resolving forever to the already-completed River job.
	if _, err := adminPool.Exec(ctx,
		"UPDATE river.river_job SET state='completed', finalized_at=now() WHERE id=$1",
		jobID,
	); err != nil {
		t.Fatal(err)
	}
	secondTx, err := queuePool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = secondTx.Rollback(ctx) }()
	secondJobID, err := publisher.Publish(ctx, secondTx, Claim{
		OutboxID: testOutbox, Kind: "dispatch_sync_run", RouteGeneration: 11,
		DeliveryAttempt: 2,
	}, testReference())
	if err != nil {
		t.Fatal(err)
	}
	if err := secondTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if secondJobID == jobID {
		t.Fatalf("rearmed delivery attempt reused completed River job %s", jobID)
	}
}

func integrationRoleURI(t *testing.T, rawURI string, role string) string {
	t.Helper()
	parsed, err := url.Parse(rawURI)
	if err != nil {
		t.Fatal(err)
	}
	parsed.User = url.UserPassword(role, integrationQueuePassword)
	return parsed.String()
}
