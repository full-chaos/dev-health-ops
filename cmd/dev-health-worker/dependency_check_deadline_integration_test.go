//go:build integration

package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestQueueAndSchemaReadyPreserveARealPgxDeadlineForClassification is the
// integration-level half of the roll-storm regression: it drives
// queueReady/riverSchemaReady against a REAL Postgres connection with a
// context that is already past its deadline, the same shape a connection
// burst produces when Postgres and its pooler are saturated long enough for
// a required check's own bounded wait to expire before it gets an answer.
// The unit-level regression (TestDependencyCheckFailedPreservesTheUnderlying
// CauseForClassification, TestPreclaimReadinessRetriesRealCheckWrappers
// WhenBothMembersTimeOut) proves the classification fix against a
// hand-constructed error of the same shape; this test proves that shape is
// actually what the real driver produces, end to end through the real
// workerDatabase implementation -- not a fake.
func TestQueueAndSchemaReadyPreserveARealPgxDeadlineForClassification(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	pg, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pg.Close(context.Background()) })

	pool, err := pgxpool.New(ctx, pg.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	database := &postgresWorkerDatabase{
		pools:       &postgres.RuntimePools{Domain: pool, QueueControl: pool},
		domainRole:  "devhealth_domain",
		queueRole:   "devhealth_queue",
		riverSchema: "river",
	}
	dependencies := &workerDependencies{database: database}

	// Already past its deadline: no schema or grants need to exist for this
	// -- pgxpool's own Acquire (and, failing that, pgconn's own context
	// check before any network I/O) refuses a context that is already done,
	// which is exactly the shape CheckQueueAuthorization/CheckSchema hit
	// when a connection-burst-saturated pooler cannot answer within a
	// required check's bounded wait.
	expiredCtx, expiredCancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
	defer expiredCancel()

	queueErr := dependencies.queueReady(expiredCtx)
	if !errors.Is(queueErr, errWorkerDependencyUnavailable) {
		t.Fatalf("queueReady() error = %v, want errWorkerDependencyUnavailable", queueErr)
	}
	if !errors.Is(queueErr, context.DeadlineExceeded) {
		t.Fatalf("queueReady() error = %v, want a real pgx deadline error still classifiable as context.DeadlineExceeded", queueErr)
	}

	schemaErr := dependencies.riverSchemaReady("river")(expiredCtx)
	if !errors.Is(schemaErr, errWorkerDependencyUnavailable) {
		t.Fatalf("riverSchemaReady() error = %v, want errWorkerDependencyUnavailable", schemaErr)
	}
	if !errors.Is(schemaErr, context.DeadlineExceeded) {
		t.Fatalf("riverSchemaReady() error = %v, want a real pgx deadline error still classifiable as context.DeadlineExceeded", schemaErr)
	}
}
