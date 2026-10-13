//go:build integration

package streamhandlers

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

const redactionMarker = "ghp_aaaaaaaaaaaaaaaaaaaaaaaa"

func newProcessingBatch(ctx context.Context, t *testing.T) (*pgxpool.Pool, *PostgresExternalBatchRepository, externalPointer) {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	pgschema.Apply(ctx, t, pool)
	pointer := externalPointer{
		IngestionID: uuid.New(), OrgID: "org-1", SourceSystem: "github", SourceInstance: "acme/api",
		SchemaVersion: "external-ingest.v1",
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO external_ingest_batches (
    ingestion_id, org_id, source_system, source_instance, status,
    updated_at, idempotency_key, payload_hash, schema_version
) VALUES ($1,$2,$3,$4,'processing',now(), gen_random_uuid()::text, 'sha256:test', 'external-ingest.v1')`,
		pointer.IngestionID, pointer.OrgID, pointer.SourceSystem, pointer.SourceInstance); err != nil {
		t.Fatal(err)
	}
	repository, err := NewPostgresExternalBatchRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	return pool, repository, pointer
}

// A rejection message echoes customer-supplied text ("Unknown record kind: '<kind>'"),
// so it must not reach external_ingest_rejections.message with a credential in it.
func TestExternalRejectionMessageIsStoredRedacted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	pool, repository, pointer := newProcessingBatch(ctx, t)

	err := repository.Complete(ctx, externalBatch{Pointer: pointer, ItemsReceived: 1}, externalCompletion{
		Rejected: 1,
		Rejections: []externalRejection{{
			Index: 0, Kind: "x", Code: "unknown_kind", Message: "Unknown record kind: '" + redactionMarker + "'",
			Path: "records[0].kind",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var message string
	if err := pool.QueryRow(ctx, `SELECT message FROM external_ingest_rejections WHERE ingestion_id = $1`,
		pointer.IngestionID.String()).Scan(&message); err != nil {
		t.Fatal(err)
	}
	if message == "" || strings.Contains(message, redactionMarker) {
		t.Fatalf("stored rejection message = %q", message)
	}
	if !strings.Contains(message, "Unknown record kind") {
		t.Fatalf("stored rejection message lost its diagnostic text: %q", message)
	}
}

// The failure reason comes from the stream runner; whatever it holds is stored in
// external_ingest_batches.error_summary, so it must be redacted too.
func TestExternalErrorSummaryReasonIsStoredRedacted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	pool, repository, pointer := newProcessingBatch(ctx, t)

	if err := repository.Fail(ctx, pointer, "poison message "+redactionMarker); err != nil {
		t.Fatal(err)
	}
	var summary string
	if err := pool.QueryRow(ctx, `SELECT error_summary::text FROM external_ingest_batches WHERE ingestion_id = $1`,
		pointer.IngestionID.String()).Scan(&summary); err != nil {
		t.Fatal(err)
	}
	if summary == "" || strings.Contains(summary, redactionMarker) {
		t.Fatalf("stored error_summary = %q", summary)
	}
	if !strings.Contains(summary, "poison message") {
		t.Fatalf("stored error_summary lost its diagnostic text: %q", summary)
	}
}
