package externalingest

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// payloadExists ports payload_store.py's payload_exists: the fail-closed
// precondition enqueueBatch checks before making a pointer visible on the
// stream (a pointer must never precede its durable payload row).
func payloadExists(ctx context.Context, pool *pgxpool.Pool, ingestionID uuid.UUID, orgID string) (bool, error) {
	var exists bool
	err := pool.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM external_ingest_batch_payloads WHERE ingestion_id = $1 AND org_id = $2)
	`, ingestionID, orgID).Scan(&exists)
	return exists, err
}

// upsertPayloadTx ports payload_store.py's upsert_payload: write (or
// refresh) the raw-payload row for ingestionID. Does not commit -- the
// caller commits once the batch status row also succeeds (CC22).
func upsertPayloadTx(ctx context.Context, tx pgx.Tx, ingestionID uuid.UUID, orgID, schemaVersion string, payloadBytes []byte) error {
	var exists bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM external_ingest_batch_payloads WHERE ingestion_id = $1 AND org_id = $2)
	`, ingestionID, orgID).Scan(&exists); err != nil {
		return err
	}
	now := time.Now().UTC()
	if exists {
		_, err := tx.Exec(ctx, `
			UPDATE external_ingest_batch_payloads
			SET schema_version = $3, payload_json = $4, byte_size = $5, created_at = $6
			WHERE ingestion_id = $1 AND org_id = $2
		`, ingestionID, orgID, schemaVersion, payloadBytes, len(payloadBytes), now)
		return err
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO external_ingest_batch_payloads
			(ingestion_id, org_id, schema_version, payload_json, byte_size, created_at)
		VALUES ($1,$2,$3,$4,$5,$6)
	`, ingestionID, orgID, schemaVersion, payloadBytes, len(payloadBytes), now)
	return err
}
