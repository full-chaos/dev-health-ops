package system

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrRetentionUnavailable is the single stable retention failure. Retention
// runs against operational tables, so its errors must never carry row values,
// tenant identifiers, or DSN fragments into logs.
var ErrRetentionUnavailable = errors.New("retention store is unavailable")

// Each retention store below owns exactly one table. The table is a compile
// -time literal inside the store, never a constructor argument or payload
// field, so no operator input and no future policy can widen what a retention
// job deletes.

// RateLimitObservationStore prunes provider_rate_limit_observations.
//
// Parity with the Celery prune_rate_limit_observations task: observations are
// telemetry with no ClickHouse mirror and no archival path, so expired rows are
// deleted outright. The Python task issues one unbounded DELETE; this store
// deletes in checkpoint-sized chunks instead, which reaches the same end state
// without holding a long transaction over a first-run backlog.
type RateLimitObservationStore struct {
	pool *pgxpool.Pool
}

func NewRateLimitObservationStore(pool *pgxpool.Pool) (*RateLimitObservationStore, error) {
	if pool == nil {
		return nil, ErrRetentionUnavailable
	}
	return &RateLimitObservationStore{pool: pool}, nil
}

func (store *RateLimitObservationStore) DeleteBefore(
	ctx context.Context,
	before time.Time,
	batchSize int,
) (int64, error) {
	if store == nil || store.pool == nil {
		return 0, ErrRetentionUnavailable
	}
	return deleteInChunks(ctx, store.pool, `
		DELETE FROM public.provider_rate_limit_observations
		WHERE id IN (
			SELECT id FROM public.provider_rate_limit_observations
			WHERE observed_at < $1
			ORDER BY observed_at, id
			FOR UPDATE SKIP LOCKED
			LIMIT $2
		)`, before, batchSize)
}

// ExternalIngestBatchStore prunes external_ingest_batches.
//
// Parity with the Celery prune_external_ingest_batches task:
//   - only terminal batches are eligible. A row still in accepted,
//     stream_unavailable, or processing past its retention window is a bug
//     signal that must stay visible, so retention never removes it;
//   - rejections are removed by the ON DELETE CASCADE on
//     external_ingest_rejections.ingestion_id, not by a second statement, so
//     the child rows can never outlive their batch;
//   - deletion is chunked with a commit per chunk so a months-old backlog does
//     not run as one long transaction.
//
// external_ingest_batch_payloads has no foreign key to batches and is pruned by
// its own owner, exactly as in the Python task.
type ExternalIngestBatchStore struct {
	pool *pgxpool.Pool
}

// AskDevConversationStore removes expired conversation content while keeping
// only the minimal lifecycle tombstone. Expiry is persisted per conversation,
// so the fixed scheduler passes its occurrence time as the cutoff; it never
// applies a second retention horizon that could turn 30 days into 60.
type AskDevConversationStore struct {
	pool *pgxpool.Pool
}

func NewAskDevConversationStore(pool *pgxpool.Pool) (*AskDevConversationStore, error) {
	if pool == nil {
		return nil, ErrRetentionUnavailable
	}
	return &AskDevConversationStore{pool: pool}, nil
}

func (store *AskDevConversationStore) DeleteBefore(
	ctx context.Context,
	before time.Time,
	batchSize int,
) (int64, error) {
	if store == nil || store.pool == nil {
		return 0, ErrRetentionUnavailable
	}
	return deleteInChunks(ctx, store.pool, `
		WITH candidates AS MATERIALIZED (
			SELECT id, org_id, user_id, retention_days, created_at
			FROM public.dev_conversations
			WHERE expires_at IS NOT NULL AND expires_at <= $1
			ORDER BY expires_at, id
			FOR UPDATE SKIP LOCKED
			LIMIT $2
		), inserted_tombstones AS (
			INSERT INTO public.dev_conversation_tombstones (
				id, conversation_id, org_id, user_id, actor_user_id, reason,
				retention_days, conversation_created_at, deleted_at
			)
			SELECT id, id, org_id, user_id, NULL,
				CASE WHEN retention_days = 0
					THEN 'ephemeral_completed'
					ELSE 'retention_expired'
				END,
				retention_days, created_at, $1
			FROM candidates
			ON CONFLICT (conversation_id) DO NOTHING
			RETURNING conversation_id
		)
		DELETE FROM public.dev_conversations AS conversations
		USING candidates
		WHERE conversations.id = candidates.id
			AND (
				EXISTS (
					SELECT 1 FROM inserted_tombstones
					WHERE conversation_id = candidates.id
				)
				OR EXISTS (
					SELECT 1 FROM public.dev_conversation_tombstones
					WHERE conversation_id = candidates.id
				)
			)`, before, batchSize)
}

func NewExternalIngestBatchStore(pool *pgxpool.Pool) (*ExternalIngestBatchStore, error) {
	if pool == nil {
		return nil, ErrRetentionUnavailable
	}
	return &ExternalIngestBatchStore{pool: pool}, nil
}

func (store *ExternalIngestBatchStore) DeleteBefore(
	ctx context.Context,
	before time.Time,
	batchSize int,
) (int64, error) {
	if store == nil || store.pool == nil {
		return 0, ErrRetentionUnavailable
	}
	return deleteInChunks(ctx, store.pool, `
		DELETE FROM public.external_ingest_batches
		WHERE ingestion_id IN (
			SELECT ingestion_id FROM public.external_ingest_batches
			WHERE created_at < $1
				AND status IN ('completed', 'partial', 'failed')
			ORDER BY created_at, ingestion_id
			FOR UPDATE SKIP LOCKED
			LIMIT $2
		)`, before, batchSize)
}

// deleteInChunks drains the cutoff in checkpoint-sized transactions. It stops
// on a short chunk or on cancellation; the job's River deadline is therefore
// the only wall-clock bound, and an interrupted drain is safe because the
// cutoff is immutable and every committed chunk is already durable.
func deleteInChunks(
	ctx context.Context,
	pool *pgxpool.Pool,
	statement string,
	before time.Time,
	batchSize int,
) (int64, error) {
	if before.IsZero() || batchSize < 1 || batchSize > 1000 {
		return 0, ErrRetentionUnavailable
	}
	var deleted int64
	for {
		if err := ctx.Err(); err != nil {
			return deleted, err
		}
		chunk, err := deleteOneChunk(ctx, pool, statement, before, batchSize)
		deleted += chunk
		if err != nil {
			return deleted, err
		}
		if chunk < int64(batchSize) {
			return deleted, nil
		}
	}
}

func deleteOneChunk(
	ctx context.Context,
	pool *pgxpool.Pool,
	statement string,
	before time.Time,
	batchSize int,
) (int64, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, ErrRetentionUnavailable
	}
	defer func() { _ = tx.Rollback(ctx) }()
	command, err := tx.Exec(ctx, statement, before, batchSize)
	if err != nil {
		return 0, ErrRetentionUnavailable
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, ErrRetentionUnavailable
	}
	return command.RowsAffected(), nil
}
