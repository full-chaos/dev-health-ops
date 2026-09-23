package externalingest

import (
	"context"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// BatchRow mirrors status.py's BatchRow: one external_ingest_batches row.
type BatchRow struct {
	IngestionID     uuid.UUID
	OrgID           string
	IdempotencyKey  string
	PayloadHash     string
	SourceSystem    string
	SourceInstance  string
	EntityFamily    string
	Producer        *string
	ProducerVersion *string
	SchemaVersion   string
	WindowStartedAt *time.Time
	WindowEndedAt   *time.Time
	Status          string
	Attempts        int
	ItemsReceived   int
	ItemsAccepted   int
	ItemsRejected   int
	CreatedAt       time.Time
	UpdatedAt       time.Time
	CompletedAt     *time.Time
	ErrorSummary    map[string]any
	// ErrorSummaryJSON and RecordCountsJSON are the columns' stored JSON
	// text (nil for SQL NULL). A reader that re-emits them decodes this text
	// with pyjson, so key order survives; ErrorSummary above is the
	// data-plane handler's decoded form.
	ErrorSummaryJSON      []byte
	RecordCountsJSON      []byte
	RecomputeStatus       string
	RecomputeDispatchedAt *time.Time
	RecomputeCompletedAt  *time.Time
	RecomputeError        *string
}

const batchColumns = `ingestion_id, org_id, idempotency_key, payload_hash, source_system,
	source_instance, entity_family, producer, producer_version, schema_version,
	window_started_at, window_ended_at, status, attempts, items_received,
	items_accepted, items_rejected, created_at, updated_at, completed_at, error_summary,
	recompute_status, recompute_dispatched_at, recompute_completed_at, recompute_error, record_counts`

func scanBatchRow(row pgx.Row) (*BatchRow, error) {
	var b BatchRow
	var errorSummary []byte
	err := row.Scan(
		&b.IngestionID, &b.OrgID, &b.IdempotencyKey, &b.PayloadHash, &b.SourceSystem,
		&b.SourceInstance, &b.EntityFamily, &b.Producer, &b.ProducerVersion, &b.SchemaVersion,
		&b.WindowStartedAt, &b.WindowEndedAt, &b.Status, &b.Attempts, &b.ItemsReceived,
		&b.ItemsAccepted, &b.ItemsRejected, &b.CreatedAt, &b.UpdatedAt, &b.CompletedAt, &errorSummary,
		&b.RecomputeStatus, &b.RecomputeDispatchedAt, &b.RecomputeCompletedAt, &b.RecomputeError,
		&b.RecordCountsJSON,
	)
	b.ErrorSummary = decodeMetadata(errorSummary)
	b.ErrorSummaryJSON = errorSummary
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &b, nil
}

// findExistingBatchTx ports status.py's find_existing_batch.
func findExistingBatchTx(ctx context.Context, tx pgx.Tx, orgID, sourceSystem, sourceInstance, entityFamily, idempotencyKey string) (*BatchRow, error) {
	row := tx.QueryRow(ctx, `SELECT `+batchColumns+` FROM external_ingest_batches
		WHERE org_id = $1 AND source_system = $2 AND source_instance = $3
		  AND entity_family = $4 AND idempotency_key = $5`,
		orgID, sourceSystem, sourceInstance, entityFamily, idempotencyKey)
	return scanBatchRow(row)
}

type createBatchParams struct {
	IngestionID     uuid.UUID
	OrgID           string
	IdempotencyKey  string
	PayloadHash     string
	SourceSystem    string
	SourceInstance  string
	EntityFamily    string
	Producer        *string
	ProducerVersion *string
	SchemaVersion   string
	WindowStartedAt *time.Time
	WindowEndedAt   *time.Time
	ItemsReceived   int
}

// createBatchTx ports status.py's create_batch: INSERT with status='accepted',
// attempts=1. The unique index on (org_id, source_system, source_instance,
// entity_family, idempotency_key) is what makes a losing concurrent racer
// fail here with a unique violation (isUniqueViolation in idempotency.go).
func createBatchTx(ctx context.Context, tx pgx.Tx, p createBatchParams) (*BatchRow, error) {
	now := time.Now().UTC()
	row := tx.QueryRow(ctx, `
		INSERT INTO external_ingest_batches (
			ingestion_id, org_id, idempotency_key, payload_hash, source_system,
			source_instance, entity_family, producer, producer_version, schema_version,
			window_started_at, window_ended_at, status, attempts, items_received,
			items_accepted, items_rejected, created_at, updated_at, recompute_status
		) VALUES (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'accepted',1,$13,0,0,$14,$14,'not_applicable'
		)
		RETURNING `+batchColumns,
		p.IngestionID, p.OrgID, p.IdempotencyKey, p.PayloadHash, p.SourceSystem,
		p.SourceInstance, p.EntityFamily, p.Producer, p.ProducerVersion, p.SchemaVersion,
		p.WindowStartedAt, p.WindowEndedAt, p.ItemsReceived, now,
	)
	return scanBatchRow(row)
}

// markStreamUnavailableTx ports status.py's mark_stream_unavailable: the
// accept sequence's failure path when enqueue_batch's XADD/fail-closed check
// fails after the batch/payload rows already committed.
func markStreamUnavailableTx(ctx context.Context, pool *pgxpool.Pool, orgID string, ingestionID uuid.UUID) error {
	_, err := pool.Exec(ctx, `
		UPDATE external_ingest_batches
		SET status = 'stream_unavailable', updated_at = $3
		WHERE org_id = $1 AND ingestion_id = $2
	`, orgID, ingestionID, time.Now().UTC())
	return err
}

// resetForRetryTx ports status.py's reset_for_retry: a CAS re-accept of the
// SAME ingestion_id (attempts += 1, status -> accepted, prior outcome
// fields cleared), winning only if the row is still at fromStatus.
func resetForRetryTx(ctx context.Context, tx pgx.Tx, orgID string, ingestionID uuid.UUID, fromStatus string) (bool, error) {
	tag, err := tx.Exec(ctx, `
		UPDATE external_ingest_batches
		SET status = 'accepted', attempts = attempts + 1, items_accepted = 0,
		    items_rejected = 0, record_counts = NULL, error_summary = NULL,
		    completed_at = NULL, updated_at = $4,
		    recompute_status = 'not_applicable', recompute_scope = NULL,
		    recompute_dispatched_at = NULL, recompute_completed_at = NULL, recompute_error = NULL
		WHERE org_id = $1 AND ingestion_id = $2 AND status = $3
	`, orgID, ingestionID, fromStatus, time.Now().UTC())
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// getBatch ports status.py's get_batch.
func getBatch(ctx context.Context, pool *pgxpool.Pool, orgID string, ingestionID uuid.UUID) (*BatchRow, error) {
	row := pool.QueryRow(ctx, `SELECT `+batchColumns+` FROM external_ingest_batches
		WHERE org_id = $1 AND ingestion_id = $2`, orgID, ingestionID)
	return scanBatchRow(row)
}

// GetBatch is status.py's get_batch: one org-scoped batch, nil when absent.
func GetBatch(ctx context.Context, pool *pgxpool.Pool, orgID string, ingestionID uuid.UUID) (*BatchRow, error) {
	return getBatch(ctx, pool, orgID, ingestionID)
}

// ListRejections is status.py's list_rejections: one page of a batch's
// rejections by record index, and their total.
func ListRejections(ctx context.Context, pool *pgxpool.Pool, orgID string, ingestionID uuid.UUID, limit, offset int) ([]RejectionRow, int, error) {
	return listRejections(ctx, pool, orgID, ingestionID, limit, offset)
}

// BatchQuery is status.py's list_batches filters. A nil filter is Python's
// None (not applied); a non-nil empty string is applied as equality, as
// Python's `is not None` checks do.
type BatchQuery struct {
	SourceSystem, SourceInstance, Status, Producer *string
	CreatedAfter, CreatedBefore                    *time.Time
	Limit, Offset                                  int
}

// ListBatches is status.py's list_batches: newest first (ingestion_id as
// the tiebreaker), org-scoped, filtered, one page and the total.
func ListBatches(ctx context.Context, pool *pgxpool.Pool, orgID string, query BatchQuery) ([]BatchRow, int, error) {
	where := `WHERE org_id = $1`
	args := []any{orgID}
	for _, filter := range []struct {
		column string
		value  *string
	}{{"source_system", query.SourceSystem}, {"source_instance", query.SourceInstance}, {"status", query.Status}, {"producer", query.Producer}} {
		if filter.value != nil {
			args = append(args, *filter.value)
			where += ` AND ` + filter.column + ` = $` + strconv.Itoa(len(args))
		}
	}
	return queryBatches(ctx, pool, where, args, query.CreatedAfter, query.CreatedBefore, query.Limit, query.Offset)
}

// listBatches is the data plane's call shape: an empty string means "not
// filtered".
func listBatches(
	ctx context.Context, pool *pgxpool.Pool, orgID string,
	statusFilter, sourceSystem, sourceInstance string, createdAfter, createdBefore *time.Time,
	limit, offset int,
) ([]BatchRow, int, error) {
	where := `WHERE org_id = $1`
	args := []any{orgID}
	if statusFilter != "" {
		args = append(args, statusFilter)
		where += ` AND status = $` + strconv.Itoa(len(args))
	}
	if sourceSystem != "" {
		args = append(args, sourceSystem)
		where += ` AND source_system = $` + strconv.Itoa(len(args))
	}
	if sourceInstance != "" {
		args = append(args, sourceInstance)
		where += ` AND source_instance = $` + strconv.Itoa(len(args))
	}
	return queryBatches(ctx, pool, where, args, createdAfter, createdBefore, limit, offset)
}

func queryBatches(
	ctx context.Context, pool *pgxpool.Pool, where string, args []any,
	createdAfter, createdBefore *time.Time, limit, offset int,
) ([]BatchRow, int, error) {
	if createdAfter != nil {
		args = append(args, *createdAfter)
		where += ` AND created_at >= $` + strconv.Itoa(len(args))
	}
	if createdBefore != nil {
		args = append(args, *createdBefore)
		where += ` AND created_at <= $` + strconv.Itoa(len(args))
	}

	var total int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM external_ingest_batches `+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	limitArgs := append(append([]any{}, args...), limit, offset)
	// created_at DESC, ingestion_id DESC: the ingestion_id tiebreaker keeps
	// pagination stable across requests, since created_at alone is not
	// unique (status.py's list_batches carries the same tiebreaker, added
	// there for the identical reason -- ties free to reorder between pages
	// under concurrent inserts otherwise).
	rows, err := pool.Query(ctx, `SELECT `+batchColumns+` FROM external_ingest_batches `+where+
		` ORDER BY created_at DESC, ingestion_id DESC LIMIT $`+strconv.Itoa(len(limitArgs)-1)+` OFFSET $`+strconv.Itoa(len(limitArgs)), limitArgs...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var batches []BatchRow
	for rows.Next() {
		row, err := scanBatchRow(rows)
		if err != nil {
			return nil, 0, err
		}
		batches = append(batches, *row)
	}
	return batches, total, rows.Err()
}

// RejectionRow mirrors status.py's RejectionRow.
type RejectionRow struct {
	Index      int
	Kind       string
	ExternalID *string
	Code       string
	Message    string
	Path       *string
}

// listRejections ports status.py's list_rejections.
func listRejections(ctx context.Context, pool *pgxpool.Pool, orgID string, ingestionID uuid.UUID, limit, offset int) ([]RejectionRow, int, error) {
	var total int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM external_ingest_rejections WHERE org_id = $1 AND ingestion_id = $2
	`, orgID, ingestionID).Scan(&total); err != nil {
		return nil, 0, err
	}
	rows, err := pool.Query(ctx, `
		SELECT record_index, record_kind, external_id, code, message, path
		FROM external_ingest_rejections
		WHERE org_id = $1 AND ingestion_id = $2
		ORDER BY record_index ASC
		LIMIT $3 OFFSET $4
	`, orgID, ingestionID, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var rejections []RejectionRow
	for rows.Next() {
		var r RejectionRow
		if err := rows.Scan(&r.Index, &r.Kind, &r.ExternalID, &r.Code, &r.Message, &r.Path); err != nil {
			return nil, 0, err
		}
		rejections = append(rejections, r)
	}
	return rejections, total, rows.Err()
}
