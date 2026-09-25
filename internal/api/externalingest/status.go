package externalingest

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
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
	// ErrorSummary is status.py's BatchStatusResponse.error_summary: a raw
	// `dict[str, Any] | None` field, so Python serializes exactly the dict
	// `_parse_json` produced from the stored JSONB, insertion order and int/float
	// distinction preserved from whatever the writer (status.py's
	// _build_error_summary, its mark_failed, or the Go worker's own writer)
	// originally wrote, decoded with pyjson (json.loads semantics). A value that
	// is not a dict is ErrStoredJSONColumn (see parseStoredDict).
	ErrorSummary *pyjson.Object
	// ErrorSummaryJSON and RecordCountsJSON are the columns' stored JSON text
	// (nil for SQL NULL), kept beside the parsed values above.
	ErrorSummaryJSON []byte
	RecordCountsJSON []byte
	// RecomputeScope is the recompute_scope column read as status.py's
	// `_parse_json` reads it (nil for None); recomputeScopeResponse rebuilds the
	// model's field order and raises where `_recompute_scope_response` raises.
	RecomputeScope *pyjson.Object
	// RecordCounts is `_parse_json(record_counts)`: parsed (and refused) on every
	// row read, as Python does, though no status response carries it.
	RecordCounts          *pyjson.Object
	RecomputeStatus       string
	RecomputeDispatchedAt *time.Time
	RecomputeCompletedAt  *time.Time
	RecomputeError        *string
}

const batchColumns = `ingestion_id, org_id, idempotency_key, payload_hash, source_system,
	source_instance, entity_family, producer, producer_version, schema_version,
	window_started_at, window_ended_at, status, attempts, items_received,
	items_accepted, items_rejected, created_at, updated_at, completed_at, error_summary,
	recompute_status, recompute_scope, recompute_dispatched_at, recompute_completed_at, recompute_error, record_counts`

func scanBatchRow(row pgx.Row) (*BatchRow, error) {
	var b BatchRow
	var errorSummary, recomputeScope []byte
	err := row.Scan(
		&b.IngestionID, &b.OrgID, &b.IdempotencyKey, &b.PayloadHash, &b.SourceSystem,
		&b.SourceInstance, &b.EntityFamily, &b.Producer, &b.ProducerVersion, &b.SchemaVersion,
		&b.WindowStartedAt, &b.WindowEndedAt, &b.Status, &b.Attempts, &b.ItemsReceived,
		&b.ItemsAccepted, &b.ItemsRejected, &b.CreatedAt, &b.UpdatedAt, &b.CompletedAt, &errorSummary,
		&b.RecomputeStatus, &recomputeScope, &b.RecomputeDispatchedAt, &b.RecomputeCompletedAt, &b.RecomputeError,
		&b.RecordCountsJSON,
	)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	// status.py's _row_to_batch parses the three JSON columns for every row it
	// reads, and raises (unhandled) on a value that is not a dict: the same
	// order and the same refusal here.
	if b.RecordCounts, err = parseStoredDict(b.RecordCountsJSON); err != nil {
		return nil, fmt.Errorf("record_counts: %w", err)
	}
	if b.ErrorSummary, err = parseStoredDict(errorSummary); err != nil {
		return nil, fmt.Errorf("error_summary: %w", err)
	}
	if b.RecomputeScope, err = parseStoredDict(recomputeScope); err != nil {
		return nil, fmt.Errorf("recompute_scope: %w", err)
	}
	b.ErrorSummaryJSON = errorSummary
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

// RecomputeJobRow mirrors recompute_status.py's RecomputeJobRow: one row
// from the external_ingest_recompute_jobs log.
type RecomputeJobRow struct {
	Task   string
	TaskID *string
	Queue  string
	RepoID *string
}

// listRecomputeJobs ports recompute_status.py's get_recompute_jobs: jobs
// from the SAME flush that produced dispatchedAt on the batch row -- every
// job row one flush inserts shares the identical dispatched_at timestamp
// (a flush coalesces N ingestion_ids, so there is no per-job FK to
// external_ingest_batches to join on instead). nil/zero dispatchedAt means
// no recompute has ever dispatched for this source, matching Python's
// "dispatched_at is None -> []" short circuit exactly.
func listRecomputeJobs(ctx context.Context, pool *pgxpool.Pool, orgID, sourceSystem, sourceInstance string, dispatchedAt *time.Time) ([]RecomputeJobRow, error) {
	if dispatchedAt == nil {
		return nil, nil
	}
	rows, err := pool.Query(ctx, `
		SELECT celery_task_name, celery_task_id, queue, repo_id
		FROM external_ingest_recompute_jobs
		WHERE org_id = $1 AND source_system = $2 AND source_instance = $3 AND dispatched_at = $4
		ORDER BY celery_task_name, repo_id
	`, orgID, sourceSystem, sourceInstance, *dispatchedAt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []RecomputeJobRow
	for rows.Next() {
		var j RecomputeJobRow
		if err := rows.Scan(&j.Task, &j.TaskID, &j.Queue, &j.RepoID); err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}
