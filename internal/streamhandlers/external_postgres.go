package streamhandlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/streamrunner"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const maxExternalRejections = 1000

type PostgresExternalBatchRepository struct {
	pool *pgxpool.Pool
	now  func() time.Time
}

func NewPostgresExternalBatchRepository(pool *pgxpool.Pool) (*PostgresExternalBatchRepository, error) {
	if pool == nil {
		return nil, streamrunner.ErrInvalidConfig
	}
	return &PostgresExternalBatchRepository{pool: pool, now: time.Now}, nil
}

func (r *PostgresExternalBatchRepository) OperationalAllowed(ctx context.Context, orgID string) (bool, error) {
	for _, feature := range []struct {
		key     string
		minTier string
	}{
		{key: "customer_push_ingest", minTier: "team"},
		{key: "canonical_incident_ingestion", minTier: "community"},
	} {
		allowed, err := r.featureAllowed(ctx, orgID, feature.key, feature.minTier)
		if err != nil || !allowed {
			return false, err
		}
	}
	return true, nil
}

func (r *PostgresExternalBatchRepository) featureAllowed(ctx context.Context, orgID, key, expectedMinTier string) (bool, error) {
	var (
		globallyEnabled bool
		minTier         string
		orgTier         *string
		licenseTier     *string
		licenseFeatures []byte
		overrideEnabled *bool
		overrideExpires *time.Time
	)
	err := r.pool.QueryRow(ctx, `
		SELECT feature.is_enabled, feature.min_tier, organization.tier,
		       license.tier, license.features_override,
		       override.is_enabled, override.expires_at
		FROM feature_flags AS feature
		JOIN organizations AS organization ON organization.id = $1
		LEFT JOIN org_licenses AS license ON license.org_id = organization.id
		LEFT JOIN org_feature_overrides AS override
		  ON override.org_id = organization.id AND override.feature_id = feature.id
		WHERE feature.key = $2
	`, orgID, key).Scan(
		&globallyEnabled, &minTier, &orgTier, &licenseTier, &licenseFeatures,
		&overrideEnabled, &overrideExpires,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("load feature decision: %w", err)
	}
	if !globallyEnabled || minTier != expectedMinTier {
		return false, nil
	}
	tier := "community"
	if orgTier != nil {
		tier = *orgTier
	}
	if licenseTier != nil {
		tier = *licenseTier
	}
	tierAllowed := externalTierRank(tier) >= externalTierRank(minTier)
	if overrideEnabled != nil && (overrideExpires == nil || overrideExpires.After(r.now().UTC())) {
		if !*overrideEnabled {
			return false, nil
		}
		return key != "customer_push_ingest" || tierAllowed, nil
	}
	if len(licenseFeatures) > 0 {
		var overrides map[string]bool
		if err := json.Unmarshal(licenseFeatures, &overrides); err != nil {
			return false, nil
		}
		if enabled, exists := overrides[key]; exists {
			if !enabled {
				return false, nil
			}
			return key != "customer_push_ingest" || tierAllowed, nil
		}
	}
	return tierAllowed, nil
}

func externalTierRank(tier string) int {
	switch tier {
	case "enterprise":
		return 2
	case "team":
		return 1
	default:
		return 0
	}
}

func (r *PostgresExternalBatchRepository) LoadForProcessing(ctx context.Context, pointer externalPointer) (externalBatch, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return externalBatch{}, fmt.Errorf("begin external status claim: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var (
		status, entityFamily   string
		itemsReceived          int
		windowStart, windowEnd *time.Time
	)
	err = tx.QueryRow(ctx, `
		SELECT status, items_received, entity_family, window_started_at, window_ended_at
		FROM external_ingest_batches
		WHERE org_id = $1 AND ingestion_id = $2
		FOR UPDATE
	`, pointer.OrgID, pointer.IngestionID.String()).Scan(&status, &itemsReceived, &entityFamily, &windowStart, &windowEnd)
	if errors.Is(err, pgx.ErrNoRows) {
		return externalBatch{}, &streamrunner.PermanentError{Reason: "external_status_missing"}
	}
	if err != nil {
		return externalBatch{}, fmt.Errorf("load external status: %w", err)
	}
	if isExternalTerminal(status) {
		if err := tx.Commit(ctx); err != nil {
			return externalBatch{}, fmt.Errorf("commit terminal external skip: %w", err)
		}
		return externalBatch{Pointer: pointer, Skip: true}, nil
	}
	switch status {
	case "accepted", "stream_unavailable":
		if _, err := tx.Exec(ctx, `
			UPDATE external_ingest_batches
			SET status = 'processing', updated_at = $3
			WHERE org_id = $1 AND ingestion_id = $2
			  AND status IN ('accepted', 'stream_unavailable')
		`, pointer.OrgID, pointer.IngestionID.String(), r.now().UTC()); err != nil {
			return externalBatch{}, fmt.Errorf("mark external processing: %w", err)
		}
	case "processing":
		// Reclaim/restart resumes the existing processing attempt.
	default:
		return externalBatch{}, &streamrunner.PermanentError{Reason: "external_status_invalid"}
	}
	// Commit-before-risky: the status is visible before payload parsing or any
	// ClickHouse write, so a kill cannot leave an apparently untouched batch.
	if err := tx.Commit(ctx); err != nil {
		return externalBatch{}, fmt.Errorf("commit external processing status: %w", err)
	}

	var payload []byte
	if err := r.pool.QueryRow(ctx, `
		SELECT payload_json
		FROM external_ingest_batch_payloads
		WHERE org_id = $1 AND ingestion_id = $2
	`, pointer.OrgID, pointer.IngestionID.String()).Scan(&payload); errors.Is(err, pgx.ErrNoRows) {
		return externalBatch{}, &streamrunner.PermanentError{Reason: "external_payload_missing"}
	} else if err != nil {
		return externalBatch{}, fmt.Errorf("load external payload: %w", err)
	}

	var sourceID uuid.UUID
	if err := r.pool.QueryRow(ctx, `
		SELECT id
		FROM external_ingest_sources
		WHERE org_id = $1 AND lower(system) = lower($2)
		  AND lower(instance) = lower($3) AND entity_family = $4
		ORDER BY CASE WHEN enabled = TRUE AND mode = 'customer_push' THEN 0 ELSE 1 END,
		         created_at
		LIMIT 1
	`, pointer.OrgID, pointer.SourceSystem, pointer.SourceInstance, entityFamily).Scan(&sourceID); errors.Is(err, pgx.ErrNoRows) {
		return externalBatch{}, &streamrunner.PermanentError{Reason: "external_source_unregistered"}
	} else if err != nil {
		return externalBatch{}, fmt.Errorf("resolve external source provenance: %w", err)
	}
	return externalBatch{
		Pointer: pointer, SourceID: sourceID, EntityFamily: entityFamily,
		ItemsReceived: itemsReceived, WindowStartedAt: windowStart,
		WindowEndedAt: windowEnd, Payload: payload,
	}, nil
}

func (r *PostgresExternalBatchRepository) Complete(ctx context.Context, batch externalBatch, completion externalCompletion) error {
	if completion.Accepted < 0 || completion.Rejected < 0 ||
		completion.Accepted+completion.Rejected != batch.ItemsReceived {
		return fmt.Errorf("external completion counts do not match received count")
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin external completion: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var status string
	if err := tx.QueryRow(ctx, `
		SELECT status FROM external_ingest_batches
		WHERE org_id = $1 AND ingestion_id = $2 FOR UPDATE
	`, batch.Pointer.OrgID, batch.Pointer.IngestionID.String()).Scan(&status); err != nil {
		return fmt.Errorf("lock external completion: %w", err)
	}
	if isExternalTerminal(status) {
		return tx.Commit(ctx)
	}
	if status != "processing" {
		return fmt.Errorf("external completion requires processing status")
	}
	terminal := externalTerminalStatus(batch.ItemsReceived, completion.Accepted, completion.Rejected)
	stored := completion.Rejections
	if len(stored) > maxExternalRejections {
		stored = stored[:maxExternalRejections]
	}
	errorSummary, err := externalErrorSummary(completion.Rejected, stored)
	if err != nil {
		return err
	}
	recordCounts, err := json.Marshal(completion.RecordCounts)
	if err != nil {
		return fmt.Errorf("marshal external record counts: %w", err)
	}
	scopeJSON, err := json.Marshal(externalStoredRecomputeScope(completion.Scope))
	if err != nil {
		return fmt.Errorf("marshal external recompute scope: %w", err)
	}
	now := r.now().UTC()
	recomputeStatus := "not_applicable"
	var recomputeScope any
	if completion.Accepted > 0 {
		recomputeStatus, recomputeScope = "pending", scopeJSON
	}
	tag, err := tx.Exec(ctx, `
		UPDATE external_ingest_batches
		SET status = $3, items_accepted = $4, items_rejected = $5,
		    record_counts = $6, error_summary = $7, completed_at = $8,
		    updated_at = $8, recompute_status = $9, recompute_scope = $10,
		    recompute_dispatched_at = NULL, recompute_completed_at = NULL,
		    recompute_error = NULL
		WHERE org_id = $1 AND ingestion_id = $2 AND status = 'processing'
	`, batch.Pointer.OrgID, batch.Pointer.IngestionID.String(), terminal,
		completion.Accepted, completion.Rejected, recordCounts, errorSummary, now,
		recomputeStatus, recomputeScope)
	if err != nil {
		return fmt.Errorf("complete external status: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("external completion lost status CAS")
	}
	for _, rejection := range stored {
		if _, err := tx.Exec(ctx, `
			INSERT INTO external_ingest_rejections (
				id, org_id, ingestion_id, record_index, record_kind,
				external_id, code, message, path, created_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		`, uuid.New().String(), batch.Pointer.OrgID, batch.Pointer.IngestionID.String(),
			rejection.Index, rejection.Kind, nullableExternalText(rejection.ExternalID),
			rejection.Code, rejection.Message, nullableExternalText(rejection.Path), now); err != nil {
			return fmt.Errorf("persist external rejection: %w", err)
		}
	}
	if _, err := tx.Exec(ctx, `
		DELETE FROM external_ingest_batch_payloads
		WHERE org_id = $1 AND ingestion_id = $2
	`, batch.Pointer.OrgID, batch.Pointer.IngestionID.String()); err != nil {
		return fmt.Errorf("delete terminal external payload: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit external completion: %w", err)
	}
	return nil
}

func (r *PostgresExternalBatchRepository) Fail(ctx context.Context, pointer externalPointer, reason string) error {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin external failure: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	summary, err := json.Marshal(map[string]any{
		"total_rejected": 0, "stored_rejections": 0, "truncated": false,
		"top_codes": []map[string]any{{"code": "processing_failed", "count": 1}},
		"reason":    reason,
	})
	if err != nil {
		return err
	}
	now := r.now().UTC()
	tag, err := tx.Exec(ctx, `
		UPDATE external_ingest_batches
		SET status = 'failed', error_summary = $3, completed_at = $4, updated_at = $4
		WHERE org_id = $1 AND ingestion_id = $2
		  AND status NOT IN ('completed', 'partial', 'failed')
	`, pointer.OrgID, pointer.IngestionID.String(), summary, now)
	if err != nil {
		return fmt.Errorf("mark external failed: %w", err)
	}
	if tag.RowsAffected() > 0 {
		if _, err := tx.Exec(ctx, `
			DELETE FROM external_ingest_batch_payloads
			WHERE org_id = $1 AND ingestion_id = $2
		`, pointer.OrgID, pointer.IngestionID.String()); err != nil {
			return fmt.Errorf("delete failed external payload: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit external failure: %w", err)
	}
	return nil
}

func isExternalTerminal(status string) bool {
	return status == "completed" || status == "partial" || status == "failed"
}

func externalTerminalStatus(received, accepted, rejected int) string {
	if received <= 0 || accepted+rejected != received {
		return "failed"
	}
	if rejected == 0 {
		return "completed"
	}
	if accepted == 0 {
		return "failed"
	}
	return "partial"
}

func externalErrorSummary(total int, stored []externalRejection) ([]byte, error) {
	if total == 0 {
		return nil, nil
	}
	counts := make(map[string]int)
	for _, rejection := range stored {
		counts[rejection.Code]++
	}
	type codeCount struct {
		Code  string `json:"code"`
		Count int    `json:"count"`
	}
	top := make([]codeCount, 0, len(counts))
	for code, count := range counts {
		top = append(top, codeCount{Code: code, Count: count})
	}
	slices.SortFunc(top, func(left, right codeCount) int {
		if left.Count != right.Count {
			return right.Count - left.Count
		}
		return strings.Compare(left.Code, right.Code)
	})
	return json.Marshal(map[string]any{
		"total_rejected": total, "stored_rejections": len(stored),
		"truncated": total > len(stored), "top_codes": top,
	})
}

func nullableExternalText(value string) any {
	if value == "" {
		return nil
	}
	return value
}

type externalStoredScope struct {
	RepoIDs         []string `json:"repoIds"`
	TeamIDs         []string `json:"teamIds"`
	RecordKinds     []string `json:"recordKinds"`
	WindowStartedAt *string  `json:"windowStartedAt"`
	WindowEndedAt   *string  `json:"windowEndedAt"`
}

func externalStoredRecomputeScope(scope ExternalRecomputeScope) externalStoredScope {
	stored := externalStoredScope{
		RepoIDs:     sortedExternalStrings(scope.RepoIDs),
		TeamIDs:     sortedExternalStrings(scope.TeamIDs),
		RecordKinds: sortedExternalStrings(scope.RecordKinds),
	}
	if scope.WindowStart != nil {
		value := scope.WindowStart.UTC().Format(time.RFC3339Nano)
		stored.WindowStartedAt = &value
	}
	if scope.WindowEnd != nil {
		value := scope.WindowEnd.UTC().Format(time.RFC3339Nano)
		stored.WindowEndedAt = &value
	}
	return stored
}
