package externalrecompute

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/streamhandlers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	CompatibilityBridgeKind = "external_ingest.recompute.compat.v1"
	CompatibilityTaskName   = "dev_health_ops.workers.tasks.dispatch_external_ingest_recompute_bridge"
)

type PostgresCompatibilityDispatcher struct {
	pool *pgxpool.Pool
	now  func() time.Time
}

func NewPostgresCompatibilityDispatcher(pool *pgxpool.Pool) (*PostgresCompatibilityDispatcher, error) {
	if pool == nil {
		return nil, ErrInvalidConfig
	}
	return &PostgresCompatibilityDispatcher{pool: pool, now: time.Now}, nil
}

type bridgeScope struct {
	BridgeVersion   int      `json:"bridgeVersion"`
	BridgeKind      string   `json:"bridgeKind"`
	BridgeID        string   `json:"bridgeId"`
	RepoIDs         []string `json:"repoIds"`
	TeamIDs         []string `json:"teamIds"`
	RecordKinds     []string `json:"recordKinds"`
	WindowStartedAt *string  `json:"windowStartedAt"`
	WindowEndedAt   *string  `json:"windowEndedAt"`
}

func (dispatcher *PostgresCompatibilityDispatcher) Dispatch(ctx context.Context, claim Claim) error {
	if dispatcher == nil || dispatcher.pool == nil || claim.ID == "" || validateScope(claim.Scope) != nil {
		return ErrInvalidConfig
	}
	ingestionIDs := claimIngestionIDs(claim)
	bridgeID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("external-ingest-recompute-bridge:"+claim.ID))
	payload := bridgeScope{
		BridgeVersion: 1, BridgeKind: CompatibilityBridgeKind, BridgeID: bridgeID.String(),
		RepoIDs: sortedUnique(claim.Scope.RepoIDs), TeamIDs: sortedUnique(claim.Scope.TeamIDs),
		RecordKinds: sortedUnique(claim.Scope.RecordKinds),
	}
	if claim.Scope.WindowStart != nil {
		value := claim.Scope.WindowStart.UTC().Format(time.RFC3339Nano)
		payload.WindowStartedAt = &value
	}
	if claim.Scope.WindowEnd != nil {
		value := claim.Scope.WindowEnd.UTC().Format(time.RFC3339Nano)
		payload.WindowEndedAt = &value
	}
	scopeJSON, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal external recompute bridge: %w", err)
	}
	now := dispatcher.now().UTC()
	tx, err := dispatcher.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin external recompute bridge: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// The deterministic primary key is the crash-after-dispatch dedupe
	// boundary. Retrying an inflight Valkey claim cannot create a second
	// compatibility row or an arbitrary Python task identity.
	if _, err := tx.Exec(ctx, `
		INSERT INTO external_ingest_recompute_jobs (
			id, org_id, source_system, source_instance, celery_task_name,
			celery_task_id, queue, repo_id, status, dispatched_at
		) VALUES ($1,$2,$3,$4,$5,$6,'default',NULL,'bridge_pending',$7)
		ON CONFLICT (id) DO NOTHING
	`, bridgeID, claim.Scope.OrgID, claim.Scope.SourceSystem,
		claim.Scope.SourceInstance, CompatibilityTaskName, bridgeID.String(), now); err != nil {
		return fmt.Errorf("persist external recompute bridge identity: %w", err)
	}
	for _, ingestionID := range ingestionIDs {
		if _, err := tx.Exec(ctx, `
			UPDATE external_ingest_batches
			SET recompute_scope = $3, updated_at = $4
			WHERE org_id = $1 AND ingestion_id = $2
			  AND recompute_status = 'pending'
		`, claim.Scope.OrgID, ingestionID, scopeJSON, now); err != nil {
			return fmt.Errorf("persist external recompute batch bridge: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit external recompute bridge: %w", err)
	}
	return nil
}

func (dispatcher *PostgresCompatibilityDispatcher) PendingScopes(
	ctx context.Context,
	limit int,
) ([]streamhandlers.ExternalRecomputeScope, error) {
	if dispatcher == nil || dispatcher.pool == nil || limit < 1 {
		return nil, ErrInvalidConfig
	}
	rows, err := dispatcher.pool.Query(ctx, `
		SELECT ingestion_id, org_id, source_system, source_instance, recompute_scope
		FROM external_ingest_batches
		WHERE recompute_status = 'pending' AND recompute_scope IS NOT NULL
		  AND COALESCE(recompute_scope ->> 'bridgeKind', '') <> $2
		ORDER BY updated_at, ingestion_id
		LIMIT $1
	`, limit, CompatibilityBridgeKind)
	if err != nil {
		return nil, fmt.Errorf("scan pending external recompute scopes: %w", err)
	}
	defer rows.Close()
	scopes := make([]streamhandlers.ExternalRecomputeScope, 0, limit)
	for rows.Next() {
		var (
			ingestionID                         uuid.UUID
			orgID, sourceSystem, sourceInstance string
			raw                                 []byte
		)
		if err := rows.Scan(&ingestionID, &orgID, &sourceSystem, &sourceInstance, &raw); err != nil {
			return nil, fmt.Errorf("scan pending external recompute scope: %w", err)
		}
		scope, bridged, err := pendingScopeFromJSON(
			raw,
			ingestionID,
			orgID,
			sourceSystem,
			sourceInstance,
		)
		if err != nil {
			return nil, err
		}
		if !bridged {
			scopes = append(scopes, scope)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending external recompute scopes: %w", err)
	}
	return scopes, nil
}

func pendingScopeFromJSON(
	raw []byte,
	ingestionID uuid.UUID,
	orgID, sourceSystem, sourceInstance string,
) (streamhandlers.ExternalRecomputeScope, bool, error) {
	var value struct {
		BridgeKind  string `json:"bridgeKind"`
		RepoIDs     []string
		TeamIDs     []string
		RecordKinds []string
		WindowStart *time.Time
		WindowEnd   *time.Time

		RepoIDsCamel         []string `json:"repoIds"`
		TeamIDsCamel         []string `json:"teamIds"`
		RecordKindsCamel     []string `json:"recordKinds"`
		WindowStartedAtCamel *string  `json:"windowStartedAt"`
		WindowEndedAtCamel   *string  `json:"windowEndedAt"`
	}
	if err := json.Unmarshal(raw, &value); err != nil {
		return streamhandlers.ExternalRecomputeScope{}, false, fmt.Errorf("decode pending external recompute scope: %w", err)
	}
	if value.BridgeKind != "" {
		if value.BridgeKind != CompatibilityBridgeKind {
			return streamhandlers.ExternalRecomputeScope{}, false, fmt.Errorf("unsupported external recompute bridge kind")
		}
		return streamhandlers.ExternalRecomputeScope{}, true, nil
	}
	scope := streamhandlers.ExternalRecomputeScope{
		OrgID: orgID, SourceSystem: sourceSystem, SourceInstance: sourceInstance,
		IngestionID: ingestionID,
		RepoIDs:     firstStrings(value.RepoIDsCamel, value.RepoIDs),
		TeamIDs:     firstStrings(value.TeamIDsCamel, value.TeamIDs),
		RecordKinds: firstStrings(value.RecordKindsCamel, value.RecordKinds),
		WindowStart: value.WindowStart, WindowEnd: value.WindowEnd,
	}
	var err error
	if scope.WindowStart == nil {
		scope.WindowStart, err = parseOptionalBridgeTime(value.WindowStartedAtCamel)
		if err != nil {
			return streamhandlers.ExternalRecomputeScope{}, false, err
		}
	}
	if scope.WindowEnd == nil {
		scope.WindowEnd, err = parseOptionalBridgeTime(value.WindowEndedAtCamel)
		if err != nil {
			return streamhandlers.ExternalRecomputeScope{}, false, err
		}
	}
	return canonicalScope(scope), false, nil
}

func claimIngestionIDs(claim Claim) []string {
	if len(claim.ingestionIDs) > 0 {
		return sortedUnique(claim.ingestionIDs)
	}
	return []string{claim.Scope.IngestionID.String()}
}

func firstStrings(primary, fallback []string) []string {
	if primary != nil {
		return primary
	}
	return fallback
}

func parseOptionalBridgeTime(raw *string) (*time.Time, error) {
	if raw == nil || *raw == "" {
		return nil, nil
	}
	value, err := time.Parse(time.RFC3339Nano, *raw)
	if err != nil {
		return nil, fmt.Errorf("parse external recompute bridge time: %w", err)
	}
	return &value, nil
}
