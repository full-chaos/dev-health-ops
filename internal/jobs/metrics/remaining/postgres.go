package remaining

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrInvalidState = errors.New("remaining metrics durable state is invalid")
	ErrLeaseLost    = errors.New("remaining metrics execution lease was lost")
	ErrUnavailable  = errors.New("remaining metrics durable state is unavailable")
)

const defaultLease = 10 * time.Minute

const (
	maxGenerationLength = 128
	maxScopeKeyLength   = 512
	maxScopesPerRun     = 1024
)

// StartRunRequest is the immutable, persisted input for a remaining-metrics
// generation. Scopes are ordered deliberately: their ordinal is the durable
// work identity, not an implementation detail of a dispatcher.
type StartRunRequest struct {
	OrganizationID            string
	Family                    string
	Generation                string
	ScopeKey                  string
	GenerationSeed            *int64
	Scopes                    []json.RawMessage
	PrerequisiteCompletionKey string
}

type Run struct {
	ID             string
	OrganizationID string
	Family         string
	Generation     string
	ScopeKey       string
	Status         string
	Seed           *int64
}

type Partition struct {
	ID      string
	RunID   string
	Ordinal int
	Scope   json.RawMessage
}

type Claim struct {
	Partition     Partition
	Token         string
	LeaseDuration time.Duration
}

type PostgresStore struct {
	pool  *pgxpool.Pool
	lease time.Duration
	now   func() time.Time
}

type PartitionPublisher interface {
	PublishPartitionTx(context.Context, pgx.Tx, Run, Partition, string) error
}

func NewPostgresStore(pool *pgxpool.Pool) (*PostgresStore, error) {
	if pool == nil {
		return nil, ErrUnavailable
	}
	return &PostgresStore{pool: pool, lease: defaultLease, now: time.Now}, nil
}

// StartRun atomically persists a deterministic generation and every partition
// it owns. Retried queue deliveries are only accepted when their immutable
// seed, count, and ordered scopes exactly match the original request.
func (store *PostgresStore) StartRun(ctx context.Context, request StartRunRequest) (Run, error) {
	if !store.valid() {
		return Run{}, ErrUnavailable
	}
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return Run{}, ErrUnavailable
	}
	defer func() {
		rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}()
	run, err := store.StartRunTx(ctx, tx, request, nil)
	if err != nil {
		return Run{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Run{}, ErrUnavailable
	}
	return run, nil
}

// StartRunTx creates or verifies a deterministic run, every ordered
// partition, and each optional outbox handoff inside the caller's transaction.
// This is the post-sync fanout seam: the source transition cannot commit while
// any remaining-metrics domain row or deferred/executable handoff is missing.
// It never commits. Run identity is derived from organization, family,
// generation, and scope key; partition identity is derived from that run ID
// and the one-based scope ordinal. The returned Run carries the canonical ID.
func (store *PostgresStore) StartRunTx(
	ctx context.Context,
	tx pgx.Tx,
	request StartRunRequest,
	publisher PartitionPublisher,
) (Run, error) {
	if !store.valid() || tx == nil {
		return Run{}, ErrUnavailable
	}
	request.Scopes = cloneScopes(request.Scopes)
	if err := validateStartRunRequest(request); err != nil {
		return Run{}, ErrInvalidState
	}
	request.OrganizationID = uuid.MustParse(request.OrganizationID).String()
	for ordinal := range request.Scopes {
		canonical, err := validateFamilyScope(request.Family, request.Scopes[ordinal])
		if err != nil {
			return Run{}, ErrInvalidState
		}
		request.Scopes[ordinal], err = canonicalJSON(canonical)
		if err != nil {
			return Run{}, ErrInvalidState
		}
	}

	runID := deterministicRunID(request)
	now := store.now().UTC()
	command, err := tx.Exec(ctx, `
INSERT INTO public.remaining_metric_runs
    (id, org_id, family, generation, scope_key, generation_seed, status, created_at, updated_at)
VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, 'pending', $7, $7)
ON CONFLICT DO NOTHING`,
		runID, request.OrganizationID, request.Family, request.Generation, request.ScopeKey, request.GenerationSeed, now)
	if err != nil {
		return Run{}, ErrUnavailable
	}
	if command.RowsAffected() == 0 {
		run, err := loadStartedRun(ctx, tx, runID)
		if err != nil {
			return Run{}, err
		}
		if !sameRunSeed(run, request) || !sameRunIdentity(run, request) {
			return Run{}, ErrInvalidState
		}
		if run.Status == "succeeded" {
			completionKey, keyErr := joboutbox.CompletionKey("remaining_metric_run", run.ID)
			if keyErr != nil {
				return Run{}, ErrInvalidState
			}
			if err := joboutbox.MarkCompletionTx(ctx, tx, completionKey); err != nil {
				return Run{}, ErrUnavailable
			}
		}
		if err := verifyStartedPartitions(ctx, tx, runID, request.Scopes); err != nil {
			return Run{}, err
		}
		if err := publishStartedPartitions(
			ctx, tx, publisher, run, request.Scopes, request.PrerequisiteCompletionKey,
		); err != nil {
			return Run{}, err
		}
		return run, nil
	}

	run := Run{
		ID:             runID,
		OrganizationID: request.OrganizationID,
		Family:         request.Family,
		Generation:     request.Generation,
		ScopeKey:       request.ScopeKey,
		Status:         "pending",
		Seed:           request.GenerationSeed,
	}
	for index, scope := range request.Scopes {
		ordinal := index + 1
		partition := Partition{
			ID: deterministicPartitionID(runID, ordinal), RunID: runID,
			Ordinal: ordinal, Scope: scope,
		}
		_, err := tx.Exec(ctx, `
INSERT INTO public.remaining_metric_partitions
    (id, run_id, ordinal, scope, status, attempt_count, created_at, updated_at)
VALUES ($1::uuid, $2::uuid, $3, $4::jsonb, 'pending', 0, $5, $5)`,
			partition.ID, runID, ordinal, scope, now)
		if err != nil {
			return Run{}, ErrUnavailable
		}
		if publisher != nil {
			if err := publisher.PublishPartitionTx(
				ctx, tx, run, partition, request.PrerequisiteCompletionKey,
			); err != nil {
				return Run{}, err
			}
		}
	}
	return run, nil
}

func (store *PostgresStore) LoadRun(ctx context.Context, runID string) (Run, error) {
	if !store.valid() || !validUUID(runID) {
		return Run{}, ErrUnavailable
	}
	var run Run
	err := store.pool.QueryRow(ctx, `
SELECT id::text, org_id::text, family, generation, scope_key, status, generation_seed
FROM public.remaining_metric_runs WHERE id = $1::uuid`, runID).Scan(
		&run.ID, &run.OrganizationID, &run.Family, &run.Generation, &run.ScopeKey, &run.Status, &run.Seed,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return Run{}, ErrInvalidState
	}
	if err != nil {
		return Run{}, ErrUnavailable
	}
	return run, nil
}

// PendingPartitions returns only incomplete work in ordinal order, so a
// backfill retry never restarts a completed partition.
func (store *PostgresStore) PendingPartitions(ctx context.Context, runID string) ([]Partition, error) {
	if !store.valid() || !validUUID(runID) {
		return nil, ErrUnavailable
	}
	rows, err := store.pool.Query(ctx, `
SELECT partition.id::text, partition.run_id::text, partition.ordinal, partition.scope
FROM public.remaining_metric_partitions AS partition
JOIN public.remaining_metric_runs AS run ON run.id = partition.run_id
WHERE partition.run_id = $1::uuid AND partition.status IN ('pending', 'failed')
  AND run.status IN ('pending', 'running')
ORDER BY partition.ordinal`, runID)
	if err != nil {
		return nil, ErrUnavailable
	}
	defer rows.Close()
	var result []Partition
	for rows.Next() {
		var partition Partition
		if err := rows.Scan(&partition.ID, &partition.RunID, &partition.Ordinal, &partition.Scope); err != nil {
			return nil, ErrUnavailable
		}
		result = append(result, partition)
	}
	if rows.Err() != nil {
		return nil, ErrUnavailable
	}
	return result, nil
}

func (store *PostgresStore) ClaimPartition(ctx context.Context, partitionID string) (*Claim, error) {
	if !store.valid() || !validUUID(partitionID) {
		return nil, ErrUnavailable
	}
	now, token := store.now().UTC(), uuid.New()
	var claim Claim
	err := store.pool.QueryRow(ctx, `
WITH claimed AS (
UPDATE public.remaining_metric_partitions AS partition
SET status = 'running', claim_token = $2, lease_expires_at = $3,
    attempt_count = attempt_count + 1, updated_at = $1
WHERE partition.id = $4::uuid AND (
    status IN ('pending', 'failed') OR
    (status = 'running' AND lease_expires_at <= $1)
  )
  AND EXISTS (
      SELECT 1 FROM public.remaining_metric_runs AS run
      WHERE run.id = partition.run_id AND run.status IN ('pending', 'running')
  )
RETURNING partition.id::text, partition.run_id::text, partition.ordinal, partition.claim_token::text
), activated_run AS (
    UPDATE public.remaining_metric_runs AS run
    SET status = 'running', updated_at = $1
    WHERE run.id = (SELECT run_id::uuid FROM claimed) AND run.status = 'pending'
)
SELECT id, run_id, ordinal, claim_token FROM claimed`,
		now, token, now.Add(store.lease), partitionID,
	).Scan(&claim.Partition.ID, &claim.Partition.RunID, &claim.Partition.Ordinal, &claim.Token)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, ErrUnavailable
	}
	claim.LeaseDuration = store.lease
	return &claim, nil
}

func (store *PostgresStore) RenewPartition(ctx context.Context, claim Claim) error {
	if !store.validClaim(claim) {
		return ErrUnavailable
	}
	now := store.now().UTC()
	command, err := store.pool.Exec(ctx, `
UPDATE public.remaining_metric_partitions
SET lease_expires_at = $1, updated_at = $2
WHERE id = $3::uuid AND run_id = $4::uuid AND status = 'running'
  AND claim_token = $5::uuid AND lease_expires_at > $2
  AND EXISTS (
      SELECT 1 FROM public.remaining_metric_runs AS run
      WHERE run.id = remaining_metric_partitions.run_id AND run.status = 'running'
  )`, now.Add(store.lease), now, claim.Partition.ID, claim.Partition.RunID, claim.Token)
	if err != nil {
		return ErrUnavailable
	}
	if command.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	return nil
}

func (store *PostgresStore) CompletePartition(ctx context.Context, claim Claim, outputEvidence string) error {
	if !store.validClaim(claim) || outputEvidence == "" || len(outputEvidence) > 4096 {
		return ErrInvalidState
	}
	now := store.now().UTC()
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return ErrUnavailable
	}
	defer func() {
		rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}()
	command, err := tx.Exec(ctx, `
UPDATE public.remaining_metric_partitions
SET status = 'succeeded', output_evidence = $1, completed_at = $2,
    claim_token = NULL, lease_expires_at = NULL, updated_at = $2
WHERE id = $3::uuid AND run_id = $4::uuid AND status = 'running'
  AND claim_token = $5::uuid AND lease_expires_at > $2
  AND EXISTS (
      SELECT 1 FROM public.remaining_metric_runs AS run
      WHERE run.id = remaining_metric_partitions.run_id AND run.status = 'running'
  )`, outputEvidence, now, claim.Partition.ID, claim.Partition.RunID, claim.Token)
	if err != nil {
		return ErrUnavailable
	}
	if command.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	runTransition, err := tx.Exec(ctx, `
UPDATE public.remaining_metric_runs AS run
SET status = 'succeeded', updated_at = $1
WHERE run.id = $2::uuid AND run.status = 'running'
  AND EXISTS (
      SELECT 1 FROM public.remaining_metric_partitions AS partition
      WHERE partition.run_id = run.id
  )
  AND NOT EXISTS (
      SELECT 1 FROM public.remaining_metric_partitions AS partition
      WHERE partition.run_id = run.id AND partition.status <> 'succeeded'
  )`, now, claim.Partition.RunID)
	if err != nil {
		return ErrUnavailable
	}
	if runTransition.RowsAffected() == 1 {
		completionKey, keyErr := joboutbox.CompletionKey(
			"remaining_metric_run", claim.Partition.RunID,
		)
		if keyErr != nil {
			return ErrInvalidState
		}
		if err := joboutbox.MarkCompletionTx(ctx, tx, completionKey); err != nil {
			return ErrUnavailable
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return ErrUnavailable
	}
	return nil
}

func (store *PostgresStore) ReleasePartition(ctx context.Context, claim Claim) error {
	if !store.validClaim(claim) {
		return ErrUnavailable
	}
	now := store.now().UTC()
	command, err := store.pool.Exec(ctx, `
UPDATE public.remaining_metric_partitions
SET status = 'failed', claim_token = NULL, lease_expires_at = NULL, updated_at = $1
WHERE id = $2::uuid AND run_id = $3::uuid AND status = 'running'
  AND claim_token = $4::uuid AND lease_expires_at > $1
  AND EXISTS (
      SELECT 1 FROM public.remaining_metric_runs AS run
      WHERE run.id = remaining_metric_partitions.run_id AND run.status = 'running'
  )`, now, claim.Partition.ID, claim.Partition.RunID, claim.Token)
	if err != nil {
		return ErrUnavailable
	}
	if command.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	return nil
}

func (store *PostgresStore) CancelRun(ctx context.Context, runID string) error {
	if !store.valid() || !validUUID(runID) {
		return ErrUnavailable
	}
	command, err := store.pool.Exec(ctx, `
UPDATE public.remaining_metric_runs
SET status = 'canceled', canceled_at = $1, updated_at = $1
WHERE id = $2::uuid AND status IN ('pending', 'running')`, store.now().UTC(), runID)
	if err != nil {
		return ErrUnavailable
	}
	if command.RowsAffected() > 1 {
		return ErrInvalidState
	}
	return nil
}

func (store *PostgresStore) FinalizeRun(ctx context.Context, runID string) error {
	if !store.valid() || !validUUID(runID) {
		return ErrUnavailable
	}
	now := store.now().UTC()
	command, err := store.pool.Exec(ctx, `
UPDATE public.remaining_metric_runs AS run
SET status = 'succeeded', updated_at = $1
WHERE run.id = $2::uuid AND run.status = 'running'
  AND EXISTS (
      SELECT 1 FROM public.remaining_metric_partitions AS partition
      WHERE partition.run_id = run.id
  )
  AND NOT EXISTS (
      SELECT 1 FROM public.remaining_metric_partitions AS partition
      WHERE partition.run_id = run.id AND partition.status <> 'succeeded'
  )`, now, runID)
	if err != nil {
		return ErrUnavailable
	}
	if command.RowsAffected() == 1 {
		return nil
	}
	run, loadErr := store.LoadRun(ctx, runID)
	if loadErr == nil && run.Status == "succeeded" {
		return nil
	}
	return ErrInvalidState
}

func (store *PostgresStore) validClaim(claim Claim) bool {
	return store.valid() && validUUID(claim.Partition.ID) && validUUID(claim.Partition.RunID) && validUUID(claim.Token)
}

func (store *PostgresStore) valid() bool {
	return store != nil && store.pool != nil && store.now != nil && store.lease >= time.Second && store.lease <= time.Hour
}

func validateStartRunRequest(request StartRunRequest) error {
	if !validUUID(request.OrganizationID) ||
		utf8.RuneCountInString(request.Generation) < 1 || utf8.RuneCountInString(request.Generation) > maxGenerationLength ||
		utf8.RuneCountInString(request.ScopeKey) < 1 || utf8.RuneCountInString(request.ScopeKey) > maxScopeKeyLength ||
		len(request.Scopes) < 1 || len(request.Scopes) > maxScopesPerRun ||
		len(request.PrerequisiteCompletionKey) > 256 {
		return ErrInvalidState
	}
	inventory, err := Load()
	if err != nil {
		return err
	}
	found := false
	for _, family := range inventory.Families {
		if request.Family == family.Name {
			found = true
			break
		}
	}
	if !found || (request.Family == "capacity") != (request.GenerationSeed != nil) {
		return ErrInvalidState
	}
	return nil
}

func deterministicRunID(request StartRunRequest) string {
	identity, err := json.Marshal([]string{
		"remaining-metrics-run", request.OrganizationID, request.Family, request.Generation, request.ScopeKey,
	})
	if err != nil {
		panic("remaining metrics run identity cannot be encoded")
	}
	return uuid.NewSHA1(uuid.NameSpaceURL, identity).String()
}

func deterministicPartitionID(runID string, ordinal int) string {
	runUUID := uuid.MustParse(runID)
	return uuid.NewSHA1(runUUID, []byte("remaining-metrics-partition/"+strconv.Itoa(ordinal))).String()
}

func canonicalJSON(raw json.RawMessage) (json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil || value == nil {
		return nil, ErrInvalidState
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, ErrInvalidState
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return nil, ErrInvalidState
	}
	return canonical, nil
}

func cloneScopes(scopes []json.RawMessage) []json.RawMessage {
	cloned := make([]json.RawMessage, len(scopes))
	for index, scope := range scopes {
		cloned[index] = append(json.RawMessage(nil), scope...)
	}
	return cloned
}

func loadStartedRun(ctx context.Context, tx pgx.Tx, runID string) (Run, error) {
	var run Run
	err := tx.QueryRow(ctx, `
SELECT id::text, org_id::text, family, generation, scope_key, status, generation_seed
FROM public.remaining_metric_runs WHERE id = $1::uuid`, runID).Scan(
		&run.ID, &run.OrganizationID, &run.Family, &run.Generation, &run.ScopeKey, &run.Status, &run.Seed,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return Run{}, ErrInvalidState
	}
	if err != nil {
		return Run{}, ErrUnavailable
	}
	return run, nil
}

func sameRunIdentity(run Run, request StartRunRequest) bool {
	return run.OrganizationID == request.OrganizationID && run.Family == request.Family &&
		run.Generation == request.Generation && run.ScopeKey == request.ScopeKey
}

func sameRunSeed(run Run, request StartRunRequest) bool {
	if run.Seed == nil || request.GenerationSeed == nil {
		return run.Seed == nil && request.GenerationSeed == nil
	}
	return *run.Seed == *request.GenerationSeed
}

func verifyStartedPartitions(ctx context.Context, tx pgx.Tx, runID string, scopes []json.RawMessage) error {
	rows, err := tx.Query(ctx, `
SELECT id::text, ordinal, scope
FROM public.remaining_metric_partitions
WHERE run_id = $1::uuid ORDER BY ordinal`, runID)
	if err != nil {
		return ErrUnavailable
	}
	defer rows.Close()
	expectedCount := 0
	expectedOrdinal := 1
	for rows.Next() {
		var id string
		var persistedOrdinal int
		var persisted json.RawMessage
		if err := rows.Scan(&id, &persistedOrdinal, &persisted); err != nil {
			return ErrUnavailable
		}
		canonical, err := canonicalJSON(persisted)
		if err != nil {
			return ErrInvalidState
		}
		if persistedOrdinal != expectedOrdinal || expectedCount >= len(scopes) ||
			id != deterministicPartitionID(runID, expectedOrdinal) || !bytes.Equal(canonical, scopes[expectedCount]) {
			return ErrInvalidState
		}
		expectedCount++
		expectedOrdinal++
	}
	if err := rows.Err(); err != nil {
		return ErrUnavailable
	}
	if expectedCount != len(scopes) {
		return fmt.Errorf("%w: partition count mismatch", ErrInvalidState)
	}
	return nil
}

func publishStartedPartitions(
	ctx context.Context,
	tx pgx.Tx,
	publisher PartitionPublisher,
	run Run,
	scopes []json.RawMessage,
	prerequisiteCompletionKey string,
) error {
	if publisher == nil {
		return nil
	}
	for index, scope := range scopes {
		ordinal := index + 1
		if err := publisher.PublishPartitionTx(ctx, tx, run, Partition{
			ID:      deterministicPartitionID(run.ID, ordinal),
			RunID:   run.ID,
			Ordinal: ordinal,
			Scope:   scope,
		}, prerequisiteCompletionKey); err != nil {
			return err
		}
	}
	return nil
}

func validUUID(value string) bool {
	_, err := uuid.Parse(value)
	return err == nil
}
