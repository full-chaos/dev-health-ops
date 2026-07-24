package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/jackc/pgx/v5"
)

const (
	generationJournalResultKey       = "go_generation_v2"
	generationJournalLegacyResultKey = "go_generation_v1"
	maxRecoveryPayloadRows           = 1
	maxRecoveryPayloadBytes          = 16 << 10
	maxGenerationJournalStateBytes   = 24 << 10
)

type GenerationBlockStatus string
type GenerationBlockResolution string

const (
	GenerationBlockPending   GenerationBlockStatus = "pending"
	GenerationBlockWriting   GenerationBlockStatus = "writing"
	GenerationBlockCommitted GenerationBlockStatus = "committed"

	GenerationBlockRetryPending  GenerationBlockResolution = "retry_pending"
	GenerationBlockMarkCommitted GenerationBlockResolution = "mark_committed"
)

type GenerationJournalBlock struct {
	Index           int                                     `json:"index"`
	ContentDigest   string                                  `json:"content_digest"`
	Status          GenerationBlockStatus                   `json:"status"`
	RecoveryPayload []providerfoundation.NormalizedEnvelope `json:"recovery_payload"`
	StartedAt       *time.Time                              `json:"started_at,omitempty"`
	CommittedAt     *time.Time                              `json:"committed_at,omitempty"`
}

type GenerationJournalState struct {
	SchemaVersion string                   `json:"schema_version"`
	Generation    string                   `json:"generation"`
	Destination   string                   `json:"destination"`
	Blocks        []GenerationJournalBlock `json:"blocks"`
	CreatedAt     time.Time                `json:"created_at"`
	UpdatedAt     time.Time                `json:"updated_at"`
}

func NewGenerationJournalState(
	blocks []providerfoundation.GenerationBlock,
	now time.Time,
) (GenerationJournalState, error) {
	if len(blocks) != 1 || now.IsZero() {
		return GenerationJournalState{}, ErrGenerationRecoveryUnsafe
	}
	recoveryPayload := blocks[0].Batch()
	if len(recoveryPayload) != maxRecoveryPayloadRows ||
		recoveryPayload[0].EntityType != "repository" {
		return GenerationJournalState{}, ErrGenerationRecoveryUnsafe
	}
	encodedPayload, err := json.Marshal(recoveryPayload)
	if err != nil || len(encodedPayload) > maxRecoveryPayloadBytes {
		return GenerationJournalState{}, ErrGenerationRecoveryUnsafe
	}
	if blocks[0].Index() != 0 || blocks[0].ContentDigest() == "" {
		return GenerationJournalState{}, ErrInvalidConfiguration
	}
	state := GenerationJournalState{
		SchemaVersion: "v2",
		Generation:    blocks[0].Generation(),
		Destination:   blocks[0].Destination(),
		CreatedAt:     now.UTC(),
		UpdatedAt:     now.UTC(),
		Blocks: []GenerationJournalBlock{{
			Index: 0, ContentDigest: blocks[0].ContentDigest(),
			Status: GenerationBlockPending, RecoveryPayload: recoveryPayload,
		}},
	}
	if state.validate() != nil {
		return GenerationJournalState{}, ErrGenerationRecoveryUnsafe
	}
	return state, nil
}

func (state GenerationJournalState) validate() error {
	if state.SchemaVersion != "v2" || state.Generation == "" ||
		state.Destination == "" || len(state.Blocks) != 1 ||
		state.CreatedAt.IsZero() || state.UpdatedAt.IsZero() {
		return ErrInvalidConfiguration
	}
	for index, block := range state.Blocks {
		if block.Index != index || block.ContentDigest == "" {
			return ErrInvalidConfiguration
		}
		switch block.Status {
		case GenerationBlockPending:
			if block.StartedAt != nil || block.CommittedAt != nil {
				return ErrInvalidConfiguration
			}
		case GenerationBlockWriting:
			if block.StartedAt == nil || block.CommittedAt != nil {
				return ErrInvalidConfiguration
			}
		case GenerationBlockCommitted:
			if block.StartedAt == nil || block.CommittedAt == nil {
				return ErrInvalidConfiguration
			}
		default:
			return ErrInvalidConfiguration
		}
	}
	if _, err := state.recoveryBlocks(); err != nil {
		return err
	}
	return nil
}

func (state GenerationJournalState) recoveryBlocks() ([]providerfoundation.GenerationBlock, error) {
	if len(state.Blocks) != 1 ||
		len(state.Blocks[0].RecoveryPayload) != maxRecoveryPayloadRows ||
		state.Blocks[0].RecoveryPayload[0].EntityType != "repository" {
		return nil, ErrGenerationRecoveryUnsafe
	}
	encodedPayload, err := json.Marshal(state.Blocks[0].RecoveryPayload)
	if err != nil || len(encodedPayload) > maxRecoveryPayloadBytes {
		return nil, ErrGenerationRecoveryUnsafe
	}
	blocks, err := providerfoundation.BuildGenerationBlocks(
		state.Generation, state.Destination, state.Blocks[0].RecoveryPayload,
	)
	if err != nil || len(blocks) != 1 || blocks[0].Index() != 0 ||
		blocks[0].ContentDigest() != state.Blocks[0].ContentDigest {
		return nil, ErrGenerationRecoveryUnsafe
	}
	return blocks, nil
}

func encodeGenerationJournalState(state GenerationJournalState) ([]byte, error) {
	if state.validate() != nil {
		return nil, ErrGenerationJournalConflict
	}
	encoded, err := json.Marshal(state)
	if err != nil || len(encoded) > maxGenerationJournalStateBytes {
		return nil, ErrGenerationRecoveryUnsafe
	}
	return encoded, nil
}

func decodeGenerationJournalState(raw []byte) (GenerationJournalState, error) {
	if len(raw) == 0 || len(raw) > maxGenerationJournalStateBytes {
		return GenerationJournalState{}, ErrGenerationRecoveryUnsafe
	}
	var state GenerationJournalState
	if json.Unmarshal(raw, &state) != nil || state.validate() != nil {
		return GenerationJournalState{}, ErrGenerationJournalConflict
	}
	return state, nil
}

func sameGenerationManifest(left, right GenerationJournalState) bool {
	if left.SchemaVersion != right.SchemaVersion ||
		left.Generation != right.Generation ||
		left.Destination != right.Destination ||
		len(left.Blocks) != len(right.Blocks) {
		return false
	}
	for index := range left.Blocks {
		if left.Blocks[index].Index != right.Blocks[index].Index ||
			left.Blocks[index].ContentDigest != right.Blocks[index].ContentDigest {
			return false
		}
	}
	return true
}

type GenerationJournal interface {
	Load(context.Context, Claim, time.Time) (GenerationJournalState, error)
	Prepare(context.Context, Claim, GenerationJournalState, time.Time) (GenerationJournalState, error)
	BeginBlock(context.Context, Claim, int, string, time.Time) error
	CommitBlock(context.Context, Claim, int, string, time.Time) error
	ResolveBlock(context.Context, Claim, int, string, GenerationBlockResolution, time.Time) error
}

func (repository *PostgresRepository) Load(
	ctx context.Context,
	claim Claim,
	now time.Time,
) (GenerationJournalState, error) {
	if repository == nil || repository.Pool == nil || ctx == nil ||
		claim.Validate() != nil || now.IsZero() {
		return GenerationJournalState{}, ErrInvalidConfiguration
	}
	var stateRaw []byte
	var legacyStatePresent bool
	if err := repository.Pool.QueryRow(
		ctx, loadGenerationJournalSQL, claim.ID, claim.Owner, now.UTC(),
	).Scan(&stateRaw, &legacyStatePresent); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return GenerationJournalState{}, ErrLeaseLost
		}
		return GenerationJournalState{}, ErrGenerationJournalConflict
	}
	if len(stateRaw) == 0 && legacyStatePresent {
		return GenerationJournalState{}, ErrGenerationRecoveryUnsafe
	}
	return decodeGenerationJournalState(stateRaw)
}

func (repository *PostgresRepository) Prepare(
	ctx context.Context,
	claim Claim,
	desired GenerationJournalState,
	now time.Time,
) (GenerationJournalState, error) {
	if desired.validate() != nil || now.IsZero() {
		return GenerationJournalState{}, ErrInvalidConfiguration
	}
	var prepared GenerationJournalState
	err := repository.mutateGenerationJournal(ctx, claim, now, func(document map[string]json.RawMessage) error {
		raw := document[generationJournalResultKey]
		if len(raw) == 0 && len(document[generationJournalLegacyResultKey]) != 0 {
			return ErrGenerationRecoveryUnsafe
		}
		if len(raw) != 0 {
			var err error
			prepared, err = decodeGenerationJournalState(raw)
			if err != nil {
				return err
			}
			if !sameGenerationManifest(prepared, desired) {
				return ErrGenerationJournalConflict
			}
			return nil
		}
		prepared = desired
		prepared.CreatedAt = now.UTC()
		prepared.UpdatedAt = now.UTC()
		encoded, err := encodeGenerationJournalState(prepared)
		if err != nil {
			return err
		}
		document[generationJournalResultKey] = encoded
		return nil
	})
	return prepared, err
}

func (repository *PostgresRepository) BeginBlock(
	ctx context.Context,
	claim Claim,
	index int,
	digest string,
	now time.Time,
) error {
	return repository.transitionGenerationBlock(
		ctx, claim, index, digest, now, GenerationBlockPending, GenerationBlockWriting,
	)
}

func (repository *PostgresRepository) CommitBlock(
	ctx context.Context,
	claim Claim,
	index int,
	digest string,
	now time.Time,
) error {
	return repository.transitionGenerationBlock(
		ctx, claim, index, digest, now, GenerationBlockWriting, GenerationBlockCommitted,
	)
}

// ResolveBlock is intentionally separate from CommitBlock: only an operator
// readback path may use it to decide whether a writing block is already exact
// in ClickHouse or wholly absent and therefore safe to retry.
func (repository *PostgresRepository) ResolveBlock(
	ctx context.Context,
	claim Claim,
	index int,
	digest string,
	resolution GenerationBlockResolution,
	now time.Time,
) error {
	if index < 0 || digest == "" || now.IsZero() {
		return ErrInvalidConfiguration
	}
	return repository.mutateGenerationJournal(ctx, claim, now, func(document map[string]json.RawMessage) error {
		state, decodeErr := decodeGenerationJournalState(document[generationJournalResultKey])
		if decodeErr != nil || index >= len(state.Blocks) ||
			state.Blocks[index].ContentDigest != digest {
			return ErrGenerationJournalConflict
		}
		block := &state.Blocks[index]
		now = now.UTC()
		switch resolution {
		case GenerationBlockMarkCommitted:
			if block.Status == GenerationBlockCommitted {
				return nil
			}
			if block.Status != GenerationBlockWriting {
				return ErrGenerationJournalConflict
			}
			block.Status = GenerationBlockCommitted
			block.CommittedAt = &now
		case GenerationBlockRetryPending:
			if block.Status == GenerationBlockPending {
				return nil
			}
			if block.Status != GenerationBlockWriting {
				return ErrGenerationJournalConflict
			}
			block.Status = GenerationBlockPending
			block.StartedAt = nil
			block.CommittedAt = nil
		default:
			return ErrInvalidConfiguration
		}
		state.UpdatedAt = now
		encoded, err := encodeGenerationJournalState(state)
		if err != nil {
			return err
		}
		document[generationJournalResultKey] = encoded
		return nil
	})
}

func (repository *PostgresRepository) transitionGenerationBlock(
	ctx context.Context,
	claim Claim,
	index int,
	digest string,
	now time.Time,
	from GenerationBlockStatus,
	to GenerationBlockStatus,
) error {
	if index < 0 || digest == "" || now.IsZero() {
		return ErrInvalidConfiguration
	}
	return repository.mutateGenerationJournal(ctx, claim, now, func(document map[string]json.RawMessage) error {
		state, decodeErr := decodeGenerationJournalState(document[generationJournalResultKey])
		if decodeErr != nil || index >= len(state.Blocks) ||
			state.Blocks[index].ContentDigest != digest {
			return ErrGenerationJournalConflict
		}
		block := &state.Blocks[index]
		if block.Status == GenerationBlockWriting && from == GenerationBlockPending {
			return ErrGenerationBlockAmbiguous
		}
		if block.Status == GenerationBlockCommitted {
			if to == GenerationBlockCommitted {
				return nil
			}
			return ErrGenerationJournalConflict
		}
		if block.Status != from {
			return ErrGenerationJournalConflict
		}
		now = now.UTC()
		switch to {
		case GenerationBlockWriting:
			block.StartedAt = &now
		case GenerationBlockCommitted:
			block.CommittedAt = &now
		default:
			return ErrInvalidConfiguration
		}
		block.Status = to
		state.UpdatedAt = now
		encoded, err := encodeGenerationJournalState(state)
		if err != nil {
			return err
		}
		document[generationJournalResultKey] = encoded
		return nil
	})
}

func (repository *PostgresRepository) mutateGenerationJournal(
	ctx context.Context,
	claim Claim,
	now time.Time,
	mutate func(map[string]json.RawMessage) error,
) error {
	if repository == nil || repository.Pool == nil || ctx == nil ||
		claim.Validate() != nil || now.IsZero() || mutate == nil {
		return ErrInvalidConfiguration
	}
	tx, err := repository.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return ErrGenerationJournalConflict
	}
	defer tx.Rollback(context.Background())
	var raw []byte
	if err := tx.QueryRow(ctx, lockGenerationJournalSQL, claim.ID, claim.Owner, now.UTC()).Scan(&raw); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrLeaseLost
		}
		return ErrGenerationJournalConflict
	}
	document := map[string]json.RawMessage{}
	if len(raw) != 0 && json.Unmarshal(raw, &document) != nil {
		return ErrGenerationJournalConflict
	}
	if err := mutate(document); err != nil {
		return err
	}
	encoded, err := json.Marshal(document)
	if err != nil {
		return ErrInvalidConfiguration
	}
	command, err := tx.Exec(ctx, updateGenerationJournalSQL, claim.ID, claim.Owner, now.UTC(), encoded)
	if err != nil || command.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	if err := tx.Commit(ctx); err != nil {
		return ErrGenerationJournalConflict
	}
	return nil
}

const lockGenerationJournalSQL = `
SELECT COALESCE(unit.result::text, '{}')
FROM public.sync_run_units AS unit
JOIN public.sync_runs AS run
  ON run.id = unit.sync_run_id AND run.org_id = unit.org_id
WHERE unit.id = $1::uuid
  AND unit.status = 'running'
  AND unit.lease_owner = $2
  AND unit.lease_expires_at IS NOT NULL
  AND unit.lease_expires_at > $3
  AND run.status NOT IN ('success', 'partial_failed', 'failed')
FOR UPDATE OF unit`

const loadGenerationJournalSQL = `
SELECT
  COALESCE(
    (COALESCE(unit.result::jsonb, '{}'::jsonb) -> 'go_generation_v2')::text,
    ''
  ),
  COALESCE(
    COALESCE(unit.result::jsonb, '{}'::jsonb) ? 'go_generation_v1',
    FALSE
  )
FROM public.sync_run_units AS unit
JOIN public.sync_runs AS run
  ON run.id = unit.sync_run_id AND run.org_id = unit.org_id
WHERE unit.id = $1::uuid
  AND unit.status = 'running'
  AND unit.lease_owner = $2
  AND unit.lease_expires_at IS NOT NULL
  AND unit.lease_expires_at > $3
  AND run.status NOT IN ('success', 'partial_failed', 'failed')`

const updateGenerationJournalSQL = `
UPDATE public.sync_run_units
SET result = $4::json,
    updated_at = $3
WHERE id = $1::uuid
  AND status = 'running'
  AND lease_owner = $2
  AND lease_expires_at IS NOT NULL
  AND lease_expires_at > $3`

var _ GenerationJournal = (*PostgresRepository)(nil)
