package providersync

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	effectLedgerResultKey     = "go_effect_ledger_v1"
	maxEffectDestinations     = 32
	maxEffectRows             = 100_000
	maxEffectPayloadBytes     = 64 << 20
	maxEffectLedgerStateBytes = 32 << 10
)

type EffectRecoveryPolicy string
type EffectInspection string

const (
	EffectReplaySafe       EffectRecoveryPolicy = "replay_safe"
	EffectReadbackRequired EffectRecoveryPolicy = "readback_required"
	EffectRecoveryBlocked  EffectRecoveryPolicy = "blocked"

	EffectExact    EffectInspection = "exact"
	EffectAbsent   EffectInspection = "absent"
	EffectConflict EffectInspection = "conflict"
)

type EffectBatch struct {
	Destination   string
	ContentDigest string
	Recovery      EffectRecoveryPolicy
	Rows          []json.RawMessage
	PayloadBytes  int
}

func BuildEffectBatch(
	destination string,
	recovery EffectRecoveryPolicy,
	rows []json.RawMessage,
) (EffectBatch, error) {
	destination = strings.TrimSpace(destination)
	if destination == "" || len(rows) > maxEffectRows || !validEffectRecovery(recovery) {
		return EffectBatch{}, ErrEffectRecoveryUnsafe
	}
	canonical := make([]json.RawMessage, 0, len(rows))
	total := 0
	for _, row := range rows {
		var compact bytes.Buffer
		if len(row) == 0 || json.Compact(&compact, row) != nil {
			return EffectBatch{}, ErrEffectRecoveryUnsafe
		}
		var object map[string]json.RawMessage
		if json.Unmarshal(compact.Bytes(), &object) != nil || object == nil {
			return EffectBatch{}, ErrEffectRecoveryUnsafe
		}
		encoded := append(json.RawMessage(nil), compact.Bytes()...)
		if total > maxEffectPayloadBytes-len(encoded) {
			return EffectBatch{}, ErrEffectRecoveryUnsafe
		}
		total += len(encoded)
		canonical = append(canonical, encoded)
	}
	sort.Slice(canonical, func(left, right int) bool {
		return bytes.Compare(canonical[left], canonical[right]) < 0
	})
	hash := sha256.New()
	hash.Write([]byte(destination))
	hash.Write([]byte{0})
	for _, row := range canonical {
		hash.Write(row)
		hash.Write([]byte{'\n'})
	}
	return EffectBatch{
		Destination: destination, ContentDigest: hex.EncodeToString(hash.Sum(nil)),
		Recovery: recovery, Rows: canonical, PayloadBytes: total,
	}, nil
}

type EffectLedgerEntry struct {
	Index         int                   `json:"index"`
	Destination   string                `json:"destination"`
	ContentDigest string                `json:"content_digest"`
	RowCount      int                   `json:"row_count"`
	Recovery      EffectRecoveryPolicy  `json:"recovery"`
	Status        GenerationBlockStatus `json:"status"`
	StartedAt     *time.Time            `json:"started_at,omitempty"`
	CommittedAt   *time.Time            `json:"committed_at,omitempty"`
}

type EffectLedgerState struct {
	SchemaVersion string              `json:"schema_version"`
	Generation    string              `json:"generation"`
	Provider      string              `json:"provider"`
	Dataset       string              `json:"dataset"`
	Effects       []EffectLedgerEntry `json:"effects"`
	CreatedAt     time.Time           `json:"created_at"`
	UpdatedAt     time.Time           `json:"updated_at"`
}

func NewEffectLedgerState(
	claim Claim,
	batches []EffectBatch,
	now time.Time,
) (EffectLedgerState, error) {
	if claim.Validate() != nil || len(batches) < 1 ||
		len(batches) > maxEffectDestinations || now.IsZero() {
		return EffectLedgerState{}, ErrEffectRecoveryUnsafe
	}
	batches = append([]EffectBatch(nil), batches...)
	sort.Slice(batches, func(left, right int) bool {
		return batches[left].Destination < batches[right].Destination
	})
	state := EffectLedgerState{
		SchemaVersion: "v1", Generation: claim.GenerationKey(),
		Provider: claim.Provider, Dataset: claim.Dataset,
		CreatedAt: now.UTC(), UpdatedAt: now.UTC(),
		Effects: make([]EffectLedgerEntry, 0, len(batches)),
	}
	for index, batch := range batches {
		if index > 0 && batches[index-1].Destination == batch.Destination {
			return EffectLedgerState{}, ErrEffectRecoveryUnsafe
		}
		state.Effects = append(state.Effects, EffectLedgerEntry{
			Index: index, Destination: batch.Destination,
			ContentDigest: batch.ContentDigest, RowCount: len(batch.Rows),
			Recovery: batch.Recovery, Status: GenerationBlockPending,
		})
	}
	if encodeEffectLedgerState(state) == nil {
		return EffectLedgerState{}, ErrEffectRecoveryUnsafe
	}
	return state, nil
}

func (state EffectLedgerState) validate() error {
	if state.SchemaVersion != "v1" || state.Generation == "" ||
		strings.TrimSpace(state.Provider) == "" || strings.TrimSpace(state.Dataset) == "" ||
		len(state.Effects) < 1 || len(state.Effects) > maxEffectDestinations ||
		state.CreatedAt.IsZero() || state.UpdatedAt.IsZero() {
		return ErrEffectLedgerConflict
	}
	seen := map[string]bool{}
	for index, effect := range state.Effects {
		if effect.Index != index || strings.TrimSpace(effect.Destination) == "" ||
			seen[effect.Destination] || !validDigest(effect.ContentDigest) ||
			effect.RowCount < 0 || effect.RowCount > maxEffectRows ||
			!validEffectRecovery(effect.Recovery) {
			return ErrEffectLedgerConflict
		}
		seen[effect.Destination] = true
		switch effect.Status {
		case GenerationBlockPending:
			if effect.StartedAt != nil || effect.CommittedAt != nil {
				return ErrEffectLedgerConflict
			}
		case GenerationBlockWriting:
			if effect.StartedAt == nil || effect.CommittedAt != nil {
				return ErrEffectLedgerConflict
			}
		case GenerationBlockCommitted:
			if effect.StartedAt == nil || effect.CommittedAt == nil {
				return ErrEffectLedgerConflict
			}
		default:
			return ErrEffectLedgerConflict
		}
	}
	return nil
}

func validEffectRecovery(policy EffectRecoveryPolicy) bool {
	switch policy {
	case EffectReplaySafe, EffectReadbackRequired, EffectRecoveryBlocked:
		return true
	default:
		return false
	}
}

func validDigest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}

func encodeEffectLedgerState(state EffectLedgerState) []byte {
	if state.validate() != nil {
		return nil
	}
	encoded, err := json.Marshal(state)
	if err != nil || len(encoded) > maxEffectLedgerStateBytes {
		return nil
	}
	return encoded
}

func decodeEffectLedgerState(raw []byte) (EffectLedgerState, error) {
	if len(raw) == 0 || len(raw) > maxEffectLedgerStateBytes {
		return EffectLedgerState{}, ErrEffectRecoveryUnsafe
	}
	var state EffectLedgerState
	if json.Unmarshal(raw, &state) != nil || state.validate() != nil {
		return EffectLedgerState{}, ErrEffectLedgerConflict
	}
	return state, nil
}

func sameEffectManifest(left, right EffectLedgerState) bool {
	if left.SchemaVersion != right.SchemaVersion ||
		left.Generation != right.Generation || left.Provider != right.Provider ||
		left.Dataset != right.Dataset || len(left.Effects) != len(right.Effects) {
		return false
	}
	for index := range left.Effects {
		a, b := left.Effects[index], right.Effects[index]
		if a.Index != b.Index || a.Destination != b.Destination ||
			a.ContentDigest != b.ContentDigest || a.RowCount != b.RowCount ||
			a.Recovery != b.Recovery {
			return false
		}
	}
	return true
}

type EffectLedger interface {
	LoadEffects(context.Context, Claim, time.Time) (EffectLedgerState, error)
	PrepareEffects(context.Context, Claim, EffectLedgerState, time.Time) (EffectLedgerState, error)
	BeginEffect(context.Context, Claim, int, string, time.Time) error
	CommitEffect(context.Context, Claim, int, string, time.Time) error
	ResolveEffect(
		context.Context,
		Claim,
		int,
		string,
		GenerationBlockResolution,
		time.Time,
	) error
}

func (repository *PostgresRepository) LoadEffects(
	ctx context.Context,
	claim Claim,
	now time.Time,
) (EffectLedgerState, error) {
	if repository == nil || repository.Pool == nil || ctx == nil ||
		claim.Validate() != nil || now.IsZero() {
		return EffectLedgerState{}, ErrInvalidConfiguration
	}
	var raw []byte
	if err := repository.Pool.QueryRow(
		ctx, loadEffectLedgerSQL, claim.ID, claim.Owner, now.UTC(),
	).Scan(&raw); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return EffectLedgerState{}, ErrLeaseLost
		}
		return EffectLedgerState{}, ErrEffectLedgerConflict
	}
	return decodeEffectLedgerState(raw)
}

func (repository *PostgresRepository) PrepareEffects(
	ctx context.Context,
	claim Claim,
	desired EffectLedgerState,
	now time.Time,
) (EffectLedgerState, error) {
	if desired.validate() != nil || now.IsZero() {
		return EffectLedgerState{}, ErrInvalidConfiguration
	}
	var prepared EffectLedgerState
	err := repository.mutateGenerationJournal(ctx, claim, now, func(document map[string]json.RawMessage) error {
		raw := document[effectLedgerResultKey]
		if len(raw) != 0 {
			var err error
			prepared, err = decodeEffectLedgerState(raw)
			if err != nil || !sameEffectManifest(prepared, desired) {
				return ErrEffectLedgerConflict
			}
			return nil
		}
		prepared = desired
		prepared.CreatedAt = now.UTC()
		prepared.UpdatedAt = now.UTC()
		encoded := encodeEffectLedgerState(prepared)
		if len(encoded) == 0 {
			return ErrEffectRecoveryUnsafe
		}
		document[effectLedgerResultKey] = encoded
		return nil
	})
	return prepared, err
}

func (repository *PostgresRepository) BeginEffect(
	ctx context.Context,
	claim Claim,
	index int,
	digest string,
	now time.Time,
) error {
	return repository.transitionEffect(
		ctx, claim, index, digest, now,
		GenerationBlockPending, GenerationBlockWriting,
	)
}

func (repository *PostgresRepository) CommitEffect(
	ctx context.Context,
	claim Claim,
	index int,
	digest string,
	now time.Time,
) error {
	return repository.transitionEffect(
		ctx, claim, index, digest, now,
		GenerationBlockWriting, GenerationBlockCommitted,
	)
}

func (repository *PostgresRepository) ResolveEffect(
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
		state, err := decodeEffectLedgerState(document[effectLedgerResultKey])
		if err != nil || index >= len(state.Effects) ||
			state.Effects[index].ContentDigest != digest {
			return ErrEffectLedgerConflict
		}
		effect := &state.Effects[index]
		now = now.UTC()
		switch resolution {
		case GenerationBlockMarkCommitted:
			if effect.Status == GenerationBlockCommitted {
				return nil
			}
			if effect.Status != GenerationBlockWriting {
				return ErrEffectLedgerConflict
			}
			effect.Status = GenerationBlockCommitted
			effect.CommittedAt = &now
		case GenerationBlockRetryPending:
			if effect.Status == GenerationBlockPending {
				return nil
			}
			if effect.Status != GenerationBlockWriting {
				return ErrEffectLedgerConflict
			}
			effect.Status = GenerationBlockPending
			effect.StartedAt = nil
			effect.CommittedAt = nil
		default:
			return ErrInvalidConfiguration
		}
		state.UpdatedAt = now
		encoded := encodeEffectLedgerState(state)
		if len(encoded) == 0 {
			return ErrEffectRecoveryUnsafe
		}
		document[effectLedgerResultKey] = encoded
		return nil
	})
}

func (repository *PostgresRepository) transitionEffect(
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
		state, err := decodeEffectLedgerState(document[effectLedgerResultKey])
		if err != nil || index >= len(state.Effects) ||
			state.Effects[index].ContentDigest != digest {
			return ErrEffectLedgerConflict
		}
		effect := &state.Effects[index]
		if effect.Status == GenerationBlockWriting && from == GenerationBlockPending {
			return ErrEffectRecoveryAmbiguous
		}
		if effect.Status == GenerationBlockCommitted {
			if to == GenerationBlockCommitted {
				return nil
			}
			return ErrEffectLedgerConflict
		}
		if effect.Status != from {
			return ErrEffectLedgerConflict
		}
		now = now.UTC()
		switch to {
		case GenerationBlockWriting:
			effect.StartedAt = &now
		case GenerationBlockCommitted:
			effect.CommittedAt = &now
		default:
			return ErrInvalidConfiguration
		}
		effect.Status = to
		state.UpdatedAt = now
		encoded := encodeEffectLedgerState(state)
		if len(encoded) == 0 {
			return ErrEffectRecoveryUnsafe
		}
		document[effectLedgerResultKey] = encoded
		return nil
	})
}

const loadEffectLedgerSQL = `
SELECT COALESCE(
  (COALESCE(unit.result::jsonb, '{}'::jsonb) -> 'go_effect_ledger_v1')::text,
  ''
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

var _ EffectLedger = (*PostgresRepository)(nil)
