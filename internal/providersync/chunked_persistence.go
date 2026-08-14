package providersync

// This file contains the additive chunk protocol for provider-unit routes.
// The old CompleteRouteHandler path remains the default. A descriptor opts in
// to chunking only when its route has a policy and the durable store is wired.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	chunkCheckpointSchemaVersion = "v1"
	chunkPayloadSchemaVersion    = "v1"
	chunkRouteVersion            = "provider-chunk.v1"
	maxChunkCursorBytes          = 4 << 10
	maxChunkResultBytes          = 64 << 10
	maxChunkAggregateDigestBytes = 64
)

var (
	ErrChunkCheckpointNotFound = errors.New("provider sync chunk checkpoint is not present")
	ErrPreparedChunkNotFound   = errors.New("provider sync prepared chunk is not present")
	ErrChunkCheckpointConflict = errors.New("provider sync chunk checkpoint conflicts with persisted state")
	ErrChunkPolicyExceeded     = errors.New("provider sync chunk policy cannot bound this route batch")
	ErrChunkContinuation       = errors.New("provider sync chunk continuation is durable and attempt-neutral")
)

// ChunkPolicy is checked-in route policy. It is deliberately not loaded from
// a process-global environment switch: changing a limit changes persistence
// and recovery semantics and must go through a reviewed route change.
type ChunkPolicy struct {
	MaxSourceItems      int
	MaxEffectRows       int
	MaxPreparedBytes    int
	MaxChunksPerAttempt int
	MaxWallTime         time.Duration
}

func DefaultChunkPolicy() ChunkPolicy {
	return ChunkPolicy{
		MaxSourceItems: 100, MaxEffectRows: 500,
		MaxPreparedBytes: 2 << 20, MaxChunksPerAttempt: 8,
		MaxWallTime: 45 * time.Second,
	}
}

func (policy ChunkPolicy) Validate() error {
	if policy.MaxSourceItems < 1 || policy.MaxSourceItems > 100_000 ||
		policy.MaxEffectRows < 1 || policy.MaxEffectRows > maxEffectRows ||
		policy.MaxPreparedBytes < 1 || policy.MaxPreparedBytes > maxPreparedRouteSnapshotBytes ||
		policy.MaxChunksPerAttempt < 1 || policy.MaxChunksPerAttempt > 1024 ||
		policy.MaxWallTime <= 0 || policy.MaxWallTime > 15*time.Minute {
		return ErrInvalidConfiguration
	}
	return nil
}

// ChunkCheckpoint is the small durable cursor. Provider data is never placed
// in it; prepared normalized chunks live in the sidecar owned by the store.
type ChunkCheckpoint struct {
	SchemaVersion     string         `json:"schema_version"`
	OrgID             string         `json:"org_id"`
	UnitID            string         `json:"unit_id"`
	Generation        string         `json:"generation"`
	Provider          string         `json:"provider"`
	Dataset           string         `json:"dataset"`
	RouteVersion      string         `json:"route_version"`
	NormalizedAt      time.Time      `json:"normalized_at"`
	NextCursor        string         `json:"next_cursor,omitempty"`
	InventoryComplete bool           `json:"inventory_complete"`
	NextOrdinal       int            `json:"next_ordinal"`
	PreparedChunks    int            `json:"prepared_chunks"`
	TotalChunks       int            `json:"total_chunks"`
	FinalOrdinal      int            `json:"final_ordinal"`
	AggregateResult   map[string]any `json:"aggregate_result,omitempty"`
	AggregateDigest   string         `json:"aggregate_digest,omitempty"`
	Owner             string         `json:"owner"`
	LeaseExpiresAt    time.Time      `json:"lease_expires_at"`
	CreatedAt         time.Time      `json:"created_at"`
	UpdatedAt         time.Time      `json:"updated_at"`
}

func (checkpoint ChunkCheckpoint) Validate(claim Claim) error {
	if claim.Validate() != nil || checkpoint.SchemaVersion != chunkCheckpointSchemaVersion ||
		checkpoint.OrgID != claim.OrgID || checkpoint.UnitID != claim.ID ||
		checkpoint.Generation != claim.GenerationKey() ||
		checkpoint.Provider != claim.Provider || checkpoint.Dataset != claim.Dataset ||
		strings.TrimSpace(checkpoint.RouteVersion) == "" || checkpoint.NormalizedAt.IsZero() ||
		checkpoint.NextOrdinal < 0 || checkpoint.PreparedChunks < 0 ||
		checkpoint.TotalChunks < 0 || checkpoint.NextOrdinal > checkpoint.PreparedChunks ||
		(checkpoint.TotalChunks > 0 && checkpoint.PreparedChunks > checkpoint.TotalChunks) ||
		strings.TrimSpace(checkpoint.Owner) == "" || checkpoint.FinalOrdinal < -1 ||
		(checkpoint.TotalChunks > 0 && checkpoint.FinalOrdinal >= checkpoint.TotalChunks) ||
		checkpoint.LeaseExpiresAt.IsZero() ||
		checkpoint.CreatedAt.IsZero() || checkpoint.UpdatedAt.IsZero() {
		return ErrChunkCheckpointConflict
	}
	if len(checkpoint.NextCursor) > maxChunkCursorBytes ||
		len(checkpoint.AggregateDigest) > maxChunkAggregateDigestBytes {
		return ErrChunkCheckpointConflict
	}
	if checkpoint.AggregateResult != nil {
		encoded, err := json.Marshal(checkpoint.AggregateResult)
		if err != nil || len(encoded) > maxChunkResultBytes {
			return ErrChunkCheckpointConflict
		}
	}
	if checkpoint.InventoryComplete &&
		(checkpoint.TotalChunks == 0 || checkpoint.NextOrdinal != checkpoint.TotalChunks ||
			checkpoint.PreparedChunks != checkpoint.TotalChunks) {
		return ErrChunkCheckpointConflict
	}
	return nil
}

// PreparedProviderChunk contains only normalized sink-ready data. It is
// bounded and tenant/generation fenced by ChunkedEffectStore before a sink
// write. The final chunk carries the complete route metadata needed by the
// ordinary unit completion boundary.
type PreparedProviderChunk struct {
	SchemaVersion       string                        `json:"schema_version"`
	RouteVersion        string                        `json:"route_version"`
	Ordinal             int                           `json:"ordinal"`
	TotalChunks         int                           `json:"total_chunks"`
	CursorBefore        string                        `json:"cursor_before,omitempty"`
	CursorAfter         string                        `json:"cursor_after,omitempty"`
	InventoryComplete   bool                          `json:"inventory_complete"`
	Effects             []EffectBatch                 `json:"effects"`
	Result              map[string]any                `json:"result,omitempty"`
	Watermark           *time.Time                    `json:"watermark,omitempty"`
	Evidence            FetchEvidence                 `json:"evidence"`
	Comparison          ShadowComparison              `json:"comparison"`
	WorklogObservations []JiraWorklogFetchObservation `json:"worklog_observations,omitempty"`
	Ledger              EffectLedgerState             `json:"ledger"`
	PayloadBytes        int                           `json:"payload_bytes"`
	ManifestDigest      string                        `json:"manifest_digest"`
}

func (chunk PreparedProviderChunk) Validate(claim Claim, policy ChunkPolicy) error {
	if claim.Validate() != nil || policy.Validate() != nil ||
		chunk.SchemaVersion != chunkPayloadSchemaVersion ||
		chunk.RouteVersion == "" || chunk.Ordinal < 0 || chunk.TotalChunks < 0 ||
		(chunk.TotalChunks > 0 && chunk.Ordinal >= chunk.TotalChunks) || len(chunk.CursorBefore) > maxChunkCursorBytes ||
		len(chunk.CursorAfter) > maxChunkCursorBytes || len(chunk.Effects) == 0 ||
		chunk.PayloadBytes < 1 || chunk.PayloadBytes > policy.MaxPreparedBytes ||
		!validDigest(chunk.ManifestDigest) || chunk.Ledger.validate() != nil ||
		chunk.Ledger.Generation != claim.GenerationKey() ||
		chunk.Ledger.Provider != claim.Provider || chunk.Ledger.Dataset != claim.Dataset {
		return ErrChunkCheckpointConflict
	}
	encoded, err := json.Marshal(chunk)
	if err != nil || len(encoded) > policy.MaxPreparedBytes {
		return ErrChunkCheckpointConflict
	}
	return nil
}

// encodedPreparedChunkPayload measures the provider sidecar without the
// effect ledger. The ledger is stored in its own JSONB column and is measured
// separately by the durable store. The small fixed-point loop accounts for
// payload_bytes being part of the encoded payload itself.
func encodedPreparedChunkPayload(chunk PreparedProviderChunk) ([]byte, error) {
	chunk.Ledger = EffectLedgerState{}
	chunk.PayloadBytes = 0
	var encoded []byte
	for attempt := 0; attempt < 3; attempt++ {
		var err error
		encoded, err = json.Marshal(chunk)
		if err != nil {
			return nil, err
		}
		if chunk.PayloadBytes == len(encoded) {
			return encoded, nil
		}
		chunk.PayloadBytes = len(encoded)
	}
	return encoded, nil
}

// ChunkedEffectStore persists the checkpoint and prepared sidecars, and owns
// all effect-ledger transitions for one ordinal. Implementations must fence
// every method by org, unit, generation, owner, live lease, and non-terminal
// run state. The Postgres implementation below uses one transaction per
// transition; no provider payload enters queue arguments or logs.
type ChunkedEffectStore interface {
	LoadChunkCheckpoint(context.Context, Claim, time.Time) (ChunkCheckpoint, error)
	PrepareChunk(context.Context, Claim, PreparedProviderChunk, time.Time) (PreparedProviderChunk, error)
	LoadPreparedChunk(context.Context, Claim, int, time.Time) (PreparedProviderChunk, error)
	BeginChunkEffect(context.Context, Claim, int, int, string, time.Time) error
	CommitChunkEffect(context.Context, Claim, int, int, string, time.Time) error
	ResolveChunkEffect(context.Context, Claim, int, int, string, GenerationBlockResolution, time.Time) error
	MarkChunkCommitted(context.Context, Claim, int, string, time.Time) error
	MarkInventoryComplete(context.Context, Claim, time.Time) error
}

type chunkEffectLedgerAdapter struct {
	store   ChunkedEffectStore
	claim   Claim
	ordinal int
	now     func() time.Time
}

func (adapter chunkEffectLedgerAdapter) LoadEffects(
	ctx context.Context, claim Claim, now time.Time,
) (EffectLedgerState, error) {
	if claim.ID != adapter.claim.ID || claim.GenerationKey() != adapter.claim.GenerationKey() {
		return EffectLedgerState{}, ErrChunkCheckpointConflict
	}
	chunk, err := adapter.store.LoadPreparedChunk(ctx, claim, adapter.ordinal, now)
	if err != nil {
		return EffectLedgerState{}, err
	}
	return chunk.Ledger, nil
}

func (adapter chunkEffectLedgerAdapter) PrepareEffects(
	context.Context, Claim, EffectLedgerState, time.Time,
) (EffectLedgerState, error) {
	return EffectLedgerState{}, ErrInvalidConfiguration
}

func (adapter chunkEffectLedgerAdapter) BeginEffect(
	ctx context.Context, claim Claim, index int, digest string, now time.Time,
) error {
	return adapter.store.BeginChunkEffect(ctx, claim, adapter.ordinal, index, digest, now)
}

func (adapter chunkEffectLedgerAdapter) CommitEffect(
	ctx context.Context, claim Claim, index int, digest string, now time.Time,
) error {
	return adapter.store.CommitChunkEffect(ctx, claim, adapter.ordinal, index, digest, now)
}

func (adapter chunkEffectLedgerAdapter) ResolveEffect(
	ctx context.Context, claim Claim, index int, digest string,
	resolution GenerationBlockResolution, now time.Time,
) error {
	return adapter.store.ResolveChunkEffect(ctx, claim, adapter.ordinal, index, digest, resolution, now)
}

func commitPreparedChunk(
	ctx context.Context,
	claim Claim,
	chunk PreparedProviderChunk,
	sink EffectSink,
	readback EffectReadback,
	now func() time.Time,
	store ChunkedEffectStore,
) (EffectCommitResult, error) {
	if store == nil || sink == nil || chunk.Ledger.validate() != nil {
		return EffectCommitResult{}, ErrInvalidConfiguration
	}
	return (EffectCommitter{
		Ledger: chunkEffectLedgerAdapter{
			store: store, claim: claim, ordinal: chunk.Ordinal, now: now,
		},
		Sink: sink, Readback: readback, Now: now,
	}).CommitPrepared(ctx, claim, chunk.Effects, chunk.Ledger)
}

// ChunkContinuationError tells the provider-unit adapter that a durable
// continuation is ready. It is not a source or sink failure and must be
// translated to River's attempt-neutral snooze path.
type ChunkContinuationError struct{ Next time.Time }

func (err ChunkContinuationError) Error() string { return ErrChunkContinuation.Error() }
func (err ChunkContinuationError) Unwrap() error { return ErrChunkContinuation }

func ChunkContinuationDelay(err error) (time.Duration, bool) {
	var continuation ChunkContinuationError
	if !errors.As(err, &continuation) || continuation.Next.IsZero() {
		return 0, false
	}
	delay := time.Until(continuation.Next)
	if delay < time.Second {
		delay = time.Second
	}
	return delay, true
}

// SplitCompleteRouteBatch is the compatibility adapter used while a route's
// provider iterator is being migrated. It bounds every prepared sidecar and
// preserves destination ordering. A route-specific iterator can replace this
// adapter without changing the persistence protocol.
func SplitCompleteRouteBatch(batch CompleteRouteBatch, policy ChunkPolicy) ([]PreparedProviderChunk, error) {
	return splitCompleteRouteBatch(batch, ShadowComparison{}, policy)
}

// SplitCompleteRouteBatchWithComparison is the production adapter. The
// comparator result is attached only to the final chunk so an exact resume
// can complete the unit without invoking the provider or Python again.
func SplitCompleteRouteBatchWithComparison(
	batch CompleteRouteBatch,
	comparison ShadowComparison,
	policy ChunkPolicy,
) ([]PreparedProviderChunk, error) {
	return splitCompleteRouteBatch(batch, comparison, policy)
}

func splitCompleteRouteBatch(
	batch CompleteRouteBatch,
	comparison ShadowComparison,
	policy ChunkPolicy,
) ([]PreparedProviderChunk, error) {
	if policy.Validate() != nil || len(batch.Effects) == 0 {
		return nil, ErrInvalidConfiguration
	}
	ordered := append([]EffectBatch(nil), batch.Effects...)
	sortEffectBatches(ordered)
	// A prepared chunk is bounded by both the provider-page budget and the
	// sink effect-row budget. The smaller bound is intentional: a provider can
	// emit several normalized effects for one source item, so using only the
	// effect cap would allow one source page to grow without limit.
	maxRows := min(policy.MaxEffectRows, policy.MaxSourceItems)
	for maxRows >= 1 {
		count := 1
		for _, effect := range ordered {
			if rows := (len(effect.Rows) + maxRows - 1) / maxRows; rows > count {
				count = rows
			}
		}
		chunks := make([]PreparedProviderChunk, 0, count)
		valid := true
		for ordinal := 0; ordinal < count; ordinal++ {
			effects := make([]EffectBatch, 0, len(ordered))
			for _, effect := range ordered {
				start := ordinal * maxRows
				if start > len(effect.Rows) {
					start = len(effect.Rows)
				}
				end := start + maxRows
				if end > len(effect.Rows) {
					end = len(effect.Rows)
				}
				part, err := BuildEffectBatch(effect.Destination, effect.Recovery, effect.Rows[start:end])
				if err != nil {
					return nil, err
				}
				effects = append(effects, part)
			}
			payloadBytes := 0
			for _, effect := range effects {
				payloadBytes += effect.PayloadBytes
			}
			if payloadBytes > policy.MaxPreparedBytes {
				valid = false
				break
			}
			chunk := PreparedProviderChunk{
				SchemaVersion: chunkPayloadSchemaVersion, RouteVersion: chunkRouteVersion,
				Ordinal: ordinal, TotalChunks: count, Effects: effects,
				InventoryComplete: ordinal == count-1, PayloadBytes: payloadBytes,
			}
			if chunk.InventoryComplete {
				chunk.Result = cloneChunkResult(batch.Result)
				chunk.Watermark = batch.Watermark
				chunk.Evidence = batch.Evidence
				chunk.Comparison = comparison
				chunk.WorklogObservations = append([]JiraWorklogFetchObservation(nil), batch.WorklogObservations...)
			}
			encoded, encodeErr := encodedPreparedChunkPayload(chunk)
			if encodeErr != nil {
				return nil, encodeErr
			}
			chunk.PayloadBytes = len(encoded)
			if chunk.PayloadBytes > policy.MaxPreparedBytes {
				valid = false
				break
			}
			chunk.ManifestDigest = preparedChunkDigest(chunk)
			chunks = append(chunks, chunk)
		}
		if valid {
			return chunks, nil
		}
		if maxRows == 1 {
			break
		}
		maxRows /= 2
	}
	return nil, ErrChunkPolicyExceeded
}

func preparedChunkDigest(chunk PreparedProviderChunk) string {
	hash := sha256.New()
	for _, effect := range chunk.Effects {
		hash.Write([]byte(effect.Destination))
		hash.Write([]byte{0})
		hash.Write([]byte(effect.ContentDigest))
		hash.Write([]byte{0})
	}
	// TotalChunks is assigned only after a streaming route observes the end of
	// inventory. It must not change the identity of an already prepared sidecar
	// when that final count is published.
	hash.Write([]byte(fmt.Sprintf("%d/%t", chunk.Ordinal, chunk.InventoryComplete)))
	return hex.EncodeToString(hash.Sum(nil))
}

func cloneChunkResult(input map[string]any) map[string]any {
	if input == nil {
		return nil
	}
	result := make(map[string]any, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}

func chunkResultDigest(result map[string]any) string {
	encoded, err := json.Marshal(result)
	if err != nil {
		return ""
	}
	encoded = bytes.TrimSpace(encoded)
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}
