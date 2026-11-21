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

	"github.com/jackc/pgx/v5/pgconn"
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

// ChunkSidecarMismatchError is the bare checkpoint conflict of a stored sidecar whose payload_bytes column and the
// payload_bytes inside its JSON disagree (CHAOS-11772). It carries the two numbers and nothing from the payload. It is
// not a ChunkStoreError, so it stays a deterministic, permanent conflict (providerunit: checkpoint_conflict) and
// matches ErrChunkCheckpointConflict with errors.Is.
type ChunkSidecarMismatchError struct {
	Field   string
	Stored  int
	Payload int
}

func (err *ChunkSidecarMismatchError) Error() string {
	return fmt.Sprintf("%s: %s mismatch (column=%d payload=%d)", ErrChunkCheckpointConflict.Error(), err.Field, err.Stored, err.Payload)
}

func (err *ChunkSidecarMismatchError) Is(target error) bool {
	return target == ErrChunkCheckpointConflict
}

// chunkConflictBecause is the bare checkpoint conflict with a fixed-vocabulary reason (never row data). It is not a
// ChunkStoreError: providerunit still classes it deterministic-terminal.
func chunkConflictBecause(reason string) error {
	return fmt.Errorf("%w (%s)", ErrChunkCheckpointConflict, reason)
}

// storedChunkRow is the relational half of a prepared chunk row.
type storedChunkRow struct {
	schemaVersion, routeVersion string
	ordinal, totalChunks        int
	cursorBefore, cursorAfter   string
	inventoryComplete           bool
	payloadBytes                int
	manifestDigest, status      string
}

// verifyStoredChunk checks a decoded sidecar against its row and returns the chunk with the row's canonical count.
// PostgresRepository and the in-process store both call it, so a refusal has one reason in both.
func verifyStoredChunk(chunk PreparedProviderChunk, row storedChunkRow) (PreparedProviderChunk, error) {
	switch {
	case row.status != "pending" && row.status != "writing" && row.status != "committed":
		return PreparedProviderChunk{}, chunkConflictBecause("status")
	case chunk.SchemaVersion != row.schemaVersion:
		return PreparedProviderChunk{}, chunkConflictBecause("schema_version")
	case chunk.RouteVersion != row.routeVersion:
		return PreparedProviderChunk{}, chunkConflictBecause("route_version")
	case chunk.Ordinal != row.ordinal:
		return PreparedProviderChunk{}, chunkConflictBecause("ordinal")
	case chunk.CursorBefore != row.cursorBefore:
		return PreparedProviderChunk{}, chunkConflictBecause("cursor_before")
	case chunk.CursorAfter != row.cursorAfter:
		return PreparedProviderChunk{}, chunkConflictBecause("cursor_after")
	case chunk.InventoryComplete != row.inventoryComplete:
		return PreparedProviderChunk{}, chunkConflictBecause("inventory_complete")
	case chunk.PayloadBytes != row.payloadBytes:
		return PreparedProviderChunk{}, &ChunkSidecarMismatchError{Field: "payload_bytes", Stored: row.payloadBytes, Payload: chunk.PayloadBytes}
	case chunk.ManifestDigest != row.manifestDigest:
		return PreparedProviderChunk{}, chunkConflictBecause("manifest_digest")
	}
	// The sidecar payload is prepared before the final inventory count is known. The relational column is finalized
	// transactionally and is the canonical count used by recovery; do not let the provisional JSON value overwrite it.
	chunk.TotalChunks = row.totalChunks
	if chunk.TotalChunks < 0 || (chunk.TotalChunks > 0 && chunk.Ordinal >= chunk.TotalChunks) {
		return PreparedProviderChunk{}, chunkConflictBecause("total_chunks")
	}
	if chunk.ManifestDigest == "" {
		return PreparedProviderChunk{}, chunkConflictBecause("manifest_digest")
	}
	if chunk.Ledger.validate() != nil {
		return PreparedProviderChunk{}, chunkConflictBecause("ledger_invalid")
	}
	return chunk, nil
}

// ChunkStoreError is a transient error (a database error, a cancelled context,
// a provider page that changed between attempts) flattened into the conflict
// sentinel. It matches ErrChunkCheckpointConflict and its cause with
// errors.Is, and errors.As tells it apart from a real state mismatch, which
// returns the bare sentinel.
type ChunkStoreError struct{ Cause error }

// NewChunkStoreError wraps a database error raised by the chunk store.
func NewChunkStoreError(cause error) error { return &ChunkStoreError{Cause: cause} }

func (err *ChunkStoreError) Error() string {
	if err.Cause == nil {
		return ErrChunkCheckpointConflict.Error()
	}
	return ErrChunkCheckpointConflict.Error() + ": " + err.Cause.Error()
}

func (err *ChunkStoreError) Unwrap() []error {
	if err.Cause == nil {
		return []error{ErrChunkCheckpointConflict}
	}
	return []error{ErrChunkCheckpointConflict, err.Cause}
}

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

// defaultMaxChunksPerAttempt is the chunk-count bound of one attempt. The wall
// bound (MaxWallTime, below) is the one meant to end a normal attempt: the
// continuation between attempts costs a 1 s River snooze plus the fetch and
// completion hand-off, and the unit's cap slot stays held through it
// (dispatch_guard.go countActiveBucketUnits counts a fresh `dispatching` unit).
// CHAOS-7692 measured: a chunked unit (the chunked routes are the cicd and tests
// datasets, see execution_registry.go) is ~380-880 chunks at ~0.66 s each, 8
// chunks ended an attempt after ~5 s, and ~80 % of the heavy-unit slot time on
// prod was spent between attempts. At ~0.66 s a chunk
// 64 chunks is ~42 s, so the count bound now sits just under the 45 s wall and
// still ends an attempt of very fast chunks; memory stays bounded per chunk
// (MaxSourceItems, MaxEffectRows, MaxPreparedBytes), not per attempt.
const defaultMaxChunksPerAttempt = 64

// testOpsMaxSourceItems is the rows-per-destination bound of the TestOps routes: with MaxEffectRows (500) the split's
// bound is min(500, 500) = 500, and both must stay at or above it. Measured 5x the rows per sub-chunk for about +8 % sub-chunk time.
const testOpsMaxSourceItems = 500

// TestOpsRouteChunkPolicy is the chunk policy of the github and gitlab cicd and tests routes: the default with 500 rows per destination.
func TestOpsRouteChunkPolicy() ChunkPolicy {
	policy := DefaultChunkPolicy()
	policy.MaxSourceItems = testOpsMaxSourceItems
	return policy
}

func DefaultChunkPolicy() ChunkPolicy {
	return ChunkPolicy{
		MaxSourceItems: 100, MaxEffectRows: 500,
		MaxPreparedBytes: 2 << 20, MaxChunksPerAttempt: defaultMaxChunksPerAttempt,
		MaxWallTime: 45 * time.Second,
	}
}

// maxChunkPayloadBytes mirrors ck_sync_chunk_payload_bytes in migration 0102.
// A policy that allows a larger sidecar than the column check accepts would
// only fail at INSERT, after the provider page has already been fetched.
const maxChunkPayloadBytes = 2 << 20

func (policy ChunkPolicy) Validate() error {
	if policy.MaxSourceItems < 1 || policy.MaxSourceItems > 100_000 ||
		policy.MaxEffectRows < 1 || policy.MaxEffectRows > maxEffectRows ||
		policy.MaxPreparedBytes < 1 || policy.MaxPreparedBytes > maxChunkPayloadBytes ||
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
	// CommittedRows counts sink rows across every committed chunk. The route's
	// completion metadata rides on a final chunk with no rows, so this is the
	// only truthful record count available at finalization (CHAOS-3823).
	CommittedRows  int64     `json:"committed_rows"`
	Owner          string    `json:"owner"`
	LeaseExpiresAt time.Time `json:"lease_expires_at"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
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
// separately by the durable store. payload_bytes is part of the encoded payload
// itself, so the stamp is a fixed point (see stampPayloadBytes).
func encodedPreparedChunkPayload(chunk PreparedProviderChunk) ([]byte, error) {
	chunk.Ledger = EffectLedgerState{}
	var encoded []byte
	var marshalErr error
	_, converged := stampPayloadBytes(func(payloadBytes int) int {
		if marshalErr != nil {
			return payloadBytes
		}
		chunk.PayloadBytes = payloadBytes
		encoded, marshalErr = json.Marshal(chunk)
		return len(encoded)
	})
	if marshalErr != nil {
		return nil, marshalErr
	}
	if !converged {
		return nil, errChunkPayloadStamp
	}
	return encoded, nil
}

// errChunkPayloadStamp is the refusal for a payload_bytes stamp that did not reach its fixed point. It cannot
// happen for a sidecar within the 2 MiB cap; it is a policy refusal, never a checkpoint conflict.
var errChunkPayloadStamp = fmt.Errorf("%w: payload_bytes stamp did not converge", ErrChunkPolicyExceeded)

// maxPayloadStampPasses bounds stampPayloadBytes. The stamp starts at 0 and each pass sets it to base+digits(stamp):
// it never decreases and grows by less than 8 per pass, so it crosses at most one power of ten and settles in four
// passes for any sidecar within the 2 MiB cap.
var maxPayloadStampPasses = 8

// stampPayloadBytes finds the payload_bytes value that the encoding of the sidecar that carries it measures
// exactly: measure(stamp) == stamp. The number is part of the bytes it counts, so a fixed number of passes is not
// enough: for lengths a few bytes above a power of ten (101, 1001-1002, 10001-10003, 100001-100004,
// 1000001-1000005) the last pass changed the digit count and the loop stopped one short (CHAOS-11772). The false
// return is a loop that did not settle; it cannot happen within the cap.
func stampPayloadBytes(measure func(payloadBytes int) int) (int, bool) {
	stamp := 0
	for pass := 0; pass < maxPayloadStampPasses; pass++ {
		length := measure(stamp)
		if length == stamp {
			return stamp, true
		}
		stamp = length
	}
	return stamp, false
}

// ChunkedEffectStore persists the checkpoint and prepared sidecars, and owns
// all effect-ledger transitions for one ordinal. Implementations must fence
// every method by org, unit, generation, owner, live lease, and non-terminal
// run state. The Postgres implementation below uses one transaction per
// transition; no provider payload enters queue arguments or logs.
type ChunkedEffectStore interface {
	LoadChunkCheckpoint(context.Context, Claim, time.Time) (ChunkCheckpoint, error)
	PrepareChunk(context.Context, Claim, PreparedProviderChunk, time.Time) (PreparedProviderChunk, error)
	// PrepareChunkGroup must persist every sub-chunk of one provider emission
	// atomically. A partially prepared emission is unrecoverable without
	// refetching the provider item, which duplicates rows (CHAOS-3821).
	PrepareChunkGroup(context.Context, Claim, []PreparedProviderChunk, time.Time) ([]PreparedProviderChunk, error)
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
//
// Reason, Chunks and Elapsed say why THIS attempt stopped (CHAOS-7692): without
// them an operator cannot tell a unit that stopped on the chunk-count bound
// from one that stopped on the wall-time bound, and the continuation loop of a
// large unit was visible only as hundreds of River snoozes.
type ChunkContinuationError struct {
	Next time.Time
	// Reason is ChunkStopChunkBound or ChunkStopWallTime; empty when a caller
	// that predates the signal built the error.
	Reason string
	// Chunks is the number of chunks this attempt committed before stopping.
	Chunks int
	// Elapsed is the wall time this attempt ran before stopping.
	Elapsed time.Duration
}

// Stop reasons an attempt reports when it hands a unit to a continuation.
const (
	ChunkStopChunkBound = "chunk_bound"
	ChunkStopWallTime   = "wall_time"
)

// newChunkContinuation builds the continuation error for an attempt that
// committed `chunks` chunks in `elapsed`. The chunk-count bound is reported
// first when both bounds are reached, because it is the one a policy change can
// move without touching the lease.
func newChunkContinuation(policy ChunkPolicy, next time.Time, chunks int, elapsed time.Duration) ChunkContinuationError {
	reason := ChunkStopWallTime
	if chunks >= policy.MaxChunksPerAttempt {
		reason = ChunkStopChunkBound
	}
	return ChunkContinuationError{Next: next, Reason: reason, Chunks: chunks, Elapsed: elapsed}
}

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
	// A prepared chunk is bounded by both the provider-page budget and the sink effect-row budget. The smaller bound is intentional:
	// a provider can emit several normalized effects for one source item, so using only the effect cap would allow one source page to
	// grow without limit.
	capRows := min(policy.MaxEffectRows, policy.MaxSourceItems)
	packer := newChunkPacker(batch, comparison, policy, ordered, capRows)
	return packer.pack()
}

// chunkPacker cuts one emission into prepared chunks greedily: a chunk takes the next r rows of EVERY destination (so chunk
// boundaries stay row-aligned across the destinations), r as large as the row cap and the byte limit allow. The byte limit follows the
// chunk's own largest slice: the reserved limit for two or more rows, the policy limit for one row (nothing left to shrink), so a row
// alone is refused only when it is over the policy limit. The trailing destination (effectSplitsLast) and the final metadata ride the
// last chunk when they fit, else a chunk of their own after it. Row sizes are summed from prefix arrays; each chunk is encoded
// exactly once to confirm its size.
type chunkPacker struct {
	batch      CompleteRouteBatch
	comparison ShadowComparison
	policy     ChunkPolicy
	ordered    []EffectBatch
	capRows    int
	body       []int // indexes into ordered of the destinations packed in row order
	tail       int   // index into ordered of the trailing destination, -1 when none
	prefix     [][]int
	maxBody    int
	skeleton   int
}

func newChunkPacker(batch CompleteRouteBatch, comparison ShadowComparison, policy ChunkPolicy, ordered []EffectBatch, capRows int) *chunkPacker {
	packer := &chunkPacker{batch: batch, comparison: comparison, policy: policy, ordered: ordered, capRows: capRows, tail: -1}
	packer.prefix = make([][]int, len(ordered))
	for index, effect := range ordered {
		if effectSplitsLast(effect.Destination) {
			packer.tail = index
		} else {
			packer.body = append(packer.body, index)
			packer.maxBody = max(packer.maxBody, len(effect.Rows))
		}
		sums := make([]int, len(effect.Rows)+1)
		for position, row := range effect.Rows {
			sums[position+1] = sums[position] + len(row) + 1
		}
		packer.prefix[index] = sums
	}
	return packer
}

func (packer *chunkPacker) slice(index, from, take int) []json.RawMessage {
	rows := packer.ordered[index].Rows
	lo, hi := min(from, len(rows)), min(from+take, len(rows))
	return rows[lo:hi]
}

// estimate is a safe upper bound of the encoded size of the chunk made of rows [from, from+take) of every body destination.
func (packer *chunkPacker) estimate(from, take int) int {
	total := packer.skeleton
	for _, index := range packer.body {
		sums := packer.prefix[index]
		lo, hi := min(from, len(sums)-1), min(from+take, len(sums)-1)
		total += sums[hi] - sums[lo]
	}
	return total
}

// build makes the chunk of rows [from, from+take) of every body destination plus tailTake rows of the trailing destination; final
// attaches the final metadata. It returns the chunk and its exact encoded size.
func (packer *chunkPacker) build(from, take, tailFrom, tailTake int, final bool) (PreparedProviderChunk, int, error) {
	effects := make([]EffectBatch, 0, len(packer.ordered))
	for index, effect := range packer.ordered {
		var rows []json.RawMessage
		if index == packer.tail {
			rows = packer.slice(index, tailFrom, tailTake)
		} else {
			rows = packer.slice(index, from, take)
		}
		part, err := BuildEffectBatch(effect.Destination, effect.Recovery, rows)
		if err != nil {
			return PreparedProviderChunk{}, 0, err
		}
		effects = append(effects, part)
	}
	payloadBytes := 0
	for _, effect := range effects {
		payloadBytes += effect.PayloadBytes
	}
	chunk := PreparedProviderChunk{
		SchemaVersion: chunkPayloadSchemaVersion, RouteVersion: chunkRouteVersion, Effects: effects, PayloadBytes: payloadBytes,
	}
	if final {
		packer.attachFinal(&chunk)
	}
	encoded, err := encodedPreparedChunkPayload(chunk)
	if err != nil {
		return PreparedProviderChunk{}, 0, err
	}
	chunk.PayloadBytes = len(encoded)
	return chunk, len(encoded), nil
}

func (packer *chunkPacker) attachFinal(chunk *PreparedProviderChunk) {
	chunk.InventoryComplete = true
	chunk.Result = cloneChunkResult(packer.batch.Result)
	chunk.Watermark = packer.batch.Watermark
	chunk.Evidence = packer.batch.Evidence
	chunk.Comparison = packer.comparison
	chunk.WorklogObservations = append([]JiraWorklogFetchObservation(nil), packer.batch.WorklogObservations...)
}

func (packer *chunkPacker) limit(rows int) int {
	if rows <= 1 {
		return packer.policy.MaxPreparedBytes
	}
	return splitPayloadLimit(packer.policy)
}

func (packer *chunkPacker) pack() ([]PreparedProviderChunk, error) {
	skeleton, _, err := packer.build(0, 0, 0, 0, false)
	if err != nil {
		return nil, err
	}
	// The estimate is the measured empty skeleton plus the rows; it can be low by the payload_bytes digits of the destinations that
	// have rows (about 40 bytes), which the exact encode of the chosen chunk below corrects.
	packer.skeleton = skeleton.PayloadBytes
	var chunks []PreparedProviderChunk
	var spans [][2]int
	for from := 0; from < packer.maxBody; {
		remaining := packer.maxBody - from
		take := min(packer.capRows, remaining)
		if take >= 2 {
			reserved := splitPayloadLimit(packer.policy)
			low, high := 1, take // the largest take >= 2 whose estimate fits the reserved limit, else 1
			for low < high {
				middle := (low + high + 1) / 2
				if packer.estimate(from, middle) <= reserved {
					low = middle
				} else {
					high = middle - 1
				}
			}
			take = low
		}
		var chunk PreparedProviderChunk
		for {
			var size int
			chunk, size, err = packer.build(from, take, 0, 0, false)
			if err != nil {
				return nil, err
			}
			if take <= 1 {
				if size > packer.policy.MaxPreparedBytes {
					return nil, ErrChunkPolicyExceeded
				}
				break
			}
			if size <= packer.limit(take) {
				break
			}
			take -= 1 + (size-packer.limit(take))/max(1, size/take)
			take = max(take, 1)
		}
		chunks = append(chunks, chunk)
		spans = append(spans, [2]int{from, take})
		from += take
	}
	return packer.finish(chunks, spans)
}

// finish places the trailing destination's rows and the final metadata, then numbers the chunks. The trailing rows go to the LAST
// ceil(rows/cap) chunks, aligned from the end (the verdict commits after every other row of the run); the final metadata rides the
// last chunk. When a chunk would pass its byte limit with them, they go into chunks of their own after the body instead.
func (packer *chunkPacker) finish(chunks []PreparedProviderChunk, spans [][2]int) ([]PreparedProviderChunk, error) {
	tailRows := 0
	if packer.tail >= 0 {
		tailRows = len(packer.ordered[packer.tail].Rows)
	}
	groups := (tailRows + packer.capRows - 1) / packer.capRows
	total := max(len(chunks), groups, 1)
	for len(spans) < total {
		spans = append(spans, [2]int{packer.maxBody, 0})
	}
	shift := total - groups
	placed := make([]PreparedProviderChunk, 0, total)
	fits := true
	for position := 0; position < total && fits; position++ {
		tailFrom, tailTake := 0, 0
		if position >= shift {
			tailFrom = (position - shift) * packer.capRows
			tailTake = min(tailRows-tailFrom, packer.capRows)
		}
		candidate, size, err := packer.build(spans[position][0], spans[position][1], tailFrom, tailTake, position == total-1)
		if err != nil {
			return nil, err
		}
		if size > packer.limit(max(spans[position][1], tailTake)) {
			fits = false
			break
		}
		placed = append(placed, candidate)
	}
	if fits {
		return packer.number(placed)
	}
	// The trailing rows or the final metadata do not fit beside the body rows: keep the body chunks as built and pack them after.
	placed = placed[:0]
	for position := range spans {
		if spans[position][1] == 0 {
			continue
		}
		candidate, size, err := packer.build(spans[position][0], spans[position][1], 0, 0, false)
		if err != nil {
			return nil, err
		}
		if size > packer.limit(spans[position][1]) && spans[position][1] > 1 || size > packer.policy.MaxPreparedBytes {
			return nil, ErrChunkPolicyExceeded
		}
		placed = append(placed, candidate)
	}
	for tailFrom := 0; ; {
		take := min(packer.capRows, tailRows-tailFrom)
		for {
			candidate, size, err := packer.build(packer.maxBody, 0, tailFrom, take, tailFrom+take >= tailRows)
			if err != nil {
				return nil, err
			}
			if size <= packer.limit(take) {
				placed = append(placed, candidate)
				break
			}
			if take <= 1 {
				return nil, ErrChunkPolicyExceeded
			}
			take = max(1, take/2)
		}
		tailFrom += take
		if tailFrom >= tailRows {
			break
		}
	}
	return packer.number(placed)
}

func (packer *chunkPacker) number(chunks []PreparedProviderChunk) ([]PreparedProviderChunk, error) {
	for ordinal := range chunks {
		chunks[ordinal].Ordinal, chunks[ordinal].TotalChunks = ordinal, len(chunks)
		chunks[ordinal].InventoryComplete = ordinal == len(chunks)-1
		encoded, err := encodedPreparedChunkPayload(chunks[ordinal])
		if err != nil {
			return nil, err
		}
		chunks[ordinal].PayloadBytes = len(encoded)
		chunks[ordinal].ManifestDigest = preparedChunkDigest(chunks[ordinal])
	}
	return chunks, nil
}

// splitReserveBytes is what the split does not measure of a stored chunk: the ledger column (bounded as a whole by
// maxEffectLedgerStateBytes), the two cursors stamped after the split (maxChunkCursorBytes each, and the sidecar is JSON: a
// character costs up to 6 bytes escaped), and about 1 KiB for the manifest digest, the executor's ordinal and the stamped
// payload_bytes. Every other field of PreparedProviderChunk (rows, final result, watermark, evidence, comparison) is measured.
const splitReserveBytes = maxEffectLedgerStateBytes + 2*6*maxChunkCursorBytes + 1<<10

// splitPayloadLimit is the sidecar size a chunk of two or more rows may reach. The store refuses a chunk whose payload and ledger
// column together pass maxChunkPayloadBytes (buildPreparedChunkMaterial). A one-row chunk is judged on MaxPreparedBytes instead.
func splitPayloadLimit(policy ChunkPolicy) int {
	return min(policy.MaxPreparedBytes, maxChunkPayloadBytes-splitReserveBytes)
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

// chunkResultDigest returns "" for an absent aggregate so the digest stays
// paired with the SQL NULL that PrepareChunk stores. Hashing a nil map would
// yield the digest of the four bytes `null` and read as a real aggregate.
func chunkResultDigest(result map[string]any) string {
	if result == nil {
		return ""
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return ""
	}
	encoded = bytes.TrimSpace(encoded)
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}

// IsChunkStoreDataError reports a chunk store failure that Postgres raised for
// the payload itself (SQLSTATE 22P05, unsupported Unicode escape: a NUL in a
// jsonb string). The same payload fails the same way on every attempt.
func IsChunkStoreDataError(err error) bool {
	var storeErr *ChunkStoreError
	if !errors.As(err, &storeErr) {
		return false
	}
	var pgErr *pgconn.PgError
	return errors.As(storeErr.Cause, &pgErr) && pgErr.Code == "22P05"
}
