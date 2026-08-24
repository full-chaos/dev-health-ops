package providersync

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	preparedRouteSnapshotSchemaVersion = "v1"
	maxPreparedRouteSnapshotBytes      = 64 << 20
)

type PreparedRouteSnapshotReference struct {
	SchemaVersion string `json:"schema_version"`
	ContentDigest string `json:"content_digest"`
	PayloadBytes  int    `json:"payload_bytes"`
}

func (reference PreparedRouteSnapshotReference) validate() error {
	if reference.SchemaVersion != preparedRouteSnapshotSchemaVersion ||
		!validDigest(reference.ContentDigest) || reference.PayloadBytes < 1 ||
		reference.PayloadBytes > maxPreparedRouteSnapshotBytes {
		return ErrEffectLedgerConflict
	}
	return nil
}

func samePreparedRouteSnapshotReference(
	left *PreparedRouteSnapshotReference,
	right *PreparedRouteSnapshotReference,
) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

type PreparedRouteManifest struct {
	Batch        CompleteRouteBatch
	Comparison   ShadowComparison
	NormalizedAt time.Time
}

type storedPreparedRouteSnapshot struct {
	SchemaVersion string                 `json:"schema_version"`
	Generation    string                 `json:"generation"`
	OrgID         string                 `json:"org_id"`
	Provider      string                 `json:"provider"`
	Dataset       string                 `json:"dataset"`
	NormalizedAt  time.Time              `json:"normalized_at"`
	Effects       []storedPreparedEffect `json:"effects"`
	Result        json.RawMessage        `json:"result"`
	Watermark     *time.Time             `json:"watermark,omitempty"`
	Evidence      FetchEvidence          `json:"evidence"`
	Comparison    ShadowComparison       `json:"comparison"`
}

type storedPreparedEffect struct {
	Destination   string               `json:"destination"`
	ContentDigest string               `json:"content_digest"`
	Recovery      EffectRecoveryPolicy `json:"recovery"`
	Rows          []json.RawMessage    `json:"rows"`
	PayloadBytes  int                  `json:"payload_bytes"`
}

func encodePreparedRouteManifest(
	claim Claim,
	batch CompleteRouteBatch,
	comparison ShadowComparison,
	normalizedAt time.Time,
) ([]byte, PreparedRouteSnapshotReference, error) {
	if validatePreparedRouteManifestIdentity(claim, batch, normalizedAt) != nil ||
		!comparison.Match {
		return nil, PreparedRouteSnapshotReference{}, ErrEffectRecoveryUnsafe
	}
	result, err := json.Marshal(batch.Result)
	if err != nil || len(result) == 0 || bytes.Equal(result, []byte("null")) {
		return nil, PreparedRouteSnapshotReference{}, ErrEffectRecoveryUnsafe
	}
	var resultValue any
	if json.Unmarshal(result, &resultValue) != nil || containsPreparedRouteSensitiveKey(resultValue) {
		return nil, PreparedRouteSnapshotReference{}, ErrEffectRecoveryUnsafe
	}
	ordered := append([]EffectBatch(nil), batch.Effects...)
	sortEffectBatches(ordered)
	effects := make([]storedPreparedEffect, 0, len(ordered))
	for _, effect := range ordered {
		for _, row := range effect.Rows {
			var rowValue any
			if json.Unmarshal(row, &rowValue) != nil || containsPreparedRouteSensitiveKey(rowValue) {
				return nil, PreparedRouteSnapshotReference{}, ErrEffectRecoveryUnsafe
			}
		}
		effects = append(effects, storedPreparedEffect{
			Destination: effect.Destination, ContentDigest: effect.ContentDigest,
			Recovery: effect.Recovery, Rows: effect.Rows, PayloadBytes: effect.PayloadBytes,
		})
	}
	watermark := batch.Watermark
	if watermark != nil {
		value := watermark.UTC()
		watermark = &value
	}
	snapshot := storedPreparedRouteSnapshot{
		SchemaVersion: preparedRouteSnapshotSchemaVersion,
		Generation:    claim.GenerationKey(), OrgID: claim.OrgID,
		Provider: claim.Provider, Dataset: claim.Dataset,
		NormalizedAt: normalizedAt.UTC(), Effects: effects, Result: result,
		Watermark: watermark, Evidence: batch.Evidence, Comparison: comparison,
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil || len(encoded) < 1 || len(encoded) > maxPreparedRouteSnapshotBytes {
		return nil, PreparedRouteSnapshotReference{}, ErrEffectRecoveryUnsafe
	}
	digest := sha256.Sum256(encoded)
	reference := PreparedRouteSnapshotReference{
		SchemaVersion: preparedRouteSnapshotSchemaVersion,
		ContentDigest: hex.EncodeToString(digest[:]), PayloadBytes: len(encoded),
	}
	return encoded, reference, nil
}

func decodePreparedRouteManifest(
	raw []byte,
	claim Claim,
	state EffectLedgerState,
) (PreparedRouteManifest, error) {
	if claim.Validate() != nil || state.validate() != nil ||
		state.SchemaVersion != "v2" || state.PreparedSnapshot == nil ||
		state.Generation != claim.GenerationKey() || state.Provider != claim.Provider ||
		state.Dataset != claim.Dataset || len(raw) < 1 ||
		len(raw) > maxPreparedRouteSnapshotBytes ||
		len(raw) != state.PreparedSnapshot.PayloadBytes {
		return PreparedRouteManifest{}, ErrEffectLedgerConflict
	}
	digest := sha256.Sum256(raw)
	if hex.EncodeToString(digest[:]) != state.PreparedSnapshot.ContentDigest {
		return PreparedRouteManifest{}, ErrEffectLedgerConflict
	}
	decoder := json.NewDecoder(io.LimitReader(bytes.NewReader(raw), maxPreparedRouteSnapshotBytes+1))
	decoder.DisallowUnknownFields()
	var snapshot storedPreparedRouteSnapshot
	if err := decoder.Decode(&snapshot); err != nil {
		return PreparedRouteManifest{}, ErrEffectLedgerConflict
	}
	if err := requirePreparedRouteSnapshotEOF(decoder); err != nil {
		return PreparedRouteManifest{}, ErrEffectLedgerConflict
	}
	if snapshot.SchemaVersion != preparedRouteSnapshotSchemaVersion ||
		snapshot.Generation != claim.GenerationKey() || snapshot.OrgID != claim.OrgID ||
		snapshot.Provider != claim.Provider || snapshot.Dataset != claim.Dataset ||
		!snapshot.NormalizedAt.Equal(state.CreatedAt) || !snapshot.Comparison.Match ||
		len(snapshot.Effects) != len(state.Effects) || len(snapshot.Result) == 0 {
		return PreparedRouteManifest{}, ErrEffectLedgerConflict
	}
	if snapshot.Evidence.Provider != claim.Provider || snapshot.Evidence.Dataset != claim.Dataset ||
		snapshot.Evidence.Requests < 0 || snapshot.Evidence.Pages < 0 || snapshot.Evidence.Records < 0 {
		return PreparedRouteManifest{}, ErrEffectLedgerConflict
	}
	var result map[string]any
	if json.Unmarshal(snapshot.Result, &result) != nil || result == nil ||
		containsPreparedRouteSensitiveKey(result) {
		return PreparedRouteManifest{}, ErrEffectLedgerConflict
	}
	effects := make([]EffectBatch, 0, len(snapshot.Effects))
	for index, stored := range snapshot.Effects {
		rebuilt, err := BuildEffectBatch(stored.Destination, stored.Recovery, stored.Rows)
		if err != nil || rebuilt.ContentDigest != stored.ContentDigest ||
			rebuilt.PayloadBytes != stored.PayloadBytes ||
			state.Effects[index].Destination != rebuilt.Destination ||
			state.Effects[index].ContentDigest != rebuilt.ContentDigest ||
			state.Effects[index].RowCount != len(rebuilt.Rows) ||
			state.Effects[index].Recovery != rebuilt.Recovery {
			return PreparedRouteManifest{}, ErrEffectLedgerConflict
		}
		effects = append(effects, rebuilt)
	}
	batch := CompleteRouteBatch{
		Effects: effects, Result: result, Watermark: snapshot.Watermark,
		Evidence: snapshot.Evidence,
	}
	if validatePreparedRouteManifestIdentity(claim, batch, snapshot.NormalizedAt) != nil {
		return PreparedRouteManifest{}, ErrEffectLedgerConflict
	}
	return PreparedRouteManifest{
		Batch: batch, Comparison: snapshot.Comparison,
		NormalizedAt: snapshot.NormalizedAt.UTC(),
	}, nil
}

func requirePreparedRouteSnapshotEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return ErrEffectLedgerConflict
	}
	return nil
}

// preparedRouteManifestDestinationsMatch compares a manifest's destination set
// against the one github emits TODAY.
//
// It is split out of the identity check, and returns its OWN error, because the
// two failures need different answers. Every other identity failure means the
// document is not this claim's, or has been tampered with, and the only safe
// response is to refuse. A destination-set difference means something else
// entirely: the document is authentically ours and simply predates a manifest
// change. Collapsing them into one error forced the caller to treat a routine
// deploy like tampering.
//
// The GITHUB list. This validator refuses any other provider outright, so the
// shared family list was never right here -- and once github grew two
// destinations of its own (CHAOS-4194) it became actively wrong: a run that
// persisted an eighteen-effect snapshot and crashed would reload it, compare
// eighteen against sixteen, and refuse to replay or complete the unit.
func preparedRouteManifestDestinationsMatch(batch CompleteRouteBatch) error {
	got := make([]string, 0, len(batch.Effects))
	for _, effect := range batch.Effects {
		got = append(got, effect.Destination)
	}
	sort.Strings(got)
	want := githubWorkItemRouteDestinations()
	sort.Strings(want)
	if !slices.Equal(got, want) {
		return ErrPreparedSnapshotManifestMismatch
	}
	return nil
}

// preparedSnapshotReplayable reports whether a persisted ledger may be
// DISCARDED and its route re-run from the claim.
//
// Discarding is only safe if every effect the old document describes can be
// produced again and land idempotently. Readback-fenced and replay-safe effects
// both qualify: the committer inspects before writing and replays only what the
// readback reports absent. An effect recorded as recovery-BLOCKED does not --
// that classification exists precisely to say "this cannot be redone safely" --
// so a document containing one is refused rather than discarded, loudly.
//
// Checked against the PERSISTED document rather than assumed from today's
// builder. Today every github destination is built EffectReadbackRequired
// (BuildGitHubWorkItemEffects), so this answers true for every real snapshot;
// but the document being judged was written by an OLDER binary, which is the
// whole situation, and reading its own recorded classification is the only
// honest way to ask.
func preparedSnapshotReplayable(state EffectLedgerState) bool {
	for _, effect := range state.Effects {
		if effect.Recovery == EffectRecoveryBlocked {
			return false
		}
	}
	return true
}

func validatePreparedRouteManifestIdentity(
	claim Claim,
	batch CompleteRouteBatch,
	normalizedAt time.Time,
) error {
	if claim.Validate() != nil || claim.Provider != "github" || claim.Dataset != "work-items" ||
		normalizedAt.IsZero() || batch.Result == nil ||
		batch.Evidence.Provider != claim.Provider || batch.Evidence.Dataset != claim.Dataset {
		return ErrEffectRecoveryUnsafe
	}
	if err := preparedRouteManifestDestinationsMatch(batch); err != nil {
		return err
	}
	return nil
}

func containsPreparedRouteSensitiveKey(value any) bool {
	switch typed := value.(type) {
	case map[string]any:
		for key, nested := range typed {
			normalized := strings.ToLower(strings.ReplaceAll(strings.TrimSpace(key), "-", "_"))
			switch normalized {
			case "authorization", "credential", "credentials", "token", "headers",
				"source_metadata", "integration_config", "ciphertext", "raw_payload",
				"response_body":
				return true
			}
			if containsPreparedRouteSensitiveKey(nested) {
				return true
			}
		}
	case []any:
		for _, nested := range typed {
			if containsPreparedRouteSensitiveKey(nested) {
				return true
			}
		}
	}
	return false
}

func (repository *PostgresRepository) PrepareRouteSnapshot(
	ctx context.Context,
	claim Claim,
	batch CompleteRouteBatch,
	comparison ShadowComparison,
	normalizedAt time.Time,
) (EffectLedgerState, error) {
	if repository == nil || repository.Pool == nil || ctx == nil {
		return EffectLedgerState{}, ErrInvalidConfiguration
	}
	payload, reference, err := encodePreparedRouteManifest(
		claim, batch, comparison, normalizedAt,
	)
	if err != nil {
		return EffectLedgerState{}, err
	}
	desired, err := NewEffectLedgerState(claim, batch.Effects, normalizedAt)
	if err != nil {
		return EffectLedgerState{}, err
	}
	desired.SchemaVersion = "v2"
	desired.PreparedSnapshot = &reference
	var prepared EffectLedgerState
	err = repository.mutateGenerationJournalTx(
		ctx, claim, normalizedAt,
		func(tx pgx.Tx, document map[string]json.RawMessage) error {
			raw := document[effectLedgerResultKey]
			if len(raw) != 0 {
				prepared, err = decodeEffectLedgerState(raw)
				if err != nil || !sameEffectManifest(prepared, desired) {
					return ErrEffectLedgerConflict
				}
				return verifyPreparedRouteSnapshotRow(ctx, tx, claim, prepared, payload)
			}
			prepared = desired
			prepared.CreatedAt = normalizedAt.UTC()
			prepared.UpdatedAt = normalizedAt.UTC()
			encoded := encodeEffectLedgerState(prepared)
			if len(encoded) == 0 {
				return ErrEffectRecoveryUnsafe
			}
			if _, err := tx.Exec(
				ctx, insertPreparedRouteSnapshotSQL,
				claim.OrgID, claim.ID, claim.GenerationKey(), claim.Provider, claim.Dataset,
				reference.SchemaVersion, reference.ContentDigest, reference.PayloadBytes,
				payload, normalizedAt.UTC(),
			); err != nil {
				return ErrEffectLedgerConflict
			}
			document[effectLedgerResultKey] = encoded
			return nil
		},
	)
	return prepared, err
}

func (repository *PostgresRepository) LoadRouteSnapshot(
	ctx context.Context,
	claim Claim,
	state EffectLedgerState,
	now time.Time,
) (PreparedRouteManifest, error) {
	if repository == nil || repository.Pool == nil || ctx == nil || now.IsZero() ||
		claim.Validate() != nil || state.validate() != nil ||
		state.SchemaVersion != "v2" || state.PreparedSnapshot == nil {
		return PreparedRouteManifest{}, ErrInvalidConfiguration
	}
	var schemaVersion, contentDigest string
	var payloadBytes int
	var payload []byte
	var leaseLive, runLive bool
	if err := repository.Pool.QueryRow(
		ctx, loadPreparedRouteSnapshotSQL,
		claim.OrgID, claim.ID, claim.GenerationKey(), claim.Provider, claim.Dataset,
		claim.Owner, now.UTC(),
	).Scan(
		&schemaVersion, &contentDigest, &payloadBytes, &payload,
		&leaseLive, &runLive,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return PreparedRouteManifest{}, ErrPreparedRouteSnapshotNotFound
		}
		return PreparedRouteManifest{}, err
	}
	// Order matters twice over. The snapshot demonstrably exists for this
	// tenant/unit/generation, so a refusal here is about entitlement, not
	// absence -- reporting it as "not found" told a recovering worker to fail
	// closed for the wrong reason and hid genuine handoffs. And a terminal run
	// is checked BEFORE the lease, because a finished run explains a live
	// lease that is simply no longer relevant; the reverse order would send an
	// operator hunting a lease problem that does not exist.
	if !runLive {
		return PreparedRouteManifest{}, ErrPreparedRouteSnapshotRunTerminal
	}
	if !leaseLive {
		return PreparedRouteManifest{}, ErrLeaseLost
	}
	if schemaVersion != state.PreparedSnapshot.SchemaVersion ||
		contentDigest != state.PreparedSnapshot.ContentDigest ||
		payloadBytes != state.PreparedSnapshot.PayloadBytes || payloadBytes != len(payload) {
		return PreparedRouteManifest{}, ErrEffectLedgerConflict
	}
	return decodePreparedRouteManifest(payload, claim, state)
}

func verifyPreparedRouteSnapshotRow(
	ctx context.Context,
	tx pgx.Tx,
	claim Claim,
	state EffectLedgerState,
	wantPayload []byte,
) error {
	var schemaVersion, digest string
	var payloadBytes int
	var payload []byte
	if err := tx.QueryRow(
		ctx, loadPreparedRouteSnapshotRowSQL,
		claim.OrgID, claim.ID, claim.GenerationKey(), claim.Provider, claim.Dataset,
	).Scan(&schemaVersion, &digest, &payloadBytes, &payload); err != nil {
		// Only an absent row is a ledger conflict. Folding every error into
		// ErrEffectLedgerConflict is what let a permission failure -- one that
		// happened on EVERY re-prepare in production -- read as ordinary
		// disagreement between the ledger and the sidecar. It would not have
		// retried forever: it would have burned the unit's MaxAttempts and
		// then terminalized under the generic provider_unit_exhausted
		// category, with the actual cause (SQLSTATE 42501) nowhere in the
		// record. A database error must arrive as itself.
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrEffectLedgerConflict
		}
		return err
	}
	reference := state.PreparedSnapshot
	if reference == nil || schemaVersion != reference.SchemaVersion ||
		digest != reference.ContentDigest || payloadBytes != reference.PayloadBytes ||
		payloadBytes != len(payload) || !bytes.Equal(payload, wantPayload) {
		return ErrEffectLedgerConflict
	}
	return nil
}

// A plain INSERT, deliberately without ON CONFLICT. The primary key is
// (org_id, sync_run_unit_id, generation), and this statement runs only on the
// branch where the ledger key was absent -- so a conflict would mean a
// snapshot row exists for a generation whose ledger does not, which is a state
// no code path produces and which should fail loudly rather than be papered
// over by an upsert.
//
// One interaction is worth naming because it is unreachable today and would
// not be obvious later: EffectLedgerReplanner lets a route discard a prepared
// manifest and build a new one for the SAME generation. A route that both
// replans and prepares snapshots would hit this insert with the old row still
// present. github/work-items has no replanner (only GitHub blame does, and it
// does not use snapshots), so the combination cannot occur -- but whoever
// gives a snapshot route a replanner must delete the old snapshot inside the
// replan transaction first.
const insertPreparedRouteSnapshotSQL = `
INSERT INTO public.sync_run_unit_effect_snapshots (
    org_id, sync_run_unit_id, generation, provider, dataset_key,
    schema_version, content_digest, payload_bytes, payload, created_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`

// loadPreparedRouteSnapshotSQL answers three questions separately on purpose:
// does this snapshot exist for this tenant/unit/generation, is the caller
// still the unit's writer, and is the owning run still live. Folding them into
// one WHERE clause made all three the same sentinel, so "someone else took
// over", "this run already finished" and "there is nothing to recover" were
// indistinguishable to the caller and in any log.
//
// Splitting lease from run status is not pedantry: a finalized run with a
// perfectly live lease reported ErrLeaseLost, which sends an operator to look
// at leases when the actual cause is a run that already reached a terminal
// state. Each term now carries its own error.
//
// The fence mirrors loadEffectLedgerSQL clause for clause, IS NOT NULL guard
// and terminal-run exclusion included: two queries deciding the same
// entitlement must not drift apart.
//
// The payload is gated behind both fences rather than selected unconditionally
// -- a refused load has no business moving up to 64 MiB across the wire, and
// the CASE means the column is never even detoasted on that path.
const loadPreparedRouteSnapshotSQL = `
SELECT fenced.schema_version, fenced.content_digest, fenced.payload_bytes,
       CASE WHEN fenced.lease_live AND fenced.run_live
            THEN fenced.payload ELSE ''::bytea END,
       fenced.lease_live, fenced.run_live
FROM (
  SELECT snapshot.schema_version, snapshot.content_digest,
         snapshot.payload_bytes, snapshot.payload,
         COALESCE(
           unit.status = 'running'
           AND unit.lease_owner = $6
           AND unit.lease_expires_at IS NOT NULL
           AND unit.lease_expires_at > $7,
           false
         ) AS lease_live,
         COALESCE(
           run.status NOT IN ('success', 'partial_failed', 'failed'),
           false
         ) AS run_live
  FROM public.sync_run_unit_effect_snapshots AS snapshot
  JOIN public.sync_run_units AS unit
    ON unit.id = snapshot.sync_run_unit_id
   AND unit.org_id = snapshot.org_id
  JOIN public.sync_runs AS run
    ON run.id = unit.sync_run_id AND run.org_id = unit.org_id
  WHERE snapshot.org_id = $1
    AND snapshot.sync_run_unit_id = $2
    AND snapshot.generation = $3
    AND snapshot.provider = $4
    AND snapshot.dataset_key = $5
) AS fenced`

// loadPreparedRouteSnapshotRowSQL deliberately takes NO row lock. It used to
// end in FOR UPDATE, which is a permission error rather than a lock: any
// row-locking clause requires an UPDATE-class privilege, and the domain role
// holds only SELECT/INSERT/DELETE here (see required_table_privileges). That
// made every re-prepare fail with "permission denied" in production while
// passing in tests, which run as the owner. Widening the grant to buy the lock
// would hand the role a privilege nothing else needs -- and the lock is
// already held anyway.
//
// The serialization comes from the caller: this runs only inside
// mutateGenerationJournalTx's callback, and that transaction opens with
// lockGenerationJournalSQL (generation_journal.go), which ends in
// `FOR UPDATE OF unit` on the owning sync_run_units row. A snapshot row is
// keyed by (org_id, sync_run_unit_id, generation) and is only ever written by
// the holder of that unit's lease, so a competing writer must take the same
// unit lock first. Locking the snapshot row again adds nothing.
//
// sync_run_units is where the repo already pays for this: the domain role is
// granted UPDATE on it partly because Fanout's FOR SHARE needs the privilege.
const loadPreparedRouteSnapshotRowSQL = `
SELECT schema_version, content_digest, payload_bytes, payload
FROM public.sync_run_unit_effect_snapshots
WHERE org_id = $1 AND sync_run_unit_id = $2 AND generation = $3
  AND provider = $4 AND dataset_key = $5`
