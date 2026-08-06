package providersync

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
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
	sort.Slice(ordered, func(left, right int) bool {
		return ordered[left].Destination < ordered[right].Destination
	})
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
	got := make([]string, 0, len(batch.Effects))
	for _, effect := range batch.Effects {
		got = append(got, effect.Destination)
	}
	sort.Strings(got)
	want := workItemRouteDestinations()
	sort.Strings(want)
	if len(got) != len(want) {
		return ErrEffectRecoveryUnsafe
	}
	for index := range got {
		if got[index] != want[index] {
			return ErrEffectRecoveryUnsafe
		}
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
	if err := repository.Pool.QueryRow(
		ctx, loadPreparedRouteSnapshotSQL,
		claim.OrgID, claim.ID, claim.GenerationKey(), claim.Provider, claim.Dataset,
		claim.Owner, now.UTC(),
	).Scan(&schemaVersion, &contentDigest, &payloadBytes, &payload); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return PreparedRouteManifest{}, ErrPreparedRouteSnapshotNotFound
		}
		return PreparedRouteManifest{}, ErrEffectLedgerConflict
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
		return ErrEffectLedgerConflict
	}
	reference := state.PreparedSnapshot
	if reference == nil || schemaVersion != reference.SchemaVersion ||
		digest != reference.ContentDigest || payloadBytes != reference.PayloadBytes ||
		payloadBytes != len(payload) || !bytes.Equal(payload, wantPayload) {
		return ErrEffectLedgerConflict
	}
	return nil
}

const insertPreparedRouteSnapshotSQL = `
INSERT INTO public.sync_run_unit_effect_snapshots (
    org_id, sync_run_unit_id, generation, provider, dataset_key,
    schema_version, content_digest, payload_bytes, payload, created_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`

const loadPreparedRouteSnapshotSQL = `
SELECT snapshot.schema_version, snapshot.content_digest,
       snapshot.payload_bytes, snapshot.payload
FROM public.sync_run_unit_effect_snapshots AS snapshot
JOIN public.sync_run_units AS unit
  ON unit.id = snapshot.sync_run_unit_id
 AND unit.org_id = snapshot.org_id
WHERE snapshot.org_id = $1
  AND snapshot.sync_run_unit_id = $2
  AND snapshot.generation = $3
  AND snapshot.provider = $4
  AND snapshot.dataset_key = $5
  AND unit.lease_owner = $6
  AND unit.lease_expires_at > $7
  AND unit.status = 'running'`

const loadPreparedRouteSnapshotRowSQL = `
SELECT schema_version, content_digest, payload_bytes, payload
FROM public.sync_run_unit_effect_snapshots
WHERE org_id = $1 AND sync_run_unit_id = $2 AND generation = $3
  AND provider = $4 AND dataset_key = $5
FOR UPDATE`
