package externalingest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"os"
	"strconv"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type idempotencyOutcomeKind string

const (
	outcomeNew      idempotencyOutcomeKind = "new"
	outcomeReplay   idempotencyOutcomeKind = "replay"
	outcomeConflict idempotencyOutcomeKind = "conflict"
	outcomeRetry    idempotencyOutcomeKind = "retry"
)

var retryableStatuses = map[string]bool{"stream_unavailable": true, "failed": true}

const acceptedStaleMinutesDefault = 15

// errIngestTemporarilyUnavailable mirrors idempotency.py's
// IngestTemporarilyUnavailableError: a true concurrent same-key race whose
// winner is not yet visible.
var errIngestTemporarilyUnavailable = errors.New("a concurrent request for the same idempotency key is in progress")

type idempotencyOutcome struct {
	Kind  idempotencyOutcomeKind
	Batch BatchRow
}

// computePayloadHash mirrors idempotency.py's compute_payload_hash: sha256
// over json.dumps(envelope.model_dump(mode="json"), sort_keys=True,
// separators=(",", ":"), ensure_ascii=True). Three things a naive port
// gets wrong, each confirmed by a live digest mismatch on a real batch
// (CHAOS-6350):
//
//  1. model_dump(mode="json") uses pydantic FIELD NAMES (snake_case),
//     never the wire aliases (camelCase) -- by_alias is not passed.
//  2. It keeps a JSON integer an int, never coerces it to a float --
//     BatchEnvelope.Records[i].Payload (map[string]any, built by
//     parseEnvelope's toAny helper for the accept path's own storage) DOES
//     coerce every integer to float64, so a payload field hashes as "5.0"
//     here and "5" in Python. Record.ordered (the exact *pyjson.Object
//     ValidateEnvelopeJSON produced, Int and Float kept distinct) is the
//     correct hash input instead.
//  3. It preserves a datetime's original aware offset and formats
//     microseconds Pydantic's way (never trimmed, omitted only when zero)
//     -- IngestWindow.StartedAt/EndedAt (UTC-normalized time.Time) lose the
//     offset; startedRaw/endedRaw (pytime.DateTime, parseEnvelope's copy of
//     ValidateEnvelopeJSON's own parse) keep it, formatted with
//     pytime.Pydantic instead of time.Time.UTC().Format(RFC3339Nano).
//
// dropping a "legacy" entityFamily the way the Python side's
// model_dump(mode="json") + explicit pop does, so a client sending the
// default value and a client omitting it hash identically.
func computePayloadHash(envelope *BatchEnvelope) (string, error) {
	payload := pyjson.NewObject()
	payload.Set("schema_version", envelope.SchemaVersion)
	payload.Set("idempotency_key", envelope.IdempotencyKey)
	payload.Set("source", sourceCanonical(envelope.Source))
	if envelope.Window != nil {
		payload.Set("window", windowCanonical(envelope.Window))
	} else {
		payload.Set("window", nil)
	}
	records := make([]pyjson.Value, len(envelope.Records))
	for i, r := range envelope.Records {
		records[i] = recordCanonical(r)
	}
	payload.Set("records", records)

	canonical, err := canonicalMarshal(payload)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(canonical)
	return hex.EncodeToString(digest[:]), nil
}

// sourceCanonical builds SourceDescriptor's model_dump(mode="json") shape:
// type, system, instance, entity_family (popped when "legacy", exactly the
// existing conditional, just snake_case), producer, producer_version.
func sourceCanonical(s SourceDescriptor) *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("type", firstNonEmpty(s.Type, "customer_push"))
	out.Set("system", s.System)
	out.Set("instance", s.Instance)
	if s.EntityFamily != "" && s.EntityFamily != legacyEntityFamily {
		out.Set("entity_family", s.EntityFamily)
	}
	out.Set("producer", optionalString(s.Producer))
	out.Set("producer_version", optionalString(s.ProducerVersion))
	return out
}

// windowCanonical builds IngestWindow's model_dump(mode="json") shape:
// started_at/ended_at, Pydantic's datetime JSON form (offset preserved,
// microseconds exact) via the parsed pytime.DateTime, not the
// UTC-normalized time.Time the accept path otherwise uses.
func windowCanonical(w *IngestWindow) *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("started_at", pytime.Pydantic(w.startedRaw))
	out.Set("ended_at", pytime.Pydantic(w.endedRaw))
	return out
}

// recordCanonical builds RecordEnvelope's model_dump(mode="json") shape:
// kind, external_id, payload -- payload from Record.ordered (Int/Float
// exact), with sanitizeNonFinite's NaN/Infinity -> null conversion applied,
// matching a live-confirmed Pydantic model_dump(mode="json") behavior.
func recordCanonical(r Record) *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("kind", r.Kind)
	out.Set("external_id", r.ExternalID)
	out.Set("payload", sanitizeNonFinite(r.ordered))
	return out
}

// sanitizeNonFinite ports model_dump(mode="json")'s float handling for a
// record payload: NaN, Infinity and -Infinity all become None (confirmed
// live against a real Pydantic model -- none of the three round-trips as a
// literal token, unlike plain json.dumps' allow_nan=True default).
// Everything else -- Int, string, bool, nil, nested objects/arrays --
// passes through unchanged; Int is never touched, so no integer loses its
// exact-int-not-float distinction here.
func sanitizeNonFinite(value pyjson.Value) pyjson.Value {
	switch typed := value.(type) {
	case pyjson.Float:
		f := float64(typed)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return nil
		}
		return typed
	case *pyjson.Object:
		out := pyjson.NewObject()
		for _, key := range typed.Keys() {
			item, _ := typed.Get(key)
			out.Set(key, sanitizeNonFinite(item))
		}
		return out
	case []pyjson.Value:
		out := make([]pyjson.Value, len(typed))
		for i, item := range typed {
			out[i] = sanitizeNonFinite(item)
		}
		return out
	default:
		return value
	}
}

func optionalString(s *string) any {
	if s == nil {
		return nil
	}
	return *s
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func acceptedStaleMinutes() int {
	if raw := os.Getenv("EXTERNAL_INGEST_ACCEPTED_STALE_MINUTES"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil {
			return parsed
		}
	}
	return acceptedStaleMinutesDefault
}

func classifyExisting(existing BatchRow, payloadHash string, now time.Time) idempotencyOutcome {
	if existing.PayloadHash != payloadHash {
		return idempotencyOutcome{Kind: outcomeConflict, Batch: existing}
	}
	if retryableStatuses[existing.Status] {
		return idempotencyOutcome{Kind: outcomeRetry, Batch: existing}
	}
	if existing.Status == "accepted" && now.Sub(existing.UpdatedAt) > time.Duration(acceptedStaleMinutes())*time.Minute {
		return idempotencyOutcome{Kind: outcomeRetry, Batch: existing}
	}
	return idempotencyOutcome{Kind: outcomeReplay, Batch: existing}
}

// resolveBatchIdempotency ports idempotency.py's resolve_batch_idempotency:
// NEW / REPLAY / CONFLICT / RETRY over the (org_id, source_system,
// source_instance, entity_family, idempotency_key) unique index. Runs
// inside tx, the accept sequence's own transaction -- the caller commits.
func resolveBatchIdempotency(
	ctx context.Context, tx pgx.Tx, params createBatchParams,
) (idempotencyOutcome, error) {
	now := time.Now().UTC()
	existing, err := findExistingBatchTx(ctx, tx, params.OrgID, params.SourceSystem, params.SourceInstance, params.EntityFamily, params.IdempotencyKey)
	if err != nil {
		return idempotencyOutcome{}, err
	}
	if existing != nil {
		return classifyExisting(*existing, params.PayloadHash, now), nil
	}

	params.IngestionID = uuid.New()
	// The INSERT runs inside a SAVEPOINT (pgx's Tx.Begin on an existing Tx
	// is a pseudo-nested transaction implemented that way), matching
	// idempotency.py's own comment on create_batch: "The insert runs
	// inside create_batch's SAVEPOINT, so a losing racer only rolls back
	// the savepoint, not the caller's session." Without this, a unique-
	// violation aborts the WHOLE outer transaction (Postgres puts a
	// failed transaction in a state where every subsequent statement is
	// rejected until rollback), so the findExistingBatchTx query just
	// below -- on the same tx -- would itself fail, turning a routine
	// concurrent-duplicate race into a 500 instead of the REPLAY/RETRY/
	// CONFLICT outcome it actually is (TestResolveBatchIdempotencyUnder
	// ConcurrentDuplicateInserts pins this).
	nested, err := tx.Begin(ctx)
	if err != nil {
		return idempotencyOutcome{}, err
	}
	created, err := createBatchTx(ctx, nested, params)
	if err != nil {
		if rollbackErr := nested.Rollback(ctx); rollbackErr != nil {
			return idempotencyOutcome{}, rollbackErr
		}
		if isUniqueViolation(err) {
			raced, raceErr := findExistingBatchTx(ctx, tx, params.OrgID, params.SourceSystem, params.SourceInstance, params.EntityFamily, params.IdempotencyKey)
			if raceErr != nil {
				return idempotencyOutcome{}, raceErr
			}
			if raced == nil {
				return idempotencyOutcome{}, errIngestTemporarilyUnavailable
			}
			return classifyExisting(*raced, params.PayloadHash, now), nil
		}
		return idempotencyOutcome{}, err
	}
	if err := nested.Commit(ctx); err != nil {
		return idempotencyOutcome{}, err
	}
	return idempotencyOutcome{Kind: outcomeNew, Batch: *created}, nil
}

func isUniqueViolation(err error) bool {
	var pgErr interface{ SQLState() string }
	if errors.As(err, &pgErr) {
		return pgErr.SQLState() == "23505"
	}
	return false
}

// marshalJSONColumn is a tiny helper so callers writing a JSON column never
// forget to handle a nil map the same way (an empty JSON object, matching
// SQLAlchemy's JSON default=dict for these columns' Python counterparts
// where relevant, or NULL where the Python model allows it -- each call site
// says which).
func marshalJSONColumn(v any) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	return json.Marshal(v)
}
