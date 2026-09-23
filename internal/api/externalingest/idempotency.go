package externalingest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"time"

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
// over the canonicalized envelope (sorted keys, compact separators),
// dropping a "legacy" entityFamily the way the Python side's
// model_dump(mode="json") + explicit pop does, so a client sending the
// default value and a client omitting it hash identically.
func computePayloadHash(envelope *BatchEnvelope) (string, error) {
	payload := map[string]any{
		"schemaVersion":  envelope.SchemaVersion,
		"idempotencyKey": envelope.IdempotencyKey,
		"records":        recordsAsJSON(envelope.Records),
	}
	source := map[string]any{
		"type":     firstNonEmpty(envelope.Source.Type, "customer_push"),
		"system":   envelope.Source.System,
		"instance": envelope.Source.Instance,
	}
	if envelope.Source.EntityFamily != "" && envelope.Source.EntityFamily != legacyEntityFamily {
		source["entityFamily"] = envelope.Source.EntityFamily
	}
	source["producer"] = optionalString(envelope.Source.Producer)
	source["producerVersion"] = optionalString(envelope.Source.ProducerVersion)
	payload["source"] = source
	if envelope.Window != nil {
		payload["window"] = map[string]any{
			"startedAt": envelope.Window.StartedAt.UTC().Format(time.RFC3339Nano),
			"endedAt":   envelope.Window.EndedAt.UTC().Format(time.RFC3339Nano),
		}
	} else {
		payload["window"] = nil
	}

	canonical, err := canonicalMarshal(payload)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(canonical)
	return hex.EncodeToString(digest[:]), nil
}

func recordsAsJSON(records []Record) []map[string]any {
	out := make([]map[string]any, len(records))
	for i, r := range records {
		out[i] = map[string]any{"kind": r.Kind, "externalId": r.ExternalID, "payload": r.Payload}
	}
	return out
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
