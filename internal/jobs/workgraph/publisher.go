package workgraph

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/jackc/pgx/v5"
)

// RequestWriter is safe to use inside a caller-owned transaction. In
// particular, native post-sync fanout can create the immutable request and its
// outbox handoff atomically, so a crash cannot produce only one half.
type RequestWriter struct {
	producer *joboutbox.Producer
	registry joboutbox.PolicyRegistry
}

func NewRequestWriter(registry joboutbox.PolicyRegistry) (*RequestWriter, error) {
	producer, err := joboutbox.NewTransactionProducer(registry)
	if err != nil {
		return nil, ErrUnavailable
	}
	return &RequestWriter{producer: producer, registry: registry}, nil
}

// WriteTx is WriteRequestTx for the callers that have nothing to report. It
// exists so the five existing producers keep one-line call sites; a producer
// that sets Coalesce, or that wants to know its start was bounded, calls
// WriteRequestTx and logs the outcome.
func (writer *RequestWriter) WriteTx(ctx context.Context, tx pgx.Tx, request Request) error {
	_, err := writer.WriteRequestTx(ctx, tx, request)
	return err
}

// WriteRequestTx writes the authoritative request and its outbox handoff, and
// reports the two effects a caller cannot otherwise see: which pending
// requests this one superseded, and whether its start had to be bounded.
//
// The ORDER inside the caller's transaction is load-bearing and is the reason
// coalescing lives here rather than in a producer:
//
//  1. bound the materialize scope, so the row that gets written and the key
//     that gets compared are the same bytes;
//  2. supersede this producer's matching pending requests, making each one
//     TERMINAL ('canceled') -- see supersedePendingSQL for why terminal-first
//     is the only ordering the outbox strand repair cannot undo;
//  3. insert this request and publish its handoff.
//
// All three commit with the caller's transaction or none of them do, so there
// is no instant at which the old request is cancelled and the new one does not
// exist.
func (writer *RequestWriter) WriteRequestTx(
	ctx context.Context, tx pgx.Tx, request Request,
) (WriteOutcome, error) {
	if writer == nil || writer.producer == nil || writer.registry == nil ||
		tx == nil || !validRequest(request) {
		return WriteOutcome{}, ErrInvalidState
	}
	var outcome WriteOutcome
	if request.Kind == KindMaterialize {
		bounded, addedFromDate, cappedWindowDays, err := boundMaterializeStart(request.Scope)
		if err != nil {
			return WriteOutcome{}, err
		}
		request.Scope = bounded
		outcome.BoundedFromDate, outcome.BoundedWindowDays = addedFromDate, cappedWindowDays
		// Re-validated because bounding rewrote the scope: the 8192-byte
		// bound is a column contract, and a scope that was one date-string
		// short of it before must not become an oversized INSERT here.
		if !validRequest(request) {
			return WriteOutcome{}, ErrInvalidState
		}
	}
	if request.Coalesce {
		superseded, err := writer.supersedeTx(ctx, tx, request)
		if err != nil {
			return WriteOutcome{}, err
		}
		outcome.SupersededRequestIDs = superseded
	}
	if err := writer.writeRowTx(ctx, tx, request); err != nil {
		return WriteOutcome{}, err
	}
	return outcome, nil
}

// supersedeTx cancels this producer's pending duplicates of request and
// returns their ids. A supersede that matches nothing is the ordinary case and
// is not an error.
func (writer *RequestWriter) supersedeTx(
	ctx context.Context, tx pgx.Tx, request Request,
) ([]string, error) {
	rows, err := tx.Query(ctx, supersedePendingSQL,
		request.OrganizationID, string(request.Kind), string(request.Scope),
		request.CorrelationID, request.ID)
	if err != nil {
		return nil, ErrUnavailable
	}
	defer rows.Close()
	var superseded []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, ErrUnavailable
		}
		superseded = append(superseded, id)
	}
	if rows.Err() != nil {
		return nil, ErrUnavailable
	}
	return superseded, nil
}

func (writer *RequestWriter) writeRowTx(ctx context.Context, tx pgx.Tx, request Request) error {
	encodedScope := string(request.Scope)
	command, err := tx.Exec(ctx, `
INSERT INTO public.work_graph_execution_requests (
    id, org_id, kind, scope, model_ref, prompt_ref, llm_concurrency,
    spend_limit_microunits, correlation_id, idempotency_key, state
) VALUES (
    $1::uuid, $2::uuid, $3, $4::jsonb, NULLIF($5, ''), NULLIF($6, ''), $7,
    $8, $9, $10, 'pending'
)
ON CONFLICT (id) DO NOTHING`, request.ID, request.OrganizationID, string(request.Kind),
		encodedScope, request.ModelRef, request.PromptRef, request.LLMConcurrency,
		request.SpendLimitMicrounits, request.CorrelationID, request.IdempotencyKey)
	if err != nil {
		return ErrUnavailable
	}
	if command.RowsAffected() == 0 {
		var existing Request
		var existingScope []byte
		var existingState string
		err = tx.QueryRow(ctx, `
SELECT id::text, org_id::text, kind, scope::text, COALESCE(model_ref, ''),
       COALESCE(prompt_ref, ''), llm_concurrency, spend_limit_microunits,
       correlation_id, idempotency_key, state
FROM public.work_graph_execution_requests WHERE id = $1::uuid`, request.ID).Scan(
			&existing.ID, &existing.OrganizationID, &existing.Kind, &existingScope,
			&existing.ModelRef, &existing.PromptRef, &existing.LLMConcurrency,
			&existing.SpendLimitMicrounits, &existing.CorrelationID, &existing.IdempotencyKey,
			&existingState,
		)
		if err != nil || !sameRequest(existing, request, existingScope) {
			return ErrInvalidState
		}
		if existingState == "succeeded" {
			completionKey, keyErr := joboutbox.CompletionKey(
				"work_graph_execution_request", request.ID,
			)
			if keyErr != nil {
				return ErrInvalidState
			}
			if err := joboutbox.MarkCompletionTx(ctx, tx, completionKey); err != nil {
				return ErrUnavailable
			}
		}
	}
	descriptor, ok := writer.registry.Descriptor(string(request.Kind))
	if !ok {
		return ErrUnavailable
	}
	// Every arm below is normalised through workGraphPublished: work-graph requests carry a CALLER-supplied
	// idempotency key (work_graph_execution_requests.idempotency_key), so a
	// re-issued request legitimately lands on an already-delivered row, and
	// that has always been a success here. This writer holds no logger and
	// owns no repair path.
	if descriptor.Executable() {
		if request.PrerequisiteCompletionKey == "" {
			return workGraphPublished(writer.producer.Publish(ctx, tx, string(request.Kind), envelopeFor(request)))
		}
		return workGraphPublished(writer.producer.PublishAfter(
			ctx, tx, string(request.Kind), envelopeFor(request),
			request.PrerequisiteCompletionKey,
		))
	}
	if request.PrerequisiteCompletionKey == "" {
		return workGraphPublished(writer.producer.PublishDeferred(ctx, tx, string(request.Kind), envelopeFor(request)))
	}
	return workGraphPublished(writer.producer.PublishDeferredAfter(
		ctx, tx, string(request.Kind), envelopeFor(request), request.PrerequisiteCompletionKey,
	))
}

func validRequest(request Request) bool {
	return request.Kind.Valid() && validUUID(request.ID) && validUUID(request.OrganizationID) &&
		json.Valid(request.Scope) && len(request.Scope) > 1 && len(request.Scope) <= 8192 &&
		request.LLMConcurrency >= 1 && request.LLMConcurrency <= 16 &&
		request.SpendLimitMicrounits >= 0 && len(request.ModelRef) <= 128 &&
		len(request.PromptRef) <= 128 && len(request.CorrelationID) > 0 &&
		len(request.CorrelationID) <= 128 && len(request.IdempotencyKey) > 0 &&
		len(request.IdempotencyKey) <= 256 &&
		len(request.PrerequisiteCompletionKey) <= 256
}

func sameRequest(existing, expected Request, scope []byte) bool {
	return existing.ID == expected.ID && existing.OrganizationID == expected.OrganizationID &&
		existing.Kind == expected.Kind && sameJSON(scope, expected.Scope) &&
		existing.ModelRef == expected.ModelRef && existing.PromptRef == expected.PromptRef &&
		existing.LLMConcurrency == expected.LLMConcurrency &&
		existing.SpendLimitMicrounits == expected.SpendLimitMicrounits &&
		existing.CorrelationID == expected.CorrelationID && existing.IdempotencyKey == expected.IdempotencyKey
}

// sameJSON compares scope JSON semantically, not byte-for-byte. Postgres
// jsonb does not preserve object-key order or source whitespace on
// round-trip (the value read back from work_graph_execution_requests.scope
// can reorder keys relative to what the caller marshaled), so comparing
// json.Compact output directly rejected a byte-identical retry the instant
// the scope had more than one key -- exactly the crash-after-write/
// before-ack retry path WriteTx exists to make harmless. Unmarshal both
// sides into a Go value and compare those instead; map/object comparison is
// key-order-independent by construction. Canonicalizing at the producer
// (the Go marshal side) would not fix this: Go's json.Marshal of a map
// already emits keys in sorted order, but Postgres re-encodes jsonb on its
// own internal schedule on the way back out, independent of what was
// written -- the mismatch is introduced by the storage round-trip, not by
// the writer, so only a semantic comparison at the check site closes it.
//
// decodeJSONPreservingNumbers is used instead of a plain json.Unmarshal
// into `any`: the default decoder converts every JSON number to float64,
// which silently rounds integers above 2^53 (9007199254740992 and
// 9007199254740993 both decode to the same float64), so two genuinely
// different large-integer scopes would compare equal. UseNumber keeps
// numbers as their original json.Number text instead, and equalJSONValue
// compares those numbers by exact rational value rather than by literal
// text: Postgres jsonb does not preserve a number's original spelling any
// more than it preserves key order, so "1", "1.0", and "1e0" can all read
// back differently from how the caller marshaled them, and a naive
// reflect.DeepEqual over json.Number text would reject that as a mutated
// duplicate the same way the pre-fix byte comparison rejected reordered
// keys.
func sameJSON(left, right []byte) bool {
	leftValue, leftErr := decodeJSONPreservingNumbers(left)
	rightValue, rightErr := decodeJSONPreservingNumbers(right)
	if leftErr != nil || rightErr != nil {
		return false
	}
	return equalJSONValue(leftValue, rightValue)
}

func decodeJSONPreservingNumbers(data []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	return value, nil
}

// equalJSONValue compares two values decoded by decodeJSONPreservingNumbers.
// It recurses through maps and slices like reflect.DeepEqual, but gives
// json.Number its own rule: two numbers are equal when they denote the same
// exact rational value, regardless of spelling (integer vs. decimal vs.
// exponent form). big.Rat parses the decimal text exactly -- unlike
// json.Number.Float64, it never rounds, so this does not reopen the
// large-integer precision gap UseNumber exists to close.
func equalJSONValue(left, right any) bool {
	switch leftTyped := left.(type) {
	case json.Number:
		rightTyped, ok := right.(json.Number)
		return ok && sameJSONNumber(leftTyped, rightTyped)
	case map[string]any:
		rightTyped, ok := right.(map[string]any)
		if !ok || len(leftTyped) != len(rightTyped) {
			return false
		}
		for key, leftElement := range leftTyped {
			rightElement, exists := rightTyped[key]
			if !exists || !equalJSONValue(leftElement, rightElement) {
				return false
			}
		}
		return true
	case []any:
		rightTyped, ok := right.([]any)
		if !ok || len(leftTyped) != len(rightTyped) {
			return false
		}
		for index, leftElement := range leftTyped {
			if !equalJSONValue(leftElement, rightTyped[index]) {
				return false
			}
		}
		return true
	default:
		return reflect.DeepEqual(left, right)
	}
}

func sameJSONNumber(left, right json.Number) bool {
	if left == right {
		return true
	}
	leftRat, leftOK := new(big.Rat).SetString(string(left))
	rightRat, rightOK := new(big.Rat).SetString(string(right))
	return leftOK && rightOK && leftRat.Cmp(rightRat) == 0
}

func envelopeFor(request Request) jobcontract.Envelope {
	organizationID := request.OrganizationID
	domain := jobcontract.DomainLink{Type: domainFor(request.Kind), ID: request.ID}
	var payload any
	switch request.Kind {
	case KindBuild:
		payload = jobcontract.WorkGraphBuildPayload{RequestID: request.ID}
	case KindMaterialize:
		payload = jobcontract.InvestmentMaterializePayload{RequestID: request.ID}
	default:
		panic(fmt.Sprintf("unsupported work graph kind %q", request.Kind))
	}
	return jobcontract.Envelope{
		ContractVersion: jobcontract.ContractVersionV1,
		OrganizationID:  &organizationID,
		CorrelationID:   request.CorrelationID,
		IdempotencyKey:  request.IdempotencyKey,
		Domain:          domain,
		Payload:         payload,
	}
}

func domainFor(kind Kind) string {
	switch kind {
	case KindBuild:
		return "work_graph_request"
	default:
		return "investment_request"
	}
}

// workGraphPublished collapses joboutbox.ErrDeliveryAlreadyTerminal to nil.
//
// It is a helper rather than an inline branch in each of the four arms above
// because four copies of the same swallow is how one of them eventually stops
// matching the others. The sentinel means the envelope IS durably staged and
// there was nothing to insert; for this writer that has always been success,
// and nothing here can act on the distinction. Every other producer error --
// contract rejection, policy rejection, an unavailable database -- passes
// through unchanged.
func workGraphPublished(err error) error {
	if joboutbox.IsPublished(err) {
		return nil
	}
	return err
}
