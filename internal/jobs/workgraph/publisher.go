package workgraph

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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

func (writer *RequestWriter) WriteTx(ctx context.Context, tx pgx.Tx, request Request) error {
	if writer == nil || writer.producer == nil || writer.registry == nil ||
		tx == nil || !validRequest(request) {
		return ErrInvalidState
	}
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
	if descriptor.Executable() {
		if request.PrerequisiteCompletionKey == "" {
			return writer.producer.Publish(ctx, tx, string(request.Kind), envelopeFor(request))
		}
		return writer.producer.PublishAfter(
			ctx, tx, string(request.Kind), envelopeFor(request),
			request.PrerequisiteCompletionKey,
		)
	}
	if request.PrerequisiteCompletionKey == "" {
		return writer.producer.PublishDeferred(ctx, tx, string(request.Kind), envelopeFor(request))
	}
	return writer.producer.PublishDeferredAfter(
		ctx, tx, string(request.Kind), envelopeFor(request), request.PrerequisiteCompletionKey,
	)
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
// numbers as their original json.Number text instead.
func sameJSON(left, right []byte) bool {
	leftValue, leftErr := decodeJSONPreservingNumbers(left)
	rightValue, rightErr := decodeJSONPreservingNumbers(right)
	if leftErr != nil || rightErr != nil {
		return false
	}
	return reflect.DeepEqual(leftValue, rightValue)
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

func envelopeFor(request Request) jobcontract.Envelope {
	organizationID := request.OrganizationID
	domain := jobcontract.DomainLink{Type: domainFor(request.Kind), ID: request.ID}
	var payload any
	switch request.Kind {
	case KindBuild:
		payload = jobcontract.WorkGraphBuildPayload{RequestID: request.ID}
	case KindMaterialize:
		payload = jobcontract.InvestmentMaterializePayload{RequestID: request.ID}
	case KindDispatch:
		payload = jobcontract.InvestmentDispatchPayload{RequestID: request.ID}
	case KindChunk:
		payload = jobcontract.InvestmentChunkPayload{ChunkID: request.ID}
	case KindFinalize:
		payload = jobcontract.InvestmentFinalizePayload{RunID: request.ID}
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
	case KindChunk:
		return "investment_chunk"
	case KindFinalize:
		return "investment_run"
	default:
		return "investment_request"
	}
}
