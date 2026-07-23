package workgraph

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/jackc/pgx/v5"
)

// RequestWriter is safe to use inside a caller-owned transaction. In
// particular, native post-sync fanout can create the immutable request and its
// outbox handoff atomically, so a crash cannot produce only one half.
type RequestWriter struct{ producer *joboutbox.Producer }

func NewRequestWriter(registry joboutbox.PolicyRegistry) (*RequestWriter, error) {
	producer, err := joboutbox.NewTransactionProducer(registry)
	if err != nil {
		return nil, ErrUnavailable
	}
	return &RequestWriter{producer: producer}, nil
}

func (writer *RequestWriter) WriteTx(ctx context.Context, tx pgx.Tx, request Request) error {
	if writer == nil || writer.producer == nil || tx == nil || !validRequest(request) {
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
		err = tx.QueryRow(ctx, `
SELECT id::text, org_id::text, kind, scope::text, COALESCE(model_ref, ''),
       COALESCE(prompt_ref, ''), llm_concurrency, spend_limit_microunits,
       correlation_id, idempotency_key
FROM public.work_graph_execution_requests WHERE id = $1::uuid`, request.ID).Scan(
			&existing.ID, &existing.OrganizationID, &existing.Kind, &existingScope,
			&existing.ModelRef, &existing.PromptRef, &existing.LLMConcurrency,
			&existing.SpendLimitMicrounits, &existing.CorrelationID, &existing.IdempotencyKey,
		)
		if err != nil || !sameRequest(existing, request, existingScope) {
			return ErrInvalidState
		}
	}
	return writer.producer.PublishDeferred(ctx, tx, string(request.Kind), envelopeFor(request))
}

func validRequest(request Request) bool {
	return request.Kind.Valid() && validUUID(request.ID) && validUUID(request.OrganizationID) &&
		json.Valid(request.Scope) && len(request.Scope) > 1 && len(request.Scope) <= 8192 &&
		request.LLMConcurrency >= 1 && request.LLMConcurrency <= 16 &&
		request.SpendLimitMicrounits >= 0 && len(request.ModelRef) <= 128 &&
		len(request.PromptRef) <= 128 && len(request.CorrelationID) > 0 &&
		len(request.CorrelationID) <= 128 && len(request.IdempotencyKey) > 0 &&
		len(request.IdempotencyKey) <= 256
}

func sameRequest(existing, expected Request, scope []byte) bool {
	return existing.ID == expected.ID && existing.OrganizationID == expected.OrganizationID &&
		existing.Kind == expected.Kind && string(scope) == string(expected.Scope) &&
		existing.ModelRef == expected.ModelRef && existing.PromptRef == expected.PromptRef &&
		existing.LLMConcurrency == expected.LLMConcurrency &&
		existing.SpendLimitMicrounits == expected.SpendLimitMicrounits &&
		existing.CorrelationID == expected.CorrelationID && existing.IdempotencyKey == expected.IdempotencyKey
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
