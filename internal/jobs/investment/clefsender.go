package investment

import (
	"context"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/llmbudget"
)

// clefSender is the systemOneSender of an org's clef account: the SystemOne
// request body goes to the account as it is (no prompt translation: clef speaks
// SystemOne), under the org's budget guard when it has one.
type clefSender struct {
	client *categorize.TypeSafeClient
	guard  *llmbudget.Guard
}

var _ systemOneSender = clefSender{}

func (sender clefSender) Model() string { return sender.client.Model() }
func (sender clefSender) Close() error  { return sender.client.Close() }

func (sender clefSender) PostSystemOneDetailed(ctx context.Context, body []byte) (categorize.SystemOneResult, error) {
	if sender.guard == nil {
		return sender.client.PostSystemOneDetailed(ctx, body)
	}
	var result categorize.SystemOneResult
	// A byte is at most one token, so the body length bounds the input the
	// reservation prices; the output is not charged for clef.
	call := llmbudget.Call{IdempotencyKey: "clef:" + uuid.NewString(), MaxInputTokens: int64(len(body))}
	err := sender.guard.Run(ctx, call, func(ctx context.Context) (llmbudget.Usage, error) {
		posted, err := sender.client.PostSystemOneDetailed(ctx, body)
		if err != nil {
			return llmbudget.Usage{}, err
		}
		result = posted
		if !posted.Usage.Reported {
			return llmbudget.Usage{}, nil
		}
		// clef lists no cache price and reports no cache reads: settling them as
		// zero bills every input token at the full rate, an upper bound; leaving
		// them unknown would refuse the call after the account already answered.
		input, output, cached := int(posted.Usage.InputTokens), int(posted.Usage.OutputTokens), 0
		return llmbudget.UsageFrom(&input, &output, &cached, nil), nil
	})
	if err != nil {
		return categorize.SystemOneResult{}, err
	}
	return result, nil
}
