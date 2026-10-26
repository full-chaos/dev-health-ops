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

// clefSettleFailure is the error of a call that clef answered (the request was
// sent and the answer is in result) whose accounting could not be settled. The
// reservation stays `reserved` and counts at its maximum; an operator lists it
// (llmbudgetstale) and settles it with llmbudget.VoidReservation.
type clefSettleFailure struct {
	result categorize.SystemOneResult
	err    error
}

func (failure *clefSettleFailure) Error() string { return failure.err.Error() }
func (failure *clefSettleFailure) Unwrap() error { return failure.err }

func (sender clefSender) Model() string { return sender.client.Model() }
func (sender clefSender) Close() error  { return sender.client.Close() }

func (sender clefSender) PostSystemOneDetailed(ctx context.Context, body []byte) (categorize.SystemOneResult, error) {
	if sender.guard == nil {
		return sender.client.PostSystemOneDetailed(ctx, body)
	}
	var result categorize.SystemOneResult
	sent := false
	// A byte is at most one token, so the body length bounds the input the
	// reservation prices; the output is not charged for clef.
	call := llmbudget.Call{IdempotencyKey: "clef:" + uuid.NewString(), MaxInputTokens: int64(len(body))}
	err := sender.guard.Run(ctx, call, func(ctx context.Context) (llmbudget.Usage, error) {
		posted, err := sender.client.PostSystemOneDetailed(ctx, body)
		if err != nil {
			return llmbudget.Usage{}, err
		}
		result, sent = posted, true
		if !posted.Usage.Reported {
			// An answer with no usage settles at the reserved amount (the whole
			// body as input, no output): a success that is priced at the most it
			// could cost. Settling it as unknown would make every later call of the
			// org's month refuse.
			input, output, cached := len(body), 0, 0
			return llmbudget.UsageFrom(&input, &output, &cached, nil), nil
		}
		// clef lists no cache price and reports no cache reads: settling them as
		// zero bills every input token at the full rate, an upper bound; leaving
		// them unknown would refuse the call after the account already answered.
		input, output, cached := int(posted.Usage.InputTokens), int(posted.Usage.OutputTokens), 0
		return llmbudget.UsageFrom(&input, &output, &cached, nil), nil
	})
	if err != nil {
		if sent {
			return categorize.SystemOneResult{}, &clefSettleFailure{result: result, err: err}
		}
		return categorize.SystemOneResult{}, err
	}
	return result, nil
}
