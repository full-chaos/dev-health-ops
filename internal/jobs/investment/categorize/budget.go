package categorize

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/llmbudget"
)

// WithBudget returns provider guarded by an organization's BYO LLM budget
// (llm/budget.py's attach_llm_budget_guard). An OpenAIProvider is guarded per
// HTTP attempt, so a retry is reserved and settled as its own billable call;
// any other provider is guarded per Complete. Mock and none providers make no
// provider call and are returned unchanged.
func WithBudget(provider Provider, guard llmbudget.Guard) Provider {
	switch typed := provider.(type) {
	case *OpenAIProvider:
		guarded := *typed
		guarded.cfg.Budget = &guard
		return &guarded
	case MockProvider, NoneProvider, nil:
		return provider
	}
	return &budgetedProvider{inner: provider, guard: guard}
}

type budgetedProvider struct {
	inner Provider
	guard llmbudget.Guard
}

var _ Provider = (*budgetedProvider)(nil)

func (p *budgetedProvider) Model() string { return p.inner.Model() }
func (p *budgetedProvider) Close() error  { return p.inner.Close() }

func (p *budgetedProvider) Complete(ctx context.Context, request CompletionRequest) (CompletionResult, error) {
	var result CompletionResult
	err := p.guard.Run(ctx, llmbudget.Call{
		IdempotencyKey:  "shared:" + uuid.NewString(),
		MaxInputTokens:  int64(max(1, len(request.Prompt)+len(request.SystemMessage))),
		MaxOutputTokens: int64(request.MaxOutputTokens),
	}, func(ctx context.Context) (llmbudget.Usage, error) {
		var err error
		result, err = p.inner.Complete(ctx, request)
		if err != nil {
			return llmbudget.Usage{}, err
		}
		return llmbudget.UsageFrom(result.InputTokens, result.OutputTokens, result.CachedInputTokens), nil
	})
	if err != nil {
		return CompletionResult{}, err
	}
	return result, nil
}

// guardedResponses runs one Responses API attempt under the budget guard. The
// reservation covers the request body's byte length as the maximum input and
// its max_output_tokens.
func (p *OpenAIProvider) guardedResponses(ctx context.Context, requestKey string, attempt int, body openAIResponsesRequest) (openAICompletionText, string, error) {
	if p.cfg.Budget == nil {
		return p.executeResponsesRequest(withLLMAttempt(ctx, attempt), body)
	}
	var (
		out    openAICompletionText
		reason string
	)
	err := p.cfg.Budget.Run(ctx, llmbudget.Call{
		IdempotencyKey:  fmt.Sprintf("%s:attempt:%d", requestKey, attempt),
		MaxInputTokens:  int64(max(1, len(body.Input)+len(body.Instructions))),
		MaxOutputTokens: int64(body.MaxOutputTokens),
	}, func(ctx context.Context) (llmbudget.Usage, error) {
		var err error
		out, reason, err = p.executeResponsesRequest(withLLMAttempt(ctx, attempt), body)
		if err != nil {
			return llmbudget.Usage{}, err
		}
		return llmbudget.UsageFrom(out.inputTokens, out.outputTokens, out.cachedInputTokens), nil
	})
	return out, reason, err
}

// budgetRefusal reports whether err is the budget guard's own answer (an
// exhausted budget or an accounting failure) rather than a provider failure.
func budgetRefusal(err error) bool {
	var accounting *llmbudget.AccountingError
	return errors.Is(err, llmbudget.ErrBudgetExceeded) || errors.As(err, &accounting)
}
