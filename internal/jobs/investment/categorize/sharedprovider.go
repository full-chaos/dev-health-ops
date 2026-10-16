package categorize

import (
	"context"
	"net/http"

	"github.com/full-chaos/dev-health-go/llm"

	"github.com/full-chaos/dev-health-ops/internal/llmbudget"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// SharedProviderConfig builds a SharedProvider. The credential is resolved by
// the caller (platform env or an org's BYO settings) and passed in here: the
// shared package reads no environment and keeps no credential store.
type SharedProviderConfig struct {
	Kind   ProviderKind
	APIKey secrets.Hidden `json:"-"`
	// BaseURL is the API root; "" is the provider's public endpoint.
	BaseURL string
	// Model is the model every request carries. "" is llm.DefaultModel, which
	// no production path reaches: every builder passes the resolved model.
	Model           string
	MaxOutputTokens int
	// HTTPClient nil is the package's hardened client. An org-supplied
	// BaseURL needs the SSRF-guarded client (orgHTTPClient).
	HTTPClient *http.Client
	Budget     *llmbudget.Guard `json:"-"`
}

// Batch file name and metadata the investment materializer has always sent.
const (
	sharedBatchFileName = "investment-categorization-batch.jsonl"
	sharedBatchSource   = "investment_materialize"
)

// SharedProvider is a Provider served by the dev-health-go llm package, the
// one provider implementation ops and acr share.
type SharedProvider struct {
	cfg   SharedProviderConfig
	inner llm.Provider
}

var _ Provider = (*SharedProvider)(nil)

// NewSharedProvider builds the llm client for cfg.Kind.
func NewSharedProvider(cfg SharedProviderConfig) (*SharedProvider, error) {
	llmConfig := llm.Config{
		Kind:            llm.Kind(cfg.Kind),
		APIKey:          llm.NewSecret(cfg.APIKey.Reveal()),
		BaseURL:         cfg.BaseURL,
		Model:           cfg.Model,
		MaxOutputTokens: cfg.MaxOutputTokens,
		HTTPClient:      cfg.HTTPClient,
	}
	if cfg.Budget != nil {
		llmConfig.Budget = sharedBudget{guard: *cfg.Budget}
	}
	inner, err := llm.New(llmConfig)
	if err != nil {
		return nil, err
	}
	return &SharedProvider{cfg: cfg, inner: inner}, nil
}

// withBudget is this provider guarded per HTTP attempt by guard.
func (p *SharedProvider) withBudget(guard llmbudget.Guard) (*SharedProvider, error) {
	cfg := p.cfg
	cfg.Budget = &guard
	return NewSharedProvider(cfg)
}

func (p *SharedProvider) Model() string { return p.inner.Model() }

func (p *SharedProvider) Close() error { return p.inner.Close() }

func (p *SharedProvider) Complete(ctx context.Context, request CompletionRequest) (CompletionResult, error) {
	result, err := p.inner.Complete(ctx, sharedRequest(request))
	if err != nil {
		return CompletionResult{}, err
	}
	return CompletionResult{
		Text:              result.Text,
		InputTokens:       result.Usage.InputTokens,
		OutputTokens:      result.Usage.OutputTokens,
		Model:             result.Model,
		CachedInputTokens: result.Usage.CachedInputTokens,
	}, nil
}

func sharedRequest(request CompletionRequest) llm.Request {
	out := llm.Request{System: request.SystemMessage, Prompt: request.Prompt, MaxOutputTokens: request.MaxOutputTokens}
	if request.JSONSchema != nil {
		out.Format = &llm.JSONFormat{Name: request.ResponseFormatName, Schema: request.JSONSchema}
	}
	return out
}

// sharedBatchProvider is the Batch API of a SharedProvider whose llm client
// has one (AsBatchProvider).
type sharedBatchProvider struct {
	*SharedProvider
	batch llm.BatchProvider
}

var _ BatchProvider = sharedBatchProvider{}

func (p sharedBatchProvider) SubmitBatch(ctx context.Context, items []BatchItem) (BatchSubmission, error) {
	if err := validateBatchItems(items); err != nil {
		return BatchSubmission{}, err
	}
	request := llm.BatchRequest{
		Items:    make([]llm.BatchItem, len(items)),
		FileName: sharedBatchFileName,
		Metadata: map[string]string{"source": sharedBatchSource},
	}
	for index, item := range items {
		request.Items[index] = llm.BatchItem{CustomID: item.CustomID, Request: sharedRequest(item.Request)}
	}
	submission, err := p.batch.SubmitBatch(ctx, request)
	if err != nil {
		return BatchSubmission{}, err
	}
	return BatchSubmission{ProviderJobID: submission.ProviderJobID, InputFileID: submission.InputFileID, ItemCount: submission.ItemCount}, nil
}

func (p sharedBatchProvider) PollBatch(ctx context.Context, providerJobID string) (BatchState, error) {
	state, err := p.batch.PollBatch(ctx, providerJobID)
	if err != nil {
		return BatchState{}, err
	}
	return BatchState{
		ProviderJobID:  state.ProviderJobID,
		Status:         BatchJobStatus(state.Status),
		ProviderStatus: state.ProviderStatus,
		TotalCount:     state.TotalCount,
		CompletedCount: state.CompletedCount,
		FailedCount:    state.FailedCount,
		OutputFileID:   state.OutputFileID,
		ErrorFileID:    state.ErrorFileID,
	}, nil
}

func (p sharedBatchProvider) FetchBatchResults(ctx context.Context, providerJobID string) ([]BatchItemResult, error) {
	results, err := p.batch.FetchBatchResults(ctx, providerJobID)
	if err != nil || len(results) == 0 {
		return nil, err
	}
	out := make([]BatchItemResult, len(results))
	for index, result := range results {
		out[index] = fromLLMBatchResult(result)
	}
	return out, nil
}

func (p sharedBatchProvider) CancelBatch(ctx context.Context, providerJobID string) error {
	return p.batch.CancelBatch(ctx, providerJobID)
}

func fromLLMBatchResult(result llm.BatchItemResult) BatchItemResult {
	return BatchItemResult{
		CustomID:          result.CustomID,
		Text:              result.Text,
		ErrorCode:         result.ErrorCode,
		ErrorMessage:      result.ErrorMessage,
		LineID:            result.LineID,
		RequestID:         result.RequestID,
		StatusCode:        result.StatusCode,
		InputTokens:       result.Usage.InputTokens,
		OutputTokens:      result.Usage.OutputTokens,
		CachedInputTokens: result.Usage.CachedInputTokens,
		ProviderErrorCode: result.ProviderErrorCode,
		ProviderErrorType: result.ProviderErrorType,
	}
}

func toLLMBatchResult(result BatchItemResult) llm.BatchItemResult {
	return llm.BatchItemResult{
		CustomID:          result.CustomID,
		Text:              result.Text,
		ErrorCode:         result.ErrorCode,
		ErrorMessage:      result.ErrorMessage,
		LineID:            result.LineID,
		RequestID:         result.RequestID,
		StatusCode:        result.StatusCode,
		Usage:             llm.Usage{InputTokens: result.InputTokens, OutputTokens: result.OutputTokens, CachedInputTokens: result.CachedInputTokens},
		ProviderErrorCode: result.ProviderErrorCode,
		ProviderErrorType: result.ProviderErrorType,
	}
}

// sharedBudget is the llm.Budget over an organization's BYO budget guard:
// every HTTP attempt is reserved and settled as its own billable call, and a
// batch is admitted only for an organization with no budget configured (a
// batch cannot be priced per attempt).
type sharedBudget struct{ guard llmbudget.Guard }

var (
	_ llm.Budget      = sharedBudget{}
	_ llm.BatchBudget = sharedBudget{}
)

func (b sharedBudget) Run(ctx context.Context, attempt llm.Attempt, invoke func(context.Context) (llm.Usage, error)) error {
	return b.guard.Run(ctx, llmbudget.Call{
		IdempotencyKey:  attempt.IdempotencyKey,
		MaxInputTokens:  attempt.MaxInputBytes,
		MaxOutputTokens: attempt.MaxOutputTokens,
	}, func(ctx context.Context) (llmbudget.Usage, error) {
		usage, err := invoke(ctx)
		if err != nil {
			return llmbudget.Usage{}, err
		}
		return llmbudget.UsageFrom(usage.InputTokens, usage.OutputTokens, usage.CachedInputTokens), nil
	})
}

func (b sharedBudget) AdmitBatch(ctx context.Context, _ llm.Kind, _, _ string) error {
	return b.guard.RefuseUnderBudget(ctx)
}
