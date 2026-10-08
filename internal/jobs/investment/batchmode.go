package investment

// batchmode.go is provider batch mode (materialize.py's
// _categorize_with_provider_batch): the pending units of a run are sent as one
// provider Batch API job, polled inside the job, and answered per unit.
//
// Every unit result goes through categorizeAccount, as on the synchronous
// path, so the rows of a unit are the rows a synchronous call with the same
// result writes:
//   - a completion text: validated, with the one repair call through the
//     synchronous provider when it is not valid (categorize.CategorizeCompletionText);
//   - a line with no completion, a missing line, a batch that ended failed,
//     expired or cancelled, a batch that did not finish within the timeout:
//     a failed request (the unit gets the llm_task_failed fallback row, as a
//     synchronous transport failure does);
//   - a deterministic failure (bad key, unknown model, quota) at submit or at
//     any later call: the run aborts, as on the synchronous path.
//
// On the timeout, and when the run context ends (lease lost, soft stop), the
// provider batch is cancelled with a short context of its own. There is no
// resume: a later run submits its own batch (Python had none either).

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
)

// Batch mode values: INVESTMENT_LLM_BATCH_MODE / scope llm_batch_mode.
const (
	LLMBatchModeSync     = "sync"
	LLMBatchModeAuto     = "auto"
	LLMBatchModeProvider = "provider_batch"
)

// Python's defaults (materialize.py:883-886). Minimum items and the poll
// interval are constants (no env setting); the scope may still name them.
const (
	defaultLLMBatchMinItems     = 25
	defaultLLMBatchPollInterval = 30 * time.Second
	defaultLLMBatchTimeout      = 3000 * time.Second

	// batchCancelTimeout bounds the cancel (and the read of billed lines) sent
	// after the run context ended or the timeout passed.
	batchCancelTimeout = 15 * time.Second
)

// Outcome words of the batch log line.
const (
	batchOutcomeCompleted    = "completed"
	batchOutcomeSubmitFailed = "submit_failed"
	batchOutcomePollFailed   = "poll_failed"
	batchOutcomeFetchFailed  = "fetch_failed"
	batchOutcomeTimeout      = "timeout"
	batchOutcomeCancelled    = "run_cancelled"
	batchOutcomeEnded        = "ended"
)

// batchSettings are the resolved settings of one run.
func batchSettings(cfg Config) (minItems int, poll, timeout time.Duration) {
	minItems, poll, timeout = cfg.LLMBatchMinItems, cfg.LLMBatchPollInterval, cfg.LLMBatchTimeout
	if minItems < 1 {
		minItems = defaultLLMBatchMinItems
	}
	if poll <= 0 {
		poll = defaultLLMBatchPollInterval
	}
	if timeout <= 0 {
		timeout = defaultLLMBatchTimeout
	}
	return minItems, poll, timeout
}

// batchCustomID is materialize.py's _batch_custom_id(run_id, index).
func batchCustomID(runID string, index int) string {
	return runID + "-" + strconv.Itoa(index)
}

// categorizeBatch answers the pending units from one provider batch and
// reports true, or reports false when the run uses the synchronous path:
// mode sync, a provider with no Batch API (the executor refuses
// provider_batch for one before the run), or auto with fewer pending units
// than the minimum.
func (m *Materializer) categorizeBatch(
	ctx context.Context, cfg Config, pending []preprocessed, limit int, account *categorizeAccount,
) bool {
	if cfg.LLMBatchMode != LLMBatchModeAuto && cfg.LLMBatchMode != LLMBatchModeProvider {
		return false
	}
	batch, supported := categorize.AsBatchProvider(m.provider)
	if !supported || m.served != nil {
		m.logger.InfoContext(ctx, "llm batch mode uses the synchronous path: the provider has no batch api",
			"run_id", cfg.RunID, "mode", cfg.LLMBatchMode, "provider", cfg.ProviderName)
		return false
	}
	minItems, poll, timeout := batchSettings(cfg)
	if cfg.LLMBatchMode == LLMBatchModeAuto && len(pending) < minItems {
		m.logger.InfoContext(ctx, "llm batch mode uses the synchronous path: too few pending units",
			"run_id", cfg.RunID, "pending", len(pending), "threshold", minItems)
		return false
	}

	model := m.provider.Model()
	items := make([]categorize.BatchItem, len(pending))
	for position, entry := range pending {
		items[position] = categorize.BatchItem{
			CustomID: batchCustomID(cfg.RunID, entry.index),
			Request:  categorize.CategorizationRequest(categorize.BuildPrompt(entry.result.Bundle.SourceBlock)),
		}
	}

	run := batchRun{m: m, cfg: cfg, batch: batch, account: account, model: model, started: time.Now(), items: len(items)}
	submission, err := batch.SubmitBatch(ctx, items)
	if err != nil {
		run.failAll(pending, err)
		run.log(ctx, batchOutcomeSubmitFailed, categorize.BatchState{})
		return true
	}
	run.jobID = submission.ProviderJobID
	// Named at once: a worker that dies before the batch ends leaves this
	// line as the only trace of a batch that may still run and bill.
	m.logger.InfoContext(ctx, "investment llm batch submitted",
		"run_id", cfg.RunID, "provider", cfg.ProviderName, "model", model,
		"provider_job_id", run.jobID, "input_file_id", submission.InputFileID, "items", len(items),
		"timeout_seconds", int(timeout/time.Second))

	state, err := run.wait(ctx, poll, timeout)
	switch {
	case ctx.Err() != nil:
		run.cancel(ctx)
		run.failAll(pending, ctx.Err())
		run.log(ctx, batchOutcomeCancelled, state)
		return true
	case errors.Is(err, errBatchTimeout):
		run.cancel(ctx)
		run.failAll(pending, categorize.BatchTimeoutError(cfg.ProviderName, model))
		run.log(ctx, batchOutcomeTimeout, state)
		return true
	case err != nil:
		run.cancel(ctx)
		run.failAll(pending, err)
		run.log(ctx, batchOutcomePollFailed, state)
		return true
	case state.Status != categorize.BatchJobSucceeded:
		run.countBilledLines(ctx)
		run.failAll(pending, categorize.BatchEndedError(state.Status, cfg.ProviderName, model))
		run.log(ctx, batchOutcomeEnded, state)
		return true
	}

	results, err := batch.FetchBatchResults(ctx, run.jobID)
	if err != nil {
		run.failAll(pending, err)
		run.log(ctx, batchOutcomeFetchFailed, state)
		return true
	}
	// A custom id twice: the last line wins, as in Python's dict.
	byID := make(map[string]categorize.BatchItemResult, len(results))
	for _, result := range results {
		if result.CustomID != "" {
			byID[result.CustomID] = result
		}
	}
	opts := categorize.CategorizeOptions{Provider: m.provider, ProviderName: cfg.ProviderName, Model: cfg.Model}
	m.forEachPending(ctx, limit, pending, account, func(ctx context.Context, entry preprocessed) (categorize.CategorizationOutcome, error) {
		result, found := byID[batchCustomID(cfg.RunID, entry.index)]
		switch {
		case !found:
			return categorize.CategorizationOutcome{}, categorize.BatchMissingResultError(cfg.ProviderName, model)
		case !result.Succeeded():
			return categorize.CategorizationOutcome{}, lineFailure(result, cfg.ProviderName, model)
		}
		return categorize.CategorizeCompletionText(ctx, entry.result.Bundle, result.Text, opts,
			result.InputTokens, result.OutputTokens, model)
	})
	run.log(ctx, batchOutcomeCompleted, state)
	return true
}

// lineFailure is the error of a line with no completion; a line that carries
// usage was billed, and its tokens are counted.
func lineFailure(result categorize.BatchItemResult, provider, model string) error {
	err := categorize.BatchItemFailure(result, provider, model)
	if result.InputTokens == nil && result.OutputTokens == nil {
		return err
	}
	return &billedFailure{err: err, inputTokens: intValue(result.InputTokens), outputTokens: intValue(result.OutputTokens)}
}

func intValue(value *int) int {
	if value == nil {
		return 0
	}
	return *value
}

var errBatchTimeout = errors.New("investment: provider batch did not finish within the timeout")

type batchRun struct {
	m       *Materializer
	cfg     Config
	batch   categorize.BatchProvider
	account *categorizeAccount
	model   string
	started time.Time
	items   int
	jobID   string
}

// wait polls until the batch is terminal, the timeout passes, a poll fails or
// ctx ends. The first poll is one interval after the submit.
func (run *batchRun) wait(ctx context.Context, poll, timeout time.Duration) (categorize.BatchState, error) {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	var state categorize.BatchState
	for {
		select {
		case <-ctx.Done():
			return state, ctx.Err()
		case <-deadline.C:
			return state, errBatchTimeout
		case <-ticker.C:
		}
		polled, err := run.batch.PollBatch(ctx, run.jobID)
		if err != nil {
			return state, err
		}
		state = polled
		if state.Status.Terminal() {
			return state, nil
		}
	}
}

// failAll records err as the result of every unit. A deterministic failure
// is one failed call, recorded once: it aborts the run, as the first such
// failure of a synchronous call does.
func (run *batchRun) failAll(pending []preprocessed, err error) {
	if _, deterministic := llmFailureOf(err); deterministic && len(pending) > 0 {
		run.account.record(pending[0].index, categorize.CategorizationOutcome{}, err)
		return
	}
	for _, entry := range pending {
		run.account.record(entry.index, categorize.CategorizationOutcome{}, err)
	}
}

// cancel cancels the provider batch with a context of its own (the run's may
// have ended), then counts the lines the provider already answered: a
// cancelled batch still bills them.
func (run *batchRun) cancel(ctx context.Context) {
	cancelCtx, release := run.detached(ctx)
	defer release()
	if err := run.batch.CancelBatch(cancelCtx, run.jobID); err != nil {
		run.m.logger.WarnContext(ctx, "llm batch cancel failed; the provider batch may still run and bill",
			"run_id", run.cfg.RunID, "provider_job_id", run.jobID, "error", err.Error())
	}
	run.countBilledLines(ctx)
}

// countBilledLines adds the usage of every line the batch has answered so far
// to the run's token count. The lines' answers are not used: the units of a
// batch that did not succeed fail as a whole, as in Python.
func (run *batchRun) countBilledLines(ctx context.Context) {
	readCtx, release := run.detached(ctx)
	defer release()
	results, err := run.batch.FetchBatchResults(readCtx, run.jobID)
	if err != nil {
		run.m.logger.WarnContext(ctx, "llm batch billed lines could not be read; their tokens are not counted",
			"run_id", run.cfg.RunID, "provider_job_id", run.jobID, "error", err.Error())
		return
	}
	calls, inputTokens, outputTokens := 0, 0, 0
	for _, result := range results {
		if result.InputTokens == nil && result.OutputTokens == nil {
			continue
		}
		calls++
		inputTokens += intValue(result.InputTokens)
		outputTokens += intValue(result.OutputTokens)
	}
	run.account.addBilledTokens(calls, inputTokens, outputTokens)
}

func (run *batchRun) detached(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), batchCancelTimeout)
}

// log writes the one log line of a batch: WARN for every outcome but
// completed, so a timeout, a cancelled run or a failed batch is loud.
func (run *batchRun) log(ctx context.Context, outcome string, state categorize.BatchState) {
	level := slog.LevelWarn
	if outcome == batchOutcomeCompleted {
		level = slog.LevelInfo
	}
	run.m.logger.Log(ctx, level, "investment llm batch complete",
		"run_id", run.cfg.RunID,
		"provider", run.cfg.ProviderName,
		"model", run.model,
		"provider_job_id", run.jobID,
		"outcome", outcome,
		"provider_status", state.ProviderStatus,
		"items", run.items,
		"provider_completed", state.CompletedCount,
		"provider_failed", state.FailedCount,
		"duration_ms", time.Since(run.started).Milliseconds(),
	)
}
