// Package investmentexplain -- this file is the ClickHouse WRITE side for
// two tables: investment_explanations (the LLM-explanation cache
// write_investment_explanation writes) and llm_token_usage
// (write_llm_token_usage, called from the same explain_investment_mix
// flow as a separate best-effort persist).
//
// CLIENT NOTE: analytics.QueryClient (dev-health-go's Client) cannot do
// this -- its Query method hard-rejects any non-read-only statement
// (validateReadOnlyStatement in dev-health-go itself), and dev-health-go
// is an external module this repo doesn't control. This file instead
// uses the SAME raw clickhouse-go/v2 PrepareBatch pattern
// internal/jobs/investment/chwrite already established for the
// worker-side writes -- a second, narrow write connection scoped to
// exactly these two tables, not a general Exec capability and not a
// change to analytics.QueryClient.
package investmentexplain

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// writeConn is the narrow ClickHouse capability this file needs, matching
// chwrite's own conn interface shape.
type writeConn interface {
	PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error)
}

// CacheWriter batch-inserts investment_explanations and llm_token_usage
// rows.
type CacheWriter struct {
	conn writeConn
}

// NewCacheWriter builds a CacheWriter over an established write-capable
// ClickHouse connection.
func NewCacheWriter(connection writeConn) (*CacheWriter, error) {
	if connection == nil {
		return nil, fmt.Errorf("investmentexplain: clickhouse write connection is required")
	}
	return &CacheWriter{conn: connection}, nil
}

// WriteInvestmentExplanation ports
// ClickHouseMetricsSink.write_investment_explanation
// (metrics/sinks/clickhouse/investment.py:325-338) -- a single-row insert
// into investment_explanations (migration 018 + 024's org_id column),
// column order: cache_key, explanation_json, llm_provider, llm_model,
// computed_at, org_id.
func (w *CacheWriter) WriteInvestmentExplanation(ctx context.Context, record InvestmentExplanationRecord) error {
	if w == nil || w.conn == nil {
		return ErrUnavailable
	}
	batch, err := w.conn.PrepareBatch(ctx, `INSERT INTO investment_explanations (
		cache_key, explanation_json, llm_provider, llm_model, computed_at, org_id
	)`)
	if err != nil {
		return fmt.Errorf("prepare investment_explanations batch: %w", err)
	}
	if err := batch.Append(
		record.CacheKey, record.ExplanationJSON, record.LLMProvider,
		record.LLMModel, record.ComputedAt.UTC(), record.OrgID,
	); err != nil {
		return fmt.Errorf("append investment_explanations row: %w", err)
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send investment_explanations batch: %w", err)
	}
	return nil
}

// LLMTokenUsageRecord ports metrics/schemas.py's LLMTokenUsageRecord.
type LLMTokenUsageRecord struct {
	OrgID        string
	RunID        string
	Provider     string
	Model        string
	Source       string
	UseCase      string
	InputTokens  int
	OutputTokens int
	Calls        int
	ComputedAt   time.Time
}

// TokenUsageInput bundles _write_investment_mix_token_usage's parameters
// (investment_mix_explain.py:51-59) -- the values explain_investment_mix
// has on hand after a provider.complete call, before any defaulting.
type TokenUsageInput struct {
	OrgID        string
	Provider     string
	Model        *string
	InputTokens  *int
	OutputTokens *int
}

// BuildLLMTokenUsageRecord ports write_llm_token_usage's defaulting and
// early-return logic (llm_token_usage.py:16-55) as a PURE function,
// separated from the actual insert so it can be golden-tested without a
// live ClickHouse: token_count(None) -> 0, provider/model/use_case fall
// back to "unknown"/"unknown"/"legacy" when falsy, calls is clamped to
// >= 0 (write_llm_token_usage's own default is calls=1, and
// _write_investment_mix_token_usage never overrides it, so this port's
// source="investment_mix_explain", use_case="legacy" (the function
// default, unpassed by this call site), run_id="" (ditto) match exactly).
// ok is false when there is nothing worth writing (calls<=0 AND both
// token counts <=0), matching the Python function's own early
// `return True` WITHOUT calling sink.write_llm_token_usage at all -- the
// caller should skip the insert entirely in that case, not write a
// zero-row.
func BuildLLMTokenUsageRecord(input TokenUsageInput, computedAt time.Time) (record LLMTokenUsageRecord, ok bool) {
	inputCount := tokenCount(input.InputTokens)
	outputCount := tokenCount(input.OutputTokens)
	const callCount = 1 // write_llm_token_usage's own default; this call site never overrides it.
	if callCount <= 0 && inputCount <= 0 && outputCount <= 0 {
		return LLMTokenUsageRecord{}, false
	}
	provider := input.Provider
	if provider == "" {
		provider = "unknown"
	}
	model := "unknown"
	if input.Model != nil && *input.Model != "" {
		model = *input.Model
	}
	calls := callCount
	if calls < 0 {
		calls = 0
	}
	return LLMTokenUsageRecord{
		OrgID:        input.OrgID,
		RunID:        "",
		Provider:     provider,
		Model:        model,
		Source:       "investment_mix_explain",
		UseCase:      "legacy",
		InputTokens:  inputCount,
		OutputTokens: outputCount,
		Calls:        calls,
		ComputedAt:   computedAt,
	}, true
}

func tokenCount(value *int) int {
	if value == nil {
		return 0
	}
	return *value
}

// WriteLLMTokenUsage ports
// ClickHouseMetricsSink.write_llm_token_usage
// (metrics/sinks/clickhouse/llm_tokens.py:29-47) -- a single-row insert
// into llm_token_usage (migrations 052/065/072), column order: org_id,
// run_id, provider, model, source, use_case, input_tokens, output_tokens,
// calls, computed_at.
func (w *CacheWriter) WriteLLMTokenUsage(ctx context.Context, record LLMTokenUsageRecord) error {
	if w == nil || w.conn == nil {
		return ErrUnavailable
	}
	batch, err := w.conn.PrepareBatch(ctx, `INSERT INTO llm_token_usage (
		org_id, run_id, provider, model, source, use_case, input_tokens, output_tokens, calls, computed_at
	)`)
	if err != nil {
		return fmt.Errorf("prepare llm_token_usage batch: %w", err)
	}
	if err := batch.Append(
		record.OrgID, record.RunID, record.Provider, record.Model,
		record.Source, record.UseCase, uint64(record.InputTokens),
		uint64(record.OutputTokens), uint64(record.Calls), record.ComputedAt.UTC(),
	); err != nil {
		return fmt.Errorf("append llm_token_usage row: %w", err)
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send llm_token_usage batch: %w", err)
	}
	return nil
}
