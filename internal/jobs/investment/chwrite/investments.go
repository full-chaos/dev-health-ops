// Package chwrite is the ClickHouse write side of investment.materialize
// (CHAOS-4441, plan.md item K). It ports write_work_unit_investments,
// write_work_unit_repo_effort and write_work_unit_investment_quotes
// (src/dev_health_ops/metrics/sinks/clickhouse/investment.py:117-188) --
// same three tables, same column order per table. computed_at is the
// ReplacingMergeTree version column on all three
// (017_investment_materialize_tables.sql, 064_work_unit_repo_effort.sql).
//
// It also writes work_unit_supersessions (plan.md section 5a, option A) --
// this port's own addition, with no Python counterpart -- the dedup
// obligation's explicit lineage record. See
// 085_work_unit_supersessions.sql for the full rationale.
package chwrite

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// ErrInvalidState is returned when a write is attempted without a usable
// connection or organization scope.
var ErrInvalidState = errors.New("chwrite: invalid write state")

// conn is the narrow ClickHouse capability this package needs, matching
// internal/jobs/metrics/daily's conn interfaces.
type conn interface {
	PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error)
}

// Writer batch-inserts the three investment.materialize output tables.
type Writer struct {
	conn conn
}

// NewWriter builds a Writer over an established ClickHouse connection.
func NewWriter(connection conn) (*Writer, error) {
	if connection == nil {
		return nil, fmt.Errorf("chwrite: clickhouse connection is required")
	}
	return &Writer{conn: connection}, nil
}

// InvestmentRecord is one work_unit_investments row -- the Go counterpart of
// WorkUnitInvestmentRecord written by write_work_unit_investments
// (sinks/clickhouse/investment.py:117-148). theme_distribution_json and
// subcategory_distribution_json are ClickHouse Map(String, Float64) columns,
// NOT serialized JSON text despite the column name (plan.md section 3) -- a
// Go writer that sends JSON text here would silently write garbage.
type InvestmentRecord struct {
	WorkUnitID   string
	WorkUnitType *string
	WorkUnitName *string
	FromTS       time.Time
	ToTS         time.Time
	RepoID       *uuid.UUID
	// RepoSource (CHAOS-5359, migration 089) records how RepoID was decided:
	// "own_edges" (collectSingleRepoID's pre-existing resolution),
	// "ancestor:<issue_id>" or "children" (the issue-hierarchy cascade,
	// investment.computeRepoHierarchyCascade), or nil when RepoID is nil.
	RepoSource                 *string
	Provider                   *string
	EffortMetric               string
	EffortValue                float64
	ThemeDistribution          map[string]float64
	SubcategoryDistribution    map[string]float64
	StructuralEvidenceJSON     string
	EvidenceQuality            float64
	EvidenceQualityBand        string
	CategorizationStatus       string
	CategorizationErrorsJSON   string
	CategorizationModelVersion string
	CategorizationInputHash    string
	CategorizationRunID        string
	ComputedAt                 time.Time
}

// RepoEffortRecord is one work_unit_repo_effort row -- the Go counterpart of
// WorkUnitRepoEffortRecord written by write_work_unit_repo_effort
// (sinks/clickhouse/investment.py:150-169).
type RepoEffortRecord struct {
	WorkUnitID string
	RepoID     *uuid.UUID
	// RepoSource mirrors InvestmentRecord.RepoSource -- see its doc comment.
	RepoSource          *string
	EffortMetric        string
	EffortValue         float64
	AllocationWeight    float64
	AllocationSource    string
	CategorizationRunID string
	ComputedAt          time.Time
}

// QuoteRecord is one work_unit_investment_quotes row -- the Go counterpart of
// WorkUnitInvestmentEvidenceQuoteRecord written by
// write_work_unit_investment_quotes (sinks/clickhouse/investment.py:171-188).
type QuoteRecord struct {
	WorkUnitID          string
	Quote               string
	SourceType          string
	SourceID            string
	ComputedAt          time.Time
	CategorizationRunID string
}

// WriteInvestments inserts work_unit_investments rows, stamping every row
// with orgID (CHAOS-4341's org-scoping discipline, applied here from the
// start rather than retrofitted after an incident, same as
// internal/jobs/metrics/daily/cicd.Writer.WriteResult).
func (w *Writer) WriteInvestments(ctx context.Context, orgID string, records []InvestmentRecord) (int, error) {
	if w == nil || w.conn == nil {
		return 0, fmt.Errorf("chwrite: writer unavailable")
	}
	if strings.TrimSpace(orgID) == "" {
		return 0, fmt.Errorf("%w: organization id is required to write work_unit_investments", ErrInvalidState)
	}
	if len(records) == 0 {
		return 0, nil
	}
	batch, err := w.conn.PrepareBatch(ctx, `INSERT INTO work_unit_investments (
		work_unit_id, work_unit_type, work_unit_name, from_ts, to_ts, repo_id,
		repo_source, provider, effort_metric, effort_value, theme_distribution_json,
		subcategory_distribution_json, structural_evidence_json, evidence_quality,
		evidence_quality_band, categorization_status, categorization_errors_json,
		categorization_model_version, categorization_input_hash,
		categorization_run_id, computed_at, org_id
	)`)
	if err != nil {
		return 0, fmt.Errorf("prepare work_unit_investments batch: %w", err)
	}
	for _, record := range records {
		if err := batch.Append(
			record.WorkUnitID, record.WorkUnitType, record.WorkUnitName,
			record.FromTS.UTC(), record.ToTS.UTC(), record.RepoID, record.RepoSource,
			record.Provider,
			record.EffortMetric, record.EffortValue, record.ThemeDistribution,
			record.SubcategoryDistribution, record.StructuralEvidenceJSON,
			record.EvidenceQuality, record.EvidenceQualityBand,
			record.CategorizationStatus, record.CategorizationErrorsJSON,
			record.CategorizationModelVersion, record.CategorizationInputHash,
			record.CategorizationRunID, record.ComputedAt.UTC(), orgID,
		); err != nil {
			return 0, fmt.Errorf("append work_unit_investments row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("send work_unit_investments batch: %w", err)
	}
	return len(records), nil
}

// WriteRepoEffort inserts work_unit_repo_effort rows, stamping every row
// with orgID.
func (w *Writer) WriteRepoEffort(ctx context.Context, orgID string, records []RepoEffortRecord) (int, error) {
	if w == nil || w.conn == nil {
		return 0, fmt.Errorf("chwrite: writer unavailable")
	}
	if strings.TrimSpace(orgID) == "" {
		return 0, fmt.Errorf("%w: organization id is required to write work_unit_repo_effort", ErrInvalidState)
	}
	if len(records) == 0 {
		return 0, nil
	}
	batch, err := w.conn.PrepareBatch(ctx, `INSERT INTO work_unit_repo_effort (
		work_unit_id, repo_id, repo_source, effort_metric, effort_value, allocation_weight,
		allocation_source, categorization_run_id, computed_at, org_id
	)`)
	if err != nil {
		return 0, fmt.Errorf("prepare work_unit_repo_effort batch: %w", err)
	}
	for _, record := range records {
		if err := batch.Append(
			record.WorkUnitID, record.RepoID, record.RepoSource, record.EffortMetric, record.EffortValue,
			record.AllocationWeight, record.AllocationSource, record.CategorizationRunID,
			record.ComputedAt.UTC(), orgID,
		); err != nil {
			return 0, fmt.Errorf("append work_unit_repo_effort row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("send work_unit_repo_effort batch: %w", err)
	}
	return len(records), nil
}

// SupersessionRecord is one work_unit_supersessions row (CHAOS-4441
// plan.md section 5a, option A) -- an explicit, additive record that a
// work_unit_id was retired by a later run that regrouped its nodes under a
// different id. Written in the SAME run that mints the replacement id, by
// whichever plane mints it (the materializer, for the investment tables).
// See 085_work_unit_supersessions.sql for the full rationale and the
// binding condition every reader owes: honour this table INDEPENDENTLY of
// investment_membership_scope.py's scope_enabled gate.
type SupersessionRecord struct {
	SupersededWorkUnitID string
	SupersededByRunID    string
	SupersededAt         time.Time
}

// WriteSupersessions inserts work_unit_supersessions rows, stamping every
// row with orgID.
func (w *Writer) WriteSupersessions(ctx context.Context, orgID string, records []SupersessionRecord) (int, error) {
	if w == nil || w.conn == nil {
		return 0, fmt.Errorf("chwrite: writer unavailable")
	}
	if strings.TrimSpace(orgID) == "" {
		return 0, fmt.Errorf("%w: organization id is required to write work_unit_supersessions", ErrInvalidState)
	}
	if len(records) == 0 {
		return 0, nil
	}
	batch, err := w.conn.PrepareBatch(ctx, `INSERT INTO work_unit_supersessions (
		org_id, superseded_work_unit_id, superseded_by_run_id, superseded_at
	)`)
	if err != nil {
		return 0, fmt.Errorf("prepare work_unit_supersessions batch: %w", err)
	}
	for _, record := range records {
		if err := batch.Append(
			orgID, record.SupersededWorkUnitID, record.SupersededByRunID, record.SupersededAt.UTC(),
		); err != nil {
			return 0, fmt.Errorf("append work_unit_supersessions row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("send work_unit_supersessions batch: %w", err)
	}
	return len(records), nil
}

// WriteQuotes inserts work_unit_investment_quotes rows, stamping every row
// with orgID.
func (w *Writer) WriteQuotes(ctx context.Context, orgID string, records []QuoteRecord) (int, error) {
	if w == nil || w.conn == nil {
		return 0, fmt.Errorf("chwrite: writer unavailable")
	}
	if strings.TrimSpace(orgID) == "" {
		return 0, fmt.Errorf("%w: organization id is required to write work_unit_investment_quotes", ErrInvalidState)
	}
	if len(records) == 0 {
		return 0, nil
	}
	batch, err := w.conn.PrepareBatch(ctx, `INSERT INTO work_unit_investment_quotes (
		work_unit_id, quote, source_type, source_id, computed_at,
		categorization_run_id, org_id
	)`)
	if err != nil {
		return 0, fmt.Errorf("prepare work_unit_investment_quotes batch: %w", err)
	}
	for _, record := range records {
		if err := batch.Append(
			record.WorkUnitID, record.Quote, record.SourceType, record.SourceID,
			record.ComputedAt.UTC(), record.CategorizationRunID, orgID,
		); err != nil {
			return 0, fmt.Errorf("append work_unit_investment_quotes row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("send work_unit_investment_quotes batch: %w", err)
	}
	return len(records), nil
}

// TokenUsageRecord is one llm_token_usage row -- the Go counterpart of
// metrics/llm_token_usage.py's LLMTokenUsageRecord. Column order below matches
// migrations 052/065/072, the same order cmd/query-api's CacheWriter uses.
type TokenUsageRecord struct {
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

// TokenUsageSourceInvestmentMaterialize is the `source` label
// materialize.py:1509 writes. It is a stored discriminator other consumers
// group by, so it is a constant rather than a literal at the call site.
const TokenUsageSourceInvestmentMaterialize = "investment_materialize"

// tokenUsageUseCaseLegacy is write_llm_token_usage's `use_case` default
// (llm_token_usage.py:23). materialize.py never overrides it, so every row this
// writer produces carries it.
const tokenUsageUseCaseLegacy = "legacy"

// WriteTokenUsage ports metrics/llm_token_usage.py:16-55 write_llm_token_usage.
//
// # THE ALL-ZERO SKIP IS BEHAVIOUR, NOT AN OPTIMISATION
//
// The reference returns early without writing when calls, input and output are
// all non-positive (:33-34). A run that made no LLM calls at all -- every unit
// skipped as unchanged, or every one falling back on insufficient evidence --
// therefore writes NO row rather than a zero row. Dropping this would put a
// zero-valued row into a table whose consumers SUM it, changing call-count
// averages for every reader.
//
// # WHAT IS DELIBERATELY NOT PORTED
//
// Python swallows every write failure and returns False (:53-54). This does
// NOT: a token-usage write failing means the ClickHouse connection is in
// trouble, and the caller is mid-materialization with three more writes to do.
// Reporting it lets the executor decide; the reference's bool return has
// exactly one caller and that caller ignores it.
func (w *Writer) WriteTokenUsage(ctx context.Context, orgID string, record TokenUsageRecord) (int, error) {
	if w == nil || w.conn == nil {
		return 0, ErrInvalidState
	}
	inputTokens := maxInt(0, record.InputTokens)
	outputTokens := maxInt(0, record.OutputTokens)
	calls := record.Calls
	if calls <= 0 && inputTokens <= 0 && outputTokens <= 0 {
		return 0, nil
	}

	// `provider or "unknown"` / `model or "unknown"` (:41-42). The defaults are
	// the reference's and are load-bearing for grouping: an empty provider would
	// silently form its own bucket alongside the named ones.
	provider := record.Provider
	if provider == "" {
		provider = "unknown"
	}
	model := record.Model
	if model == "" {
		model = "unknown"
	}
	useCase := record.UseCase
	if useCase == "" {
		useCase = tokenUsageUseCaseLegacy
	}

	batch, err := w.conn.PrepareBatch(ctx, `INSERT INTO llm_token_usage (
		org_id, run_id, provider, model, source, use_case, input_tokens, output_tokens, calls, computed_at
	)`)
	if err != nil {
		return 0, fmt.Errorf("prepare llm_token_usage batch: %w", err)
	}
	if err := batch.Append(
		orgID, record.RunID, provider, model, record.Source, useCase,
		uint64(inputTokens), uint64(outputTokens), uint64(maxInt(0, calls)),
		record.ComputedAt.UTC(),
	); err != nil {
		return 0, fmt.Errorf("append llm_token_usage row: %w", err)
	}
	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("send llm_token_usage batch: %w", err)
	}
	return 1, nil
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
