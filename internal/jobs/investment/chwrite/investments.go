// Package chwrite is the ClickHouse write side of investment.materialize
// (CHAOS-4441, plan.md item K). It ports write_work_unit_investments,
// write_work_unit_repo_effort and write_work_unit_investment_quotes
// (src/dev_health_ops/metrics/sinks/clickhouse/investment.py:117-188) --
// same three tables, same column order per table. computed_at is the
// ReplacingMergeTree version column on all three
// (017_investment_materialize_tables.sql, 064_work_unit_repo_effort.sql).
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
	WorkUnitID                 string
	WorkUnitType               *string
	WorkUnitName               *string
	FromTS                     time.Time
	ToTS                       time.Time
	RepoID                     *uuid.UUID
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
	WorkUnitID          string
	RepoID              *uuid.UUID
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
		provider, effort_metric, effort_value, theme_distribution_json,
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
			record.FromTS.UTC(), record.ToTS.UTC(), record.RepoID, record.Provider,
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
		work_unit_id, repo_id, effort_metric, effort_value, allocation_weight,
		allocation_source, categorization_run_id, computed_at, org_id
	)`)
	if err != nil {
		return 0, fmt.Errorf("prepare work_unit_repo_effort batch: %w", err)
	}
	for _, record := range records {
		if err := batch.Append(
			record.WorkUnitID, record.RepoID, record.EffortMetric, record.EffortValue,
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
