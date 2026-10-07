package chwrite

// The write side of the investment shadow trial (CHAOS-8868, parent
// CHAOS-8865): two append-only ClickHouse tables, created by migrations 109
// and 110.
//
//   - work_unit_investment_shadow: one row for each shadow classification.
//     Written with WriteShadowInvestments.
//   - llm_categorization_attempts: one row for each HTTP attempt. Rows are
//     collected in an AttemptBuffer during a run and written once, as one
//     batch, with FlushAttempts.
//
// Nothing in this file is read by a served path, and nothing here writes
// work_unit_investments, llm_token_usage or the execution ledger. No caller
// uses these writers yet: the shadow phase that does is a later change.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

// ErrShadowTableMissing marks a write that failed because the table does not
// exist, which means migration 109 or 110 is not applied. The shadow phase
// treats it as a reason to stop the phase, never as a run failure.
var ErrShadowTableMissing = errors.New("chwrite: shadow table is missing (migration 109 or 110 not applied)")

// clickHouseUnknownTable is ClickHouse server error code 60, UNKNOWN_TABLE.
const clickHouseUnknownTable = 60

// MaxAttemptRows is the row cap of an AttemptBuffer. A run can hold 250,000
// components, so an uncapped buffer could hold an unbounded number of rows.
const MaxAttemptRows = 50_000

// AttemptWriteTimeout bounds the one batched attempt insert of a run. It is the
// same bound that flushTokenUsage puts on its write.
const AttemptWriteTimeout = 15 * time.Second

// Retention of the two tables, the same as the TTL of migrations 109 and 110. A
// row whose ComputedAt is older than this is deleted at the next merge, so the
// sinks refuse it. A zero time and the Unix epoch are both far older, so this
// one check covers them (time.Unix(0, 0) is not IsZero, but it is 1970).
const (
	ShadowRetention  = 90 * 24 * time.Hour
	AttemptRetention = 400 * 24 * time.Hour
)

// computedAtError names the reason a ComputedAt is not writable, or returns nil.
// The sinks never stamp a time: ComputedAt is the version of a ReplacingMergeTree
// row, so the caller of a run chooses it, and a stamp here would change it on a
// retry.
func computedAtError(at time.Time, retention time.Duration) error {
	if at.Before(time.Now().Add(-retention)) {
		return fmt.Errorf("ComputedAt %s is older than the table retention of %d days: the TTL would delete the row at the next merge", at.UTC().Format(time.RFC3339), int(retention/(24*time.Hour)))
	}
	return nil
}

// Roles of an attempt row. Only RoleShadow is written in the shadow build.
const (
	RoleServed   = "served"
	RoleShadow   = "shadow"
	RoleFallback = "fallback"
)

// ShadowRecord is one work_unit_investment_shadow row.
//
// There is no ThemeDistribution field on purpose: the writer computes the theme
// map from SubcategoryDistribution with units.RollupSubcategoriesToThemes, so a
// row cannot carry a theme map that a backend made. A failure state has an
// empty SubcategoryDistribution and then an empty theme map: a shadow row must
// not look like a mix.
//
// There is no text field for a prompt, a response or a quote either: the
// evidence is a span id, a handle and a source id. Warnings, ErrorCodes,
// EvidenceHandle and ModelReturned are free String columns, so the caller must
// put closed codes and ids there, never text taken from a response.
type ShadowRecord struct {
	WorkUnitID string
	// InputHash is categorization_input_hash of the bundle that was asked.
	InputHash string
	// ShadowConfig is the identity stamp of the candidate configuration.
	ShadowConfig  string
	RubricSHA256  string
	ModelReturned string
	// State is the shadow state (ok, zero_support, evidence_none, ...).
	State string
	// CategorizationStatus is the existing status that State maps to.
	CategorizationStatus    string
	CompleteStrict          bool
	SubcategoryDistribution map[string]float64
	Levels                  map[string]uint8
	// LevelProbabilities maps a question id to the renormalised probability of
	// each level.
	LevelProbabilities map[string][]float32
	// SufficiencyLevel is 0, 1 or 2, and -1 when the question was not answered.
	SufficiencyLevel   int8
	EvidenceSpanID     string
	EvidenceHandle     string
	EvidenceSourceType string
	EvidenceSourceID   string
	Warnings           []string
	ErrorCodes         []string
	// ServedRunID is the categorization run id of the materialize run that hosted
	// the shadow phase.
	ServedRunID string
	ComputedAt  time.Time
}

// ShadowThemeDistribution is the theme map of a shadow row: the deterministic
// roll-up of its subcategory mix, and an empty map for an empty mix.
func ShadowThemeDistribution(subcategories map[string]float64) map[string]float64 {
	if len(subcategories) == 0 {
		return map[string]float64{}
	}
	return units.RollupSubcategoriesToThemes(subcategories)
}

// WriteShadowInvestments inserts work_unit_investment_shadow rows in one batch,
// stamping every row with orgID. A write of a missing table returns an error
// that wraps ErrShadowTableMissing.
func (w *Writer) WriteShadowInvestments(ctx context.Context, orgID string, records []ShadowRecord) (int, error) {
	if w == nil || w.conn == nil {
		return 0, fmt.Errorf("chwrite: writer unavailable")
	}
	if strings.TrimSpace(orgID) == "" {
		return 0, fmt.Errorf("%w: organization id is required to write work_unit_investment_shadow", ErrInvalidState)
	}
	if len(records) == 0 {
		return 0, nil
	}
	for i, record := range records {
		if err := computedAtError(record.ComputedAt, ShadowRetention); err != nil {
			return 0, fmt.Errorf("%w: work_unit_investment_shadow row %d: %v", ErrInvalidState, i, err)
		}
	}
	batch, err := w.conn.PrepareBatch(ctx, `INSERT INTO work_unit_investment_shadow (
		org_id, work_unit_id, categorization_input_hash, shadow_config, rubric_sha256,
		model_returned, state, categorization_status, complete_strict,
		subcategory_distribution_json, theme_distribution_json, levels,
		level_probabilities, sufficiency_level, evidence_span_id, evidence_handle,
		evidence_source_type, evidence_source_id, warnings, error_codes,
		served_run_id, computed_at
	)`)
	if err != nil {
		return 0, shadowTableError("prepare work_unit_investment_shadow batch", err)
	}
	for _, record := range records {
		subcategories := record.SubcategoryDistribution
		if subcategories == nil {
			subcategories = map[string]float64{}
		}
		levels := record.Levels
		if levels == nil {
			levels = map[string]uint8{}
		}
		probabilities := record.LevelProbabilities
		if probabilities == nil {
			probabilities = map[string][]float32{}
		}
		var completeStrict uint8
		if record.CompleteStrict {
			completeStrict = 1
		}
		if err := batch.Append(
			orgID, record.WorkUnitID, record.InputHash, record.ShadowConfig, record.RubricSHA256,
			record.ModelReturned, record.State, record.CategorizationStatus, completeStrict,
			subcategories, ShadowThemeDistribution(subcategories), levels,
			probabilities, record.SufficiencyLevel, record.EvidenceSpanID, record.EvidenceHandle,
			record.EvidenceSourceType, record.EvidenceSourceID, nonNilStrings(record.Warnings), nonNilStrings(record.ErrorCodes),
			record.ServedRunID, record.ComputedAt.UTC(),
		); err != nil {
			return 0, fmt.Errorf("append work_unit_investment_shadow row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return 0, shadowTableError("send work_unit_investment_shadow batch", err)
	}
	return len(records), nil
}

// AttemptRecord is one llm_categorization_attempts row. Scalars only: no
// prompt, no response and no source text. ModelReturned, ErrorClass, State,
// RequestID and Config are free String columns, so the caller must put closed
// codes and ids there, never text taken from a response.
//
// A row is identified by (RunID, WorkUnitID, Role, Config, Kind, Attempt): that
// is the sorting key, and a ReplacingMergeTree merge keeps one row for each such
// identity. Two attempts that differ in any of those six fields stay two rows.
type AttemptRecord struct {
	RunID        string
	WorkUnitID   string
	Role         string
	Config       string
	RubricSHA256 string
	Provider     string
	// APIMode is "responses" or "systemone".
	APIMode        string
	ModelRequested string
	ModelReturned  string
	// Attempt is 1 for the first send.
	Attempt uint8
	// Kind is first, retry, repair or fallback.
	Kind       string
	HTTPStatus uint16
	// ErrorClass is the failure class of the attempt, or an empty string.
	ErrorClass string
	// State is the final state on the last attempt of a classification and an
	// empty string on the others.
	State             string
	RequestID         string
	InputTokens       uint32
	OutputTokens      uint32
	CachedInputTokens uint32
	BilledCostUSD     float64
	RatesVersion      string
	LatencyMS         uint32
	RetryWaitMS       uint32
	ComputedAt        time.Time
}

// WriteAttempts inserts llm_categorization_attempts rows in ONE batch,
// stamping every row with orgID. It does not cap and does not swallow: a run
// uses AttemptBuffer and FlushAttempts, which do. A write of a missing table
// returns an error that wraps ErrShadowTableMissing.
func (w *Writer) WriteAttempts(ctx context.Context, orgID string, records []AttemptRecord) (int, error) {
	if w == nil || w.conn == nil {
		return 0, fmt.Errorf("chwrite: writer unavailable")
	}
	if strings.TrimSpace(orgID) == "" {
		return 0, fmt.Errorf("%w: organization id is required to write llm_categorization_attempts", ErrInvalidState)
	}
	if len(records) == 0 {
		return 0, nil
	}
	for i, record := range records {
		if err := computedAtError(record.ComputedAt, AttemptRetention); err != nil {
			return 0, fmt.Errorf("%w: llm_categorization_attempts row %d: %v", ErrInvalidState, i, err)
		}
	}
	batch, err := w.conn.PrepareBatch(ctx, `INSERT INTO llm_categorization_attempts (
		org_id, run_id, work_unit_id, role, config, rubric_sha256, provider, api_mode,
		model_requested, model_returned, attempt, kind, http_status, error_class, state,
		request_id, input_tokens, output_tokens, cached_input_tokens, billed_cost_usd,
		rates_version, latency_ms, retry_wait_ms, computed_at
	)`)
	if err != nil {
		return 0, shadowTableError("prepare llm_categorization_attempts batch", err)
	}
	for _, record := range records {
		if err := batch.Append(
			orgID, record.RunID, record.WorkUnitID, record.Role, record.Config, record.RubricSHA256,
			record.Provider, record.APIMode, record.ModelRequested, record.ModelReturned,
			record.Attempt, record.Kind, record.HTTPStatus, record.ErrorClass, record.State,
			record.RequestID, record.InputTokens, record.OutputTokens, record.CachedInputTokens,
			record.BilledCostUSD, record.RatesVersion, record.LatencyMS, record.RetryWaitMS,
			record.ComputedAt.UTC(),
		); err != nil {
			return 0, fmt.Errorf("append llm_categorization_attempts row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return 0, shadowTableError("send llm_categorization_attempts batch", err)
	}
	return len(records), nil
}

// AttemptBuffer collects the attempt rows of one run, up to a row cap. It is
// safe for concurrent use by the goroutines of a shadow phase. A row over the
// cap is not stored: it is counted, and the count goes to the run log line.
type AttemptBuffer struct {
	mu      sync.Mutex
	rows    []AttemptRecord
	limit   int
	dropped int
}

// NewAttemptBuffer builds a buffer that holds at most limit rows. A limit of
// zero or less means MaxAttemptRows.
func NewAttemptBuffer(limit int) *AttemptBuffer {
	if limit <= 0 {
		limit = MaxAttemptRows
	}
	return &AttemptBuffer{limit: limit}
}

// Add stores one row and reports whether it was stored. At the cap, or for a
// row with no ComputedAt (a zero time is 1970 and the TTL would delete it), it
// counts the row as dropped and returns false.
func (b *AttemptBuffer) Add(record AttemptRecord) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if computedAtError(record.ComputedAt, AttemptRetention) != nil || len(b.rows) >= b.limit {
		b.dropped++
		return false
	}
	b.rows = append(b.rows, record)
	return true
}

// Len is the number of rows that the buffer holds now.
func (b *AttemptBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.rows)
}

// Dropped is the number of rows that the buffer refused: over the cap, or with
// a ComputedAt older than the table retention.
func (b *AttemptBuffer) Dropped() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.dropped
}

// drain takes every row and the dropped count and empties the buffer, so a
// second flush cannot write the same rows again.
func (b *AttemptBuffer) drain() ([]AttemptRecord, int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	rows, dropped := b.rows, b.dropped
	b.rows, b.dropped = nil, 0
	return rows, dropped
}

// AttemptFlushResult is what FlushAttempts reports. FlushAttempts returns no
// error: a write failure of the attempt rows must not fail a run. The caller
// (the shadow phase) logs and counts from this value.
type AttemptFlushResult struct {
	// Written is the number of rows the insert took.
	Written int
	// Dropped is the number of rows the buffer refused (over the cap, or with no
	// ComputedAt).
	Dropped int
	// Failed is true when the insert failed. Those rows are lost, not retried.
	Failed bool
	// TableMissing is true when the insert failed because migration 110 is not
	// applied.
	TableMissing bool
	// Err is the cause of a failed insert, for the caller's log line. It is data,
	// not a return value: nil when Failed is false.
	Err error
}

// FlushAttempts writes the buffered attempt rows as one batch and empties the
// buffer. The write has its own AttemptWriteTimeout and honours ctx: a lost
// lease means "stop writing", so a cancelled ctx writes nothing. It logs
// nothing and returns no error: the result carries the counts and the cause.
func (w *Writer) FlushAttempts(ctx context.Context, orgID string, buffer *AttemptBuffer) AttemptFlushResult {
	if buffer == nil {
		return AttemptFlushResult{}
	}
	rows, dropped := buffer.drain()
	result := AttemptFlushResult{Dropped: dropped}
	if len(rows) == 0 {
		return result
	}
	writeCtx, cancel := context.WithTimeout(ctx, AttemptWriteTimeout)
	defer cancel()
	written, err := w.WriteAttempts(writeCtx, orgID, rows)
	if err != nil {
		result.Failed = true
		result.TableMissing = errors.Is(err, ErrShadowTableMissing)
		result.Err = err
		return result
	}
	result.Written = written
	return result
}

// shadowTableError wraps a prepare or send error. An UNKNOWN_TABLE error also
// wraps ErrShadowTableMissing.
func shadowTableError(what string, err error) error {
	var exception *clickhouse.Exception
	if errors.As(err, &exception) && exception.Code == clickHouseUnknownTable {
		return fmt.Errorf("%s: %w: %w", what, ErrShadowTableMissing, err)
	}
	return fmt.Errorf("%s: %w", what, err)
}

func nonNilStrings(values []string) []string {
	if values == nil {
		return []string{}
	}
	return values
}
