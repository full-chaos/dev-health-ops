// Package dimensionfold keeps small ReplacingMergeTree dimension tables at one
// physical row per key.
//
// Provider syncs rewrite every row of `repos` and `teams` on each run. The new
// version lands as a new part, and until a merge folds it into the old one the
// key has two physical rows. Readers that join these tables without FINAL
// multiply every matching row for as long as that lasts. Background merges
// leave a small new part next to a large settled one for an hour or more, and
// an age-forced merge never includes the large part while writes keep
// arriving, because every merge of the small parts restarts their age.
//
// The fixed scheduler therefore enqueues system.dimension_fold on an interval,
// and this package runs `OPTIMIZE TABLE <t> FINAL` for each declared table. On
// a single-partition table that is one merge of every active part, which drops
// each superseded version at any write rate. optimize_skip_merged_partitions
// makes a table that is already one merged part a no-op, so an idle table is
// never rewritten.
//
// OPTIMIZE FINAL rewrites the whole partition, so a table qualifies only while
// it has exactly one partition and at most MaxRows rows. Both are read from
// system.parts before every statement, so the fold can never reach a fact
// table through a schema change that outgrows the bound.
package dimensionfold

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"time"
)

// TableEvent is the stable log event written once per table per run.
const TableEvent = "dimension_fold.table"

// Outcomes of one table in one run.
const (
	OutcomeFolded             = "folded"
	OutcomeSkippedAsMerged    = "skipped_as_merged"
	OutcomeSkippedEmpty       = "skipped_empty"
	OutcomeSkippedPartitioned = "skipped_multi_partition"
	OutcomeSkippedOversize    = "skipped_above_row_bound"
	OutcomeFailed             = "failed"
)

// DeclaredTables are the dimension tables every provider sync rewrites.
var DeclaredTables = []string{"repos", "teams"}

// ErrInvalidConfig identifies a Folder that cannot be operated.
var ErrInvalidConfig = errors.New("dimension fold configuration is invalid")

var tableNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

// Conn is the ClickHouse capability the fold needs.
type Conn interface {
	Exec(ctx context.Context, query string, args ...any) error
	QueryRow(ctx context.Context, query string, args ...any) Row
}

// Row is one scanned result row.
type Row interface {
	Scan(dest ...any) error
}

// Config bounds one Folder.
type Config struct {
	// Tables are folded in order; a failure on one never stops the next.
	Tables []string
	// MaxRows is the row bound above which a table is refused.
	MaxRows uint64
	// TableTimeout bounds each table's reads and its OPTIMIZE, so one stuck
	// merge cannot hold the job past its budget.
	TableTimeout time.Duration
}

// DefaultConfig is the production configuration.
func DefaultConfig() Config {
	return Config{
		Tables:       append([]string(nil), DeclaredTables...),
		MaxRows:      1_000_000,
		TableTimeout: 30 * time.Second,
	}
}

func (cfg Config) validate() error {
	if len(cfg.Tables) == 0 || cfg.MaxRows == 0 ||
		cfg.TableTimeout <= 0 || cfg.TableTimeout > 10*time.Minute {
		return ErrInvalidConfig
	}
	seen := make(map[string]struct{}, len(cfg.Tables))
	for _, table := range cfg.Tables {
		if !tableNamePattern.MatchString(table) {
			return ErrInvalidConfig
		}
		if _, duplicate := seen[table]; duplicate {
			return ErrInvalidConfig
		}
		seen[table] = struct{}{}
	}
	return nil
}

// TableReport is what one run did to one table.
type TableReport struct {
	Table       string
	Outcome     string
	Partitions  uint64
	Rows        uint64
	PartsBefore uint64
	PartsAfter  uint64
	Duration    time.Duration
	Err         error
	// CrossedBound reports that the table was inside the row bound when the
	// fold started and outside it when the fold finished, so a concurrent
	// write grew it past the bound during this run. The bound holds between
	// runs, never within one: the next run refuses the table.
	CrossedBound bool
}

// Folder folds the declared tables.
type Folder struct {
	conn   Conn
	config Config
	logger *slog.Logger
}

// NewFolder constructs a Folder. Every collaborator is required.
func NewFolder(conn Conn, cfg Config, logger *slog.Logger) (*Folder, error) {
	if conn == nil || logger == nil || cfg.validate() != nil {
		return nil, ErrInvalidConfig
	}
	cfg.Tables = append([]string(nil), cfg.Tables...)
	return &Folder{conn: conn, config: cfg, logger: logger}, nil
}

// Run folds every declared table once and reports each. A table's failure is
// logged with its cause and the next table still runs; Run itself fails only
// when the caller's context ends first.
func (folder *Folder) Run(ctx context.Context) ([]TableReport, error) {
	reports := make([]TableReport, 0, len(folder.config.Tables))
	for _, table := range folder.config.Tables {
		if err := ctx.Err(); err != nil {
			return reports, err
		}
		report := folder.foldTable(ctx, table)
		folder.log(ctx, report)
		reports = append(reports, report)
	}
	return reports, nil
}

type partsState struct {
	partitions uint64
	parts      uint64
	rows       uint64
	names      string
}

const partsStateSQL = `
SELECT
  uniqExact(partition_id),
  count(),
  sum(rows),
  arrayStringConcat(arraySort(groupArray(name)), ',')
FROM system.parts
WHERE database = currentDatabase() AND table = ? AND active`

func (folder *Folder) readParts(ctx context.Context, table string) (partsState, error) {
	var state partsState
	err := folder.conn.QueryRow(ctx, partsStateSQL, table).Scan(
		&state.partitions, &state.parts, &state.rows, &state.names,
	)
	return state, err
}

func (folder *Folder) foldTable(parent context.Context, table string) TableReport {
	started := time.Now()
	ctx, cancel := context.WithTimeout(parent, folder.config.TableTimeout)
	defer cancel()
	report := TableReport{Table: table}
	finish := func(outcome string, err error) TableReport {
		report.Outcome, report.Err = outcome, err
		report.Duration = time.Since(started)
		return report
	}
	before, err := folder.readParts(ctx, table)
	if err != nil {
		return finish(OutcomeFailed, fmt.Errorf("read parts: %w", err))
	}
	report.Partitions, report.Rows = before.partitions, before.rows
	report.PartsBefore, report.PartsAfter = before.parts, before.parts
	switch {
	case before.parts == 0:
		return finish(OutcomeSkippedEmpty, nil)
	case before.partitions > 1:
		return finish(OutcomeSkippedPartitioned, nil)
	case before.rows > folder.config.MaxRows:
		return finish(OutcomeSkippedOversize, nil)
	}
	// The table name comes from the validated declared list, never from input.
	if err := folder.conn.Exec(ctx, "OPTIMIZE TABLE "+table+
		" FINAL SETTINGS optimize_skip_merged_partitions = 1"); err != nil {
		return finish(OutcomeFailed, fmt.Errorf("optimize: %w", err))
	}
	after, err := folder.readParts(ctx, table)
	if err != nil {
		return finish(OutcomeFailed, fmt.Errorf("read parts after optimize: %w", err))
	}
	report.PartsAfter, report.Rows = after.parts, after.rows
	report.CrossedBound = after.rows > folder.config.MaxRows
	if before.parts == 1 && after.names == before.names {
		return finish(OutcomeSkippedAsMerged, nil)
	}
	return finish(OutcomeFolded, nil)
}

func (folder *Folder) log(ctx context.Context, report TableReport) {
	attrs := []slog.Attr{
		slog.String("table", report.Table),
		slog.String("outcome", report.Outcome),
		slog.Uint64("partitions", report.Partitions),
		slog.Uint64("rows", report.Rows),
		slog.Uint64("parts_before", report.PartsBefore),
		slog.Uint64("parts_after", report.PartsAfter),
		slog.Bool("skipped_as_merged", report.Outcome == OutcomeSkippedAsMerged),
		slog.Bool("crossed_row_bound", report.CrossedBound),
		slog.Int64("duration_ms", report.Duration.Milliseconds()),
		slog.Uint64("max_rows", folder.config.MaxRows),
	}
	level := slog.LevelInfo
	switch report.Outcome {
	case OutcomeFailed:
		level = slog.LevelWarn
		attrs = append(attrs, slog.String("error", report.Err.Error()))
	case OutcomeSkippedPartitioned, OutcomeSkippedOversize:
		level = slog.LevelWarn
	}
	if report.CrossedBound {
		level = slog.LevelWarn
	}
	folder.logger.LogAttrs(ctx, level, TableEvent, attrs...)
}
