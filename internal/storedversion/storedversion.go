// Package storedversion keeps established column values when a writer of a
// ClickHouse ReplacingMergeTree table inserts a new version of a row.
//
// A FINAL read serves the newest physical row as a whole, so a new version
// that leaves a column empty erases whatever an earlier version (from this
// writer or another one sharing the key) held. Each writer declares one
// contract table naming every column it inserts and what its source means for
// that column; Apply reads the current version of every key in one FINAL
// lookup and copies held values into the new version as the contract says.
//
// The rules are decided data semantics:
//
//	R1 NoField: the writer's source has no field for the column, so the held
//	   value is kept.
//	R2 Unstated: the source has fields for the column but this row does not
//	   state them (an absent external key, a failed enrichment lookup), so the
//	   held value is kept; a stated value, including an explicit null, is
//	   written. Stated columns are written as given; StateCoupled marks
//	   lifecycle timestamps tied to a required state/status, written as given
//	   so a reopen can clear them.
//	R3 terminal: the column records an event that cannot un-happen
//	   (merged_at, first_review_at), so a held value is never replaced by null.
//	R4 scope: every writer of an in-scope table has one contract table naming
//	   every column it writes.
//
// The read and the insert are two statements with no lock between them. A
// second writer that inserts the same key inside that window loses its value
// when this writer's version has the newer version column: this version
// carries what the read saw. A failed read fails Apply, so the caller writes
// nothing instead of blanks over established values.
package storedversion

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// Rule states what one writer's source means for one column it writes.
type Rule uint8

const (
	Identity Rule = iota + 1
	Writer
	Stated
	StateCoupled
	NoField
	Unstated
)

// Column is one row of a writer's contract table. Fields names the source
// keys the column is translated from (for Unstated: the keys whose absence
// makes the column unstated). Value produces a column the writer appends to a
// translation that does not produce it.
type Column struct {
	Name     string
	Rule     Rule
	Fields   []string
	Terminal bool
	// With names a terminal column this column is kept together with: when
	// that column's stated null is refused, this column keeps its held value
	// too, so the row does not contradict itself.
	With  string
	Value func(payload map[string]any) any
}

// Contract is one writer's contract table for one table.
type Contract struct {
	Writer  string
	Table   string
	Columns []Column
}

// Spec is one writer as the invariant enumeration drives it: its contract,
// its insert and its carry decision for a payload of source fields.
// NullIsUnstated marks a source that cannot tell an absent field from a null
// one.
type Spec struct {
	Name           string
	Contract       Contract
	Insert         string
	NullIsUnstated bool
	Carry          func(payload map[string]any) map[string]bool
}

// Row is one new version in insert column order, with the columns this row
// carries from the held version.
type Row struct {
	Values []any
	Carry  map[string]bool
}

// Outcome reports, per row, the columns taken from the held version.
// Carried follows the row's carry set; Refused is a terminal column whose
// null was replaced by a held value.
type Outcome struct {
	Key              []any
	Carried, Refused []string
}

// Querier is the read side of a ClickHouse connection.
type Querier interface {
	Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
}

// Keys returns the identity columns in contract order.
func (c Contract) Keys() []string {
	var keys []string
	for _, column := range c.Columns {
		if column.Rule == Identity {
			keys = append(keys, column.Name)
		}
	}
	return keys
}

func containsName(names []string, name string) bool {
	for _, candidate := range names {
		if candidate == name {
			return true
		}
	}
	return false
}

// Kept returns the columns Apply reads: R1, R2, terminal columns and the
// columns kept together with a terminal column.
func (c Contract) Kept() []string {
	var kept []string
	for _, column := range c.Columns {
		if column.Rule == NoField || column.Rule == Unstated || column.Terminal || column.With != "" {
			kept = append(kept, column.Name)
		}
	}
	return kept
}

// Terminal returns the R3 columns.
func (c Contract) Terminal() []string {
	var terminal []string
	for _, column := range c.Columns {
		if column.Terminal {
			terminal = append(terminal, column.Name)
		}
	}
	return terminal
}

// Reads reports whether the contract keeps any column.
func (c Contract) Reads() bool { return len(c.Kept()) > 0 }

// Carry marks the R1 columns and the R2 columns whose every field is
// unstated for one row.
func (c Contract) Carry(unstated func(field string) bool) map[string]bool {
	carry := make(map[string]bool, len(c.Columns))
	for _, column := range c.Columns {
		switch column.Rule {
		case NoField:
			carry[column.Name] = true
		case Unstated:
			all := true
			for _, field := range column.Fields {
				all = all && unstated(field)
			}
			carry[column.Name] = all
		}
	}
	return carry
}

// Apply reads the held version of every row's key and rewrites the rows in
// place. Rows sharing a key fold in order: a later row sees the earlier one
// as the held version.
func (c Contract) Apply(ctx context.Context, conn Querier, orgID, insert string, rows []Row) ([]Outcome, error) {
	if len(rows) == 0 {
		return nil, nil
	}
	positions, rowKeys, err := c.check(insert, rows)
	if err != nil {
		return nil, err
	}
	stored, err := read(ctx, conn, c.Table, orgID, c.Keys(), c.Kept(), rowKeys)
	if err != nil {
		return nil, fmt.Errorf("read current %s versions: %w", c.Table, err)
	}
	return c.fold(positions, rowKeys, rows, func(key []any) (map[string]any, bool) {
		version, ok := stored[versionKey(key)]
		return version, ok
	}), nil
}

// Fold applies the contract to rows against held versions supplied by the
// caller instead of a ClickHouse read; held returns the kept columns of the
// current version of a key.
func (c Contract) Fold(insert string, rows []Row, held func(key []any) (map[string]any, bool)) ([]Outcome, error) {
	positions, rowKeys, err := c.check(insert, rows)
	if err != nil {
		return nil, err
	}
	return c.fold(positions, rowKeys, rows, held), nil
}

func (c Contract) check(insert string, rows []Row) (map[string]int, [][]any, error) {
	positions, err := Positions(insert)
	if err != nil {
		return nil, nil, err
	}
	keys := c.Keys()
	terminal := c.Terminal()
	for _, column := range c.Columns {
		if column.With != "" && !containsName(terminal, column.With) {
			return nil, nil, fmt.Errorf("stored version of %s: %s is kept with %s, which is not terminal", c.Table, column.Name, column.With)
		}
	}
	for _, column := range append(append([]string{}, keys...), c.Kept()...) {
		if _, ok := positions[column]; !ok {
			return nil, nil, fmt.Errorf("stored version of %s: column %s is not written by %q", c.Table, column, insert)
		}
	}
	rowKeys := make([][]any, len(rows))
	for i, row := range rows {
		if len(row.Values) != len(positions) {
			return nil, nil, fmt.Errorf("stored version of %s: row has %d values for %d columns", c.Table, len(row.Values), len(positions))
		}
		key := make([]any, len(keys))
		for j, column := range keys {
			key[j] = row.Values[positions[column]]
		}
		rowKeys[i] = key
	}
	return positions, rowKeys, nil
}

func (c Contract) fold(
	positions map[string]int, rowKeys [][]any, rows []Row, held func(key []any) (map[string]any, bool),
) []Outcome {
	kept, terminal := c.Kept(), c.Terminal()
	batch := make(map[string]map[string]any, len(rows))
	outcomes := make([]Outcome, len(rows))
	for i, row := range rows {
		k := versionKey(rowKeys[i])
		version, ok := batch[k]
		if !ok {
			version, ok = held(rowKeys[i])
		}
		outcome := Outcome{Key: rowKeys[i]}
		if ok {
			for _, column := range kept {
				if row.Carry[column] {
					row.Values[positions[column]] = version[column]
					if version[column] != nil {
						outcome.Carried = append(outcome.Carried, column)
					}
				}
			}
			for _, column := range terminal {
				if row.Values[positions[column]] == nil && version[column] != nil {
					row.Values[positions[column]] = version[column]
					outcome.Refused = append(outcome.Refused, column)
				}
			}
			refused := outcome.Refused
			for _, column := range c.Columns {
				if column.With != "" && containsName(refused, column.With) {
					row.Values[positions[column.Name]] = version[column.Name]
					outcome.Refused = append(outcome.Refused, column.Name)
				}
			}
		}
		next := make(map[string]any, len(kept))
		for _, column := range kept {
			next[column] = row.Values[positions[column]]
		}
		batch[k] = next
		outcomes[i] = outcome
	}
	return outcomes
}

// Log emits one DEBUG line per row with carried columns and one WARN line per
// row with refused terminal nulls, under the caller's stable event names.
func (c Contract) Log(ctx context.Context, orgID, carriedEvent, refusedEvent string, outcomes []Outcome) {
	logger := slog.Default()
	attrs := func(outcome Outcome, columns []string) []slog.Attr {
		return []slog.Attr{
			slog.String("writer", c.Writer), slog.String("org_id", orgID),
			slog.String("table", c.Table), slog.String("key", LogKey(outcome.Key)),
			slog.String("columns", strings.Join(columns, ",")),
		}
	}
	for _, outcome := range outcomes {
		if len(outcome.Carried) > 0 {
			logger.LogAttrs(ctx, slog.LevelDebug, carriedEvent, attrs(outcome, outcome.Carried)...)
		}
		if len(outcome.Refused) > 0 {
			logger.LogAttrs(ctx, slog.LevelWarn, refusedEvent, attrs(outcome, outcome.Refused)...)
		}
	}
}

// read is one FINAL lookup per call, filtered by each key column's distinct
// values; the exact tuple match happens on the returned rows.
func read(
	ctx context.Context, conn Querier, table, orgID string, keys, kept []string, rowKeys [][]any,
) (map[string]map[string]any, error) {
	args := []any{orgID}
	filters := make([]string, 0, len(keys))
	for position, column := range keys {
		seen := make(map[string]bool, len(rowKeys))
		values := make([]any, 0, len(rowKeys))
		for _, key := range rowKeys {
			value := key[position]
			if id, ok := value.(uuid.UUID); ok {
				value = id.String()
			}
			text := fmt.Sprint(value)
			if seen[text] {
				continue
			}
			seen[text] = true
			values = append(values, value)
		}
		filters = append(filters, column+" IN (?)")
		args = append(args, values)
	}
	selected := append(append([]string{}, keys...), kept...)
	query := fmt.Sprintf("SELECT %s FROM %s FINAL WHERE org_id = ? AND %s",
		strings.Join(selected, ", "), table, strings.Join(filters, " AND "))
	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	types := rows.ColumnTypes()
	stored := make(map[string]map[string]any, len(rowKeys))
	for rows.Next() {
		targets := make([]any, len(types))
		for i, columnType := range types {
			targets[i] = reflect.New(columnType.ScanType()).Interface()
		}
		if err := rows.Scan(targets...); err != nil {
			return nil, err
		}
		key := make([]any, len(keys))
		for i := range keys {
			key[i] = scalar(targets[i])
		}
		version := make(map[string]any, len(kept))
		for i, column := range kept {
			version[column] = scalar(targets[len(keys)+i])
		}
		stored[versionKey(key)] = version
	}
	return stored, rows.Err()
}

// scalar turns a scan target into the value an insert takes: a null Nullable
// column becomes an untyped nil, everything else its plain value.
func scalar(target any) any {
	value := reflect.ValueOf(target).Elem()
	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return nil
		}
		return value.Elem().Interface()
	}
	return value.Interface()
}

func versionKey(key []any) string {
	parts := make([]string, len(key))
	for i, value := range key {
		parts[i] = fmt.Sprint(value)
	}
	return strings.Join(parts, "\x00")
}

// LogKey renders a row key for a log line.
func LogKey(key []any) string {
	parts := make([]string, len(key))
	for i, value := range key {
		parts[i] = fmt.Sprint(value)
	}
	return strings.Join(parts, "/")
}

// Positions maps each column of an INSERT statement's column list to its
// position.
func Positions(insert string) (map[string]int, error) {
	open, end := strings.IndexByte(insert, '('), strings.LastIndexByte(insert, ')')
	if open < 0 || end <= open {
		return nil, fmt.Errorf("insert statement has no column list: %q", insert)
	}
	columns := strings.Split(insert[open+1:end], ",")
	positions := make(map[string]int, len(columns))
	for i, column := range columns {
		positions[strings.TrimSpace(column)] = i
	}
	return positions, nil
}
