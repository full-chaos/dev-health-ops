package streamhandlers

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"strings"

	"github.com/google/uuid"
)

// storedVersionCarriedEvent and storedVersionRefusedEvent are stable log
// event names. "carried" is the normal path for a column the payload does not
// state; "refused" means a payload stated null for a column whose established
// value cannot legitimately be cleared, which points at a stale or racing
// upstream read.
const (
	storedVersionCarriedEvent = "streamhandlers.stored_version.carried_forward"
	storedVersionRefusedEvent = "streamhandlers.stored_version.null_over_value_refused"
)

// columnRule states what one writer's payload means for one column it writes.
// The rules are decided data semantics:
//
//	R1 columnNoField: the payload has no field for the column, so the value
//	   the row already holds is kept.
//	R2 columnUnstated: an external record's optional key is absent, so the
//	   held value is kept; a present key, including an explicit null, is
//	   written. The internal ingest API serialises every declared field, so
//	   there a null field is the client's statement and columnStated writes
//	   it as given; its one columnUnstated row is updated_at, a non-null
//	   column the writer would otherwise fill with created_at.
//	   columnStateCoupled marks lifecycle timestamps tied to the required
//	   state/status, written as given so a reopen can clear them.
//	R3 terminal: the column records an event that cannot un-happen
//	   (merged_at, first_review_at), so a held value is never replaced by null.
//	R4 scope: every stream-handler writer of git_pull_requests,
//	   git_pull_request_reviews, git_commits, deployments and work_items has
//	   one contract table naming every column it writes.
type columnRule uint8

const (
	columnIdentity columnRule = iota + 1
	columnWriter
	columnStated
	columnStateCoupled
	columnNoField
	columnUnstated
)

// columnContract is one row of a writer's contract table. fields names the
// payload keys the column is translated from. value produces the column for
// a column the writer appends to a translation that does not produce it.
type columnContract struct {
	column   string
	rule     columnRule
	fields   []string
	terminal bool
	value    func(payload map[string]any) any
}

// writerContract is one writer's contract table for one table.
type writerContract struct {
	writer  string
	table   string
	columns []columnContract
}

func (c writerContract) versions() storedVersionWrite {
	write := storedVersionWrite{writer: c.writer, table: c.table}
	for _, column := range c.columns {
		switch {
		case column.rule == columnIdentity:
			write.keys = append(write.keys, column.column)
		case column.rule == columnNoField || column.rule == columnUnstated || column.terminal:
			write.columns = append(write.columns, column.column)
		}
		if column.terminal {
			write.terminal = append(write.terminal, column.column)
		}
	}
	return write
}

// carry marks the R1 columns and the R2 columns whose every payload field is
// unstated.
func (c writerContract) carry(unstated func(field string) bool) map[string]bool {
	carry := make(map[string]bool, len(c.columns))
	for _, column := range c.columns {
		switch column.rule {
		case columnNoField:
			carry[column.column] = true
		case columnUnstated:
			all := true
			for _, field := range column.fields {
				all = all && unstated(field)
			}
			carry[column.column] = all
		}
	}
	return carry
}

func (c writerContract) reads() bool { return len(c.versions().columns) > 0 }

// storedVersionWrite describes how one writer keeps the established values of
// a ReplacingMergeTree row. A FINAL read serves the newest physical row as a
// whole, so a new version that leaves a column empty erases whatever an
// earlier version (from this writer or another one sharing the key) held.
// The writer therefore reads the current version of every key it is about to
// write and copies established values into the new version for:
//   - carry columns chosen per row (the payload does not state the column), and
//   - terminal columns, whose established value is never replaced by null.
//
// The read and the insert are two statements with no lock between them. A
// second writer that inserts the same key inside that window loses its value
// when this writer's version has the newer last_synced: this version carries
// what the read saw. The window is shared only by writers of one key running
// at once: external ingest (one consumer) against provider sync on shared
// github/gitlab keys, and internal ingest replicas against each other or,
// for work items, against external ingest and provider sync.
// The read failing aborts the write so the message redelivers: inserting
// without it would write blanks over established values.
type storedVersionWrite struct {
	writer   string
	table    string
	keys     []string
	columns  []string
	terminal []string
}

type storedVersionRow struct {
	values []any
	carry  map[string]bool
}

func (w storedVersionWrite) apply(
	ctx context.Context, conn productClickHouse, orgID, insert string, rows []storedVersionRow,
) error {
	if len(rows) == 0 {
		return nil
	}
	positions, err := insertColumnPositions(insert)
	if err != nil {
		return err
	}
	for _, column := range append(append(append([]string{}, w.keys...), w.columns...), w.terminal...) {
		if _, ok := positions[column]; !ok {
			return fmt.Errorf("stored version of %s: column %s is not written by %q", w.table, column, insert)
		}
	}
	for _, column := range w.terminal {
		if !containsString(w.columns, column) {
			return fmt.Errorf("stored version of %s: terminal column %s is not read", w.table, column)
		}
	}
	keys := make([][]any, len(rows))
	for i, row := range rows {
		if len(row.values) != len(positions) {
			return fmt.Errorf("stored version of %s: row has %d values for %d columns", w.table, len(row.values), len(positions))
		}
		key := make([]any, len(w.keys))
		for j, column := range w.keys {
			key[j] = row.values[positions[column]]
		}
		keys[i] = key
	}
	stored, err := w.read(ctx, conn, orgID, keys)
	if err != nil {
		return fmt.Errorf("read current %s versions: %w", w.table, err)
	}
	for i, row := range rows {
		k := storedVersionKey(keys[i])
		var carried, refused []string
		if version, ok := stored[k]; ok {
			for _, column := range w.columns {
				if row.carry[column] {
					row.values[positions[column]] = version[column]
					if version[column] != nil {
						carried = append(carried, column)
					}
				}
			}
			for _, column := range w.terminal {
				if row.values[positions[column]] == nil && version[column] != nil {
					row.values[positions[column]] = version[column]
					refused = append(refused, column)
				}
			}
		}
		next := make(map[string]any, len(w.columns))
		for _, column := range w.columns {
			next[column] = row.values[positions[column]]
		}
		stored[k] = next
		w.log(ctx, orgID, keys[i], carried, refused)
	}
	return nil
}

func (w storedVersionWrite) log(ctx context.Context, orgID string, key []any, carried, refused []string) {
	logger := slog.Default()
	attrs := func(columns []string) []slog.Attr {
		return []slog.Attr{
			slog.String("writer", w.writer), slog.String("org_id", orgID),
			slog.String("table", w.table), slog.String("key", storedVersionLogKey(key)),
			slog.String("columns", strings.Join(columns, ",")),
		}
	}
	if len(carried) > 0 {
		logger.LogAttrs(ctx, slog.LevelDebug, storedVersionCarriedEvent, attrs(carried)...)
	}
	if len(refused) > 0 {
		logger.LogAttrs(ctx, slog.LevelWarn, storedVersionRefusedEvent, attrs(refused)...)
	}
}

// read is one FINAL lookup per write call, filtered by each key column's
// distinct values; the exact tuple match happens on the returned rows.
func (w storedVersionWrite) read(
	ctx context.Context, conn productClickHouse, orgID string, keys [][]any,
) (map[string]map[string]any, error) {
	args := []any{orgID}
	filters := make([]string, 0, len(w.keys))
	for position, column := range w.keys {
		seen := make(map[string]bool, len(keys))
		values := make([]any, 0, len(keys))
		for _, key := range keys {
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
	selected := append(append([]string{}, w.keys...), w.columns...)
	query := fmt.Sprintf("SELECT %s FROM %s FINAL WHERE org_id = ? AND %s",
		strings.Join(selected, ", "), w.table, strings.Join(filters, " AND "))
	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	types := rows.ColumnTypes()
	stored := make(map[string]map[string]any, len(keys))
	for rows.Next() {
		targets := make([]any, len(types))
		for i, columnType := range types {
			targets[i] = reflect.New(columnType.ScanType()).Interface()
		}
		if err := rows.Scan(targets...); err != nil {
			return nil, err
		}
		key := make([]any, len(w.keys))
		for i := range w.keys {
			key[i] = storedScalar(targets[i])
		}
		version := make(map[string]any, len(w.columns))
		for i, column := range w.columns {
			version[column] = storedScalar(targets[len(w.keys)+i])
		}
		stored[storedVersionKey(key)] = version
	}
	return stored, rows.Err()
}

// storedScalar turns a scan target into the value an insert takes: a null
// Nullable column becomes an untyped nil, everything else its plain value.
func storedScalar(target any) any {
	value := reflect.ValueOf(target).Elem()
	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return nil
		}
		return value.Elem().Interface()
	}
	return value.Interface()
}

func storedVersionKey(key []any) string {
	parts := make([]string, len(key))
	for i, value := range key {
		parts[i] = fmt.Sprint(value)
	}
	return strings.Join(parts, "\x00")
}

func storedVersionLogKey(key []any) string {
	parts := make([]string, len(key))
	for i, value := range key {
		parts[i] = fmt.Sprint(value)
	}
	return strings.Join(parts, "/")
}

func insertColumnPositions(insert string) (map[string]int, error) {
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

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
