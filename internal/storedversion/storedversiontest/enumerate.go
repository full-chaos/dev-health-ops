// Package storedversiontest proves the stored-version invariant by
// enumeration: after any sequence of writers on one key, FINAL serves for
// each column the last value stated by a writer that is authoritative for it,
// and no writer's silence erases a value; a terminal column's held value is
// never replaced by null.
package storedversiontest

import (
	"fmt"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/storedversion"
)

// Writer is one writer of a table as the enumeration drives it, through its
// production contract, insert and carry decision.
type Writer struct {
	Name     string
	Contract storedversion.Contract
	Insert   string
	// NullIsUnstated: the writer's source cannot tell an absent field from a
	// null one (the internal ingest API serialises every declared field).
	NullIsUnstated bool
	// Carry is the writer's production carry decision for one payload.
	Carry func(payload map[string]any) map[string]bool
}

type statement uint8

const (
	absent statement = iota
	null
	value
)

type write struct {
	writer int
	fields map[string]statement
}

// Enumerate checks every sequence of 1..maxLen writes on one key, for every
// non-identity column any writer inserts, over every statement of that column's payload
// fields, against an in-memory FINAL (the newest version wins whole). A
// sequence of one writer is also applied as one batch. It returns the number
// of (column, sequence, statement) cells checked.
func Enumerate(t *testing.T, writers []Writer, maxLen int) int {
	t.Helper()
	positions := make([]map[string]int, len(writers))
	for i, writer := range writers {
		p, err := storedversion.Positions(writer.Insert)
		if err != nil {
			t.Fatal(err)
		}
		positions[i] = p
	}
	cells := 0
	for _, column := range columns(writers, positions) {
		for length := 1; length <= maxLen; length++ {
			for _, sequence := range sequences(len(writers), length) {
				for _, writes := range statements(writers, column, sequence) {
					cells = cells + 1
					want := oracle(writers, column, writes)
					if got := replay(t, writers, positions, column, writes, false); got != want {
						t.Fatalf("%s: %s: FINAL %v, want %v", column, describe(writers, writes), got, want)
					}
					if sameWriter(writes) {
						if got := replay(t, writers, positions, column, writes, true); got != want {
							t.Fatalf("%s: %s as one batch: FINAL %v, want %v", column, describe(writers, writes), got, want)
						}
					}
				}
			}
		}
	}
	return cells
}

// columns lists every column any writer inserts, except identity columns.
func columns(writers []Writer, positions []map[string]int) []string {
	identity := map[string]bool{}
	for _, writer := range writers {
		for _, key := range writer.Contract.Keys() {
			identity[key] = true
		}
	}
	seen := map[string]bool{}
	var out []string
	for i, writer := range writers {
		for _, c := range writer.Contract.Columns {
			if _, written := positions[i][c.Name]; written && !identity[c.Name] && !seen[c.Name] {
				seen[c.Name] = true
				out = append(out, c.Name)
			}
		}
	}
	return out
}

// Fingerprint renders a contract's rows for comparing contract tables.
func Fingerprint(contract storedversion.Contract) string {
	out := contract.Table
	for _, c := range contract.Columns {
		out += fmt.Sprintf("|%s:%d:%v:%v", c.Name, c.Rule, c.Fields, c.Terminal)
	}
	return out
}

func sequences(writers, length int) [][]int {
	if length == 0 {
		return [][]int{nil}
	}
	var out [][]int
	for _, prefix := range sequences(writers, length-1) {
		for w := 0; w < writers; w++ {
			out = append(out, append(append([]int{}, prefix...), w))
		}
	}
	return out
}

func rule(writer Writer, column string) (storedversion.Column, bool) {
	for _, c := range writer.Contract.Columns {
		if c.Name == column {
			return c, true
		}
	}
	return storedversion.Column{}, false
}

// alphabet lists every statement of one write's fields for the column.
func alphabet(writer Writer, column string) []map[string]statement {
	c, ok := rule(writer, column)
	if !ok {
		return []map[string]statement{{}}
	}
	switch c.Rule {
	case storedversion.NoField, storedversion.Writer:
		return []map[string]statement{{}}
	case storedversion.Stated, storedversion.StateCoupled:
		return []map[string]statement{{"": value}, {"": null}}
	}
	out := []map[string]statement{{}}
	for _, field := range c.Fields {
		var next []map[string]statement
		for _, partial := range out {
			for _, s := range []statement{absent, null, value} {
				m := map[string]statement{}
				for k, v := range partial {
					m[k] = v
				}
				m[field] = s
				next = append(next, m)
			}
		}
		out = next
	}
	return out
}

func statements(writers []Writer, column string, sequence []int) [][]write {
	out := [][]write{nil}
	for _, w := range sequence {
		var next [][]write
		for _, prefix := range out {
			for _, fields := range alphabet(writers[w], column) {
				next = append(next, append(append([]write{}, prefix...), write{writer: w, fields: fields}))
			}
		}
		out = next
	}
	return out
}

// stated is the independent reading of R1/R2: whether the write states the
// column, and whether it states a value.
func stated(writer Writer, column string, fields map[string]statement) (bool, bool) {
	c, ok := rule(writer, column)
	if !ok {
		return true, false
	}
	switch c.Rule {
	case storedversion.NoField:
		return false, false
	case storedversion.Writer:
		return true, true
	case storedversion.Stated, storedversion.StateCoupled:
		return true, fields[""] == value
	}
	anyValue, anyNull := false, false
	for _, s := range fields {
		anyValue = anyValue || s == value
		anyNull = anyNull || s == null
	}
	if anyValue {
		return true, true
	}
	return anyNull && !writer.NullIsUnstated, false
}

func token(i int) string        { return fmt.Sprintf("w%d", i) }
func defaultToken(i int) string { return fmt.Sprintf("default%d", i) }

// oracle reads terminal per table: a column any writer marks terminal
// records an event, so no writer may replace its held value with null.
func oracle(writers []Writer, column string, writes []write) any {
	terminal := false
	for _, writer := range writers {
		if c, ok := rule(writer, column); ok && c.Terminal {
			terminal = true
		}
	}
	var held any
	exists := false
	for i, w := range writes {
		writer := writers[w.writer]
		isStated, isValue := stated(writer, column, w.fields)
		switch {
		case isStated && isValue:
			held = token(i)
		case isStated:
			if !(terminal && exists && held != nil) {
				held = nil
			}
		case !exists:
			held = defaultToken(i)
		}
		exists = true
	}
	return held
}

// row builds one write's insert values: key columns "k", the column under
// test from its statement, every other column nil.
func row(writer Writer, positions map[string]int, column string, i int, fields map[string]statement) storedversion.Row {
	values := make([]any, len(positions))
	for _, key := range writer.Contract.Keys() {
		values[positions[key]] = "k"
	}
	payload := map[string]any{}
	if c, ok := rule(writer, column); ok {
		for _, field := range c.Fields {
			switch fields[field] {
			case value:
				payload[field] = token(i)
			case null:
				payload[field] = nil
			}
		}
	}
	if p, ok := positions[column]; ok {
		isStated, isValue := stated(writer, column, fields)
		switch {
		case isValue:
			values[p] = token(i)
		case !isStated:
			values[p] = defaultToken(i)
		}
	}
	return storedversion.Row{Values: values, Carry: writer.Carry(payload)}
}

func replay(t *testing.T, writers []Writer, positions []map[string]int, column string, writes []write, batch bool) any {
	t.Helper()
	var table map[string]any
	held := func([]any) (map[string]any, bool) { return table, table != nil }
	apply := func(writer int, rows []storedversion.Row) {
		if _, err := writers[writer].Contract.Fold(writers[writer].Insert, rows, held); err != nil {
			t.Fatal(err)
		}
		last := rows[len(rows)-1]
		table = map[string]any{}
		for name, p := range positions[writer] {
			table[name] = last.Values[p]
		}
	}
	if batch {
		rows := make([]storedversion.Row, len(writes))
		for i, w := range writes {
			rows[i] = row(writers[w.writer], positions[w.writer], column, i, w.fields)
		}
		apply(writes[0].writer, rows)
	} else {
		for i, w := range writes {
			apply(w.writer, []storedversion.Row{row(writers[w.writer], positions[w.writer], column, i, w.fields)})
		}
	}
	return table[column]
}

func sameWriter(writes []write) bool {
	for _, w := range writes {
		if w.writer != writes[0].writer {
			return false
		}
	}
	return len(writes) > 1
}

func describe(writers []Writer, writes []write) string {
	out := ""
	for i, w := range writes {
		out += fmt.Sprintf("[%d %s %v]", i, writers[w.writer].Name, w.fields)
	}
	return out
}
