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

// Writer is one writer of a table as the enumeration drives it.
type Writer = storedversion.Spec

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
	for _, field := range c.Fields {
		anyValue = anyValue || fields[field] == value
		anyNull = anyNull || fields[field] == null
	}
	if anyValue {
		return true, true
	}
	return anyNull && !writer.NullIsUnstated, false
}

func token(i int) string        { return fmt.Sprintf("w%d", i) }
func defaultToken(i int) string { return fmt.Sprintf("default%d", i) }

// oracle reads terminal per table: a column any writer marks terminal
// records an event, so no writer may replace its held value with null. A
// column a writer keeps With a terminal column keeps its held value when
// that writer's write refuses the terminal column's null.
func oracle(writers []Writer, column string, writes []write) any {
	history := oracleHistory(writers, column, writes)
	return history[len(history)-1]
}

// oracleHistory returns the column's FINAL value after each write.
func oracleHistory(writers []Writer, column string, writes []write) []any {
	terminal := terminalColumn(writers, column)
	history := make([]any, len(writes))
	var held any
	exists := false
	withHistory := map[string][]any{}
	for i, w := range writes {
		writer := writers[w.writer]
		isStated, isValue := stated(writer, column, w.fields)
		c, _ := rule(writer, column)
		switch {
		case isStated && isValue:
			held = token(i)
		case isStated:
			keep := terminal && exists && held != nil
			if c.With != "" && exists && held != nil {
				if _, ok := withHistory[c.With]; !ok {
					withHistory[c.With] = oracleHistory(writers, c.With, writes)
				}
				keep = keep || refusedAt(writers, c.With, writes, withHistory[c.With], i)
			}
			if !keep {
				held = nil
			}
		case !exists:
			held = defaultToken(i)
		}
		exists = true
		history[i] = held
	}
	return history
}

func terminalColumn(writers []Writer, column string) bool {
	for _, writer := range writers {
		if c, ok := rule(writer, column); ok && c.Terminal {
			return true
		}
	}
	return false
}

// refusedAt reports whether write i states the terminal column null over a
// held value.
func refusedAt(writers []Writer, column string, writes []write, history []any, i int) bool {
	isStated, isValue := stated(writers[writes[i].writer], column, writes[i].fields)
	return i > 0 && isStated && !isValue && history[i-1] != nil
}

// row builds one write's insert values: key columns "k", and every other
// column from the statement of its own source fields (a column whose fields
// are not under test is absent: its default, or null when stated).
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
	for _, c := range writer.Contract.Columns {
		p, ok := positions[c.Name]
		if !ok || c.Rule == storedversion.Identity {
			continue
		}
		if c.Name != column && !sharesFields(writer, c.Name, column) {
			if c.Rule == storedversion.NoField || c.Rule == storedversion.Unstated {
				values[p] = defaultToken(i)
			}
			continue
		}
		isStated, isValue := stated(writer, c.Name, fields)
		switch {
		case isValue:
			values[p] = token(i)
		case !isStated:
			values[p] = defaultToken(i)
		}
	}
	return storedversion.Row{Values: values, Carry: writer.Carry(payload)}
}

// sharesFields reports whether two unstated columns of a writer are
// translated from the same source fields.
func sharesFields(writer Writer, a, b string) bool {
	ca, okA := rule(writer, a)
	cb, okB := rule(writer, b)
	if !okA || !okB || ca.Rule != storedversion.Unstated || cb.Rule != storedversion.Unstated || len(ca.Fields) != len(cb.Fields) {
		return false
	}
	for i := range ca.Fields {
		if ca.Fields[i] != cb.Fields[i] {
			return false
		}
	}
	return true
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
