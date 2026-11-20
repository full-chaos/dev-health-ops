package providersync

import (
	"fmt"
	"testing"
	"time"
)

func coberturaLinesCell(t *testing.T, body string) string {
	t.Helper()
	row, err := parseCoberturaRow([]byte(body), "c.xml", "a", "r", "1", "o", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	show := func(v *int64) string {
		if v == nil {
			return "NULL"
		}
		return fmt.Sprint(*v)
	}
	return fmt.Sprintf("lines %s/%s br %s/%s", show(row.LinesTotal), show(row.LinesCovered), show(row.BranchesTotal), show(row.BranchesCovered))
}

// CHAOS-14303: a file holds each line number once whatever its source (class lines, method lines, a repeated filename);
// hits are the maximum over its entries, so the element order never decides.
func TestCoberturaLineSourcesDedupeByFileAndLineNumber(t *testing.T) {
	wrap := func(classes string) string {
		return `<coverage><packages><package name="e"><classes>` + classes + `</classes></package></packages></coverage>`
	}
	methods := func(lines string) string {
		return `<methods><method name="m"><lines>` + lines + `</lines></method></methods>`
	}
	class := func(file, inner string) string { return `<class filename="` + file + `">` + inner + `</class>` }
	lines := func(l string) string { return `<lines>` + l + `</lines>` }
	cells := []struct{ name, body, want string }{
		{"method line not in class lines", wrap(class("foo.go", methods(`<line number="2" hits="0"/>`)+lines(`<line number="1" hits="1"/>`))), "lines 2/1 br NULL/NULL"},
		{"method line repeats class line", wrap(class("foo.go", methods(`<line number="1" hits="1"/>`)+lines(`<line number="1" hits="1"/><line number="2" hits="0"/>`))), "lines 2/1 br NULL/NULL"},
		{"method lines only", wrap(class("foo.go", methods(`<line number="1" hits="1"/><line number="2" hits="0"/>`))), "lines 2/1 br NULL/NULL"},
		{"method line without number is ignored", wrap(class("foo.go", methods(`<line hits="1"/>`)+lines(`<line number="1" hits="0"/>`))), "lines 1/0 br NULL/NULL"},
		{"method hit on a line the class shows as 0", wrap(class("foo.go", methods(`<line number="1" hits="3"/>`)+lines(`<line number="1" hits="0"/>`))), "lines 1/1 br NULL/NULL"},
		{"same filename, two classes, other lines", wrap(class("foo.go", lines(`<line number="1" hits="1"/>`)) + class("foo.go", lines(`<line number="2" hits="0"/>`))), "lines 2/1 br NULL/NULL"},
		{"same filename, same line, hit in the later class", wrap(class("foo.go", lines(`<line number="1" hits="0"/>`)) + class("foo.go", lines(`<line number="1" hits="2"/>`))), "lines 1/1 br NULL/NULL"},
		{"same filename, same line, hit in the earlier class", wrap(class("foo.go", lines(`<line number="1" hits="2"/>`)) + class("foo.go", lines(`<line number="1" hits="0"/>`))), "lines 1/1 br NULL/NULL"},
		{"same filename, later class empty", wrap(class("foo.go", lines(`<line number="1" hits="1"/>`)) + class("foo.go", `<lines/>`)), "lines 1/1 br NULL/NULL"},
		{"same filename, earlier class empty", wrap(class("foo.go", `<lines/>`) + class("foo.go", lines(`<line number="1" hits="1"/>`))), "lines 1/1 br NULL/NULL"},
		{"empty filename", wrap(class("", lines(`<line number="1" hits="1"/><line number="2" hits="0"/>`))), "lines 2/1 br NULL/NULL"},
		{"two empty filenames, same line number, count apart", wrap(class("", lines(`<line number="1" hits="1"/>`)) + class("", lines(`<line number="1" hits="0"/>`))), "lines 2/1 br NULL/NULL"},
		{"branch pair repeated on a method line counts once", wrap(class("foo.go", methods(`<line number="1" hits="1" branch="true" condition-coverage="50% (1/2)"/>`)+lines(`<line number="1" hits="1" branch="true" condition-coverage="50% (1/2)"/>`))), "lines 1/1 br 2/1"},
		{"control: class-only report", wrap(class("foo.go", lines(`<line number="1" hits="1" branch="true" condition-coverage="50% (1/2)"/><line number="2" hits="0"/>`)) + class("bar.go", lines(`<line number="1" hits="0"/>`))), "lines 3/1 br 2/1"},
	}
	for _, cell := range cells {
		if got := coberturaLinesCell(t, cell.body); got != cell.want {
			t.Errorf("CELL %s: stored %s, want %s", cell.name, got, cell.want)
		}
	}
}
