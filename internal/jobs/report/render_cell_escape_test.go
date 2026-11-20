package report

import (
	"strings"
	"testing"
)

// unescapedPipes counts the "|" of a Markdown table row that are not escaped
// with a backslash (a GFM table splits its cells there).
func unescapedPipes(row string) int {
	n := 0
	for i := 0; i < len(row); i++ {
		if row[i] == '\\' {
			i++
			continue
		}
		if row[i] == '|' {
			n++
		}
	}
	return n
}

// cellText is what a CommonMark reader shows for a cell: each backslash escape
// of an ASCII punctuation character is that character.
func cellText(cell string) string {
	var b strings.Builder
	for i := 0; i < len(cell); i++ {
		if cell[i] == '\\' && i+1 < len(cell) && strings.ContainsRune("!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~", rune(cell[i+1])) {
			i++
		}
		b.WriteByte(cell[i])
	}
	return b.String()
}

// splitRow splits "| a | b | c |" on its unescaped pipes.
func splitRow(row string) []string {
	var cells []string
	start := 0
	for i := 0; i < len(row); i++ {
		if row[i] == '\\' {
			i++
			continue
		}
		if row[i] == '|' {
			cells = append(cells, row[start:i])
			start = i + 1
		}
	}
	return cells[1:]
}

// A chart label is text: its x and group cells stay one cell each (no "|" or
// line break splits the row), and Markdown inline syntax in it is escaped, so
// a reader shows the label as it is (a line break as a space).
func TestRenderChartEscapesLabelCells(t *testing.T) {
	labels := []string{
		"billing | checkout", "line one\nline two", "a\r\nb", "*old*", "_old_", "`code`",
		"[link](https://example.invalid)", "<b>bold</b>", `back\slash`, `a\*b`, "~~gone~~", "a & b", "a # b", "## Provenance", "### Provenance", "plain-label/path.1",
	}
	for _, label := range labels {
		t.Run(label, func(t *testing.T) {
			out := renderChart(ChartResult{Title: "Chart", DataPoints: []DataPoint{{X: label, Y: 5, Group: label}}})
			lines := strings.Split(out, "\n")
			if len(lines) != 5 {
				t.Fatalf("chart has %d lines, want 5 (title, blank, header, rule, one row):\n%s", len(lines), out)
			}
			row := lines[4]
			if n := unescapedPipes(row); n != 4 {
				t.Fatalf("row %q has %d unescaped pipes, want 4 (3 cells)", row, n)
			}
			cells := splitRow(row)
			want := strings.NewReplacer("\r\n", " ", "\n", " ", "\r", " ").Replace(label)
			for _, i := range []int{0, 2} {
				cell := strings.TrimSpace(cells[i])
				if got := cellText(cell); got != want {
					t.Errorf("cell %d %q reads %q, want %q", i, cell, got, want)
				}
				for j := 0; j < len(cell); j++ {
					if cell[j] == '\\' {
						j++
						continue
					}
					if strings.IndexByte("*_`[]<>~&#", cell[j]) >= 0 {
						t.Errorf("cell %d %q has an unescaped %q", i, cell, cell[j])
					}
				}
			}
			if got := strings.TrimSpace(cells[1]); got != "5" {
				t.Errorf("y cell %q, want 5", got)
			}
		})
	}
}
