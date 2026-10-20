package bigboyrender

import (
	"fmt"
	"io"
	"sort"
	"strings"
)

// grantsCompare: `grants-compare <actual-grants-tsv> <authorization.go source>` -- both are the TEXT, not paths.
// Prints the bigboy_grants_check summary and GRANTS_CHECK_OK or GRANTS_CHECK_FAIL; exit 0 on a match, else 1.
func grantsCompare(args []string, stdout, stderr io.Writer) int {
	if len(args) < 2 {
		fmt.Fprintln(stderr, "usage: cigate bigboy grants-compare <actual-grants-tsv> <authorization.go source>")
		return 1
	}
	pairs, ok := chManifest(args[1])
	if !ok {
		// Python stopped here on an IndexError (no func APIPosture): nothing on stdout, the error on stderr, exit 1.
		fmt.Fprintln(stderr, "GRANTS_CHECK_FAIL manifest not parsed: no func APIPosture")
		return 1
	}
	expected := map[string][]string{}
	for _, g := range pairs {
		expected[g.privilege+"\t"+g.table] = []string{g.privilege, g.table}
	}
	if len(expected) == 0 {
		fmt.Fprintln(stdout, "GRANTS_CHECK_FAIL manifest not parsed")
		return 1
	}
	actual := map[string][]string{}
	for _, line := range pyLines(args[0]) {
		if strings.Contains(line, "\t") {
			actual[line] = strings.Split(line, "\t")
		}
	}
	missing := difference(expected, actual)
	extra := difference(actual, expected)
	fmt.Fprintf(stdout, "bigboy_grants_check expected=%d actual=%d missing=%s extra=%s\n", len(expected), len(actual), pyTuples(missing), pyTuples(extra))
	if len(missing) == 0 && len(extra) == 0 {
		fmt.Fprintln(stdout, "GRANTS_CHECK_OK")
		return 0
	}
	fmt.Fprintln(stdout, "GRANTS_CHECK_FAIL")
	return 1
}

// difference is sorted(a - b), tuples compared element by element.
func difference(a, b map[string][]string) [][]string {
	var out [][]string
	for key, tuple := range a {
		if _, in := b[key]; !in {
			out = append(out, tuple)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		x, y := out[i], out[j]
		for k := 0; k < len(x) && k < len(y); k++ {
			if x[k] != y[k] {
				return pyLess(x[k], y[k])
			}
		}
		return len(x) < len(y)
	})
	return out
}

// pyTuples is the repr of a Python list of string tuples.
func pyTuples(list [][]string) string {
	parts := make([]string, len(list))
	for i, tuple := range list {
		items := make([]string, len(tuple))
		for k, item := range tuple {
			items[k] = pyRepr(item)
		}
		parts[i] = "(" + strings.Join(items, ", ") + ")"
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

// pyRepr is repr(str) for the strings a TSV line or a table name can hold. A rune Python's str.isprintable() refuses
// (the Unicode 15 table of unicode15.go) is escaped as \xhh, \uhhhh or \Uhhhhhhhh; a byte that is not UTF-8 is a lone
// surrogate in Python's argv and prints as \udcXX.
func pyRepr(s string) string {
	quote := '\''
	if strings.Contains(s, "'") && !strings.Contains(s, `"`) {
		quote = '"'
	}
	var b strings.Builder
	b.WriteRune(quote)
	for _, r := range surrogateEscape(s) {
		switch {
		case r == '\\':
			b.WriteString(`\\`)
		case r == quote:
			b.WriteString(`\` + string(r))
		case r == '\n':
			b.WriteString(`\n`)
		case r == '\r':
			b.WriteString(`\r`)
		case r == '\t':
			b.WriteString(`\t`)
		case r == ' ' || r > ' ' && r != 0x7f && inRanges(pyPrintRanges, r):
			b.WriteRune(r)
		case r <= 0xff:
			fmt.Fprintf(&b, `\x%02x`, r)
		case r <= 0xffff:
			fmt.Fprintf(&b, `\u%04x`, r)
		default:
			fmt.Fprintf(&b, `\U%08x`, r)
		}
	}
	b.WriteRune(quote)
	return b.String()
}

// pyLess compares two strings by code point as Python does (an invalid byte is the surrogate U+DC00+byte).
func pyLess(a, b string) bool {
	x, y := surrogateEscape(a), surrogateEscape(b)
	for i := 0; i < len(x) && i < len(y); i++ {
		if x[i] != y[i] {
			return x[i] < y[i]
		}
	}
	return len(x) < len(y)
}
