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
		return 2
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
				return x[k] < y[k]
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

// pyRepr is repr(str) for the strings a TSV line or a table name can hold.
func pyRepr(s string) string {
	quote := byte('\'')
	if strings.Contains(s, "'") && !strings.Contains(s, `"`) {
		quote = '"'
	}
	var b strings.Builder
	b.WriteByte(quote)
	for _, r := range s {
		switch {
		case r == '\\':
			b.WriteString(`\\`)
		case r == rune(quote):
			b.WriteString(`\` + string(r))
		case r == '\n':
			b.WriteString(`\n`)
		case r == '\r':
			b.WriteString(`\r`)
		case r == '\t':
			b.WriteString(`\t`)
		case r < 0x20 || r == 0x7f:
			fmt.Fprintf(&b, `\x%02x`, r)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte(quote)
	return b.String()
}
