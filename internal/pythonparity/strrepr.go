package pythonparity

import (
	"sort"
	"strconv"
	"strings"
)

// IsPrintable is CPython's str.isprintable() for one code point, from the
// generated pythonNonPrintableRuns table (Go's unicode.IsPrint tracks a
// different Unicode version and differs on spaces).
func IsPrintable(r rune) bool {
	index := sort.Search(len(pythonNonPrintableRuns), func(i int) bool { return pythonNonPrintableRuns[i][1] >= r })
	return index == len(pythonNonPrintableRuns) || r < pythonNonPrintableRuns[index][0]
}

// StrRepr is repr() of a Python str: single quotes, or double quotes when
// the text holds a single quote and no double quote; backslash, the active
// quote, \t, \n and \r escaped; every other non-printable code point as
// \xhh, \uhhhh or \Uhhhhhhhh. Invalid UTF-8 bytes (which a Python str
// cannot hold) are written as U+FFFD, as Go decodes them.
func StrRepr(text string) string {
	quote := '\''
	if strings.ContainsRune(text, '\'') && !strings.ContainsRune(text, '"') {
		quote = '"'
	}
	var out strings.Builder
	out.WriteRune(quote)
	for _, r := range text {
		switch {
		case r == quote || r == '\\':
			out.WriteByte('\\')
			out.WriteRune(r)
		case r == '\t':
			out.WriteString(`\t`)
		case r == '\n':
			out.WriteString(`\n`)
		case r == '\r':
			out.WriteString(`\r`)
		case IsPrintable(r):
			out.WriteRune(r)
		case r < 0x100:
			out.WriteString(`\x` + hexPad(r, 2))
		case r < 0x10000:
			out.WriteString(`\u` + hexPad(r, 4))
		default:
			out.WriteString(`\U` + hexPad(r, 8))
		}
	}
	out.WriteRune(quote)
	return out.String()
}

func hexPad(r rune, width int) string {
	digits := strconv.FormatInt(int64(r), 16)
	return strings.Repeat("0", width-len(digits)) + digits
}
