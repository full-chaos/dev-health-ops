package docstools

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"unicode"
	"unicode/utf8"
)

// a1Traceback is where Python died on an uncaught exception: one line "Kind: message" on stderr (the exception
// line of the traceback, without the stack), exit code 1. The parity cases pin that line (stderr.traceback).
func a1Traceback(stderr io.Writer, kind, msg string) int {
	fmt.Fprintf(stderr, "%s: %s\n", kind, msg)
	return 1
}

// a1Exception is a Python exception of a given kind.
type a1Exception struct{ kind, msg string }

func (e a1Exception) Error() string { return e.kind + ": " + e.msg }

// a1DecodeError is Python's UnicodeDecodeError: the file is not UTF-8.
type a1DecodeError struct{ path string }

func (e a1DecodeError) Error() string { return e.path + ": invalid UTF-8" }

// a1ReadFail prints the one stderr line for a failed a1ReadText and returns exit code 1: the UnicodeDecodeError
// line for a file that is not UTF-8, else "<prefix> <error>".
func a1ReadFail(stderr io.Writer, prefix string, err error) int {
	var de a1DecodeError
	if errors.As(err, &de) {
		return a1Traceback(stderr, "UnicodeDecodeError", de.Error())
	}
	fmt.Fprintln(stderr, prefix, err)
	return 1
}

// a1ReadText reads a file the way Path.read_text does (UTF-8, universal newlines). A file that is not valid UTF-8
// gives an a1DecodeError.
func a1ReadText(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	if !utf8.Valid(raw) {
		return "", a1DecodeError{path}
	}
	s := string(raw)
	if strings.Contains(s, "\r") {
		s = strings.ReplaceAll(s, "\r\n", "\n")
		s = strings.ReplaceAll(s, "\r", "\n")
	}
	return s, nil
}

// a1PathStr renders a path as str(Path(p)) does: no empty or "." parts, no trailing slash.
func a1PathStr(p string) string {
	var parts []string
	for _, c := range strings.Split(p, "/") {
		if c != "" && c != "." {
			parts = append(parts, c)
		}
	}
	out := strings.Join(parts, "/")
	if strings.HasPrefix(p, "/") {
		return "/" + out
	}
	if out == "" {
		return "."
	}
	return out
}

// a1PathParts splits a path like Path.parts (without the anchor handling beyond a leading "/").
func a1PathParts(p string) []string {
	s := a1PathStr(p)
	var parts []string
	if strings.HasPrefix(s, "/") {
		parts = append(parts, "/")
	}
	for _, c := range strings.Split(s, "/") {
		if c != "" {
			parts = append(parts, c)
		}
	}
	return parts
}

// a1PartsLess orders paths the way Path < Path does (component-wise).
func a1PartsLess(a, b string) bool {
	pa, pb := a1PathParts(a), a1PathParts(b)
	for i := 0; i < len(pa) && i < len(pb); i++ {
		if pa[i] != pb[i] {
			return pa[i] < pb[i]
		}
	}
	return len(pa) < len(pb)
}

// a1Repr is repr() of a Python str.
func a1Repr(s string) string {
	q := byte('\'')
	if strings.Contains(s, "'") && !strings.Contains(s, `"`) {
		q = '"'
	}
	var b strings.Builder
	b.WriteByte(q)
	for _, r := range s {
		switch {
		case r == rune(q) || r == '\\':
			b.WriteByte('\\')
			b.WriteRune(r)
		case r == '\n':
			b.WriteString(`\n`)
		case r == '\r':
			b.WriteString(`\r`)
		case r == '\t':
			b.WriteString(`\t`)
		case r < 0x20 || r == 0x7f:
			fmt.Fprintf(&b, `\x%02x`, r)
		case r < 0x7f || unicode.IsPrint(r):
			b.WriteRune(r)
		case r < 0x100:
			fmt.Fprintf(&b, `\x%02x`, r)
		case r < 0x10000:
			fmt.Fprintf(&b, `\u%04x`, r)
		default:
			fmt.Fprintf(&b, `\U%08x`, r)
		}
	}
	b.WriteByte(q)
	return b.String()
}

func a1ListRepr(items []string) string {
	parts := make([]string, len(items))
	for i, it := range items {
		parts[i] = a1Repr(it)
	}
	return "[" + strings.Join(parts, ", ") + "]"
}
