package chmigrate

import "strings"

// StripLineComments removes `-- ...` line comments, ignoring `--` inside
// single- or double-quoted strings. Block comments are left untouched. It is
// a port of strip_line_comments in
// src/dev_health_ops/migrations/clickhouse/__init__.py; the split oracle test
// runs both over every real migration file.
func StripLineComments(sql string) string {
	lines := splitLines(sql)
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		runes := []rune(line)
		inSingle, inDouble := false, false
		cut := len(runes)
		for i := 0; i < len(runes); i++ {
			switch ch := runes[i]; {
			case ch == '\'' && !inDouble:
				inSingle = !inSingle
			case ch == '"' && !inSingle:
				inDouble = !inDouble
			case ch == '-' && i+1 < len(runes) && runes[i+1] == '-' && !inSingle && !inDouble:
				cut = i
			}
			if cut != len(runes) {
				break
			}
		}
		out = append(out, string(runes[:cut]))
	}
	return strings.Join(out, "\n")
}

// SplitStatements splits one migration file into its statements: line
// comments are stripped first, so a `;` inside a comment is harmless, then the
// rest is split on `;`, each fragment is trimmed, and empty fragments are
// dropped. A port of split_sql_statements.
func SplitStatements(sql string) []string {
	var statements []string
	for _, fragment := range strings.Split(StripLineComments(sql), ";") {
		if statement := strings.TrimSpace(fragment); statement != "" {
			statements = append(statements, statement)
		}
	}
	return statements
}

// splitLines matches Python's str.splitlines(): it breaks on \n, \r\n, \r and
// the other Unicode line boundaries, and a trailing line break yields no empty
// final line.
func splitLines(text string) []string {
	var lines []string
	runes := []rune(text)
	start := 0
	for i := 0; i < len(runes); i++ {
		switch runes[i] {
		case '\r':
			lines = append(lines, string(runes[start:i]))
			if i+1 < len(runes) && runes[i+1] == '\n' {
				i++
			}
			start = i + 1
		case '\n', '\v', '\f', '\x1c', '\x1d', '\x1e', '\x85', ' ', ' ':
			lines = append(lines, string(runes[start:i]))
			start = i + 1
		}
	}
	if start < len(runes) {
		lines = append(lines, string(runes[start:]))
	}
	return lines
}
