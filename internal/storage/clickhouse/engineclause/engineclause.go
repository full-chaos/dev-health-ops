// Package engineclause parses a ClickHouse MergeTree engine clause the same way on
// self-hosted, Replicated* and ClickHouse Cloud Shared* servers.
//
// Self-hosted:  ReplacingMergeTree(computed_at)
// Replicated:   ReplicatedReplacingMergeTree('/path', '{replica}', computed_at)
// Cloud:        SharedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', computed_at)
//
// All three have the same family (ReplacingMergeTree) and the same arguments
// (computed_at) once the two replication arguments are dropped. An unknown shape
// is refused with ErrShape, never passed.
package engineclause

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// ErrShape means the text is not a MergeTree engine clause this package knows.
var ErrShape = errors.New("engineclause: not a recognised MergeTree engine clause")

const (
	prefixShared     = "Shared"
	prefixReplicated = "Replicated"
	suffixMergeTree  = "MergeTree"

	// FamilyReplacing is the family whose rows collapse under FINAL.
	FamilyReplacing = "ReplacingMergeTree"
)

// Clause is one parsed engine clause.
type Clause struct {
	// Name is the engine name as the server reports it, e.g. SharedReplacingMergeTree.
	Name string
	// Family is Name without a leading Shared or Replicated, e.g. ReplacingMergeTree.
	Family string
	// Prefix is "Shared", "Replicated" or "".
	Prefix string
	// Args are the engine arguments with the two replication arguments dropped,
	// trimmed, quotes kept.
	Args []string
}

// Parse reads the engine clause at the start of s. s may be the bare
// system.tables.engine value or the start of engine_full; text after the closing
// parenthesis (ORDER BY ..., SETTINGS ...) is ignored.
func Parse(s string) (Clause, error) {
	clause, _, err := parsePrefix(s)
	return clause, err
}

// VersionColumn returns the ReplacingMergeTree version column, or "" when the
// clause names none. It refuses a clause of another family, a version argument
// that is not a plain column name, and any argument after the version (the
// is_deleted form changes what FINAL returns).
func (c Clause) VersionColumn() (string, error) {
	if c.Family != FamilyReplacing {
		return "", fmt.Errorf("%w: %s is not %s", ErrShape, c.Name, FamilyReplacing)
	}
	if len(c.Args) == 0 {
		return "", nil
	}
	if len(c.Args) > 1 {
		return "", fmt.Errorf("%w: %s has arguments after the version column: %q", ErrShape, c.Name, strings.Join(c.Args, ", "))
	}
	column, ok := Identifier(c.Args[0])
	if !ok {
		return "", fmt.Errorf("%w: version argument %q is not a column name", ErrShape, c.Args[0])
	}
	return column, nil
}

// IsReplacingOn reports whether the clause is ReplacingMergeTree(<column>) with
// that one argument and nothing else.
func (c Clause) IsReplacingOn(column string) bool {
	if c.Family != FamilyReplacing || len(c.Args) != 1 {
		return false
	}
	got, ok := Identifier(c.Args[0])
	return ok && got == column
}

var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// Identifier returns the column name of arg, unquoting one layer of backticks or
// double quotes. ok is false for anything that is not a plain column name.
func Identifier(arg string) (string, bool) {
	arg = strings.TrimSpace(arg)
	if len(arg) >= 2 && (arg[0] == '`' && arg[len(arg)-1] == '`' || arg[0] == '"' && arg[len(arg)-1] == '"') {
		arg = arg[1 : len(arg)-1]
	}
	if !identifierPattern.MatchString(arg) {
		return "", false
	}
	return arg, true
}

// CanonicalizeDDL rewrites the ENGINE clause of a SHOW CREATE TABLE text from
// Shared*/Replicated*('path', 'replica', args...) to Family(args...), so a
// substring check written for the self-hosted form holds on every server. A text
// with no Shared or Replicated engine, or with no ENGINE clause, is returned
// unchanged. A Shared or Replicated clause that does not parse is refused.
func CanonicalizeDDL(ddl string) (string, error) {
	at := engineKeyword.FindStringIndex(ddl)
	if at == nil {
		return ddl, nil
	}
	clause, end, err := parsePrefix(ddl[at[1]:])
	if err != nil {
		return "", err
	}
	if clause.Prefix == "" {
		return ddl, nil
	}
	rewritten := clause.Family
	if len(clause.Args) > 0 {
		rewritten += "(" + strings.Join(clause.Args, ", ") + ")"
	}
	return ddl[:at[1]] + rewritten + ddl[at[1]+end:], nil
}

var engineKeyword = regexp.MustCompile(`\bENGINE\s*=\s*`)

func parsePrefix(s string) (Clause, int, error) {
	start := 0
	for start < len(s) && isSpace(s[start]) {
		start++
	}
	i := start
	for i < len(s) && isNameByte(s[i]) {
		i++
	}
	name := s[start:i]
	if name == "" {
		return Clause{}, 0, fmt.Errorf("%w: no engine name in %q", ErrShape, abbreviate(s))
	}
	clause := Clause{Name: name, Family: name}
	switch {
	case strings.HasPrefix(name, prefixShared) && len(name) > len(prefixShared):
		clause.Prefix, clause.Family = prefixShared, name[len(prefixShared):]
	case strings.HasPrefix(name, prefixReplicated) && len(name) > len(prefixReplicated):
		clause.Prefix, clause.Family = prefixReplicated, name[len(prefixReplicated):]
	}
	if !strings.HasSuffix(clause.Family, suffixMergeTree) {
		return Clause{}, 0, fmt.Errorf("%w: engine %q", ErrShape, name)
	}

	open := i
	for open < len(s) && isSpace(s[open]) {
		open++
	}
	if open >= len(s) || s[open] != '(' {
		if i < len(s) && !isSpace(s[i]) {
			return Clause{}, 0, fmt.Errorf("%w: %q follows engine %q", ErrShape, abbreviate(s[i:]), name)
		}
		return clause, i, nil
	}

	args, end, err := splitArgs(s, open+1)
	if err != nil {
		return Clause{}, 0, err
	}
	if clause.Prefix != "" && len(args) > 0 {
		if len(args) < 2 || !isStringLiteral(args[0]) || !isStringLiteral(args[1]) {
			return Clause{}, 0, fmt.Errorf("%w: %s must start with two quoted replication arguments, got %q",
				ErrShape, name, strings.Join(args, ", "))
		}
		args = args[2:]
		if len(args) == 0 {
			args = nil
		}
	}
	clause.Args = args
	return clause, end, nil
}

// splitArgs reads comma separated arguments from s[from:] up to the closing
// parenthesis and returns them with the offset just past it.
func splitArgs(s string, from int) ([]string, int, error) {
	var args []string
	depth := 0
	argStart := from
	flush := func(upTo int, closing bool) error {
		arg := strings.TrimSpace(s[argStart:upTo])
		if arg == "" {
			if closing && len(args) == 0 && strings.TrimSpace(s[from:upTo]) == "" {
				return nil
			}
			return fmt.Errorf("%w: empty engine argument", ErrShape)
		}
		args = append(args, arg)
		return nil
	}
	for i := from; i < len(s); i++ {
		switch c := s[i]; c {
		case '\'', '`', '"':
			end, ok := skipQuoted(s, i)
			if !ok {
				return nil, 0, fmt.Errorf("%w: unterminated quote in engine arguments", ErrShape)
			}
			i = end
		case '(', '[':
			depth++
		case ']':
			depth--
			if depth < 0 {
				return nil, 0, fmt.Errorf("%w: unbalanced bracket in engine arguments", ErrShape)
			}
		case ')':
			if depth == 0 {
				if err := flush(i, true); err != nil {
					return nil, 0, err
				}
				return args, i + 1, nil
			}
			depth--
		case ',':
			if depth == 0 {
				if err := flush(i, false); err != nil {
					return nil, 0, err
				}
				argStart = i + 1
			}
		}
	}
	return nil, 0, fmt.Errorf("%w: engine arguments are not closed", ErrShape)
}

// skipQuoted returns the index of the closing quote of the literal opened at
// s[open]. A doubled quote and a backslash escape stay inside the literal.
func skipQuoted(s string, open int) (int, bool) {
	quote := s[open]
	for i := open + 1; i < len(s); i++ {
		switch {
		case s[i] == '\\':
			i++
		case s[i] == quote:
			if i+1 < len(s) && s[i+1] == quote {
				i++
				continue
			}
			return i, true
		}
	}
	return 0, false
}

func isStringLiteral(arg string) bool {
	return len(arg) >= 2 && arg[0] == '\'' && arg[len(arg)-1] == '\''
}

func isSpace(b byte) bool { return b == ' ' || b == '\t' || b == '\n' || b == '\r' }

func isNameByte(b byte) bool {
	return b >= 'A' && b <= 'Z' || b >= 'a' && b <= 'z' || b >= '0' && b <= '9' || b == '_'
}

func abbreviate(s string) string {
	if len(s) > 40 {
		return s[:40] + "..."
	}
	return s
}
