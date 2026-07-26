package domaingrants

import (
	"strings"
)

// A real parser for PostgreSQL's LOCK statement, replacing a regex that
// recognised exactly ONE of its nine accepted shapes.
//
// The grammar (all forms below verified accepted by PostgreSQL 18.4):
//
//	LOCK [ TABLE ] [ ONLY ] name [ * ] [, [ONLY] name [*] ...] [ IN <mode> MODE ] [ NOWAIT ]
//
// The regex required the TABLE keyword, a single target, and an explicit mode.
// So every one of these derived NOTHING -- no table, no privilege, no diagnostic:
//
//	LOCK public.t IN SHARE ROW EXCLUSIVE MODE          -- TABLE omitted
//	LOCK TABLE public.a, public.b IN ... MODE          -- multiple targets
//	LOCK TABLE public.t                                -- mode omitted (= ACCESS EXCLUSIVE)
//	LOCK TABLE public.t *                              -- descendant marker
//
// `LOCK public.a, public.b` in coordinator-only code would derive neither table
// nor privilege, CI would pass, and production would return 42501. Silently
// ignoring input it does not recognise is the worst possible shape for a tool
// whose entire job is to fail closed, so an unrecognised LOCK is now a RECORDED
// FACT that fails the gate rather than an absence.

// lockStatement is one successfully parsed LOCK.
type lockStatement struct {
	// Targets are the raw (possibly schema-qualified) names, in source order.
	Targets []string
	// Mode is the normalized mode, defaulted when the statement omits it.
	Mode string
}

// quotedTargetRefusal explains why a quoted lock target is refused rather than
// resolved. It is a REFUSAL, not a skip, and that distinction is the point.
//
// PostgreSQL's quoting rules make a quoted identifier opaque: `"private.outbox"`
// is ONE relation whose name contains a dot, and `"Outbox"` is a DIFFERENT
// relation from `outbox`. Downstream, splitSchemaQualified reads embedded dots as
// a schema separator and table keys are lowercased -- so a quoted target would be
// re-interpreted as schema `private` (then dropped as out-of-scope, silently) or
// folded into `outbox`. A posture covering the misidentified table then passes
// while the real target holds no privileges.
//
// The mis-parse is not the defect; the SILENT SKIP is. This analyzer cannot
// represent a quoted identifier faithfully, so it says so instead of guessing,
// and the statement lands in UnparsedLocks where a reader can see it.
const quotedTargetRefusal = "quoted lock target cannot be resolved faithfully " +
	"(quoting makes dots and case significant, which this analyzer's schema-splitting " +
	"and lower-casing would silently reinterpret)"

// lockModeWords are the tokens that may appear inside a mode name. Parsing stops
// at MODE, so this only has to bound the scan.
var lockModeWords = map[string]bool{
	"ACCESS": true, "SHARE": true, "ROW": true, "EXCLUSIVE": true, "UPDATE": true,
}

// parseLockStatements finds every statement-initial LOCK in clean and returns the
// ones it fully understood plus the verbatim text of the ones it did not.
//
// Statement-initial means preceded (ignoring whitespace) by nothing or by `;`.
// That anchoring keeps the word LOCK inside a string literal or an identifier
// from being read as a statement -- important now that an unparsed LOCK FAILS the
// gate, because a false positive here would be a false failure.
func parseLockStatements(clean string) (statements []lockStatement, unparsed []string) {
	upper := strings.ToUpper(clean)
	for index := 0; index < len(upper); {
		found := strings.Index(upper[index:], "LOCK")
		if found < 0 {
			break
		}
		at := index + found
		index = at + 4

		if !isStatementStart(upper, at) || !isWordEnd(upper, at+4) {
			continue
		}
		statement, rest, ok := parseOneLock(clean[at+4:])
		if !ok {
			unparsed = append(unparsed, statementText(clean[at:]))
			// Skip past this statement so its tail is not re-scanned.
			if end := strings.IndexByte(clean[at:], ';'); end >= 0 {
				index = at + end
			}
			continue
		}
		statements = append(statements, statement)
		index = len(clean) - len(rest)
	}
	return statements, unparsed
}

// isStatementStart reports whether position at begins a statement: only
// whitespace back to either the start of the text or a semicolon.
func isStatementStart(upper string, at int) bool {
	for i := at - 1; i >= 0; i-- {
		switch upper[i] {
		case ' ', '\t', '\n', '\r':
			continue
		case ';':
			return true
		default:
			return false
		}
	}
	return true
}

func isWordEnd(upper string, at int) bool {
	if at >= len(upper) {
		return true
	}
	c := upper[at]
	return !(c == '_' || (c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z'))
}

// statementText returns the LOCK statement's text for reporting, bounded so a
// diagnostic stays readable.
func statementText(from string) string {
	text := from
	if end := strings.IndexByte(text, ';'); end >= 0 {
		text = text[:end]
	}
	text = strings.Join(strings.Fields(text), " ")
	const maxLen = 120
	if len(text) > maxLen {
		text = text[:maxLen] + "..."
	}
	return text
}

// parseOneLock parses the remainder after the LOCK keyword.
func parseOneLock(after string) (lockStatement, string, bool) {
	rest := after
	tok, rest, ok := nextToken(rest)
	if !ok {
		return lockStatement{}, after, false
	}
	if strings.EqualFold(tok, "TABLE") {
		tok, rest, ok = nextToken(rest)
		if !ok {
			return lockStatement{}, after, false
		}
	}

	statement := lockStatement{Mode: lockDefaultMode}
	for {
		if strings.EqualFold(tok, "ONLY") {
			if tok, rest, ok = nextToken(rest); !ok {
				return lockStatement{}, after, false
			}
		}
		if !isNameToken(tok) {
			return lockStatement{}, after, false
		}
		if strings.HasPrefix(tok, quotedTokenPrefix) {
			// Refused, not skipped -- see quotedTargetRefusal.
			return lockStatement{}, after, false
		}
		statement.Targets = append(statement.Targets, tok)

		// Optional descendant marker, then either another target or the end of
		// the target list.
		peek, peekRest, havePeek := nextToken(rest)
		if havePeek && peek == "*" {
			rest = peekRest
			peek, peekRest, havePeek = nextToken(rest)
		}
		if havePeek && peek == "," {
			if tok, rest, ok = nextToken(peekRest); !ok {
				return lockStatement{}, after, false
			}
			continue
		}
		if !havePeek {
			return statement, rest, true // no mode clause: default stands
		}
		tok, rest = peek, peekRest
		break
	}

	if !strings.EqualFold(tok, "IN") {
		// Only NOWAIT or a statement terminator may follow the target list.
		if strings.EqualFold(tok, "NOWAIT") || tok == ";" {
			return statement, rest, true
		}
		return lockStatement{}, after, false
	}

	var modeWords []string
	for {
		tok, rest, ok = nextToken(rest)
		if !ok {
			return lockStatement{}, after, false
		}
		if strings.EqualFold(tok, "MODE") {
			break
		}
		if !lockModeWords[strings.ToUpper(tok)] {
			// An unrecognised word inside the mode clause. Refuse the whole
			// statement rather than guess a mode -- see lockRequirementForMode.
			return lockStatement{}, after, false
		}
		modeWords = append(modeWords, strings.ToUpper(tok))
	}
	if len(modeWords) == 0 {
		return lockStatement{}, after, false
	}
	statement.Mode = normalizeLockMode(strings.Join(modeWords, " "))

	if peek, peekRest, havePeek := nextToken(rest); havePeek {
		if strings.EqualFold(peek, "NOWAIT") {
			rest = peekRest
		} else if peek != ";" {
			return lockStatement{}, after, false
		}
	}
	return statement, rest, true
}

// nextToken returns the next SQL token: a punctuation character, a quoted
// identifier, or a run of identifier characters (dots included, so a qualified
// name arrives whole).
// quotedTokenPrefix marks a token that arrived double-quoted. A byte that cannot
// appear in an unquoted SQL identifier, so it cannot collide with a real name.
const quotedTokenPrefix = "\x00quoted:"

func nextToken(s string) (string, string, bool) {
	i := 0
	for i < len(s) && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r') {
		i++
	}
	if i >= len(s) {
		return "", "", false
	}
	switch s[i] {
	case ',', '*', ';':
		return string(s[i]), s[i+1:], true
	case '"':
		end := strings.IndexByte(s[i+1:], '"')
		if end < 0 {
			return "", "", false
		}
		// The caller needs to know this was quoted; signal it with a sentinel
		// prefix that cannot occur in an unquoted identifier.
		return quotedTokenPrefix + s[i+1:i+1+end], s[i+2+end:], true
	}
	start := i
	for i < len(s) && (isIdentChar(s[i]) || s[i] == '.') {
		i++
	}
	if i == start {
		return string(s[start]), s[start+1:], true
	}
	return s[start:i], s[i:], true
}

func isIdentChar(c byte) bool {
	return c == '_' || (c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
}

func isNameToken(tok string) bool {
	if strings.HasPrefix(tok, quotedTokenPrefix) {
		return len(tok) > len(quotedTokenPrefix)
	}
	if tok == "" {
		return false
	}
	for i := 0; i < len(tok); i++ {
		if !isIdentChar(tok[i]) && tok[i] != '.' {
			return false
		}
	}
	return isIdentChar(tok[0]) || tok[0] == '"'
}
