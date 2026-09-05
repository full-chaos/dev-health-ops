// Package pycc computes Python cyclomatic complexity with the same numbers
// radon produces, so the `complexity` metrics family can run natively in Go
// (CHAOS-4291 / CHAOS-4971a) instead of on the Python compatibility bridge.
//
// # WHY A HAND-WRITTEN TOKENIZER
//
// radon's rules are defined over Python's AST, so the honest port is an AST
// walk. The three production images build with CGO_ENABLED=0
// (docker/go-worker.Dockerfile:15, docker/query-api.Dockerfile:25,
// docker/Dockerfile:29), which rules out tree-sitter and every other cgo
// Python grammar. A pure-Go Python parser is therefore the only route that
// can run inside the worker binary, and this file is its lexical half.
//
// # WHAT THIS TOKENIZER DOES AND DOES NOT PROMISE
//
// It is NOT a general Python front end and must never be described as one.
// It resolves exactly the lexical facts radon's complexity rules depend on:
//
//   - which NAME tokens are real keywords rather than text inside a string
//     or comment -- the single largest source of wrong complexity numbers,
//     because `# if x:` and `"if"` are both invisible to a naive scan;
//   - logical-line structure, so `if` at the start of a statement is
//     distinguishable from `if` in a ternary or a comprehension;
//   - INDENT/DEDENT, so a block's extent (and therefore which `else`
//     belongs to which opener) is recoverable without a full grammar.
//
// Expression-level structure is deliberately NOT built here. The complexity
// rules that need it (BoolOp's `len(values) - 1`, a comprehension's `ifs`)
// are counted from the token stream in cc.go, where the equivalence to
// radon's AST arithmetic is argued per rule rather than assumed.
package pycc

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// TokenKind classifies a token only as finely as the complexity rules need.
type TokenKind int

const (
	// TokenName is an identifier or keyword. Keyword-ness is a property of
	// the string, and for `match`/`case` it is contextual -- see cc.go.
	TokenName TokenKind = iota
	// TokenOp is any operator or delimiter, including brackets and the
	// colon that opens a suite.
	TokenOp
	// TokenNumber and TokenString are inert for complexity purposes; they
	// are emitted so bracket depth and line structure stay correct, and so
	// keyword-like text inside a literal can never be miscounted.
	TokenNumber
	TokenString
	// TokenNewline ends a LOGICAL line (never a line continued by a
	// backslash or by an open bracket).
	TokenNewline
	// TokenIndent and TokenDedent bracket a suite.
	TokenIndent
	TokenDedent
	// TokenEOF terminates the stream.
	TokenEOF
)

func (k TokenKind) String() string {
	switch k {
	case TokenName:
		return "NAME"
	case TokenOp:
		return "OP"
	case TokenNumber:
		return "NUMBER"
	case TokenString:
		return "STRING"
	case TokenNewline:
		return "NEWLINE"
	case TokenIndent:
		return "INDENT"
	case TokenDedent:
		return "DEDENT"
	case TokenEOF:
		return "EOF"
	}
	return "UNKNOWN"
}

// Token is one lexical unit. Line is 1-based to match Python's own
// numbering, so a divergence can be reported against the source the oracle
// saw rather than an off-by-one shadow of it.
type Token struct {
	Kind TokenKind
	Text string
	Line int
	// Col is the 0-based rune offset within the physical line, matching
	// ast.AST.col_offset, which radon records on every block.
	Col int
}

// ErrTokenize reports source this tokenizer cannot lex. radon's caller
// (`_analyze_python`, analytics/complexity.py:219) catches every exception
// from cc_visit and returns None, dropping the file from the day's scan
// entirely. Returning an error here lets the caller reproduce that exact
// skip rather than silently scoring a file zero -- a zero and a skip are
// very different rows downstream.
type ErrTokenize struct {
	Line int
	Msg  string
}

func (e *ErrTokenize) Error() string {
	return fmt.Sprintf("python tokenize error at line %d: %s", e.Line, e.Msg)
}

// pythonKeywords is Python 3.14's reserved word set (keyword.kwlist).
//
// `match`, `case` and `_` are NOT here: they are soft keywords, reserved
// only in the specific positions PEP 634 defines. Treating them as
// unconditional keywords would score every variable named `match` as a
// match statement, which is a real pattern in this codebase (regex results
// are routinely bound to `match`).
var pythonKeywords = map[string]bool{
	"False": true, "None": true, "True": true, "and": true, "as": true,
	"assert": true, "async": true, "await": true, "break": true,
	"class": true, "continue": true, "def": true, "del": true,
	"elif": true, "else": true, "except": true, "finally": true,
	"for": true, "from": true, "global": true, "if": true, "import": true,
	"in": true, "is": true, "lambda": true, "nonlocal": true, "not": true,
	"or": true, "pass": true, "raise": true, "return": true, "try": true,
	"while": true, "with": true, "yield": true,
}

// IsKeyword reports whether text is a hard (non-contextual) Python keyword.
func IsKeyword(text string) bool { return pythonKeywords[text] }

// stringPrefixes are the legal prefix letters on a string literal. They are
// matched case-insensitively and in any order, which is what Python allows
// (rb, bR, Rb ... are all valid), so a prefixed literal is never mistaken
// for a NAME followed by a separate string.
const stringPrefixes = "rbufRBUF"

type tokenizer struct {
	src    []rune
	pos    int
	line   int
	col    int
	tokens []Token
	// indents is the indentation stack, exactly as CPython's tokenizer
	// keeps it, EXCEPT each level records TWO widths -- indents[i][0] is
	// the column with tabs expanded to the next multiple of 8,
	// indents[i][1] is the column with every tab counted as width 1.
	// CPython compares indentation using both metrics and raises TabError
	// the moment they disagree on ordering (tokenizer.c's `indstack`/
	// `altindstack`); a single-metric comparison accepts inputs CPython
	// rejects, such as a dedent that lands on the same tabsize-8 column a
	// tab-indented line used but was reached with spaces instead of tabs.
	indents [][2]int
	// depth is bracket nesting. Inside brackets, newlines are implicit
	// continuations and indentation is not significant -- this is what
	// makes a multi-line call signature or a wrapped boolean expression
	// lex correctly.
	depth int
	// brackets is the stack of closers each open bracket expects, so
	// `(1]` is rejected instead of silently treated as balanced (Python
	// requires each closer to match its own opener, not just any closer).
	brackets []rune
	// atLineStart is true when the scanner is positioned where indentation
	// would be measured, i.e. after a logical newline and not inside
	// brackets.
	atLineStart bool
}

// Tokenize converts Python source into the token stream cc.go consumes.
//
// The stream is normalised so a consumer never has to special-case the end
// of input: every open suite is closed with a DEDENT and the stream always
// ends with exactly one EOF.
func Tokenize(src string) ([]Token, error) {
	// Python normalises line endings before tokenizing; doing the same here
	// keeps a CRLF file from producing a stray token at every line end and
	// keeps `loc` (a splitlines() count on the Python side) comparable.
	src = strings.ReplaceAll(src, "\r\n", "\n")
	src = strings.ReplaceAll(src, "\r", "\n")

	t := &tokenizer{
		src:         []rune(src),
		line:        1,
		indents:     [][2]int{{0, 0}},
		atLineStart: true,
	}
	if err := t.run(); err != nil {
		return nil, err
	}
	return t.tokens, nil
}

func (t *tokenizer) run() error {
	for {
		if t.atLineStart && t.depth == 0 {
			blank, err := t.handleLineStart()
			if err != nil {
				return err
			}
			if blank {
				continue
			}
		}
		if t.pos >= len(t.src) {
			break
		}

		c := t.src[t.pos]
		switch {
		case c == '#':
			t.skipComment()
		case c == '\n':
			t.consumeNewline()
		case c == '\\' && t.peekAt(1) == '\n':
			// Explicit line joiner: consume both, and do NOT emit a
			// NEWLINE or re-measure indentation. The logical line
			// continues, which is the whole point of the backslash.
			t.pos += 2
			t.line++
			t.col = 0
			if t.pos >= len(t.src) {
				// A continuation promises MORE tokens complete the
				// logical line; EOF right after it means the file ends
				// mid-statement (an empty continued line, in the given
				// repro). CPython raises "unexpected EOF while parsing"
				// here -- a plain trailing-newline close (the case just
				// above, with nothing after it) is fine because it isn't
				// promising a continuation.
				return &ErrTokenize{
					Line: t.line,
					Msg:  "unexpected EOF while parsing",
				}
			}
		case c == '\\':
			// A backslash NOT immediately followed by a newline (including
			// one sitting right at EOF) is not a valid continuation.
			// CPython raises "unexpected character after line continuation
			// character" / a SyntaxError at EOF; radon's caller turns that
			// into a skipped file, so this must be a hard error rather
			// than falling through to consumeOp and being lexed as a
			// stray operator token that quietly contributes nothing.
			return &ErrTokenize{
				Line: t.line,
				Msg:  "unexpected character after line continuation character",
			}
		case c == ' ' || c == '\t' || c == '\f':
			t.pos++
			t.col++
		case isStringStart(t.src, t.pos):
			if err := t.consumeString(); err != nil {
				return err
			}
		case unicode.IsDigit(c):
			t.consumeNumber()
		case isIdentStart(c):
			t.consumeName()
		default:
			if err := t.consumeOp(); err != nil {
				return err
			}
		}
	}

	// An unclosed bracket at EOF is the same class of defect as the
	// continuation-then-EOF case above: input that promises more tokens
	// (here, a matching closer) and never delivers them. CPython raises
	// "unexpected EOF while parsing" for `x = (1, 2` with no close; without
	// this check the file would silently parse as if the paren had closed
	// at EOF, scoring a row for invalid Python.
	if len(t.brackets) > 0 {
		return &ErrTokenize{Line: t.line, Msg: "unexpected EOF while parsing"}
	}

	// A file that ends mid-line still ends a logical line, and every open
	// suite still closes. Emitting these unconditionally means cc.go never
	// needs an "or end of input" branch beside each DEDENT check.
	if len(t.tokens) > 0 && t.tokens[len(t.tokens)-1].Kind != TokenNewline {
		t.emit(TokenNewline, "")
	}
	for len(t.indents) > 1 {
		t.indents = t.indents[:len(t.indents)-1]
		t.emit(TokenDedent, "")
	}
	t.emit(TokenEOF, "")
	return nil
}

// handleLineStart measures indentation and emits INDENT/DEDENT. It reports
// blank=true for a line that carries no token at all (empty or
// comment-only), which Python ignores for indentation purposes entirely --
// a comment indented to column 0 in the middle of a nested suite must not
// close that suite.
func (t *tokenizer) handleLineStart() (bool, error) {
	start := t.pos
	// width8 expands tabs to the next multiple of 8 (what a naive
	// single-metric tokenizer would compare). width1 counts every tab as
	// exactly one column. CPython requires BOTH metrics to agree on the
	// ordering between a line and the enclosing indent level; a mix of
	// tabs and spaces that only LOOKS consistent under one metric is a
	// TabError, not a silently-accepted indentation change.
	width8, width1 := 0, 0
	for t.pos < len(t.src) {
		c := t.src[t.pos]
		if c == ' ' {
			width8++
			width1++
		} else if c == '\t' {
			width8 += 8 - (width8 % 8)
			width1++
		} else if c == '\f' {
			width8 = 0
			width1 = 0
		} else {
			break
		}
		t.pos++
	}

	if t.pos >= len(t.src) {
		t.col = width8
		t.atLineStart = false
		return false, nil
	}

	switch t.src[t.pos] {
	case '\n':
		t.pos++
		t.line++
		t.col = 0
		return true, nil
	case '#':
		t.skipComment()
		if t.pos < len(t.src) && t.src[t.pos] == '\n' {
			t.pos++
			t.line++
			t.col = 0
		}
		return true, nil
	}

	t.col = width8
	t.atLineStart = false

	top := t.indents[len(t.indents)-1]
	switch {
	case width8 > top[0]:
		if width1 <= top[1] {
			// width8 says deeper, width1 says not deeper (or equal) --
			// the two metrics disagree on direction.
			return false, &ErrTokenize{
				Line: t.line,
				Msg:  "inconsistent use of tabs and spaces in indentation",
			}
		}
		t.indents = append(t.indents, [2]int{width8, width1})
		t.emit(TokenIndent, string(t.src[start:t.pos]))
	case width8 < top[0]:
		for len(t.indents) > 1 && t.indents[len(t.indents)-1][0] > width8 {
			t.indents = t.indents[:len(t.indents)-1]
			t.emit(TokenDedent, "")
		}
		newTop := t.indents[len(t.indents)-1]
		if newTop[0] != width8 {
			// CPython raises IndentationError here. radon's caller
			// turns any exception into a skipped file, so surfacing
			// this as an error keeps the two sides agreeing on which
			// files produce no row at all.
			return false, &ErrTokenize{
				Line: t.line,
				Msg:  "unindent does not match any outer indentation level",
			}
		}
		if newTop[1] != width1 {
			return false, &ErrTokenize{
				Line: t.line,
				Msg:  "inconsistent use of tabs and spaces in indentation",
			}
		}
	default: // width8 == top[0]
		if width1 != top[1] {
			return false, &ErrTokenize{
				Line: t.line,
				Msg:  "inconsistent use of tabs and spaces in indentation",
			}
		}
	}
	return false, nil
}

func (t *tokenizer) skipComment() {
	for t.pos < len(t.src) && t.src[t.pos] != '\n' {
		t.pos++
	}
}

func (t *tokenizer) consumeNewline() {
	// Inside brackets a physical newline is not a logical one. Suppressing
	// the token here is what lets a wrapped `if (a and\n b):` count its
	// BoolOp once rather than losing the second operand.
	if t.depth > 0 {
		t.pos++
		t.line++
		t.col = 0
		return
	}
	t.emit(TokenNewline, "")
	t.pos++
	t.line++
	t.col = 0
	t.atLineStart = true
}

func (t *tokenizer) consumeName() {
	start := t.pos
	startCol := t.col
	for t.pos < len(t.src) && isIdentPart(t.src[t.pos]) {
		t.pos++
		t.col++
	}
	text := string(t.src[start:t.pos])

	// A string prefix binds to the quote that follows it, so `rb"..."` is
	// one STRING token, not NAME + STRING. Checked here because the
	// prefix letters are themselves valid identifiers.
	if t.pos < len(t.src) && (t.src[t.pos] == '"' || t.src[t.pos] == '\'') && isStringPrefix(text) {
		t.pos = start
		t.col = startCol
		if err := t.consumeString(); err == nil {
			return
		}
		// consumeString failing here means the quote is unterminated;
		// fall through and emit the NAME so run() reports the error at
		// the quote rather than silently swallowing the identifier.
		t.pos = start + len(text)
		t.col = startCol + len(text)
	}

	t.tokens = append(t.tokens, Token{
		Kind: TokenName, Text: text, Line: t.line, Col: startCol,
	})
}

func (t *tokenizer) consumeNumber() {
	start := t.pos
	startCol := t.col
	for t.pos < len(t.src) {
		c := t.src[t.pos]
		// Underscore separators, hex/binary digits and exponent signs are
		// all swallowed. Numbers are inert for complexity, so the only
		// requirement is that the whole literal is consumed and cannot be
		// re-lexed as something meaningful.
		if unicode.IsDigit(c) || c == '_' || c == '.' ||
			(c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F') ||
			c == 'x' || c == 'X' || c == 'o' || c == 'O' ||
			c == 'b' || c == 'B' || c == 'e' || c == 'E' || c == 'j' || c == 'J' {
			t.pos++
			t.col++
			continue
		}
		if (c == '+' || c == '-') && t.pos > start {
			prev := t.src[t.pos-1]
			if prev == 'e' || prev == 'E' {
				t.pos++
				t.col++
				continue
			}
		}
		break
	}
	t.tokens = append(t.tokens, Token{
		Kind: TokenNumber, Text: string(t.src[start:t.pos]), Line: t.line, Col: startCol,
	})
}

// consumeString handles every Python string form: short and triple quoted,
// raw or not, with any legal prefix. Raw strings still honour a backslash
// for the purpose of NOT ending the literal on an escaped quote, which is
// CPython's actual rule and a classic source of a runaway lexer.
func (t *tokenizer) consumeString() error {
	start := t.pos
	startCol := t.col
	startLine := t.line

	raw := false
	isFString := false
	for t.pos < len(t.src) && isIdentPart(t.src[t.pos]) {
		switch t.src[t.pos] {
		case 'r', 'R':
			raw = true
		case 'f', 'F':
			isFString = true
		}
		t.pos++
		t.col++
	}

	if t.pos >= len(t.src) {
		return &ErrTokenize{Line: startLine, Msg: "unterminated string literal"}
	}
	quote := t.src[t.pos]
	triple := false
	if t.pos+2 < len(t.src) && t.src[t.pos+1] == quote && t.src[t.pos+2] == quote {
		triple = true
		t.pos += 3
		t.col += 3
	} else {
		t.pos++
		t.col++
	}
	bodyStart := t.pos
	bodyStartLine := t.line
	bodyStartCol := t.col

	// braceDepth tracks nesting of f-string replacement fields (`{...}`)
	// while scanning for the OUTER literal's closing quote. It is always 0
	// for a non-f-string. PEP 701 (Python 3.12+, and this repo's CI runs
	// 3.14) allows a replacement field to contain a nested string literal
	// using the SAME quote character as the outer f-string -- e.g.
	// `f"{x["key"]}"`. Without brace-depth awareness, the naive `c == quote`
	// check below closes the outer string at that inner `"`, hiding
	// whatever comes after it (an `if`/`else`, an `and`/`or`) from the
	// complexity count entirely (CHAOS-4291 r2 P1 #2). While braceDepth > 0,
	// a quote character is therefore routed to skipNestedStringInline
	// instead of being tested against `quote`, exactly mirroring how
	// findFieldEnd/skipNestedString (below) already treat a field's
	// contents once expandFString takes over.
	braceDepth := 0
	for {
		if t.pos >= len(t.src) {
			return &ErrTokenize{Line: startLine, Msg: "unterminated string literal"}
		}
		c := t.src[t.pos]

		if isFString && braceDepth == 0 && c == '{' &&
			t.pos+1 < len(t.src) && t.src[t.pos+1] == '{' {
			// Escaped literal brace outside any field.
			t.pos += 2
			t.col += 2
			continue
		}
		if isFString && braceDepth == 0 && c == '}' &&
			t.pos+1 < len(t.src) && t.src[t.pos+1] == '}' {
			t.pos += 2
			t.col += 2
			continue
		}
		if isFString && c == '{' {
			braceDepth++
			t.pos++
			t.col++
			continue
		}
		if isFString && braceDepth > 0 && c == '}' {
			braceDepth--
			t.pos++
			t.col++
			continue
		}
		if isFString && braceDepth > 0 && (c == '"' || c == '\'') {
			t.skipNestedStringInline(c)
			continue
		}

		if c == '\\' && (!isFString || braceDepth == 0) {
			// In a raw string the backslash is retained but STILL
			// prevents the next character from closing the literal, so
			// the skip is identical either way. Keeping one branch
			// documents that rather than implying raw strings ignore
			// backslashes entirely. Gated to braceDepth == 0: inside an
			// active replacement field a bare backslash is expression
			// syntax (or belongs to a nested string, already consumed by
			// skipNestedStringInline above), not an outer-literal escape.
			_ = raw
			t.pos += 2
			t.col += 2
			continue
		}
		if c == '\n' {
			if !triple {
				return &ErrTokenize{
					Line: startLine, Msg: "unterminated string literal",
				}
			}
			t.pos++
			t.line++
			t.col = 0
			continue
		}
		if c == quote && braceDepth == 0 {
			if triple {
				if t.pos+2 < len(t.src) && t.src[t.pos+1] == quote && t.src[t.pos+2] == quote {
					t.pos += 3
					t.col += 3
					break
				}
				t.pos++
				t.col++
				continue
			}
			t.pos++
			t.col++
			break
		}
		t.pos++
		t.col++
	}

	closeLen := 1
	if triple {
		closeLen = 3
	}
	bodyEnd := t.pos - closeLen

	t.tokens = append(t.tokens, Token{
		Kind: TokenString, Text: string(t.src[start:t.pos]), Line: startLine, Col: startCol,
	})

	if isFString && bodyEnd > bodyStart {
		// f-strings embed real expressions inside `{...}` replacement
		// fields (PEP 498/701) -- radon's AST sees a JoinedStr containing
		// FormattedValue nodes and its generic_visit walks straight into
		// them, so an `if`/`else`/`and`/`or`/comprehension inside a
		// formatted value is a genuine decision point, not inert text.
		// Splicing the field expressions' own tokens in right after the
		// opaque STRING token lets cc.go's flat keyword scan see them
		// without needing to understand string literals at all. Line/Col
		// on the spliced tokens are approximate (pinned to where the
		// string began) -- nothing downstream uses a nested token's
		// position, only its Kind/Text.
		fieldToks, err := expandFString(t.src[bodyStart:bodyEnd], bodyStartLine, bodyStartCol)
		if err != nil {
			// CHAOS-4291 r2 P2 #3: a field that never closes, or whose
			// expression is not valid Python (e.g. a mismatched bracket),
			// is a real SyntaxError on the CPython/radon side ("f-string:
			// expecting '}'", or the expression's own parse error) --
			// radon's caller catches it and skips the file. Swallowing the
			// error here and emitting only the opaque STRING token instead
			// silently accepted invalid Python as a normally-scored file.
			return err
		}
		t.tokens = append(t.tokens, fieldToks...)
	}
	return nil
}

// skipNestedStringInline advances t.pos/t.line/t.col past one quoted
// literal (short or triple) that opens at the current position with quote
// character `quote`, honouring backslash escapes and embedded newlines
// (legal inside a triple-quoted nested literal under PEP 701). It does not
// require the nested quote to differ from the enclosing f-string's own
// quote -- see the braceDepth comment in consumeString.
func (t *tokenizer) skipNestedStringInline(quote rune) {
	triple := t.pos+2 < len(t.src) && t.src[t.pos+1] == quote && t.src[t.pos+2] == quote
	if triple {
		t.pos += 3
		t.col += 3
	} else {
		t.pos++
		t.col++
	}
	for t.pos < len(t.src) {
		c := t.src[t.pos]
		if c == '\\' && t.pos+1 < len(t.src) {
			t.pos += 2
			t.col += 2
			continue
		}
		if c == '\n' {
			t.pos++
			t.line++
			t.col = 0
			continue
		}
		if c == quote {
			if triple {
				if t.pos+2 < len(t.src) && t.src[t.pos+1] == quote && t.src[t.pos+2] == quote {
					t.pos += 3
					t.col += 3
					return
				}
				t.pos++
				t.col++
				continue
			}
			t.pos++
			t.col++
			return
		}
		t.pos++
		t.col++
	}
	// Ran off the end without finding a closer: the outer consumeString
	// loop's own `t.pos >= len(t.src)` check reports this as an unterminated
	// string literal on the next iteration, which is the correct outcome --
	// CPython raises SyntaxError/unterminated string for the same input.
}

// expandFString extracts the real expression tokens out of an f-string's
// replacement fields (`{expr}`, `{expr!conv}`, `{expr:spec}`), recursing
// into a format spec's own nested fields (`{expr:{width}}`). Literal text
// outside `{...}` and an escaped `{{`/`}}` contribute no tokens, matching
// radon: JoinedStr's plain Constant/str pieces carry no complexity.
func expandFString(body []rune, line, col int) ([]Token, error) {
	var out []Token
	n := len(body)
	i := 0
	for i < n {
		c := body[i]
		if c == '{' {
			if i+1 < n && body[i+1] == '{' {
				i += 2
				continue
			}
			exprEnd, specStart, fieldEnd, ok := findFieldEnd(body, i+1)
			if !ok {
				// The field never closed: CPython raises `SyntaxError:
				// f-string: expecting '}'` and radon's caller skips the
				// file. In practice consumeString's own braceDepth
				// tracking already requires every `{` to be balanced
				// before it will accept the outer literal as closed, so
				// this is unreachable via the normal Tokenize path -- it
				// stays a hard error (never a silent "treat as if it
				// closed at the end") so a future change that decouples
				// the two scans fails loudly instead of under-counting.
				return nil, &ErrTokenize{Line: line, Msg: "f-string: expecting '}'"}
			}
			if exprEnd > i+1 {
				toks, err := tokenizeFStringExpr(body[i+1:exprEnd], line, col)
				if err != nil {
					// CHAOS-4291 r2 P2 #3: a field expression that is not
					// valid Python (mismatched bracket, stray token) must
					// fail the whole file, matching radon's SyntaxError ->
					// skip. Discarding the error here and moving on used to
					// silently accept invalid Python as a normally-scored
					// block.
					return nil, err
				}
				out = append(out, toks...)
			}
			if specStart >= 0 && specStart < fieldEnd {
				specToks, err := expandFString(body[specStart:fieldEnd], line, col)
				if err != nil {
					return nil, err
				}
				out = append(out, specToks...)
			}
			i = fieldEnd + 1
			continue
		}
		if c == '}' && i+1 < n && body[i+1] == '}' {
			i += 2
			continue
		}
		i++
	}
	return out, nil
}

// findFieldEnd scans one replacement field starting right after its opening
// `{`, returning: exprEnd (exclusive end of the expression portion),
// specStart (start of a format-spec, or -1 if there is none), and fieldEnd
// (the index of the field's closing `}`). It tracks bracket depth and skips
// over nested string literals so a `:` or `!` inside them, or inside a
// nested call/subscript/dict/set, is never mistaken for the field's own
// conversion or format-spec marker.
func findFieldEnd(body []rune, exprStart int) (exprEnd, specStart, fieldEnd int, ok bool) {
	depth := 0
	i := exprStart
	n := len(body)
	exprEnd, specStart = -1, -1
	for i < n {
		c := body[i]
		switch {
		case c == '\'' || c == '"':
			i = skipNestedString(body, i)
		case c == '(' || c == '[' || c == '{':
			depth++
			i++
		case c == ')' || c == ']':
			if depth > 0 {
				depth--
			}
			i++
		case c == '}':
			if depth == 0 {
				if exprEnd < 0 {
					exprEnd = i
				}
				return exprEnd, specStart, i, true
			}
			depth--
			i++
		case c == '!' && depth == 0 && exprEnd < 0 && i+2 < n &&
			(body[i+1] == 's' || body[i+1] == 'r' || body[i+1] == 'a') &&
			(body[i+2] == '}' || body[i+2] == ':'):
			exprEnd = i
			i += 2
		case c == ':' && depth == 0 && exprEnd < 0:
			exprEnd = i
			specStart = i + 1
			i++
		case c == ':' && depth == 0 && exprEnd >= 0 && specStart < 0:
			// A format-spec colon following a `!conversion`.
			specStart = i + 1
			i++
		default:
			i++
		}
	}
	// The field's own `}` was never found at depth 0: an unterminated
	// replacement field (CHAOS-4291 r2 P2 #3). ok=false tells the caller to
	// fail closed rather than silently treating the field as if it closed
	// at the end of body.
	if exprEnd < 0 {
		exprEnd = n
	}
	return exprEnd, specStart, n, false
}

// skipNestedString skips one quoted literal (short or triple, any prefix
// already excluded by the caller) starting at a quote character, honouring
// backslash escapes, and returns the index right after it. It does not
// require the quote to match the OUTER f-string's quote character: Python
// 3.12+ (PEP 701) allows reusing the same quote inside a replacement field,
// and this repository's CI runs Python 3.14.
func skipNestedString(body []rune, i int) int {
	quote := body[i]
	n := len(body)
	triple := i+2 < n && body[i+1] == quote && body[i+2] == quote
	if triple {
		i += 3
	} else {
		i++
	}
	for i < n {
		if body[i] == '\\' && i+1 < n {
			i += 2
			continue
		}
		if body[i] == quote {
			if triple {
				if i+2 < n && body[i+1] == quote && body[i+2] == quote {
					return i + 3
				}
				i++
				continue
			}
			return i + 1
		}
		i++
	}
	return n
}

// tokenizeFStringExpr lexes one replacement field's expression text with
// the same rules as the outer source, so a nested `if`/`and`/`or`/`for`
// inside an f-string is counted instead of vanishing into an opaque
// string. depth is seeded at 1 so a raw newline inside the fragment (legal
// inside `{}` under PEP 701) is treated as a continuation rather than
// triggering indentation handling, which has no meaning inside an
// expression; atLineStart stays false for the same reason.
func tokenizeFStringExpr(src []rune, line, col int) ([]Token, error) {
	sub := &tokenizer{
		src:     src,
		line:    line,
		col:     col,
		indents: [][2]int{{0, 0}},
		depth:   1,
	}
	if err := sub.run(); err != nil {
		return nil, err
	}
	toks := sub.tokens
	// run() unconditionally appends a closing NEWLINE (and always an EOF)
	// for what it thinks is a whole file; strip both since they describe
	// the fragment's own end, not a boundary in the outer statement.
	for len(toks) > 0 && (toks[len(toks)-1].Kind == TokenEOF || toks[len(toks)-1].Kind == TokenNewline) {
		toks = toks[:len(toks)-1]
	}
	return toks, nil
}

func (t *tokenizer) consumeOp() error {
	c := t.src[t.pos]
	switch c {
	case '(', '[', '{':
		t.depth++
		t.brackets = append(t.brackets, matchingCloser(c))
	case ')', ']', '}':
		if len(t.brackets) == 0 || t.brackets[len(t.brackets)-1] != c {
			// CPython's parser rejects `(1]` outright ("closing parenthesis
			// ']' does not match opening parenthesis '('"). Accepting any
			// closer for any opener would let invalid Python through as a
			// scored row instead of the required skip.
			return &ErrTokenize{
				Line: t.line,
				Msg:  fmt.Sprintf("closing bracket %q does not match the most recent opening bracket", string(c)),
			}
		}
		t.brackets = t.brackets[:len(t.brackets)-1]
		if t.depth > 0 {
			t.depth--
		}
	}
	startCol := t.col
	t.pos++
	t.col++
	t.tokens = append(t.tokens, Token{
		Kind: TokenOp, Text: string(c), Line: t.line, Col: startCol,
	})
	return nil
}

// matchingCloser returns the closing bracket rune an opener requires.
func matchingCloser(open rune) rune {
	switch open {
	case '(':
		return ')'
	case '[':
		return ']'
	case '{':
		return '}'
	}
	return 0
}

func (t *tokenizer) emit(kind TokenKind, text string) {
	t.tokens = append(t.tokens, Token{Kind: kind, Text: text, Line: t.line, Col: t.col})
}

func (t *tokenizer) peekAt(offset int) rune {
	if t.pos+offset >= len(t.src) {
		return utf8.RuneError
	}
	return t.src[t.pos+offset]
}

func isIdentStart(c rune) bool {
	return c == '_' || unicode.IsLetter(c)
}

func isIdentPart(c rune) bool {
	// PEP 3131: an identifier's continuation characters are XID_Continue,
	// which is XID_Start (letters, `_`) plus decimal digits PLUS the
	// Unicode combining-mark categories Mn (nonspacing) and Mc (spacing
	// combining) -- e.g. U+0301 COMBINING ACUTE ACCENT. Without Mn/Mc,
	// `if́ = 1` (a valid identifier assignment) tokenizes as the
	// keyword `if` followed by a stray combining-mark rune, inventing a
	// decision point that does not exist (CHAOS-4291 r2 P2 #4). isIdentStart
	// is deliberately untouched: a combining mark cannot legally START an
	// identifier either in Python or in this port.
	return c == '_' || unicode.IsLetter(c) || unicode.IsDigit(c) ||
		unicode.Is(unicode.Mn, c) || unicode.Is(unicode.Mc, c)
}

func isStringPrefix(text string) bool {
	if len(text) > 3 {
		return false
	}
	for _, c := range text {
		if !strings.ContainsRune(stringPrefixes, c) {
			return false
		}
	}
	return len(text) > 0
}

// isStringStart reports whether position i begins a string literal, either
// with a bare quote or with a legal prefix followed by one.
func isStringStart(src []rune, i int) bool {
	if i >= len(src) {
		return false
	}
	if src[i] == '"' || src[i] == '\'' {
		return true
	}
	j := i
	for j < len(src) && j-i < 3 && strings.ContainsRune(stringPrefixes, src[j]) {
		j++
	}
	return j > i && j < len(src) && (src[j] == '"' || src[j] == '\'')
}
