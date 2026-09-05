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
	// keeps it: a column wider than the top pushes one INDENT, a narrower
	// one pops until it matches, and failing to match is an error.
	indents []int
	// depth is bracket nesting. Inside brackets, newlines are implicit
	// continuations and indentation is not significant -- this is what
	// makes a multi-line call signature or a wrapped boolean expression
	// lex correctly.
	depth int
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
		indents:     []int{0},
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
			t.consumeOp()
		}
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
	width := 0
	for t.pos < len(t.src) {
		c := t.src[t.pos]
		if c == ' ' {
			width++
		} else if c == '\t' {
			// CPython expands tabs to the next multiple of 8 when
			// comparing indentation levels.
			width += 8 - (width % 8)
		} else if c == '\f' {
			width = 0
		} else {
			break
		}
		t.pos++
	}

	if t.pos >= len(t.src) {
		t.col = width
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

	t.col = width
	t.atLineStart = false

	top := t.indents[len(t.indents)-1]
	switch {
	case width > top:
		t.indents = append(t.indents, width)
		t.emit(TokenIndent, string(t.src[start:t.pos]))
	case width < top:
		for len(t.indents) > 1 && t.indents[len(t.indents)-1] > width {
			t.indents = t.indents[:len(t.indents)-1]
			t.emit(TokenDedent, "")
		}
		if t.indents[len(t.indents)-1] != width {
			// CPython raises IndentationError here. radon's caller
			// turns any exception into a skipped file, so surfacing
			// this as an error keeps the two sides agreeing on which
			// files produce no row at all.
			return false, &ErrTokenize{
				Line: t.line,
				Msg:  "unindent does not match any outer indentation level",
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
	for t.pos < len(t.src) && isIdentPart(t.src[t.pos]) {
		if t.src[t.pos] == 'r' || t.src[t.pos] == 'R' {
			raw = true
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

	for {
		if t.pos >= len(t.src) {
			return &ErrTokenize{Line: startLine, Msg: "unterminated string literal"}
		}
		c := t.src[t.pos]

		if c == '\\' {
			// In a raw string the backslash is retained but STILL
			// prevents the next character from closing the literal, so
			// the skip is identical either way. Keeping one branch
			// documents that rather than implying raw strings ignore
			// backslashes entirely.
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
		if c == quote {
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

	t.tokens = append(t.tokens, Token{
		Kind: TokenString, Text: string(t.src[start:t.pos]), Line: startLine, Col: startCol,
	})
	return nil
}

func (t *tokenizer) consumeOp() {
	c := t.src[t.pos]
	switch c {
	case '(', '[', '{':
		t.depth++
	case ')', ']', '}':
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
	return c == '_' || unicode.IsLetter(c) || unicode.IsDigit(c)
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
