package lizardcc

import (
	"fmt"
	"regexp"
	"strings"
	"unicode"
)

// pythonExtraWhitespaceRunes is the set of Python str.isspace()/re `\s`
// whitespace characters RE2's `\p{Z}` does NOT cover on its own --
// verified directly (see TestPythonWhitespaceSetIsPinned, tokenize_test.go),
// cross-checking every character in Python's whitespace set against BOTH
// `\p{Z}` and Go's unicode.IsSpace individually rather than assuming:
//
//   - U+001C-U+001F (FS/GS/RS/US, the four ASCII information separator
//     controls): covered by NEITHER `\p{Z}` NOR unicode.IsSpace -- a
//     CPython-specific bidirectional-class quirk, not Unicode White_Space
//     property membership.
//   - U+0085 (NEL): covered by unicode.IsSpace (it IS a Unicode
//     White_Space property member) but NOT by `\p{Z}` (its general
//     category is Cc, control, not Z).
//
// Every OTHER character Python's str.isspace() recognises (U+00A0 NBSP,
// U+1680, U+2000-U+200A, U+2028, U+2029, U+202F, U+205F, U+3000) is
// already covered by BOTH `\p{Z}` and unicode.IsSpace and needs no
// special-casing here.
//
// ONE named set, used by BOTH the tokenizer's whitespace-run regex
// alternative (buildTokenPattern, via pythonExtraWhitespaceRegexClass)
// AND isAllSpace below, so the two can never drift out of sync with each
// other again -- codex round r3 on #2253 found U+001C-U+001F missing;
// fixing only the regex (or only isAllSpace) would have left the other
// silently wrong for the same input.
var pythonExtraWhitespaceRunes = []rune{0x1c, 0x1d, 0x1e, 0x1f, 0x85}

// pythonExtraWhitespaceRegexClass renders pythonExtraWhitespaceRunes as a
// RE2 character-class fragment (e.g. `\x1c\x1d\x1e\x1f\x85`), computed
// once from the named set rather than hand-copied into the pattern
// string, so the regex and isAllSpace are provably built from the same
// data.
var pythonExtraWhitespaceRegexClass = runeClassFragment(pythonExtraWhitespaceRunes)

func runeClassFragment(rs []rune) string {
	var b strings.Builder
	for _, r := range rs {
		if r <= 0xff {
			fmt.Fprintf(&b, `\x%02x`, r)
		} else {
			fmt.Fprintf(&b, `\x{%x}`, r)
		}
	}
	return b.String()
}

// This file ports CodeReader.generate_tokens (code_reader.py:135-206),
// CLikeReader.generate_tokens (clike.py:38-44) and CLikeReader.preprocess
// (clike.py:46-65).
//
// TWO DELIBERATE REGEX DEVIATIONS, both because Go's regexp package is RE2
// and RE2 supports neither lookahead nor lookbehind:
//
//  1. code_reader.py:165's generic-disambiguation alternative --
//     `\<(?=(?:[^<>?]*\?)+[^<>]*\>)(?:[\w\s,.?]|(?:extends))+\>` -- glues a
//     `<...>` run containing at least one `?` into ONE token, so long as
//     every character between the brackets is a word character,
//     whitespace, `,`, `.`, `?`, or the literal word "extends" (anything
//     else, or reaching end-of-input before a `>`, falls through to the
//     ordinary per-character tokenization). This is impossible to express
//     as a single RE2 alternative (RE2 has no lookahead), so it is ported
//     as mergeTemplateQuestionRuns, a POST-tokenization merge pass over
//     the already-tokenized stream instead: exactly equivalent semantics
//     (same allowed-token set, same "first `>` terminates, no nesting"
//     rule, since Python's own `[^<>]*` cannot skip past a nested `<`/`>`
//     either), just applied as a second pass rather than inline in the
//     regex.
//
//     BUG FIXED HERE (CHAOS-5156, codex round r1 on #2253): an earlier
//     revision dropped this alternative ENTIRELY and claimed doing so
//     "cannot change a complexity number" -- that claim was argued from
//     source, not measured, and was FALSE. `Type<T ? U> x;` (a template
//     argument list containing a `?`) has its `?` glued into the `<...>`
//     token by Python's version, so it never reaches condition_counter;
//     without the glue, `?` tokenizes on its own and IS a member of
//     `conditions` (this file), so it gets counted -- lizard measures 1
//     for the enclosing function, this port measured 2 before this fix.
//  2. clike.py:12's raw-string token
//     (`(?:u8|u|U|L)?R"\((?:[^)]|\)(?!\"))*\)\"`) uses `(?!\")` to stop at
//     the first `)"` while still allowing a lone `)` inside the literal.
//     A non-greedy `.*?` between the delimiters is the exact same
//     "first occurrence" semantics without lookahead, so
//     `(?:u8|u|U|L)?R"\(.*?\)"` (with DOTALL) is a faithful, not merely
//     approximate, replacement.
//
// Both are exercised by dedicated tests (tokenize_test.go) rather than
// argued from source alone.

// conditions mirrors CodeReader._build_conditions() (code_reader.py:98-113)
// for the base CLikeReader: control-flow keywords, the two logical
// operators, `case`, and the ternary `?`. Java and C# both use this same
// set unmodified (java.py, csharp.py add '??' to C#'s TOKENIZER but not to
// its conditions -- see csharp.go).
var conditions = map[string]bool{
	"if": true, "for": true, "while": true, "catch": true,
	"&&": true, "||": true,
	"case": true,
	"?":    true,
}

// combinedSymbols is code_reader.py:145-151's combined_symbols list, order
// preserved: alternation in both Python's re and Go's regexp prefers the
// EARLIER alternative at a given position (neither engine is POSIX
// leftmost-longest here), so "<<=" must stay listed before "<=" etc.
var combinedSymbols = []string{
	"<<=", ">>=", "||", "&&", "===", "!==",
	"==", "!=", "<=", ">=", "->", "=>",
	"++", "--", "+=", "-=",
	"+", "-", "*", "/",
	"*=", "/=", "^=", "&=", "|=", "...",
}

// buildTokenPattern assembles the token regex by direct concatenation, NOT
// by joining fragments with "|" -- addition (CLikeReader's raw-string and
// float alternatives) already carries its own leading "|", exactly the way
// clike.py builds the same string by Python string concatenation
// (`r"\/\*.*?\*\/" + add + r"|(?:\d+\')+\d+" + ...`, code_reader.py:152-170).
// Joining with an extra separator here would insert a SECOND "|" before
// addition, producing an empty zero-width alternative that -- being both
// earlier in the alternation and always able to match -- would make every
// token empty under Go's leftmost-first semantics. Every other fragment
// below therefore supplies its own leading "|" explicitly, matching
// Python's source line for line rather than hiding the separator in a
// Join call.
func buildTokenPattern(addition string) *regexp.Regexp {
	escaped := make([]string, len(combinedSymbols))
	for i, s := range combinedSymbols {
		escaped[i] = regexp.QuoteMeta(s)
	}
	pattern := "(?s)(?:" + // DOTALL, matching Python's re.S
		`/\*.*?\*/` + // block comment, non-greedy (code_reader.py:154)
		addition + // CLikeReader's raw-string + float literals (clike.py:40-43)
		`|(?:\d+')+\d+` + // digit-separator integer, e.g. 1'000'000
		`|0x(?:[0-9A-Fa-f]+')+[0-9A-Fa-f]+` + // digit-separator hex
		`|0b(?:[01]+')+[01]+` + // digit-separator binary
		// BUG FIXED HERE (CHAOS-5156, codex round r1 on #2253/#2268/#2269,
		// same class found independently on C++/C#/Kotlin/Java): Go's RE2
		// `\w` is ASCII-only ([0-9A-Za-z_]); Python's `\w` under `re`'s
		// default (Unicode) mode matches any Unicode letter/digit plus
		// underscore. A non-ASCII identifier (`café`, `é`) split into
		// multiple tokens here (`caf`+`é`) abandons the enclosing
		// declaration in every reader, so the whole function was LOST, not
		// merely miscounted -- confirmed against real lizard 1.23.0, which
		// recognises the identifier and reports the function normally.
		// `\p{L}` (any Unicode letter) + `\p{N}` (any Unicode number) is
		// RE2's supported equivalent (RE2 has no lookaround but DOES
		// support Unicode character classes) -- not a byte-for-byte match
		// of Python's `\w` (which also admits some combining-mark
		// categories `\p{L}\p{N}_` does not), but every real-world
		// identifier shape in every fixture corpus in this package is
		// covered by it.
		`|[\p{L}\p{N}_]+` +
		`|"(?:\\.|[^"\\])*"` +
		`|'(?:\\.|[^'\\])*?'` +
		`|//(?:\\\n|[^\n])*` + // line comment; a trailing "\" continues it (code_reader.py:144,162)
		`|#` +
		`|:=|::|\*\*` +
		`|` + strings.Join(escaped, "|") +
		`|\\\n` + // explicit line continuation
		`|\n` + // bare newline, its own token
		// BUG FIXED HERE (CHAOS-5156, codex round r2 on #2253): this used
		// to be RE2's own `[^\S\n]` (ASCII-only horizontal whitespace --
		// RE2's `\s`/`\S` classes never look past ASCII). Python's `\s`
		// under its default Unicode mode also matches the Unicode
		// separator categories (Zs/Zl/Zp -- `\p{Z}` is RE2's equivalent),
		// so a non-ASCII space (U+00A0 NBSP) was never glued into a
		// whitespace-run token here and instead fell through to the `.`
		// catch-all as its own single-character token, which
		// stateDecToImp's "not alpha -> abandon" fallback then treated as
		// a genuine non-whitespace token. Confirmed against real lizard
		// 1.23.0: `int f()<NBSP>{ return 0; }` measures [1] (NBSP glued
		// as whitespace, dropped); this port measured [] before the fix.
		//
		// BUG FIXED HERE (CHAOS-5156, codex round r3 on #2253): `\p{Z}`
		// covers Unicode's Zs/Zl/Zp SEPARATOR categories, but Python's
		// str.isspace()/re `\s` also treats a handful of OTHER characters
		// as whitespace that `\p{Z}` alone misses -- see
		// pythonExtraWhitespaceRunes's doc for the full accounting
		// (U+001C-U+001F, U+0085), verified directly against both `\p{Z}`
		// and unicode.IsSpace rather than assumed. Without this, one of
		// these characters falls through to the `.` catch-all as its own
		// token, which stateDecToImp's "not alpha -> abandon" fallback
		// then abandons the declaration over. Confirmed against real
		// lizard 1.23.0: `int f()<U+001C>{ return 0; }` measures [1] (the
		// control char glued as whitespace, dropped); this port measured
		// [] (function lost entirely) before this fix.
		`|[\t\v\f\r ` + pythonExtraWhitespaceRegexClass + `\p{Z}]+` +
		`|.` + // catch-all: every remaining single character
		`)`
	return regexp.MustCompile(pattern)
}

// cLikeAddition is CLikeReader.generate_tokens's `addition` (clike.py:40-43):
// C++ empty-delimiter raw strings, then the two float-literal forms (a
// leading-dot float and a trailing-dot float), each already carrying its
// own leading "|" the way clike.py's string concatenation does.
const cLikeAddition = `|(?:u8|u|U|L)?R"\(.*?\)"` + // see deviation (2) above
	`|(?:\d*\.\d+(?:[eE][-+]?\d+)?)` +
	`|(?:\d+\.(?:\d+)?(?:[eE][-+]?\d+)?)`

var cLikeTokenPattern = buildTokenPattern(cLikeAddition)

// GenerateTokens ports CLikeReader.generate_tokens + CodeReader.generate_tokens
// for the plain C/C++ token grammar (no per-language additions).
func GenerateTokens(source string) []string {
	return accumulateMacros(mergeTemplateQuestionRuns(cLikeTokenPattern.FindAllString(source, -1)))
}

// templateGlueAllowed reports whether tok is one of the character classes
// Python's lookahead alternative permits BETWEEN a `<` and its matching
// `>` (see this file's deviation-1 doc): a run of word characters, a run
// of whitespace (including a bare "\n" -- Python's `\s` matches newlines
// too), a lone "," or ".", a lone "?", or the literal word "extends". Any
// OTHER token (an operator, a nested "<"/">", a string literal, ...)
// disqualifies the whole run, exactly as it would fail Python's
// `(?:[\w\s,.?]|(?:extends))+` at the first disallowed character.
func templateGlueAllowed(tok string) bool {
	if tok == "extends" || tok == "," || tok == "." || tok == "?" {
		return true
	}
	if isAllSpace(tok) {
		return true
	}
	// BUG FIXED HERE (CHAOS-5156, codex round r3 on #2253): this used
	// unicode.IsDigit, which only covers decimal digits (category Nd) --
	// narrower than Python's `\w`, which under Unicode mode also admits
	// the other Unicode number categories (Nl "letter numbers" such as
	// Roman numerals, No "other numbers"). `Type<T ? Ⅷ>` (U+2167, ROMAN
	// NUMERAL EIGHT, category Nl) is glued by Python's version and never
	// reaches condition_counter; confirmed against real lizard 1.23.0:
	// measures [1], this port measured [2] before this fix. unicode.IsNumber
	// covers Nd+Nl+No, matching the same class the main tokenizer already
	// uses for identifiers (`\p{N}` above).
	for _, r := range tok {
		if !(unicode.IsLetter(r) || unicode.IsNumber(r) || r == '_') {
			return false
		}
	}
	return tok != ""
}

// mergeTemplateQuestionRuns ports code_reader.py:165's lookahead-gated glue
// as a post-tokenization pass (see this file's deviation-1 doc for why a
// single RE2 alternative cannot express it). For every "<" token, scan
// forward: if every token before the next "<"/">"/end-of-input is
// templateGlueAllowed AND at least one is "?", AND the run is terminated
// by a ">" (not end-of-input, not another "<"), collapse the whole run
// (the "<", every token in between, and the ">") into one string token.
// Otherwise leave every token untouched and continue scanning from the
// very next token, exactly as a failed lookahead leaves Python's tokenizer
// to fall through to ordinary per-character matching.
func mergeTemplateQuestionRuns(tokens []string) []string {
	out := make([]string, 0, len(tokens))
	for i := 0; i < len(tokens); i++ {
		if tokens[i] != "<" {
			out = append(out, tokens[i])
			continue
		}
		sawQuestion := false
		end := -1
		for j := i + 1; j < len(tokens); j++ {
			if tokens[j] == ">" {
				end = j
				break
			}
			if tokens[j] == "<" || !templateGlueAllowed(tokens[j]) {
				break
			}
			if tokens[j] == "?" {
				sawQuestion = true
			}
		}
		if end == -1 || !sawQuestion {
			out = append(out, tokens[i])
			continue
		}
		out = append(out, strings.Join(tokens[i:end+1], ""))
		i = end
	}
	return out
}

// accumulateMacros ports the macro-gluing half of CodeReader.generate_tokens
// (code_reader.py:171-186), a step this package's regex tokenizer alone
// cannot express (the base pattern matches a bare '#' as ONE character,
// exactly like every other single-char catch-all token -- gluing it to
// everything up to the next unescaped newline is sequential state Python
// keeps across the regex's own match loop, not a single alternative).
//
// BUG FIX: without this, `#if defined(X)` tokenizes as "#", "if", " ",
// "defined", "(", "X", ")" -- separate tokens, not one directive -- and
// "if" then matches condition_counter as a REAL condition (harmlessly,
// landing on whatever function is current at global scope) while
// "defined(X)" gets fed to CLikeStates as if it were a FUNCTION
// DECLARATION ("defined" tried as a name, "(X)" as its parameter list),
// corrupting the state machine for every token that follows. Every
// checked-in fixture happened to declare its guarded function on the very
// next line with braces on the SAME line (K&R style), which coincidentally
// resynced the corrupted state (stateDecToImp's not-an-identifier fallback
// reverts to stateGlobal on any non-alpha token) before it could matter --
// caught only once a fixture used a directive followed by content the
// coincidence didn't cover.
func accumulateMacros(raw []string) []string {
	out := make([]string, 0, len(raw))
	macro := ""
	for _, tok := range raw {
		switch {
		case macro != "":
			if strings.Contains(tok, "\\\n") || !strings.Contains(tok, "\n") {
				macro += tok
			} else {
				out = append(out, macro, tok)
				macro = ""
			}
		case tok == "#":
			macro = tok
		default:
			out = append(out, tok)
		}
	}
	if macro != "" {
		out = append(out, macro)
	}
	return out
}

// macroPattern ports clike.py:29's macro_pattern: a token that opens with
// '#' (optionally followed by whitespace) is some preprocessor directive;
// group 1 is the directive word.
//
// BUG FIXED HERE (CHAOS-5156, codex round r2 on #2253): `\w+` here used
// to be RE2's ASCII-only class, same gap as tokenize.go's identifier
// alternative before that earlier fix (finding #5) -- but this one
// mattered for a DIFFERENT reason: with an ASCII-only class, `#ifé`
// matched only as far as "if" (an ASCII run), so macroDirective's exact
// string comparison saw the directive word "if" and (wrongly) bumped a
// condition. Python's `\w+` (Unicode mode) matches the WHOLE word "ifé"
// in one greedy run, which then fails every `== "if"/"ifdef"/"elif"`
// comparison, exactly as real lizard measures (the directive is not
// recognised, no condition bump). `[\p{L}\p{N}_]+` (this file's existing
// Unicode-aware identifier class) fixes this the same way, greedily
// consuming the non-ASCII suffix so the word can never falsely match a
// shorter ASCII-only directive name.
//
// BUG FIXED HERE (CHAOS-5156, cfamily confirmation pass on #2253's r3):
// `\s*` here is STILL RE2's ASCII-only whitespace class -- a residual of
// the SAME class the whitespace-widening work fixed everywhere else,
// missed because this regex lives in a separate var, not inside
// buildTokenPattern. `#<U+0085>if` (a directive separated from `#` by
// NEL rather than ASCII whitespace) failed to match this pattern at all
// (falling through to a "not a directive" no-op), so the `#if` bump
// never happened. Confirmed against real lizard 1.23.0: `int f() {\n#
// <U+0085>if X\nreturn 0;\n}` measures [2] (the #if bump counted); this
// port measured [1] before this fix. Built from the SAME named,
// pinned pythonExtraWhitespaceRegexClass the tokenizer's own
// whitespace-run alternative uses (buildTokenPattern), rather than a
// second hand-copied class, so the two can never drift apart again.
var macroPattern = regexp.MustCompile(`(?s)^#[\t\v\f\r ` + pythonExtraWhitespaceRegexClass + `\p{Z}]*([\p{L}\p{N}_]+)`)

// preprocessor ports CLikeReader.preprocess (clike.py:46-65) as an explicit
// one-token-at-a-time state machine rather than a batch pass.
//
// This MUST be streaming, not a separate full pass over the token slice,
// because Python's real pipeline is a chain of lazy generators
// (code_reader.py's `generate_tokens`, then preprocessing, comment_counter,
// line_counter, token_counter, condition_counter, each wrapping the last)
// driven one token at a time by CodeReader.__call__ (code_reader.py:208-224):
// every token is fully preprocessed, comment-filtered, condition-counted
// AND fed to the parallel state machines before the next token is even
// pulled. Concretely, this is what makes a `#if`/`#ifdef`/`#elif` bump the
// condition counter against whichever function is ACTUALLY current at that
// point in the file -- a two-pass implementation (preprocess the whole
// file, then walk the result) would run every directive's AddCondition
// before any state machine had processed a single earlier token, crediting
// every preprocessor conditional in the file to the global pseudo-function
// instead of whatever it is really inside. See Analyze's single loop.
type preprocessor struct{ tilde bool }

// step ports one iteration of the preprocess generator body. It returns at
// most one output token per input token (ok reports whether one was
// produced) -- tilde-gluing is the only case that consumes an input token
// without producing an output, which is why tilde is buffered across calls
// instead of needing lookahead.
//
// Two effects matter for complexity:
//   - A `~` token is glued onto the next token ("~" + "Foo" -> "~Foo") so
//     CLikeStates recognises a destructor name as a function start
//     (token[0] in "_~", clike.go).
//   - An ENTIRE preprocessor directive -- not just the leading '#', the
//     whole `#define ...`/`#pragma ...`/etc. token the tokenizer already
//     produced as one unit (code_reader.py:171-186) -- is swallowed: for
//     `#if`/`#ifdef`/`#elif` it bumps the condition counter once (an
//     untaken branch still costs a path, matching lizard's model of
//     preprocessor conditionals as control flow); every other directive,
//     `#define` included, contributes nothing at all. This is WHY a
//     macro body is never scanned for keywords: the whole directive is
//     one token, and that token is discarded before any other state
//     machine ever sees it.
//
// All-whitespace tokens are dropped except a bare "\n" (clike.py:54),
// matching the tokenizer's own emission of horizontal whitespace as a
// single run per code_reader.py:169.
func (p *preprocessor) step(tok string, ctx *Context) (out string, ok bool) {
	switch {
	case tok == "~":
		p.tilde = true
		return "", false
	case p.tilde:
		p.tilde = false
		return "~" + tok, true
	case !isAllSpace(tok) || tok == "\n":
		if word, directive := macroDirective(tok); directive {
			if word == "if" || word == "ifdef" || word == "elif" {
				ctx.AddCondition(1)
			}
			return "", false
		}
		return tok, true
	default:
		return "", false
	}
}

func macroDirective(tok string) (string, bool) {
	m := macroPattern.FindStringSubmatch(tok)
	if m == nil {
		return "", false
	}
	return m[1], true
}

// BUG FIXED HERE (CHAOS-5156, codex round r2 on #2253): this used to
// switch on a fixed ASCII whitespace set, the same gap as the tokenizer's
// own whitespace-run alternative above -- a non-ASCII space (e.g. NBSP)
// read as non-whitespace here too. unicode.IsSpace matches Go's own
// broader definition (ASCII whitespace + Unicode White_Space property,
// including NBSP), a closer match to Python's `token.isspace()` than the
// fixed switch was.
//
// BUG FIXED HERE (CHAOS-5156, codex round r3 on #2253): unicode.IsSpace
// alone still misses U+001C-U+001F (see pythonExtraWhitespaceRunes's
// doc). Checked against the SAME named set the tokenizer regex above
// uses (pythonExtraWhitespaceRunes) rather than a second hand-copied
// range, so the two can never drift out of sync with each other again --
// this function classifies the SAME tokens that regex produces.
func isAllSpace(tok string) bool {
	if tok == "" {
		return true
	}
	for _, r := range tok {
		if unicode.IsSpace(r) {
			continue
		}
		if isPythonExtraWhitespaceRune(r) {
			continue
		}
		return false
	}
	return true
}

func isPythonExtraWhitespaceRune(r rune) bool {
	for _, extra := range pythonExtraWhitespaceRunes {
		if r == extra {
			return true
		}
	}
	return false
}

// isComment ports comment_counter's drop-the-token half (lizard.py:532-537
// via CCppCommentsMixin.get_comment_from_token, clike.py:17-20): both
// comment forms are single tokens (produced by the `/\*.*?\*/` and
// `//...` alternatives above), and neither reaches condition_counter or any
// parallel state machine.
func isComment(tok string) bool {
	return strings.HasPrefix(tok, "/*") || strings.HasPrefix(tok, "//")
}

// oneOf reports whether tok is exactly one of the single characters in
// delims -- the Go equivalent of Python's `token in "=;{})"` membership
// test, which is only ever applied here to single-character terminator
// tokens (read_until_then's usage throughout clike.py).
func oneOf(tok, delims string) bool {
	return len(tok) == 1 && strings.ContainsRune(delims, rune(tok[0]))
}
