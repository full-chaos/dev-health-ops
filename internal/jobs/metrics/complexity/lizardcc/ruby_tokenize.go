package lizardcc

import "strings"

// This file ports lizard_languages/ruby.py's RubyReader.generate_tokens
// (ruby.py:20-58), which itself delegates to ScriptLanguageMixIn.
// generate_common_tokens (script_language.py:26-31 -- a thin wrapper around
// CodeReader.generate_tokens, this package's buildTokenPattern) and is
// decorated with @js_style_regex_expression (already ported for TypeScript
// as mergeJSRegexLiterals, typescript_tokenize.go -- reused unmodified
// here, since the decorator operates on an already-tokenized stream and
// carries no TypeScript-specific assumption).

// rubyIdentClass is this package's shared Unicode-aware identifier class
// (tokenize.go's own `\w`-is-ASCII-only bug fix), used everywhere ruby.py
// writes a bare `\w` in its own addition -- RE2's `\w` would otherwise
// split a non-ASCII identifier the same way it did for every earlier
// reader in this port (see tokenize.go's own doc for the confirmed-against-
// real-lizard repro).
const rubyIdentClass = `[\p{L}\p{N}_]`

// rubyUntilEnd ports script_language.py:28's `_until_end` local
// (`(?:\\\n|[^\n])*`): "everything up to the next un-escaped newline",
// used here for the "#" comment-to-end-of-line alternative that
// ScriptLanguageMixIn.generate_common_tokens prepends to EVERY script
// reader's addition (script_language.py:29-31: `r"|\#" + _until_end +
// addition`). Placing it first, ahead of Ruby's own addition below,
// matches that exact concatenation order -- alternation is leftmost-first
// in both Python's re and Go's RE2 (neither is POSIX leftmost-longest),
// so a "#" always opens a comment-to-EOL token here, never falls through
// to Ruby's own symbol/percent-literal alternatives.
const rubyUntilEnd = `(?:\\\n|[^\n])*`

// rubyAddition ports ruby.py:33-42's five-line regex concatenation
// (RubyReader.generate_tokens's own `process_source` inner function),
// prefixed by the comment alternative every ScriptLanguageMixIn reader gets
// for free (see rubyUntilEnd's doc). Order preserved exactly, since a token
// starting position can only match ONE alternative and Go's regexp package
// resolves ties the same leftmost-first way Python's re does:
//
//  1. "#" comment to end of line (script_language.py, not ruby.py itself).
//  2. `=begin`/`=end` embedded-document markers, anchored to start of
//     LINE (Python's `^` under `re.M`, which CodeReader.generate_tokens
//     always sets regardless of what `addition` supplies -- ported here as
//     an inline `(?m:...)` group scoped to just these two alternatives,
//     since buildTokenPattern's own top-level flags don't include `m` and
//     nothing else in this file needs it).
//     3-6. The four `%[qQrwiI]?...` percent-literal forms (`{}`, `[]`, `<>`,
//     `()`). BUG PRESERVED, NOT FIXED, HERE: ruby.py:37's paren form uses
//     `[^\>\\]` (the SAME negated class as the angle-bracket form
//     immediately above it), not `[^\)\\]` -- almost certainly a
//     copy-paste slip in lizard itself, but this port's entire purpose is
//     bit-for-bit agreement with the real tool, not a "more correct"
//     independent reimplementation, so it is reproduced verbatim. Confirmed
//     against real lizard 1.23.0 (see ruby_parity_test.go).
//  7. `\w+:` -- Ruby's hash-shorthand symbol-key syntax (`key:`).
//  8. `\$\w+` -- global variables.
//  9. `\.+` -- one-or-more dots, so `..`/`...` range operators tokenize as
//     one unit rather than two/three separate "." tokens.
//  10. `:?\@{0,2}\w+\??\!?` -- symbols (`:sym`), instance/class variables
//     (`@x`/`@@x`), and predicate/bang method names (`valid?`/`save!`).
const rubyAddition = "|#" + rubyUntilEnd +
	"|(?m:^=begin)" +
	"|(?m:^=end)" +
	`|%[qQrwiI]?\{(?:\\.|[^\}\\])*?\}` +
	`|%[qQrwiI]?\[(?:\\.|[^\]\\])*?\]` +
	`|%[qQrwiI]?\<(?:\\.|[^\>\\])*?\>` +
	`|%[qQrwiI]?\((?:\\.|[^\>\\])*?\)` + // NOT a typo: see doc above.
	`|` + rubyIdentClass + `+:` +
	`|\$` + rubyIdentClass + `+` +
	`|\.+` +
	`|:?\@{0,2}` + rubyIdentClass + `+\??\!?`

var rubyTokenPattern = buildTokenPattern(rubyAddition)

// GenerateTokensRuby ports the full RubyReader.generate_tokens pipeline:
// the stateful, restart-driven outer loop (ruby.py:43-58) that re-tokenizes
// a truncated remainder of the source whenever it finds Ruby string
// interpolation (`#{...}`), followed by the js_style_regex_expression
// decorator (mergeJSRegexLiterals, shared with TypeScript/JavaScript).
//
// mergeTemplateQuestionRuns (the RE2 lookahead workaround for
// CodeReader.generate_tokens's generic-with-a-`?`-inside glue,
// tokenize.go) is DELIBERATELY NOT applied here, unlike every other reader
// built on buildTokenPattern. In Python this merge happens INLINE, as part
// of the single compiled regex (Python's re supports lookahead); this
// package's post-tokenization workaround only preserves token TEXT, not
// per-token source offsets, and this function's own restart logic depends
// on exact byte offsets into the CURRENT source segment (MyToken.begin in
// Python) to slice the next segment correctly -- threading corrected
// offsets through a merge pass has no realistic payoff for a construct
// (`<...?...>` angle-bracket generics) that cannot occur in valid Ruby
// grammar: the lookahead's own allowed-character class between `<` and `>`
// (`[\w\s,.?]` or the literal word "extends") excludes ":", so it can never
// span a real Ruby ternary (`a < b ? c : d`) either. If a future language
// in this package both reuses buildTokenPattern's generic alternative AND
// needs restart-based re-tokenization, that combination will need its own
// offset-preserving merge -- flagged here so the next port doesn't assume
// this file already solved it.
func GenerateTokensRuby(source string) []string {
	return mergeJSRegexLiterals(rubyGenerateTokensRaw(source))
}

// rubyGenerateTokensRaw ports ruby.py:43-58's while/for/break/else
// structure directly: process_source(source) is retokenized from scratch
// every time interpolation is detected (bracket_stack persists across
// restarts, exactly as Python's closure-captured local does), and the
// inner for loop's Python "else" clause (source = None, ending the outer
// while) is represented here by the "did we restart" flag checked after
// the inner loop.
func rubyGenerateTokensRaw(source string) []string {
	var out []string
	src := source
	bracketStack := make([]string, 0, 4)
	for {
		matches := rubyTokenPattern.FindAllStringIndex(src, -1)
		restarted := false
		for _, m := range matches {
			tokStart, tokEnd := m[0], m[1]
			tok := src[tokStart:tokEnd]

			switch {
			case tok == "{":
				bracketStack = append(bracketStack, "{")

			case tok == "}":
				popped := ""
				if len(bracketStack) > 0 {
					popped = bracketStack[len(bracketStack)-1]
					bracketStack = bracketStack[:len(bracketStack)-1]
				}
				if popped == "#{" {
					out = append(out, tok)
					src = `"` + src[tokStart+1:]
					restarted = true
				}

			case strings.HasPrefix(tok, `"`):
				if idx := strings.Index(tok, "#{"); idx >= 0 {
					first := tok[:idx]
					out = append(out, first+`"`, "${")
					bracketStack = append(bracketStack, "#{")
					src = src[tokStart+idx+2:]
					restarted = true
				}
			}

			if restarted {
				break
			}
			out = append(out, tok)
		}
		if !restarted {
			return out
		}
	}
}

// jsRegexLiteralPattern/jsRegexFlagsPattern/mergeJSRegexLiterals live in
// typescript_tokenize.go and are reused here unmodified -- see this file's
// package doc.
