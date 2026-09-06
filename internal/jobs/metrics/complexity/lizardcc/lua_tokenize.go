package lizardcc

// This file ports lizard_languages/lua.py's LuaReader.generate_tokens
// (lua.py:15-21). UNLIKE Ruby (ruby_tokenize.go), Lua's generate_tokens
// calls `RubylikeReader.generate_tokens(...)` -- and RubylikeReader itself
// (rubylike.py) defines NO generate_tokens override at all, so that call
// resolves to the plain BASE `CodeReader.generate_tokens` (this package's
// buildTokenPattern), NOT RubyReader's own stateful restart-loop tokenizer
// (RubyReader.generate_tokens is a SEPARATE override on a SIBLING class,
// never in RubylikeReader's own method-resolution chain). Lua therefore
// needs none of ruby_tokenize.go's interpolation-restart machinery, the
// js_style_regex_expression decorator (LuaReader.generate_tokens carries
// no such decorator either), or MyToken's begin-offset tracking -- just
// the ordinary single-pass regex tokenizer plus Lua's own three additions.

// luaAddition ports lua.py:18-20's three-alternative concatenation, in
// the SAME order (alternation is leftmost-first in both Python's re and
// Go's RE2 -- neither is POSIX leftmost-longest -- so order here must
// match order there):
//  1. `--\[\[.*?\]\]` -- Lua's block comment. MUST be checked before
//     alternative 3 (the line-comment form), or `--[[` would be
//     (wrongly) matched as a line comment up to the first line break,
//     truncating a genuine multi-line block comment.
//  2. `\[\=*\[.*?\]\=*\]` -- Lua's "long bracket" string literal
//     (`[[...]]`, `[=[...]=]`, `[==[...]==]`, ...). NOT anchored to the
//     opening/closing `=` COUNT matching (Python's own regex doesn't
//     either -- `\=*` on each side is independent, so `[==[...]=]`
//     would satisfy this alternative too, a real lizard quirk this port
//     reproduces rather than fixes by cross-referencing the two counts).
//  3. `--.*?$` -- Lua's line comment, wrapped in an inline `(?m:...)`
//     group so `$` means "end of THIS line", matching Python's `re.M`
//     (always set by CodeReader.generate_tokens regardless of what
//     `addition` supplies) -- see ruby_tokenize.go's `=begin`/`=end`
//     doc for the same substitution.
const luaAddition = `|\-\-\[\[.*?\]\]` +
	`|\[\=*\[.*?\]\=*\]` +
	`|(?m:\-\-.*?$)`

var luaTokenPattern = buildTokenPattern(luaAddition)

// GenerateTokensLua ports the full LuaReader.generate_tokens pipeline:
// base regex tokenize, then the SAME generic-question-mark-glue and
// macro-accumulation passes every buildTokenPattern-based reader in this
// package applies (tokenize.go) -- Lua needs accumulateMacros for the
// identical reason PHP does (php_tokenize.go's own doc): Lua's own
// additions claim no bare "#" (Lua uses "--" for comments, not "#"), so a
// literal "#" (Lua's LENGTH operator, `#someTable`) falls through to the
// base pattern's bare "#" alternative and would otherwise leak as its own
// single-character token, mis-triggering the generic C-preprocessor-style
// macro-gluing convention -- reproduced here exactly, not avoided, since
// this package's job is bit-for-bit agreement with the real tool.
func GenerateTokensLua(source string) []string {
	raw := luaTokenPattern.FindAllString(source, -1)
	return accumulateMacros(mergeTemplateQuestionRuns(raw))
}
