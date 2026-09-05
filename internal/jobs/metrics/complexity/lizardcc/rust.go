package lizardcc

// This file ports lizard_languages/rust.py: RustReader + RustStates.
// RustStates adds nothing but FUNC_KEYWORD="fn" to GoLikeStates
// (golike.go) -- no extraGlobal hook needed, same as Go.

// rustConditions ports RustReader's separated condition categories
// (rust.py:14-18): the usual set PLUS `match` (Rust pattern matching,
// scored per-keyword rather than per-arm the way Kotlin's `when` is) and
// `where` (a trait-bound clause -- an unusual inclusion, ported as-is since
// this package's contract is numeric equality with lizard, not agreement
// with its choices), no `case` keyword (Rust uses match arms), and `?`
// scored even though it is Rust's error-propagation operator, not a
// ternary -- lizard's own condition set does not distinguish the two uses
// of the same token.
var rustConditions = map[string]bool{
	"if": true, "for": true, "while": true, "catch": true,
	"match": true, "where": true,
	"&&": true, "||": true,
	"?": true,
}

// rustAddition is RustReader.generate_tokens's addition (rust.py:27-28):
// lifetimes and loop labels (`'a`, `'outer`) as ONE token each, so the
// leading `'` is never mistaken for the start of a char literal (which
// requires a closing `'` the base pattern's char-literal alternative would
// otherwise search for, into the wrong place, on a lone lifetime).
//
// Note Python REPLACES `addition` here (`addition = r"|(?:'\w+\b)"`) rather
// than appending to it -- RustReader passes no further addition of its own
// through CLikeReader either, so the net effect is identical either way;
// ported as a plain constant rather than a function to keep that fact
// visible rather than hidden behind an unused parameter.
//
// BUG FIXED HERE (CHAOS-5156, codex round r2 on #2266): this used RE2's
// `\w`, ASCII-only like every other identifier class in this package
// before its own fix (see tokenize.go's buildTokenPattern doc) -- a
// Unicode lifetime name (`'é`) was never glued into one token here, so
// the base pattern's char-literal alternative or catch-all consumed from
// the leading `'` into later text (including a generic's closing `>`),
// corrupting the token stream and losing the enclosing function
// entirely. Confirmed against real lizard 1.23.0: `fn f<'é>(x: &'é str)
// -> &'é str { if x == x {} }` measures [2] (1 function); this port
// measured [] (function LOST) before this fix. `[\p{L}\p{N}_]+` matches
// this package's shared identifier class (tokenize.go); the trailing
// `\b` is dropped as redundant -- a greedy Unicode run already stops at
// the first non-identifier byte, and RE2's `\b` is itself defined only
// over ASCII word characters, so keeping it risked NOT matching at a
// boundary immediately following a non-ASCII identifier character.
const rustAddition = `|(?:'[\p{L}\p{N}_]+)`

var rustTokenPattern = buildTokenPattern(rustAddition)

// AnalyzeRust is the AnalyzerFunc for Rust (.rs).
func AnalyzeRust(path, source string) ([]int, bool, error) {
	ctx := NewContext()
	ctx.SetPath(path)
	// BUG FIXED HERE (CHAOS-5156, codex round r2 on #2266): same gap as
	// go_lang.go's AnalyzeGo -- see that fix's comment.
	raw := mergeTemplateQuestionRuns(rustTokenPattern.FindAllString(source, -1))
	tokens := filterWhitespaceKeepNewline(raw) // RustReader has no .preprocess override
	root := newRustMachine(ctx)
	return runGoLikeFamily(tokens, rustConditions, root, ctx)
}

type rustMachine struct {
	g *goLike
}

func newRustMachine(ctx *Context) *rustMachine {
	m := &rustMachine{}
	m.g = newGoLike(ctx, "fn", nil, func() subMachine { return newRustMachine(ctx) })
	return m
}

func (m *rustMachine) feed(tok string) bool { return m.g.feed(tok) }
