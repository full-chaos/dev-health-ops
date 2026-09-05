package lizardcc

// This file ports lizard_languages/go.py: GoReader + GoStates. GoStates
// adds NOTHING to GoLikeStates (golike.go) -- `class GoStates(GoLikeStates):
// pass` -- so this is the plain base with no overrides, and no extraGlobal
// hook is needed at all.
//
// GoReader.__call__ is ALSO overridden in Python (go.py:26-38), but its
// override branches on `token.startswith('\`') and token.endswith('\`')`
// where BOTH branches do exactly what CodeReader.__call__'s default does
// (feed every parallel state, yield the token) -- the override is a no-op
// by construction, kept here as a documented fact rather than replicated,
// since golikedriver.go's shared driving loop already matches
// CodeReader.__call__ exactly.

// goConditions ports the base CodeReader conditions (GoReader declares no
// override, code_reader.py:98-113): if/for/while/catch, &&/||, case, ?.
var goConditions = map[string]bool{
	"if": true, "for": true, "while": true, "catch": true,
	"&&": true, "||": true,
	"case": true,
	"?":    true,
}

// goAddition is GoReader.generate_tokens's addition (go.py:22-23):
// backtick-quoted raw strings as ONE token, so their contents (which can
// contain anything, including text that looks like a keyword) never reach
// the tokenizer's other alternatives.
const goAddition = "|`[^`]*`"

var goTokenPattern = buildTokenPattern(goAddition)

// AnalyzeGo is the AnalyzerFunc for Go (.go).
func AnalyzeGo(path, source string) ([]int, bool, error) {
	ctx := NewContext()
	ctx.SetPath(path)
	// BUG FIXED HERE (CHAOS-5156, codex round r2 on #2266): this read
	// goTokenPattern.FindAllString directly, bypassing
	// mergeTemplateQuestionRuns (tokenize.go) entirely -- that merge pass
	// is only wired into cfamily's own GenerateTokens, not here. A
	// generic-with-question shape (`T<A ? B>`) never got glued into one
	// token, so its `?` reached condition_counter as a real ternary.
	// Confirmed against real lizard 1.23.0: `func f() int { _ = T<A ? B>
	// ; return 0 }` measures [1] (glued); this port measured [2] before
	// this fix.
	raw := mergeTemplateQuestionRuns(goTokenPattern.FindAllString(source, -1))
	tokens := filterWhitespaceKeepNewline(raw) // GoReader has no .preprocess override
	root := newGoMachine(ctx)
	return runGoLikeFamily(tokens, goConditions, root, ctx)
}

type goMachine struct {
	g *goLike
}

func newGoMachine(ctx *Context) *goMachine {
	m := &goMachine{}
	m.g = newGoLike(ctx, "func", nil, func() subMachine { return newGoMachine(ctx) })
	return m
}

func (m *goMachine) feed(tok string) bool { return m.g.feed(tok) }
