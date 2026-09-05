package lizardcc

// This file ports lizard_languages/swift.py: SwiftReader + SwiftStates, a
// GoLikeStates subclass (golike.go) that keeps the base FUNC_KEYWORD
// ("func", same as Go/Kotlin -- Swift's own class attribute of that name on
// the READER, not the states class, is dead weight in the Python source).

// swiftConditions ports SwiftReader's separated condition categories
// (swift.py:33-36): the usual control-flow set PLUS `guard`, `&&`/`||`,
// `case` (pattern matching), and a single `?` as the ternary operator.
var swiftConditions = map[string]bool{
	"if": true, "for": true, "while": true, "catch": true, "guard": true,
	"&&": true, "||": true,
	"case": true,
	"?":    true,
}

// swiftAlphaConditions is swiftConditions' alphabetic subset, fed to
// ReplaceArgumentLabels (swiftlabel.go) -- THE reason that file exists.
var swiftAlphaConditions = []string{"if", "for", "while", "catch", "guard", "case"}

// swiftAddition is SwiftReader.generate_tokens's addition (swift.py:39-44):
// backtick-quoted identifiers, `?`/`!`-suffixed identifiers (optional
// types / forced unwraps) and `??` (nil-coalescing). A bare ternary `?`
// with no preceding whitespace fuses into the PRECEDING word instead
// (`cond?a:b` would tokenize as "cond?" one token) -- a real, narrow lizard
// quirk this package reproduces rather than "fixes"; idiomatic Swift always
// spaces a ternary (`cond ? a : b`), where it is unaffected.
const swiftAddition = "|`" + `\w+` + "`" + `|\w+\?` + `|\w+\!` + `|\?\?`

var swiftTokenPattern = buildTokenPattern(swiftAddition)

// AnalyzeSwift is the AnalyzerFunc for Swift (.swift).
func AnalyzeSwift(path, source string) ([]int, bool, error) {
	ctx := NewContext()
	ctx.SetPath(path)
	raw := swiftTokenPattern.FindAllString(source, -1)
	tokens := preprocessSwiftLabel(raw, swiftAlphaConditions)
	root := newSwiftMachine(ctx)
	return runGoLikeFamily(tokens, swiftConditions, root, ctx)
}

// swiftMachine wraps goLike with Swift's extra states. Swift overrides
// neither _function_name nor _expect_function_impl, so funcNameState and
// expectFunctionImplState stay at goLike's own defaults.
type swiftMachine struct {
	g   *goLike
	ctx *Context
}

func newSwiftMachine(ctx *Context) *swiftMachine {
	s := &swiftMachine{ctx: ctx}
	s.g = newGoLike(ctx, "func", nil, func() subMachine { return newSwiftMachine(ctx) })
	s.g.extraGlobal = s.extraGlobal
	return s
}

func (s *swiftMachine) feed(tok string) bool { return s.g.feed(tok) }

// extraGlobal ports SwiftStates._state_global's branches before its
// `else: super()._state_global(token)` fallback (swift.py:47-60).
func (s *swiftMachine) extraGlobal(tok string) bool {
	switch tok {
	case "init", "subscript":
		s.g.ctx.PushNewFunction()
		s.g.state = s.g.funcNameState
		s.g.funcNameState(tok)
		return true
	case "get", "set", "willSet", "didSet", "deinit":
		s.g.ctx.PushNewFunction()
		s.g.state = s.g.expectFunctionImplState
		return true
	case "protocol":
		s.g.state = s.stateProtocol
		return true
	case "let", "var", "case", ",":
		s.g.state = s.stateExpectDeclarationName
		return true
	}
	return false
}

// stateExpectDeclarationName ports _expect_declaration_name (swift.py:62-63).
func (s *swiftMachine) stateExpectDeclarationName(tok string) { s.g.state = s.g.stateGlobal }

// stateProtocol ports _protocol (swift.py:65-68): skip a protocol body
// entirely (a bare bracket-depth wait, body runs and decides only at the
// close, same no-end_state shape as golike.go's stateStructDefinition).
func (s *swiftMachine) stateProtocol(tok string) {
	s.g.brCount += bracketDelta(tok, "{", "}")
	if s.g.brCount == 0 && tok == "}" {
		s.g.state = s.g.stateGlobal
	}
}
