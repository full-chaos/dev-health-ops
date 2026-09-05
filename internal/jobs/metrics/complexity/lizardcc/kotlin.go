package lizardcc

// This file ports lizard_languages/kotlin.py: KotlinReader + KotlinStates,
// a GoLikeStates subclass (golike.go) with FUNC_KEYWORD "fun".

// kotlinConditions ports KotlinReader's separated condition categories
// (kotlin.py:16-19): control-flow keywords, `&&`/`||`, NO case keyword
// (Kotlin uses `when`, scored differently -- see stateWhenCases), and the
// Elvis operator `?:` as its one ternary-like operator.
var kotlinConditions = map[string]bool{
	"if": true, "for": true, "while": true, "catch": true,
	"&&": true, "||": true,
	"?:": true,
}

// kotlinAlphaConditions is kotlinConditions' alphabetic subset, the set
// ReplaceArgumentLabels needs (swiftlabel.go) -- Kotlin shares Swift's
// argument-label rewrite via the same SwiftReplaceLabel mixin
// (kotlin.py:9).
var kotlinAlphaConditions = []string{"if", "for", "while", "catch"}

// kotlinAddition is KotlinReader.generate_tokens's addition (kotlin.py:24-31):
// backtick-quoted identifiers, `?`/`!!`-suffixed identifiers (nullable /
// non-null-asserted types), `??`, and the Elvis `?:`.
//
// BUG FIXED HERE (CHAOS-5156, codex round r2 on #2268): all three `\w+`
// alternatives here were still RE2's ASCII-only `\w`, unlike the shared
// identifier class (tokenize.go) fixed earlier -- a Unicode identifier
// (`Café?`) was never glued into one token here, so its `?` reached
// condition_counter as a real ternary. Confirmed against real lizard
// 1.23.0: `fun f(): Café? { if (true) {...} }` measures [2] (glued);
// this port measured [3] before this fix. `[\p{L}\p{N}_]+` matches this
// package's shared identifier class.
const kotlinAddition = "|`" + `[\p{L}\p{N}_]+` + "`" + `|[\p{L}\p{N}_]+\?` + `|[\p{L}\p{N}_]+\!!` + `|\?\?` + `|\?:`

var kotlinTokenPattern = buildTokenPattern(kotlinAddition)

// AnalyzeKotlin is the AnalyzerFunc for Kotlin (.kt/.kts).
func AnalyzeKotlin(path, source string) ([]int, bool, error) {
	ctx := NewContext()
	ctx.SetPath(path)
	// BUG FIXED HERE (CHAOS-5156, codex round r2 on #2268, class sweep):
	// this used to skip mergeTemplateQuestionRuns entirely, the same gap
	// found independently in go_lang.go/rust.go (#2266 r2) and csharp.go
	// (#2268 r2) -- a generic containing a `?` (e.g. `List<Foo?>`) never
	// got glued into one token, so its `?` reached condition_counter as
	// a real ternary/Elvis operand. Ordered first, before
	// preprocessSwiftLabel, matching every other reader's "merge right
	// after tokenization" placement.
	raw := mergeTemplateQuestionRuns(kotlinTokenPattern.FindAllString(source, -1))
	tokens := preprocessSwiftLabel(raw, kotlinAlphaConditions)
	root := newKotlinMachine(ctx, false)
	return runGoLikeFamily(tokens, kotlinConditions, root, ctx)
}

// kotlinMachine wraps goLike with Kotlin's extra states. inWhenCases mirrors
// KotlinStates.__init__'s optional flag (kotlin.py:36-38): true only for
// the sub-machine stateWhenCases spawns to read one `when { ... }` block.
type kotlinMachine struct {
	g           *goLike
	ctx         *Context
	inWhenCases bool
}

func newKotlinMachine(ctx *Context, inWhenCases bool) *kotlinMachine {
	k := &kotlinMachine{ctx: ctx, inWhenCases: inWhenCases}
	k.g = newGoLike(ctx, "fun", k.extraGlobal, func() subMachine { return newKotlinMachine(ctx, false) })
	k.g.funcNameState = k.stateFunctionName
	k.g.expectFunctionImplState = k.stateExpectFunctionImpl
	return k
}

func (k *kotlinMachine) feed(tok string) bool { return k.g.feed(tok) }

// extraGlobal ports KotlinStates._state_global's branches BEFORE its
// `else: super()._state_global(token)` fallback (kotlin.py:41-56), plus the
// FUNC_KEYWORD branch redirected to Kotlin's own stateFunctionName (see
// golike.go's funcNameState doc for why this needs redirecting at all).
func (k *kotlinMachine) extraGlobal(tok string) bool {
	switch tok {
	case "fun":
		k.g.ctx.PushNewFunction()
		k.g.state = k.g.funcNameState
		return true
	case "get", "set":
		k.g.ctx.PushNewFunction()
		k.g.state = k.g.expectFunctionImplState
		return true
	case "->":
		if k.inWhenCases {
			k.g.ctx.AddCondition(1)
		} else {
			k.g.ctx.PushNewFunction()
			// Python: `self._state = super(KotlinStates, self)._expect_function_impl`
			// -- the BASE class's own method, bound, NOT Kotlin's override.
			k.g.state = k.g.stateExpectFunctionImpl
		}
		return true
	case "val", "var", ",":
		k.g.state = k.stateExpectDeclarationName
		return true
	case "interface":
		k.g.state = k.stateInterface
		return true
	case "when":
		k.g.state = k.stateWhenCases
		return true
	}
	return false
}

// stateExpectDeclarationName ports _expect_declaration_name (kotlin.py:58-59).
func (k *kotlinMachine) stateExpectDeclarationName(tok string) { k.g.state = k.g.stateGlobal }

// stateExpectFunctionImpl ports Kotlin's own _expect_function_impl
// (kotlin.py:61-63): unlike the base (golike.go), this ALSO accepts '='
// (Kotlin's expression-bodied `fun f() = expr`), and does not check
// lastToken != "interface" at all.
func (k *kotlinMachine) stateExpectFunctionImpl(tok string) {
	if tok == "{" || tok == "=" {
		k.g.state = k.g.stateFunctionImpl
		k.g.stateFunctionImpl(tok)
	}
}

// stateInterface ports _interface (kotlin.py:65-67): a bare bracket-depth
// wait with no end_state, so the body only runs (and only THEN decides the
// transition) at the closing brace -- see golike.go's stateStructDefinition
// for the same no-end_state shape.
func (k *kotlinMachine) stateInterface(tok string) {
	k.g.brCount += bracketDelta(tok, "{", "}")
	if k.g.brCount == 0 {
		if tok == "}" {
			k.g.state = k.g.stateGlobal
		}
	}
}

// stateFunctionName ports Kotlin's own _function_name (kotlin.py:69-72):
// skip a `<...>` generic parameter list on the function name, falling back
// to the base for everything else.
func (k *kotlinMachine) stateFunctionName(tok string) {
	if tok == "<" {
		k.g.state = k.stateTemplate
		k.stateTemplate(tok)
		return
	}
	k.g.stateFunctionName(tok)
}

// stateTemplate ports _template (kotlin.py:74-76).
func (k *kotlinMachine) stateTemplate(tok string) {
	k.g.brCount += bracketDelta(tok, "<", ">")
	if k.g.brCount == 0 {
		k.g.state = k.g.funcNameState
	}
}

// stateWhenCases ports _when_cases (kotlin.py:78-83): a `when { ... }`
// block is read by a FRESH kotlinMachine with inWhenCases=true, so its own
// extraGlobal scores each `->` case arm as +1; the callback removes one
// (an N-arm when contributes N-1, the same "extra branches beyond the
// first" shape as an if/elif chain or a BoolOp).
func (k *kotlinMachine) stateWhenCases(tok string) {
	if tok != "{" {
		return
	}
	k.g.subState(newKotlinMachine(k.ctx, true), func() {
		k.g.ctx.AddCondition(-1)
		k.g.state = k.g.stateGlobal
	})
}
