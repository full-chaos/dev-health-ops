package lizardcc

// This file ports lizard_languages/golike.py's GoLikeStates, the shared base
// for Kotlin, Scala and Swift here (Go and Rust reuse it unmodified in a
// later PR). Unlike CLikeStates -- one opaque brace counter per function
// body (clike.go's stateImp) -- GoLikeStates tracks EVERY `{...}` scope by
// recursively cloning itself into a fresh sub-machine (code_reader.py's
// sub_state, ported in submachine.go), so a nested function literal gets
// its OWN FunctionInfo and complexity rather than folding into its
// enclosing function's count. `push_new_function` confirms a function the
// MOMENT its keyword is seen (unlike CLikeReader's try/confirm split),
// which matters: anything counted between the keyword and the body's `{`
// (e.g. a generic constraint) lands on that function permanently, with no
// later reset.
//
// Kotlin/Scala/Swift each override only `_state_global`'s dispatch (their
// own tokens first, falling back to this base for anything they don't
// special-case) and, for Scala alone, how a function body actually closes.
// Rather than three near-duplicate 80-line copies, this base takes an
// `extraGlobal` hook checked first by stateGlobal, and a `clone` factory
// each language sets to "make a fresh copy of ME" (their own wrapper
// struct, extraGlobal included) -- ports Python's polymorphic
// `self.__class__(self.context)` without Go inheritance.
type goLike struct {
	core
	ctx *Context

	funcKeyword string // GoLikeStates.FUNC_KEYWORD ("func"; "fn" for Rust, a later PR)
	extraGlobal func(tok string) bool
	clone       func() subMachine

	// funcNameState and expectFunctionImplState exist ONLY because Python's
	// self._function_name / self._expect_function_impl, referenced from
	// INSIDE golike.py's own methods below, resolve dynamically to a
	// subclass's override at runtime -- Kotlin overrides both (kotlin.go),
	// Scala overrides the second (scala.go). Every place golike.py spells
	// one of those two names as a plain `self.` reference is ported here as
	// a read of the corresponding field, defaulting to this base's own
	// method; every other self-reference in this file targets a method
	// NOTHING overrides, so it stays a direct, hardcoded call.
	funcNameState           state
	expectFunctionImplState state

	lastToken string
	brCount   int
}

func newGoLike(ctx *Context, funcKeyword string, extraGlobal func(string) bool, clone func() subMachine) *goLike {
	g := &goLike{ctx: ctx, funcKeyword: funcKeyword, extraGlobal: extraGlobal, clone: clone}
	g.state = g.stateGlobal
	g.funcNameState = g.stateFunctionName
	g.expectFunctionImplState = g.stateExpectFunctionImpl
	return g
}

func (g *goLike) feed(tok string) bool {
	exited := g.core.feed(tok)
	g.lastToken = tok
	return exited
}

// stateGlobal ports _state_global (golike.py:10-19).
func (g *goLike) stateGlobal(tok string) {
	if g.extraGlobal != nil && g.extraGlobal(tok) {
		return
	}
	switch tok {
	case g.funcKeyword:
		g.state = g.funcNameState
		g.ctx.PushNewFunction()
	case "type":
		g.state = g.stateTypeDefinition
	case "{":
		g.subState(g.clone(), nil)
	case "}":
		g.statemachineReturn()
	}
}

// stateTypeDefinition ports _type_definition (golike.py:21-22).
func (g *goLike) stateTypeDefinition(tok string) { g.state = g.stateAfterTypeName }

// stateAfterTypeName ports _after_type_name (golike.py:24-30).
func (g *goLike) stateAfterTypeName(tok string) {
	switch tok {
	case "struct":
		g.state = g.stateStructDefinition
	case "interface":
		g.state = g.stateInterfaceDefinition
	default:
		g.state = g.stateGlobal
	}
}

// stateStructDefinition ports _struct_definition (golike.py:32-34).
func (g *goLike) stateStructDefinition(tok string) {
	g.brCount += bracketDelta(tok, "{", "}")
	if g.brCount == 0 {
		g.state = g.stateGlobal
	}
}

// stateInterfaceDefinition ports _interface_definition (golike.py:36-38).
func (g *goLike) stateInterfaceDefinition(tok string) {
	g.brCount += bracketDelta(tok, "{", "}")
	if g.brCount == 0 {
		g.state = g.stateGlobal
	}
}

// stateFunctionName ports _function_name (golike.py:40-49).
//
// The "(" branch disambiguates a method receiver's parens (Go's `func (r
// R) M()`) from an anonymous function literal's OWN parameter list (`func
// (a int) int { ... }` assigned to a variable, no name at all): both reach
// here with "(" as the very next token after "func"/"fn". Python decides
// with `len(stacked_functions) > 0 and stacked_functions[-1].name !=
// '*global*'` -- ctx.InRealFunction() (counter.go) is that same check
// without name-tracking. BUG FIXED HERE: an earlier revision always chose
// stateMemberFunction, so a closure assigned to a local variable
// (`adder := func(a int) int {...}`) was misread as a method receiver's
// parens around its OWN parameter, corrupting (and ultimately losing) that
// nested function -- caught by a fixture with exactly this shape.
func (g *goLike) stateFunctionName(tok string) {
	if tok == "`" {
		return
	}
	switch tok {
	case "(":
		if g.ctx.InRealFunction() {
			g.state = g.stateFunctionDec
			g.stateFunctionDec(tok)
			return
		}
		g.state = g.stateMemberFunction
		g.stateMemberFunction(tok)
	case "{":
		g.state = g.expectFunctionImplState
		g.expectFunctionImplState(tok)
	default:
		g.state = g.stateExpectFunctionDec
	}
}

// stateExpectFunctionDec ports _expect_function_dec (golike.py:51-56).
func (g *goLike) stateExpectFunctionDec(tok string) {
	switch tok {
	case "(":
		g.state = g.stateFunctionDec
		g.stateFunctionDec(tok)
	case "<":
		g.state = g.stateGeneralize
		g.stateGeneralize(tok)
	default:
		g.state = g.stateGlobal
	}
}

// stateGeneralize ports _generalize (golike.py:58-60): skip `<...>` generic
// constraints on a function name.
func (g *goLike) stateGeneralize(tok string) {
	g.brCount += bracketDelta(tok, "<", ">")
	if g.brCount == 0 {
		g.state = g.stateExpectFunctionDec
	}
}

// stateMemberFunction ports _member_function (golike.py:62-64): a method
// receiver's own `(...)` before the method name (Go's `func (r R) M()`).
func (g *goLike) stateMemberFunction(tok string) {
	g.brCount += bracketDelta(tok, "(", ")")
	if g.brCount == 0 {
		g.state = g.funcNameState
	}
}

// stateFunctionDec ports _function_dec (golike.py:66-69): the parameter
// list. Every non-bracket token is a parameter in Python; a no-op here
// since this package tracks no names/params, matching clike.go's
// CLikeStates.stateDec.
func (g *goLike) stateFunctionDec(tok string) {
	g.brCount += bracketDelta(tok, "(", ")")
	if g.brCount == 0 {
		g.state = g.expectFunctionImplState
	}
}

// stateExpectFunctionImpl ports _expect_function_impl (golike.py:71-73).
func (g *goLike) stateExpectFunctionImpl(tok string) {
	if tok == "{" && g.lastToken != "interface" {
		g.state = g.stateFunctionImpl
		g.stateFunctionImpl(tok)
	}
}

// stateFunctionImpl ports _function_impl (golike.py:75-79): the function's
// own body is a FRESH clone, so nested functions inside it get their own
// FunctionInfo; when that clone's own matching '}' fires its
// statemachine_return, the callback closes THIS function via
// context.end_of_function() directly (GoLikeStates never uses the
// nesting-stack machinery clike.go's CLikeNestingStackStates provides --
// counter.go's AddBareNesting/PopNesting are simply never called here).
//
// The opening '{' itself is NOT fed to the clone (Python: `sub_state(state,
// callback)`, no third argument) -- the clone starts fresh on the first
// token INSIDE the body, and its own _state_global returns on the first
// bare '}' it meets directly, because every deeper '{' already spawned a
// further clone that consumes its own matching '}' first.
func (g *goLike) stateFunctionImpl(tok string) {
	g.subState(g.clone(), func() {
		g.state = g.stateGlobal
		g.ctx.EndOfFunction()
	})
}
