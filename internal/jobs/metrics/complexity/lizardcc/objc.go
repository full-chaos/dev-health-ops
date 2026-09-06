package lizardcc

// This file ports lizard_languages/objc.py: ObjCReader + ObjCStates, a
// CLikeStates subclass (clike.go) reusing clike.go's shared hooks
// (globalState/decToImpState) exactly the way csharpMachine (csharp.go)
// does -- see that file's own doc for the wrapper shape this mirrors: no
// separate `state` field on objcMachine at all, every ObjC-specific state
// method is assigned DIRECTLY into the shared *clikeStates' own `state`
// field as a bound method value, so objcMachine.feed can simply delegate
// to `m.feed`.
//
// ObjCReader has NO generate_tokens override (objc.py has none at all) and
// NO condition-category override either -- both are inherited from
// CLikeReader/CodeReader UNCHANGED, so this file reuses tokenize.go's
// GenerateTokens and clike.go's shared `conditions` map directly, the same
// way java.go does (java_parity_test.go's own doc: "Java has no conditions
// map of its own").
//
// ObjCReader.parallel_states is [ObjCStates, CLikeNestingStackStates] --
// TWO machines, not three: like C# (csharp.go), Objective-C has no
// equivalent of CppRValueRefStates (objc.py never mentions rvalue
// references), so AnalyzeObjC's driver loop omits it too.

// objcMachine wraps clikeStates with ObjC's overrides.
type objcMachine struct {
	m   *clikeStates
	ctx *Context
}

func newObjCMachine(ctx *Context) *objcMachine {
	o := &objcMachine{ctx: ctx}
	o.m = newCLikeStates(ctx)
	o.m.globalState = o.stateGlobal
	o.m.decToImpState = o.stateDecToImp
	o.m.state = o.stateGlobal
	return o
}

func (o *objcMachine) feed(tok string) { o.m.feed(tok) }

// stateGlobal ports ObjCStates._state_global (objc.py:26-30): run
// CLikeStates' own dispatch FIRST (Python's `super()._state_global(token)`),
// then apply ObjC's two overrides unconditionally, based on tok's raw
// value alone, regardless of what the base call just did.
//
// This unconditional-after-super shape is safe because the two trigger
// tokens never collide with a case the base dispatch would have
// "correctly" claimed for something else in a way this port needs to
// preserve differently:
//   - "typedef" IS alpha, so the base call's tryNewFunction("typedef")
//     already fired, wrongly treating the keyword itself as a candidate
//     function name -- this override DISCARDS that by forcing state to
//     stateTypedef instead, which is the entire reason it exists.
//   - "(" matches none of the base dispatch's own branches (not alpha,
//     not "_"/"~", not "["), so the base call is a true no-op for it;
//     this override is the ONLY thing that reacts to a bare "(" reaching
//     global scope (an ObjC method's parenthesized return type, e.g.
//     `- (void)foo`, when nothing already redirected state away from
//     stateGlobal for the preceding "-"/"+").
func (o *objcMachine) stateGlobal(tok string) {
	o.m.stateGlobal(tok)
	switch tok {
	case "typedef":
		o.m.state = o.stateTypedef
		o.stateTypedef(tok)
	case "(":
		o.m.state = o.m.stateDec
		o.m.stateDec(tok)
	}
}

// stateTypedef ports the @CodeStateMachine.read_until_then(';')-decorated
// _typedef (objc.py:71-73): skip every token of a `typedef ... ;`
// declaration (never a function), returning to ObjC's OWN stateGlobal
// (not CLikeStates' base) once the terminating ";" is seen -- Python's
// wrapped body calls `self.next(self._state_global)`, which resolves to
// the subclass's override at runtime, same as every other bare
// `self._state_global` reference in this file.
func (o *objcMachine) stateTypedef(tok string) {
	if tok == ";" {
		o.m.state = o.stateGlobal
	}
}

// stateDecToImp ports ObjCStates._state_dec_to_imp (objc.py:32-40).
//
// The "did the base call confirm and enter a plain C-style function body"
// check Python spells as `if self._state != self._state_imp` is ported
// here as `tok != "{"` instead of a func-value comparison (Go function
// values are not comparable except to nil -- the same reason scala.go
// tracks inExpectFunctionBody as an explicit bool rather than comparing
// state to a method value): clikeStates.stateDecToImp (clike.go) has
// EXACTLY ONE branch that ends in stateImp within a single synchronous
// call -- its own `case tok == "{"` -- every other branch (const/&/&&,
// throw, throws, "->", noexcept, "(", ":", "[", the alpha/non-alpha
// fallbacks) transitions to some OTHER intermediate state. Since this
// check runs synchronously right after that one call, for the SAME token,
// "did it reach stateImp" and "was tok literally \"{\"" are exactly
// equivalent here.
//
// PRESERVED, NOT FIXED: for any OTHER token the base call doesn't
// immediately confirm-and-enter (e.g. "const", which the base handles as
// a pure no-op that stays in stateDecToImp itself), this override treats
// that token as if it were a NEW ObjC method name regardless -- restarting
// a function unconditionally. A C++-qualified method written in a .m/.mm
// file (`int f() const { ... }`) would have its real function "f" silently
// discarded and replaced by a bogus restart on "const" this way. Confirmed
// against real lizard 1.23.0, not merely reasoned from source (see
// TestGoMatchesLizardGoldenObjC's own fixture for this).
func (o *objcMachine) stateDecToImp(tok string) {
	if tok == "+" || tok == "-" {
		o.m.state = o.stateGlobal
		return
	}
	o.m.stateDecToImp(tok)
	if tok != "{" {
		o.ctx.RestartNewFunction()
		o.m.state = o.stateObjcDecBegin
	}
}

// stateObjcDecBegin ports _state_objc_dec_begin (objc.py:42-49): waiting
// for either a ":" (another selector segment follows, e.g.
// `foo:(type)param ANOTHER_PART:` in a multi-part ObjC selector) or the
// method body's opening "{". Anything else abandons back to stateGlobal
// (this is not the method declaration shape it looked like).
func (o *objcMachine) stateObjcDecBegin(tok string) {
	switch tok {
	case ":":
		o.m.state = o.stateObjcDec
	case "{":
		o.m.stateEnteringImp(tok)
	default:
		o.m.state = o.stateGlobal
	}
}

// stateObjcDec ports _state_objc_dec (objc.py:51-60): after a selector
// segment's ":", either a parameter type's "(" opens (-> stateObjcParamType),
// a "," is skipped (multiple params in one segment -- rare but tolerated),
// "{" starts the body directly (a selector segment with no typed
// parameter), or anything else is another bare selector keyword segment,
// returning to stateObjcDecBegin to wait for its own ":" or the body.
func (o *objcMachine) stateObjcDec(tok string) {
	switch tok {
	case "(":
		o.m.state = o.stateObjcParamType
	case ",":
		// no-op: skip a comma between old-style multiple parameters.
	case "{":
		o.m.stateEnteringImp(tok)
	default:
		o.m.state = o.stateObjcDecBegin
	}
}

// stateObjcParamType ports _state_objc_param_type (objc.py:62-64): skip a
// parameter's parenthesized type until its closing ")".
func (o *objcMachine) stateObjcParamType(tok string) {
	if tok == ")" {
		o.m.state = o.stateObjcParam
	}
}

// stateObjcParam ports _state_objc_param (objc.py:66-67): unconditionally
// skip exactly one token (the parameter's own name) before resuming
// stateObjcDec to look for the next selector segment.
func (o *objcMachine) stateObjcParam(tok string) {
	o.m.state = o.stateObjcDec
}

// AnalyzeObjC is the AnalyzerFunc for Objective-C (.m/.mm). It reuses
// tokenize.go's GenerateTokens (CLikeReader's own tokenizer, unmodified --
// ObjCReader defines no generate_tokens override) and clike.go's shared
// `conditions` map (ObjCReader defines no condition-category override
// either), driving objcMachine + CLikeNestingStackStates in parallel --
// see this file's package doc for why CppRValueRefStates is omitted.
func AnalyzeObjC(path, source string) ([]int, bool, error) {
	ctx := NewContext()
	ctx.SetPath(path)
	raw := GenerateTokens(source)

	oc := newObjCMachine(ctx)
	ns := newNestingStates(ctx)
	pp := &preprocessor{}

	for _, rawTok := range raw {
		tok, ok := pp.step(rawTok, ctx)
		if !ok {
			continue
		}
		if tok == "\n" {
			continue
		}
		if isComment(tok) {
			if ctx.HandleCommentDirectives(tok) {
				break
			}
			continue
		}
		if conditions[tok] {
			ctx.AddCondition(1)
		}
		oc.feed(tok)
		ns.feed(tok)
	}
	return ctx.Complexities, false, nil
}
