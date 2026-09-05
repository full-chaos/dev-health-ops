package lizardcc

// This file ports lizard_languages/csharp.py: CSharpReader + CSharpStates,
// a CLikeStates subclass (clike.go) reusing clike.go's shared hooks
// (globalState/decToImpState) instead of duplicating its ~250 lines.
// Unlike Java (java.go), C# never uses sub_state -- every override here is
// a plain state-function replacement, the same shape clike.go's own hooks
// already exist to support.

// csharpConditions ports CSharpReader's separated condition categories
// (csharp.py:13-16): the base set plus `??` (null-coalescing) alongside
// `?` as ternary-like operators.
var csharpConditions = map[string]bool{
	"if": true, "for": true, "while": true, "catch": true,
	"&&": true, "||": true,
	"case": true,
	"?":    true, "??": true,
}

// csharpAddition is CSharpReader.generate_tokens's addition (csharp.py:29-31):
// just `??`, layered on top of CLikeReader's own C++ raw-string/float
// additions (csharp.py calls `CLikeReader.generate_tokens`, not
// `CodeReader.generate_tokens` directly, unlike Kotlin/Scala/Swift).
const csharpAddition = cLikeAddition + `|(?:\?\?)`

var csharpTokenPattern = buildTokenPattern(csharpAddition)

// AnalyzeCSharp is the AnalyzerFunc for C# (.cs). It reuses
// CLikeNestingStackStates unchanged (csharp.py:22: parallel_states =
// [CSharpStates, CLikeNestingStackStates]) -- C# has no equivalent of
// CppRValueRefStates.
//
// BUG FIXED HERE (CHAOS-5156, found by codex round r1 on #2268, confirmed
// against real lizard 1.23.0): an earlier revision ran preprocess as a
// SEPARATE batch pass over the whole token slice before any state machine
// saw a single token (see this file's git history for the removed
// `preprocess` helper) -- exactly the two-pass shape tokenize.go's
// `preprocessor` doc already warns against for C/C++. Every `#if`/`#ifdef`/
// `#elif` bump landed while `ctx.current` was still `&ctx.global`, since
// nothing had been "entered" yet, so EVERY preprocessor conditional in a C#
// file was misattributed to global scope instead of whichever function (or
// none) was actually active at that point in the stream. Confirmed with a
// `#if` inside a method body: this reader produced 2, real lizard measures
// 3. Fixed by interleaving preprocess with condition-counting and dispatch
// one token at a time, exactly matching clike.go's Analyze.
func AnalyzeCSharp(path, source string) ([]int, bool, error) {
	ctx := NewContext()
	ctx.SetPath(path)
	// accumulateMacros (tokenize.go) is required here too: C# has #if/
	// #region-style directives, and without it they leak as separate
	// tokens exactly the way they did for C/C++ before that fix.
	raw := accumulateMacros(csharpTokenPattern.FindAllString(source, -1))

	cs := newCSharpMachine(ctx)
	ns := newNestingStates(ctx)
	pp := &preprocessor{}

	// ONE loop, in token order: preprocess -> comment filter -> condition
	// count -> parallel state dispatch, for each token before the next is
	// even considered -- see clike.go's Analyze for the identical pattern
	// and tokenize.go's preprocessor doc for why a two-pass version
	// misattributes every #if/#ifdef/#elif to the wrong function.
	for _, rawTok := range raw {
		tok, ok := pp.step(rawTok, ctx)
		if !ok {
			continue
		}
		// Dropped here for the same reason clike.go's Analyze drops it:
		// line_counter consumes a bare "\n" before condition_counter or
		// the parallel states ever see it (lizard.py:554-568). Forwarding
		// it broke Allman-style braces (`int F()\n{`) -- see clike.go's
		// Analyze for the fixture that caught it.
		if tok == "\n" {
			continue
		}
		if isComment(tok) {
			// Same `#lizard forgive`/"GENERATED CODE" handling as
			// clike.go's Analyze -- see HandleCommentDirectives' doc
			// (counter.go) and clike.go's Analyze for the fixture that
			// proved this against real lizard 1.23.0.
			if ctx.HandleCommentDirectives(tok) {
				break
			}
			continue
		}
		if csharpConditions[tok] {
			ctx.AddCondition(1)
		}
		cs.feed(tok)
		ns.feed(tok)
	}
	return ctx.Complexities, false, nil
}

// csharpMachine wraps clikeStates with C#'s overrides. Python's
// in_primary_constructor field is dropped: its only reader is
// try_new_function's override, which this package never needs (no name
// tracking), and the flag is always false again by the time anything else
// could observe it (statePrimaryConstructor resets it the instant the
// parameter list's brackets balance, before returning to
// stateClassDeclaration) -- provably inert here, not merely simplified.
type csharpMachine struct {
	m   *clikeStates
	ctx *Context
}

func newCSharpMachine(ctx *Context) *csharpMachine {
	c := &csharpMachine{ctx: ctx}
	c.m = newCLikeStates(ctx)
	c.m.globalState = c.stateGlobal
	c.m.decToImpState = c.stateDecToImp
	c.m.state = c.stateGlobal
	return c
}

func (c *csharpMachine) feed(tok string) { c.m.feed(tok) }

// stateGlobal ports CSharpStates._state_global (csharp.py:38-42): a
// class/struct/record declaration is intercepted before falling back to
// CLikeStates' own dispatch.
func (c *csharpMachine) stateGlobal(tok string) {
	if tok == "class" || tok == "struct" || tok == "record" {
		c.m.state = c.stateClassDeclaration
		return
	}
	c.m.stateGlobal(tok)
}

// stateDecToImp ports CSharpStates._state_dec_to_imp (csharp.py:24-29): an
// expression-bodied member (`int X => 1;`) confirms immediately and reads
// its body as a bare expression up to ';', falling back to CLikeStates'
// own declaration-to-implementation dispatch otherwise.
func (c *csharpMachine) stateDecToImp(tok string) {
	if tok == "=>" {
		c.m.ctx.ConfirmNewFunction()
		c.m.state = c.stateExpressionBody
		return
	}
	c.m.stateDecToImp(tok)
}

// stateExpressionBody ports _state_expression_body (csharp.py:31-35).
func (c *csharpMachine) stateExpressionBody(tok string) {
	if tok == ";" {
		c.m.ctx.EndOfFunction()
		c.m.state = c.stateGlobal
	}
}

// stateClassDeclaration ports _state_class_declaration (csharp.py:44-51).
// The class/struct/record NAME itself is never read (this package tracks
// no names), so only the primary-constructor and body-open branches have
// any effect.
func (c *csharpMachine) stateClassDeclaration(tok string) {
	switch {
	case tok == "(":
		c.m.state = c.statePrimaryConstructor
	case tok == "{":
		c.m.state = c.stateGlobal
	}
}

// statePrimaryConstructor ports _state_primary_constructor (csharp.py:53-56):
// skip a C# 12 primary constructor's parameter list without treating it as
// a function.
func (c *csharpMachine) statePrimaryConstructor(tok string) {
	c.m.brCount += bracketDelta(tok, "(", ")")
	if c.m.brCount == 0 {
		c.m.state = c.stateClassDeclaration
	}
}
