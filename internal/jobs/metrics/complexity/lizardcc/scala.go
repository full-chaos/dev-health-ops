package lizardcc

// This file ports lizard_languages/scala.py: ScalaReader + ScalaStates, a
// GoLikeStates subclass (golike.go) with FUNC_KEYWORD "def". Scala has no
// generate_tokens override (plain CodeReader tokenizer, no addition) and no
// preprocess override (the generic whitespace-only filter,
// filterWhitespaceKeepNewline in golikedriver.go).

// scalaConditions ports ScalaReader's separated condition categories
// (scala.py:14-17): the usual control-flow set PLUS `do` (Scala's
// do/while), `&&`/`||`, `case` (Scala pattern matching), and `?` as its
// ternary-like operator.
var scalaConditions = map[string]bool{
	"if": true, "for": true, "while": true, "catch": true, "do": true,
	"&&": true, "||": true,
	"case": true,
	"?":    true,
}

var scalaTokenPattern = buildTokenPattern("")

// AnalyzeScala is the AnalyzerFunc for Scala (.scala).
func AnalyzeScala(path, source string) ([]int, bool, error) {
	ctx := NewContext()
	ctx.SetPath(path)
	raw := scalaTokenPattern.FindAllString(source, -1)
	tokens := filterWhitespaceKeepNewline(raw)
	root := newScalaMachine(ctx)
	return runGoLikeFamily(tokens, scalaConditions, root, ctx)
}

// scalaMachine wraps goLike with Scala's two overrides:
// _expect_function_impl (accepts an expression body after '=') and
// statemachine_before_return (closes a function left open in
// _expect_function_body at end of file).
type scalaMachine struct {
	g   *goLike
	ctx *Context
	// inExpectFunctionBody mirrors the ONLY thing statemachine_before_return
	// checks (`self._state == self._expect_function_body`, scala.py:36-38):
	// Go compares func values by identity awkwardly, so a bool flag set
	// exactly when goLike.state IS stateExpectFunctionBody is the direct
	// translation.
	inExpectFunctionBody bool
}

func newScalaMachine(ctx *Context) *scalaMachine {
	s := &scalaMachine{ctx: ctx}
	s.g = newGoLike(ctx, "def", nil, func() subMachine { return newScalaMachine(ctx) })
	s.g.expectFunctionImplState = s.stateExpectFunctionImpl
	s.g.beforeReturn = s.doBeforeReturn
	return s
}

func (s *scalaMachine) feed(tok string) bool { return s.g.feed(tok) }

// beforeReturn is the EOF hook golikedriver.go's runGoLikeFamily calls on
// the top-level machine (ports the `for state in parallel_states:
// state.statemachine_before_return()` sweep, lizard.py:614/620).
func (s *scalaMachine) beforeReturn() { s.doBeforeReturn() }

// doBeforeReturn ports statemachine_before_return (scala.py:36-38).
func (s *scalaMachine) doBeforeReturn() {
	if s.inExpectFunctionBody {
		s.ctx.EndOfFunction()
	}
}

// stateExpectFunctionImpl ports Scala's own _expect_function_impl
// (scala.py:25-29): an expression body (`def f(x) = x + 1`, no braces)
// starts at '=', falling back to the base (a brace body) otherwise.
func (s *scalaMachine) stateExpectFunctionImpl(tok string) {
	if tok == "=" {
		s.inExpectFunctionBody = true
		s.g.state = s.stateExpectFunctionBody
		return
	}
	s.inExpectFunctionBody = false
	s.g.stateExpectFunctionImpl(tok)
}

// stateExpectFunctionBody ports _expect_function_body (scala.py:31-34).
//
// The brace branch spawns a bare clone with NO callback (unlike
// golike.go's stateFunctionImpl) -- when that clone's own closing '}'
// fires, control returns HERE, to expect_function_body, not to
// state_global, and inExpectFunctionBody is deliberately left true: this is
// exactly the delayed-close scala.py itself relies on (see this file's
// package-level doc note below) -- a brace-bodied def is ended on the
// first Newline-flagged token AFTER its body closes, not by the brace
// closing itself.
func (s *scalaMachine) stateExpectFunctionBody(tok string) {
	if s.ctx.Newline {
		s.inExpectFunctionBody = false
		s.g.ctx.EndOfFunction()
		s.g.state = s.g.stateGlobal
		s.g.stateGlobal(tok)
		return
	}
	if tok == "{" {
		// Python passes no callback (`sub_state(ScalaStates(self.context))`)
		// -- no state change beyond core.feed's generic restore-to-saved-
		// state, and no end_of_function call here at all (see this
		// method's doc). The callback below exists ONLY to keep
		// inExpectFunctionBody accurate for doBeforeReturn's EOF check
		// (Python re-derives the equivalent fact by comparing `self._state`
		// directly every time, which this bool flag must track by hand
		// through every transition -- it is bookkeeping, not new
		// behaviour: false while delegating to the clone, matching
		// Python's `self._state` being the clone object rather than
		// `self._expect_function_body` during that whole span).
		s.inExpectFunctionBody = false
		s.g.subState(newScalaMachine(s.ctx), func() { s.inExpectFunctionBody = true })
	}
}
