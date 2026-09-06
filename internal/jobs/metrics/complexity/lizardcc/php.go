package lizardcc

// This file ports lizard_languages/php.py's PHPLanguageStates + PHPReader.
// UNLIKE every other reader ported so far (TypeScript, Ruby, Kotlin/Scala/
// Swift), PHPLanguageStates is a FLAT state machine: it never sub_states
// into a cloned/nested instance of itself. A single running `braceLevel`
// counter (php.py's brace_level) plays the same role clike.go's stateImp
// does for plain CLikeReader -- one opaque nesting depth, not a stack --
// except PHP shares that ONE counter across BOTH "are we still inside the
// enclosing class body" and "are we still inside the current function
// body" bookkeeping simultaneously (see stateFunctionBody's doc).
//
// PRESERVED, NOT FIXED, CONSEQUENCE OF THIS FLAT DESIGN: a function or
// class declared LEXICALLY INSIDE another function's body (a PHP closure
// defined inside a method, or `new class { ... }` used as an expression
// inside a method) is INVISIBLE to this state machine -- stateFunctionBody
// has no case for the "function"/"class"/"trait" tokens at all, so their
// braces are silently absorbed into the ENCLOSING function's own brace
// counting, and none of their own control-flow tokens are attributed to a
// separate function (they still bump AddCondition against whatever
// function IS current, i.e. the enclosing one, since condition-counting
// happens at the token-driving level regardless of state). Confirmed
// against real lizard 1.23.0, not merely reasoned from source (see
// TestGoMatchesLizardGoldenPHP's nested-closure fixture).
//
// NAMES DROPPED: unlike typescript.go's documented EXCEPTION (function
// names genuinely load-bearing for TypeScript's control flow), nothing in
// PHPLanguageStates branches on the VALUE of a class/trait/function name,
// an assigned-to variable name, or a parameter name -- every place
// php.py reads self.function_name/self.class_name/self.trait_name/
// self.assignments/self.last_tokens exists ONLY to build a human-facing
// name string passed to push_new_function/add_to_long_function_name/
// parameter, none of which this package's Context (counter.go) even
// exposes (see its own package doc: names/params dropped by policy).
// self.last_token is set every call but never READ anywhere in
// php.py -- entirely dead code for CC purposes. All of the above are
// dropped from this port with no behavioural effect; only the BOOLEAN
// fact "did a real class/trait name token fire" (inClass/inTrait) survives,
// since php.py:212's `self.brace_level == self.in_class` comparison reads
// in_class as a 0/1 integer, not as a name.

// phpConditions ports PHPReader's separated condition categories
// (php.py:206-209): control-flow keywords (note "elseif", not "elsif" as
// Ruby oddly also recognises -- PHP's real keyword IS "elseif", one word),
// `&&`/`||` (PHP's word forms `and`/`or` are DELIBERATELY EXCLUDED here,
// matching Python's own comment "PHP also has 'and'/'or' with different
// precedence" -- lizard's PHPReader does not count them, unlike Ruby's
// RubylikeReader which counts both spellings), `case`, and `?`.
var phpConditions = map[string]bool{
	"if": true, "elseif": true, "for": true, "foreach": true,
	"while": true, "catch": true, "match": true,
	"&&": true, "||": true,
	"case": true,
	"?":    true,
}

// phpMachine ports PHPLanguageStates (php.py:9-30, states only -- fields
// with no CC-relevant read are dropped, see this file's package doc).
type phpMachine struct {
	core
	ctx *Context

	inClass         bool
	inTrait         bool
	braceLevel      int
	bracketLevel    int
	startedFunction bool
	inMatch         bool
	matchCaseCount  int
}

func newPHPMachine(ctx *Context) *phpMachine {
	m := &phpMachine{ctx: ctx}
	m.state = m.stateGlobal
	return m
}

func (m *phpMachine) feed(tok string) bool { return m.core.feed(tok) }

// stateGlobal ports _state_global (php.py:32-83). Case order mirrors
// Python's if/elif chain exactly -- notably "}" while inMatch is handled
// by the EARLIER case, before the generic brace-counting case, so a
// match expression's own closing brace never touches braceLevel at all
// (php.py's elif chain has the exact same short-circuit).
func (m *phpMachine) stateGlobal(tok string) {
	switch {
	case tok == "use":
		m.state = m.stateUse
	case tok == "class":
		m.state = m.stateClassDeclaration
	case tok == "trait":
		m.state = m.stateTraitDeclaration
	case tok == "function":
		m.state = m.stateFunctionName
	case tok == "fn":
		// Arrow functions (PHP 7.4+) are deliberately skipped, matching
		// Python's bare `pass` -- lizard does not score them as functions.
	case tok == "match":
		m.inMatch = true
		m.matchCaseCount = 0
		m.state = m.stateMatchExpression
	case tok == "if" || tok == "switch" || tok == "for" || tok == "foreach" ||
		tok == "while" || tok == "catch":
		m.state = m.stateConditionExpected
	case tok == "public" || tok == "private" || tok == "protected" || tok == "static":
		// Visibility/staticness modifiers are skipped.
	case tok == "=>" && m.inMatch:
		m.matchCaseCount++
	case tok == "}" && m.inMatch:
		m.inMatch = false
		for i := 0; i < m.matchCaseCount-1; i++ {
			m.ctx.AddCondition(1)
		}
		m.matchCaseCount = 0
	case tok == "{":
		m.braceLevel++
	case tok == "}":
		m.braceLevel--
		if m.braceLevel == 0 {
			m.inClass = false
			m.inTrait = false
		}
	}
}

// stateUse ports _state_use (php.py:85-89): skip everything in a
// `use ...;` statement (namespace imports, `use function`, `use const`).
func (m *phpMachine) stateUse(tok string) {
	if tok == ";" {
		m.state = m.stateGlobal
	}
}

// stateTraitDeclaration ports _trait_declaration (php.py:91-96). The
// driver never forwards an all-whitespace token (filterWhitespaceKeepNewline
// drops them, and a bare "\n" is consumed entirely by runGoLikeFamily
// before root.feed ever sees it -- golikedriver.go), so Python's
// `token and not token.isspace()` guard is always true for anything
// reaching this method and is omitted here.
func (m *phpMachine) stateTraitDeclaration(tok string) {
	switch {
	case tok != "{" && tok != "(":
		m.inTrait = true
		m.state = m.stateGlobal
	case tok == "{":
		m.braceLevel++
		m.state = m.stateGlobal
	}
}

// stateClassDeclaration ports _class_declaration (php.py:98-103).
// PRESERVED, NOT FIXED: an anonymous class (`new class extends Foo { ...
// }`, no name token before "extends"/"implements"/"{") never sets inClass
// true at all -- only a genuine name token does -- so its methods are
// scored using braceLevel==0 as the closing test (stateFunctionBody),
// exactly as if they were declared directly at file scope. Confirmed
// against real lizard 1.23.0 (TestGoMatchesLizardGoldenPHP's anonymous-
// class fixture).
func (m *phpMachine) stateClassDeclaration(tok string) {
	switch {
	case tok != "{" && tok != "(" && tok != "extends" && tok != "implements":
		m.inClass = true
		m.state = m.stateGlobal
	case tok == "{":
		m.braceLevel++
		m.state = m.stateGlobal
	}
}

// stateFunctionName ports _function_name (php.py:105-131). Every naming
// branch collapses to "confirm a function" (see this file's package doc);
// the anonymous-function ("(" immediately after "function") branch also
// enters parameter-scanning at bracketLevel 1 directly, since the "("
// that triggered this branch IS the parameter list's own opener.
func (m *phpMachine) stateFunctionName(tok string) {
	switch {
	case tok != "(":
		m.state = m.stateFunctionArgs
	case tok == "(":
		m.bracketLevel = 1
		m.state = m.stateFunctionArgsContinue
		m.ctx.PushNewFunction()
		m.startedFunction = true
	}
}

// stateFunctionArgs ports _function_args (php.py:133-141): wait for the
// parameter list's opening "(" (whatever named the function was already
// consumed by stateFunctionName).
func (m *phpMachine) stateFunctionArgs(tok string) {
	if tok == "(" {
		m.bracketLevel = 1
		m.ctx.PushNewFunction()
		m.startedFunction = true
		m.state = m.stateFunctionArgsContinue
	}
}

// stateFunctionArgsContinue ports _function_args_continue (php.py:143-153).
// The `token.startswith('$')` parameter branch is a dropped no-op here
// (parameter values never feed CC -- see this file's package doc).
func (m *phpMachine) stateFunctionArgsContinue(tok string) {
	switch tok {
	case "(":
		m.bracketLevel++
	case ")":
		m.bracketLevel--
		if m.bracketLevel == 0 {
			m.state = m.stateFunctionReturnTypeOrBody
		}
	}
}

// stateFunctionReturnTypeOrBody ports _function_return_type_or_body
// (php.py:155-166): ":" opens a return-type declaration to skip past,
// "{" opens the real body, ";" closes a forward declaration (interface
// method signature) with no body at all.
func (m *phpMachine) stateFunctionReturnTypeOrBody(tok string) {
	switch tok {
	case ":":
		m.state = m.stateFunctionBodyOrReturnType
	case "{":
		m.braceLevel++
		m.state = m.stateFunctionBody
	case ";":
		if m.startedFunction {
			m.ctx.EndOfFunction()
			m.startedFunction = false
		}
		m.state = m.stateGlobal
	}
}

// stateFunctionBodyOrReturnType ports _function_body_or_return_type
// (php.py:168-172): silently skip every token of the return-type
// declaration until its body's opening "{".
func (m *phpMachine) stateFunctionBodyOrReturnType(tok string) {
	if tok == "{" {
		m.braceLevel++
		m.state = m.stateFunctionBody
	}
}

// stateFunctionBody ports _function_body (php.py:174-182). The exit test
// compares braceLevel to inClass AS AN INTEGER (0 or 1), not to zero,
// because braceLevel is ONE COUNTER SHARED with the ENCLOSING class body's
// own brace-tracking (stateGlobal's generic "{"/"}" case bumps the same
// field while scanning a class's own top level) -- a method's body starts
// counting from braceLevel==1 (the class's own brace already counted) when
// inside a class, so its OWN closing brace returns braceLevel to 1, not 0.
// PRESERVED, NOT FIXED: this compares against inClass only, never inTrait
// -- a trait's own method never gets the "+1 enclosing scope" adjustment,
// exactly as php.py:180 spells it (`self.brace_level == self.in_class`,
// no `or self.in_trait` anywhere).
func (m *phpMachine) stateFunctionBody(tok string) {
	switch tok {
	case "{":
		m.braceLevel++
	case "}":
		m.braceLevel--
		target := 0
		if m.inClass {
			target = 1
		}
		if m.braceLevel == target {
			if m.startedFunction {
				m.ctx.EndOfFunction()
				m.startedFunction = false
			}
			m.state = m.stateGlobal
		}
	}
}

// stateConditionExpected/stateConditionContinue port _condition_expected/
// _condition_continue (php.py:184-195): skip an `if`/`switch`/`for`/
// `foreach`/`while`/`catch` condition's ENTIRE parenthesized expression as
// opaque, bracket-matched content, so a closure literal appearing inside
// it (e.g. `if (someCall(function() { ... }))`) is never mistaken for a
// real function declaration by stateGlobal.
//
// PRESERVED, NOT FIXED: "elseif" is absent from stateGlobal's routing set
// above (php.py:47's tuple has no "elseif" either), so an elseif's own
// condition expression is NOT given this same opaque-skip protection --
// a closure inside an `elseif (...)` condition WOULD be misdetected as a
// real function declaration by stateGlobal, an asymmetry real lizard 1.23.0
// itself has, not a transcription gap in this port.
func (m *phpMachine) stateConditionExpected(tok string) {
	if tok == "(" {
		m.bracketLevel = 1
		m.state = m.stateConditionContinue
	}
}

func (m *phpMachine) stateConditionContinue(tok string) {
	switch tok {
	case "(":
		m.bracketLevel++
	case ")":
		m.bracketLevel--
		if m.bracketLevel == 0 {
			m.state = m.stateGlobal
		}
	}
}

// stateMatchExpression/stateMatchExpressionContinue port _match_expression/
// _match_expression_continue (php.py:197-204): the SAME opaque-skip shape
// as stateConditionExpected/Continue, for a `match (...)` expression's own
// discriminant. Once this returns to stateGlobal, the match's arms
// (its `{ ... }` body) are read by stateGlobal itself -- every "=>" seen
// while inMatch is true counts as one arm (stateGlobal's own case), a
// heuristic that also counts a "=>" inside a NESTED array literal within
// an arm's value expression, exactly as real lizard measures (not fixed
// here -- see php.py's own comment at line 63).
func (m *phpMachine) stateMatchExpression(tok string) {
	if tok == "(" {
		m.bracketLevel = 1
		m.state = m.stateMatchExpressionContinue
	}
}

func (m *phpMachine) stateMatchExpressionContinue(tok string) {
	switch tok {
	case "(":
		m.bracketLevel++
	case ")":
		m.bracketLevel--
		if m.bracketLevel == 0 {
			m.state = m.stateGlobal
		}
	}
}

// AnalyzePHP is the AnalyzerFunc for PHP (.php). It reuses
// golikedriver.go's runGoLikeFamily unmodified: PHPReader inherits
// CCppCommentsMixin (clike.py), the SAME "/*"/"//" comment shape isComment
// already recognises, and PHPLanguageStates reads ctx.Newline nowhere at
// all (confirmed by inspection of every state above), so the driver's
// newline-tracking runs harmlessly unused, exactly as it already does for
// Kotlin/Swift (neither reads it either).
func AnalyzePHP(path, source string) ([]int, bool, error) {
	ctx := NewContext()
	ctx.SetPath(path)
	tokens := filterWhitespaceKeepNewline(GenerateTokensPHP(source))
	root := newPHPMachine(ctx)
	return runGoLikeFamily(tokens, phpConditions, root, ctx)
}
