package lizardcc

import (
	"unicode"
	"unicode/utf8"
)

// This file ports typescript.py's TypeScriptStates and
// TypeScriptTypeAnnotationStates (typescript.py:139-547) -- the shared
// state machine behind JavaScriptReader, TypeScriptReader and VueReader
// (compute.go's "javascript"/"typescript"/"vue" languageByExtension keys;
// none of the three subclasses override TypeScriptStates itself). Field
// and method names mirror Python's `_`-prefixed attributes/methods
// one-for-one; anything Python computes ONLY for a human-readable function
// name/long-name (never read as a CONTROL-FLOW condition anywhere else) is
// still carried here as an internal field, not dropped, because several of
// TypeScript's branches genuinely test name equality/truthiness to decide
// arrow-function-vs-call-vs-definition -- unlike CLikeStates
// (counter.go's package doc), naming state here IS load-bearing for which
// token stream a condition lands against, so it is ported in full even
// though FileComplexity never exposes it.
//
// Driven by golikedriver.go's runGoLikeFamily (comment/newline/condition
// counting + whitespace filtering are IDENTICAL to the GoLikeStates family;
// only the tokenizer, condition set and root subMachine differ).
type tsMachine struct {
	core
	ctx *Context

	lastTokens         string
	functionName       string
	startedFunction    bool
	asObject           bool
	getterSetterPrefix string // "" means unset (Python: None)

	tsDeclare         bool
	staticSeen        bool
	asyncSeen         bool
	prevToken         string
	inPropValue       bool
	inAbstractContext bool

	genericDepthInDec int // _generic_depth_in_dec, only meaningful inside stateDec
}

func newTSMachine(ctx *Context) *tsMachine {
	m := &tsMachine{ctx: ctx}
	m.state = m.stateGlobal
	// Wires core.beforeReturn (submachine.go) so THIS instance's own
	// statemachine_return() call (typescript.py: every normal exit AND the
	// core.go equivalent) also runs statemachine_before_return -- ports
	// Python's statemachine_return unconditionally calling
	// self.statemachine_before_return() (code_reader.py:39-41), not just
	// the top-level EOF sweep. See tsRoot below for that separate sweep.
	m.beforeReturn = m.selfBeforeReturn
	return m
}

// clone ports every `self.__class__(self.context)` call site: a fresh
// TypeScriptStates instance sharing the same Context.
func (m *tsMachine) clone() *tsMachine { return newTSMachine(m.ctx) }

// selfBeforeReturn ports statemachine_before_return (typescript.py:143-146):
// a machine that finishes (whether via a matching '}'/')' or via the
// top-level EOF sweep, see tsRoot) with a function still open closes it.
func (m *tsMachine) selfBeforeReturn() {
	if m.startedFunction {
		m.popFunctionFromStack()
	}
}

// tsRoot adapts *tsMachine to expose a public beforeReturn() method for
// golikedriver.go's runGoLikeFamily EOF sweep (`root.(interface{
// beforeReturn() })`) WITHOUT colliding with core's embedded beforeReturn
// FIELD -- tsMachine itself cannot also declare a method of that exact
// name (same shape scala.go's scalaMachine/goLike split avoids by never
// embedding core anonymously in the first place; tsRoot is the equivalent
// one-hop indirection here, since tsMachine's own extensive use of the
// promoted core methods made a full non-anonymous-embed rewrite the wrong
// tradeoff). Only the OUTERMOST machine for a file is wrapped this way;
// every nested clone() a `{`/`(` creates stays a bare *tsMachine, relying
// solely on core.beforeReturn (wired above) for its own exit.
type tsRoot struct{ *tsMachine }

func (r tsRoot) beforeReturn() { r.tsMachine.selfBeforeReturn() }

// stateGlobal ports _state_global (typescript.py:148-354). This is the
// hub every other state eventually returns to.
func (m *tsMachine) stateGlobal(tok string) {
	if tok == "declare" {
		m.tsDeclare = true
		return
	}
	if tok == "function" && m.tsDeclare {
		m.tsDeclare = false
		m.state = func(t string) {
			if t == ";" || m.ctx.Newline {
				m.state = m.stateGlobal
			}
		}
		return
	}
	m.tsDeclare = false

	if tok == "type" && !m.asObject {
		m.consumeTypeAlias()
		return
	}
	if tok == "interface" {
		m.consumeInterface()
		return
	}
	if tok == "abstract" && m.asObject {
		m.inAbstractContext = true
		return
	}
	if tok == "static" {
		m.staticSeen = true
		m.prevToken = tok
		return
	}
	if tok == "async" {
		m.asyncSeen = true
		m.prevToken = tok
		return
	}
	if tok == "new" {
		m.prevToken = tok
		return
	}

	if m.asObject {
		if tok == "get" || tok == "set" {
			m.getterSetterPrefix = tok
			return
		}
		if m.getterSetterPrefix != "" {
			m.lastTokens = m.getterSetterPrefix + " " + tok
			m.getterSetterPrefix = ""
			return
		}
		if tok == "[" {
			m.collectComputedName()
			return
		}
		switch {
		case tok == ":":
			if name := m.lastTokens; isIdentStart(name) {
				m.functionName = name
			}
			m.inPropValue = true
			return
		case tok == "<" || (len(tok) > 1 && tok[0] == '<' && tok[len(tok)-1] == '>'):
			if tok == "<" {
				m.consumeGenericTypeParams()
			}
			return
		case tok == "(":
			if m.prevToken == "." || m.prevToken == "new" {
				m.subState(m.clone(), nil)
				m.prevToken = tok
				return
			}
			if m.inPropValue && (m.functionName == "" || m.lastTokens != m.functionName) {
				m.subState(m.clone(), nil)
				m.prevToken = tok
				return
			}
			if !m.startedFunction {
				if m.lastTokens == "=" && m.functionName != "" {
					m.function(m.functionName)
				} else {
					m.function(m.lastTokens)
				}
			}
			m.state = m.stateFunction
			m.stateFunction(tok)
			return
		case (m.asyncSeen || m.staticSeen) && tok != "*" && tok != "function" && tok != "=>":
			if tok == "=" {
				m.staticSeen = false
				m.asyncSeen = false
				// fall through to the general '=' handling below.
			} else {
				m.lastTokens = tok
				return
			}
		}
	}

	switch {
	case tok == ".":
		m.state = m.stateField
		m.lastTokens += tok
		m.prevToken = tok
		return
	case tok == "function":
		if m.startedFunction && !m.asObject {
			m.popFunctionFromStack()
		}
		m.state = m.stateFunction
	case tok == "if" || tok == "switch" || tok == "for" || tok == "while" || tok == "catch":
		m.state = m.stateExpectingConditionAndStatementBlock
	case tok == "else" || tok == "do" || tok == "try" || tok == "final":
		m.state = m.stateExpectingStatementOrBlock
	case tok == "=>":
		m.state = m.stateArrowFunction
	case tok == "=":
		if name := m.lastTokens; isIdentStart(name) {
			m.functionName = name
		}
	case tok == "(":
		if m.prevToken == "." || m.prevToken == "new" {
			m.subState(m.clone(), nil)
		} else if m.functionName != "" {
			if m.lastTokens != m.functionName && m.prevToken != "=" && m.prevToken != "async" && m.prevToken != ">" {
				m.functionName = ""
				m.subState(m.clone(), nil)
			} else {
				if !m.startedFunction {
					m.function(m.functionName)
				}
				m.state = m.stateFunction
				m.stateFunction(tok)
			}
		} else {
			m.subState(m.clone(), nil)
		}
	case tok == "{":
		if m.startedFunction {
			m.subState(m.clone(), m.popFunctionFromStack)
		} else {
			m.readObject()
		}
	case tok == "}" || tok == ")":
		m.statemachineReturn()
	case m.ctx.Newline || tok == ";":
		m.functionName = ""
		m.popFunctionFromStack()
		m.staticSeen = false
		m.asyncSeen = false
		m.inAbstractContext = false
		m.inPropValue = false
		m.prevToken = ""
	}

	if tok == "`" {
		m.state = m.stateTemplateLiteral
	}
	if !m.asObject && tok == ":" {
		m.consumeTypeAnnotation()
		m.prevToken = tok
		return
	}
	if m.asObject && tok == "," {
		m.inPropValue = false
	}
	m.lastTokens = tok
	if m.prevToken != "new" && m.prevToken != "." {
		m.prevToken = tok
	}
}

// isIdentStart mirrors Python's `name[0].isalpha() or name[0] in ('_','$','#')`,
// decoding the first RUNE of name (not its first byte, which would
// misclassify a non-ASCII identifier's leading multi-byte UTF-8 character).
func isIdentStart(name string) bool {
	if name == "" {
		return false
	}
	r, _ := utf8.DecodeRuneInString(name)
	return unicode.IsLetter(r) || r == '_' || r == '$' || r == '#'
}

// consumeTypeAlias ports the `type Name = ...`/`type Name<T> = ...` skip
// (typescript.py:171-217): the whole alias, whether an object type literal
// or a simple type expression, is consumed and discarded before any
// runtime function inside it (an arrow-typed field, say) could be
// misread as a real one.
func (m *tsMachine) consumeTypeAlias() {
	phase := 0
	braceCount := 0
	genericDepth := 0
	m.state = func(t string) {
		switch phase {
		case 0:
			if isIdentStart(t) {
				phase = 1
			} else {
				m.lastTokens = "type"
				m.state = m.stateGlobal
				m.stateGlobal(t)
			}
		case 1:
			switch t {
			case "<":
				genericDepth = 1
				phase = 3
			case "=":
				phase = 2
			case ";":
				m.state = m.stateGlobal
			}
		case 2:
			switch {
			case t == "{":
				braceCount = 1
				phase = 4
			case t == ";" || m.ctx.Newline:
				m.state = m.stateGlobal
				if t != ";" {
					m.stateGlobal(t)
				}
			}
		case 3:
			switch t {
			case "<":
				genericDepth++
			case ">":
				genericDepth--
				if genericDepth == 0 {
					phase = 1
				}
			}
		case 4:
			switch t {
			case "{":
				braceCount++
			case "}":
				braceCount--
				if braceCount == 0 {
					m.state = m.stateGlobal
				}
			}
		}
	}
}

// consumeInterface ports the `interface Name { ... }` skip
// (typescript.py:219-233): method SIGNATURES inside an interface are not
// runtime functions.
func (m *tsMachine) consumeInterface() {
	braceCount := 0
	started := false
	m.state = func(t string) {
		switch {
		case t == "{":
			started = true
			braceCount++
		case t == "}" && started:
			braceCount--
			if braceCount == 0 {
				m.state = m.stateGlobal
			}
		}
	}
}

// readObject ports read_object (typescript.py:356-365): the object/class
// body is a fresh clone with asObject=true, carrying static/async modifiers
// along (an object literal's own `static`/`async` field markers are set on
// THIS machine just before the `{`, per stateGlobal's modifier-tracking
// branches, then handed to the nested reader).
func (m *tsMachine) readObject() {
	obj := m.clone()
	obj.asObject = true
	obj.staticSeen = m.staticSeen
	obj.asyncSeen = m.asyncSeen
	m.subState(obj, func() { m.state = m.stateGlobal })
	m.staticSeen = false
	m.asyncSeen = false
}

// stateExpectingConditionAndStatementBlock ports
// _expecting_condition_and_statement_block (typescript.py:367-376).
func (m *tsMachine) stateExpectingConditionAndStatementBlock(tok string) {
	if tok == "await" {
		return
	}
	if tok != "(" {
		m.state = m.stateGlobal
		m.stateGlobal(tok)
		return
	}
	m.subState(m.clone(), func() { m.state = m.stateExpectingStatementOrBlock })
}

// stateExpectingStatementOrBlock ports _expecting_statement_or_block
// (typescript.py:378-384).
func (m *tsMachine) stateExpectingStatementOrBlock(tok string) {
	if tok == "{" {
		m.subState(m.clone(), func() { m.state = m.stateGlobal })
		return
	}
	m.state = m.stateGlobal
	m.stateGlobal(tok)
}

func (m *tsMachine) pushFunctionToStack() {
	if m.inAbstractContext {
		return
	}
	m.startedFunction = true
	m.ctx.PushNewFunction()
}

func (m *tsMachine) popFunctionFromStack() {
	if m.startedFunction {
		m.ctx.EndOfFunction()
	}
	m.startedFunction = false
	m.inPropValue = false
}

// function ports _function's NON-"(" branch (typescript.py:395-406) called
// DIRECTLY with a name string -- exactly how Python's own "(" handlers
// call it: `self._function(self.last_tokens)`/`self._function(self.function_name)`
// BEFORE ALSO calling `self.next(self._function, token)` with the REAL "("
// token immediately after (see the two call sites below, both mirroring
// this same two-call shape). This does NOT push the function -- pushing
// happens only on the SECOND call, via stateFunction's own "(" branch,
// exactly as Python's `_function` only reaches `_push_function_to_stack`
// in its "else" (token == '(') arm.
//
// BUG FIXED HERE: an earlier revision made this call pushFunctionToStack()
// directly and skipped the static/async-modifier reset _function's
// non-"(" branch performs -- `static create(...) { ... }` never cleared
// staticSeen afterward, so the class's NEXT member's opening '{' (e.g.
// `async fetchAndAdd(...) { ... }`) matched the (asyncSeen||staticSeen)
// case in stateGlobal's object branch INSTEAD of the "{" case, silently
// routing the entire method body through the class-body machine itself
// rather than a fresh function-body clone -- confirmed live:
// classes_methods.ts.txt lost fetchAndAdd/loadValue/identity entirely
// (functions_count 6 vs lizard's 9) until this fix.
func (m *tsMachine) function(name string) {
	if isIdentStart(name) {
		m.functionName = name
	} else {
		m.functionName = ""
	}
	m.staticSeen = false
	m.asyncSeen = false
}

// stateArrowFunction ports _arrow_function (typescript.py:386-393).
func (m *tsMachine) stateArrowFunction(tok string) {
	if !m.startedFunction {
		m.pushFunctionToStack()
	}
	m.functionName = ""
	m.asyncSeen = false
	m.staticSeen = false
	m.state = m.stateGlobal
	m.stateGlobal(tok)
}

// stateFunction ports _function (typescript.py:395-411).
func (m *tsMachine) stateFunction(tok string) {
	if tok == "*" {
		return
	}
	if tok == "<" {
		m.consumeGenericTypeParams()
		return
	}
	if len(tok) > 1 && tok[0] == '<' && tok[len(tok)-1] == '>' {
		return
	}
	if tok != "(" {
		if isIdentStart(tok) {
			m.functionName = tok
		} else {
			m.functionName = ""
		}
		m.staticSeen = false
		m.asyncSeen = false
		return
	}
	if !m.startedFunction {
		m.pushFunctionToStack()
	}
	m.genericDepthInDec = 0
	m.state = m.stateDec
	m.stateDec(tok)
}

// stateField ports _field (typescript.py:413-415): a bare no-op consumer
// for the token right after a lone '.', folded back into last_tokens.
func (m *tsMachine) stateField(tok string) {
	m.lastTokens += tok
	m.state = m.stateGlobal
}

// stateDec ports _dec (typescript.py:417-437): the parameter list. Every
// branch here only affects Python's parameter COUNT/name list (never read
// elsewhere in this file), so this port only tracks the generic-angle-
// bracket depth needed to find the matching ')' correctly -- a `<`/`>`
// pair around a generic parameter type (`Map<K, V>`) must not be mistaken
// for the closing paren's sibling commas.
func (m *tsMachine) stateDec(tok string) {
	switch {
	case tok == ")":
		m.state = m.stateExpectFuncOpeningBracket
	case tok == "(":
		// no-op: Python's add_to_long_function_name branch, name-only.
	case tok == "<":
		m.genericDepthInDec++
	case tok == ">":
		if m.genericDepthInDec > 0 {
			m.genericDepthInDec--
		}
	}
}

// stateExpectFuncOpeningBracket ports _expecting_func_opening_bracket
// (typescript.py:439-452).
//
// Python's real source is `if token==':': self._consume_type_annotation()
// ... ; self.next(self._state_global, token)` -- the trailing
// self.next(...) call is UNCONDITIONAL, so for a ':' it immediately
// redispatches the SAME token into _state_global, which -- because
// Python's self._state is one field that can transparently hold either a
// plain state function OR a whole sub-machine object -- simply clobbers
// the sub_state assignment _consume_type_annotation() just made and
// replaces it with whatever _state_global's OWN ':' handling decides
// (which, for an as_object method's return type, is a COMPLETELY
// DIFFERENT branch that never touches the type-annotation sub-machine at
// all -- see stateGlobal's object-branch ':' case). This port's core
// (submachine.go) tracks "in a sub-state" via a SEPARATE `sub` field
// rather than overloading `state`, so blindly replicating "call
// subState() then immediately overwrite state again in the same
// synchronous path" would leave `sub` dangling and hijack every
// subsequent token -- confirmed live: classes_methods.ts.txt's Shape/
// Circle classes lost describe/constructor/area entirely to exactly this
// corruption before this branch was rewritten to compute Python's NET
// effect directly instead of replaying the double-dispatch literally.
func (m *tsMachine) stateExpectFuncOpeningBracket(tok string) {
	switch {
	case tok == ":":
		if m.asObject {
			m.state = m.stateGlobal
			m.stateGlobal(tok)
		} else {
			m.consumeTypeAnnotation()
			m.prevToken = tok
		}
		return
	case tok == ";" && m.asObject && m.inAbstractContext:
		if m.startedFunction {
			m.popFunctionFromStack()
		}
		m.inAbstractContext = false
	case tok != "{" && tok != "=>":
		if m.startedFunction {
			m.ctx.Forgive = true
			m.ctx.EndOfFunction()
		}
		m.startedFunction = false
	}
	m.state = m.stateGlobal
	m.stateGlobal(tok)
}

// stateTemplateLiteral ports _state_template_literal (typescript.py:454-456):
// consume tokens until the matching closing backtick. Since
// splitTemplateLiterals (typescript_tokenize.go) already breaks a
// template-literal token into quote/content/`${`/expr/`}`/quote pieces
// before this state machine ever sees them, the only thing arriving here
// between the two backticks is the pre-split middle tokens; none of them
// can themselves be a bare "`" (that would only occur for a literal split
// abandoned as unterminated, which yields the WHOLE original token instead
// -- see splitOneTemplateLiteral's doc), so this simply waits for the
// single closing backtick token.
func (m *tsMachine) stateTemplateLiteral(tok string) {
	if tok == "`" {
		m.state = m.stateGlobal
	}
}

// collectComputedName ports _collect_computed_name (typescript.py:458-472):
// consume tokens between `[` and `]` for a computed property name. Python
// re-derives a camelCase name from them for the function name Object
// output; since this port never exposes a name, only the CONSUMPTION
// matters (so the matching `]` is found and stateGlobal resumes
// correctly) -- the name is set to a fixed non-empty placeholder so the
// SAME truthiness/equality branches that read functionName/lastTokens
// downstream behave as they would for any other named property.
func (m *tsMachine) collectComputedName() {
	m.state = func(t string) {
		if t == "]" {
			m.lastTokens = "[computed]"
			m.state = m.stateGlobal
		}
	}
}

// consumeGenericTypeParams ports _consume_generic_type_params
// (typescript.py:495-505): skip a `<...>` generic parameter list on a
// function/method name so functionName (already set) survives it.
func (m *tsMachine) consumeGenericTypeParams() {
	depth := 1
	m.state = func(t string) {
		switch t {
		case "<":
			depth++
		case ">":
			depth--
			if depth == 0 {
				m.state = m.stateGlobal
			}
		}
	}
}

// consumeTypeAnnotation ports _consume_type_annotation (typescript.py:507-511):
// hand off to TypeScriptTypeAnnotationStates as a sub-state; if it saved a
// token on its way out (typescript.py's `typeStates.saved_token`), re-feed
// it to THIS machine's current state once the sub-state returns.
func (m *tsMachine) consumeTypeAnnotation() {
	ts := newTSTypeAnnotationStates(m.ctx)
	m.subState(ts, func() {
		if ts.savedToken != "" {
			saved := ts.savedToken
			ts.savedToken = ""
			m.feed(saved)
		}
	})
}

// tsTypeAnnotationStates ports TypeScriptTypeAnnotationStates
// (typescript.py:513-547): skips a type annotation after `:` -- a simple
// type name, a `<...>` generic type, an inline `{...}` object type, or a
// `(...)` function-type signature -- stopping at the first token that
// cannot be part of one, which is handed back to the caller via
// savedToken.
type tsTypeAnnotationStates struct {
	core
	ctx        *Context
	savedToken string
}

func newTSTypeAnnotationStates(ctx *Context) *tsTypeAnnotationStates {
	t := &tsTypeAnnotationStates{ctx: ctx}
	t.state = t.stateGlobal
	return t
}

func (t *tsTypeAnnotationStates) stateGlobal(tok string) {
	if tok == "{" {
		t.state = t.stateInlineTypeAnnotation
		t.stateInlineTypeAnnotation(tok)
		return
	}
	t.state = t.stateSimpleType
	t.stateSimpleType(tok)
}

func (t *tsTypeAnnotationStates) stateSimpleType(tok string) {
	switch {
	case tok == "<":
		t.state = t.stateGenericType
		t.stateGenericType(tok)
	case tok == "{" || tok == "=" || tok == ";" || tok == ")":
		t.savedToken = tok
		t.statemachineReturn()
	case tok == "(":
		t.state = t.stateFunctionTypeAnnotation
		t.stateFunctionTypeAnnotation(tok)
	case tok == "=>":
		t.savedToken = tok
		t.statemachineReturn()
	}
}

// bracketDepthThen ports CodeStateMachine.read_inside_brackets_then's
// decorator behaviour (code_reader.py:66-86): consume tokens, tracking
// nesting of the given open/close pair (both starting depth 1, since the
// FIRST opening bracket is the token that dispatched into this state), and
// call `then` with the token that brings depth back to zero.
func bracketDepthThen(depth *int, open, close string, tok string, then func(string)) {
	*depth += bracketDelta(tok, open, close)
	if *depth == 0 {
		then(tok)
	}
}

func (t *tsTypeAnnotationStates) stateInlineTypeAnnotation(tok string) {
	depth := 0
	t.state = func(tk string) {
		bracketDepthThen(&depth, "{", "}", tk, func(string) {
			t.statemachineReturn()
		})
	}
	t.state(tok)
}

func (t *tsTypeAnnotationStates) stateGenericType(tok string) {
	depth := 0
	t.state = func(tk string) {
		bracketDepthThen(&depth, "<", ">", tk, func(string) {
			t.statemachineReturn()
		})
	}
	t.state(tok)
}

func (t *tsTypeAnnotationStates) stateFunctionTypeAnnotation(tok string) {
	depth := 0
	t.state = func(tk string) {
		bracketDepthThen(&depth, "(", ")", tk, func(string) {
			t.statemachineReturn()
		})
	}
	t.state(tok)
}

// tsConditions ports TypeScriptReader's separated condition categories
// (typescript.py:55-58): control-flow keywords (control_flow_keywords
// ADDS "elseif" to the base CodeReader set -- not a real JS/TS token
// under normal tokenization, kept anyway since this port's job is
// bit-for-bit agreement with lizard, not correcting what looks like an
// upstream typo), the two logical operators, `case`, and the ternary `?`.
var tsConditions = map[string]bool{
	"if": true, "elseif": true, "for": true, "while": true, "catch": true,
	"&&": true, "||": true,
	"case": true,
	"?":    true,
}

// AnalyzeTypeScript is the AnalyzerFunc for TypeScript (.ts). JavaScriptReader
// (javascript.py) inherits TypeScriptReader unchanged -- no override of
// generate_tokens, conditions, or states -- so AnalyzeJavaScript below is
// this exact function under a second name, matching Python's inheritance
// with no Go-side divergence to introduce.
func AnalyzeTypeScript(path, source string) ([]int, bool, error) {
	tokens := filterWhitespaceKeepNewline(GenerateTokensTS(source))
	ctx := NewContext()
	ctx.SetPath(path)
	root := tsRoot{newTSMachine(ctx)}
	return runGoLikeFamily(tokens, tsConditions, root, ctx)
}

// AnalyzeJavaScript is the AnalyzerFunc for JavaScript (.js/.jsx/.mjs/.cjs).
// See AnalyzeTypeScript's doc: JavaScriptReader is TypeScriptReader with no
// overrides at all (javascript.py:8-15 only narrows `ext`/`language_names`).
func AnalyzeJavaScript(path, source string) ([]int, bool, error) {
	return AnalyzeTypeScript(path, source)
}
