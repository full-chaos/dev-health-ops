package lizardcc

// This file ports lizard_languages/java.py in full: JavaReader, JavaStates,
// JavaFunctionBodyStates and JavaClassBodyStates.
//
// Unlike C# (csharp.go), which reuses clike.go's clikeStates through two
// small hooks because it overrides very little, Java overrides enough of
// CLikeStates (try_new_function, _state_imp, _state_old_c_params,
// _state_global) and adds enough new behaviour (records, annotations,
// nested/anonymous classes via sub_state) that this is its own
// self-contained port -- the same judgment call that separated
// Kotlin/Scala/Swift (golike.go-based, own files) from Go/Rust (golike.go
// used unmodified). Package-level helpers (bracketDelta, oneOf,
// parameterOpen/parameterClose, firstByte, isAlpha) are reused from
// clike.go; the declaration-parsing states below (stateFunction,
// stateDec/stateDecBody, stateDecToImp, stateThrows,
// stateTemplateInName) are copied from it because Java needs them
// UNCHANGED, minus the pieces Java can never reach: C++ operator overloads
// ("::"/"operator" in clike.py's _state_function/_state_name_with_space,
// never used by Java's own declaration syntax), throw-specifiers,
// trailing-return-types, initializer lists and C++11 attributes (Java has
// none of these), and lambda-check ('[' in _state_global) -- clike.py's
// own base _state_global disables that path FOR Java specifically via
// `if not hasattr(self, 'class_name')`, and JavaStates always has one.
//
// ONE dynamic-dispatch hook survives here for the same reason clike.go
// needed globalState/decToImpState: JavaStates, JavaFunctionBodyStates and
// JavaClassBodyStates share almost everything (try_new_function,
// _try_start_a_class, the whole declaration-parsing chain) through Python
// inheritance, but each has its OWN `_state_global` -- and the SHARED code
// (stateFunction's fallback, stateDec's abort branch, stateDecToImp's
// not-an-identifier fallback) references it as a bare `self._state_global`,
// which resolves to whichever one is active. globalState is that same
// hook, set once per spawned instance to the right one of the three
// javaXStateGlobal methods below. Where Python instead says
// `JavaStates._state_global(self, ...)` (an EXPLICIT, statically-known
// target, not dynamic dispatch -- JavaClassBodyStates.java.py:235-236,250),
// this port calls javaStateGlobal directly, matching that explicitness.

// javaMachine holds every JavaStates/JavaFunctionBodyStates/
// JavaClassBodyStates field. All three conceptual "subclasses" are this
// SAME Go type; only globalState (and, for a class-body instance, the
// class-body-specific fields below) differ per spawn.
type javaMachine struct {
	core
	ctx *Context

	// Shared JavaStates fields (java.py:23-30). className is the ONE name
	// this package tracks anywhere (every other identifier is dropped, per
	// counter.go's package doc) -- it must be a real string, not just a
	// "do we have one" bool, because tryNewFunction's record-compact-
	// constructor detection genuinely needs the VALUE: lizard 1.23.0
	// compares a candidate name against it verbatim (java.py:65), and gets
	// this famously wrong for a record with a static factory method
	// returning its own type (`static Point origin()` inside `record
	// Point`) -- "Point" the return-type token is tried as a name via the
	// after-static-keyword double-dispatch (java.py:235-236) and, because
	// it equals the record's own name, is misread as an attempted compact
	// constructor. in_record_constructor then never resets (only the
	// compact constructor's own '}' does), silently swallowing every
	// later declaration in that class body. Measured directly against
	// real lizard 1.23.0 (not assumed): this is confirmed, reproducible
	// lizard behavior for that exact shape, not a hypothetical -- and a
	// static factory returning the record's own type is common, idiomatic
	// Java, so replicating it (not "fixing" it) is this package's actual
	// parity contract. This same fix also makes a record's real, EXPLICIT
	// compact constructor (`record Point(int x, int y) { public Point {
	// if (x < 0) throw ...; } }`) score correctly: stateRecordCompactConstructor/
	// stateRecordConstructorBody below never call ConfirmNewFunction at
	// all, matching Python's own zero-complexity no-op for it exactly.
	className           string
	isRecord            bool
	inRecordConstructor bool
	inMethodBody        bool
	handlingDotClass    bool
	handlingMethodRef   bool

	// clike.go-derived declaration-parsing fields (clike.go's clikeStates
	// carries the same two; see its doc for why sharing one brCount
	// across every decorated state on one instance is safe).
	brCount      int
	bracketStack []string

	// globalState is the per-spawn hook described above.
	globalState state

	// JavaFunctionBodyStates-only (java.py:163).
	ignoreTokens bool

	// JavaClassBodyStates-only (java.py:210-214).
	afterStaticKeyword bool
	bodyBraceDepth     int
	firstCall          bool // true only before this instance's first feed
	lastToken          string
}

// newJavaMachine spawns a top-level JavaStates instance (java.py:22-30).
func newJavaMachine(ctx *Context) *javaMachine {
	m := &javaMachine{ctx: ctx, firstCall: true}
	m.globalState = m.javaStateGlobal
	m.state = m.globalState
	return m
}

// newJavaFunctionBodyMachine spawns a JavaFunctionBodyStates instance
// (java.py:159-163): a JavaStates whose in_method_body starts true and
// whose _state_global is the combined-bracket-tracking one.
func newJavaFunctionBodyMachine(ctx *Context) *javaMachine {
	m := &javaMachine{ctx: ctx, inMethodBody: true, firstCall: true}
	m.globalState = m.javaFunctionBodyStateGlobal
	m.state = m.globalState
	return m
}

// newJavaClassBodyMachine spawns a JavaClassBodyStates instance
// (java.py:205-214), carrying the enclosing class's name/record-ness
// forward -- className is genuinely read (see this struct's own doc), not
// just tracked for parity with Python's report. An anonymous class passes
// the literal string "(anonymous)" (java.py:200), which can never equal a
// real record's name, so the compact-constructor comparison is safely
// never true for it.
func newJavaClassBodyMachine(ctx *Context, className string, isRecord bool) *javaMachine {
	m := &javaMachine{ctx: ctx, className: className, isRecord: isRecord, firstCall: true}
	m.globalState = m.javaClassBodyStateGlobal
	m.state = m.globalState
	return m
}

func (m *javaMachine) feed(tok string) bool {
	exited := m.core.feed(tok)
	m.lastToken = tok
	m.firstCall = false
	return exited
}

// AnalyzeJava is the AnalyzerFunc for Java (.java). It reuses
// CLikeNestingStackStates unchanged (java.py:17-19: parallel_states =
// [JavaStates, CLikeNestingStackStates]) -- Java has no equivalent of
// CppRValueRefStates. Complexity bookkeeping (TryNewFunction/
// ConfirmNewFunction/AddCondition/EndOfFunction via PopNesting) flows
// through the SAME Context and nestingStates this whole family shares;
// javaMachine's own sub_state recursion exists only to recognise
// declarations correctly (a nested/anonymous class's methods still end up
// calling the very same ctx methods a top-level one would), not to
// duplicate that bookkeeping.
// BUG FIXED HERE (CHAOS-5156, found by codex round r1 on #2269, same class
// as csharp.go's fix): preprocess used to run as a SEPARATE batch pass over
// the whole token slice before any state machine saw a single token, so a
// `#if`/`#ifdef`/`#elif` condition bump landed on `ctx.current == &ctx.global`
// unconditionally instead of whichever function was actually active at that
// point in the stream. Real Java source never legally contains a
// preprocessor directive, so this was likely never reachable through a
// compilable .java file -- but tokenize.go's tokenizer (reused wholesale
// here) still recognises a leading '#' as a macro token regardless of
// language, and lizard's own JavaReader inherits CLikeReader.preprocess
// unmodified (java.py has no override), so parity requires matching this
// behavior exactly rather than assuming it is dead code. Fixed by
// interleaving preprocess with condition-counting and dispatch one token at
// a time, exactly matching clike.go's Analyze and csharp.go's AnalyzeCSharp.
func AnalyzeJava(path, source string) ([]int, bool, error) {
	ctx := NewContext()
	ctx.SetPath(path)
	// GenerateTokens already runs accumulateMacros (tokenize.go); Java has
	// no preprocessor, so it never produces a '#' token for that to glue,
	// but reusing the C-family tokenizer wholesale is simpler than a
	// second copy that differs only in never being exercised.
	raw := GenerateTokens(source)

	root := newJavaMachine(ctx)
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
			// Same `#lizard forgive`/"GENERATED CODE" handling as
			// clike.go's Analyze -- see HandleCommentDirectives' doc
			// (counter.go).
			if ctx.HandleCommentDirectives(tok) {
				break
			}
			continue
		}
		if conditions[tok] {
			ctx.AddCondition(1)
		}
		root.feed(tok)
		ns.feed(tok)
	}
	return ctx.Complexities, false, nil
}

// consumeJavaExpressionTokens ports _consume_java_expression_tokens
// (java.py:32-47).
func (m *javaMachine) consumeJavaExpressionTokens(tok string) bool {
	if tok == "::" {
		m.handlingMethodRef = true
		return true
	}
	if m.handlingMethodRef {
		m.handlingMethodRef = false
		return true
	}
	if tok == "." && !m.handlingDotClass {
		m.handlingDotClass = true
		return true
	}
	if m.handlingDotClass {
		m.handlingDotClass = false
		if tok == "class" {
			return true
		}
	}
	return false
}

// tryNewFunction ports try_new_function (java.py:63-72), minus the
// long-name bookkeeping this package never tracks (className is the one
// exception -- see this struct's own doc for why the comparison below
// must be a real string equality, not a "did we set one" bool).
func (m *javaMachine) tryNewFunction(tok string) {
	if m.isRecord && m.className != "" && tok == m.className {
		m.inRecordConstructor = true
		m.state = m.stateRecordCompactConstructor
		return
	}
	m.ctx.TryNewFunction()
	m.state = m.stateFunction
}

// tryStartAClass ports _try_start_a_class (java.py:74-90).
func (m *javaMachine) tryStartAClass(tok string) bool {
	switch tok {
	case "class", "enum":
		m.className = ""
		m.isRecord = false
		m.inRecordConstructor = false
		m.state = m.stateClassDeclaration
		return true
	case "record":
		if m.inMethodBody {
			return false
		}
		m.state = m.stateAfterRecordKeyword
		return true
	}
	return false
}

// stateAfterRecordKeyword ports _state_after_record_keyword (java.py:92-101).
func (m *javaMachine) stateAfterRecordKeyword(tok string) {
	b := firstByte(tok)
	if b == '_' || isAlpha(b) {
		m.className = ""
		m.isRecord = true
		m.inRecordConstructor = false
		m.state = m.stateClassDeclaration
		m.stateClassDeclaration(tok)
		return
	}
	m.tryNewFunction("record")
	m.state(tok)
}

// javaStateGlobal ports JavaStates._state_global (java.py:103-112): the
// TOP-LEVEL dispatch, used both for file scope and (per clike.go's
// CLikeStates design this reuses) between statements generally.
func (m *javaMachine) javaStateGlobal(tok string) {
	if m.consumeJavaExpressionTokens(tok) {
		return
	}
	if tok == "@" {
		m.state = m.stateDecorator
		return
	}
	if m.tryStartAClass(tok) {
		return
	}
	if !m.inRecordConstructor {
		m.clikeStateGlobal(tok)
	}
}

// clikeStateGlobal ports CLikeStates._state_global (clike.go's
// clikeStates.stateGlobal) for Java: the lambda-check branch is dropped
// (unreachable -- see this file's package doc).
func (m *javaMachine) clikeStateGlobal(tok string) {
	b := firstByte(tok)
	if b == '_' || isAlpha(b) {
		m.tryNewFunction(tok)
	}
}

// stateDecorator/statePostDecorator/stateAnnotationArguments port
// java.py:114-129: skip an `@Annotation` or `@Annotation(...)` so its
// contents are never mistaken for a method declaration.
func (m *javaMachine) stateDecorator(tok string) { m.state = m.statePostDecorator }

func (m *javaMachine) statePostDecorator(tok string) {
	switch tok {
	case ".":
		m.state = m.stateDecorator
	case "(":
		m.state = m.stateAnnotationArguments
		m.stateAnnotationArguments(tok)
	default:
		m.state = m.globalState
		m.globalState(tok)
	}
}

func (m *javaMachine) stateAnnotationArguments(tok string) {
	m.brCount += bracketDelta(tok, "(", ")")
	if m.brCount == 0 {
		m.state = m.globalState
	}
}

// stateClassDeclaration ports _state_class_declaration (java.py:131-140).
//
// BUG FIXED HERE (CHAOS-5156, self-found while diagnosing #2269's r1 void
// round -- independently confirmed against real lizard 1.23.0 before
// fixing, not merely argued): this branch used to accept firstByte(tok)
// == '_' as well as isAlpha, but java.py's own _state_class_declaration
// only checks `token[0].isalpha()` -- NO underscore acceptance here,
// unlike _state_after_record_keyword's OWN predicate two states earlier
// (java.py:92-101 / this file's stateAfterRecordKeyword), which
// deliberately DOES accept '_' and is untouched by this fix. The two
// states have genuinely different predicates in the real reader; porting
// the wrong one into stateClassDeclaration made `record _R(int x) { _R
// {...} }` silently exempt itself from scoring (a compact constructor
// match requires tok == m.className, which only fires when className was
// actually captured) -- real lizard never captures "_R" as class_name
// here, so it measures the compact-constructor body as a REAL function
// instead. Measured directly: this exact fixture went from Go=0 functions
// to Go=1 function (matching lizard's own 1, complexity 1) after this
// fix.
func (m *javaMachine) stateClassDeclaration(tok string) {
	switch {
	case tok == "{":
		className, isRecord := m.className, m.isRecord
		m.subStateTok(newJavaClassBodyMachine(m.ctx, className, isRecord), func() {
			m.state = m.globalState
		}, tok)
	case tok == "(":
		m.state = m.stateRecordParameters
	case isAlpha(firstByte(tok)):
		if m.className == "" {
			m.className = tok
		}
	}
}

func (m *javaMachine) stateRecordParameters(tok string) {
	if tok == ")" {
		m.state = m.stateClassDeclaration
	}
}

func (m *javaMachine) stateRecordCompactConstructor(tok string) {
	if tok == "{" {
		m.state = m.stateRecordConstructorBody
		return
	}
	m.state = m.globalState
	m.globalState(tok)
}

func (m *javaMachine) stateRecordConstructorBody(tok string) {
	if tok == "}" {
		m.inRecordConstructor = false
		m.state = m.globalState
	}
}

// ---------------------------------------------------------------------
// Declaration parsing, copied from clike.go's clikeStates where Java needs
// it unchanged, with globalState substituted for every self._state_global
// clike.py references from code Java shares unmodified.
// ---------------------------------------------------------------------

func (m *javaMachine) stateFunction(tok string) {
	switch tok {
	case "(":
		m.state = m.stateDec
		m.stateDec(tok)
	case "<":
		m.state = m.stateTemplateInName
		m.stateTemplateInName(tok)
	default:
		m.state = m.globalState
		m.globalState(tok)
	}
}

func (m *javaMachine) stateTemplateInName(tok string) {
	switch tok {
	case "<":
		m.brCount++
	case ">":
		m.brCount--
	}
	if m.brCount == 0 {
		m.state = m.stateFunction
	}
}

func (m *javaMachine) stateDec(tok string) {
	m.brCount += bracketDelta(tok, "(", ")")
	m.stateDecBody(tok)
	if m.brCount == 0 {
		m.state = m.stateDecToImp
	}
}

func (m *javaMachine) stateDecBody(tok string) {
	switch {
	case parameterOpen(tok):
		m.bracketStack = append(m.bracketStack, tok)
	case parameterClose(tok):
		if len(m.bracketStack) > 0 {
			m.bracketStack = m.bracketStack[:len(m.bracketStack)-1]
		} else {
			m.state = m.globalState
		}
	}
}

// stateDecToImp ports _state_dec_to_imp for Java: only the branches Java
// can reach survive (throws, "{"/entering-imp, "["/attribute, the
// not-an-identifier fallback, and the old-c-params default) --
// throw/noexcept/trailing-return specifiers and initializer lists are
// C++-only and unreachable here.
//
// BUG FIXED HERE (CHAOS-5156, self-found while diagnosing #2269's r1 void
// round -- independently confirmed against real lizard 1.23.0 before
// fixing): the original port DROPPED clike.py's `elif token == "[":
// self._state = self._state_attribute` branch entirely, on the (wrong)
// assumption that it was C++-attribute-only and therefore unreachable
// for Java. It is reachable: legacy Java array-return syntax (`int
// f()[] { ... }`, valid since Java 1.0, still compiles today) puts a
// bare "[" right here, and without this branch it fell into the
// not-an-identifier fallback, which abandons the declaration back to
// global scope -- silently dropping the function body's own condition
// count. Measured directly: `int f()[] { if(true){...} return ...; }`
// went from Go=[1] to Go=[2] (matching real lizard's [2], base 1 + if)
// after adding this branch + stateAttribute below.
func (m *javaMachine) stateDecToImp(tok string) {
	switch {
	case tok == "throws":
		m.state = m.stateThrows
	case tok == "{":
		m.stateEnteringImp(tok)
	case tok == "[":
		m.state = m.stateAttribute
		m.stateAttribute(tok)
	case !(firstByte(tok) == '_' || isAlpha(firstByte(tok))):
		m.state = m.globalState
		m.globalState(tok)
	default:
		m.state = m.stateOldCParams
	}
}

// stateAttribute ports _state_attribute (clike.py:319-322) for Java:
// skip a bracketed `[...]` span (old-style array-return brackets after a
// parameter list, e.g. `int f()[]`) and return to stateDecToImp once the
// brackets close. Mirrors clikeStates.stateAttribute (clike.go) exactly,
// but returns to THIS machine's own stateDecToImp directly -- Java has
// no separate hook-field indirection here, unlike clikeStates' decToImpState
// hook, since javaMachine is never subclassed further.
func (m *javaMachine) stateAttribute(tok string) {
	m.brCount += bracketDelta(tok, "[", "]")
	if m.brCount == 0 {
		m.state = m.stateDecToImp
	}
}

// stateThrows ports _state_throws (clike.py:255-258): a Java `throws A, B`
// clause.
func (m *javaMachine) stateThrows(tok string) {
	if !oneOf(tok, ";{") {
		return
	}
	m.state = m.stateDecToImp
	m.stateDecToImp(tok)
}

// stateOldCParams ports Java's OWN _state_old_c_params (java.py:49-51):
// simplified from clike.go's version to a bare "wait for '{'", since Java
// has no old-style K&R parameter declarations.
func (m *javaMachine) stateOldCParams(tok string) {
	if tok == "{" {
		m.stateDecToImp(tok)
	}
}

// stateEnteringImp ports Java's OWN _state_imp (java.py:53-61): unlike
// clike.go's opaque stateImp, entering a method body confirms the function
// then hands EVERY subsequent token to a fresh JavaFunctionBodyStates
// sub-machine (fed the opening '{' immediately, matching Python's 3-arg
// `sub_state(state, callback, token)` -- unlike golike.go's
// stateFunctionImpl, which deliberately does NOT feed its opening brace;
// the two families differ here because Java's sub-machine needs to see
// its own '{' to seed _handle_class_body_brace-style bookkeeping the same
// way JavaClassBodyStates does, whereas GoLikeStates' clone has no such
// bookkeeping to seed).
func (m *javaMachine) stateEnteringImp(tok string) {
	m.ctx.ConfirmNewFunction()
	m.inMethodBody = true
	m.subStateTok(newJavaFunctionBodyMachine(m.ctx), func() {
		m.inMethodBody = false
		m.state = m.globalState
	}, tok)
}

// ---------------------------------------------------------------------
// JavaFunctionBodyStates (java.py:159-203)
// ---------------------------------------------------------------------

// javaFunctionBodyStateGlobal ports JavaFunctionBodyStates._state_global's
// raw body (java.py:167-185), combining BOTH decorators' bracket tracking
// (java.py:165-166 track "{}" and "()" together into ONE shared br_count)
// into a single delta per token, applied before the body runs -- matching
// the decorator chain's actual order (outer's delta, then inner's delta,
// then the raw body). The decorators' own "_state_dummy" transition is NOT
// ported: it only ever fires in the SAME call statemachine_return() does
// (both check br_count==0), and by then this sub-machine is being
// discarded by its caller (stateEnteringImp above) regardless of what
// _state is left pointing at.
func (m *javaMachine) javaFunctionBodyStateGlobal(tok string) {
	m.brCount += bracketDelta(tok, "(", ")") + bracketDelta(tok, "{", "}")

	if m.consumeJavaExpressionTokens(tok) {
		return
	}
	if m.ignoreTokens {
		m.ignoreTokens = false
		return
	}
	if tok == "new" {
		m.state = m.stateNew
		return
	}
	if m.tryStartAClass(tok) {
		return
	}
	if m.brCount == 0 {
		m.statemachineReturn()
	}
}

func (m *javaMachine) stateNew(tok string) { m.state = m.stateNewParameters }

// stateNewParameters ports _state_new_parameters (java.py:193-202): `new
// Foo(...)` (a plain constructor call -- its arguments are read by ANOTHER
// function-body-shaped sub-machine, matching Python exactly) or `new
// Foo(...) { ... }` (an anonymous class body).
func (m *javaMachine) stateNewParameters(tok string) {
	switch tok {
	case "(":
		m.subStateTok(newJavaFunctionBodyMachine(m.ctx), nil, tok)
	case "{":
		m.subStateTok(newJavaClassBodyMachine(m.ctx, "(anonymous)", false), func() {
			m.state = m.globalState
		}, tok)
	default:
		m.state = m.globalState
		m.globalState(tok)
	}
}

// ---------------------------------------------------------------------
// JavaClassBodyStates (java.py:205-253)
// ---------------------------------------------------------------------

// handleClassBodyBrace ports _handle_class_body_brace (java.py:216-227):
// reports whether tok closes THIS class body, tracking nested (unhandled,
// e.g. field-initializer) braces separately. firstCall stands in for
// Python's `self.last_token is None` (true only before this instance's own
// first token, which is always its own opening '{', fed by whichever
// caller spawned it -- stateClassDeclaration/stateNewParameters above).
func (m *javaMachine) handleClassBodyBrace(tok string) bool {
	switch tok {
	case "{":
		if !m.firstCall {
			m.bodyBraceDepth++
		}
		return false
	case "}":
		if m.bodyBraceDepth > 0 {
			m.bodyBraceDepth--
			return false
		}
		return true
	}
	return false
}

// javaClassBodyStateGlobal ports JavaClassBodyStates._state_global
// (java.py:229-252).
func (m *javaMachine) javaClassBodyStateGlobal(tok string) {
	if m.afterStaticKeyword {
		m.afterStaticKeyword = false
		if tok == "{" {
			m.subStateTok(newJavaFunctionBodyMachine(m.ctx), func() {}, tok)
			return
		}
		m.javaStateGlobal("static")
		m.javaStateGlobal(tok)
		if m.handleClassBodyBrace(tok) {
			m.statemachineReturn()
		}
		return
	}

	if tok == "static" {
		m.afterStaticKeyword = true
		return
	}

	if tok == "{" && oneOf(m.lastToken, "{};") {
		m.subStateTok(newJavaFunctionBodyMachine(m.ctx), func() {}, tok)
		return
	}

	m.javaStateGlobal(tok)
	if m.handleClassBodyBrace(tok) {
		m.statemachineReturn()
	}
}
