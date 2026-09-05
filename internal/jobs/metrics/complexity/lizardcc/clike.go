package lizardcc

import (
	"unicode"
	"unicode/utf8"
)

// This file ports clike.py's three parallel state machines --
// CLikeStates (function-boundary detection, lines 150-401),
// CLikeNestingStackStates (brace/namespace/template scoping, lines 90-146)
// and CppRValueRefStates (the "&&" rvalue-reference correction, lines
// 68-86) -- plus CLikeReader's own token-driving loop
// (FileAnalyzer.analyze_source_code, lizard.py:607-620, and reader.__call__,
// code_reader.py:208-224).
//
// Every method below is named after, and cites, the Python method it
// reproduces. Anything Python does ONLY to build a human-readable name,
// long name or parameter list is dropped -- see counter.go's package doc --
// so a method's body here is often shorter than its Python original while
// preserving every branch that can change which function a condition token
// counts against.
//
// state is a bound method value, the same trick lizard's own `self._state =
// self._some_method` relies on: reassigning it changes what the NEXT token
// is dispatched to, exactly like Python rebinding an instance attribute.
type state func(tok string)

// Analyze runs the full CLikeReader pipeline over one file's source and
// returns each function's cyclomatic complexity, in the order its body
// closed -- the AnalyzerFunc contract compute.go defines. skipped is never
// true for this reader (lizard's CLikeReader has no parse-failure path the
// way an AST-based reader would); a source lizard cannot make sense of
// still tokenizes and simply yields a low/zero-function result, matching
// lizard's own behaviour of never raising on C-family input.
//
// path is used ONLY for logDroppedFunction's log lines (CHAOS-5156 review
// checklist telemetry rule) -- never read by any complexity computation.
// Added alongside this rule rather than at initial port time, so every
// PRE-EXISTING call site needed updating; both of them (compute.go's
// CFamilyAnalyzer, this package's own parity test) were.
func Analyze(path, source string) (complexities []int, skipped bool, err error) {
	ctx := NewContext()
	ctx.SetPath(path)
	raw := GenerateTokens(source)

	cs := newCLikeStates(ctx)
	ns := newNestingStates(ctx)
	rv := newRValueRefStates(ctx)
	pp := &preprocessor{}

	// ONE loop, in token order: preprocess -> comment filter -> condition
	// count -> parallel state dispatch, for each token before the next is
	// even considered. This mirrors Python's lazy-generator pipeline
	// exactly (see preprocessor.step's doc for why a two-pass version
	// would misattribute every #if/#ifdef/#elif to the wrong function).
	for _, rawTok := range raw {
		tok, ok := pp.step(rawTok, ctx)
		if !ok {
			continue
		}
		// A bare "\n" survives preprocess (clike.py:54 keeps it) but is
		// consumed by line_counter, the NEXT real pipeline stage
		// (lizard.py:554-568), and never reaches condition_counter or the
		// parallel states at all. Forwarding it here was a real bug:
		// stateDecToImp's not-an-identifier fallback fires on ANY
		// non-alpha token, "\n" included, so Allman-style braces (`int
		// f()\n{` -- the opening brace on its own line) abandoned the
		// declaration and lost the function entirely. Every fixture
		// measured here happened to use K&R style, so nothing caught it.
		if tok == "\n" {
			continue
		}
		if isComment(tok) {
			// BUG FIXED HERE (CHAOS-5156, codex round r1 on #2253):
			// `#lizard forgive` and a "GENERATED CODE" marker were
			// previously silent no-ops -- see HandleCommentDirectives'
			// doc (counter.go). A comment carrying "GENERATED CODE" ends
			// the WHOLE analysis right here, exactly as Python's
			// comment_counter generator returning does -- no more tokens
			// reach any state machine, matching lizard's behavior of
			// producing only the functions seen before that comment.
			if ctx.HandleCommentDirectives(tok) {
				break
			}
			continue
		}
		if conditions[tok] {
			ctx.AddCondition(1)
		}
		// Parallel dispatch order matches CLikeReader.__init__'s tuple
		// (clike.py:33-36): CLikeStates first, so a just-confirmed
		// function is visible to CLikeNestingStackStates' add_bare_nesting
		// on the SAME token (see counter.go's createNesting doc).
		cs.feed(tok)
		ns.feed(tok)
		rv.feed(tok)
	}
	return ctx.Complexities, false, nil
}

// ---------------------------------------------------------------------
// CppRValueRefStates (clike.py:68-86)
// ---------------------------------------------------------------------

type rvalueRefStates struct {
	ctx   *Context
	state state
	rut   []string
}

func newRValueRefStates(ctx *Context) *rvalueRefStates {
	m := &rvalueRefStates{ctx: ctx}
	m.state = m.stateGlobal
	return m
}

func (m *rvalueRefStates) feed(tok string) { m.state(tok) }

func (m *rvalueRefStates) stateGlobal(tok string) {
	switch tok {
	case "&&":
		m.state = m.stateRValueRef
	case "typedef":
		m.state = m.stateTypedef
	}
}

// stateRValueRef ports _r_value_ref (clike.py:77-81): a `T&& x = ...` is a
// reference-qualified declaration, not a logical-and, so the "&&" that
// condition_counter already scored is subtracted back out the moment an
// `=` confirms it is a declaration rather than an expression.
func (m *rvalueRefStates) stateRValueRef(tok string) {
	if !oneOf(tok, "=;{})") {
		return
	}
	if tok == "=" {
		m.ctx.AddCondition(-1)
	}
	m.state = m.stateGlobal
}

// stateTypedef ports _typedef (clike.py:83-86): every "&&" between
// `typedef` and its terminating ';' is a reference-type alias, never a
// logical-and, so all of them are subtracted back out at once.
func (m *rvalueRefStates) stateTypedef(tok string) {
	if tok != ";" {
		if tok == "&&" {
			m.rut = append(m.rut, tok)
		}
		return
	}
	m.ctx.AddCondition(-len(m.rut))
	m.rut = nil
	m.state = m.stateGlobal
}

// ---------------------------------------------------------------------
// CLikeNestingStackStates (clike.py:90-146)
// ---------------------------------------------------------------------

type nestingStates struct {
	ctx     *Context
	state   state
	brCount int
}

func newNestingStates(ctx *Context) *nestingStates {
	m := &nestingStates{ctx: ctx}
	m.state = m.stateGlobal
	return m
}

func (m *nestingStates) feed(tok string) { m.state(tok) }

// stateGlobal ports _state_global (clike.py:107-118).
func (m *nestingStates) stateGlobal(tok string) {
	switch {
	case tok == "template":
		m.state = m.stateTemplateDeclaration
	case tok == ".":
		m.state = m.stateDot
	case tok == "struct" || tok == "class" || tok == "namespace" || tok == "union":
		m.state = m.stateReadNamespace
	case tok == "{":
		m.ctx.AddBareNesting()
	case tok == "}":
		m.ctx.PopNesting()
	}
}

// stateDot ports _dot (clike.py:120-121): a lone '.' after a name (e.g. a
// namespace-looking prefix that was actually a member-access expression)
// aborts the namespace read for exactly one token, then resumes normal
// dispatch.
func (m *nestingStates) stateDot(tok string) { m.state = m.stateGlobal }

// stateReadNamespace ports _read_namespace (clike.py:123-129).
func (m *nestingStates) stateReadNamespace(tok string) {
	if tok == "[" {
		m.state = m.stateReadAttribute
	} else {
		m.state = m.stateReadNamespaceName
	}
	m.state(tok)
}

// stateReadNamespaceName ports _read_namespace_name (clike.py:131-137).
// Python also builds a display name from the accumulated tokens
// (itertools.takewhile over namespace separators); nothing here ever reads
// that name, so only the "did a body actually open" branch survives.
func (m *nestingStates) stateReadNamespaceName(tok string) {
	if !oneOf(tok, ")({;") {
		return
	}
	m.state = m.stateGlobal
	if tok == "{" {
		m.ctx.AddNamespace()
	}
}

// stateTemplateDeclaration ports _template_declaration (clike.py:139-142):
// skip a `template<...>` parameter list without registering it as a
// function or a scope.
func (m *nestingStates) stateTemplateDeclaration(tok string) {
	switch tok {
	case "<":
		m.brCount++
	case ">":
		m.brCount--
	}
	if m.brCount == 0 {
		m.state = m.stateGlobal
	}
}

// stateReadAttribute ports _read_attribute (clike.py:144-146): ignore a
// C++11 `[[attribute]]` between a class/struct keyword and its name.
func (m *nestingStates) stateReadAttribute(tok string) {
	switch tok {
	case "[":
		m.brCount++
	case "]":
		m.brCount--
	}
	if m.brCount == 0 {
		m.state = m.stateReadNamespace
	}
}

// ---------------------------------------------------------------------
// CLikeStates (clike.py:150-401)
// ---------------------------------------------------------------------

type clikeStates struct {
	ctx     *Context
	state   state
	brCount int
	// bracketStack ports self.bracket_stack: shared between _state_dec and
	// the lambda states exactly as in Python (see clike.go's package doc
	// for why one shared field is safe: every user of it returns the stack
	// to empty before any other user is entered).
	bracketStack []string

	// savedTokens ports self._saved_tokens (clike.py:248,272-288):
	// stateOldCParams's own buffer, populated by stateDecToImp's else
	// branch and replayed through stateGlobal on ambiguous input. See
	// stateOldCParams' doc for why this exists at all.
	savedTokens []string
}

func newCLikeStates(ctx *Context) *clikeStates {
	m := &clikeStates{ctx: ctx}
	m.state = m.stateGlobal
	return m
}

func (m *clikeStates) feed(tok string) { m.state(tok) }

// isAlpha reports whether r is a valid identifier-start letter in lizard's
// Unicode-aware sense -- Python's `token[0].isalpha()` equivalent.
//
// BUG FIXED HERE (CHAOS-5156, codex round r1 on #2253/#2268/#2269): this
// used to take a byte and check only the ASCII ranges, so a token whose
// first BYTE was part of a multi-byte UTF-8 encoding (e.g. `é`, whose
// leading byte is 0xC3) was never recognised as an identifier start, even
// after tokenize.go's `\w+`->`[\p{L}\p{N}_]+` fix glues the whole rune
// sequence into one token. Confirmed against real lizard 1.23.0: a
// function/method whose name begins with a non-ASCII letter (`café()` in
// C++, `é()` in Java) is recognised there and was silently dropped here.
func isAlpha(r rune) bool { return unicode.IsLetter(r) }

// firstByte decodes tok's first RUNE (not byte, despite the name kept for
// call-site continuity) guarded against an empty token. The tokenizer never
// actually emits one (every alternative in tokenize.go requires at least
// one character), but every C-family state that switches on a token's
// first character reads this instead of indexing/decoding directly, so
// malformed input can never panic the analyzer -- see AnalyzeFile's
// contract in compute.go (an error, never a panic, is how a bad file is
// reported). utf8.DecodeRuneInString returns utf8.RuneError (still a valid,
// non-matching rune) on invalid UTF-8, so this never panics either.
func firstByte(tok string) rune {
	if tok == "" {
		return 0
	}
	r, _ := utf8.DecodeRuneInString(tok)
	return r
}

// tryNewFunction ports try_new_function (clike.py:161-165).
func (m *clikeStates) tryNewFunction(tok string) {
	m.ctx.TryNewFunction()
	m.state = m.stateFunction
	if tok == "operator" {
		m.state = m.stateOperator
	}
}

// stateGlobal ports _state_global (clike.py:167-174).
func (m *clikeStates) stateGlobal(tok string) {
	b := firstByte(tok)
	if b == '_' || b == '~' || isAlpha(b) {
		m.tryNewFunction(tok)
	} else if tok == "[" {
		m.state = m.stateLambdaCheck
	}
}

// stateFunction ports _state_function (clike.py:176-185).
func (m *clikeStates) stateFunction(tok string) {
	switch tok {
	case "(":
		m.state = m.stateDec
		m.stateDec(tok)
	case "::":
		m.state = m.stateNameWithSpace
	case "<":
		m.state = m.stateTemplateInName
		m.stateTemplateInName(tok)
	default:
		m.state = m.stateGlobal
		m.stateGlobal(tok)
	}
}

// stateTemplateInName ports _state_template_in_name (clike.py:187-189):
// skip `<...>` inside a qualified/templated function name.
func (m *clikeStates) stateTemplateInName(tok string) {
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

// stateOperator ports _state_operator (clike.py:191-194).
func (m *clikeStates) stateOperator(tok string) {
	if tok != "(" {
		m.state = m.stateOperatorNext
	}
}

// stateOperatorNext ports _state_operator_next (clike.py:196-200).
func (m *clikeStates) stateOperatorNext(tok string) {
	if tok == "(" {
		m.stateFunction(tok)
	}
}

// stateNameWithSpace ports _state_name_with_space (clike.py:202-205):
// `Foo::Bar` (a qualified name or an out-of-line definition).
func (m *clikeStates) stateNameWithSpace(tok string) {
	if tok == "operator" {
		m.state = m.stateOperator
	} else {
		m.state = m.stateFunction
	}
}

// parameterOpen/parameterClose port parameter_bracket_open = '(<' and
// parameter_bracket_close = ')>' (clike.py:153-154).
func parameterOpen(tok string) bool  { return tok == "(" || tok == "<" }
func parameterClose(tok string) bool { return tok == ")" || tok == ">" }

// stateDec ports _state_dec (clike.py:207-220). Its ONLY effect that can
// ever reach a complexity number is the early-abort branch (a stray '>'
// or ')' seen while bracketStack is already empty forces control straight
// back to _state_global, matching a real, if obscure, lizard quirk for a
// default-argument expression using a bare '>' comparison) -- everything
// else here exists purely to build a parameter list Python reports and
// this package never reads, so it is preserved for the abort branch alone.
func (m *clikeStates) stateDec(tok string) {
	m.brCount += bracketDelta(tok, "(", ")")
	m.stateDecBody(tok)
	if m.brCount == 0 {
		m.state = m.stateDecToImp
	}
}

func (m *clikeStates) stateDecBody(tok string) {
	switch {
	case parameterOpen(tok):
		m.bracketStack = append(m.bracketStack, tok)
	case parameterClose(tok):
		if len(m.bracketStack) > 0 {
			m.bracketStack = m.bracketStack[:len(m.bracketStack)-1]
		} else {
			m.state = m.stateGlobal
		}
	}
}

func bracketDelta(tok, open, close string) int {
	switch tok {
	case open:
		return 1
	case close:
		return -1
	default:
		return 0
	}
}

// stateDecToImp ports _state_dec_to_imp (clike.py:222-249).
//
// BUG FIXED HERE (CHAOS-5156, codex round r1 on #2253): Python's real
// _state_dec_to_imp checks `token in ('const', '&', '&&')` FIRST, before
// the "not alpha -> abandon" fallback -- that branch only ever calls
// `add_to_long_function_name`, a name-building side effect this package
// never reads, so it is a pure no-op here: the token is consumed and
// stateDecToImp stays active for the next one. Without this branch, a
// ref-qualified C++ member (`int f() & { ... }`, `int g() const { ... }`)
// hit the "not alpha" fallback instead (since '&' and '&&' are not
// alphabetic) and the whole function was abandoned. Confirmed against
// real lizard 1.23.0: `int f() & { return 0; }` measures `[1]`, this
// reader measured `[]` before the fix.
func (m *clikeStates) stateDecToImp(tok string) {
	switch {
	case tok == "const" || tok == "&" || tok == "&&":
		// no-op: stay in stateDecToImp, matching Python's
		// add_to_long_function_name-only side effect.
	case tok == "throw":
		m.state = m.stateThrow
	case tok == "throws":
		m.state = m.stateThrows
	case tok == "->":
		m.state = m.stateTrailingReturn
	case tok == "noexcept":
		m.state = m.stateNoexcept
	case tok == "(":
		// Python passes current_function.long_name here (a function
		// returning a function pointer, e.g. `int (*make(int))(int)`).
		// This package tracks no name, so the dummy argument below is
		// never "operator" (the only string tryNewFunction inspects) --
		// functionally identical for every reachable long_name value.
		m.tryNewFunction("")
		m.stateFunction(tok)
	case tok == "{":
		m.stateEnteringImp(tok)
	case tok == ":":
		m.state = m.stateInitializationList
	case tok == "[":
		m.state = m.stateAttribute
		m.stateAttribute(tok)
	case !(firstByte(tok) == '_' || isAlpha(firstByte(tok))):
		m.state = m.stateGlobal
		m.stateGlobal(tok)
	default:
		m.state = m.stateOldCParams
		m.savedTokens = []string{tok}
	}
}

// stateThrow ports _state_throw (clike.py:251-253): skip a C++
// `throw(...)` exception specification.
func (m *clikeStates) stateThrow(tok string) {
	m.brCount += bracketDelta(tok, "(", ")")
	if m.brCount == 0 {
		m.state = m.stateDecToImp
	}
}

// stateThrows ports _state_throws (clike.py:255-258): skip a Java
// `throws A, B` clause.
func (m *clikeStates) stateThrows(tok string) {
	if !oneOf(tok, ";{") {
		return
	}
	m.state = m.stateDecToImp
	m.stateDecToImp(tok)
}

// stateNoexcept ports _state_noexcept (clike.py:260-265).
func (m *clikeStates) stateNoexcept(tok string) {
	if tok == "(" {
		m.state = m.stateThrow
	} else {
		m.state = m.stateDecToImp
	}
	m.state(tok)
}

// stateTrailingReturn ports _state_trailing_return (clike.py:267-270):
// skip a C++11 `-> ReturnType` trailing return type.
func (m *clikeStates) stateTrailingReturn(tok string) {
	if !oneOf(tok, ";{") {
		return
	}
	m.state = m.stateDecToImp
	m.stateDecToImp(tok)
}

// stateOldCParams ports _state_old_c_params (clike.py:272-288) in full,
// including its buffer/replay semantics -- an old-style K&R parameter-type
// list between ')' and '{' (`int f(a, b)\n  int a, b;\n{ ... }`) is the
// INTENDED shape, but the buffer exists precisely because Python cannot
// tell that shape apart from an ordinary trailing qualifier (`override`,
// `final`, ...) until it sees what follows: every token entered here is
// appended to savedTokens FIRST (initialized to [tok] by stateDecToImp's
// own else branch, see its doc), then:
//   - ';' clears the buffer and returns to stateDecToImp with nothing
//     replayed (the K&R-params reading was correct).
//   - '{' with EXACTLY 2 buffered tokens (the original trigger token plus
//     this '{' itself, i.e. ZERO tokens appeared in between) re-enters
//     stateDecToImp on '{' directly -- confirming the function normally.
//   - '{' with any OTHER buffer length, or '(': the whole reading was
//     WRONG (more than one bare token appeared where K&R params or a
//     single qualifier was expected) -- every buffered token, in order,
//     is REPLAYED through stateGlobal, exactly as if none of this had
//     ever been a function declaration.
//
// BUG FIXED HERE (CHAOS-5156, codex round r1 on #2253, mirroring a REAL
// lizard quirk rather than "fixing" it): the earlier revision only
// tracked confirm-vs-abandon, never replayed the buffered tokens through
// stateGlobal on abandonment. Confirmed against real lizard 1.23.0:
// `virtual int f() const override final { return 0; }` -- "const" is
// consumed by stateDecToImp's own no-op branch, "override" enters this
// state (savedTokens=["override"]), "final" is buffered
// (savedTokens=["override","final"], matches no case), then '{' arrives
// with a 3-token buffer (not 2), so lizard treats the WHOLE thing as
// abandoned and replays "override","final","{" through stateGlobal --
// none of which forms a valid declaration, so `f` is dropped entirely.
// Lizard measures NO function for this input; this port measured [1]
// (confirmed the function) before this fix. This is a genuine, surprising
// lizard behavior this package MIRRORS, not a bug to design around --
// see the pinned regression fixture's own comment.
func (m *clikeStates) stateOldCParams(tok string) {
	m.savedTokens = append(m.savedTokens, tok)
	switch tok {
	case ";":
		m.savedTokens = nil
		m.state = m.stateDecToImp
	case "{":
		if len(m.savedTokens) == 2 {
			m.savedTokens = nil
			m.stateDecToImp(tok)
			return
		}
		m.replaySavedTokensThroughGlobal()
	case "(":
		m.replaySavedTokensThroughGlobal()
	}
}

// replaySavedTokensThroughGlobal ports the two identical replay branches
// in _state_old_c_params (clike.py:283-288): reset to stateGlobal, then
// feed each buffered token through WHATEVER m.state is at that moment --
// exactly like Python's `self._state(tkn)` re-reading the instance
// attribute fresh each iteration, since a replayed token can itself
// change m.state (e.g. an identifier-shaped one starts a new candidate
// function via tryNewFunction) before the next replayed token arrives.
func (m *clikeStates) replaySavedTokensThroughGlobal() {
	// Telemetry (CHAOS-5156 review checklist): the candidate this
	// declaration was building never reached ConfirmNewFunction (that only
	// happens in stateEnteringImp, on '{' from stateDecToImp directly --
	// never from here), so nothing in Complexities is being removed. But a
	// reader watching real source go by would reasonably expect a
	// function-shaped declaration ending in '{' to become one; silently
	// discarding it with no signal is exactly the swallowed-outcome shape
	// worth a log line, even though no "score" is being thrown away.
	logDroppedFunction(m.ctx, "ambiguous declaration abandoned (stateOldCParams replay)")
	saved := m.savedTokens
	m.savedTokens = nil
	m.state = m.stateGlobal
	for _, t := range saved {
		m.state(t)
	}
}

// stateInitializationList ports _state_initialization_list (clike.py:290-293):
// a constructor's `: base(x), member(y)` initializer list.
func (m *clikeStates) stateInitializationList(tok string) {
	m.state = m.stateOneInitialization
	if tok == "{" {
		m.stateEnteringImp(tok)
	}
}

// stateOneInitialization ports _state_one_initialization (clike.py:295-301).
func (m *clikeStates) stateOneInitialization(tok string) {
	if !oneOf(tok, "({") {
		return
	}
	if tok == "(" {
		m.state = m.stateInitializationValue1
	} else {
		m.state = m.stateInitializationValue2
	}
	m.state(tok)
}

// stateInitializationValue1 ports _state_initialization_value1 (clike.py:303-305).
func (m *clikeStates) stateInitializationValue1(tok string) {
	m.brCount += bracketDelta(tok, "(", ")")
	if m.brCount == 0 {
		m.state = m.stateInitializationList
	}
}

// stateInitializationValue2 ports _state_initialization_value2 (clike.py:307-309).
func (m *clikeStates) stateInitializationValue2(tok string) {
	m.brCount += bracketDelta(tok, "{", "}")
	if m.brCount == 0 {
		m.state = m.stateInitializationList
	}
}

// stateEnteringImp ports _state_entering_imp (clike.py:311-313): the
// declaration is real, confirm it and enter its body.
func (m *clikeStates) stateEnteringImp(tok string) {
	m.ctx.ConfirmNewFunction()
	m.state = m.stateImp
	m.stateImp(tok)
}

// stateImp ports _state_imp (clike.py:315-317): swallow the entire
// function body, tracking only brace depth, until the matching '}'.
func (m *clikeStates) stateImp(tok string) {
	m.brCount += bracketDelta(tok, "{", "}")
	if m.brCount == 0 {
		m.state = m.stateGlobal
	}
}

// stateAttribute ports _state_attribute (clike.py:319-322): ignore a
// C++11 `[[attribute]]` function attribute.
func (m *clikeStates) stateAttribute(tok string) {
	m.brCount += bracketDelta(tok, "[", "]")
	if m.brCount == 0 {
		m.state = m.stateDecToImp
	}
}

// stateLambdaCheck ports _state_lambda_check (clike.py:324-334).
func (m *clikeStates) stateLambdaCheck(tok string) {
	switch tok {
	case "]":
		m.state = m.stateLambdaParams
	case "[":
		m.state = m.stateAttribute
	default:
		m.state = m.stateLambdaCapture
	}
}

// stateLambdaParams ports _state_lambda_params (clike.py:336-345).
func (m *clikeStates) stateLambdaParams(tok string) {
	if tok == "(" {
		m.bracketStack = append(m.bracketStack, "(")
		m.state = m.stateLambdaParamList
	} else {
		m.state = m.stateLambdaBody
		m.stateLambdaBody(tok)
	}
}

// stateLambdaParamList ports _state_lambda_param_list (clike.py:347-362).
func (m *clikeStates) stateLambdaParamList(tok string) {
	top := func() string {
		if len(m.bracketStack) == 0 {
			return ""
		}
		return m.bracketStack[len(m.bracketStack)-1]
	}
	switch {
	case tok == "(":
		m.bracketStack = append(m.bracketStack, "(")
	case tok == ")":
		if top() == "(" {
			m.bracketStack = m.bracketStack[:len(m.bracketStack)-1]
			if len(m.bracketStack) == 0 {
				m.state = m.stateLambdaBody
			}
		}
	case tok == "<" || tok == "[":
		m.bracketStack = append(m.bracketStack, tok)
	case tok == ">" && top() == "<":
		m.bracketStack = m.bracketStack[:len(m.bracketStack)-1]
	case tok == "]" && top() == "[":
		m.bracketStack = m.bracketStack[:len(m.bracketStack)-1]
	}
}

// stateLambdaBody ports _state_lambda_body (clike.py:364-382).
func (m *clikeStates) stateLambdaBody(tok string) {
	switch {
	case tok == "{":
		m.bracketStack = append(m.bracketStack, "{")
		m.state = m.stateLambdaBodySkip
	case tok == "mutable" || tok == "noexcept" || tok == "constexpr" || tok == "consteval":
	case tok == "->":
	case tok == ";" || tok == "," || tok == ")":
		m.state = m.stateGlobal
		m.stateGlobal(tok)
	}
}

// stateLambdaBodySkip ports _state_lambda_body_skip (clike.py:384-393).
func (m *clikeStates) stateLambdaBodySkip(tok string) {
	switch tok {
	case "{":
		m.bracketStack = append(m.bracketStack, "{")
	case "}":
		if len(m.bracketStack) > 0 && m.bracketStack[len(m.bracketStack)-1] == "{" {
			m.bracketStack = m.bracketStack[:len(m.bracketStack)-1]
			if len(m.bracketStack) == 0 {
				m.state = m.stateGlobal
			}
		}
	}
}

// stateLambdaCapture ports _state_lambda_capture (clike.py:395-400).
func (m *clikeStates) stateLambdaCapture(tok string) {
	if tok == "]" {
		m.state = m.stateLambdaParams
	}
}
