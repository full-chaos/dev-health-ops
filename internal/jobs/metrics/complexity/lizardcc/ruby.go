package lizardcc

import "strings"

// This file ports lizard_languages/rubylike.py's RubylikeStateMachine +
// RubylikeReader for Ruby (lizard_languages/ruby.py's RubyReader inherits
// RubylikeReader unchanged -- the only thing ruby.py itself adds is the
// tokenizer, ruby_tokenize.go).
//
// UNLIKE Kotlin/Scala/Swift/Go/Rust (golike.go), RubylikeStateMachine is
// NOT a GoLikeStates subclass -- it extends CodeStateMachine directly.
// There is no FUNC_KEYWORD-triggered "confirm the function the instant its
// keyword is seen, then brace-count a body" shape here. Instead:
//
//   - A `def` starts a small dedicated sub-chain (stateDef ->
//     stateDefParameters/stateDefContinue/stateDefClassMethod) that scans
//     past an optional parameter list and an optional `self.name` dotted
//     receiver, then hands the REST of the function body to a freshly
//     cloned *rubyMachine (sub_state), closing the function via
//     Context.EndOfFunction only when that clone returns.
//   - Every OTHER nesting construct (`begin`, `do`, `class`, `module`, a
//     bare `{`, and `${` -- the synthetic token this package's tokenizer
//     emits to open a string-interpolation scope) spawns a bare clone of
//     the SAME machine with no callback at all: nesting depth is tracked
//     purely by how many clones are alive, not by any brace counter field.
//   - Function names/parameter values are dropped entirely (this
//     package's declared policy, counter.go's package doc) -- confirmed
//     ruby.py/rubylike.py never branches on a name's VALUE (only whether
//     def_parameters saw a "(" vs any other first token), unlike
//     typescript.go's documented exception.
//
// BUG PRESERVED, NOT FIXED: RubylikeStateMachine never gives `case`/`when`
// its own nesting scope (it is not a member of stateGlobal's
// begin/do/class/module/{/${ tuple) -- a `case ... end` inside a `def`'s
// body sub-machine closes THAT sub-machine at the case statement's own
// `end`, ending the enclosing function early, exactly as real lizard 1.23.0
// measures. This package's corpus keeps `case`/`when` at file scope (see
// testdata/corpus_ruby's own doc) specifically to avoid exercising this
// interaction in a way that would make the fixtures hard to read -- the
// quirk itself is proven, not avoided, by TestGoMatchesLizardGoldenRuby's
// byte-parity contract against whatever real lizard actually reports for
// every corpus file, quirks included.

// rubyControlFlowKeywords/rubyLogicalOperators/rubyTernaryOperators port
// RubylikeReader's separated condition categories (rubylike.py:103-107).
// _case_keywords is deliberately empty for Ruby (case/when scoring comes
// entirely from the bare "when" member of control-flow keywords below, not
// a separate category) -- CodeReader._build_conditions (code_reader.py)
// unions all four categories, so the split has no runtime effect beyond
// documentation; this package folds them into one rubyConditions map the
// same way clike.go's shared `conditions` does for the base CLikeReader.
var rubyConditions = map[string]bool{
	"if": true, "elsif": true, "elseif": true, "until": true, "for": true,
	"while": true, "rescue": true, "ensure": true, "when": true,
	"and": true, "or": true, "||": true, "&&": true,
	"?": true,
}

// rubyFuncKeyword ports RubylikeStateMachine.FUNC_KEYWORD (rubylike.py:15),
// the DEFAULT value -- LuaStateMachine overrides this class attribute to
// "function" (lua.py:34), read dynamically wherever rubylike.py's own
// _state_global spells `self.FUNC_KEYWORD` (its one read site). Ported as
// an instance field (rubyMachine.funcKeyword), not a package const, for
// exactly that reason -- see funcKeyword's own doc below.
const rubyFuncKeyword = "def"

// rubyMachine ports RubylikeStateMachine (rubylike.py:18-95). It embeds
// core directly (not goLike -- see this file's package doc for why) and
// tracks last_token itself, the same pattern golike.go's own feed wrapper
// uses, since core has no notion of "the previous token" on its own.
type rubyMachine struct {
	core
	ctx       *Context
	lastToken string

	// funcKeyword ports self.FUNC_KEYWORD, read by stateGlobal's own
	// dispatch -- rubyFuncKeyword ("def") by default, "function" for a
	// LuaStateMachine instance (lua.go). An instance field rather than a
	// direct reference to the package const because stateGlobal itself is
	// the SAME method value for both languages (Lua overrides
	// _state_global as a whole, but falls back to calling this exact
	// method via super() for every token that isn't "="), so the
	// distinguishing behaviour has to live in data this method reads, not
	// in which method got called.
	funcKeyword string

	// globalState exists ONLY because Python's bare `self._state_global`
	// references INSIDE this file's own methods (stateDefContinue's
	// callback, stateIf, stateForWhile) resolve dynamically to a
	// subclass's override at runtime -- LuaStateMachine overrides
	// _state_global (lua.go). Every such reference is ported here as a
	// read of this field, defaulting to this base's own stateGlobal, the
	// same pattern golike.go's funcNameState/expectFunctionImplState use
	// for the same reason. stateGlobal itself is UNAFFECTED: nothing
	// inside it calls itself by name, so the method value stored in
	// m.state (set directly by whichever constructor built this instance)
	// is all dispatch ever needs there.
	globalState state

	// clone ports self.statemachine_clone() (`self.__class__(self.context)`,
	// code_reader.py:20-21) -- Python's dynamic dispatch always
	// constructs the ACTUAL runtime subclass, never hardcoding
	// RubylikeStateMachine. Every nested block/if/loop-body spawn in this
	// file goes through this field instead of calling newRubyMachine
	// directly, so LuaStateMachine's override (lua.go) is used for every
	// NESTED scope too, not just the file's top-level machine.
	clone func() subMachine
}

func newRubyMachine(ctx *Context) *rubyMachine {
	m := &rubyMachine{ctx: ctx, funcKeyword: rubyFuncKeyword}
	m.state = m.stateGlobal
	m.globalState = m.stateGlobal
	m.clone = func() subMachine { return newRubyMachine(ctx) }
	return m
}

func (m *rubyMachine) feed(tok string) bool {
	exited := m.core.feed(tok)
	m.lastToken = tok
	return exited
}

// isNewline ports RubylikeStateMachine.is_newline (rubylike.py:33-34).
func (m *rubyMachine) isNewline() bool {
	return m.ctx.Newline || m.lastToken == ";"
}

// stateGlobal ports _state_global (rubylike.py:21-42). Case order mirrors
// Python's if/elif chain exactly -- a Go tagless switch has the same
// first-match-wins, fall-through-to-next-case-on-false semantics.
func (m *rubyMachine) stateGlobal(tok string) {
	switch {
	case tok == "end" || tok == "}":
		m.statemachineReturn()

	case tok == m.funcKeyword:
		m.state = m.stateDef

	case tok == "it":
		m.state = m.stateIt

	case (tok == "begin" || tok == "do" || tok == "class" || tok == "module" ||
		tok == "{" || tok == "${") && m.lastToken != ".":
		m.subState(m.clone(), nil)

	case tok == "while" || tok == "for":
		if m.isNewline() {
			m.state = m.stateForWhile
		}

	case tok == "if" || tok == "unless":
		if m.isNewline() {
			m.subState(m.clone(), nil)
		} else {
			m.state = m.stateIf
		}

	case tok == "=begin":
		m.subState(embeddedDoc{}, nil)
	}
}

// stateDef ports _def (rubylike.py:44-50). Function/parameter names are
// dropped (push_new_function's argument is never read by this package --
// see counter.go's package doc), so both branches reduce to "confirm a new
// function, then decide which state reads the rest of the signature."
func (m *rubyMachine) stateDef(tok string) {
	m.ctx.PushNewFunction()
	if tok == "(" {
		m.state = m.stateDefParameters
		return
	}
	m.state = m.stateDefContinue
}

// stateIt ports _it (rubylike.py:52-54): Ruby's `it "description" do ...
// end` / `it("x") { ... }` block-as-function-body convention. Any token
// that is not "do"/"{" is silently ignored, staying in this state --
// PRESERVED, NOT FIXED: a plain identifier/method named "it" used outside
// this convention (e.g. `it = 5`) leaves the machine stuck here until a
// literal "do" or "{" eventually appears somewhere later in the file,
// exactly as real lizard measures (rubylike.py has no escape branch either).
func (m *rubyMachine) stateIt(tok string) {
	if tok == "do" || tok == "{" {
		m.ctx.PushNewFunction()
		m.state = m.stateDefContinue
	}
}

// stateDefContinue ports _def_continue (rubylike.py:56-65). The "anything
// else" branch spawns a fresh clone AND feeds it tok immediately
// (subStateTok, matching Python's `sub_state(clone, callback, token)` --
// contrast stateForWhile below, which spawns via plain subState/no token).
func (m *rubyMachine) stateDefContinue(tok string) {
	switch tok {
	case ".":
		m.state = m.stateDefClassMethod
	case "(":
		m.state = m.stateDefParameters
	default:
		m.subStateTok(m.clone(), func() {
			m.ctx.EndOfFunction()
			m.state = m.globalState
		}, tok)
	}
}

// stateDefClassMethod ports _def_class_method (rubylike.py:67-69): the
// token right after a `.` in `def self.foo` -- add_to_function_name is a
// dropped no-op here (see this file's package doc), so only the state
// transition survives.
func (m *rubyMachine) stateDefClassMethod(tok string) {
	m.state = m.stateDefContinue
}

// stateDefParameters ports _def_parameters (rubylike.py:71-77).
// context.parameter/add_to_long_function_name are both dropped no-ops
// (parameter VALUES never feed CC); only the ")"-terminates-the-list
// transition survives.
func (m *rubyMachine) stateDefParameters(tok string) {
	if tok == ")" {
		m.state = m.stateDefContinue
	}
}

// stateIf ports _if (rubylike.py:79-84): reached only for a NON-newline
// `if`/`unless` (a statement modifier, e.g. `return x if cond`). The
// newline branch re-feeds tok into stateGlobal directly rather than going
// through m.feed -- safe because m.sub is guaranteed nil here (this state
// is only ever active when this machine itself, not a nested clone, is
// currently being driven), matching the same "compute Python's net
// next(state, token) effect directly" approach typescript.go's
// stateExpectFuncOpeningBracket doc explains at length.
func (m *rubyMachine) stateIf(tok string) {
	switch {
	case m.isNewline():
		m.state = m.globalState
		m.globalState(tok)
	case tok == "then":
		m.state = m.globalState
		m.subState(m.clone(), nil)
	}
}

// stateForWhile ports _for_while (rubylike.py:86-90). PRESERVED, NOT
// FIXED: the body clone is spawned via plain subState (no token fed), so
// the very FIRST token of a naked (non-"do") loop body is silently
// dropped -- e.g. `for x in arr\n  foo\nend`'s "foo" never reaches any
// state machine. Confirmed this is Python's own behaviour, not a
// transcription slip: rubylike.py:90's `self.sub_state(self.
// statemachine_clone())` call carries no third (token) argument, unlike
// stateDefContinue's default branch immediately above.
func (m *rubyMachine) stateForWhile(tok string) {
	if m.isNewline() || tok == "do" {
		m.state = m.globalState
		if tok != "end" {
			m.subState(m.clone(), nil)
		}
	}
}

// embeddedDoc ports the module-level state_embedded_doc function
// (rubylike.py:12-13): a trivial single-purpose "state" that only exists
// to be sub_state'd into globally, without needing a whole *rubyMachine
// clone -- it never confirms a function, tracks no fields, and reports
// itself finished the instant it sees "=end". Modelled as its own
// subMachine (rather than reusing core.state, which has no way to signal
// completion by return value -- see submachine.go's doc) since Python's
// sub_state accepts a bare function here, not a whole CodeStateMachine
// instance, and this package's core type distinguishes the two cases via
// separate fields.
type embeddedDoc struct{}

func (embeddedDoc) feed(tok string) bool { return tok == "=end" }

// isRubyComment reports whether tok is a Ruby-style "#"-to-end-of-line
// comment token, as produced by rubyAddition's leading "#"+rubyUntilEnd
// alternative (ruby_tokenize.go). This is Ruby's OWN comment shape
// (ScriptLanguageMixIn.get_comment_from_token, script_language.py:12-13's
// `token.startswith("#")`), distinct from isComment's "/*"/"//" shape that
// every C-family/GoLikeStates reader in this package uses.
func isRubyComment(tok string) bool { return strings.HasPrefix(tok, "#") }

// handleRubyCommentDirectives ports ScriptLanguageMixIn.get_comment_from_token
// (script_language.py:9-23) composed with comment_counter's directive
// dispatch (lizard.py:534-550), for one already-identified Ruby comment
// token. This is NOT the same stripping rule as the shared
// Context.HandleCommentDirectives (counter.go), which only strips a
// leading "/*"/"//" -- deliberately so, since it exists for C-family
// comments where a "#lizard forgive" directive keeps its OWN literal "#"
// inside a "//"-style comment. Ruby's comment marker IS "#", so the
// directive convention here is the marker-stripped form: `# lizard forgive`
// (a single leading "#", Ruby's own comment opener, then plain text) is the
// NORMAL case; `token.lstrip('#')` also tolerates a doubled `##lizard
// forgive` or `# #lizard forgive` by falling back to checking for a
// SECOND leading "#" in what's left, matching Python's own two-prefix
// check exactly.
func handleRubyCommentDirectives(ctx *Context, commentTok string) (stopProcessing bool) {
	stripped := strings.TrimSpace(strings.TrimLeft(commentTok, "#"))
	switch {
	case strings.HasPrefix(stripped, "lizard forgive global"), strings.HasPrefix(stripped, "#lizard forgive global"):
		// Recognised but deliberately inert -- see Context.Forgive's doc
		// (counter.go) for why forgive_global is never ported.
	case strings.HasPrefix(stripped, "lizard forgives("), strings.HasPrefix(stripped, "#lizard forgives("):
		// Named-metric forgiveness -- deliberately unparsed, same as
		// Context.HandleCommentDirectives.
	case strings.HasPrefix(stripped, "lizard forgive"), strings.HasPrefix(stripped, "#lizard forgive"):
		ctx.Forgive = true
	}
	if strings.Contains(commentTok, "GENERATED CODE") {
		logDroppedFunction(ctx, `"GENERATED CODE" comment stopped all further processing`)
		return true
	}
	return false
}

// runRubyFamily is Ruby's own token-driving loop -- golikedriver.go's
// runGoLikeFamily cannot be reused unmodified because Ruby's comment shape
// and directive-stripping rule both differ (isRubyComment/
// handleRubyCommentDirectives above), and Ruby has no macro/preprocessor
// step (script_language.py defines none, and RubylikeReader doesn't add
// one either). Newline-tracking (line_counter, lizard.py:554-568) is
// otherwise identical to every other reader in this package: a bare "\n"
// token is consumed here and never forwarded to the state machine, only
// recorded onto ctx.Newline for whichever REAL token comes next.
func runRubyFamily(tokens []string, root subMachine, ctx *Context) ([]int, bool, error) {
	newlineLocal := true
	for _, tok := range tokens {
		if tok == "\n" {
			newlineLocal = true
			continue
		}
		if isRubyComment(tok) {
			if strings.Contains(tok, "\n") {
				newlineLocal = true
			}
			if handleRubyCommentDirectives(ctx, tok) {
				break
			}
			continue
		}
		ctx.Newline = strings.Contains(tok, "\n") || newlineLocal
		newlineLocal = false
		if rubyConditions[tok] {
			ctx.AddCondition(1)
		}
		root.feed(tok)
	}
	if r, ok := root.(interface{ beforeReturn() }); ok {
		r.beforeReturn()
	}
	return ctx.Complexities, false, nil
}

// AnalyzeRuby is the AnalyzerFunc for Ruby (.rb).
func AnalyzeRuby(path, source string) ([]int, bool, error) {
	ctx := NewContext()
	ctx.SetPath(path)
	tokens := filterWhitespaceKeepNewline(GenerateTokensRuby(source))
	root := newRubyMachine(ctx)
	return runRubyFamily(tokens, root, ctx)
}
