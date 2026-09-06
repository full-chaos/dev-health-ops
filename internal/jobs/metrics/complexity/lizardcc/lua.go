package lizardcc

import "strings"

// This file ports lizard_languages/lua.py's LuaReader + LuaStateMachine.
// LuaStateMachine(RubylikeStateMachine) overrides ONLY _state_global and
// FUNC_KEYWORD, and adds two new states (_assigning, _anonymous_def) --
// every other Ruby state (_def, _def_continue, _def_parameters,
// _def_class_method, _it, _if, _for_while) is inherited UNCHANGED. This
// port wraps a *rubyMachine (ruby.go) rather than duplicating it, using
// the SAME globalState/clone/funcKeyword hooks csharp.go/objc.go use to
// extend clikeStates -- see ruby.go's own doc for why those hooks exist
// at all (Python's `self.__class__(...)`/dynamic `self._state_global`
// resolution needed an explicit Go equivalent once a second reader needed
// to extend RubylikeStateMachine).
//
// luaMachine calls RubylikeStateMachine's OWN _def/_def_parameters states
// directly (l.r.stateDef/l.r.stateDefParameters) for _anonymous_def's two
// branches -- these are NOT overridden by Lua, so Python's plain
// `self._def`/`self._def_parameters` references resolve to
// RubylikeStateMachine's own methods for a LuaStateMachine instance too,
// exactly like objcMachine calling clikeStates.stateEnteringImp directly
// (objc.go's own doc) for a method CLikeStates defines and ObjCStates
// never overrides.

// luaMachine wraps rubyMachine with Lua's overrides.
type luaMachine struct {
	r   *rubyMachine
	ctx *Context
}

func newLuaMachine(ctx *Context) *luaMachine {
	l := &luaMachine{ctx: ctx}
	l.r = newRubyMachine(ctx)
	l.r.funcKeyword = "function" // LuaStateMachine.FUNC_KEYWORD (lua.py:34)
	l.r.globalState = l.stateGlobal
	l.r.clone = func() subMachine { return newLuaMachine(ctx) }
	l.r.state = l.stateGlobal
	return l
}

func (l *luaMachine) feed(tok string) bool { return l.r.feed(tok) }

// stateGlobal ports LuaStateMachine._state_global (lua.py:38-42).
// probable_function_name is dropped entirely: it exists purely to record
// a candidate NAME for push_new_function, which this package's Context
// never reads (counter.go's package doc) -- Python re-assigns it on
// EVERY token reaching this method (even ones _anonymous_def never uses
// it for), so there is no other reachable effect to preserve.
func (l *luaMachine) stateGlobal(tok string) {
	if tok == "=" {
		l.r.state = l.stateAssigning
		return
	}
	l.r.stateGlobal(tok)
}

// stateAssigning ports _assigning (lua.py:44-48): after a bare "=" at
// global scope, "function" opens a function-expression RHS
// (`x = function(...) ... end`, Lua's anonymous-function-assignment
// idiom); any other token means this was an ordinary assignment, and
// that token is re-fed through stateGlobal -- Python's
// `self.next(self._state_global, token)` is dynamic in principle, but
// since this IS Lua's own stateGlobal (the only override that exists),
// calling it directly is exactly equivalent, the same "compute Python's
// net next(state, token) effect directly" shape ruby.go's own stateIf
// doc explains at length.
func (l *luaMachine) stateAssigning(tok string) {
	if tok == "function" {
		l.r.state = l.stateAnonymousDef
		return
	}
	l.stateGlobal(tok)
}

// stateAnonymousDef ports _anonymous_def (lua.py:50-55): the token right
// after "function" in an assignment RHS. If it's not "(", this is a
// NAMED function expression (`x = function foo(...) ... end`) -- hand off
// to RubylikeStateMachine's own _def (unmodified, re-fed this token,
// matching Python's `self.next(self._def, token)`). If it IS "(", this is
// a truly anonymous function literal: push a new function directly and
// move to _def_parameters WITHOUT re-feeding "(" -- matching Ruby's own
// _def "(" branch's identical no-refeed shape (rubylike.py:47-49,
// ruby.go's stateDef).
func (l *luaMachine) stateAnonymousDef(tok string) {
	if tok != "(" {
		l.r.state = l.r.stateDef
		l.r.stateDef(tok)
		return
	}
	l.ctx.PushNewFunction()
	l.r.state = l.r.stateDefParameters
}

// isLuaComment reports whether tok is a Lua comment token, as produced by
// luaAddition's two comment alternatives (lua_tokenize.go): both the
// line-comment ("--...") and block-comment ("--[[...]]") forms start with
// "--", exactly matching LuaReader.get_comment_from_token's own predicate
// (lua.py:23-24) -- a DIFFERENT shape from both isComment's "/*"/"//" (the
// C-family/GoLikeStates shape) and isRubyComment's "#" (ruby.go), so Lua
// gets its own driver rather than reusing either.
func isLuaComment(tok string) bool { return strings.HasPrefix(tok, "--") }

// handleLuaCommentDirectives ports comment_counter's directive dispatch
// (lizard.py:534-550) for a Lua comment token, using LuaReader's OWN
// get_comment_from_token (lua.py:23-24, `if token.startswith("--"): return
// token` -- the comment marker itself is NEVER stripped, unlike Ruby's
// `#`-marker-stripping override). This means Lua's own forgive-directive
// convention needs an EXPLICIT "#" the same way C-family's does
// (`-- #lizard forgive`, not Ruby's marker-agnostic `# lizard forgive`) --
// so this reuses the SAME "#lizard forgive" prefix-matching Context.
// HandleCommentDirectives (counter.go) already applies for "/*"/"//",
// just triggered by "--" instead; not calling that shared function
// directly since its own doc scopes it to the C-family's two markers
// specifically, and duplicating its ~15 lines here keeps that scoping
// honest rather than silently widening it.
func handleLuaCommentDirectives(ctx *Context, commentTok string) (stopProcessing bool) {
	stripped := strings.TrimSpace(commentTok)
	switch {
	case strings.HasPrefix(stripped, "#lizard forgive global"):
		// Recognised but deliberately inert -- see Context.Forgive's doc
		// (counter.go) for why forgive_global is never ported.
	case strings.HasPrefix(stripped, "#lizard forgives("):
		// Named-metric forgiveness -- deliberately unparsed, same as
		// Context.HandleCommentDirectives.
	case strings.HasPrefix(stripped, "#lizard forgive"):
		ctx.Forgive = true
	}
	if strings.Contains(commentTok, "GENERATED CODE") {
		logDroppedFunction(ctx, `"GENERATED CODE" comment stopped all further processing`)
		return true
	}
	return false
}

// runLuaFamily is Lua's own token-driving loop -- neither
// golikedriver.go's runGoLikeFamily (C-family comment shape) nor ruby.go's
// runRubyFamily ("#"-marker comment shape) matches Lua's "--"-marker
// comments, and Lua has no macro/preprocessor step of its own beyond
// accumulateMacros (already folded into GenerateTokensLua,
// lua_tokenize.go). Newline-tracking mirrors every other driver in this
// package: a bare "\n" is consumed here, never forwarded to the state
// machine, only recorded onto ctx.Newline for whichever real token comes
// next -- read by rubyMachine.isNewline (ruby.go), reused unmodified for
// Lua's own inherited _if/_for_while states.
func runLuaFamily(tokens []string, root subMachine, ctx *Context) ([]int, bool, error) {
	newlineLocal := true
	for _, tok := range tokens {
		if tok == "\n" {
			newlineLocal = true
			continue
		}
		if isLuaComment(tok) {
			if strings.Contains(tok, "\n") {
				newlineLocal = true
			}
			if handleLuaCommentDirectives(ctx, tok) {
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

// AnalyzeLua is the AnalyzerFunc for Lua (.lua). It reuses rubyConditions
// (ruby.go) directly: LuaReader defines no condition-category override of
// its own (lua.py has none), so it inherits RubylikeReader's separated
// categories unmodified -- the same "no override, reuse the shared map"
// shape java.go/objc.go use for clike.go's own conditions.
func AnalyzeLua(path, source string) ([]int, bool, error) {
	ctx := NewContext()
	ctx.SetPath(path)
	tokens := filterWhitespaceKeepNewline(GenerateTokensLua(source))
	root := newLuaMachine(ctx)
	return runLuaFamily(tokens, root, ctx)
}
