// Package lizardcc ports lizard 1.23.0's C-family cyclomatic-complexity
// engine to Go (CHAOS-5156 / CHAOS-4971b), for the extensions Python routes
// to lizard's CLikeReader and its direct subclasses: c, cpp, cc, cxx, h,
// hpp (CLikeReader itself), java (JavaReader) and cs (CSharpReader).
//
// # WHY A STATE-MACHINE PORT, NOT AN AST WALK
//
// Unlike radon (an AST visitor, ported in the sibling pycc package), lizard
// never parses a grammar at all: it scans a REGEX-tokenized stream through a
// hand-written state machine that tracks brace/paren nesting well enough to
// find function boundaries, and increments a per-function counter whenever
// one of a small set of literal condition tokens goes by. Reproducing lizard
// therefore means reproducing that state machine, token-for-token and
// state-for-state -- an AST-based reimplementation would compute a DIFFERENT
// (arguably more "correct") number that happens to disagree with the tool
// this family's output must match bit for bit.
//
// # WHAT IS AND ISN'T PORTED
//
// This package keeps every state transition from lizard_languages/clike.py
// that can change which token stream a condition-token lands against (brace
// nesting, template/namespace scoping, parameter parsing, lambda bodies,
// rvalue-reference disambiguation). It drops everything lizard computes
// ONLY for its own human-facing report -- function names, long names,
// parameter lists, NLOC, token counts -- because CHAOS-4291's FileComplexity
// never reads them: BuildFileResult (compute.go) derives LOC, count, total,
// average and both threshold counters from the raw per-function complexity
// numbers alone. Every exported entry point below returns exactly that: a
// slice of per-function complexities, mirroring the AnalyzerFunc contract
// compute.go defines.
package lizardcc

import "strings"

// DropLogger is the narrow logging capability this package needs to report
// a function silently dropped instead of scored (chris, CHAOS-5156 review
// checklist: "add telemetry and debug logs... a swallowed error... is a
// REVIEW FINDING"). This mirrors the codebase's established SetLogger-style
// optionality (internal/jobs/metrics/remaining's MembershipLogger et al.,
// satisfied directly by *slog.Logger) but as a PACKAGE-LEVEL var rather than
// a struct field + setter: every AnalyzerFunc in this package is a pure
// function (compute.go's contract), with no executor/struct to attach a
// setter to, and threading a logger through every call site's argument
// list would ripple across compute.go's DefaultAnalyzers() map for every
// language for a single non-fatal, informational log line.
type DropLogger interface {
	Warn(msg string, args ...any)
}

// Logger receives one Warn call per function this package drops instead of
// appending to Complexities. Nil (the default) is a no-op, checked before
// every use -- same discipline as the codebase's other optional loggers.
var Logger DropLogger

// logDroppedFunction is the single call site every drop path in this
// package routes through, so the log line's shape never drifts between
// callers. path may be empty (not every AnalyzerFunc signature threads it
// through; see Context.SetPath).
func logDroppedFunction(ctx *Context, reason string) {
	if Logger == nil {
		return
	}
	Logger.Warn("lizardcc: function dropped, not scored", "reason", reason, "path", ctx.path)
}

// function is one analysed function or method body. lizard's FunctionInfo
// (lizard.py:302-372) carries name/params/nloc/fan-in/fan-out for its report;
// none of that feeds a stored row here, so only the one field CC needs is
// kept.
type function struct {
	cc int
}

// nesting is one entry on the brace-nesting stack (lizard.py:400-438's
// NestingStack, folded into Context since nothing else needs it standalone).
// A '{' that opens a just-confirmed function's body pushes the function
// itself, so its matching '}' can find and close it; every other '{'
// (a class body, a namespace, a bare block, an initializer list) pushes a
// bare marker that unwinds without ending any function.
type nesting struct {
	fn *function // non-nil iff this entry IS the function whose body this brace opened
}

// Context is lizard's "shared counter": FileInfoBuilder (lizard.py:441-524)
// ported far enough to reproduce CyclomaticComplexity per function and
// nothing else. It is the context object every reader's parallel state
// machines call into -- CLikeStates, CLikeNestingStackStates and
// CppRValueRefStates in this PR; GoLikeStates-derived readers (Go, Rust,
// Kotlin, Scala, Swift) reuse the same TryNewFunction/PushNewFunction/
// AddCondition surface in a later PR without needing a second counter.
type Context struct {
	global  function // the '*global* pseudo-function; see endOfFunction
	current *function

	// stacked holds a suspended function while a NESTED function is being
	// parsed inside it. Plain CLikeReader never pushes onto this (C/C++/
	// Java/C# don't nest function declarations lexically the way Go's
	// closures do) but PushNewFunction is kept for the GoLike family that
	// reuses this Context.
	stacked []*function

	nestingStack []nesting

	// pending is the function whose body's opening '{' has not yet been
	// consumed by AddBareNesting. ConfirmNewFunction sets it,
	// createNesting consumes it exactly once (lizard.py:416-424's
	// start_new_function_nesting / _create_nesting).
	pending *function

	// Complexities collects each finished function's cyclomatic
	// complexity, in the order its body closed. This IS the `complexities`
	// return value of the AnalyzerFunc contract (compute.go).
	Complexities []int

	// Newline ports FileInfoBuilder.newline (lizard.py:454, set by
	// add_nloc at lizard.py:482): true when the token currently being
	// processed is the first one on a new source line (or itself contains
	// an embedded newline, e.g. a multi-line comment). Neither Go nor Rust
	// reads this (golikedriver.go maintains it unconditionally for every
	// GoLikeStates-derived reader since a later PR's Scala reader does).
	Newline bool

	// Forgive ports FileInfoBuilder.forgive (lizard.py:452, read/reset by
	// end_of_function at lizard.py:516-521): a bare `#lizard forgive`
	// comment anywhere in the file sets this true, and the very next
	// function to close is excluded from Complexities entirely (not
	// scored as zero -- simply never appended), matching lizard's own
	// per-function opt-out. Reset to false unconditionally after every
	// EndOfFunction call, forgiving exactly one function per comment.
	//
	// forgive_global (lizard.py:453, set by a `#lizard forgive global`
	// comment) is deliberately NOT ported: its only effect is excluding
	// the GLOBAL PSEUDO-FUNCTION from function_list, which this package's
	// EndOfFunction already never appends regardless (the `c.current !=
	// &c.global` guard below) -- forgive_global can only ever narrow a
	// check that is already unconditionally false here, so the extra
	// field would track state nothing reads.
	//
	// BUG FIXED HERE (CHAOS-5156, codex round r1 on #2253): this
	// mechanism did not exist in this package at all before this fix --
	// `#lizard forgive` and a "GENERATED CODE" comment marker (see
	// HandleCommentDirectives below) were silently no-ops, so a forgiven
	// function was scored and counted like any other, and lizard's
	// stop-processing-entirely behavior on GENERATED CODE was never
	// honored. Confirmed against real lizard 1.23.0.
	Forgive bool

	// path is the file path being analysed, purely for logDroppedFunction's
	// benefit -- never read by any complexity computation. Set via SetPath;
	// empty by default (compute.go's AnalyzerFunc contract passes path to
	// every reader, but not every reader threads it into NewContext, so
	// this is set as a distinct step rather than a NewContext parameter to
	// avoid changing that constructor's signature everywhere it's called).
	path string
}

// SetPath records the file path being analysed, for logDroppedFunction's
// log lines only.
func (c *Context) SetPath(path string) { c.path = path }

// NewContext returns a Context positioned at file scope, matching
// FileInfoBuilder.__init__'s global_pseudo_function (lizard.py:449-457).
func NewContext() *Context {
	c := &Context{}
	c.current = &c.global
	return c
}

// TryNewFunction ports FileInfoBuilder.try_new_function (lizard.py:484-489).
// Every alpha/underscore/tilde-leading token in a declaration position
// starts a candidate function; most candidates are abandoned (a variable
// declaration, a type name) without ever being confirmed, exactly as in
// Python -- try_new_function only ever creates a throwaway that the next
// TryNewFunction call silently discards if ConfirmNewFunction never ran.
func (c *Context) TryNewFunction() { c.current = &function{cc: 1} }

// ConfirmNewFunction ports lizard.py:491-493: the candidate is real (its
// declaration reached a body), so its complexity starts counting from the
// base case of 1, and it registers itself to become its own nesting-stack
// entry on the next '{' (see AddBareNesting).
func (c *Context) ConfirmNewFunction() {
	c.pending = c.current
	c.current.cc = 1
}

// RestartNewFunction ports lizard.py:495-497 (try + confirm in one step),
// used by readers whose grammar confirms a function the moment its name is
// seen rather than after a separate declaration/body split (kept for the
// GoLike family sharing this Context in a later PR; CLikeReader itself
// always goes through the two-step TryNewFunction/ConfirmNewFunction path).
func (c *Context) RestartNewFunction() {
	c.TryNewFunction()
	c.ConfirmNewFunction()
}

// PushNewFunction ports lizard.py:499-501 (kept for the GoLike family; plain
// CLikeReader never calls it, since C/C++/Java/C# don't lexically nest one
// function's declaration inside another's).
func (c *Context) PushNewFunction() {
	c.stacked = append(c.stacked, c.current)
	c.RestartNewFunction()
}

// InRealFunction ports the ONE reader of stacked_functions in Python
// outside PushNewFunction/EndOfFunction themselves: GoLikeStates._function_name's
// disambiguation between a method receiver's parens and an anonymous
// function literal's own parameter list (golike.py:41-45 --
// `len(self.context.stacked_functions) > 0 and
// self.context.stacked_functions[-1].name != '*global*'`). Since this
// package tracks no names, "the top of the stack is the global
// pseudo-function" is checked the same way EndOfFunction already
// distinguishes it: pointer identity against &c.global.
func (c *Context) InRealFunction() bool {
	return len(c.stacked) > 0 && c.stacked[len(c.stacked)-1] != &c.global
}

// AddCondition ports lizard.py:503-504. inc is usually 1 (condition_counter,
// clike.go's driver) but CppRValueRefStates passes a negative inc to
// subtract a false-positive "&&" back out (clike.py:77-86).
func (c *Context) AddCondition(inc int) { c.current.cc += inc }

// AddBareNesting ports NestingStack.add_bare_nesting (lizard.py:409-410).
// Every '{' calls this exactly once (CLikeNestingStackStates._state_global,
// clike.go), regardless of whether it opens a function body, a class body,
// or a bare block -- createNesting is what tells the two apart.
func (c *Context) AddBareNesting() {
	c.nestingStack = append(c.nestingStack, c.createNesting())
}

// createNesting ports NestingStack._create_nesting (lizard.py:419-424): a
// pending function (just confirmed, its body's '{' being processed right
// now) consumes the pending slot and becomes the nesting entry itself, so
// its own matching '}' is distinguishable from an ordinary block's.
func (c *Context) createNesting() nesting {
	fn := c.pending
	c.pending = nil
	if fn != nil {
		return nesting{fn: fn}
	}
	return nesting{}
}

// AddNamespace ports NestingStack.add_namespace (lizard.py:412-414). A
// class/struct/namespace/union body is pushed as its own entry so its
// closing brace is counted by PopNesting like any other nesting level; for
// CC purposes it behaves exactly like a bare nesting; the name lizard
// records here (for qualifying member names) is never read for CC and is
// dropped.
func (c *Context) AddNamespace() {
	c.pending = nil
	c.nestingStack = append(c.nestingStack, nesting{})
}

// PopNesting ports FileInfoBuilder.pop_nesting (lizard.py:468-476): pop one
// level, and if it was a function's own entry, close that function out and
// restore whichever function was active before it (a stacked function, or
// file scope).
func (c *Context) PopNesting() {
	c.pending = nil
	if len(c.nestingStack) == 0 {
		return
	}
	n := c.nestingStack[len(c.nestingStack)-1]
	c.nestingStack = c.nestingStack[:len(c.nestingStack)-1]
	if n.fn == nil {
		return
	}
	c.EndOfFunction()
	// BUG FIXED HERE (CHAOS-5156, found building the Java port; latent
	// for C/C++/C# too, just never exercised -- see below): Python's real
	// pop_nesting (lizard.py:468-476) does NOT stop at end_of_function's
	// own restore. It ALSO overrides current_function again by scanning
	// the REMAINING nesting stack for the innermost still-open function
	// (NestingStack.last_function, lizard.py:435-438) -- because a
	// function can close while an ENCLOSING function (not a stacked one;
	// CLikeReader/Java never populate that list) is still open on the
	// brace-nesting stack: an anonymous class's method defined inside
	// another method's body is exactly this shape, and this line is what
	// makes ctx.current go back to the OUTER method rather than file
	// scope once the inner one closes. Every C/C++ fixture measured so
	// far happened to avoid a function-within-a-function shape (C++
	// lambdas never create a nesting entry at all, so they never exposed
	// this), so nothing caught it until Java's anonymous-class fixture
	// did.
	if enclosing := c.lastFunctionInNestingStack(); enclosing != nil {
		c.current = enclosing
	}
}

// lastFunctionInNestingStack ports NestingStack.last_function
// (lizard.py:435-438): the innermost nesting entry that is a function, or
// nil if none remain open.
func (c *Context) lastFunctionInNestingStack() *function {
	for i := len(c.nestingStack) - 1; i >= 0; i-- {
		if c.nestingStack[i].fn != nil {
			return c.nestingStack[i].fn
		}
	}
	return nil
}

// EndOfFunction ports FileInfoBuilder.end_of_function in full (lizard.py:
// 515-523): record the finished function AND restore whichever function
// was active before it (a stacked function, or file scope) -- ONE method,
// not two. PopNesting calls this for the ordinary case (a function's own
// '}'); a later PR's GoLikeStates-derived readers, whose function bodies
// never go through the nesting-stack machinery this file's AddBareNesting/
// PopNesting provide, will call it directly instead.
//
// The append-only half is guarded by two things: the `c.current !=
// &c.global` pointer check (the safe, narrower form of Python's redundant
// `or not self.forgive_global` clause -- end_of_function is only ever
// invoked on a function that was actually confirmed and is therefore never
// the global pseudo-function) and now `!c.Forgive` (CHAOS-5156, codex round
// r1 on #2253 -- see the Forgive field doc for why forgive_global itself is
// not ported). Forgive resets unconditionally below, matching Python's
// `self.forgive = False` running regardless of which branch fired.
//
// The restore half is inert for CLikeReader today (stacked is never
// populated -- PushNewFunction has no CLikeReader caller), but belongs
// here rather than only in PopNesting: splitting them is what let a later
// PR's direct EndOfFunction call silently skip the restore entirely.
func (c *Context) EndOfFunction() {
	if c.current != &c.global {
		if c.Forgive {
			logDroppedFunction(c, "lizard forgive comment")
		} else {
			c.Complexities = append(c.Complexities, c.current.cc)
		}
	}
	c.Forgive = false
	if len(c.stacked) > 0 {
		c.current = c.stacked[len(c.stacked)-1]
		c.stacked = c.stacked[:len(c.stacked)-1]
	} else {
		c.current = &c.global
	}
}

// HandleCommentDirectives ports comment_counter's directive-scanning half
// (lizard.py:532-550) for one already-identified comment token (isComment
// reports which tokens qualify). It returns stopProcessing=true when the
// comment contains "GENERATED CODE" anywhere -- lizard's comment_counter
// generator RETURNS at that point, permanently ending the token stream, so
// every caller must stop its own loop (not just skip this one token) the
// instant this reports true.
//
// `#lizard forgives(metric1, metric2)` (named-metric forgiveness) is
// deliberately NOT recognised here: its only effect in real lizard is
// excusing specific named metrics from a THRESHOLD VIOLATION REPORT, a
// concept this package's AnalyzerFunc contract (raw per-function
// complexities only, see this file's package doc) never produces, so
// there is nothing for it to change here even if parsed.
func (c *Context) HandleCommentDirectives(commentTok string) (stopProcessing bool) {
	text := commentTok
	if strings.HasPrefix(text, "/*") || strings.HasPrefix(text, "//") {
		text = text[2:]
	}
	stripped := strings.TrimSpace(text)
	switch {
	case strings.HasPrefix(stripped, "#lizard forgive global"):
		// Recognised but deliberately inert -- see the Forgive field doc.
	case strings.HasPrefix(stripped, "#lizard forgives("):
		// Named-metric forgiveness -- deliberately unparsed, see doc above.
	case strings.HasPrefix(stripped, "#lizard forgive"):
		c.Forgive = true
	}
	if strings.Contains(text, "GENERATED CODE") {
		// Telemetry (CHAOS-5156 review checklist): every token from here
		// to end-of-file is discarded by the caller's loop, including any
		// function currently mid-declaration (TryNewFunction'd but never
		// reaching ConfirmNewFunction/EndOfFunction) -- log once, at the
		// single shared call site every reader routes through, rather
		// than at each caller's own break statement.
		logDroppedFunction(c, `"GENERATED CODE" comment stopped all further processing`)
		return true
	}
	return false
}
