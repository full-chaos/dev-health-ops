package lizardcc

// This file ports the sub_state half of code_reader.py's CodeStateMachine
// (lines 11-56) -- the piece plain CLikeReader (clike.go) never needed,
// because none of its three state machines ever nest a WHOLE separate
// machine instance inside themselves. Java's method/class-body readers,
// and every GoLikeStates-derived reader (Kotlin, Scala, Swift here; Go and
// Rust in a later PR), all rely on it: a `{` can hand token processing to a
// freshly cloned instance of the SAME machine, recursively, so that a
// nested function or block is tracked with its own state rather than one
// opaque brace counter (contrast CLikeStates._state_imp, which is exactly
// that opaque counter and is why C/C++ never needed this file).
//
// subMachine is anything that can become the CURRENT state of an outer
// machine (Python: `self._state = some_other_machine_instance`). feed
// reports whether the sub-machine just finished (its own
// statemachine_return fired) -- code_reader.py:48-56's
// `if self._state(token): ...`, where `self._state` may be either a plain
// per-token function OR another whole machine object, is the reason Go
// needs an explicit interface here where clike.go needed none.
type subMachine interface {
	feed(tok string) (exited bool)
}

// core is the shared engine every state machine in this file embeds --
// code_reader.py's `self._state`/`self.saved_state`/`self.callback`/
// `self.to_exit` fields, plus __call__'s dispatch and sub_state/next's
// transition logic, factored out once so java.go/kotlin.go/scala.go/
// swift.go each carry only their OWN states, not this machinery too.
type core struct {
	state      state
	sub        subMachine
	savedState state
	callback   func()
	toExit     bool
	// beforeReturn is invoked every time this machine exits, whether via
	// statemachineReturn (code_reader.py:39-41's unconditional call) or via
	// the top-level driver's end-of-file sweep -- ports
	// statemachine_before_return, a no-op by default (code_reader.py:61-62)
	// that Scala overrides (scala.go).
	beforeReturn func()
}

// feed ports CodeStateMachine.__call__'s dispatch (code_reader.py:48-56).
// It returns whether THIS machine should now be considered finished, so a
// machine embedding core can implement subMachine by simply delegating to
// this method.
func (c *core) feed(tok string) bool {
	if c.sub != nil {
		if c.sub.feed(tok) {
			c.sub = nil
			c.state = c.savedState
			c.savedState = nil
			if c.callback != nil {
				cb := c.callback
				c.callback = nil
				cb()
			}
		}
		return c.toExit
	}
	c.state(tok)
	return c.toExit
}

// subState ports CodeStateMachine.sub_state with no immediate token
// (code_reader.py:43-46, called as `self.sub_state(m, cb)`): hand token
// processing to m, remembering the state to resume once m finishes.
func (c *core) subState(m subMachine, cb func()) {
	c.savedState = c.state
	c.callback = cb
	c.sub = m
	c.state = nil
}

// subStateTok is subState immediately followed by feeding tok into the new
// sub-machine -- ports `self.sub_state(m, cb, token)` (code_reader.py:43-46
// combined with next's optional re-feed, code_reader.py:29-32). A sub-
// machine can finish on its very FIRST token (e.g. an empty `{}` block), so
// this returns whether IT (the sub-machine, now possibly already unwound)
// reports exited too.
func (c *core) subStateTok(m subMachine, cb func(), tok string) bool {
	c.subState(m, cb)
	return c.feed(tok)
}

// statemachineReturn ports CodeStateMachine.statemachine_return
// (code_reader.py:39-41): mark this machine finished and run its own
// before-return cleanup, if it has any.
func (c *core) statemachineReturn() {
	c.toExit = true
	if c.beforeReturn != nil {
		c.beforeReturn()
	}
}
