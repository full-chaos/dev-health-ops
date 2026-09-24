// Package routeswitch is the per-operation reachability gate for query-api
// (CHAOS-4366 Wave 0, deliverable 6).
//
// Plan §6: "deploy an empty Go query-api and prove a route becomes
// reachable when, and only when, its individual switch is enabled (the
// CHAOS-3033 'cited constructor is not proof of capability' lesson, applied
// here as a table-driven reachability test)." A registered operation handler
// existing in the Mux is exactly the "cited constructor" — it is NOT proof
// the operation is reachable. Only Switch.Enabled(name) being true, checked
// on every request before dispatch, makes it reachable.
//
// Switch is deliberately an interface, not a concrete Postgres-backed type:
// plan §6 also says query-api's first reachable route "should already be
// built on the extracted [dev-health-go] readers, not on hand-rolled
// ClickHouse [or Postgres] queries that would need porting again later."
// The dev-health-go module (CHAOS-4377) has not landed yet, so Wave 0 ships
// StaticSwitch (an in-memory map) as the only implementation; a
// go_api_registry-backed Switch (reading the ROUTING_STATE table
// src/dev_health_ops/models/go_api_registry.py defines) is a follow-up that
// implements this same interface, not a redesign of it.
package routeswitch

import (
	"net/http"
	"sync"
)

// Switch decides whether a named operation is currently reachable.
type Switch interface {
	// Enabled reports whether operation may be dispatched right now.
	// Must be safe for concurrent use.
	Enabled(operation string) bool
}

// StaticSwitch is a fixed, in-memory Switch. An operation absent from the
// map is disabled — the safe default (plan §5: "unregistered documents ...
// stay on Python"; the Go-side equivalent is "stay unreachable").
type StaticSwitch map[string]bool

// Enabled implements Switch.
func (s StaticSwitch) Enabled(operation string) bool {
	return s[operation]
}

// DynamicSwitch is a concurrency-safe, mutable Switch for tests and for a
// future registry-backed implementation that refreshes on a poll interval.
type DynamicSwitch struct {
	mu    sync.RWMutex
	state map[string]bool
}

// NewDynamicSwitch returns a DynamicSwitch with every named operation
// initially disabled.
func NewDynamicSwitch() *DynamicSwitch {
	return &DynamicSwitch{state: make(map[string]bool)}
}

// Enabled implements Switch.
func (d *DynamicSwitch) Enabled(operation string) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.state[operation]
}

// Set enables or disables one operation. Safe for concurrent use with
// Enabled; does not affect any other operation's state.
func (d *DynamicSwitch) Set(operation string, enabled bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.state == nil {
		d.state = make(map[string]bool)
	}
	d.state[operation] = enabled
}

// Mux dispatches by operation name to a registered handler, gated by a
// Switch. An operation with a registered handler but a disabled switch is
// NOT reachable -- it returns 404, identical to an operation with no
// handler at all. That equivalence is the point: from the outside, a wired
// route and an unwired route are indistinguishable until the switch flips.
type Mux struct {
	sw       Switch
	handlers map[string]http.Handler
}

// NewMux builds a Mux gated by sw. sw must not be nil.
func NewMux(sw Switch) *Mux {
	if sw == nil {
		panic("routeswitch: NewMux requires a non-nil Switch")
	}
	return &Mux{sw: sw, handlers: make(map[string]http.Handler)}
}

// Register wires operation to handler. Registration alone does not make the
// operation reachable -- see the package doc comment.
func (m *Mux) Register(operation string, handler http.Handler) {
	m.handlers[operation] = handler
}

// Dispatch serves the request via the handler registered for operation, but
// only when m.sw.Enabled(operation) is true. Otherwise it writes 404,
// whether or not a handler is registered.
func (m *Mux) Dispatch(operation string, w http.ResponseWriter, r *http.Request) {
	if !m.sw.Enabled(operation) {
		http.NotFound(w, r)
		return
	}
	handler, ok := m.handlers[operation]
	if !ok {
		http.NotFound(w, r)
		return
	}
	handler.ServeHTTP(w, r)
}
