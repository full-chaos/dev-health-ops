package routeswitch

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func handlerNamed(name string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(name))
	})
}

// TestOperationOnlyReachableWithItsOwnSwitchEnabled is the CHAOS-3033
// "cited constructor is not proof of capability" lesson, applied to route
// reachability (plan §6): registering an operation's handler in the Mux
// must not be sufficient for it to respond. Only that operation's own
// switch, enabled, makes it reachable -- enabling a DIFFERENT operation's
// switch must not leak reachability to this one.
func TestOperationOnlyReachableWithItsOwnSwitchEnabled(t *testing.T) {
	operations := []string{"featureFlags", "reviewEdges", "hotspots"}

	for _, enabledOp := range operations {
		enabledOp := enabledOp
		t.Run("only_"+enabledOp+"_enabled", func(t *testing.T) {
			sw := NewDynamicSwitch()
			mux := NewMux(sw)
			for _, op := range operations {
				mux.Register(op, handlerNamed(op))
			}

			// Enable ONLY this table row's operation -- table-driven,
			// clause-by-clause, not a wholesale "enable everything" check
			// (root AGENTS.md's mutation-testing discipline applied here:
			// a test that enabled every switch at once could not
			// distinguish "the gate works" from "the gate is a no-op").
			sw.Set(enabledOp, true)

			for _, op := range operations {
				rec := httptest.NewRecorder()
				req := httptest.NewRequest(http.MethodGet, "/query", nil)
				mux.Dispatch(op, rec, req)

				if op == enabledOp {
					if rec.Code != http.StatusOK {
						t.Errorf("operation %q: switch enabled but got status %d, want 200", op, rec.Code)
					}
					if rec.Body.String() != op {
						t.Errorf("operation %q: got body %q, want %q", op, rec.Body.String(), op)
					}
				} else {
					if rec.Code != http.StatusNotFound {
						t.Errorf("operation %q: switch NOT enabled (only %q is) but got status %d, want 404 -- reachability leaked across operations", op, enabledOp, rec.Code)
					}
				}
			}
		})
	}
}

// TestRegisteredHandlerAloneDoesNotGrantReachability is the direct
// "constructor is not proof of capability" assertion: a handler exists in
// the Mux, but with every switch left at its default (disabled), every
// operation must be unreachable.
func TestRegisteredHandlerAloneDoesNotGrantReachability(t *testing.T) {
	sw := NewDynamicSwitch()
	mux := NewMux(sw)
	mux.Register("featureFlags", handlerNamed("featureFlags"))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/query", nil)
	mux.Dispatch("featureFlags", rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("registered-but-not-switched-on operation responded %d, want 404 (a wired route is not a reachable route)", rec.Code)
	}
}

// TestUnregisteredOperationIsUnreachableEvenIfSwitchEnabled: flipping a
// switch for an operation that has no handler must not panic or 500 --
// it is indistinguishable from "not reachable", the safe default.
func TestUnregisteredOperationIsUnreachableEvenIfSwitchEnabled(t *testing.T) {
	sw := NewDynamicSwitch()
	sw.Set("neverRegistered", true)
	mux := NewMux(sw)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/query", nil)
	mux.Dispatch("neverRegistered", rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("unregistered operation with an enabled switch responded %d, want 404", rec.Code)
	}
}

// TestDisablingAnOperationRevokesReachability proves the ROLLBACK direction
// -- plan §5: "rollback is a registry change, not an image rollback." The
// switch is the whole mechanism; disabling it after having been enabled
// must take effect immediately, with no separate deploy.
func TestDisablingAnOperationRevokesReachability(t *testing.T) {
	sw := NewDynamicSwitch()
	mux := NewMux(sw)
	mux.Register("featureFlags", handlerNamed("featureFlags"))

	sw.Set("featureFlags", true)
	rec := httptest.NewRecorder()
	mux.Dispatch("featureFlags", rec, httptest.NewRequest(http.MethodGet, "/query", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 while enabled, got %d", rec.Code)
	}

	sw.Set("featureFlags", false)
	rec = httptest.NewRecorder()
	mux.Dispatch("featureFlags", rec, httptest.NewRequest(http.MethodGet, "/query", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 immediately after disabling (rollback = registry change), got %d", rec.Code)
	}
}

func TestNewMuxPanicsOnNilSwitch(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected NewMux(nil) to panic")
		}
	}()
	NewMux(nil)
}

func TestStaticSwitchTreatsAbsentOperationAsDisabled(t *testing.T) {
	sw := StaticSwitch{"featureFlags": true}
	if !sw.Enabled("featureFlags") {
		t.Fatal("expected featureFlags enabled")
	}
	if sw.Enabled("reviewEdges") {
		t.Fatal("expected an operation absent from StaticSwitch to be disabled by default")
	}
}
