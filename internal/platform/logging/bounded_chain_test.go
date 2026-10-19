package logging

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"
)

// CHAOS-7933: a hostile error must not stall or crash a log call: a self-unwrapping error, a cycle, a very deep chain, an Unwrap
// / Is / SQLState / Timeout method that panics (typed nil receivers).

type selfUnwrap struct{}

func (e *selfUnwrap) Error() string { return "self" }
func (e *selfUnwrap) Unwrap() error { return e }

type pairA struct{ next *pairB }
type pairB struct{ next *pairA }

func (e *pairA) Error() string { return "a" }
func (e *pairA) Unwrap() error { return e.next }
func (e *pairB) Error() string { return "b" }
func (e *pairB) Unwrap() error { return e.next }

type nilReceiver struct{ cause error }

func (e *nilReceiver) Error() string { return "nil receiver" }
func (e *nilReceiver) Unwrap() error { return e.cause }

type panickingIs struct{}

func (panickingIs) Error() string    { return "panicking is" }
func (panickingIs) Is(error) bool    { panic("is") }
func (panickingIs) SQLState() string { panic("state") }
func (panickingIs) Timeout() bool    { panic("timeout") }
func (panickingIs) Unwrap() []error  { panic("unwrap") }

type panickingAs struct{ cause error }

func (panickingAs) Error() string   { return "panicking as" }
func (panickingAs) As(any) bool     { panic("as") }
func (e panickingAs) Unwrap() error { return e.cause }

type panickingTimeout struct{}

func (panickingTimeout) Error() string { return "panicking timeout" }
func (panickingTimeout) Timeout() bool { panic("timeout") }

func withinASecond(t *testing.T, name string, run func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		run()
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s: stalled on a hostile error", name)
	}
}

func hostileErrors() map[string]error {
	a := &pairA{}
	b := &pairB{next: a}
	a.next = b
	var typedNil *nilReceiver
	deep := error(io.ErrUnexpectedEOF)
	for index := 0; index < 5000; index++ {
		deep = fmt.Errorf("layer %d: %w", index, deep)
	}
	return map[string]error{
		"self unwrap":           &selfUnwrap{},
		"two-cycle":             a,
		"typed nil":             typedNil,
		"panicking":             panickingIs{},
		"joined cycle":          errors.Join(&selfUnwrap{}, a),
		"5000 deep":             deep,
		"wrapped self":          fmt.Errorf("x: %w", &selfUnwrap{}),
		"panicking As":          panickingAs{cause: io.ErrUnexpectedEOF},
		"wrapped panicking As":  fmt.Errorf("x: %w", panickingAs{}),
		"panicking Timeout":     panickingTimeout{},
		"panicking Is, wrapped": fmt.Errorf("x: %w", panickingIs{}),
	}
}

func TestEveryClassifierSurvivesAHostileError(t *testing.T) {
	for name, err := range hostileErrors() {
		withinASecond(t, name, func() {
			attrs := ErrorAttrs(err)
			if len(attrs) < 2 {
				t.Errorf("%s: attrs = %v", name, attrs)
			}
			_ = ErrorAttr(err)
			_ = ErrorClass(err)
			_ = ErrorType(err)
			_ = TransportClass(err)
			_ = TransportFailure(err).Error()
			_ = DecodeFailure(err).Error()
			if timeout, ok := TransportFailure(err).(interface{ Timeout() bool }); ok {
				_ = timeout.Timeout()
			}
		})
	}
}

func TestAHostileErrorStillGetsItsFixedAlphabet(t *testing.T) {
	text := attrsText(&selfUnwrap{})
	if !strings.Contains(text, "error_class=other") || !strings.Contains(text, "error_type=*logging.selfUnwrap") {
		t.Fatalf("self unwrap:\n%s", text)
	}
	var typedNil *nilReceiver
	if got := ErrorType(typedNil); got != "*logging.nilReceiver" {
		t.Fatalf("typed nil type = %q", got)
	}
	// it implements SQLState (which panics): class by identity, and no code is read from it
	if got := ErrorClass(panickingIs{}); got != ErrorClassPostgres {
		t.Fatalf("panicking is class = %q", got)
	}
	if text := attrsText(panickingIs{}); strings.Contains(text, "error_code") {
		t.Fatalf("a code was read from a panicking SQLState:\n%s", text)
	}
	// the chain walk still finds what it should, deep in a legitimate chain
	deep := fmt.Errorf("a: %w", fmt.Errorf("b: %w", fmt.Errorf("c: %w", context.DeadlineExceeded)))
	if got := ErrorClass(deep); got != ErrorClassDeadline {
		t.Fatalf("deep deadline class = %q", got)
	}
	if got := TransportClass(fmt.Errorf("x: %w", io.ErrUnexpectedEOF)); got != "eof" {
		t.Fatalf("eof class = %q", got)
	}
	if got := ErrorClass(errors.Join(errors.New("x"), context.Canceled)); got != ErrorClassCanceled {
		t.Fatalf("joined canceled class = %q", got)
	}
}

// An As method that panics must not stop the walk, and the chain beyond it is still classified.
func TestAPanickingAsDoesNotHideTheRestOfTheChain(t *testing.T) {
	err := fmt.Errorf("x: %w", panickingAs{cause: io.ErrUnexpectedEOF})
	if got := TransportClass(err); got != "eof" {
		t.Fatalf("class behind a panicking As = %q, want eof", got)
	}
	if got := TransportClass(panickingTimeout{}); got == "timeout" {
		t.Fatalf("a panicking Timeout was trusted: %q", got)
	}
}

func TestErrorAsIsBoundedAndSurvivesHostileErrors(t *testing.T) {
	type target struct{ *selfUnwrap }
	for name, err := range hostileErrors() {
		withinASecond(t, name, func() {
			var pointer *selfUnwrap
			_ = ErrorAs(err, &pointer)
			var timeout interface{ Timeout() bool }
			_ = ErrorAs(err, &timeout)
		})
	}
	var found *selfUnwrap
	if !ErrorAs(fmt.Errorf("x: %w", &selfUnwrap{}), &found) || found == nil {
		t.Fatal("ErrorAs did not find a *selfUnwrap behind a wrapper")
	}
	var absent *nilReceiver
	if ErrorAs(errors.New("plain"), &absent) {
		t.Fatal("ErrorAs found a type that is not in the chain")
	}
	if ErrorAs(nil, &found) || ErrorAs(errors.New("x"), nil) {
		t.Fatal("ErrorAs accepted nil")
	}
	_ = target{}
}

func wrapDeep(err error, depth int) error {
	for range depth {
		err = fmt.Errorf("w: %w", err)
	}
	return err
}

// The walk reaches as deep as it has nodes (maxChainNodes): a target 40 wrappers down is found; one past the node bound is not.
func TestErrorAsReachesTheNodeBoundAndNoFurther(t *testing.T) {
	var found *selfUnwrap
	if !ErrorAs(wrapDeep(&selfUnwrap{}, 40), &found) || found == nil {
		t.Fatal("ErrorAs missed a target 40 wrappers deep")
	}
	if !ErrorAs(wrapDeep(&selfUnwrap{}, maxChainNodes-1), &found) {
		t.Fatalf("ErrorAs missed a target %d wrappers deep, the last node inside the bound", maxChainNodes-1)
	}
	if ErrorAs(wrapDeep(&selfUnwrap{}, maxChainNodes+6), &found) {
		t.Fatal("ErrorAs walked past the node bound")
	}
}

// ErrorType keeps its own 32-step reach: a chain deeper than that names the wrapper found at 32 unwraps, as before the walk bound grew.
func TestErrorTypeKeepsItsThirtyTwoStepReach(t *testing.T) {
	if got := ErrorType(wrapDeep(errors.New("leaf"), 40)); got != "*fmt.wrapError" {
		t.Fatalf("ErrorType of a 40-deep chain = %q, want *fmt.wrapError", got)
	}
	if got := ErrorType(wrapDeep(errors.New("leaf"), 20)); got != "*errors.errorString" {
		t.Fatalf("ErrorType of a 20-deep chain = %q, want *errors.errorString", got)
	}
}
