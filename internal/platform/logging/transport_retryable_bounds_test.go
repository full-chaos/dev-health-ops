package logging

import (
	"errors"
	"net"
	"net/url"
	"strconv"
	"testing"
)

func TestRetryableTransportDeepNestedURLErrors(t *testing.T) {
	for _, depth := range []int{33, 65, 97, 1000} {
		var err error = stubTimeout{true}
		for range depth {
			err = &url.Error{Op: "Get", URL: "x", Err: err}
		}
		if !RetryableTransport(err) {
			t.Errorf("depth %d: RetryableTransport = false, want true", depth)
		}
		if got := TransportClass(err); got != "timeout" {
			t.Errorf("depth %d: TransportClass = %q, want timeout", depth, got)
		}
		if !TransportFailure(err).(interface{ Timeout() bool }).Timeout() {
			t.Errorf("depth %d: TransportFailure.Timeout = false, want true", depth)
		}
		if !RetryableTransport(asAdapter{timeout: err.(*url.Error)}) {
			t.Errorf("depth %d: as-adapter timeout did not retry", depth)
		}
	}
}

func TestRetryableTransportWideJoin(t *testing.T) {
	for _, before := range []int{62, 63, 64, 200, 900} {
		errs := make([]error, 0, before+1)
		for range before {
			errs = append(errs, errors.New("ordinary"))
		}
		withTimeout := append(append([]error{}, errs...), stubTimeout{true})
		if !RetryableTransport(errors.Join(withTimeout...)) {
			t.Errorf("%d ordinary errors before a timeout: RetryableTransport = false, want true", before)
		}
		withDial := append(append([]error{}, errs...), &net.OpError{Op: "dial", Err: errors.New("refused")})
		if !RetryableTransport(errors.Join(withDial...)) {
			t.Errorf("%d ordinary errors before a dial: RetryableTransport = false, want true", before)
		}
	}
	if RetryableTransport(errors.Join(errors.New("a"), errors.New("b"))) {
		t.Error("join of ordinary errors: RetryableTransport = true, want false")
	}
}

func nestedTimeoutChain(depth int, leaf error) error {
	err := leaf
	for range depth {
		err = &url.Error{Op: "Get", URL: "x", Err: err}
	}
	return err
}

func TestSafeTimeoutAllocatesNothing(t *testing.T) {
	for _, depth := range []int{8, 1000} {
		err := nestedTimeoutChain(depth, stubTimeout{true}).(*url.Error)
		if allocs := testing.AllocsPerRun(20, func() { _ = safeTimeout(err) }); allocs != 0 {
			t.Errorf("depth %d: safeTimeout %v allocs, want 0", depth, allocs)
		}
	}
}

func BenchmarkRetryableTransportNegativeDeep(b *testing.B) {
	for _, depth := range []int{8, 1000, 65536} {
		err := nestedTimeoutChain(depth, errors.New("ordinary"))
		b.Run("depth="+strconv.Itoa(depth), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				if RetryableTransport(err) {
					b.Fatal("ordinary leaf must not retry")
				}
			}
		})
	}
}

func TestSafeTimeoutCycleWithTail(t *testing.T) {
	a := &url.Error{Op: "a"}
	b := &url.Error{Op: "b", Err: a}
	c := &url.Error{Op: "c", Err: b}
	a.Err = c
	var tail error = c
	for range 5 {
		tail = &url.Error{Op: "tail", Err: tail}
	}
	if safeTimeout(tail.(*url.Error)) {
		t.Error("cycle behind a tail: safeTimeout = true, want false")
	}
}
