package logging

import (
	"errors"
	"net"
	"net/url"
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
