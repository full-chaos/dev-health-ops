package logging

import (
	"fmt"
	"net"
	"net/url"
	"testing"
)

// A typed-nil *url.Error in the chain is found by errors.As with a nil target: TransportFailure must not dereference it
// (CHAOS-7933 r1).
func TestTransportFailureSurvivesATypedNilURLError(t *testing.T) {
	var nilURLError *url.Error
	for name, err := range map[string]error{"bare": error(nilURLError), "wrapped": fmt.Errorf("call: %w", error(nilURLError))} {
		got := TransportFailure(err)
		if got == nil || got.Error() != "exchange request failed: protocol" {
			t.Fatalf("%s: TransportFailure = %v, want the class-only text with the default operation", name, got)
		}
	}
}

// CHAOS-8127: the shared checks answer for a typed-nil transport error instead of panicking.
func TestSharedTransportChecksSurviveTypedNilErrors(t *testing.T) {
	var nilURLError *url.Error
	var nilOpError *net.OpError
	for name, err := range map[string]error{"url bare": error(nilURLError), "url wrapped": fmt.Errorf("x: %w", error(nilURLError)), "op bare": error(nilOpError), "op wrapped": fmt.Errorf("x: %w", error(nilOpError))} {
		if URLError(err) != nil {
			t.Errorf("%s: URLError returned a typed nil", name)
		}
		if RetryableTransport(err) {
			t.Errorf("%s: RetryableTransport answered true for a typed-nil error", name)
		}
	}
	real := &url.Error{Op: "Get", URL: "https://x.example.test/", Err: &net.OpError{Op: "dial"}}
	if got := URLError(fmt.Errorf("w: %w", real)); got != real {
		t.Errorf("URLError = %v, want the real one", got)
	}
	if !RetryableTransport(real) {
		t.Error("a dial failure is not retryable")
	}
}
