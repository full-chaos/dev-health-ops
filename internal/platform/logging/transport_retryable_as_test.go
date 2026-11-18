package logging

import (
	"errors"
	"net"
	"net/url"
	"testing"
)

type asAdapter struct {
	dial    *net.OpError
	timeout interface{ Timeout() bool }
}

func (a asAdapter) Error() string { return "adapter" }
func (a asAdapter) As(target any) bool {
	switch typed := target.(type) {
	case **net.OpError:
		if a.dial != nil {
			*typed = a.dial
			return true
		}
	case *interface{ Timeout() bool }:
		if a.timeout != nil {
			*typed = a.timeout
			return true
		}
	}
	return false
}

func TestRetryableTransportReadsAsAdapters(t *testing.T) {
	dial := asAdapter{dial: &net.OpError{Op: "dial", Err: errors.New("refused")}}
	timeout := asAdapter{timeout: stubTimeout{true}}
	for name, err := range map[string]error{
		"dial adapter":          dial,
		"timeout adapter":       timeout,
		"non-timeout / adapter": errors.Join(stubTimeout{false}, timeout),
		"adapter / dial":        errors.Join(asAdapter{}, dial),
	} {
		if !RetryableTransport(err) {
			t.Errorf("%s: RetryableTransport = false, want true", name)
		}
	}
	if RetryableTransport(asAdapter{}) {
		t.Error("empty adapter: RetryableTransport = true, want false")
	}
}

func TestRetryableTransportCyclicURLErrorDoesNotCrash(t *testing.T) {
	cyclic := &url.Error{Op: "Get", URL: "x"}
	cyclic.Err = cyclic
	other := &url.Error{Op: "read", Err: errors.New("ordinary")}
	dial := &net.OpError{Op: "dial", Err: errors.New("refused")}
	if !RetryableTransport(errors.Join(other, cyclic, dial)) {
		t.Error("cyclic link before a dial: RetryableTransport = false, want true")
	}
	if RetryableTransport(errors.Join(other, cyclic)) {
		t.Error("cyclic link without dial or timeout: RetryableTransport = true, want false")
	}
}
