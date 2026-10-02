package logging

import (
	"errors"
	"net"
	"net/url"
)

// URLError is the *url.Error in the error's chain; nil when the chain holds none or holds a typed-nil one (the nil pointer): errors.As reports a
// match with a nil target for a typed-nil pointer, and reading a field of it (or walking its Unwrap) panics. The walk is the
// bounded one: a self-unwrapping, cyclic or panicking error cannot stall or crash the caller (CHAOS-8127).
func URLError(err error) *url.Error {
	var found *url.Error
	if chainAs(boundedChain(err), &found) {
		return found // a typed-nil match is the nil pointer: callers test for nil
	}
	return nil
}

// RetryableTransport reports whether a transport error is a timeout of any phase or a failure to dial (a refused connection, an
// unreachable host, a name that does not resolve), by the bounded walk with the Timeout method under recover: a typed-nil
// transport error answers false instead of panicking (CHAOS-8127).
func RetryableTransport(err error) bool {
	chain := boundedChain(err)
	var timeout interface{ Timeout() bool }
	if chainAs(chain, &timeout) && safeTimeout(timeout) {
		return true
	}
	var operation *net.OpError
	return chainAs(chain, &operation) && operation != nil && operation.Op == "dial"
}

// URLErrorLeafText is the text of the cause of the *url.Error in the error's chain when that cause is a leaf (it unwraps to
// nothing), and false when the chain holds no url error, the url error has no cause, the cause has a cause of its own, or ANY of
// these reads panics (a typed-nil cause answers Unwrap and Error by dereferencing nil): the whole read is under recover, so it
// holds at every hop of the chain (CHAOS-8127).
func URLErrorLeafText(err error) (text string, ok bool) {
	defer func() {
		if recover() != nil {
			text, ok = "", false
		}
	}()
	urlErr := URLError(err)
	if urlErr == nil || urlErr.Err == nil || errors.Unwrap(urlErr.Err) != nil {
		return "", false
	}
	return urlErr.Err.Error(), true
}
