package logging

import (
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
