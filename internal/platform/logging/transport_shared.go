package logging

import (
	"errors"
	"net"
	"net/url"
	"strings"
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

// IsLocationParseRefusal reports whether the error is net/http's refusal of a redirect whose Location does not parse: a
// *url.Error whose cause is a leaf (it unwraps to nothing) with the refusal's own words. It answers a yes or a no and never hands
// the text out (the text holds the whole Location). The whole read is under recover, so a typed-nil cause at any hop of the chain
// (it answers Unwrap and Error by dereferencing nil) is a "no" (CHAOS-8127).
func IsLocationParseRefusal(err error) (refusal bool) {
	defer func() {
		if recover() != nil {
			refusal = false
		}
	}()
	urlErr := URLError(err)
	if urlErr == nil || urlErr.Err == nil || errors.Unwrap(urlErr.Err) != nil {
		return false
	}
	return strings.HasPrefix(urlErr.Err.Error(), "failed to parse Location header ")
}
