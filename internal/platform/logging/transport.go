package logging

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"net"
	"syscall"
)

// TransportFailure returns an error for a failed exchange with a remote
// service whose text is the request operation and a failure class only:
// "<Op> request failed: <class>". net/http's errors quote the request URL and,
// for a malformed response, the peer's status line or header values; none of
// that text reaches the result. errors.Is and errors.As still reach the
// original error, and Timeout reports as the original does. The classes are
// canceled, deadline, timeout, dns, refused, reset, tls, eof and protocol.
func TransportFailure(err error) error {
	if err == nil {
		return nil
	}
	op := "exchange"
	if urlErr := URLError(err); urlErr != nil && urlErr.Op != "" {
		op = urlErr.Op
	}
	return &classifiedError{text: op + " request failed: " + TransportClass(err), cause: err}
}

// TransportClass names the class of a transport failure from the error's
// type and identity, never from its text.
func TransportClass(err error) string {
	chain := boundedChain(err)
	var dnsErr *net.DNSError
	var recordErr tls.RecordHeaderError
	var alertErr tls.AlertError
	var verifyErr *tls.CertificateVerificationError
	var unknownAuthority x509.UnknownAuthorityError
	var hostnameErr x509.HostnameError
	var certificateErr x509.CertificateInvalidError
	var timeout interface{ Timeout() bool }
	switch {
	case chainIs(chain, context.Canceled):
		return "canceled"
	case chainIs(chain, context.DeadlineExceeded):
		return "deadline"
	case chainAs(chain, &dnsErr):
		return "dns"
	case chainAs(chain, &timeout) && safeTimeout(timeout):
		return "timeout"
	case chainIs(chain, syscall.ECONNREFUSED):
		return "refused"
	case chainIs(chain, syscall.ECONNRESET), chainIs(chain, syscall.EPIPE), chainIs(chain, net.ErrClosed):
		return "reset"
	case chainAs(chain, &recordErr), chainAs(chain, &alertErr), chainAs(chain, &verifyErr),
		chainAs(chain, &unknownAuthority), chainAs(chain, &hostnameErr), chainAs(chain, &certificateErr):
		return "tls"
	case chainIs(chain, io.EOF), chainIs(chain, io.ErrUnexpectedEOF):
		return "eof"
	}
	return "protocol"
}

func safeTimeout(timeout interface{ Timeout() bool }) (result bool) {
	defer func() {
		if recover() != nil {
			result = false
		}
	}()
	return timeout.Timeout()
}

// DecodeFailure returns an error for a response that could not be decoded,
// whose text is "decode response: <class>" only: encoding/json's errors quote
// the offending value. errors.Is and errors.As still reach the original
// error. The classes are syntax, type, eof and invalid.
func DecodeFailure(err error) error {
	if err == nil {
		return nil
	}
	return &classifiedError{text: "decode response: " + decodeClass(err), cause: err}
}

func decodeClass(err error) string {
	chain := boundedChain(err)
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError
	switch {
	case chainAs(chain, &syntaxErr):
		return "syntax"
	case chainAs(chain, &typeErr):
		return "type"
	case chainIs(chain, io.EOF), chainIs(chain, io.ErrUnexpectedEOF):
		return "eof"
	}
	return "invalid"
}

// classifiedError carries our own text and keeps the original error for
// errors.Is and errors.As.
type classifiedError struct {
	text  string
	cause error
}

func (e *classifiedError) Error() string { return e.text }

func (e *classifiedError) Unwrap() error { return e.cause }

// Timeout reports whether the original failure was a timeout.
func (e *classifiedError) Timeout() bool {
	var timeout interface{ Timeout() bool }
	return chainAs(boundedChain(e.cause), &timeout) && safeTimeout(timeout)
}
