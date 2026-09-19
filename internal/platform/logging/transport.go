package logging

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/url"
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
	var urlErr *url.Error
	if errors.As(err, &urlErr) && urlErr.Op != "" {
		op = urlErr.Op
	}
	return &classifiedError{text: op + " request failed: " + TransportClass(err), cause: err}
}

// TransportClass names the class of a transport failure from the error's
// type and identity, never from its text.
func TransportClass(err error) string {
	var dnsErr *net.DNSError
	var recordErr tls.RecordHeaderError
	var alertErr tls.AlertError
	var verifyErr *tls.CertificateVerificationError
	var unknownAuthority x509.UnknownAuthorityError
	var hostnameErr x509.HostnameError
	var certificateErr x509.CertificateInvalidError
	var timeout interface{ Timeout() bool }
	switch {
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	case errors.As(err, &dnsErr):
		return "dns"
	case errors.As(err, &timeout) && timeout.Timeout():
		return "timeout"
	case errors.Is(err, syscall.ECONNREFUSED):
		return "refused"
	case errors.Is(err, syscall.ECONNRESET), errors.Is(err, syscall.EPIPE), errors.Is(err, net.ErrClosed):
		return "reset"
	case errors.As(err, &recordErr), errors.As(err, &alertErr), errors.As(err, &verifyErr),
		errors.As(err, &unknownAuthority), errors.As(err, &hostnameErr), errors.As(err, &certificateErr):
		return "tls"
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return "eof"
	}
	return "protocol"
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
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError
	switch {
	case errors.As(err, &syntaxErr):
		return "syntax"
	case errors.As(err, &typeErr):
		return "type"
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
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
	return errors.As(e.cause, &timeout) && timeout.Timeout()
}
