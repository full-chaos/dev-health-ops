package logging

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/url"
	"strings"
	"syscall"
	"testing"
)

type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout canary-timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

// TestTransportFailureIsOperationAndClassOnly: whatever the transport error
// quotes (URL, query, userinfo, the peer's status line or header values),
// the result is "<Op> request failed: <class>", and errors.Is / errors.As /
// Timeout still reach the original.
func TestTransportFailureIsOperationAndClassOnly(t *testing.T) {
	t.Parallel()
	wrap := func(cause error) error {
		return &url.Error{Op: "Post", URL: "https://user:canary-pass@api.example.test/v1?token=canary-query", Err: cause}
	}
	cases := []struct {
		name  string
		err   error
		class string
	}{
		{"canceled", wrap(context.Canceled), "canceled"},
		{"deadline", wrap(context.DeadlineExceeded), "deadline"},
		{"dns", wrap(&net.OpError{Op: "dial", Err: &net.DNSError{Name: "canary-host", Err: "no such host"}}), "dns"},
		{"timeout", wrap(&net.OpError{Op: "dial", Err: timeoutError{}}), "timeout"},
		{"refused", wrap(&net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}), "refused"},
		{"reset", wrap(&net.OpError{Op: "read", Err: syscall.ECONNRESET}), "reset"},
		{"tls", wrap(tls.RecordHeaderError{Msg: "canary-tls"}), "tls"},
		{"eof", wrap(io.ErrUnexpectedEOF), "eof"},
		{"protocol quoting a header", wrap(errors.New(`unsupported transfer encoding: "canary-header"`)), "protocol"},
		{"protocol quoting a status line", wrap(errors.New(`malformed HTTP status code "canary-status"`)), "protocol"},
		{"no url.Error", errors.New("read: canary-plain"), "protocol"},
	}
	for _, testCase := range cases {
		failure := TransportFailure(testCase.err)
		want := "Post request failed: " + testCase.class
		if testCase.name == "no url.Error" {
			want = "exchange request failed: protocol"
		}
		if failure.Error() != want || strings.Contains(failure.Error(), "canary") {
			t.Errorf("%s: %q, want %q", testCase.name, failure.Error(), want)
		}
		if !errors.Is(failure, testCase.err) {
			t.Errorf("%s: errors.Is to the original lost", testCase.name)
		}
		if TransportClass(testCase.err) != testCase.class {
			t.Errorf("%s: class %q", testCase.name, TransportClass(testCase.err))
		}
	}
	if !errors.Is(TransportFailure(wrap(context.DeadlineExceeded)), context.DeadlineExceeded) {
		t.Fatal("errors.Is to the context error lost")
	}
	var opErr *net.OpError
	if !errors.As(TransportFailure(wrap(&net.OpError{Op: "dial", Err: syscall.ECONNREFUSED})), &opErr) {
		t.Fatal("errors.As to the net.OpError lost")
	}
	if timeout, ok := TransportFailure(wrap(timeoutError{})).(interface{ Timeout() bool }); !ok || !timeout.Timeout() {
		t.Fatal("Timeout lost")
	}
	if TransportFailure(nil) != nil || DecodeFailure(nil) != nil {
		t.Fatal("nil must stay nil")
	}
}

// TestDecodeFailureIsClassOnly: encoding/json errors quote the offending
// value; the result names the class only and keeps the original.
func TestDecodeFailureIsClassOnly(t *testing.T) {
	t.Parallel()
	var target struct {
		Count int `json:"count"`
	}
	typeErr := json.Unmarshal([]byte(`{"count":98765432101234567890}`), &target)
	syntaxErr := json.Unmarshal([]byte(`{"count":canary}`), &target)
	for err, want := range map[error]string{
		typeErr:                  "decode response: type",
		syntaxErr:                "decode response: syntax",
		io.ErrUnexpectedEOF:      "decode response: eof",
		errors.New("canary-bad"): "decode response: invalid",
	} {
		failure := DecodeFailure(err)
		if failure.Error() != want || strings.Contains(failure.Error(), "canary") || strings.Contains(failure.Error(), "98765") {
			t.Errorf("%v -> %q, want %q", err, failure.Error(), want)
		}
		if !errors.Is(failure, err) {
			t.Errorf("%v: errors.Is lost", err)
		}
	}
}
