package logging

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"syscall"
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

// CHAOS-8127: IsLocationParseRefusal answers yes only for net/http's own refusal and never panics on a typed-nil cause at any hop.
func TestIsLocationParseRefusal(t *testing.T) {
	phrase := errors.New("failed to parse Location header \"x\": planted detail")
	var nilURL *url.Error
	var nilOp *net.OpError
	rows := map[string]struct {
		err  error
		want bool
	}{
		"the refusal":                                {&url.Error{Op: "Get", URL: "https://x.example.test", Err: phrase}, true},
		"the refusal, wrapped":                       {fmt.Errorf("w: %w", &url.Error{Op: "Get", URL: "https://x.example.test", Err: phrase}), true},
		"an ordinary url error":                      {&url.Error{Op: "Get", URL: "https://x.example.test", Err: errors.New("dial tcp: refused")}, false},
		"the phrase with a cause":                    {&url.Error{Op: "Get", URL: "https://x.example.test", Err: fmt.Errorf("failed to parse Location header \"x\": %w", errors.New("inner"))}, false},
		"the phrase, no url error":                   {phrase, false},
		"a leaf cause that only mentions the phrase": {&url.Error{Op: "Get", URL: "https://x.example.test", Err: errors.New("x: failed to parse Location header y")}, false},
		"typed-nil url error":                        {error(nilURL), false},
		"typed-nil url cause":                        {&url.Error{Err: error(nilURL)}, false},
		"typed-nil op cause":                         {&url.Error{Err: error(nilOp)}, false},
		"typed-nil url cause, depth two":             {fmt.Errorf("a: %w", fmt.Errorf("b: %w", &url.Error{Err: error(nilURL)})), false},
		"no error":                                   {nil, false},
		// CHAOS-8127 r2: a cause that is any multi-error shape is not a leaf, whatever its first message says.
		"cause is errors.Join, phrase first":         {&url.Error{Err: errors.Join(phrase, errors.New("second"))}, false},
		"cause is errors.Join of one":                {&url.Error{Err: errors.Join(phrase)}, false},
		"cause is a two-%w wrapper":                  {&url.Error{Err: fmt.Errorf("failed to parse Location header %w %w", errors.New("a"), errors.New("b"))}, false},
		"cause is a custom Unwrap() []error":         {&url.Error{Err: multiCause{msg: "failed to parse Location header x", causes: []error{errors.New("c")}}}, false},
		"cause is a custom multi with a typed nil":   {&url.Error{Err: multiCause{msg: "failed to parse Location header x", causes: []error{error(nilOp)}}}, false},
		"cause is a custom multi, panicking Unwrap":  {&url.Error{Err: multiCause{msg: "failed to parse Location header x", boom: true}}, false},
		"cause is a join under a single wrap":        {&url.Error{Err: fmt.Errorf("failed to parse Location header %w", errors.Join(errors.New("a"), errors.New("b")))}, false},
		"cause is a join with a typed-nil url error": {&url.Error{Err: errors.Join(error(nilURL), phrase)}, false},
		"an empty multi cause is a leaf":             {&url.Error{Err: multiCause{msg: "failed to parse Location header x"}}, true},
		"the refusal found inside a join":            {errors.Join(&url.Error{Op: "Get", URL: "https://x.example.test", Err: phrase}, errors.New("other")), true},
		"the refusal found inside a two-%w wrapper":  {fmt.Errorf("%w; %w", errors.New("other"), &url.Error{Op: "Get", URL: "https://x.example.test", Err: phrase}), true},
		"a typed-nil url error then the refusal":     {errors.Join(error(nilURL), &url.Error{Op: "Get", URL: "https://x.example.test", Err: phrase}), true},
	}
	for name, row := range rows {
		if got := IsLocationParseRefusal(row.err); got != row.want {
			t.Errorf("%s: IsLocationParseRefusal = %v, want %v", name, got, row.want)
		}
	}
}

// CHAOS-8127 vet: a *net.OpError that is neither a dial nor a timeout (a read reset) is NOT retried (httpx: only TimeoutException
// and ConnectError are); a dial is.
func TestRetryableTransportRetriesOnlyDialAndTimeout(t *testing.T) {
	rows := map[string]struct {
		err  error
		want bool
	}{
		"dial":                  {&net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}, true},
		"dial, wrapped":         {fmt.Errorf("w: %w", &net.OpError{Op: "dial", Err: errors.New("x")}), true},
		"read reset":            {&net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}, false},
		"write broken pipe":     {&net.OpError{Op: "write", Net: "tcp", Err: syscall.EPIPE}, false},
		"read reset in url err": {&url.Error{Op: "Get", URL: "https://x.example.test", Err: &net.OpError{Op: "read", Err: syscall.ECONNRESET}}, false},
		"plain error":           {errors.New("boom"), false},
		"no error":              {nil, false},
	}
	for name, row := range rows {
		if got := RetryableTransport(row.err); got != row.want {
			t.Errorf("%s: RetryableTransport = %v, want %v", name, got, row.want)
		}
	}
}

// multiCause is a hostile multi-error: it answers Unwrap() []error (nothing, a list, or a panic).
type multiCause struct {
	msg    string
	causes []error
	boom   bool
}

func (m multiCause) Error() string { return m.msg }
func (m multiCause) Unwrap() []error {
	if m.boom {
		panic("hostile Unwrap")
	}
	return m.causes
}
