package restcore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"syscall"
	"testing"
	"time"
)

// net/http's error text quotes the whole request URL, query included, and the transport's own text (CHAOS-7935). The text of
// an error this core returns names the operation and the class of the failure only. The request URL below holds a planted
// secret in its query, its path and its host; none of them may appear.

const plantedTransportMarker = "planted-transport-detail"

const plantedTarget = "https://planteduser:plantedsecret@internal-host-7935.example.test/orgs/42/repos?access_token=planted-transport-detail#planted-fragment"

var plantedLeaks = []string{plantedTransportMarker, "planteduser", "plantedsecret", "internal-host-7935", "orgs/42", "access_token", "planted-fragment", "10.1.2.3"}

type failingTransport struct {
	err   error
	calls *int
}

func (transport failingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	if transport.calls != nil {
		*transport.calls++
	}
	return nil, transport.err
}

func coreWith(err error, calls *int) Core {
	return Core{
		Provider:       "github",
		RetryAfter:     func(Response) time.Duration { return 0 },
		Sleep:          func(context.Context, time.Duration) error { return nil },
		MaxRetries:     3,
		InitialBackoff: time.Nanosecond,
		HTTP:           &http.Client{Transport: failingTransport{err: err, calls: calls}},
	}
}

func assertNoLeak(t *testing.T, message string) {
	t.Helper()
	for _, leak := range plantedLeaks {
		if strings.Contains(message, leak) {
			t.Fatalf("the error text carries %q: %s", leak, message)
		}
	}
}

func TestATransportErrorThatIsNotRetriedNamesTheClassNotTheURL(t *testing.T) {
	for name, row := range map[string]struct {
		cause error
		class string
	}{
		"a generic failure":  {errors.New("tls: handshake exploded talking to 10.1.2.3"), "protocol"},
		"a reset connection": {fmt.Errorf("read tcp 10.1.2.3:443: %w", syscall.ECONNRESET), "reset"},
		"a generic text":     {fmt.Errorf("GET %s: %w", plantedTarget, errors.New("unexpected end")), "protocol"},
		"an unexpected EOF":  {fmt.Errorf("GET %s: %w", plantedTarget, io.ErrUnexpectedEOF), "eof"},
	} {
		t.Run(name, func(t *testing.T) {
			calls := 0
			_, err := coreWith(row.cause, &calls).Get(context.Background(), plantedTarget, "GET /probe")
			var apiErr *Error
			if !errors.As(err, &apiErr) || apiErr.Class != "TransportError" {
				t.Fatalf("err = %v, want a TransportError", err)
			}
			assertNoLeak(t, apiErr.Message)
			if want := "request failed: " + row.class; !strings.HasSuffix(apiErr.Message, want) {
				t.Fatalf("message = %q, want it to end with %q", apiErr.Message, want)
			}
			if calls != 1 {
				t.Fatalf("a failure that is not retried made %d requests", calls)
			}
		})
	}
}

func TestARetriedTransportErrorNamesTheClassNotTheURLWhenTheRetriesRunOut(t *testing.T) {
	dial := &net.OpError{Op: "dial", Net: "tcp", Err: fmt.Errorf("connect to 10.1.2.3: %w", syscall.ECONNREFUSED)}
	calls := 0
	_, err := coreWith(dial, &calls).Get(context.Background(), plantedTarget, "GET /probe")
	var apiErr *Error
	if !errors.As(err, &apiErr) || apiErr.Class != "APIException" {
		t.Fatalf("err = %v, want an APIException", err)
	}
	assertNoLeak(t, apiErr.Message)
	if want := "github request failed on GET /probe: refused"; apiErr.Message != want {
		t.Fatalf("message = %q, want %q", apiErr.Message, want)
	}
	if calls != 3 {
		t.Fatalf("a dial failure was tried %d times, want the 3 retries", calls)
	}
}

func TestAnInvalidRequestURLNamesNoPartOfIt(t *testing.T) {
	_, err := coreWith(errors.New("unused"), nil).Get(context.Background(), "https://internal-host-7935.example.test/orgs/42/ bad\x7f?access_token="+plantedTransportMarker, "GET /probe")
	var apiErr *Error
	if !errors.As(err, &apiErr) || apiErr.Class != "APIException" {
		t.Fatalf("err = %v, want an APIException", err)
	}
	assertNoLeak(t, apiErr.Message)
	if want := "github request failed on GET /probe: invalid request URL"; apiErr.Message != want {
		t.Fatalf("message = %q, want %q", apiErr.Message, want)
	}
}

// An ordinary transport error keeps the operation: "Get request failed: <class>".
func TestATransportErrorKeepsTheOperationName(t *testing.T) {
	_, err := coreWith(errors.New("boom"), nil).Get(context.Background(), plantedTarget, "GET /probe")
	var apiErr *Error
	if !errors.As(err, &apiErr) || apiErr.Message != "Get request failed: protocol" {
		t.Fatalf("err = %v, want \"Get request failed: protocol\"", err)
	}
}

// The cause of a transport failure is not reachable from the error: Error has no Unwrap (the comment in the core says so).
func TestAnErrorOfTheCoreHasNoCauseToUnwrap(t *testing.T) {
	cause := fmt.Errorf("read tcp 10.1.2.3:443: %w", syscall.ECONNRESET)
	_, err := coreWith(cause, nil).Get(context.Background(), plantedTarget, "GET /probe")
	if errors.Unwrap(err) != nil || errors.Is(err, syscall.ECONNRESET) {
		t.Fatalf("the error reaches its cause: %v", err)
	}
}

// Every status path of the core, driven with a request URL that holds a marker in its userinfo, path, query and fragment (the
// response body is clean: a provider body is the provider's own text, named in RISK-NOTES): no error text holds any of them.
type statusTransport struct {
	status   int
	location string
}

func (transport statusTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	header := http.Header{}
	if transport.location != "" {
		header.Set("Location", transport.location)
	}
	return &http.Response{StatusCode: transport.status, Header: header, Body: io.NopCloser(strings.NewReader("provider says no")), Request: request}, nil
}

func TestNoErrorTextOfAStatusPathHoldsAMarkerOfTheRequestURL(t *testing.T) {
	for _, status := range []int{401, 403, 404, 409, 418, 422, 429, 500, 502, 503} {
		core := coreWith(nil, nil)
		core.HTTP = &http.Client{Transport: statusTransport{status: status}}
		core.IsRetryable = func(Response) bool { return false }
		_, err := core.Get(context.Background(), plantedTarget, "GET /probe")
		if err == nil {
			t.Fatalf("status %d: no error", status)
		}
		text := err.Error()
		if status == 404 {
			// the ONE named row: the text keeps scheme, host and path (byte parity with the frozen recorded Python answer of the
			// sync admin venue oracles) and takes out the userinfo, the credential-named query values and the fragment.
			for _, leak := range []string{"planteduser", "plantedsecret", plantedTransportMarker, "planted-fragment"} {
				if strings.Contains(text, leak) {
					t.Fatalf("the NotFound text carries %q: %s", leak, text)
				}
			}
			if !strings.Contains(text, "internal-host-7935.example.test/orgs/42/repos?access_token=[REDACTED]") {
				t.Fatalf("the NotFound text lost its host, path or query shape: %s", text)
			}
			continue
		}
		assertNoLeak(t, fmt.Sprintf("status %d: %s", status, text))
	}
}

func TestRedactRequestURLKeepsEveryOtherByte(t *testing.T) {
	for _, same := range []string{
		"http://127.0.0.1:34081/api/v4/groups/gitlab-examples%2Fno-such-group/projects?page=1&per_page=100",
		"https://api.github.com/orgs/acme/repos?per_page=100&type=all",
		"https://ghe.test/api/v3/search/repositories?q=org%3Aacme+widgets&page=2",
		"not a url at all",
		"https://host.test?x=a@b",
		"https://host.test/x?mail=a@b.test",
		"",
	} {
		if got := redactRequestURL(same); got != same {
			t.Fatalf("redactRequestURL(%q) = %q, want it unchanged", same, got)
		}
	}
	for raw, want := range map[string]string{
		"https://u:p@host.test/x?page=1":                     "https://[REDACTED]@host.test/x?page=1",
		"https://host.test/x?access_token=abc&page=1":        "https://host.test/x?access_token=[REDACTED]&page=1",
		"https://host.test/x?page=1&api_key=abc":             "https://host.test/x?page=1&api_key=[REDACTED]",
		"https://host.test/x?private%5Ftoken=abc":            "https://host.test/x?private%5Ftoken=[REDACTED]",
		"https://host.test/x?client_secret=abc&password=def": "https://host.test/x?client_secret=[REDACTED]&password=[REDACTED]",
		"https://host.test/x?page=1#frag":                    "https://host.test/x?page=1",
		"https://u@host.test":                                "https://[REDACTED]@host.test",
		"https://u:p@ss@host.test/x":                         "https://[REDACTED]@host.test/x",
		"https://host.test/x?%74oken=abc":                    "https://host.test/x?%74oken=[REDACTED]",
	} {
		if got := redactRequestURL(raw); got != want {
			t.Fatalf("redactRequestURL(%q) = %q, want %q", raw, got, want)
		}
	}
}

// the 3xx rows of the derived status test: a Location that holds a marker in userinfo, path, query and fragment (the redirect-text
// change keeps only scheme and host); the core follows nothing.
func TestNoErrorTextOfARedirectHoldsAMarkerOfTheLocation(t *testing.T) {
	for _, status := range []int{301, 302, 303, 307, 308} {
		core := coreWith(nil, nil)
		core.HTTP = &http.Client{Transport: statusTransport{status: status, location: plantedTarget}}
		core.IsRetryable = func(Response) bool { return false }
		_, err := core.Get(context.Background(), "https://api.example.test/start", "GET /probe")
		if err == nil {
			t.Fatalf("status %d: no error", status)
		}
		for _, leak := range []string{plantedTransportMarker, "planteduser", "plantedsecret", "orgs/42", "access_token", "planted-fragment"} {
			if strings.Contains(err.Error(), leak) {
				t.Fatalf("status %d: the redirect text carries %q: %s", status, leak, err)
			}
		}
		if !strings.Contains(err.Error(), "https://internal-host-7935.example.test") {
			t.Fatalf("status %d: the host of the Location is missing: %s", status, err)
		}
	}
}
