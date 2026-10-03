package restcore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

// The core follows no redirect: a 3xx is an error whose text names where the server wanted to send the request. That text
// reaches logs and API answers, and a Location can hold userinfo, a query with a token, a path with an id, a fragment: it
// must carry the scheme and the host only (CHAOS-7927).

const plantedLocationSecret = "sk-planted-location-7927"

func redirectError(t *testing.T, location string, setLocation bool) (class, message string) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if setLocation {
			w.Header().Set("Location", location)
		}
		w.WriteHeader(http.StatusFound)
	}))
	t.Cleanup(server.Close)
	_, err := Core{Provider: "github", RetryAfter: func(Response) time.Duration { return 0 }}.Get(context.Background(), server.URL, "GET /probe")
	var apiErr *Error
	if !errors.As(err, &apiErr) {
		t.Fatalf("err = %v, want an *Error", err)
	}
	return apiErr.Class, apiErr.Message
}

func TestRedirectErrorCarriesTheSchemeAndHostOnly(t *testing.T) {
	for _, row := range []struct {
		name, location, want string
	}{
		{"userinfo, port, path, query and fragment", "https://admin:" + plantedLocationSecret + "@files.example.test:8443/orgs/42/repos?access_token=" + plantedLocationSecret + "#frag", "https://files.example.test:8443"},
		{"a plain absolute URL", "https://api.example.test/next", "https://api.example.test"},
		{"a scheme-relative URL", "//cdn.example.test/x?token=" + plantedLocationSecret, "//cdn.example.test"},
		{"an IPv6 host", "http://[2001:db8::1]:9/x?k=" + plantedLocationSecret, "http://[2001:db8::1]:9"},
	} {
		t.Run(row.name, func(t *testing.T) {
			class, message := redirectError(t, row.location, true)
			if class != "APIException" {
				t.Fatalf("the error class is %q, want APIException (a redirect is an API exception, not a transport error)", class)
			}
			for _, leak := range []string{plantedLocationSecret, "admin", "orgs/42", "access_token", "frag", "token=", "k="} {
				if strings.Contains(message, leak) {
					t.Fatalf("the error text carries %q: %s", leak, message)
				}
			}
			if !strings.Contains(message, "HTTP 302 -> "+row.want+";") {
				t.Fatalf("the error text does not name %q as the target: %s", row.want, message)
			}
		})
	}
}

func TestRedirectErrorNamesNoTargetForARelativeUnparsableOrMissingLocation(t *testing.T) {
	for _, row := range []struct {
		name, location string
		set            bool
		want           string
	}{
		{"a relative path with a query", "/orgs/42/repos?token=" + plantedLocationSecret, true, "<relative Location>"},
		{"a bare name", "next-page?token=" + plantedLocationSecret, true, "<relative Location>"},
		{"unparsable (net/http refuses it and quotes it)", "https://exa%zzmple.test/x?token=" + plantedLocationSecret, true, "<unparsable Location>"},
		{"a scheme with no host", "mailto:" + plantedLocationSecret, true, "<relative Location>"},
		{"no Location header", "", false, "<no Location header>"},
	} {
		t.Run(row.name, func(t *testing.T) {
			class, message := redirectError(t, row.location, row.set)
			if class != "APIException" {
				t.Fatalf("the error class is %q, want APIException: net/http's own refusal of an unparsable Location must not change the class", class)
			}
			if strings.Contains(message, plantedLocationSecret) || strings.Contains(message, "orgs/42") {
				t.Fatalf("the error text carries part of the Location: %s", message)
			}
			if !strings.Contains(message, row.want) {
				t.Fatalf("the error text = %s, want the target %q", message, row.want)
			}
		})
	}
}

func TestRedirectTargetIsTheSchemeAndHostOfALocation(t *testing.T) {
	for location, want := range map[string]string{
		"https://u:p@h.example.test/a?b#c": "https://h.example.test",
		"http://h.example.test":            "http://h.example.test",
		"//h.example.test/p":               "//h.example.test",
		"/only/a/path":                     "<relative Location>",
		"":                                 "<relative Location>",
		"%zz":                              "<unparsable Location>",
		"  https://h.example.test/x \t":    "https://h.example.test",
	} {
		if got := RedirectTarget(location); got != want {
			t.Errorf("RedirectTarget(%q) = %q, want %q", location, got, want)
		}
	}
}

// A transport error that is not a redirect keeps its class and its (class-only) text: only net/http's refusal of a Location is reworded.
func TestAnOrdinaryTransportErrorKeepsItsClassAndText(t *testing.T) {
	core := Core{
		Provider:   "github",
		RetryAfter: func(Response) time.Duration { return 0 },
		HTTP:       &http.Client{Transport: failingTransport{err: errors.New("tls: handshake exploded")}},
	}
	_, err := core.Get(context.Background(), "https://api.example.test/x", "GET /probe")
	var apiErr *Error
	if !errors.As(err, &apiErr) || apiErr.Class != "TransportError" || apiErr.Message != "Get request failed: protocol" {
		t.Fatalf("err = %v, want a TransportError with the class-only text", err)
	}
	if strings.Contains(apiErr.Message, "unexpected redirect") {
		t.Fatalf("an ordinary transport error was reworded as a redirect: %s", apiErr.Message)
	}
}

// CHAOS-7927 r1: an ordinary transport error that merely mentions net/http's phrase, or wraps a cause behind it, keeps its class.
func TestATransportErrorMentioningTheLocationPhraseKeepsItsClass(t *testing.T) {
	for name, cause := range map[string]error{
		"mentioned in the middle": errors.New("the proxy said: failed to parse Location header for the planted detail"),
		"wrapped cause behind it": fmt.Errorf("failed to parse Location header %q: %w", "x", errors.New("inner cause")),
	} {
		core := Core{Provider: "github", RetryAfter: func(Response) time.Duration { return 0 }, HTTP: &http.Client{Transport: failingTransport{err: cause}}}
		_, err := core.Get(context.Background(), "https://api.example.test/x", "GET /probe")
		var apiErr *Error
		if !errors.As(err, &apiErr) || apiErr.Class != "TransportError" || strings.Contains(apiErr.Message, "unexpected redirect") {
			t.Fatalf("%s: err = %v, want a TransportError that is not reworded as a redirect", name, err)
		}
	}
}

// CHAOS-7927 r1: a supplied client with the DEFAULT redirect policy must not follow a Location (the core never does): the second
// server sees no request and the error is the host-only 3xx text.
func TestASuppliedClientWithTheDefaultPolicyDoesNotFollowARedirect(t *testing.T) {
	var followed int
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { followed++ }))
	defer second.Close()
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, second.URL+"/private/record-42?planted=detail", http.StatusFound)
	}))
	defer first.Close()
	supplied := &http.Client{}
	core := Core{Provider: "gitlab", RetryAfter: func(Response) time.Duration { return 0 }, HTTP: supplied}
	_, err := core.Get(context.Background(), first.URL+"/x", "GET /probe")
	if supplied.CheckRedirect != nil {
		t.Fatal("the core changed the redirect policy of the caller's client")
	}
	var apiErr *Error
	if !errors.As(err, &apiErr) || apiErr.Class != "APIException" || !strings.Contains(apiErr.Message, "unexpected redirect") {
		t.Fatalf("err = %v, want the 3xx APIException", err)
	}
	if followed != 0 {
		t.Fatalf("the redirect was followed %d time(s)", followed)
	}
	if strings.Contains(apiErr.Message, "record-42") || strings.Contains(apiErr.Message, "planted") {
		t.Fatalf("the text carries the Location's path or query: %s", apiErr.Message)
	}
}

// CHAOS-7927 r2: an ordinary transport error whose text STARTS with net/http's refusal phrase (exactly, no cause behind it, or
// joined) is not a redirect: no 3xx answer came back, so it keeps its transport class and is never reworded.
func TestATransportErrorStartingWithTheLocationPhraseIsNotARedirect(t *testing.T) {
	for name, cause := range map[string]error{
		"exact prefix":    errors.New("failed to parse Location header \"x\": planted detail"),
		"joined":          errors.Join(errors.New("failed to parse Location header \"x\""), errors.New("second")),
		"prefix then url": errors.New("failed to parse Location header "),
	} {
		core := Core{Provider: "github", RetryAfter: func(Response) time.Duration { return 0 }, HTTP: &http.Client{Transport: failingTransport{err: cause}}}
		_, err := core.Get(context.Background(), "https://api.example.test/x", "GET /probe")
		var apiErr *Error
		if !errors.As(err, &apiErr) || apiErr.Class != "TransportError" || strings.Contains(apiErr.Message, "unexpected redirect") {
			t.Fatalf("%s: err = %v, want a TransportError that is not reworded as a redirect", name, err)
		}
	}
}

type erroringBody struct{ err error }

func (b erroringBody) Read([]byte) (int, error) { return 0, b.err }
func (b erroringBody) Close() error             { return nil }

type redirectThenBrokenBody struct{ err error }

func (r redirectThenBrokenBody) RoundTrip(request *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusFound, Status: "302 Found", Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
		Header:  http.Header{"Location": []string{"https://next.example.test/x"}},
		Body:    erroringBody{err: r.err},
		Request: request,
	}, nil
}

// CHAOS-7927 vet: the witness is set (a 3xx with a VALID Location came back) and then reading its body fails with an error that
// starts with net/http's phrase: that is still an ordinary transport error. Only the shape (a *url.Error holding the phrase)
// together with the witness is a refused Location, so this state is where the shape half decides.
func TestAWitnessedRedirectWhoseBodyReadFailsStaysATransportError(t *testing.T) {
	for name, cause := range map[string]error{
		"plain read failure":           errors.New("connection reset by peer"),
		"phrase-shaped error":          errors.New("failed to parse Location header \"x\": planted detail"),
		"url error without the phrase": &url.Error{Op: "Get", URL: "https://api.example.test/x", Err: errors.New("read: connection reset by peer")},
	} {
		core := Core{Provider: "github", RetryAfter: func(Response) time.Duration { return 0 }, HTTP: &http.Client{Transport: redirectThenBrokenBody{err: cause}}}
		_, err := core.Get(context.Background(), "https://api.example.test/x", "GET /probe")
		var apiErr *Error
		if !errors.As(err, &apiErr) || strings.Contains(apiErr.Message, "unexpected redirect") {
			t.Fatalf("%s: err = %v, want an error that is not reworded as a redirect", name, err)
		}
	}
}

// CHAOS-8127: errors.As reports a match with a nil target for a typed-nil *url.Error in the chain; the refusal check must not
// read its fields. A witnessed 3xx whose body read fails with such an error is an ordinary transport error.
func TestATypedNilURLErrorAfterAWitnessedRedirectDoesNotPanic(t *testing.T) {
	var nilURLError *url.Error
	for name, cause := range map[string]error{
		"bare":    error(nilURLError),
		"wrapped": fmt.Errorf("read: %w", error(nilURLError)),
	} {
		core := Core{Provider: "github", RetryAfter: func(Response) time.Duration { return 0 }, HTTP: &http.Client{Transport: redirectThenBrokenBody{err: cause}}}
		var apiErr *Error
		func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					t.Fatalf("%s: Core.Get panicked: %v", name, recovered)
				}
			}()
			_, err := core.Get(context.Background(), "https://api.example.test/x", "GET /probe")
			if !errors.As(err, &apiErr) || strings.Contains(apiErr.Message, "unexpected redirect") {
				t.Fatalf("%s: err = %v, want an error that is not reworded as a redirect", name, err)
			}
		}()
	}
	if isLocationParseFailure(error(nilURLError)) {
		t.Fatal("a typed-nil url error was taken for a refused Location")
	}
	var nilOpError *net.OpError
	if retryableTransport(error(nilOpError)) || retryableTransport(error(nilURLError)) {
		t.Fatal("a typed-nil transport error was taken for a retryable one")
	}
}

type selfUnwrappingError struct{}

func (e *selfUnwrappingError) Error() string { return "loop" }
func (e *selfUnwrappingError) Unwrap() error { return e }

// CHAOS-8127: the refusal check and the retry check walk the error through the bounded chain: an error whose Unwrap returns
// itself must not stall the request path.
func TestTheRequestPathChecksAreBoundedOnASelfUnwrappingError(t *testing.T) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = isLocationParseFailure(&selfUnwrappingError{})
		_ = retryableTransport(&selfUnwrappingError{})
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a check did not return on a self-unwrapping error")
	}
}

// CHAOS-8127 (every hop): a witnessed 3xx whose body read fails with a hostile error at chain depth 0, 1 and 2 -- a typed-nil
// *url.Error, a real *url.Error whose INNER cause is a typed-nil (of its own type and of another), and an error whose Unwrap
// returns itself -- must neither panic nor stall Core.Get, and the error is never reworded as a redirect.
func TestEveryHopOfAHostileChainAfterAWitnessedRedirectIsSurvived(t *testing.T) {
	var nilURLError *url.Error
	var nilOpError *net.OpError
	var nilLeaf *panickingLeafError
	shapes := map[string]error{
		"typed-nil url error":                                             error(nilURLError),
		"url error with a typed-nil url cause":                            &url.Error{Op: "Get", URL: "https://x.example.test", Err: error(nilURLError)},
		"url error with a typed-nil op cause":                             &url.Error{Op: "Get", URL: "https://x.example.test", Err: error(nilOpError)},
		"self-unwrapping error":                                           &selfUnwrappingError{},
		"url error with a typed-nil leaf cause (no Unwrap, Error panics)": &url.Error{Op: "Get", URL: "https://x.example.test", Err: error(nilLeaf)},
	}
	for name, shape := range shapes {
		for depth := 0; depth <= 2; depth++ {
			cause := shape
			for hop := 0; hop < depth; hop++ {
				cause = fmt.Errorf("hop %d: %w", hop, cause)
			}
			label := fmt.Sprintf("%s at depth %d", name, depth)
			done := make(chan error, 1)
			go func() {
				defer func() {
					if recovered := recover(); recovered != nil {
						done <- fmt.Errorf("Core.Get panicked: %v", recovered)
					}
				}()
				core := Core{Provider: "github", RetryAfter: func(Response) time.Duration { return 0 }, HTTP: &http.Client{Transport: redirectThenBrokenBody{err: cause}}}
				_, err := core.Get(context.Background(), "https://api.example.test/x", "GET /probe")
				var apiErr *Error
				if !errors.As(err, &apiErr) || strings.Contains(apiErr.Message, "unexpected redirect") {
					done <- fmt.Errorf("err = %v, want an error that is not reworded as a redirect", err)
					return
				}
				done <- nil
			}()
			select {
			case failure := <-done:
				if failure != nil {
					t.Errorf("%s: %v", label, failure)
				}
			case <-time.After(5 * time.Second):
				t.Errorf("%s: Core.Get did not return", label)
			}
		}
	}
}

type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

type timeoutThenOK struct{ calls int }

func (t *timeoutThenOK) RoundTrip(request *http.Request) (*http.Response, error) {
	t.calls++
	if t.calls == 1 {
		return nil, timeoutError{}
	}
	return &http.Response{StatusCode: 200, Status: "200 OK", Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("{}")), Request: request}, nil
}

// CHAOS-8127 vet (U6): a timeout of any phase is retried; the clause that says so has its own row (a dial failure has another).
func TestATimeoutIsRetriedByTheCore(t *testing.T) {
	transport := &timeoutThenOK{}
	core := Core{Provider: "github", RetryAfter: func(Response) time.Duration { return 0 }, Sleep: func(context.Context, time.Duration) error { return nil }, HTTP: &http.Client{Transport: transport}}
	response, err := core.Get(context.Background(), "https://api.example.test/x", "GET /probe")
	if err != nil || response.Status != 200 {
		t.Fatalf("Get = %v, %v, want the second attempt's 200", response.Status, err)
	}
	if transport.calls != 2 {
		t.Fatalf("the transport was called %d times, want 2 (one timeout, one retry)", transport.calls)
	}
}

// CHAOS-8127 r2: after a witnessed 3xx, a body-read error whose *url.Error cause is ANY multi-error shape (errors.Join, a two-%w
// wrapper, a custom Unwrap() []error) is not net/http's refusal of a Location, whatever its first message says: it is an ordinary
// transport error and is never reworded as a redirect.
func TestAMultiErrorCauseAfterAWitnessedRedirectIsNotARefusedLocation(t *testing.T) {
	phrase := errors.New("failed to parse Location header \"x\": planted detail")
	for name, cause := range map[string]error{
		"errors.Join, phrase first": &url.Error{Op: "Get", URL: "https://api.example.test/x", Err: errors.Join(phrase, errors.New("second"))},
		"two-%w wrapper":            &url.Error{Op: "Get", URL: "https://api.example.test/x", Err: fmt.Errorf("failed to parse Location header %w %w", errors.New("a"), errors.New("b"))},
		"join under a single wrap":  fmt.Errorf("read: %w", &url.Error{Op: "Get", URL: "https://api.example.test/x", Err: errors.Join(phrase)}),
	} {
		core := Core{Provider: "github", RetryAfter: func(Response) time.Duration { return 0 }, HTTP: &http.Client{Transport: redirectThenBrokenBody{err: cause}}}
		_, err := core.Get(context.Background(), "https://api.example.test/x", "GET /probe")
		var apiErr *Error
		if !errors.As(err, &apiErr) || apiErr.Class != "TransportError" || strings.Contains(apiErr.Message, "unexpected redirect") {
			t.Fatalf("%s: err = %v, want a TransportError that is not reworded as a redirect", name, err)
		}
	}
}

// panickingLeafError is a leaf by structure (no Unwrap) whose Error dereferences its receiver: a typed-nil pointer of it panics in
// Error, after the leaf check and inside the refusal predicate, where only its recover stops it (CHAOS-8127 vet).
type panickingLeafError struct{ msg string }

func (n *panickingLeafError) Error() string { return n.msg }
