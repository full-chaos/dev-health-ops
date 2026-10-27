package venueoracle

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// dummyToken is a credential-shaped value built at run time, never a literal.
func dummyToken(t *testing.T) string {
	t.Helper()
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}

// closedBase is a loopback base URL nothing listens on.
func closedBase(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	base := "http://" + listener.Addr().String()
	_ = listener.Close()
	return base
}

type failureRecorder struct{ text string }

func (r *failureRecorder) Helper() {}
func (r *failureRecorder) Fatal(args ...any) {
	r.text += strings.TrimSpace(fmt.Sprint(args...))
	panic(stopRequest{})
}
func (r *failureRecorder) Fatalf(f string, args ...any) {
	r.text += fmt.Sprintf(f, args...)
	panic(stopRequest{})
}

type stopRequest struct{}

func fatalText(t *testing.T, run func(RowsReporter)) string {
	t.Helper()
	r := &failureRecorder{}
	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				if _, ok := recovered.(stopRequest); !ok {
					panic(recovered)
				}
			}
		}()
		run(r)
	}()
	return r.text
}

func TestDoReportsATransportFailureWithoutTheQueryValuesOrTheUserinfo(t *testing.T) {
	token, password := dummyToken(t), dummyToken(t)
	base := strings.Replace(closedBase(t), "http://", "http://user:"+password+"@", 1)
	got := fatalText(t, func(r RowsReporter) {
		Do(r, base, Request{Method: "GET", Path: "/api/v1/auth/verify?token=" + token + "&email=someone@example.com"})
	})
	for _, secret := range []string{token, password, "someone@example.com"} {
		if strings.Contains(got, secret) {
			t.Fatalf("the failure text holds %q: %s", secret, got)
		}
	}
	if !strings.Contains(got, "/api/v1/auth/verify") || !strings.Contains(got, "refused") {
		t.Fatalf("the failure text lost the path or the cause: %s", got)
	}
}

func TestDoReportsARequestThatCannotBeBuiltWithoutTheQueryValues(t *testing.T) {
	token := dummyToken(t)
	got := fatalText(t, func(r RowsReporter) {
		Do(r, "http://127.0.0.1:1", Request{Method: "GET", Path: "/verify\x7f?token=" + token})
	})
	if got == "" || strings.Contains(got, token) {
		t.Fatalf("failure text %q", got)
	}
}

func TestADifferenceOnANon2xxAnswerHoldsNoTokenFromItsLocationBodyOrHeaders(t *testing.T) {
	token := dummyToken(t)
	redirect := "http://app.example.test/auth/verify?token=" + token + "&next=%2Fhome"
	python := Response{Status: 302, Headers: map[string]string{"location": redirect, "content-type": "text/plain"}, Body: "moved to " + redirect}
	goResponse := Response{Status: 400, Headers: map[string]string{"location": redirect}, Body: `{"detail":"bad link ` + redirect + `"}`}
	got := DescribeDifference("verify: a link", python, goResponse, []string{"content-type", "location"})
	if strings.Contains(got, token) {
		t.Fatalf("the difference text holds the token: %s", got)
	}
	if !strings.Contains(got, "verify: a link") || !strings.Contains(got, "302") || !strings.Contains(got, "400") || !strings.Contains(got, "/auth/verify") {
		t.Fatalf("the difference text lost its parts: %s", got)
	}
}

func TestADiffReportsANon2xxMismatchWithoutTheToken(t *testing.T) {
	token := dummyToken(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "http://app.example.test/x?token="+token)
		w.WriteHeader(http.StatusFound)
		_, _ = w.Write([]byte("see http://app.example.test/x?token=" + token))
	}))
	defer server.Close()
	answer := Do(t, server.URL, Request{Method: "GET", Path: "/a?token=" + token})
	got := DescribeDifference("redirect", Response{Status: 200, Body: "ok"}, answer, []string{"location"})
	if strings.Contains(got, token) {
		t.Fatalf("the difference text holds the token: %s", got)
	}
}

func TestRedactTextRemovesQueryValuesAndUserinfoOfEveryURL(t *testing.T) {
	token := dummyToken(t)
	text := "a https://u:p@h.example/p?token=" + token + "&b=2#f and http://h.example/q?x=" + token + " end, ?token=" + token
	got := RedactText(text)
	for _, secret := range []string{token, "u:p@"} {
		if strings.Contains(got, secret) {
			t.Fatalf("%q survives: %s", secret, got)
		}
	}
	if !strings.Contains(got, "h.example/p") || !strings.Contains(got, "h.example/q") || !strings.Contains(got, "end") {
		t.Fatalf("the text lost more than the secrets: %s", got)
	}
	if plain := "no url here, 100% sure"; RedactText(plain) != plain {
		t.Fatalf("plain text changed: %s", RedactText(plain))
	}
}

// A URL fragment can carry a credential too (an implicit-flow redirect puts access_token there): it is removed from every
// URL in a text, on a Location header and in a body, and plain text with a '#' after a space is left alone.
func TestRedactTextRemovesTheFragmentOfEveryURL(t *testing.T) {
	token := dummyToken(t)
	for name, text := range map[string]string{
		"absolute URL":         "Location: https://app.example.test/cb#access_token=" + token + "&token_type=bearer",
		"URL with a query":     "see http://h.example/p?next=%2F#id_token=" + token + " end",
		"relative Location":    "location: /auth/done#" + token,
		"fragment with a path": "https://h.example/a/b#/route?token=" + token,
	} {
		got := RedactText(text)
		if strings.Contains(got, token) {
			t.Errorf("%s: the fragment survives: %s", name, got)
		}
	}
	if got := RedactText("issue #123 and ## heading"); got != "issue #123 and ## heading" {
		t.Errorf("plain '#' text changed: %q", got)
	}
	if got := RedactText("https://h.example/p#frag"); !strings.HasPrefix(got, "https://h.example/p#") {
		t.Errorf("the URL lost its path: %q", got)
	}
}

type failingBody struct{ message string }

func (b failingBody) Read([]byte) (int, error) { return 0, errors.New(b.message) }
func (b failingBody) Close() error             { return nil }

type bodyFailureTransport struct{}

// RoundTrip answers 200 with a body whose read fails with an error that names the request URL, as a transport's read error can.
func (bodyFailureTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: 200, Header: http.Header{}, Request: request,
		Body: failingBody{message: "read " + request.URL.String() + ": connection reset"}}, nil
}

// A body that fails in the middle of the read goes through the same redaction as the request failure: the URL it names holds a
// query value, userinfo and a fragment, none of which reaches the failure text.
func TestDoReportsABodyReadFailureWithoutTheQueryValueTheUserinfoOrTheFragment(t *testing.T) {
	token, password, fragment := dummyToken(t), dummyToken(t), dummyToken(t)
	original := noRedirects.Transport
	noRedirects.Transport = bodyFailureTransport{}
	t.Cleanup(func() { noRedirects.Transport = original })
	got := fatalText(t, func(r RowsReporter) {
		Do(r, "http://user:"+password+"@app.example.test", Request{Method: "GET", Path: "/auth/verify?token=" + token + "#access_token=" + fragment})
	})
	for _, secret := range []string{token, password, fragment} {
		if strings.Contains(got, secret) {
			t.Fatalf("the failure text holds %q: %s", secret, got)
		}
	}
	if !strings.Contains(got, "/auth/verify") || !strings.Contains(got, "connection reset") {
		t.Fatalf("the failure text lost the path or the cause: %s", got)
	}
}
