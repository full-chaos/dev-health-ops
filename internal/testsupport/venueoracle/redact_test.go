package venueoracle

import (
	"crypto/rand"
	"encoding/base64"
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
