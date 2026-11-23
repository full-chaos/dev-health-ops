package providerfoundation

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

type reproBrokenBody struct{ head io.Reader }

func (b *reproBrokenBody) Read(p []byte) (int, error) {
	n, err := b.head.Read(p)
	if err == io.EOF {
		return n, errors.New("synthetic connection reset")
	}
	return n, err
}
func (*reproBrokenBody) Close() error { return nil }

func reproClient(t *testing.T, provider string, doer HTTPDoerFunc) *HTTPClient {
	t.Helper()
	client, err := NewHTTPClient(provider, "https://api.example.test", fakehttp.Client(doer),
		func(*http.Request) error { return nil },
		RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// REPRO claim 1 (client level): a 404 whose body read fails midway. Good state: the error says the read failed.
func TestReproInterrupted404BodyReadErrorIsNotSwallowed(t *testing.T) {
	client := reproClient(t, "github", func(request *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusNotFound, Header: http.Header{}, Request: request,
			Body: &reproBrokenBody{head: strings.NewReader(`{"message":"Not Fo`)}}, nil
	})
	_, err := client.Do(context.Background(), http.MethodGet, "/repos/acme/api/releases", nil)
	var providerErr *ProviderError
	if !errors.As(err, &providerErr) {
		t.Fatalf("err = %v, want a ProviderError", err)
	}
	t.Logf("class=%q status=%d reason=%q afterResponse=%v err=%v", providerErr.Class, providerErr.StatusCode, providerErr.Reason, providerErr.AfterResponse(), err)
	if !providerErr.AfterResponse() && !strings.Contains(err.Error(), "body_read") && !strings.Contains(err.Error(), "reset") {
		t.Fatalf("a 404 with a failed body read is returned as a plain not_found verdict (class=%q afterResponse=%v): the read failure is dropped; err=%v", providerErr.Class, providerErr.AfterResponse(), err)
	}
}

// REPRO claim 3A: a 403 permission denial with an unrelated body field containing "secondary".
func TestReproForbiddenPermissionBodyWithSecondaryWordIsNotRateLimited(t *testing.T) {
	got := ClassifyHTTPWithMessage("github", http.StatusForbidden, http.Header{},
		`{"message":"Resource not accessible by integration","secondary_owner":"team-a"}`)
	if got == nil || got.Class == ErrorRateLimited {
		t.Fatalf("a permission 403 whose body only has the word secondary in another field is classed %v", got)
	}
}

// REPRO claim 3B: the documented secondary-limit sentence, as the client sees it on the wire, must be rate_limited.
func TestReproSecondaryLimitSentenceIsRateLimited(t *testing.T) {
	for name, body := range map[string]string{
		"plain":    `{"message":"You have exceeded a secondary rate limit. Please wait a few minutes."}`,
		"upper":    `{"message":"YOU HAVE EXCEEDED A SECONDARY RATE LIMIT."}`,
		"escaped":  `{"message":"You have exceeded a secondary rate limit."}`,
		"past cap": `{"pad":"` + strings.Repeat("x", maxProviderErrorBody) + `","message":"You have exceeded a secondary rate limit."}`,
	} {
		body := body
		client := reproClient(t, "github", func(request *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusForbidden, Header: http.Header{}, Request: request, Body: io.NopCloser(strings.NewReader(body))}, nil
		})
		_, err := client.Do(context.Background(), http.MethodGet, "/repos/acme/api/x", nil)
		var providerErr *ProviderError
		if !errors.As(err, &providerErr) || providerErr.Class != ErrorRateLimited {
			t.Errorf("%s: err=%v, want rate_limited", name, err)
		}
	}
}
