package providerfoundation

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

const secondaryLimitBody = `{"message":"You have exceeded a secondary rate limit. Please wait a few minutes."}`

// CHAOS-10071: a GitHub secondary limit whose only signal is the body message
// must be rate_limited with its dimension in the App token exchange.
func TestGitHubAppInstallationTokenReadsBodyForSecondaryLimit(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	pemKey := string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))
	credential := NewCredential("github", "id", nil, map[string]secrets.Value{"app_id": secrets.NewValue("1"), "private_key": secrets.NewValue(pemKey), "installation_id": secrets.NewValue("2")})
	doer := HTTPDoerFunc(func(request *http.Request) (*http.Response, error) {
		return testHTTPResponse(request, http.StatusForbidden, nil, secondaryLimitBody), nil
	})
	auth, err := NewGitHubAppAuth(credential, "https://api.github.com", fakehttp.Client(doer))
	if err != nil {
		t.Fatal(err)
	}
	_, err = auth.installationToken(context.Background())
	var providerErr *ProviderError
	if !errors.As(err, &providerErr) {
		t.Fatalf("err = %v, want a ProviderError", err)
	}
	if providerErr.Class != ErrorRateLimited || providerErr.Dimension != "secondary_abuse_risk" || providerErr.StatusCode != http.StatusForbidden {
		t.Fatalf("class=%q dimension=%q status=%d, want rate_limited/secondary_abuse_risk/403", providerErr.Class, providerErr.Dimension, providerErr.StatusCode)
	}
}

// CHAOS-10071: the request metric of DoUnauthenticated records the body-only
// limit as rate_limited, and the caller still reads the whole body.
func TestDoUnauthenticatedMetricReadsBodyForSecondaryLimit(t *testing.T) {
	t.Parallel()
	client := newTestHTTPClient(t, HTTPDoerFunc(func(request *http.Request) (*http.Response, error) {
		return testHTTPResponse(request, http.StatusForbidden, nil, secondaryLimitBody), nil
	}), RetryPolicy{MaxAttempts: 1, InitialWait: time.Millisecond, MaxWait: time.Millisecond})
	client.Metrics = NewMetrics()

	response, err := client.DoUnauthenticated(context.Background(), http.MethodGet, "https://objects.example.test/artifact")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil || string(body) != secondaryLimitBody {
		t.Fatalf("body = %q err = %v, want the whole body restored", body, err)
	}
	var rendered bytes.Buffer
	if err := client.Metrics.WritePrometheus(&rendered); err != nil {
		t.Fatal(err)
	}
	if want := `dev_health_provider_requests_total{provider="github",class="rate_limited"} 1`; !strings.Contains(rendered.String(), want) {
		t.Fatalf("missing %q in:\n%s", want, rendered.String())
	}
	if strings.Contains(rendered.String(), `class="authentication"`) {
		t.Fatalf("body-only limit still recorded as authentication:\n%s", rendered.String())
	}
}

func unauthenticatedWithBody(t *testing.T, status int, body io.ReadCloser) (*http.Response, error) {
	t.Helper()
	client := newTestHTTPClient(t, HTTPDoerFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: status, Header: http.Header{}, Body: body, Request: request}, nil
	}), RetryPolicy{MaxAttempts: 1, InitialWait: time.Millisecond, MaxWait: time.Millisecond})
	client.Metrics = NewMetrics()
	return client.DoUnauthenticated(context.Background(), http.MethodGet, "https://objects.example.test/artifact")
}

// blockingBody blocks Read until Close, and reports both events.
type blockingBody struct {
	closed    chan struct{}
	readEnded chan struct{}
}

func (b *blockingBody) Read([]byte) (int, error) {
	<-b.closed
	close(b.readEnded)
	return 0, io.ErrClosedPipe
}

func (b *blockingBody) Close() error {
	select {
	case <-b.closed:
	default:
		close(b.closed)
	}
	return nil
}

// A 403 body that never reaches EOF must not hold DoUnauthenticated: it returns
// within the peek bound with the header classification, the body is closed and
// the reader goroutine has ended.
func TestDoUnauthenticatedStalledErrorBodyReturnsWithinBoundAndIsClosed(t *testing.T) {
	t.Parallel()
	body := &blockingBody{closed: make(chan struct{}), readEnded: make(chan struct{})}
	started := time.Now()
	response, err := unauthenticatedWithBody(t, http.StatusForbidden, body)
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > bodyPeekTimeout+2*time.Second {
		t.Fatalf("DoUnauthenticated took %s, bound is %s", elapsed, bodyPeekTimeout)
	}
	select {
	case <-body.closed:
	case <-time.After(time.Second):
		t.Fatal("the stalled body was not closed")
	}
	select {
	case <-body.readEnded:
	case <-time.After(time.Second):
		t.Fatal("the reader goroutine did not end")
	}
	if _, readErr := io.ReadAll(response.Body); readErr == nil {
		t.Fatal("a read of the closed body returned no error")
	}
}

// A read error on the body reaches the caller as before the peek.
func TestDoUnauthenticatedKeepsTheBodyReadError(t *testing.T) {
	t.Parallel()
	response, err := unauthenticatedWithBody(t, http.StatusForbidden,
		io.NopCloser(io.MultiReader(strings.NewReader("body"), &failOnce{err: io.ErrUnexpectedEOF})))
	if err != nil {
		t.Fatal(err)
	}
	got, readErr := io.ReadAll(response.Body)
	if string(got) != "body" || !errors.Is(readErr, io.ErrUnexpectedEOF) {
		t.Fatalf("body = %q err = %v, want body and io.ErrUnexpectedEOF", got, readErr)
	}
}

type failOnce struct {
	err  error
	done bool
}

func (f *failOnce) Read([]byte) (int, error) {
	if f.done {
		return 0, io.EOF
	}
	f.done = true
	return 0, f.err
}

type untouchedBody struct{ t *testing.T }

func (b untouchedBody) Read([]byte) (int, error) {
	b.t.Error("the body of a status that never consults it was read at classification")
	return 0, io.EOF
}
func (untouchedBody) Close() error { return nil }

// Only a GitHub 403 consults the body; every other failure leaves it unread.
func TestDoUnauthenticatedLeavesBodiesOfOtherFailuresUnread(t *testing.T) {
	t.Parallel()
	for _, status := range []int{http.StatusUnauthorized, http.StatusNotFound, http.StatusTooManyRequests, http.StatusBadGateway} {
		if _, err := unauthenticatedWithBody(t, status, untouchedBody{t}); err != nil {
			t.Fatal(err)
		}
	}
}

type dataAndError struct{ done bool }

func (d *dataAndError) Read(p []byte) (int, error) {
	if d.done {
		return 0, io.EOF
	}
	d.done = true
	return copy(p, "body"), io.ErrUnexpectedEOF
}

// A body that returns its data and its error in one Read keeps doing so.
func TestDoUnauthenticatedKeepsDataAndErrorOfOneRead(t *testing.T) {
	t.Parallel()
	response, err := unauthenticatedWithBody(t, http.StatusForbidden, io.NopCloser(&dataAndError{}))
	if err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 16)
	n, readErr := response.Body.Read(buffer)
	if n != 4 || string(buffer[:n]) != "body" || !errors.Is(readErr, io.ErrUnexpectedEOF) {
		t.Fatalf("Read() = %d, %v, %q; want 4, io.ErrUnexpectedEOF, body", n, readErr, buffer[:n])
	}
}
