package credentials

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func testPrivateKey(t *testing.T) string {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))
}

// roundTripperFunc is an in-memory client transport: no socket is opened.
type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

// The credentials client sets no request timeout, so the App token exchange
// must bring its own: GitHubAppTokenProvider posts with a 30 s timeout and
// retries a timed-out attempt as a transient failure (up to three attempts).
// The timeout and the first retry delay are shortened here; the shape is the
// same. The exchange runs in a bubble on a virtual clock against an in-memory
// transport, so the return instant is exact: two hung attempts end at their
// own timeout, the retries wait the configured delays (the second doubles the
// first), and the third attempt answers at once.
func TestInstallationTokenAttemptTimesOutAndIsRetried(t *testing.T) {
	previousTimeout, previousDelay := installationTokenTimeout, installationTokenRetryDelay
	installationTokenTimeout, installationTokenRetryDelay = 100*time.Millisecond, 5*time.Millisecond
	t.Cleanup(func() { installationTokenTimeout, installationTokenRetryDelay = previousTimeout, previousDelay })
	gc := &githubCredentials{appID: "1", privateKey: testPrivateKey(t), installationID: "111", app: true}

	synctest.Test(t, func(t *testing.T) {
		var requests atomic.Int32
		transport := roundTripperFunc(func(request *http.Request) (*http.Response, error) {
			if requests.Add(1) <= 2 {
				// A hung attempt ends only through its own context.
				<-request.Context().Done()
				return nil, request.Context().Err()
			}
			return &http.Response{
				StatusCode: http.StatusCreated,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(`{"token":"inst_ok","expires_at":"2099-01-01T00:00:00Z"}`)),
				Request:    request,
			}, nil
		})
		h := handlers{client: &http.Client{Transport: transport}} // no Timeout, as the production default
		start := time.Now()
		token, err := h.installationToken(context.Background(), gc, "https://api.github.test")
		returnedAt := time.Now()
		if err != nil || token != "inst_ok" {
			t.Fatalf("token %q, err %v; want the third attempt's token", token, err)
		}
		if got := requests.Load(); got != 3 {
			t.Errorf("%d requests, want 3 (two timed-out attempts, then success)", got)
		}
		want := start.Add(2*installationTokenTimeout + installationTokenRetryDelay + 2*installationTokenRetryDelay)
		if !returnedAt.Equal(want) {
			t.Errorf("the exchange returned at %v of virtual time after the start, want %v (two attempt timeouts and the retry delays 5 ms and 10 ms)", returnedAt.Sub(start), want.Sub(start))
		}
	})
}

// Every attempt hangs until its own timeout. In the bubble the timeout runs
// on the virtual clock, which moves only when every goroutine is blocked, so
// the JWT signing inside an attempt cannot spend its budget: each of the three
// attempts reaches the transport, whatever the host load.
func TestInstallationTokenGivesUpAfterThreeTimedOutAttempts(t *testing.T) {
	previousTimeout, previousDelay := installationTokenTimeout, installationTokenRetryDelay
	installationTokenTimeout, installationTokenRetryDelay = 50*time.Millisecond, 5*time.Millisecond
	t.Cleanup(func() { installationTokenTimeout, installationTokenRetryDelay = previousTimeout, previousDelay })
	gc := &githubCredentials{appID: "1", privateKey: testPrivateKey(t), installationID: "111", app: true}

	synctest.Test(t, func(t *testing.T) {
		var requests atomic.Int32
		transport := roundTripperFunc(func(request *http.Request) (*http.Response, error) {
			requests.Add(1)
			<-request.Context().Done()
			return nil, request.Context().Err()
		})
		h := handlers{client: &http.Client{Transport: transport}}
		start := time.Now()
		_, err := h.installationToken(context.Background(), gc, "https://api.github.test")
		returnedAt := time.Now()
		if err == nil {
			t.Fatal("a hung exchange answered a token")
		}
		if !transientTokenError(err) {
			t.Errorf("err %v; want the transient failure of the last timed-out attempt", err)
		}
		if got := requests.Load(); got != int32(installationTokenAttempts) {
			t.Errorf("%d requests, want %d", got, installationTokenAttempts)
		}
		want := start.Add(3*installationTokenTimeout + installationTokenRetryDelay + 2*installationTokenRetryDelay)
		if !returnedAt.Equal(want) {
			t.Errorf("the exchange gave up at %v of virtual time after the start, want %v (three attempt timeouts and the retry delays 5 ms and 10 ms)", returnedAt.Sub(start), want.Sub(start))
		}
	})
}
