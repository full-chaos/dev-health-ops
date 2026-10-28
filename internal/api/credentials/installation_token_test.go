package credentials

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
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

// The credentials client sets no request timeout, so the App token exchange
// must bring its own: GitHubAppTokenProvider posts with a 30 s timeout and
// retries a timed-out attempt as a transient failure (up to three attempts).
// The timeout and the first retry delay are shortened here; the shape is the
// same.
// attemptRecorder is the client transport of the retry test: for every attempt it records when the attempt was handed to it, and
// then the deadline the attempt's context carries.
type attemptRecorder struct {
	mu       sync.Mutex
	attempts []recordedAttempt
}

type recordedAttempt struct {
	handedAt, deadline time.Time
	hasDeadline        bool
}

func (r *attemptRecorder) RoundTrip(request *http.Request) (*http.Response, error) {
	handedAt := time.Now()
	deadline, ok := request.Context().Deadline()
	r.mu.Lock()
	r.attempts = append(r.attempts, recordedAttempt{handedAt: handedAt, deadline: deadline, hasDeadline: ok})
	r.mu.Unlock()
	return http.DefaultTransport.RoundTrip(request)
}

func TestInstallationTokenAttemptTimesOutAndIsRetried(t *testing.T) {
	previousTimeout, previousDelay := installationTokenTimeout, installationTokenRetryDelay
	installationTokenTimeout, installationTokenRetryDelay = 100*time.Millisecond, 5*time.Millisecond
	t.Cleanup(func() { installationTokenTimeout, installationTokenRetryDelay = previousTimeout, previousDelay })

	var requests atomic.Int32
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := requests.Add(1)
		if attempt <= 2 {
			select {
			case <-release:
			case <-r.Context().Done():
			}
			return
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"token":"inst_ok","expires_at":"2099-01-01T00:00:00Z"}`))
	}))
	t.Cleanup(func() { close(release); server.Close() })

	recorder := &attemptRecorder{}
	h := handlers{client: &http.Client{Transport: recorder}} // no Timeout, as the production default
	gc := &githubCredentials{appID: "1", privateKey: testPrivateKey(t), installationID: "111", app: true}
	// A hung attempt ends only through the client's own timeout (the handler waits for release or the request context), so a token
	// after two hung attempts shows that each of them timed out.
	token, err := h.installationToken(context.Background(), gc, server.URL)
	if err != nil || token != "inst_ok" {
		t.Fatalf("token %q, err %v; want the third attempt's token", token, err)
	}
	if got := requests.Load(); got != 3 {
		t.Errorf("%d requests, want 3 (two timed-out attempts, then success)", got)
	}
	// Each attempt carries a deadline no later than twice the configured attempt timeout after it was handed to the transport. The
	// check reads the deadline the attempt was given: it measures no elapsed time, so a slow host cannot fail it.
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if len(recorder.attempts) != 3 {
		t.Fatalf("the transport saw %d attempts, want 3", len(recorder.attempts))
	}
	for i, attempt := range recorder.attempts {
		if !attempt.hasDeadline {
			t.Errorf("attempt %d carries no deadline: a hung attempt would never time out", i+1)
			continue
		}
		if attempt.deadline.After(attempt.handedAt.Add(2 * installationTokenTimeout)) {
			t.Errorf("attempt %d deadline is later than twice the configured attempt timeout %v after it was handed over", i+1, installationTokenTimeout)
		}
	}
}

func TestInstallationTokenGivesUpAfterThreeTimedOutAttempts(t *testing.T) {
	previousTimeout, previousDelay := installationTokenTimeout, installationTokenRetryDelay
	installationTokenTimeout, installationTokenRetryDelay = 50*time.Millisecond, 5*time.Millisecond
	t.Cleanup(func() { installationTokenTimeout, installationTokenRetryDelay = previousTimeout, previousDelay })

	var requests atomic.Int32
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(func() { close(release); server.Close() })

	h := handlers{client: &http.Client{}}
	gc := &githubCredentials{appID: "1", privateKey: testPrivateKey(t), installationID: "111", app: true}
	if _, err := h.installationToken(context.Background(), gc, server.URL); err == nil {
		t.Fatal("a hung exchange answered a token")
	}
	if got := requests.Load(); got != 3 {
		t.Errorf("%d requests, want 3", got)
	}
}
