package credentials

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
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

	h := handlers{client: &http.Client{}} // no Timeout, as the production default
	gc := &githubCredentials{appID: "1", privateKey: testPrivateKey(t), installationID: "111", app: true}
	started := time.Now()
	token, err := h.installationToken(context.Background(), gc, server.URL)
	if err != nil || token != "inst_ok" {
		t.Fatalf("token %q, err %v; want the third attempt's token", token, err)
	}
	if got := requests.Load(); got != 3 {
		t.Errorf("%d requests, want 3 (two timed-out attempts, then success)", got)
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Errorf("took %v: an attempt did not time out", elapsed)
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
