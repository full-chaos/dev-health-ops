package authapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/token"
)

// hangingKeySigner is a real file signer whose key-file reads block while
// hang is set, as a stuck filesystem does, and read the real file otherwise.
type hangingKeySigner struct {
	AccessSigner
	hang    atomic.Bool
	entered chan struct{}
	release chan struct{}
	reads   atomic.Int64
}

func newHangingKeySigner(t *testing.T) *hangingKeySigner {
	t.Helper()
	dir := t.TempDir()
	writeEd25519Key(t, dir, "k1")
	manifest := writeKeySet(t, dir, "k1", "active")
	h := &hangingKeySigner{entered: make(chan struct{}, 1024), release: make(chan struct{})}
	reads := token.NewFileReads(func(path string, limit int64) ([]byte, error) {
		h.reads.Add(1)
		if h.hang.Load() {
			h.entered <- struct{}{}
			<-h.release
		}
		return os.ReadFile(path)
	})
	h.AccessSigner = token.NewFileSigner(token.FileConfig{KeySetPath: manifest, Reads: reads})
	t.Cleanup(func() {
		select {
		case <-h.release:
		default:
			close(h.release)
		}
	})
	return h
}

func TestPreflightWithAHungKeyReadAnswers503AtTheRequestDeadline(t *testing.T) {
	signer := newHangingKeySigner(t)
	signer.hang.Store(true)
	f := onEdDSA(t, newCompatFixture(t), signer)
	f.sessions.result = accepted(f.record)

	for range 3 {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		request := httptest.NewRequestWithContext(ctx, http.MethodPost, PatternCompatRefresh,
			strings.NewReader(`{"refresh_token":"`+credential()+`"}`))
		response := httptest.NewRecorder()
		f.server.ServeHTTP(response, request)
		cancel()
		expectGoAPIUnavailable(t, response)
	}
	if n := f.sessions.exchanges; n != 0 {
		t.Fatalf("%d exchanges behind a refused preflight", n)
	}
	if n := signer.reads.Load(); n != 1 {
		t.Fatalf("%d reads of the hung manifest across 3 requests, want 1", n)
	}
	close(signer.release)
}

func TestSignWithAHungKeyReadAnswers503AtTheRequestDeadline(t *testing.T) {
	signer := newHangingKeySigner(t)
	f := onEdDSA(t, newCompatFixture(t), signer)
	withClock(t, f.issuer)
	f.sessions.result = accepted(f.record)
	if err := f.issuer.Preflight(context.Background()); err != nil {
		t.Fatalf("healthy preflight: %v", err)
	}
	signer.hang.Store(true)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	request := httptest.NewRequestWithContext(ctx, http.MethodPost, PatternCompatRefresh,
		strings.NewReader(`{"refresh_token":"`+credential()+`"}`))
	response := httptest.NewRecorder()
	f.server.ServeHTTP(response, request)
	expectGoAPIUnavailable(t, response)

	mintCtx, mintCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer mintCancel()
	_, err := f.issuer.AccessContext(mintCtx, f.grant())
	if !errors.Is(err, errSignerUnavailable) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("mint with a hung key read: %v", err)
	}
	close(signer.release)
}
