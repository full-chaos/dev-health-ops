package authapi

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
)

// jwksClock is the document's clock a test moves.
type jwksClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *jwksClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *jwksClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// jwksHarness serves the JWKS routes over keys. start runs the background
// refresh; stop ends it as the server lifecycle does.
type jwksHarness struct {
	handler http.Handler
	clock   *jwksClock
	start   func(t *testing.T)
	stop    func(ctx context.Context) error
}

func newJWKSHarness(t *testing.T, keys KeySetSource, refreshEvery time.Duration) *jwksHarness {
	t.Helper()
	clock := &jwksClock{now: time.Unix(1_800_000_000, 0)}
	document := NewJWKSDocument(JWKSDocumentConfig{
		Keys: keys, RefreshEvery: refreshEvery, ReadTimeout: time.Hour, Now: clock.Now,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	document.refresh(context.Background())
	mux := http.NewServeMux()
	for _, route := range WellKnownRoutes(WellKnownDeps{JWKS: document, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}) {
		mux.Handle(route.Method+" "+route.Pattern, route.Handler)
	}
	return &jwksHarness{
		handler: requestid.Middleware(mux), clock: clock,
		start: func(t *testing.T) {
			t.Helper()
			if err := document.Start(context.Background()); err != nil {
				t.Fatalf("start: %v", err)
			}
		},
		stop: document.Shutdown,
	}
}

// documentOf is a document over keys holding one read, with no refresher: a
// test that changes keys calls refresh to publish the change.
func documentOf(keys KeySetSource) *JWKSDocument {
	document := NewJWKSDocument(JWKSDocumentConfig{Keys: keys})
	document.refresh(context.Background())
	return document
}
