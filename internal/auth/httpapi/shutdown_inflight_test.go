package httpapi

import (
	"context"
	"net/http"
	"testing"
	"time"
)

// SIGTERM cancels the context the server was started with. A request already in
// flight must still finish: Shutdown drains it, and only the drain deadline ends
// it. A cancelled request context would fail the store call under it and the
// caller would see a 500.
func TestServerShutdownDoesNotCancelInFlightRequest(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	seen := make(chan error, 1)
	options := testOptions(Route{
		Method:  http.MethodGet,
		Pattern: "/slow",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			close(entered)
			<-release
			seen <- r.Context().Err()
			w.WriteHeader(http.StatusNoContent)
		}),
	})
	options.RequestTimeout = time.Minute
	server, err := NewServer(options)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	startCtx, cancelStart := context.WithCancel(context.Background())
	if err := server.Start(startCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	status := make(chan int, 1)
	go func() {
		response, err := http.Get("http://" + server.Address() + "/slow")
		if err != nil {
			status <- -1
			return
		}
		_ = response.Body.Close()
		status <- response.StatusCode
	}()
	<-entered

	cancelStart()
	shutdown := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		shutdown <- server.Shutdown(ctx)
	}()
	time.Sleep(100 * time.Millisecond)
	close(release)

	if err := <-seen; err != nil {
		t.Fatalf("the in-flight request context ended with %v when the start context was cancelled", err)
	}
	if got := <-status; got != http.StatusNoContent {
		t.Fatalf("in-flight request answered %d, want 204", got)
	}
	if err := <-shutdown; err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}
