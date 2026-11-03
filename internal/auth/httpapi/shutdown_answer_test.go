package httpapi

import (
	"context"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"
)

// slowRoute is a handler that ignores its context, as a password compare does,
// and stays in flight until release closes: it stands in for a request on a slow
// host that the drain budget cannot wait for. After release it tries to answer
// 500 and reports what that write returned.
func slowRoute(entered chan<- struct{}, release <-chan struct{}, wrote chan<- error, answerFirst bool) Route {
	return Route{
		Method:  http.MethodGet,
		Pattern: "/slow",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if answerFirst {
				// A body past the connection's write buffer sends the header
				// to the client now, with no flush.
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write(make([]byte, 64<<10))
			}
			close(entered)
			<-release
			if !answerFirst {
				w.WriteHeader(http.StatusInternalServerError)
			}
			_, err := w.Write([]byte("late"))
			wrote <- err
		}),
	}
}

type slowAnswer struct {
	response *http.Response
	err      error
}

// startSlow starts a server with the slow route, sends one request and returns
// once the handler is in flight.
func startSlow(t *testing.T, answerFirst bool) (*Server, <-chan slowAnswer, chan struct{}, <-chan error) {
	t.Helper()
	entered := make(chan struct{})
	release := make(chan struct{})
	wrote := make(chan error, 1)
	options := testOptions(slowRoute(entered, release, wrote, answerFirst))
	options.RequestTimeout = time.Minute
	server, err := NewServer(options)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	answers := make(chan slowAnswer, 1)
	go func() {
		response, err := http.Get("http://" + server.Address() + "/slow")
		answers <- slowAnswer{response: response, err: err}
	}()
	<-entered
	return server, answers, release, wrote
}

// The drain budget runs out while a request is still in its handler (a slow
// host). The request gets an answer before the connections close: 503 with
// Retry-After 1 and the request id, never a dropped connection and never the
// handler's late 500. The answer arrives while the handler still runs (release
// closes only after the client has it), and the handler's late write is refused.
func TestShutdownAnswersAnInFlightRequest503AtTheDrainDeadline(t *testing.T) {
	server, answers, release, wrote := startSlow(t, false)
	shutdown := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		shutdown <- server.Shutdown(ctx)
	}()

	got := <-answers
	if got.err != nil {
		close(release)
		t.Fatalf("the in-flight request got no answer at the drain deadline: %v", got.err)
	}
	_, _ = io.Copy(io.Discard, got.response.Body)
	_ = got.response.Body.Close()
	close(release)
	if got.response.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("the in-flight request at the drain deadline answered %d, want 503", got.response.StatusCode)
	}
	if ra := got.response.Header.Get("Retry-After"); ra != "1" {
		t.Errorf("Retry-After = %q, want 1", ra)
	}
	if cc := got.response.Header.Get("Cache-Control"); cc != "no-store" {
		t.Errorf("Cache-Control = %q, want no-store", cc)
	}
	if !got.response.Close {
		t.Error("the 503 at the drain deadline keeps the connection, want Connection: close")
	}
	if got.response.Header.Get(RequestIDHeader) == "" {
		t.Error("the 503 at the drain deadline carries no request id")
	}
	if err := <-wrote; !errors.Is(err, errAnsweredAtShutdown) {
		t.Errorf("the handler's late write after the 503 returned %v, want errAnsweredAtShutdown", err)
	}
	<-shutdown
}

// A request whose handler already sent its header keeps that answer: the drain
// deadline does not rewrite a response that has started.
func TestShutdownKeepsAnAnswerThatHasStarted(t *testing.T) {
	server, answers, release, wrote := startSlow(t, true)
	got := <-answers
	if got.err != nil {
		close(release)
		t.Fatalf("the started answer did not reach the client: %v", got.err)
	}
	shutdown := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		shutdown <- server.Shutdown(ctx)
	}()
	_, _ = io.Copy(io.Discard, got.response.Body)
	_ = got.response.Body.Close()
	close(release)
	if got.response.StatusCode != http.StatusOK {
		t.Errorf("the started answer is %d, want its own 200", got.response.StatusCode)
	}
	if err := <-wrote; errors.Is(err, errAnsweredAtShutdown) {
		t.Error("the drain deadline took over an answer that had started")
	}
	<-shutdown
}
