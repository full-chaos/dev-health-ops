package health

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// failAfter is the hang guard of these tests: it fires only when the awaited event never comes.
const failAfter = 30 * time.Second

// gatedServer serves a readiness check that signals when it runs and waits for release.
type gatedServer struct {
	server  *Server
	entered chan struct{}
	release chan struct{}
	swept   chan struct{}
	once    sync.Once
	// served is closed when a /metrics request reaches its handler (the readiness handler answers without its
	// checks once the stop has closed readiness, so only /metrics shows a request served after the stop).
	served     chan struct{}
	servedOnce sync.Once
}

func (g *gatedServer) WritePrometheus(w io.Writer) error {
	g.servedOnce.Do(func() { close(g.served) })
	_, err := io.WriteString(w, "edge_gate_served 1\n")
	return err
}

// open lets the readiness check return.
func (g *gatedServer) open() { g.once.Do(func() { close(g.release) }) }

func startGatedServer(t *testing.T, configure func(*Server)) *gatedServer {
	t.Helper()
	g := &gatedServer{entered: make(chan struct{}), release: make(chan struct{}), swept: make(chan struct{}), served: make(chan struct{})}
	var enteredOnce sync.Once
	t.Cleanup(g.open)
	registry := NewRegistry(time.Minute)
	if err := registry.RegisterRequired("edge_gate", func(ctx context.Context) error {
		enteredOnce.Do(func() { close(g.entered) })
		select {
		case <-g.release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterMetrics("edge_gate", g); err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(ServerOptions{Address: "127.0.0.1:0", Registry: registry, Service: "test", Version: "dev"})
	if err != nil {
		t.Fatal(err)
	}
	server.sweptHook = func() { close(g.swept) }
	if configure != nil {
		configure(server)
	}
	if err := server.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	registry.SetReady(true)
	g.server = server
	return g
}

// holdTrackedConn opens a connection that sends nothing and returns once the server tracks it: the listener accepts
// in order and tracks each connection before the next accept, so an answer on a later connection is the signal.
func holdTrackedConn(t *testing.T, address string) net.Conn {
	t.Helper()
	held, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = held.Close() })
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	response, err := client.Get("http://" + address + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	return held
}

func shutdownAsync(server *Server) <-chan error {
	done := make(chan error, 1)
	go func() { done <- server.Shutdown(context.Background()) }()
	return done
}

func await[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(failAfter):
		t.Fatalf("%s did not happen", what)
		var zero T
		return zero
	}
}

func readAll(conn net.Conn) <-chan string {
	out := make(chan string, 1)
	go func() {
		body, _ := io.ReadAll(conn)
		out <- string(body)
	}()
	return out
}

const (
	readyRequest   = "GET /readyz HTTP/1.1\r\nHost: operator\r\nConnection: close\r\n\r\n"
	metricsRequest = "GET /metrics HTTP/1.1\r\nHost: operator\r\nConnection: close\r\n\r\n"
)

// A request whose bytes reached the server before the stop is served whole: the stop never cuts it.
func TestARequestReadBeforeTheStopIsServedWhole(t *testing.T) {
	g := startGatedServer(t, nil)
	conn, err := net.Dial("tcp", g.server.Address())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := io.WriteString(conn, readyRequest); err != nil {
		t.Fatal(err)
	}
	await(t, g.entered, "the readiness request reaching its check")
	done := shutdownAsync(g.server)
	await(t, g.swept, "the stop's sweep")
	g.open()
	raw := await(t, readAll(conn), "the response")
	response, err := http.ReadResponse(bufio.NewReader(strings.NewReader(raw)), nil)
	if err != nil {
		t.Fatalf("a request read before the stop got %q: %v", raw, err)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil || response.StatusCode != http.StatusOK || string(body) != "{\"status\":\"ok\"}\n" {
		t.Fatalf("a request read before the stop got %d %q (%v), want the whole readiness answer", response.StatusCode, body, err)
	}
	if err := await(t, done, "Shutdown returning"); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

// A request sent on a connection the stop already closed is refused whole: no byte of a response, no handler run.
func TestARequestSentAfterTheStopIsRefusedWhole(t *testing.T) {
	g := startGatedServer(t, nil)
	held := holdTrackedConn(t, g.server.Address())
	done := shutdownAsync(g.server)
	await(t, g.swept, "the stop's sweep")
	_, _ = io.WriteString(held, metricsRequest)
	if response := await(t, readAll(held), "the refusal"); response != "" {
		t.Fatalf("a request sent after the stop got %q, want no response", response)
	}
	if err := await(t, done, "Shutdown returning"); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	select {
	case <-g.served:
		t.Fatal("the refused request reached its handler")
	default:
	}
}

// Bytes the server reads while the stop claims the connection are handed on to nobody: the request is refused whole,
// never served from a closed connection. The stop is held after its sweep until the server is done with the
// connection, so the HTTP server's own shutdown check cannot be what refuses it.
func TestBytesReadAsTheStopClaimsTheConnectionAreRefusedWhole(t *testing.T) {
	reached, proceed := make(chan struct{}), make(chan struct{})
	swept, allowStop := make(chan struct{}), make(chan struct{})
	heldClosed := make(chan struct{})
	var readOnce, closeOnce, allowOnce sync.Once
	t.Cleanup(func() { allowOnce.Do(func() { close(allowStop) }) })
	var heldAddress atomic.Pointer[string]
	isHeld := func(conn net.Conn) bool {
		held := heldAddress.Load()
		return held != nil && conn.RemoteAddr().String() == *held
	}
	g := startGatedServer(t, func(s *Server) {
		s.firstReadHook = func(conn net.Conn) {
			if isHeld(conn) {
				readOnce.Do(func() {
					close(reached)
					<-proceed
				})
			}
		}
		s.closedHook = func(conn net.Conn) {
			if isHeld(conn) {
				closeOnce.Do(func() { close(heldClosed) })
			}
		}
		s.sweptHook = func() {
			close(swept)
			<-allowStop
		}
	})
	held := holdTrackedConn(t, g.server.Address())
	address := held.LocalAddr().String()
	heldAddress.Store(&address)
	if _, err := io.WriteString(held, metricsRequest); err != nil {
		t.Fatal(err)
	}
	await(t, reached, "the server's first read")
	done := shutdownAsync(g.server)
	await(t, swept, "the stop's sweep")
	close(proceed)
	await(t, heldClosed, "the server finishing with the claimed connection")
	select {
	case <-g.served:
		t.Fatal("bytes read after the stop claimed the connection reached the handler")
	default:
	}
	if got := await(t, readAll(held), "the claimed connection closing"); got != "" {
		t.Fatalf("the claimed connection answered %q, want no response", got)
	}
	allowOnce.Do(func() { close(allowStop) })
	if err := await(t, done, "Shutdown returning"); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

// A connection accepted after the stop's sweep, while the listener is still open, is closed at once when it has sent
// nothing: the sweep covers every connection, not only the ones it saw.
func TestAConnectionAcceptedAfterTheSweepIsClosedWhenUnused(t *testing.T) {
	swept, allowStop := make(chan struct{}), make(chan struct{})
	var allowOnce sync.Once
	t.Cleanup(func() { allowOnce.Do(func() { close(allowStop) }) })
	g := startGatedServer(t, func(s *Server) {
		// Only the stop may close the late connection here: the read timeouts would close it too, later.
		s.server.ReadHeaderTimeout, s.server.ReadTimeout = time.Hour, time.Hour
		s.sweptHook = func() {
			close(swept)
			<-allowStop
		}
	})
	done := shutdownAsync(g.server)
	await(t, swept, "the stop's sweep")
	late, err := net.Dial("tcp", g.server.Address())
	if err != nil {
		t.Fatal(err)
	}
	defer late.Close()
	if got := await(t, readAll(late), "the late connection closing"); got != "" {
		t.Fatalf("the late connection answered %q, want none", got)
	}
	allowOnce.Do(func() { close(allowStop) })
	if err := await(t, done, "Shutdown returning"); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}
