package health

import (
	"bufio"
	"context"
	"errors"
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
}

// open lets the readiness check return.
func (g *gatedServer) open() { g.once.Do(func() { close(g.release) }) }

func startGatedServer(t *testing.T, configure func(*Server)) *gatedServer {
	t.Helper()
	g := &gatedServer{entered: make(chan struct{}), release: make(chan struct{}), swept: make(chan struct{})}
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

const readyRequest = "GET /readyz HTTP/1.1\r\nHost: operator\r\nConnection: close\r\n\r\n"

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
	_, _ = io.WriteString(held, readyRequest)
	if response := await(t, readAll(held), "the refusal"); response != "" {
		t.Fatalf("a request sent after the stop got %q, want no response", response)
	}
	if err := await(t, done, "Shutdown returning"); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	select {
	case <-g.entered:
		t.Fatal("the refused request reached its handler")
	default:
	}
}

// Bytes the server reads while the stop claims the connection are handed on to nobody: the request is refused whole,
// never served from a closed connection.
func TestBytesReadAsTheStopClaimsTheConnectionAreRefusedWhole(t *testing.T) {
	reached, proceed := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var heldAddress atomic.Pointer[string]
	g := startGatedServer(t, func(s *Server) {
		s.firstReadHook = func(conn net.Conn) {
			if held := heldAddress.Load(); held == nil || conn.RemoteAddr().String() != *held {
				return
			}
			once.Do(func() {
				close(reached)
				<-proceed
			})
		}
	})
	held := holdTrackedConn(t, g.server.Address())
	address := held.LocalAddr().String()
	heldAddress.Store(&address)
	if _, err := io.WriteString(held, readyRequest); err != nil {
		t.Fatal(err)
	}
	await(t, reached, "the server's first read")
	done := shutdownAsync(g.server)
	await(t, g.swept, "the stop's sweep")
	close(proceed)
	response := readAll(held)
	select {
	case <-g.entered:
		t.Fatal("bytes read after the stop claimed the connection reached the handler")
	case got := <-response:
		if got != "" {
			t.Fatalf("the claimed connection answered %q, want no response", got)
		}
	case <-time.After(failAfter):
		t.Fatal("the claimed connection neither closed nor answered")
	}
	if err := await(t, done, "Shutdown returning"); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Shutdown: %v", err)
	}
}
