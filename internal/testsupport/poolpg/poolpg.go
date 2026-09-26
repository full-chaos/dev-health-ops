// Package poolpg is a minimal PostgreSQL server for tests that need REAL
// pgxpool connections (so Stat().AcquiredConns() is real) without a database:
// it completes the startup handshake, accepts every simple query with an empty
// result, and tracks BEGIN/ROLLBACK/COMMIT in its transaction status byte. It
// is for pool-saturation tests (CHAOS-6771), not a SQL engine.
package poolpg

import (
	"context"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

// Server is a running fake server.
type Server struct {
	addr string
	// stallBegin makes the server accept BEGIN and never answer it: a database
	// that took the connection and then hangs (CHAOS-6771 r1).
	stallBegin atomic.Bool
	// dropConnections makes the server close every connection at its next query
	// without answering: a recreated pooler whose established server-side
	// connections just died (the CHAOS-4029 2026-08-20 shape).
	dropConnections atomic.Bool
}

// Addr is the host:port the server listens on.
func (s *Server) Addr() string { return s.addr }

// StallBegin makes every later BEGIN hang (true) or answer normally (false).
func (s *Server) StallBegin(stall bool) { s.stallBegin.Store(stall) }

// DropConnections makes every later query kill its connection (true) or answer
// normally (false). Connections opened while dropping die at their first query.
func (s *Server) DropConnections(drop bool) { s.dropConnections.Store(drop) }

// Serve starts the fake server on a loopback port and returns its address. It
// stops with the test.
func Serve(t testing.TB) string { return Start(t).Addr() }

// Start is Serve with a handle for controlling the server.
func Start(t testing.TB) *Server {
	t.Helper()
	server := &Server{}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		_ = listener.Close()
		wg.Wait()
	})
	server.addr = listener.Addr().String()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer connection.Close()
				go func() { <-ctx.Done(); _ = connection.Close() }()
				server.serve(ctx, connection)
			}()
		}
	}()
	return server
}

func (server *Server) serve(ctx context.Context, connection net.Conn) {
	backend := pgproto3.NewBackend(connection, connection)
	for {
		message, err := backend.ReceiveStartupMessage()
		if err != nil {
			return
		}
		switch message.(type) {
		case *pgproto3.SSLRequest, *pgproto3.GSSEncRequest:
			if _, err := connection.Write([]byte{'N'}); err != nil {
				return
			}
			continue
		case *pgproto3.StartupMessage:
		default:
			return
		}
		break
	}
	backend.Send(&pgproto3.AuthenticationOk{})
	backend.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: "17.0"})
	backend.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
	backend.Send(&pgproto3.BackendKeyData{ProcessID: 1, SecretKey: []byte{0, 0, 0, 1}})
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if err := backend.Flush(); err != nil {
		return
	}
	status := byte('I')
	for {
		message, err := backend.Receive()
		if err != nil {
			return
		}
		switch typed := message.(type) {
		case *pgproto3.Query:
			if server.dropConnections.Load() {
				return
			}
			sql := strings.ToUpper(strings.TrimSpace(typed.String))
			switch {
			case strings.HasPrefix(sql, "BEGIN") && server.stallBegin.Load():
				<-ctx.Done()
				return
			case strings.HasPrefix(sql, "BEGIN"):
				status = 'T'
				backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("BEGIN")})
			case strings.HasPrefix(sql, "ROLLBACK"), strings.HasPrefix(sql, "COMMIT"):
				status = 'I'
				backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("ROLLBACK")})
			default:
				backend.Send(&pgproto3.EmptyQueryResponse{})
			}
			backend.Send(&pgproto3.ReadyForQuery{TxStatus: status})
			if err := backend.Flush(); err != nil {
				return
			}
		case *pgproto3.Terminate:
			return
		default:
			return
		}
	}
}
