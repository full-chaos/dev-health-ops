// Package fakepg is a minimal PostgreSQL server for tests that need REAL
// pgxpool connections (so Stat().AcquiredConns() is real) without a database:
// it completes the startup handshake, accepts every simple query with an empty
// result, and tracks BEGIN/ROLLBACK/COMMIT in its transaction status byte. It
// is for pool-saturation tests (CHAOS-6771), not a SQL engine.
package fakepg

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

// Serve starts the fake server on a loopback port and returns its address. It
// stops with the test.
func Serve(t testing.TB) string {
	t.Helper()
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
				serve(connection)
			}()
		}
	}()
	return listener.Addr().String()
}

func serve(connection net.Conn) {
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
			sql := strings.ToUpper(strings.TrimSpace(typed.String))
			switch {
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
