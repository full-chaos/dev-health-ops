//go:build integration

package providersync

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// THROWAWAY PROBE (CHAOS-14574 r1 opinion; never merged). lostAckConn lets one
// statement whose text carries marker reach the server and run to its end
// (autocommit: committed), reads the whole server reply up to ReadyForQuery,
// then drops the connection: the client sees a lost reply after the commit.
type lostAckControl struct {
	mu      sync.Mutex
	armed   bool
	marker  []byte
	cutting bool
	cuts    int
}

type lostAckConn struct {
	net.Conn
	control *lostAckControl
}

func (conn *lostAckConn) Write(p []byte) (int, error) {
	control := conn.control
	control.mu.Lock()
	if control.armed && bytes.Contains(p, control.marker) {
		control.armed, control.cutting = false, true
	}
	control.mu.Unlock()
	return conn.Conn.Write(p)
}

func (conn *lostAckConn) Read(p []byte) (int, error) {
	control := conn.control
	control.mu.Lock()
	cutting := control.cutting
	control.mu.Unlock()
	if !cutting {
		return conn.Conn.Read(p)
	}
	var reply []byte
	chunk := make([]byte, 4096)
	for {
		n, err := conn.Conn.Read(chunk)
		reply = append(reply, chunk[:n]...)
		for len(reply) >= 5 {
			size := int(binary.BigEndian.Uint32(reply[1:5]))
			if len(reply) < 1+size {
				break
			}
			if reply[0] == 'Z' {
				control.mu.Lock()
				control.cutting = false
				control.cuts++
				control.mu.Unlock()
				_ = conn.Conn.Close()
				return 0, io.ErrUnexpectedEOF
			}
			reply = reply[1+size:]
		}
		if err != nil {
			return 0, err
		}
	}
}

// lostAckPool is a one-connection pool over the cut dialer; one extended-protocol
// round trip per statement (QueryExecModeExec), so the cut follows the Execute.
func lostAckPool(t *testing.T, ctx context.Context, uri string, control *lostAckControl) *pgxpool.Pool {
	t.Helper()
	config, err := pgxpool.ParseConfig(uri)
	if err != nil {
		t.Fatal(err)
	}
	config.MaxConns = 1
	config.ConnConfig.TLSConfig = nil
	config.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
	dialer := &net.Dialer{}
	config.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		raw, err := dialer.DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return &lostAckConn{Conn: raw, control: control}, nil
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	return pool
}
