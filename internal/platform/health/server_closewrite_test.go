package health

import (
	"io"
	"net"
	"testing"
)

// tcpPair returns both ends of one loopback TCP connection.
func tcpPair(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, _ := listener.Accept()
		accepted <- conn
	}()
	client, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	server := await(t, accepted, "the accept")
	if server == nil {
		t.Fatal("accept failed")
	}
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	return server, client
}

func trackedOver(conn net.Conn) (*Server, *trackedConn) {
	s := &Server{conns: map[*trackedConn]struct{}{}}
	tracked := &trackedConn{Conn: conn, server: s}
	s.conns[tracked] = struct{}{}
	return s, tracked
}

func readResult(conn net.Conn) <-chan error {
	out := make(chan error, 1)
	go func() {
		_, err := io.ReadAll(conn)
		out <- err
	}()
	return out
}

// CloseWrite half-closes a TCP connection the way http.Server expects: the peer reads the end of the stream, and the
// server side still reads what the peer sends and stays registered until Close.
func TestTrackedConnCloseWriteHalfClosesATCPConnection(t *testing.T) {
	serverEnd, peer := tcpPair(t)
	s, tracked := trackedOver(serverEnd)
	if err := tracked.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite: %v", err)
	}
	if err := await(t, readResult(peer), "the peer reading the end of the stream"); err != nil {
		t.Fatalf("the peer read %v after CloseWrite, want the end of the stream", err)
	}
	if _, err := io.WriteString(peer, "x"); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 1)
	if n, err := tracked.Read(buf); n != 1 || err != nil || buf[0] != 'x' {
		t.Fatalf("read after CloseWrite = %d %q %v, want the byte the peer sent", n, buf[:n], err)
	}
	if _, registered := s.conns[tracked]; !registered {
		t.Fatal("CloseWrite dropped the connection from the server's set; only Close may")
	}
	_ = tracked.Close()
	if _, registered := s.conns[tracked]; registered {
		t.Fatal("Close left the connection in the server's set")
	}
}

// Without a half-close on the underlying connection, CloseWrite closes it (and drops it from the server's set).
func TestTrackedConnCloseWriteClosesAConnectionWithoutHalfClose(t *testing.T) {
	serverEnd, peer := net.Pipe()
	t.Cleanup(func() { _ = serverEnd.Close(); _ = peer.Close() })
	s, tracked := trackedOver(serverEnd)
	if err := tracked.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite: %v", err)
	}
	if err := await(t, readResult(peer), "the peer reading the close"); err != nil {
		t.Fatalf("the peer read %v after CloseWrite, want the end of the stream", err)
	}
	if _, registered := s.conns[tracked]; registered {
		t.Fatal("CloseWrite on a connection without a half-close left it in the server's set")
	}
}
