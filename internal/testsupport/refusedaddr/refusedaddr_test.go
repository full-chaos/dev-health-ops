package refusedaddr

import (
	"errors"
	"net"
	"syscall"
	"testing"
)

func TestAddrIsRefusedOnDialAndHeldAgainstListeners(t *testing.T) {
	t.Parallel()
	addr := Addr(t)
	conn, err := net.Dial("tcp", addr)
	if err == nil {
		_ = conn.Close()
		t.Fatalf("dial %s succeeded, want ECONNREFUSED", addr)
	}
	if !errors.Is(err, syscall.ECONNREFUSED) {
		t.Fatalf("dial %s error = %v, want ECONNREFUSED", addr, err)
	}
	listener, err := net.Listen("tcp", addr)
	if err == nil {
		_ = listener.Close()
		t.Fatalf("listen on %s succeeded, want EADDRINUSE", addr)
	}
	if !errors.Is(err, syscall.EADDRINUSE) {
		t.Fatalf("listen on %s error = %v, want EADDRINUSE", addr, err)
	}
}
