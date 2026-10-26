//go:build unix

package apiservice

import (
	"errors"
	"net"
	"strconv"
	"syscall"
	"testing"
	"time"
)

// closedAddress returns a loopback address that refuses connections for the
// whole test. The socket is bound and never listens, so the port stays owned
// (no stub can rebind it) and a dial gets a reset, not a hang.
func closedAddress(t *testing.T) string {
	t.Helper()
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = syscall.Close(fd) })
	if err := syscall.Bind(fd, &syscall.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}}); err != nil {
		t.Fatal(err)
	}
	bound, err := syscall.Getsockname(fd)
	if err != nil {
		t.Fatal(err)
	}
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(bound.(*syscall.SockaddrInet4).Port))
}

// The "closed" address must stay refused for the whole test: nothing in the
// process may bind it, and a dial must be refused, not hang.
func TestClosedAddressStaysRefusedAndUnrebindable(t *testing.T) {
	address := closedAddress(t)
	if listener, err := net.Listen("tcp", address); err == nil {
		listener.Close()
		t.Fatalf("%s was free to bind after closedAddress returned", address)
	}
	for range 200 {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		got := listener.Addr().String()
		listener.Close()
		if got == address {
			t.Fatalf("an ephemeral bind reused %s", address)
		}
	}
	conn, err := net.DialTimeout("tcp", address, time.Second)
	if err == nil {
		conn.Close()
		t.Fatalf("dial %s succeeded", address)
	}
	if !errors.Is(err, syscall.ECONNREFUSED) {
		t.Fatalf("dial %s = %v, want connection refused", address, err)
	}
}
