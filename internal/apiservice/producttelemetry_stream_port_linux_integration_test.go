//go:build integration && linux

package apiservice

import (
	"context"
	"errors"
	"net"
	"strconv"
	"syscall"
	"testing"
	"time"
)

// heldPort returns a loopback address that refuses connections while the
// returned socket is held: it is bound with SO_REUSEPORT and never listens.
// Only a listener that also sets SO_REUSEPORT can take the same port, so the
// port is never free for anyone else between the refused and the served phase.
func heldPort(t *testing.T) (address string, fd int) {
	t.Helper()
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = syscall.Close(fd) })
	if err := syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEPORT, 1); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Bind(fd, &syscall.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}}); err != nil {
		t.Fatal(err)
	}
	bound, err := syscall.Getsockname(fd)
	if err != nil {
		t.Fatal(err)
	}
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(bound.(*syscall.SockaddrInet4).Port)), fd
}

func listenOnHeldPort(address string) (net.Listener, error) {
	config := net.ListenConfig{Control: func(_, _ string, conn syscall.RawConn) error {
		var setErr error
		if err := conn.Control(func(fd uintptr) {
			setErr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEPORT, 1)
		}); err != nil {
			return err
		}
		return setErr
	}}
	return config.Listen(context.Background(), "tcp", address)
}

// The refused phase and the served phase share one address with no moment
// at which the port is free: refused while held, accepted after
// listenOnHeldPort, and the held socket is still open when that happens.
func TestHeldPortIsRefusedThenServedOnTheSameAddressWhileStillHeld(t *testing.T) {
	address, fd := heldPort(t)
	conn, err := net.DialTimeout("tcp", address, time.Second)
	if err == nil {
		conn.Close()
		t.Fatalf("dial %s succeeded while the port was held without a listener", address)
	}
	if !errors.Is(err, syscall.ECONNREFUSED) {
		t.Fatalf("dial %s = %v, want connection refused", address, err)
	}
	listener, err := listenOnHeldPort(address)
	if err != nil {
		t.Fatalf("listen on held %s: %v", address, err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	if listener.Addr().String() != address {
		t.Fatalf("listener on %s, want %s", listener.Addr(), address)
	}
	if _, err := syscall.Getsockname(fd); err != nil {
		t.Fatalf("held socket no longer open after the listener took the address: %v", err)
	}
	accepted := make(chan error, 1)
	go func() {
		peer, err := listener.Accept()
		if err == nil {
			peer.Close()
		}
		accepted <- err
	}()
	client, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		t.Fatalf("dial %s after listenOnHeldPort: %v", address, err)
	}
	client.Close()
	if err := <-accepted; err != nil {
		t.Fatalf("accept on %s: %v", address, err)
	}
}
