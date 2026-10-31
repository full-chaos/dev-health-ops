//go:build integration && linux && (amd64 || arm64)

package ingressplanes

import (
	"context"
	"net"
	"strconv"
	"syscall"
	"testing"
)

// soReusePort is SO_REUSEPORT on linux/amd64 and linux/arm64; the frozen syscall
// package defines it on arm64 only.
const soReusePort = 0xf

// heldPort returns a loopback address that refuses connections while the
// returned socket is held: it is bound with SO_REUSEPORT and never listens.
// Only a listener that also sets SO_REUSEPORT can take the same port, so the
// port is never free for anyone else between the refused and the served phase.
func heldPort(t *testing.T) string {
	t.Helper()
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = syscall.Close(fd) })
	if err := syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, soReusePort, 1); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Bind(fd, &syscall.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}}); err != nil {
		t.Fatal(err)
	}
	bound, err := syscall.Getsockname(fd)
	if err != nil {
		t.Fatal(err)
	}
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(bound.(*syscall.SockaddrInet4).Port))
}

// listenOnHeldPort listens on an address heldPort returned, while it is still held.
func listenOnHeldPort(address string) (net.Listener, error) {
	config := net.ListenConfig{Control: func(_, _ string, conn syscall.RawConn) error {
		var setErr error
		if err := conn.Control(func(fd uintptr) {
			setErr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, soReusePort, 1)
		}); err != nil {
			return err
		}
		return setErr
	}}
	return config.Listen(context.Background(), "tcp", address)
}
