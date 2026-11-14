// Package refusedaddr gives tests an address whose dials are refused and stay refused.
package refusedaddr

import (
	"net"
	"strconv"
	"syscall"
	"testing"
)

// Addr returns a 127.0.0.1 address with a bound socket that never listens: the kernel refuses every dial (ECONNREFUSED), and no
// other listener, in this process or another parallel test, can be handed the port until the test ends. A port that was bound,
// closed and then dialled can be taken by another test's listener in between.
func Addr(t testing.TB) string {
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
