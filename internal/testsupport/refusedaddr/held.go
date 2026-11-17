package refusedaddr

import (
	"net"
	"os"
	"strconv"
	"syscall"
	"testing"
)

// Held is a bound socket that is not listening yet: dials are refused until Listen, and no other listener can take the port.
type Held struct {
	f    *os.File
	addr string
}

func Hold(t testing.TB) *Held {
	t.Helper()
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	syscall.CloseOnExec(fd)
	f := os.NewFile(uintptr(fd), "refusedaddr-held")
	t.Cleanup(func() { _ = f.Close() })
	if err := syscall.Bind(fd, &syscall.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}}); err != nil {
		t.Fatal(err)
	}
	bound, err := syscall.Getsockname(fd)
	if err != nil {
		t.Fatal(err)
	}
	return &Held{f: f, addr: net.JoinHostPort("127.0.0.1", strconv.Itoa(bound.(*syscall.SockaddrInet4).Port))}
}

func (h *Held) Addr() string { return h.addr }

func (h *Held) Listen(t testing.TB) net.Listener {
	t.Helper()
	if err := syscall.Listen(int(h.f.Fd()), 128); err != nil {
		t.Fatal(err)
	}
	listener, err := net.FileListener(h.f)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener
}
