package refusedaddr

import (
	"errors"
	"net"
	"syscall"
	"testing"
	"time"
)

func TestHeldRefusesBeforeListenAndAcceptsAfterWhileTheThiefNeverGetsThePort(t *testing.T) {
	t.Parallel()
	h := Hold(t)
	thief := func(when string) {
		if l, err := net.Listen("tcp", h.Addr()); err == nil {
			_ = l.Close()
			t.Fatalf("%s: a thief took %s", when, h.Addr())
		} else if !errors.Is(err, syscall.EADDRINUSE) {
			t.Fatalf("%s: thief listen error = %v, want EADDRINUSE", when, err)
		}
	}
	for i := 0; i < 3; i++ {
		conn, err := net.Dial("tcp", h.Addr())
		if err == nil {
			_ = conn.Close()
			t.Fatalf("dial %d before Listen succeeded, want ECONNREFUSED", i)
		}
		if !errors.Is(err, syscall.ECONNREFUSED) {
			t.Fatalf("dial %d error = %v, want ECONNREFUSED", i, err)
		}
	}
	thief("before Listen")
	listener := h.Listen(t)
	thief("after Listen")
	accepted := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			_ = conn.Close()
		}
		accepted <- err
	}()
	conn, err := net.DialTimeout("tcp", h.Addr(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial after Listen: %v", err)
	}
	_ = conn.Close()
	if err := <-accepted; err != nil {
		t.Fatalf("Accept: %v", err)
	}
	if listener.Addr().String() != h.Addr() {
		t.Fatalf("listener at %s, held %s", listener.Addr(), h.Addr())
	}
}
