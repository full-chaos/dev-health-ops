//go:build !unix

package apiservice

import (
	"net"
	"testing"
)

// Without raw sockets the port is held by a listener that is never accepted
// on; a dial is not refused here, only kept off every other bind.
func closedAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener.Addr().String()
}
