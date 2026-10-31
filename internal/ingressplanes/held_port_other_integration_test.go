//go:build integration && !(linux && (amd64 || arm64))

package ingressplanes

import (
	"net"
	"testing"
)

func heldPort(t *testing.T) string {
	t.Helper()
	t.Skip("heldPort needs SO_REUSEPORT semantics of linux/amd64 or linux/arm64")
	return ""
}

func listenOnHeldPort(address string) (net.Listener, error) {
	return nil, nil
}
