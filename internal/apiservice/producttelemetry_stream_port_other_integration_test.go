//go:build integration && !(linux && (amd64 || arm64))

package apiservice

import (
	"net"
	"testing"
)

func heldPort(t *testing.T) (address string, fd int) {
	t.Helper()
	t.Skip("heldPort needs SO_REUSEPORT semantics of linux/amd64 or linux/arm64")
	return "", 0
}

func listenOnHeldPort(address string) (net.Listener, error) {
	return nil, nil
}
