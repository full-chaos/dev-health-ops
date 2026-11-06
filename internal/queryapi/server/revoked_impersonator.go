package server

import (
	"context"
	"errors"
)

// errRevokedImpersonatorWrite is liveScope's answer on the write door for a
// caller that does not hold admin.impersonate while its go-api impersonation
// row is live (a revoked impersonator): its row is never applied, and a
// write is refused while the row lives.
var errRevokedImpersonatorWrite = errors.New("revoked impersonator write refused")

type writeDoorKey struct{}

// withWriteDoor marks a request of the mutation-only write door, the one
// /query route liveScope reads a non-holder's row on.
func withWriteDoor(ctx context.Context) context.Context {
	return context.WithValue(ctx, writeDoorKey{}, true)
}

func onWriteDoor(ctx context.Context) bool {
	door, _ := ctx.Value(writeDoorKey{}).(bool)
	return door
}
