//go:build integration

package workload

import (
	"errors"
	"testing"

	"github.com/google/uuid"
)

func pinnedBinding(t *testing.T, e *env) (string, BindingSpec) {
	t.Helper()
	ctx := ctxT(t)
	n := name()
	if _, err := e.writer.Register(ctx, Spec{Name: n, Audiences: []string{"auth-service"}}, Actor{}); err != nil {
		t.Fatal(err)
	}
	b := tuple("ns-" + n)
	b.ServiceAccountUID = uuid.NewString()
	if _, err := e.writer.AddBinding(ctx, n, b, Actor{}); err != nil {
		t.Fatal(err)
	}
	return n, b
}

func TestBindCannotMoveAPinOnTheSameTuple(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	n, b := pinnedBinding(t, e)
	moved := b
	moved.ServiceAccountUID = uuid.NewString()
	if _, err := e.writer.AddBinding(ctx, n, moved, Actor{}); !errors.Is(err, ErrBindingTaken) {
		t.Fatalf("bind with a new uid = %v, want ErrBindingTaken", err)
	}
	if _, err := e.reader.Resolve(ctx, moved.input()); RefusalReason(err) != string(ReasonUIDMismatch) {
		t.Fatalf("Resolve with a recreated uid = %v, want uid_mismatch", err)
	}
}
