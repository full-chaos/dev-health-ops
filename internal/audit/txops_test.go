package audit

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// TestTxOpsCannotBeAssertedBackToACommitCapableValue is the permanent form of
// the probe that killed round 1's repair.
//
// Round 1 narrowed Apply's parameter to an INTERFACE. Round 2 escaped it in one
// line, because an interface hides methods from the method set while leaving
// the dynamic value intact:
//
//	raw := tx.(interface{ Commit(context.Context) error })
//
// TxOps is now a concrete struct, so `tx.(anything)` does not compile at all --
// a type assertion requires an interface operand. This test asserts the
// property from the outside: box the value in `any`, which is the only way to
// get an interface to assert on, and confirm the assertion fails.
//
// It exists because the compile error is invisible in review. Someone changing
// TxOps back to an interface would restore the escape and every other test
// would still pass.
func TestTxOpsCannotBeAssertedBackToACommitCapableValue(t *testing.T) {
	var boxed any = TxOps{}

	if _, ok := boxed.(interface{ Commit(context.Context) error }); ok {
		t.Error("TxOps satisfies a Commit-capable interface — round 2's escape is open again")
	}
	if _, ok := boxed.(interface{ Rollback(context.Context) error }); ok {
		t.Error("TxOps satisfies a Rollback-capable interface")
	}
	if _, ok := boxed.(interface{ Conn() any }); ok {
		t.Error("TxOps exposes the connection")
	}

	// The accepting half: it must still satisfy what a mutation legitimately
	// needs, or the wrapper is useless and this test would pass on an empty
	// struct that helps nobody.
	if _, ok := boxed.(interface {
		Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	}); !ok {
		t.Error("TxOps does not satisfy Exec — a mutation cannot write anything")
	}
}
