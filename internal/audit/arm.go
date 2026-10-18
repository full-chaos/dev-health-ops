package audit

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// armTx marks tx as one that may write the guarded tables (migration 0006). It
// is for this package's own background writers -- the publisher and the reaper
// update and delete outbox rows -- which are internal/audit code and so inside
// the guard's rule, exactly as Commit is.
func armTx(ctx context.Context, tx pgx.Tx) error {
	if _, err := tx.Exec(ctx, `SELECT set_config('`+commitGuardSetting+`', pg_current_xact_id()::text, true)`); err != nil {
		return fmt.Errorf("%w: arming the commit guard: %w", ErrMutationFailed, err)
	}
	return nil
}
