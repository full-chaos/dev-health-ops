package providersync

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
)

// SelectionLockExecer is the one method of a pgx transaction the lock needs.
type SelectionLockExecer interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

// LockIntegrationSelection holds the dataset-selection lock of one integration
// to the end of the transaction. Lock hierarchy, the same in every writer of
// integration_datasets rows: org repo-limit lock, then selection locks (the
// parent's integration first, then others in ascending id), then dataset rows
// (keys ascending), then configuration rows. The key text is shared with
// every deployed version: never change it.
func LockIntegrationSelection(ctx context.Context, tx SelectionLockExecer, orgID string, integrationID uuid.UUID) error {
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
		fmt.Sprintf("sync-target-dataset-reconcile:%s:%s", orgID, integrationID)); err != nil {
		return fmt.Errorf("dataset selection lock: %w", err)
	}
	return nil
}

// LockIntegrationSelections locks first, then every other integration in
// ascending id, each once.
func LockIntegrationSelections(ctx context.Context, tx SelectionLockExecer, orgID string, first uuid.UUID, others ...uuid.UUID) error {
	if err := LockIntegrationSelection(ctx, tx, orgID, first); err != nil {
		return err
	}
	rest := make([]uuid.UUID, 0, len(others))
	seen := map[uuid.UUID]bool{first: true}
	for _, id := range others {
		if !seen[id] {
			seen[id] = true
			rest = append(rest, id)
		}
	}
	sort.Slice(rest, func(i, j int) bool { return rest[i].String() < rest[j].String() })
	for _, id := range rest {
		if err := LockIntegrationSelection(ctx, tx, orgID, id); err != nil {
			return err
		}
	}
	return nil
}

// IsDeadlockDetected reports SQLSTATE 40P01. It is a backstop for the answer
// of a route (409), never the fix: the lock hierarchy prevents the deadlock.
func IsDeadlockDetected(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "40P01"
}
