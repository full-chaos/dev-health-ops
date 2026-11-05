package audit

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/auth/repoidentityevent"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// ErrRepositoryIdentity wraps ErrMutationFailed: AppendRepositoryIdentity refused the events before it wrote anything,
// or failed part way. The caller owns the transaction and must roll it back on any error.
var ErrRepositoryIdentity = fmt.Errorf("%w: repository identity events", ErrMutationFailed)

// RepositoryIdentityEvent is one repository a provider sync observed (repoidentityevent.Event).
type RepositoryIdentityEvent = repoidentityevent.Event

// AppendRepositoryIdentity appends one repository_identity.observed event per repository to the auth outbox inside the
// caller's transaction tx, so the caller's state change and its events commit or roll back together.
//
// It writes through the auth schema's append_repository_identity_event function (migration 0056), not through the
// tables: the caller's login holds EXECUTE on that function and no table privilege. The function fixes the aggregate
// type, the event type and the payload, assigns the revision and the idempotency key, and arms and disarms the commit
// guard itself. Go checks every event first, so a refusal here writes nothing.
//
// Events are sent in aggregate-id order (one lock order for every writer).
func AppendRepositoryIdentity(ctx context.Context, tx pgx.Tx, schema authschema.ValidatedIdentifier, events []RepositoryIdentityEvent) error {
	if tx == nil || len(events) == 0 {
		return fmt.Errorf("%w: a transaction and at least one event are required", ErrRepositoryIdentity)
	}
	ordered := append([]RepositoryIdentityEvent(nil), events...)
	for _, e := range ordered {
		if err := e.Validate(); err != nil {
			return fmt.Errorf("%w: %w", ErrRepositoryIdentity, err)
		}
	}
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].AggregateID() < ordered[j].AggregateID() })
	statement := `SELECT ` + authschema.Quote(schema) + `.` + authschema.RepoIdentityFunction + `($1, $2, $3, $4, $5)`
	for _, e := range ordered {
		var revision int64
		if err := tx.QueryRow(ctx, statement, e.OrganizationID, e.Provider, e.ProviderRepoID, e.Path, e.ObservedAt.UTC()).Scan(&revision); err != nil {
			return fmt.Errorf("%w: appending an event: %w", ErrRepositoryIdentity, err)
		}
	}
	return nil
}

// RepositoryIdentityOutbox is the auth schema whose append function receives a provider sync's events.
type RepositoryIdentityOutbox struct {
	Schema authschema.ValidatedIdentifier
}

// AppendRepositoryIdentity is AppendRepositoryIdentity on the outbox's schema.
func (o RepositoryIdentityOutbox) AppendRepositoryIdentity(ctx context.Context, tx pgx.Tx, events []RepositoryIdentityEvent) error {
	return AppendRepositoryIdentity(ctx, tx, o.Schema, events)
}

// ErrRepositoryIdentityOutboxNotExecutable: the login cannot execute the append function (absent, or no EXECUTE).
var ErrRepositoryIdentityOutboxNotExecutable = errors.New("repository identity append function is not executable by this login")

// Check reads the catalog only: the append function exists in the schema and the login of db may execute it.
func (o RepositoryIdentityOutbox) Check(ctx context.Context, db interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}) error {
	procedure := authschema.Quote(o.Schema) + "." + authschema.RepoIdentityFunction + authschema.RepoIdentityFunctionSignature
	var exists, executable bool
	if err := db.QueryRow(ctx, `SELECT to_regprocedure($1) IS NOT NULL,
		COALESCE(has_function_privilege(to_regprocedure($1), 'EXECUTE'), false)`, procedure).Scan(&exists, &executable); err != nil {
		return fmt.Errorf("%w: %w", ErrRepositoryIdentityOutboxNotExecutable, err)
	}
	switch {
	case !exists:
		return fmt.Errorf("%w: %s does not exist", ErrRepositoryIdentityOutboxNotExecutable, procedure)
	case !executable:
		return fmt.Errorf("%w: no EXECUTE on %s", ErrRepositoryIdentityOutboxNotExecutable, procedure)
	}
	return nil
}
