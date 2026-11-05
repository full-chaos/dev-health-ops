package audit

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// ErrRepositoryIdentity wraps ErrMutationFailed: AppendRepositoryIdentity refused the events before it wrote anything,
// or failed part way. The caller owns the transaction and must roll it back on any error.
var ErrRepositoryIdentity = fmt.Errorf("%w: repository identity events", ErrMutationFailed)

// The shapes of migration 0056 (repository_identities) and of the 0057 append function.
var (
	repoIdentityProviderShape = regexp.MustCompile(`^[a-z]{1,16}$`)
	repoIdentityIDShape       = regexp.MustCompile(`^[0-9]{1,20}$`)
	repoIdentityPathShape     = regexp.MustCompile(`^[a-z0-9][a-z0-9._/-]*$`)
)

// RepositoryIdentityEvent is one repository a provider sync observed: its organization, the provider and the
// provider's stable id, the live path (a canonical repository key) and the instant the sync fetched it.
type RepositoryIdentityEvent struct {
	OrganizationID uuid.UUID
	Provider       string
	ProviderRepoID string
	Path           string
	ObservedAt     time.Time
}

// AggregateID is the outbox aggregate id of the repository: "<organization>/<provider>/<provider id>".
func (e RepositoryIdentityEvent) AggregateID() string {
	return e.OrganizationID.String() + "/" + e.Provider + "/" + e.ProviderRepoID
}

func (e RepositoryIdentityEvent) validate() error {
	switch {
	case e.OrganizationID == uuid.Nil:
		return fmt.Errorf("%w: organization id is required", ErrRepositoryIdentity)
	case !repoIdentityProviderShape.MatchString(e.Provider):
		return fmt.Errorf("%w: provider is not a provider name", ErrRepositoryIdentity)
	case !repoIdentityIDShape.MatchString(e.ProviderRepoID):
		return fmt.Errorf("%w: provider repository id is not a number", ErrRepositoryIdentity)
	case len(e.Path) > 255 || !repoIdentityPathShape.MatchString(e.Path):
		return fmt.Errorf("%w: path is not a canonical repository key", ErrRepositoryIdentity)
	case e.ObservedAt.IsZero():
		return fmt.Errorf("%w: observed_at is required", ErrRepositoryIdentity)
	}
	return nil
}

// AppendRepositoryIdentity appends one repository_identity.observed event per repository to the auth outbox inside the
// caller's transaction tx, so the caller's state change and its events commit or roll back together.
//
// It writes through the auth schema's append_repository_identity_event function (migration 0057), not through the
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
		if err := e.validate(); err != nil {
			return err
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
