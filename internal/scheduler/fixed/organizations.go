package fixed

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// maximumFanoutOrganizations bounds one fan-out occurrence. Exceeding it is an
// error rather than a truncation: silently scheduling a prefix of the tenant
// list would look identical to a healthy nightly run.
const maximumFanoutOrganizations = 5000

// PostgresOrganizationLister enumerates active organizations for fan-out
// schedules.
type PostgresOrganizationLister struct{}

// NewPostgresOrganizationLister constructs the production organization lister.
func NewPostgresOrganizationLister() OrganizationLister {
	return PostgresOrganizationLister{}
}

// ActiveOrganizationIDs returns every active organization in a deterministic
// order, inside the caller's transaction.
//
// Only UUID-shaped identifiers are returned. The legacy Python discovery
// falls back to a literal "default" organization when the table is empty,
// which single-tenant installations relied on. The remaining-metrics domain
// tables cast org_id to uuid, so a non-UUID identifier cannot start a run:
// emitting it would fail the whole occurrence instead of scheduling the real
// tenants. Such rows are excluded here and reported by the caller as a skip
// reason rather than crashing the schedule.
//
// A read failure is never converted into an empty list. The legacy dispatchers
// used strict discovery (CHAOS-2439) precisely because a swallowed database
// error silently cancelled every nightly safety net.
func (PostgresOrganizationLister) ActiveOrganizationIDs(
	ctx context.Context,
	tx pgx.Tx,
) ([]string, error) {
	if ctx == nil || tx == nil {
		return nil, ErrProducerUnavailable
	}
	rows, err := tx.Query(ctx, activeOrganizationsSQL, maximumFanoutOrganizations+1)
	if err != nil {
		return nil, fmt.Errorf("read active organizations: %w", err)
	}
	defer rows.Close()

	identifiers := make([]string, 0, 64)
	for rows.Next() {
		var identifier string
		if err := rows.Scan(&identifier); err != nil {
			return nil, fmt.Errorf("scan active organization: %w", err)
		}
		if _, parseErr := uuid.Parse(identifier); parseErr != nil {
			continue
		}
		identifiers = append(identifiers, identifier)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read active organizations: %w", err)
	}
	if len(identifiers) > maximumFanoutOrganizations {
		return nil, fmt.Errorf(
			"active organization count exceeds the %d bounded fan-out limit",
			maximumFanoutOrganizations,
		)
	}
	return identifiers, nil
}

const activeOrganizationsSQL = `
SELECT id::text
FROM public.organizations
WHERE is_active = TRUE
ORDER BY id
LIMIT $1
`
