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
// Only UUID-shaped identifiers are returned. The remaining-metrics domain
// tables cast org_id to uuid, so a non-UUID identifier cannot start a run:
// emitting it would fail the whole occurrence instead of scheduling the real
// tenants. In practice this excludes nothing real, because organizations.id is
// itself a uuid column.
//
// Deliberate divergence from the legacy Python discovery, reviewed and pinned:
// `_discover_active_org_ids` returns a literal "default" organization when the
// table is empty, and that sentinel is not reproducible here. Seeding a
// synthetic uuid for it, or widening the column to text, would both produce
// runs whose scope matches no data, since the legacy ClickHouse rows are keyed
// on the org_id='default' string. Either "fix" manufactures zero-work runs that
// report success, which is the false-pass class the cutover plan forbids. The
// empty-table case is therefore surfaced by the caller as a bounded skip with
// reason SkipNoActiveOrganizations and exported as
// fixed_scheduler_schedule_degraded, never silently treated as healthy work.
//
// Whether any live installation actually depends on that fallback is an
// evidence question, not a design one, and is answered at CUT-17 baseline
// capture: if PostgreSQL organizations is empty while ClickHouse holds
// org_id='default' rows, a mapping gets designed then, on that evidence.
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
