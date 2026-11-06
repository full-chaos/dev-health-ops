package repoidentityprojection

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/restorefence"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// FloorInputName is the restore-fence input of the repository identity projection.
const FloorInputName = "repository-identity-projection-store"

// ErrNoFloors: the projection holds no event-written row. An empty store is no evidence of "no revision above 0", so
// the fence skips the input.
var ErrNoFloors = errors.New("repoidentityprojection: no projected revisions")

// FloorKey is the fence's key of one repository: aggregate_type:aggregate_id.
func FloorKey(aggregateID string) string { return "repository_identity:" + aggregateID }

type floorInput struct {
	pool *pgxpool.Pool
	q    string
}

// NewFloorInput is the projection as a restore-fence floor input: the highest revision it has seen per repository,
// applied or parked. Status is always usable: every table it reads is in the database the fence restores; the input
// exists so a projection moved off that database later cannot be forgotten by the consumer census.
func NewFloorInput(pool *pgxpool.Pool, schema authschema.ValidatedIdentifier) restorefence.FloorInput {
	return floorInput{pool: pool, q: authschema.Quote(schema)}
}

func (floorInput) Name() string                 { return FloorInputName }
func (floorInput) Status(context.Context) error { return nil }

func (f floorInput) Floors(ctx context.Context) (map[string]int64, error) {
	rows, err := f.pool.Query(ctx, `
		SELECT organization_id::text || '/' || provider || '/' || provider_repo_id, source_revision FROM `+f.q+`.repository_identities
		WHERE source_revision > 0
		UNION ALL
		SELECT aggregate_id, revision FROM `+f.q+`.projection_retry WHERE stream = 'repository_identity'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	floors := map[string]int64{}
	for rows.Next() {
		var id string
		var revision int64
		if err := rows.Scan(&id, &revision); err != nil {
			return nil, err
		}
		if revision > floors[FloorKey(id)] {
			floors[FloorKey(id)] = revision
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(floors) == 0 {
		return nil, ErrNoFloors
	}
	return floors, nil
}
