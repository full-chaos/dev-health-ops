package authschema

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// SessionReaderStatements returns the statements auth-migrate runs for each of
// options.SessionReaderRoles, so a test elsewhere applies the real grant.
func SessionReaderStatements(ctx context.Context, pool *pgxpool.Pool, options Options) ([]string, error) {
	schemaID, err := NewValidatedIdentifier(options.Schema)
	if err != nil {
		return nil, err
	}
	return sessionReaderStatements(ctx, pool, options, schemaID)
}
