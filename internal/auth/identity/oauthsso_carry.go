package identity

import (
	"context"

	"github.com/google/uuid"
)

// CarryExtraIdentity links one more key to an existing principal in the users
// backfill: the OAuth-SSO row key of a user whose social key is already carried.
// It reports whether a link was (or, in a dry run, would be) written.
func (s *Store) CarryExtraIdentity(ctx context.Context, principal uuid.UUID, key ExternalKey, dryRun bool) (bool, error) {
	return s.backfillIdentity(ctx, BackfillTarget{ID: principal, Identity: &key}, dryRun)
}
