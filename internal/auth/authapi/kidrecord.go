package authapi

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

// KidRecorder records, on the session row, the kid that signed an access
// token of the session. An empty kid records an hs256_compat mint: the set
// becomes '{}', not NULL, so a compromise of an EdDSA kid leaves the session
// alone. It reports an error for a session that has no row.
type KidRecorder interface {
	RecordSigningKid(ctx context.Context, sessionID uuid.UUID, kid string) error
}

// WithKidRecorder makes every mint record its kid before the token is
// returned. A failed record returns no token. Without a recorder (tests) a
// mint records nothing.
func (i *Issuer) WithKidRecorder(recorder KidRecorder) *Issuer {
	i.kids = recorder
	return i
}

func (i *Issuer) recordKid(ctx context.Context, grant Grant, compact, kid string) (string, error) {
	if i.kids == nil {
		return compact, nil
	}
	if err := i.kids.RecordSigningKid(ctx, grant.SessionID, kid); err != nil {
		return "", fmt.Errorf("%w: recording the signing kid: %w", errIssue, err)
	}
	return compact, nil
}
