package session

import (
	"context"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

// LegacyRefresh is a go-api refresh token whose signature, type and expiry the
// caller verified: its subject, its org_id claim and its jti.
type LegacyRefresh struct {
	PrincipalID    uuid.UUID
	OrganizationID *uuid.UUID
	JTI            contracts.SecretString
}

// ExchangeLegacy exchanges a go-api refresh token once for a new session.
func (s *Store) ExchangeLegacy(context.Context, LegacyRefresh) (Result, error) {
	return Result{Reason: ReasonRefreshUnknown}, nil
}
