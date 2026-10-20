package session

import (
	"context"
	"errors"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

// CreateAudited is not built yet.
func (s *Store) CreateAudited(context.Context, uuid.UUID, *uuid.UUID, []string, audit.AuditEvent) (Issued, error) {
	return Issued{}, errors.New("session: CreateAudited is not built")
}

// RevokeByCredential is not built yet.
func (s *Store) RevokeByCredential(context.Context, contracts.SecretString, Actor) (bool, error) {
	return false, errors.New("session: RevokeByCredential is not built")
}
