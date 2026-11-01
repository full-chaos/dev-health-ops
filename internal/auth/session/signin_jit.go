package session

import (
	"context"
	"errors"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

// InsertIn writes a sign-in's session inside tx.
func (s *Store) InsertIn(context.Context, audit.TxOps, uuid.UUID, *uuid.UUID, []string, audit.AuditEvent) (Issued, error) {
	return Issued{}, errors.New("session: InsertIn is not built")
}
