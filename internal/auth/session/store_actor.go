package session

import "github.com/google/uuid"

// Actor names who asked for a revocation or revision change, for the audit row.
type Actor struct {
	PrincipalID *uuid.UUID
	RequestID   *string
}
