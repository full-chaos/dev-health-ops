// Package pushexpiry emits the expired lifecycle event of External Push
// credentials whose expires_at passed and that were never revoked.
package pushexpiry

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

const (
	BatchLimit = 100
	RunCap     = 1000
)

// Token is the stored state the sweep needs: nothing that names or
// authenticates the credential.
type Token struct {
	ID        uuid.UUID
	OrgID     string
	SourceID  *uuid.UUID
	Scopes    []byte
	ExpiresAt time.Time
}

// Cursor is the keyset position of a scan: the last token read.
type Cursor struct {
	ExpiresAt time.Time
	ID        uuid.UUID
}

// Store reads the expired, unrevoked tokens and sends one expired event each.
type Store interface {
	Page(ctx context.Context, after Cursor, limit int) ([]Token, error)
	Send(ctx context.Context, token Token) error
}

type Handler struct {
	store  Store
	on     bool
	logger *slog.Logger
}

func NewHandler(store Store, on bool, logger *slog.Logger) (*Handler, error) {
	return &Handler{store: store, on: on, logger: logger}, nil
}

// ExpiredEvent is the expired event of a stored token, with its snapshot.
func ExpiredEvent(token Token) (audit.PushLifecycleEvent, error) {
	return audit.PushLifecycleEvent{}, nil
}

func (handler *Handler) Work(
	ctx context.Context,
	execution *jobruntime.Execution[jobruntime.ExternalPushExpirySweepArgs],
) error {
	return nil
}
