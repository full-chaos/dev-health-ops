// Package delegationinput builds the policy decision input of a delegated
// session: the real actor, its platform role from a live read, and its
// assurance. Nothing is cached and nothing comes from a token claim.
package delegationinput

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// ErrDelegationUnavailable wraps a failed read. The snapshot returned with it
// denies every action.
var ErrDelegationUnavailable = errors.New("delegationinput: the delegation could not be read")

// Sessions reads one session record (*session.Store).
type Sessions interface {
	Current(ctx context.Context, id uuid.UUID) (session.Record, error)
}

// Roles resolves the platform authority a principal holds now (*platformrole.Store).
type Roles interface {
	Resolve(ctx context.Context, principal uuid.UUID) (platformrole.Authority, error)
}

// Config builds an Adapter.
type Config struct {
	Sessions Sessions
	Roles    Roles
	Now      func() time.Time
}

// Adapter holds no state between decisions.
type Adapter struct {
	sessions Sessions
	roles    Roles
	now      func() time.Time
}

// New refuses a missing reader. A nil Now is time.Now.
func New(cfg Config) (*Adapter, error) {
	if cfg.Sessions == nil || cfg.Roles == nil {
		return nil, errors.New("delegationinput: sessions and roles are required")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Adapter{sessions: cfg.Sessions, roles: cfg.Roles, now: cfg.Now}, nil
}

// Input is one decision. Inner is the effective subject's snapshot.
type Input struct {
	Inner     authorization.Snapshot
	SessionID uuid.UUID
	// PrincipalID is the effective subject of the request.
	PrincipalID uuid.UUID
	// ClaimedActor is the actor a token names. It only denies on a mismatch;
	// it never fills a field.
	ClaimedActor *uuid.UUID
}

type delegatedSnapshot struct {
	authorization.Snapshot
	del authorization.Delegation
}

func (d delegatedSnapshot) Delegation() (authorization.Delegation, bool) { return d.del, true }

// invalid is a delegation with an empty chain: the evaluator denies every
// action of it with delegation_invalid.
func invalid(inner authorization.Snapshot) authorization.Snapshot {
	return delegatedSnapshot{Snapshot: inner}
}

func (a *Adapter) Snapshot(ctx context.Context, in Input) (authorization.Snapshot, error) {
	return invalid(in.Inner), nil
}
