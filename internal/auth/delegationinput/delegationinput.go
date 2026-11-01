// Package delegationinput builds the policy decision input of a delegated
// session: the real actor, its platform role from a live read, and its
// assurance. Nothing is cached and nothing comes from a token claim.
package delegationinput

import (
	"context"
	"errors"
	"fmt"
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

// Snapshot reads the session and the actor's platform role now and returns the
// snapshot to evaluate. A session row that is not delegated returns Inner
// unchanged, unless a token names an actor. Every failure returns a snapshot
// that denies, and an error when the cause is a failed read.
func (a *Adapter) Snapshot(ctx context.Context, in Input) (authorization.Snapshot, error) {
	if in.Inner == nil {
		return nil, errors.New("delegationinput: no inner snapshot")
	}
	if in.SessionID == uuid.Nil {
		return invalid(in.Inner), nil
	}
	rec, err := a.sessions.Current(ctx, in.SessionID)
	if err != nil {
		return invalid(in.Inner), fmt.Errorf("%w: session: %w", ErrDelegationUnavailable, err)
	}
	if rec.Delegation == nil {
		if in.ClaimedActor != nil {
			return invalid(in.Inner), nil
		}
		return in.Inner, nil
	}
	actor := rec.Delegation.Actor
	if rec.PrincipalID != in.PrincipalID || rec.RevokedAt != nil || !a.now().Before(rec.ExpiresAt) ||
		rec.OrganizationID == nil || actor == uuid.Nil || (in.ClaimedActor != nil && *in.ClaimedActor != actor) {
		return invalid(in.Inner), nil
	}
	auth, err := a.roles.Resolve(ctx, actor)
	if err != nil {
		return invalid(in.Inner), fmt.Errorf("%w: platform role: %w", ErrDelegationUnavailable, err)
	}
	if !auth.Allows(session.ImpersonateAction) {
		return invalid(in.Inner), nil
	}
	role := ""
	if auth.IsOwner() {
		role = platformrole.PlatformOwner
	}
	return delegatedSnapshot{Snapshot: in.Inner, del: authorization.Delegation{
		Chain: []authorization.Hop{{
			ActorPrincipalID: actor.String(),
			PermittedActions: append([]string(nil), rec.Delegation.PermittedActions...),
		}},
		ActorPlatformRole:    role,
		TargetOrganizationID: rec.OrganizationID.String(),
		Assurance:            session.AssuranceOf(rec.AMR),
	}}, nil
}
