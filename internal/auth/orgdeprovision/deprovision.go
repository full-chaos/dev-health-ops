// Package orgdeprovision ends an organization in the auth schema in one
// audit.Commit: every pending invite revoked, every live membership removed
// (both tables each), every session of the organization ended, and the
// organization suspended. go-api runs it before it purges the organization's
// data, so the purge never leaves an auth membership, invite or session live.
package orgdeprovision

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/grantend"
	"github.com/full-chaos/dev-health-ops/internal/auth/invite"
	"github.com/full-chaos/dev-health-ops/internal/auth/membership"
	"github.com/full-chaos/dev-health-ops/internal/auth/org"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

var (
	ErrNotOwned = errors.New("orgdeprovision: auth-service does not own users, so go-api writes memberships and invites")
	ErrNotFound = errors.New("orgdeprovision: the organization is not in the auth schema")
	ErrDenied   = errors.New("orgdeprovision: the actor may not deprovision the organization")
	// ErrStateMoved: the organization's rows kept changing while the
	// deprovision was planned; the caller may retry.
	ErrStateMoved = errors.New("orgdeprovision: the organization's rows changed while the deprovision was planned")
	ErrStore      = errors.New("orgdeprovision: store unavailable")
)

// errSessionsBusy reports that a session or go-api refresh token row of the
// organization is locked by another writer (session.ErrBusy): the commit is
// rolled back, which releases the rows that writer may wait for, and planned again.
var errSessionsBusy = errors.New("orgdeprovision: a session of the organization is locked by another writer")

// EventDeprovisioned is the audit event of a deprovision.
const EventDeprovisioned = "auth.organization.deprovisioned"

type Config struct {
	Pool        *pgxpool.Pool
	Schema      authschema.ValidatedIdentifier
	Orgs        *org.Store
	Memberships *membership.Store
	Invites     *invite.Store
	// Sessions ends the organization's credentials in the commit; nil ends none.
	Sessions org.SessionEnder
	Logger   *slog.Logger
	Meter    metric.Meter
}

type Actor struct {
	PrincipalID *uuid.UUID
	RequestID   *string
	// Caller is the credential the request came with; the audit row names it.
	Caller audit.Caller
}

// Authorize decides, inside the commit and after the organization row lock,
// whether the actor may deprovision. A nil Authorize refuses (ErrDenied).
type Authorize func(ctx context.Context, tx audit.TxOps, orgID uuid.UUID) error

type Input struct {
	OrgID     uuid.UUID
	Actor     Actor
	Authorize Authorize
}

// Result counts what a deprovision ended. A repeat on an organization with
// nothing live is a no-op: zero counts and Suspended false.
type Result struct {
	RemovedMemberships int
	RevokedInvites     int
	Suspended          bool
}

type Service struct {
	pool     *pgxpool.Pool
	q        string
	orgs     *org.Store
	members  *membership.Store
	invites  *invite.Store
	sessions org.SessionEnder
	commit   func(context.Context, audit.Mutation) error
	logger   *slog.Logger
	outcomes metric.Int64Counter
}

func New(cfg Config) (*Service, error) {
	switch {
	case cfg.Pool == nil:
		return nil, errors.New("orgdeprovision: no connection pool")
	case cfg.Schema.String() == "":
		return nil, errors.New("orgdeprovision: no auth schema")
	case cfg.Orgs == nil || cfg.Memberships == nil || cfg.Invites == nil:
		return nil, errors.New("orgdeprovision: needs the organization, membership and invite stores")
	}
	s := &Service{pool: cfg.Pool, q: authschema.Quote(cfg.Schema), orgs: cfg.Orgs, members: cfg.Memberships, invites: cfg.Invites, sessions: cfg.Sessions, logger: cfg.Logger}
	s.commit = func(ctx context.Context, m audit.Mutation) error { return audit.Commit(ctx, cfg.Pool, cfg.Schema, m) }
	if s.logger == nil {
		s.logger = slog.Default()
	}
	meter := cfg.Meter
	if meter == nil {
		meter = otel.Meter("github.com/full-chaos/dev-health-ops/internal/auth/orgdeprovision")
	}
	var err error
	if s.outcomes, err = meter.Int64Counter("auth.org.deprovisions",
		metric.WithDescription("Organization deprovisions, by outcome.")); err != nil {
		return nil, fmt.Errorf("orgdeprovision: registering the outcome counter: %w", err)
	}
	return s, nil
}

// planAttempts bounds the plan-then-lock loop. A deprovision reads a whole
// organization, so it allows more re-plans than a one-pair verb.
const planAttempts = 5

// plan is one attempt's lock-free read: audit.Mutation fixes the audit row and
// the events before the transaction, so the locked read must find the same
// rows, or the deprovision is planned again.
type plan struct {
	active  bool
	members *membership.OrgRemoval
	invites *invite.OrgRevocation
	grants  *grantend.Plan
}

// Deprovision runs the one commit, in the auth lock order (membership.Store readPairs): the principal rows
// its rows name FOR KEY SHARE (step 1: members go-api wrote with no auth row,
// inviters of go-api-only invites, the actor of its audit row), the organization row FOR UPDATE
// (step 3, exclusive), Authorize, the pending invites (step 4), every
// membership row (steps 5 and 6), the organization's sessions and go-api
// refresh tokens NOWAIT (step 6a, session.Store.EndOrganizationInTx), the
// suspension, then the revisions inside audit.Commit. It takes no public.users
// row. Every membership and invite writer reaches the organization row before
// any membership or invite row, and takes principal rows only before it, so
// none waits in a cycle with it. A row step 6a finds locked rolls the commit
// back, and after session.BusyAttempts attempts the deprovision is ErrStore
// with nothing written; go-api stops before its purge and a repeat is safe.
func (s *Service) Deprovision(ctx context.Context, in Input) (res *Result, err error) {
	defer func() { s.record(ctx, err) }()
	busy := 0
	for range planAttempts {
		var p plan
		if p, err = s.plan(ctx, in.OrgID); err != nil {
			return nil, err
		}
		res, err = s.once(ctx, in, p)
		if errors.Is(err, errSessionsBusy) {
			if busy++; busy == session.BusyAttempts {
				break
			}
			if werr := session.WaitBusy(ctx, busy); werr != nil {
				return nil, fmt.Errorf("%w: %w", ErrStore, werr)
			}
			continue
		}
		if !errors.Is(err, membership.ErrStateMoved) {
			return res, err
		}
	}
	if errors.Is(err, errSessionsBusy) {
		s.logger.WarnContext(ctx, "orgdeprovision: gave up: a session of the organization stayed locked", "organization_id", in.OrgID.String())
		return nil, fmt.Errorf("%w: %w", ErrStore, err)
	}
	return nil, ErrStateMoved
}

func (s *Service) plan(ctx context.Context, orgID uuid.UUID) (plan, error) {
	var p plan
	o, err := s.orgs.Get(ctx, orgID)
	switch {
	case errors.Is(err, org.ErrNotFound):
		// The locked read decides the refusal after Authorize.
	case err != nil:
		return plan{}, fmt.Errorf("%w: %w", ErrStore, err)
	default:
		p.active = o.Status == org.StatusActive
	}
	if p.members, err = s.members.PlanRemoveAll(ctx, orgID); err != nil {
		return plan{}, storeErr(err)
	}
	if p.invites, err = s.invites.PlanRevokeAll(ctx, orgID); err != nil {
		return plan{}, storeErr(err)
	}
	read := grantend.ReaderFunc(func(ctx context.Context, sql string, args ...any) (grantend.Rows, error) {
		return s.pool.Query(ctx, sql, args...)
	})
	if p.grants, err = grantend.PlanOrg(ctx, read, s.q, orgID); err != nil {
		return plan{}, fmt.Errorf("%w: plan: %w", ErrStore, err)
	}
	return p, nil
}

func (s *Service) once(ctx context.Context, in Input, p plan) (*Result, error) {
	events := append(p.invites.Events(), p.members.Events()...)
	events = append(events, p.grants.Events()...)
	if p.active || len(events) == 0 {
		events = append(events, org.SuspendedEvent(in.OrgID))
	}
	var ended session.Ended
	err := s.commit(ctx, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			// The auth lock order (membership.Store readPairs), step -1: authorize decides under this lock.
			if err := platformrole.LockWritesShared(ctx, tx); err != nil {
				return err
			}
			named := append(p.members.Referenced(), p.invites.Referenced()...)
			if in.Actor.PrincipalID != nil {
				// The audit row names the actor (a foreign key, KEY SHARE at commit).
				named = append(named, *in.Actor.PrincipalID)
			}
			if err := s.members.LockPrincipals(ctx, tx, named); err != nil {
				return err
			}
			active, err := s.orgs.LockForUpdate(ctx, tx, in.OrgID)
			if errors.Is(err, org.ErrNotFound) {
				if aerr := authorize(ctx, tx, in); aerr != nil {
					return aerr
				}
				return ErrNotFound
			}
			if err != nil {
				return err
			}
			if err := authorize(ctx, tx, in); err != nil {
				return err
			}
			if active != p.active {
				return membership.ErrStateMoved
			}
			if err := s.invites.ApplyRevokeAll(ctx, tx, p.invites); err != nil {
				return err
			}
			if err := s.members.ApplyRemoveAll(ctx, tx, p.members); err != nil {
				return err
			}
			if s.sessions != nil {
				if ended, err = s.sessions.EndOrganizationInTx(ctx, tx, in.OrgID); err != nil {
					if errors.Is(err, session.ErrBusy) {
						return errSessionsBusy
					}
					return err
				}
			}
			// Step 6d of the auth lock order: the organization's grant rows, after the memberships and sessions.
			if err := p.grants.Apply(ctx, tx, in.Actor.PrincipalID); err != nil {
				if errors.Is(err, grantend.ErrStateMoved) {
					return membership.ErrStateMoved
				}
				return err
			}
			if !active {
				return nil
			}
			return s.orgs.ApplySuspend(ctx, tx, in.OrgID)
		},
		Audit:    auditFor(in, p.active),
		Also:     p.grants.Audit(grantend.Actor{PrincipalID: in.Actor.PrincipalID, RequestID: in.Actor.RequestID, Caller: in.Actor.Caller}),
		Events:   events,
		Refusals: []error{errSessionsBusy},
	})
	switch {
	case err == nil:
		if ended != (session.Ended{}) {
			s.sessions.RecordEnded(ctx, ended)
			s.logger.InfoContext(ctx, "organization deprovisioned; its sessions ended", "organization_id", in.OrgID.String(),
				"sessions", ended.Sessions, "refresh_tokens", ended.RefreshTokens)
		}
		return &Result{RemovedMemberships: p.members.Len(), RevokedInvites: p.invites.Len(), Suspended: p.active}, nil
	case errors.Is(err, audit.ErrNoStateChange):
		return &Result{}, nil
	}
	for _, refusal := range []error{ErrNotFound, ErrDenied, membership.ErrStateMoved, errSessionsBusy} {
		if errors.Is(err, refusal) {
			return nil, refusal
		}
	}
	s.logger.ErrorContext(ctx, "orgdeprovision: store failure", logging.ErrorAttr(err))
	return nil, fmt.Errorf("%w: %w", ErrStore, err)
}

func authorize(ctx context.Context, tx audit.TxOps, in Input) error {
	if in.Authorize == nil {
		return ErrDenied
	}
	if err := in.Authorize(ctx, tx, in.OrgID); err != nil {
		if errors.Is(err, ErrDenied) {
			return ErrDenied
		}
		return err
	}
	return nil
}

func storeErr(err error) error {
	if errors.Is(err, membership.ErrNotOwned) || errors.Is(err, invite.ErrNotOwned) {
		return ErrNotOwned
	}
	return fmt.Errorf("%w: plan: %w", ErrStore, err)
}

// auditFor is the deprovision's one audit row: the organization and, when it
// was active, its status change. The membership and invite ends are its
// provisioning events.
func auditFor(in Input, wasActive bool) audit.AuditEvent {
	id := in.OrgID.String()
	ev := audit.AuditEvent{EventType: EventDeprovisioned, Outcome: audit.OutcomeAllowed, OrganizationID: &id, RequestID: in.Actor.RequestID,
		Attributes: map[string]any{"target_kind": "organization", "target_id": id, "reason_code": "organization_updated"}}
	if in.Actor.PrincipalID != nil {
		actor := in.Actor.PrincipalID.String()
		ev.ActorPrincipalID = &actor
	}
	in.Actor.Caller.Annotate(ev.Attributes)
	if wasActive {
		ev.Attributes["changed_fields"] = []string{"status", "disabled_at"}
		ev.Attributes["previous_status"] = string(org.StatusActive)
		ev.Attributes["new_status"] = string(org.StatusSuspended)
	}
	return ev
}

func (s *Service) record(ctx context.Context, err error) {
	s.outcomes.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", outcomeLabel(err))))
}

// outcomeLabel maps an error to the closed outcome label set of the counter.
func outcomeLabel(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, ErrNotFound):
		return "not_found"
	case errors.Is(err, ErrDenied):
		return "denied"
	case errors.Is(err, ErrNotOwned):
		return "not_owned"
	case errors.Is(err, ErrStateMoved):
		return "state_moved"
	}
	return "error"
}
