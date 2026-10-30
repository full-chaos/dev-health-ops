package invite

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/membership"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole"
	"github.com/full-chaos/dev-health-ops/internal/auth/signedtoken"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

type Config struct {
	Pool   *pgxpool.Pool
	Schema authschema.ValidatedIdentifier
	// OwnsUsers is AUTH_USERS_OWNED. Off, go-api writes invites and every verb
	// refuses with ErrNotOwned.
	OwnsUsers bool
	// Memberships writes the membership an accept creates, in the accept's commit.
	Memberships *membership.Store
	// Secret signs new tokens; AcceptSecrets verify them (the current secret
	// first). They are go-api's invite secrets, so a token either side minted
	// is accepted by both.
	Secret        string
	AcceptSecrets []string
	// Now is injectable for tests; nil is time.Now.
	Now    func() time.Time
	Logger *slog.Logger
	Meter  metric.Meter
}

// Authorize decides, inside the verb's transaction, whether the actor may
// create or revoke an invite of the organization. A nil Authorize refuses.
type Authorize func(ctx context.Context, tx audit.TxOps, orgID uuid.UUID) error

// PublicAudit is the request part of go-api's audit_logs row the verb writes
// in its commit (the one writer of that row while auth-service owns users).
type PublicAudit struct {
	Description     *string
	RequestMetadata []byte
}

type CreateInput struct {
	OrgID       uuid.UUID
	Email       string
	Role        string
	TTL         *time.Duration
	InvitedBy   uuid.UUID
	Actor       Actor
	Authorize   Authorize
	PublicAudit *PublicAudit
}

// Created is a new invite. Token is shown once and never stored.
type Created struct {
	ID        uuid.UUID
	Token     string
	ExpiresAt time.Time
}

type AcceptInput struct {
	Token       string
	PrincipalID uuid.UUID
	Actor       Actor
	PublicAudit *PublicAudit
	// EnsureOrganization projects the invite's organization into the auth
	// schema when it is missing there (identity.Store.EnsureOrganization). It
	// runs only for a token that names a pending, unexpired invite, before the
	// membership is planned. Nil projects nothing.
	EnsureOrganization func(ctx context.Context, orgID uuid.UUID) error
}

type Accepted struct {
	InviteID     uuid.UUID
	OrgID        uuid.UUID
	MembershipID uuid.UUID
	Role         string
}

type RevokeInput struct {
	OrgID     uuid.UUID
	InviteID  uuid.UUID
	Actor     Actor
	Authorize Authorize
}

type Store struct {
	pool          *pgxpool.Pool
	q             string
	ownsUsers     bool
	members       *membership.Store
	secret        string
	acceptSecrets []string
	now           func() time.Time
	commit        func(context.Context, audit.Mutation) error
	logger        *slog.Logger
	mutations     metric.Int64Counter
}

func New(cfg Config) (*Store, error) {
	switch {
	case cfg.Pool == nil:
		return nil, errors.New("invite: no connection pool")
	case cfg.Schema.String() == "":
		return nil, errors.New("invite: no auth schema")
	case cfg.Memberships == nil:
		return nil, errors.New("invite: no membership store")
	case cfg.Secret == "" || len(cfg.AcceptSecrets) == 0:
		return nil, errors.New("invite: no token secret")
	}
	s := &Store{pool: cfg.Pool, q: authschema.Quote(cfg.Schema), ownsUsers: cfg.OwnsUsers, members: cfg.Memberships,
		secret: cfg.Secret, acceptSecrets: append([]string(nil), cfg.AcceptSecrets...), now: cfg.Now, logger: cfg.Logger}
	s.commit = func(ctx context.Context, m audit.Mutation) error { return audit.Commit(ctx, cfg.Pool, cfg.Schema, m) }
	if s.now == nil {
		s.now = time.Now
	}
	if s.logger == nil {
		s.logger = slog.Default()
	}
	meter := cfg.Meter
	if meter == nil {
		meter = otel.Meter("github.com/full-chaos/dev-health-ops/internal/auth/invite")
	}
	var err error
	if s.mutations, err = meter.Int64Counter("auth.invite.mutations",
		metric.WithDescription("Invite mutations, by verb and outcome.")); err != nil {
		return nil, fmt.Errorf("invite: registering the mutation counter: %w", err)
	}
	return s, nil
}

func (s *Store) record(ctx context.Context, v verb, err error) {
	s.mutations.Add(ctx, 1, metric.WithAttributes(attribute.String("verb", string(v)), attribute.String("outcome", outcomeLabel(err))))
}

// refusals are the store's named outcomes: returned as they are, never wrapped.
var refusals = []error{ErrNotOwned, ErrInviteInvalid, ErrAccountInactive, ErrEmailUnverified, ErrEmailMismatch, ErrEmailInvalid, ErrRoleInvalid,
	ErrTTLInvalid, ErrPendingExists, ErrNotFound, ErrNotPending, ErrAlreadyMember, ErrOrgNotFound, ErrOrgSuspended, ErrOrgConflict,
	ErrInviterNotFound, ErrDenied, membership.ErrStateMoved}

func (s *Store) run(ctx context.Context, v verb, m audit.Mutation) (err error) {
	defer func() {
		if !errors.Is(err, membership.ErrStateMoved) {
			s.record(ctx, v, err)
		}
		if errors.Is(err, audit.ErrNoStateChange) {
			err = nil
		}
	}()
	err = s.commit(ctx, m)
	if err == nil || errors.Is(err, audit.ErrNoStateChange) {
		return err
	}
	for _, refusal := range refusals {
		if errors.Is(err, refusal) {
			return refusal
		}
	}
	s.logger.ErrorContext(ctx, "invite: store failure", "verb", string(v), logging.ErrorAttr(err))
	return fmt.Errorf("%w: %s: %w", ErrStore, v, err)
}

func authorize(ctx context.Context, tx audit.TxOps, auth Authorize, orgID uuid.UUID) error {
	if auth == nil {
		return ErrDenied
	}
	if err := auth(ctx, tx, orgID); err != nil {
		if errors.Is(err, ErrDenied) {
			return ErrDenied
		}
		return err
	}
	return nil
}

// lockActor takes steps 1 and 2 of the auth lock order (membership.Store
// readPairs): the actor's auth.principals and public.users rows FOR SHARE, the
// inviter's FOR KEY SHARE (the invite rows name it: a foreign key), one row per
// statement in id order. Authorize then reads the actor's account and role as
// they stay until the commit: a demotion, a disable or a superuser change of
// the actor commits before it or waits for the verb. A missing row is no
// refusal here (Authorize decides on the actor).
func (s *Store) lockActor(ctx context.Context, tx audit.TxOps, actor, inviter *uuid.UUID) error {
	return s.members.LockRows(ctx, tx, membership.LockOrder(nil, inviter, actor))
}

// lockOrg takes the organization row FOR SHARE (step 3 of the membership lock
// order). A missing or suspended organization is named only to an actor
// Authorize accepts; any other actor gets ErrDenied.
func (s *Store) lockOrg(ctx context.Context, tx audit.TxOps, orgID uuid.UUID, auth Authorize) error {
	var active bool
	err := tx.QueryRow(ctx, `SELECT disabled_at IS NULL FROM `+s.q+`.organizations WHERE id = $1 FOR SHARE`, orgID).Scan(&active)
	var lookup error
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		lookup = ErrOrgNotFound
	case err != nil:
		return err
	case !active:
		lookup = ErrOrgSuspended
	}
	if aerr := authorize(ctx, tx, auth, orgID); aerr != nil {
		return aerr
	}
	return lookup
}

// publicAuditRow is go-api's audit_logs row (internal/api/audit.PGWriter shape).
func (s *Store) publicAuditRow(ctx context.Context, tx audit.TxOps, orgID uuid.UUID, userID *uuid.UUID, action, resourceID string,
	description string, changes *pyjson.Object, pa *PublicAudit, at time.Time) error {
	encoded, err := pyjson.Marshal(changes)
	if err != nil {
		return err
	}
	metadata := []byte("{}")
	if pa != nil && len(pa.RequestMetadata) > 0 {
		metadata = pa.RequestMetadata
	}
	if pa != nil && pa.Description != nil {
		description = *pa.Description
	}
	_, err = tx.Exec(ctx, `INSERT INTO public.audit_logs
	(id, org_id, user_id, action, resource_type, resource_id, description, changes, request_metadata, status, created_at)
VALUES ($1, $2, $3, $4, 'membership', $5, $6, $7::json, $8::json, 'success', $9)`,
		uuid.New(), orgID, userID, action, resourceID, description, string(encoded), string(metadata), at)
	return err
}

// Create mints an invite: both invite rows (the token's SHA-256 digest only),
// the audit row, the event and go-api's member_invited audit_logs row, in one
// commit. One pending, unexpired invite per organization and address.
func (s *Store) Create(ctx context.Context, in CreateInput) (*Created, error) {
	if !s.ownsUsers {
		s.record(ctx, verbCreate, ErrNotOwned)
		return nil, ErrNotOwned
	}
	email, err := normalizeEmail(in.Email)
	if err == nil && !identity.IsOrganizationRole(in.Role) {
		err = ErrRoleInvalid
	}
	var ttl time.Duration
	if err == nil {
		ttl, err = lifetime(in.TTL)
	}
	if err != nil {
		s.record(ctx, verbCreate, err)
		return nil, err
	}
	id := uuid.New()
	token, digest := signedtoken.Build(id, s.secret)
	now := s.now().UTC()
	expires := now.Add(ttl)
	err = s.run(ctx, verbCreate, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			// The auth lock order (membership.Store readPairs), step -1: Authorize decides under this lock.
			if err := platformrole.LockWritesShared(ctx, tx); err != nil {
				return err
			}
			if err := s.lockActor(ctx, tx, in.Actor.PrincipalID, &in.InvitedBy); err != nil {
				return err
			}
			if err := s.lockOrg(ctx, tx, in.OrgID, in.Authorize); err != nil {
				return err
			}
			var inviterKnown bool
			if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM `+s.q+`.principals WHERE id = $1)
  AND EXISTS (SELECT 1 FROM public.users WHERE id = $1)`, in.InvitedBy).Scan(&inviterKnown); err != nil {
				return err
			}
			if !inviterKnown {
				return ErrInviterNotFound
			}
			// One create per organization and address at a time, so the pending check below holds.
			if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, "invite:"+in.OrgID.String()+":"+email); err != nil {
				return err
			}
			var pending bool
			if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM public.org_invites
  WHERE org_id = $1 AND lower(email) = $2 AND status = 'pending' AND expires_at >= $3)
  OR EXISTS (SELECT 1 FROM `+s.q+`.invites
  WHERE organization_id = $1 AND email_normalized = $2 AND status = 'pending' AND expires_at >= $3)`, in.OrgID, email, now).Scan(&pending); err != nil {
				return err
			}
			if pending {
				return ErrPendingExists
			}
			if _, err := tx.Exec(ctx, `INSERT INTO `+s.q+`.invites
	(id, organization_id, email_normalized, role, token_hash, invited_by, status, expires_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, 'pending', $7, $8, $8)`, id, in.OrgID, email, in.Role, digest, in.InvitedBy, expires, now); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `INSERT INTO public.org_invites
	(id, org_id, email, role, token_hash, invited_by_id, status, expires_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, 'pending', $7, $8, $8)`, id, in.OrgID, email, in.Role, digest, in.InvitedBy, expires, now); err != nil {
				return err
			}
			changes := pyjson.NewObject()
			changes.Set("email", email)
			changes.Set("role", in.Role)
			changes.Set("status", string(StatusPending))
			inviter := in.InvitedBy
			return s.publicAuditRow(ctx, tx, in.OrgID, &inviter, "member_invited", id.String(), "Organization invite created", changes, in.PublicAudit, now)
		},
		Audit:  auditFor(verbCreate, in.OrgID, id, nil, in.Actor),
		Events: []audit.OutboxEvent{eventFor(verbCreate, id)},
	})
	if err != nil {
		return nil, err
	}
	return &Created{ID: id, Token: token, ExpiresAt: expires}, nil
}

// planAttempts bounds the plan-then-lock loop of an accept.
const planAttempts = 3

// inviteRow is one invite as an accept reads it, from auth.invites or, for an
// invite go-api made before, from public.org_invites only.
type inviteRow struct {
	row
	inviter   *uuid.UUID
	createdAt time.Time
	inAuth    bool
}

const readPublicInvite = `SELECT id, org_id, email, role, status, expires_at, invited_by_id, created_at FROM public.org_invites WHERE token_hash = $1`

func (s *Store) readInvite(ctx context.Context, rowOf func(string, ...any) pgx.Row, digest string, lock bool) (inviteRow, error) {
	forUpdate := ""
	if lock {
		forUpdate = " FOR UPDATE"
	}
	var r inviteRow
	scan := func(row pgx.Row) error {
		var status string
		err := row.Scan(&r.id, &r.orgID, &r.email, &r.role, &status, &r.expiresAt, &r.inviter, &r.createdAt)
		r.status = Status(status)
		return err
	}
	err := scan(rowOf(`SELECT id, organization_id, email_normalized, role, status, expires_at, invited_by, created_at FROM `+s.q+
		`.invites WHERE token_hash = $1`+forUpdate, digest))
	if err == nil {
		r.found, r.inAuth = true, true
		if lock {
			// Step 4 of the auth lock order holds go-api's row too, before the
			// membership rows: the accept updates it last.
			var id uuid.UUID
			if err := rowOf(`SELECT id FROM public.org_invites WHERE id = $1 FOR UPDATE`, r.id).Scan(&id); err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return r, err
			}
		}
		return r, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return r, err
	}
	err = scan(rowOf(readPublicInvite+forUpdate, digest))
	if errors.Is(err, pgx.ErrNoRows) {
		return inviteRow{}, nil
	}
	if err != nil {
		return r, err
	}
	r.found = true
	return r, nil
}

// Accept consumes a token once: the membership (role and inviter from the
// invite) and both invite rows marked accepted, the audit row, both events and
// go-api's member_joined audit_logs row, in one commit. An unusable token is
// ErrInviteInvalid and nothing else.
func (s *Store) Accept(ctx context.Context, in AcceptInput) (*Accepted, error) {
	if !s.ownsUsers {
		s.record(ctx, verbAccept, ErrNotOwned)
		return nil, ErrNotOwned
	}
	if ok, err := signedtoken.ValidAny(in.Token, s.acceptSecrets); err != nil || !ok {
		s.record(ctx, verbAccept, ErrInviteInvalid)
		return nil, ErrInviteInvalid
	}
	digest := signedtoken.Hash(in.Token)
	var err error
	for range planAttempts {
		var got *Accepted
		got, err = s.acceptOnce(ctx, in, digest)
		if !errors.Is(err, membership.ErrStateMoved) {
			return got, err
		}
	}
	return nil, err
}

func (s *Store) acceptOnce(ctx context.Context, in AcceptInput, digest string) (*Accepted, error) {
	now := s.now().UTC()
	planned, err := s.readInvite(ctx, func(sql string, args ...any) pgx.Row { return s.pool.QueryRow(ctx, sql, args...) }, digest, false)
	if err != nil {
		s.logger.ErrorContext(ctx, "invite: read failure", logging.ErrorAttr(err))
		return nil, fmt.Errorf("%w: read: %w", ErrStore, err)
	}
	if err := checkConsumable(planned.row, now); err != nil {
		s.record(ctx, verbAccept, err)
		return nil, err
	}
	if in.EnsureOrganization != nil {
		if err := s.ensureOrganization(ctx, in.EnsureOrganization, planned.orgID); err != nil {
			return nil, err
		}
	}
	change := membership.Change{OrgID: planned.orgID, PrincipalID: in.PrincipalID, Role: planned.role, InvitedBy: planned.inviter,
		Actor: membershipActor(in.Actor)}
	plan, err := s.members.PlanAdd(ctx, change)
	if errors.Is(err, membership.ErrInviterNotFound) {
		// The inviter left (go-api's ON DELETE SET NULL): the membership records none.
		change.InvitedBy = nil
		plan, err = s.members.PlanAdd(ctx, change)
	}
	if err != nil {
		err = acceptRefusal(err)
		if errors.Is(err, ErrAlreadyMember) {
			// A membership commits with its invite's accept, so a member who
			// holds a used token sees the used token, not the membership.
			again, rerr := s.readInvite(ctx, func(sql string, args ...any) pgx.Row { return s.pool.QueryRow(ctx, sql, args...) }, digest, false)
			if rerr == nil && checkConsumable(again.row, now) != nil {
				err = ErrInviteInvalid
			}
		}
		s.record(ctx, verbAccept, err)
		return nil, err
	}
	subject := in.PrincipalID
	err = s.run(ctx, verbAccept, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			step4 := func() error {
				locked, err := s.readInvite(ctx, func(sql string, args ...any) pgx.Row { return tx.QueryRow(ctx, sql, args...) }, digest, true)
				if err != nil {
					return err
				}
				if err := checkConsumable(locked.row, now); err != nil {
					return err
				}
				if locked.id != planned.id || locked.orgID != planned.orgID || locked.role != planned.role {
					return membership.ErrStateMoved
				}
				// The account's users row is held FOR UPDATE since step 2, so a
				// deactivation either committed before this read or waits for the accept.
				var email string
				var verified, active bool
				if err := tx.QueryRow(ctx, `SELECT email, is_verified, COALESCE(is_active, false) FROM public.users WHERE id = $1`, in.PrincipalID).
					Scan(&email, &verified, &active); err != nil {
					return err
				}
				if !active {
					return ErrAccountInactive
				}
				if err := emailRefusal(locked.email, email, verified); err != nil {
					return err
				}
				if locked.inAuth {
					return nil
				}
				// An invite go-api made before: take it over into auth.invites.
				// invited_by is the inviter the plan locked at step 1, or NULL when
				// the plan had none: never an id that no step-1 lock holds.
				_, err = tx.Exec(ctx, `INSERT INTO `+s.q+`.invites
	(id, organization_id, email_normalized, role, token_hash, invited_by, status, expires_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, (SELECT p.id FROM `+s.q+`.principals p WHERE p.id = $6), 'pending', $7, $8, $9)`,
					locked.id, locked.orgID, locked.email, locked.role, digest, change.InvitedBy, locked.expiresAt, locked.createdAt, now)
				return err
			}
			if err := s.members.ApplyAdd(ctx, tx, plan, step4); err != nil {
				return acceptRefusal(err)
			}
			if _, err := tx.Exec(ctx, `UPDATE `+s.q+`.invites SET status = 'accepted', accepted_at = $2, accepted_principal_id = $3, updated_at = $2
WHERE id = $1`, planned.id, now, in.PrincipalID); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `UPDATE public.org_invites SET status = 'accepted', accepted_at = $2, updated_at = $2 WHERE id = $1`,
				planned.id, now); err != nil {
				return err
			}
			changes := pyjson.NewObject()
			changes.Set("invite_id", planned.id.String())
			changes.Set("user_id", in.PrincipalID.String())
			changes.Set("role", planned.role)
			return s.publicAuditRow(ctx, tx, planned.orgID, &subject, "member_joined", plan.MembershipID().String(), "Invite accepted",
				changes, in.PublicAudit, now)
		},
		Audit:  auditFor(verbAccept, planned.orgID, planned.id, &subject, in.Actor),
		Events: []audit.OutboxEvent{eventFor(verbAccept, planned.id), plan.Event()},
	})
	if err != nil {
		return nil, err
	}
	return &Accepted{InviteID: planned.id, OrgID: planned.orgID, MembershipID: plan.MembershipID(), Role: planned.role}, nil
}

// ensureOrganization runs the accept's projection of the invite's
// organization. An organization go-api does not have either makes the invite
// unusable (ErrInviteInvalid, the one refusal); a slug collision is
// ErrOrgConflict; any other failure is ErrStore.
func (s *Store) ensureOrganization(ctx context.Context, ensure func(context.Context, uuid.UUID) error, orgID uuid.UUID) error {
	err := ensure(ctx, orgID)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, identity.ErrOrganizationNotFound):
		err = ErrInviteInvalid
	case errors.Is(err, identity.ErrOrganizationSlugCollision):
		err = ErrOrgConflict
	default:
		s.logger.ErrorContext(ctx, "invite: organization projection failure", logging.ErrorAttr(err))
		err = fmt.Errorf("%w: ensure organization: %w", ErrStore, err)
	}
	s.record(ctx, verbAccept, err)
	return err
}

// acceptRefusal maps a membership refusal to the invite's: an organization
// gone or suspended makes the invite unusable, with the one refusal.
func acceptRefusal(err error) error {
	switch {
	case errors.Is(err, membership.ErrAlreadyMember), errors.Is(err, membership.ErrSuspended):
		return ErrAlreadyMember
	case errors.Is(err, membership.ErrOrgNotFound), errors.Is(err, membership.ErrOrgSuspended):
		return ErrInviteInvalid
	case errors.Is(err, membership.ErrNotOwned):
		return ErrNotOwned
	}
	return err
}

// Revoke ends a pending invite: both rows revoked, in one commit. A repeat is
// a no-op; an accepted invite stays accepted (ErrNotPending).
func (s *Store) Revoke(ctx context.Context, in RevokeInput) error {
	if !s.ownsUsers {
		s.record(ctx, verbRevoke, ErrNotOwned)
		return ErrNotOwned
	}
	return s.run(ctx, verbRevoke, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			// The auth lock order (membership.Store readPairs), step -1: Authorize decides under this lock.
			if err := platformrole.LockWritesShared(ctx, tx); err != nil {
				return err
			}
			// The inviter, read before any lock (every invite has its public row),
			// is locked with the actor: a take-over below names it in auth.invites.
			var inviter *uuid.UUID
			if err := tx.QueryRow(ctx, `SELECT invited_by_id FROM public.org_invites WHERE id = $1 AND org_id = $2`, in.InviteID, in.OrgID).
				Scan(&inviter); err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return err
			}
			if err := s.lockActor(ctx, tx, in.Actor.PrincipalID, inviter); err != nil {
				return err
			}
			if err := s.lockOrg(ctx, tx, in.OrgID, in.Authorize); err != nil {
				return err
			}
			now := s.now().UTC()
			var status string
			var expiresAt time.Time
			inAuth := true
			err := tx.QueryRow(ctx, `SELECT status, expires_at FROM `+s.q+`.invites WHERE id = $1 AND organization_id = $2 FOR UPDATE`, in.InviteID, in.OrgID).
				Scan(&status, &expiresAt)
			if errors.Is(err, pgx.ErrNoRows) {
				inAuth = false
				err = tx.QueryRow(ctx, `SELECT status, expires_at FROM public.org_invites WHERE id = $1 AND org_id = $2 FOR UPDATE`, in.InviteID, in.OrgID).
					Scan(&status, &expiresAt)
			}
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			if err != nil {
				return err
			}
			noop, err := decideRevoke(Status(status), expiresAt, now)
			if err != nil || noop {
				return err
			}
			// A take-over writes as invited_by the inviter locked above, or NULL:
			// never an id read after the locks, so no lock is taken after step 3.
			if inAuth {
				if _, err := tx.Exec(ctx, `UPDATE `+s.q+`.invites SET status = 'revoked', updated_at = $2 WHERE id = $1`, in.InviteID, now); err != nil {
					return err
				}
			} else if _, err := tx.Exec(ctx, `INSERT INTO `+s.q+`.invites
	(id, organization_id, email_normalized, role, token_hash, invited_by, status, expires_at, created_at, updated_at)
SELECT p.id, p.org_id, p.email, p.role, p.token_hash, (SELECT a.id FROM `+s.q+`.principals a WHERE a.id = $3::uuid AND a.id = p.invited_by_id),
       'revoked', p.expires_at, p.created_at, $2
FROM public.org_invites p WHERE p.id = $1`, in.InviteID, now, inviter); err != nil {
				return err
			}
			_, err = tx.Exec(ctx, `UPDATE public.org_invites SET status = 'revoked', updated_at = $2 WHERE id = $1`, in.InviteID, now)
			return err
		},
		Audit:  auditFor(verbRevoke, in.OrgID, in.InviteID, nil, in.Actor),
		Events: []audit.OutboxEvent{eventFor(verbRevoke, in.InviteID)},
	})
}

// membershipActor is the actor of the membership an accept adds: the accept's
// own principal, request and caller.
func membershipActor(a Actor) membership.Actor {
	return membership.Actor{PrincipalID: a.PrincipalID, RequestID: a.RequestID, Caller: a.Caller}
}
