package identity

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/registerorg"
	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

var (
	// ErrOrganizationMember refuses an onboarding of a principal that holds a
	// membership.
	ErrOrganizationMember = errors.New("identity: the principal already has a membership")
	// ErrAccountInactive refuses an onboarding of a disabled account.
	ErrAccountInactive = errors.New("identity: the account is not active")
	// ErrOrganizationDenied is the refusal of CreateOrganizationRequest.Authorize.
	ErrOrganizationDenied = errors.New("identity: the actor may not create an organization")
)

// OnboardOrganizationRequest creates Principal's first organization.
type OnboardOrganizationRequest struct {
	Principal       uuid.UUID
	Name            string
	RequestID       *string
	Caller          audit.Caller
	Description     *string
	RequestMetadata []byte
}

// CreateOrganizationRequest creates an organization for a platform actor.
// Owner nil creates it with no member. LockRoles takes platformrole's write lock
// shared (identity cannot link that package); Authorize decides under the locks.
type CreateOrganizationRequest struct {
	Actor       uuid.UUID
	RequestID   *string
	Caller      audit.Caller
	Slug, Name  string
	Description *string
	Settings    []byte
	Tier        string
	ManagedBy   string
	Owner       *uuid.UUID
	LockRoles   func(ctx context.Context, tx audit.TxOps) error
	Authorize   func(ctx context.Context, tx audit.TxOps) error
}

// BornOrganization is an organization a verb created.
type BornOrganization struct {
	ID           uuid.UUID
	MembershipID *uuid.UUID
	Slug         string
}

// organizationBirth is one new organization. Each birth writes go-api's row, the
// auth row and the owner membership of both schemas in one commit, because with
// AUTH_ORGS_OWNED the projection creates no auth row (ProjectOrganization).
type organizationBirth struct {
	ID, MembershipID uuid.UUID
	Slug, Name       string
	Description      *string
	Settings         []byte
	Tier, ManagedBy  string
	Owner            *uuid.UUID
}

func newOrganizationBirth(slug, name string, owner *uuid.UUID) organizationBirth {
	return organizationBirth{ID: uuid.New(), MembershipID: uuid.New(), Slug: slug, Name: name, Settings: []byte("{}"),
		Tier: "community", ManagedBy: "stripe", Owner: owner}
}

// freeOrganizationSlug returns slug, or slug with one random suffix when either
// schema holds it in any case, as go-api's slug read folds case. A purge leaves the suspended auth row, so a slug can be taken
// in auth alone. It runs before the commit: the outbox event carries the slug.
func (s *Store) freeOrganizationSlug(ctx context.Context, slug string) (string, error) {
	held, err := s.organizationSlugHeld(ctx, slug)
	if err != nil || !held {
		return slug, err
	}
	suffix := make([]byte, 4)
	if _, err := rand.Read(suffix); err != nil {
		return "", err
	}
	next := slug + "-" + hex.EncodeToString(suffix)
	if held, err = s.organizationSlugHeld(ctx, next); err != nil {
		return "", err
	}
	if held {
		return "", ErrOrganizationSlugCollision
	}
	return next, nil
}

func (s *Store) organizationSlugHeld(ctx context.Context, slug string) (bool, error) {
	var held bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM public.organizations WHERE lower(slug) = $1)
  OR EXISTS (SELECT 1 FROM `+s.q+`.organizations WHERE lower(slug) = $1)`, pythonparity.Lower(slug)).Scan(&held)
	return held, err
}

// insertOrganizationBirth writes the birth inside the caller's audit.Commit. A
// slug taken since freeOrganizationSlug is ErrOrganizationSlugCollision.
func (s *Store) insertOrganizationBirth(ctx context.Context, tx audit.TxOps, b organizationBirth) error {
	_, err := tx.Exec(ctx, `INSERT INTO public.organizations
	(id, slug, name, description, settings, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5::json, $6, $7, true, now(), now())`,
		b.ID, b.Slug, b.Name, b.Description, string(b.Settings), b.Tier, b.ManagedBy)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return ErrOrganizationSlugCollision
	}
	if err != nil {
		return err
	}
	var inserted uuid.UUID
	err = tx.QueryRow(ctx, `INSERT INTO `+s.q+`.organizations (id, slug, name) VALUES ($1, $2, $3)
ON CONFLICT DO NOTHING RETURNING id`, b.ID, b.Slug, b.Name).Scan(&inserted)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrOrganizationSlugCollision
	}
	if err != nil || b.Owner == nil {
		return err
	}
	return s.insertJoinMembership(ctx, tx, membershipJoin{ID: b.MembershipID, Principal: *b.Owner, OrgID: b.ID, Role: "owner"})
}

// birthRecords are the audit rows and outbox events of a birth.
func (s *Store) birthRecords(ctx context.Context, b organizationBirth, actor *string, requestID *string, caller audit.Caller) ([]audit.AuditEvent, []audit.OutboxEvent) {
	org := b.ID.String()
	created := organizationAudit("auth.organization.created", "organization_created", org)
	created.ActorPrincipalID, created.RequestID = actor, requestID
	created.Attributes["new_status"] = "active"
	caller.Annotate(created.Attributes)
	records := []audit.AuditEvent{created}
	events := []audit.OutboxEvent{s.organizationEvent(ctx, org, "organization.created", orgState{slug: b.Slug, name: b.Name, active: true}, true)}
	if b.Owner == nil {
		return records, events
	}
	subject, target := b.Owner.String(), b.MembershipID.String()
	added := audit.AuditEvent{EventType: "auth.membership.added", Outcome: audit.OutcomeAllowed, OrganizationID: &org, SubjectPrincipalID: &subject,
		ActorPrincipalID: actor, RequestID: requestID, Attributes: map[string]any{"reason_code": "membership_added", "target_kind": "membership",
			"target_id": target, "new_status": "active"}}
	caller.Annotate(added.Attributes)
	return append(records, added), append(events, membershipEvent(b.MembershipID, "membership.activated",
		map[string]any{"status": "active", "role_id": "rol_owner"}))
}

var birthRefusals = []error{ErrOrganizationMember, ErrAccountInactive, ErrUserNotFound, ErrOrganizationSlugCollision, ErrOrganizationDenied}

func (s *Store) birthResult(ctx context.Context, what string, b organizationBirth, err error) (*BornOrganization, error) {
	if err == nil {
		born := &BornOrganization{ID: b.ID, Slug: b.Slug}
		if b.Owner != nil {
			born.MembershipID = &b.MembershipID
		}
		return born, nil
	}
	for _, refusal := range birthRefusals {
		if errors.Is(err, refusal) {
			return nil, refusal
		}
	}
	return nil, s.storeErr(ctx, what, err)
}

// OnboardOrganization creates the first organization of a principal with no
// membership, owned by it, and go-api's audit_logs row, in one commit. It locks
// the principal, then public.users (the auth lock order), so two onboardings or
// an onboarding and an invite accept of one account run one after the other.
func (s *Store) OnboardOrganization(ctx context.Context, req OnboardOrganizationRequest) (*BornOrganization, error) {
	if !s.ownsUsers {
		return nil, ErrSignupNotAvailable
	}
	slug, err := s.freeOrganizationSlug(ctx, registerorg.OnboardSlug(req.Name, req.Principal))
	if err != nil {
		return s.birthResult(ctx, "read the organization slug", organizationBirth{}, err)
	}
	b := newOrganizationBirth(slug, req.Name, &req.Principal)
	actor := req.Principal.String()
	records, events := s.birthRecords(ctx, b, &actor, req.RequestID, req.Caller)
	err = audit.Commit(ctx, s.pool, s.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			if err := s.lockOrgless(ctx, tx, req.Principal); err != nil {
				return err
			}
			if err := s.insertOrganizationBirth(ctx, tx, b); err != nil {
				return err
			}
			return insertOrganizationCreatedLog(ctx, tx, b, req)
		},
		Audit: records[0], Also: records[1:], Events: events, Refusals: birthRefusals,
	})
	return s.birthResult(ctx, "onboard an organization", b, err)
}

// lockOrgless takes the principal and public.users rows and refuses an account
// that is disabled or that holds a membership in either schema.
func (s *Store) lockOrgless(ctx context.Context, tx audit.TxOps, principal uuid.UUID) error {
	var active, publicActive bool
	err := tx.QueryRow(ctx, `SELECT disabled_at IS NULL FROM `+s.q+`.principals WHERE id = $1 FOR UPDATE`, principal).Scan(&active)
	if err == nil {
		err = tx.QueryRow(ctx, `SELECT is_active FROM public.users WHERE id = $1 FOR UPDATE`, principal).Scan(&publicActive)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrUserNotFound
	}
	if err != nil {
		return err
	}
	if !active || !publicActive {
		return ErrAccountInactive
	}
	var public, auth bool
	if err := tx.QueryRow(ctx, `SELECT
  EXISTS (SELECT 1 FROM public.memberships WHERE user_id = $1),
  EXISTS (SELECT 1 FROM `+s.q+`.memberships WHERE principal_id = $1 AND status <> 'removed')`, principal).Scan(&public, &auth); err != nil {
		return err
	}
	if public || auth {
		return ErrOrganizationMember
	}
	return nil
}

// insertOrganizationCreatedLog is go-api's audit_logs row of an onboarding.
func insertOrganizationCreatedLog(ctx context.Context, tx audit.TxOps, b organizationBirth, req OnboardOrganizationRequest) error {
	changes := pyjson.NewObject()
	changes.Set("organization_name", b.Name)
	encoded, err := pyjson.Marshal(changes)
	if err != nil {
		return err
	}
	metadata := []byte("{}")
	if len(req.RequestMetadata) > 0 {
		metadata = req.RequestMetadata
	}
	description := "Organization created during onboarding"
	if req.Description != nil {
		description = *req.Description
	}
	_, err = tx.Exec(ctx, `INSERT INTO public.audit_logs
	(id, org_id, user_id, action, resource_type, resource_id, description, changes, request_metadata, status, created_at)
VALUES ($1, $2, $3, 'create', 'organization', $4, $5, $6::json, $7::json, 'success', now())`,
		uuid.New(), b.ID, req.Principal, b.ID.String(), description, string(encoded), string(metadata))
	return err
}

// CreateOrganization creates an organization for a platform actor, with its
// owner when one is named, in one commit. Authorize reads the platform roles,
// so the role write lock comes first (step -1), then the principal and
// public.users rows of the actor and the owner.
func (s *Store) CreateOrganization(ctx context.Context, req CreateOrganizationRequest) (*BornOrganization, error) {
	if !s.ownsUsers {
		return nil, ErrSignupNotAvailable
	}
	if req.Authorize == nil || req.LockRoles == nil {
		return nil, ErrOrganizationDenied
	}
	slug, err := s.freeOrganizationSlug(ctx, req.Slug)
	if err != nil {
		return s.birthResult(ctx, "read the organization slug", organizationBirth{}, err)
	}
	b := newOrganizationBirth(slug, req.Name, req.Owner)
	b.Description, b.Tier, b.ManagedBy = req.Description, req.Tier, req.ManagedBy
	if len(req.Settings) > 0 {
		b.Settings = req.Settings
	}
	actor := req.Actor.String()
	records, events := s.birthRecords(ctx, b, &actor, req.RequestID, req.Caller)
	err = audit.Commit(ctx, s.pool, s.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			if err := req.LockRoles(ctx, tx); err != nil {
				return err
			}
			if err := s.lockCreator(ctx, tx, req.Actor, req.Owner); err != nil {
				return err
			}
			if err := req.Authorize(ctx, tx); err != nil {
				return err
			}
			return s.insertOrganizationBirth(ctx, tx, b)
		},
		Audit: records[0], Also: records[1:], Events: events, Refusals: birthRefusals,
	})
	return s.birthResult(ctx, "create an organization", b, err)
}

// lockCreator takes the rows in id order: the owner FOR UPDATE, the actor FOR
// SHARE. A missing owner is ErrUserNotFound; a missing actor is Authorize's.
func (s *Store) lockCreator(ctx context.Context, tx audit.TxOps, actor uuid.UUID, owner *uuid.UUID) error {
	type lock struct {
		id   uuid.UUID
		mode string
	}
	locks := []lock{{actor, " FOR SHARE"}}
	if owner != nil {
		if *owner == actor {
			locks[0].mode = " FOR UPDATE"
		} else {
			locks = append(locks, lock{*owner, " FOR UPDATE"})
		}
	}
	slices.SortFunc(locks, func(a, b lock) int { return bytes.Compare(a.id[:], b.id[:]) })
	for _, table := range []string{s.q + ".principals", "public.users"} {
		for _, l := range locks {
			var id uuid.UUID
			err := tx.QueryRow(ctx, fmt.Sprintf(`SELECT id FROM %s WHERE id = $1%s`, table, l.mode), l.id).Scan(&id)
			if errors.Is(err, pgx.ErrNoRows) {
				if owner != nil && l.id == *owner {
					return ErrUserNotFound
				}
				continue
			}
			if err != nil {
				return err
			}
		}
	}
	return nil
}
