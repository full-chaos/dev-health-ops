package session

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/clientaddr"
	"github.com/full-chaos/dev-health-ops/internal/auth/delegationend"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
)

// ImpersonateAction is the catalog action that lets a platform role start a
// delegated session.
const ImpersonateAction = "admin.impersonate"

// MaxDelegation is a delegated session's whole life (ACP-ADR-03).
const MaxDelegation = 15 * time.Minute

// maxDelegationReason is the longest reason, in characters; sessions_delegation_shape holds
// the same bound.
const maxDelegationReason = 500

var (
	// ErrNotOwned refuses a delegation while auth-service does not own the
	// platform roles and the users. The locks of Start need the users-owned
	// grants, and the authority it checks is the platform role assignment.
	ErrNotOwned = errors.New("session: auth-service does not own platform roles and users")
	// ErrActorNotAuthorized refuses an actor that does not hold the action now.
	ErrActorNotAuthorized = errors.New("session: the actor may not start a delegation")
	// ErrActorUnavailable refuses an actor that is disabled or not a user.
	ErrActorUnavailable = errors.New("session: the actor is unavailable")
	// ErrBaseNotLive refuses a base session that is unknown, revoked or expired.
	ErrBaseNotLive = errors.New("session: the base session is not live")
	// ErrNestedDelegation refuses a base session that is itself delegated.
	ErrNestedDelegation = errors.New("session: a delegated session cannot delegate")
	// ErrTargetIsActor refuses a delegation to oneself.
	ErrTargetIsActor = errors.New("session: the target is the actor")
	// ErrTargetUnavailable refuses a target that is unknown, disabled, inactive
	// or not a user.
	ErrTargetUnavailable = errors.New("session: the target is unavailable")
	// ErrTargetPlatformRole refuses a target that holds a platform role.
	ErrTargetPlatformRole = errors.New("session: the target holds a platform role")
	// ErrTargetNotMember refuses a target with no membership in the organization.
	ErrTargetNotMember = errors.New("session: the target is not a member of the organization")
	// ErrTargetRoleUnknown refuses a target whose role is not an organization
	// role of the catalog, or one that gives a delegation no action.
	ErrTargetRoleUnknown = errors.New("session: the target's role permits no delegated action")
	// ErrOrganizationUnavailable refuses an unknown or disabled organization.
	ErrOrganizationUnavailable = errors.New("session: the organization is unavailable")
	// ErrInvalidReason refuses a blank, oversized or unstorable reason.
	ErrInvalidReason = errors.New("session: a delegation needs a reason of 1 to 500 characters")
	// ErrNotDelegated refuses EndDelegation of a session that is not delegated.
	ErrNotDelegated = errors.New("session: the session is not delegated")
)

var delegationRefusals = []error{
	ErrActorNotAuthorized, ErrActorUnavailable, ErrBaseNotLive, ErrNestedDelegation, ErrTargetIsActor, ErrTargetUnavailable,
	ErrTargetPlatformRole, ErrTargetNotMember, ErrTargetRoleUnknown, ErrOrganizationUnavailable,
}

// Outcomes of auth.session.delegations. No "ended": a cascade ends a delegation
// in internal/auth/delegationend, which has no meter, and a partial count would
// mislead. The delegation_ended audit row is the record of an end.
const (
	delegationStarted = "started"
	delegationRefused = "refused"
)

// StartInput is one delegation request. It names no actor: the actor is the
// principal of the base session (G-4).
type StartInput struct {
	BaseSessionID     uuid.UUID
	TargetPrincipalID uuid.UUID
	OrganizationID    uuid.UUID
	Reason            string
}

// StartDelegation starts a delegated session of the base session's principal
// as the target, in the organization, in one audit.Commit transaction. The row
// is the target's session with the actor on it, and no refresh credential: it
// ends at expires_at at the latest, which is the first of 15 minutes, the base
// session's effective expiry and the actor's role assignment.
//
// Locks follow the auth lock order (membership.Store readPairs). Prior rows of
// the actor end first, expired ones too, so an actor runs one delegation. A
// base or prior row another writer holds is ErrBusy, retried BusyAttempts times.
func (s *Store) StartDelegation(ctx context.Context, in StartInput) (Record, error) {
	record, err := s.startDelegation(ctx, in)
	switch {
	case err == nil:
		s.delegations.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", delegationStarted)))
	case isDelegationRefusal(err):
		s.delegations.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", delegationRefused)))
	}
	return record, err
}

func isDelegationRefusal(err error) bool {
	if errors.Is(err, ErrNotOwned) || errors.Is(err, ErrInvalidReason) {
		return true
	}
	for _, refusal := range delegationRefusals {
		if errors.Is(err, refusal) {
			return true
		}
	}
	return false
}

func (s *Store) startDelegation(ctx context.Context, in StartInput) (Record, error) {
	if !s.platformOwned || !s.usersOwned {
		return Record{}, ErrNotOwned
	}
	reason, ok := delegationReason(in.Reason)
	if !ok {
		return Record{}, ErrInvalidReason
	}
	var actorText string
	err := s.pool.QueryRow(ctx, `SELECT principal_id::text FROM `+s.q+`.sessions WHERE id = $1`, in.BaseSessionID.String()).Scan(&actorText)
	if errors.Is(err, pgx.ErrNoRows) {
		return Record{}, ErrBaseNotLive
	}
	if err != nil {
		return Record{}, fmt.Errorf("%w: reading the base session: %w", ErrStore, err)
	}
	actor, err := uuid.Parse(actorText)
	if err != nil {
		return Record{}, fmt.Errorf("%w: reading the base session: %w", ErrStore, err)
	}
	if actor == in.TargetPrincipalID {
		return Record{}, ErrTargetIsActor
	}
	for attempt := 1; ; attempt++ {
		record, err := s.startOnce(ctx, in, actor, reason)
		if !errors.Is(err, ErrBusy) || attempt >= BusyAttempts {
			return record, err
		}
		if err := WaitBusy(ctx, attempt); err != nil {
			return Record{}, err
		}
	}
}

func delegationReason(raw string) (string, bool) {
	reason := strings.TrimSpace(raw)
	if !utf8.ValidString(reason) || strings.ContainsRune(reason, 0) {
		return "", false
	}
	n := utf8.RuneCountInString(reason)
	return reason, n >= 1 && n <= maxDelegationReason
}

func (s *Store) startOnce(ctx context.Context, in StartInput, actor uuid.UUID, reason string) (Record, error) {
	id := uuid.New()
	idText, actorText, target, org := id.String(), actor.String(), in.TargetPrincipalID.String(), in.OrganizationID.String()
	var out Record
	err := audit.Commit(ctx, s.pool, s.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			permitted, err := s.lockDelegation(ctx, tx, in, actor)
			if err != nil {
				return err
			}
			if err := s.lockBaseAndPriors(ctx, tx, in.BaseSessionID, actor); err != nil {
				return err
			}
			return s.insertDelegation(ctx, tx, delegationRow{id: idText, target: target, org: org, base: in.BaseSessionID.String(),
				actor: actorText, reason: reason, permitted: permitted}, &out)
		},
		Audit: audit.AuditEvent{
			EventType: delegationend.EventStarted, Outcome: audit.OutcomeAllowed,
			ActorPrincipalID: &actorText, SubjectPrincipalID: &target, OrganizationID: &org, RequestID: requestid.Audit(ctx),
			Attributes: map[string]any{
				"reason_code": "accepted", "target_kind": "session", "target_id": idText, "action_key": ImpersonateAction,
			},
		},
		OffStream: audit.OffStreamSessionLifecycle,
		Refusals:  append([]error{ErrBusy}, delegationRefusals...),
	})
	if err == nil {
		s.logger.InfoContext(ctx, "delegated session started", "session_id", idText, "actor_principal_id", actorText, "principal_id", target)
		return out, nil
	}
	if errors.Is(err, ErrBusy) {
		return Record{}, ErrBusy
	}
	for _, refusal := range delegationRefusals {
		if errors.Is(err, refusal) {
			s.logger.WarnContext(ctx, "delegated session refused", "actor_principal_id", actorText, "principal_id", target, "reason", refusal.Error())
			return Record{}, refusal
		}
	}
	return Record{}, fmt.Errorf("%w: starting a delegated session: %w", ErrStore, err)
}

// lockDelegation takes steps -1 to 6 of the auth lock order and decides on what
// they hold: both principals, the organization, the target's membership, and
// the actor's authority under the shared platform role lock. It returns the
// actions the delegation permits: the delegable actions of the target's role,
// or the role's full set for an actor who holds platform_owner now. The target
// role is an organization role, so no platform action enters the set.
func (s *Store) lockDelegation(ctx context.Context, tx audit.TxOps, in StartInput, actor uuid.UUID) ([]string, error) {
	if _, err := tx.Exec(ctx, rule.SharedWriteLockSQL, rule.WriteLock); err != nil {
		return nil, err
	}
	first, second := actor, in.TargetPrincipalID
	if strings.Compare(first.String(), second.String()) > 0 {
		first, second = second, first
	}
	for _, id := range []uuid.UUID{first, second} {
		usable, err := s.lockPerson(ctx, tx, id)
		if err != nil {
			return nil, err
		}
		switch {
		case usable:
		case id == actor:
			return nil, ErrActorUnavailable
		default:
			return nil, ErrTargetUnavailable
		}
	}
	var orgActive bool
	err := tx.QueryRow(ctx, `SELECT disabled_at IS NULL FROM `+s.q+`.organizations WHERE id = $1 FOR SHARE`, in.OrganizationID).Scan(&orgActive)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrOrganizationUnavailable
	}
	if err != nil {
		return nil, err
	}
	if !orgActive {
		return nil, ErrOrganizationUnavailable
	}
	var role *string
	err = tx.QueryRow(ctx, `SELECT role FROM public.memberships WHERE user_id = $1 AND org_id = $2 ORDER BY id FOR SHARE`,
		in.TargetPrincipalID, in.OrganizationID).Scan(&role)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrTargetNotMember
	}
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `SELECT 1 FROM `+s.q+`.memberships WHERE organization_id = $1 AND principal_id = $2 ORDER BY id FOR SHARE`,
		in.OrganizationID, in.TargetPrincipalID); err != nil {
		return nil, err
	}
	owner, err := s.checkAuthority(ctx, tx, actor, in.TargetPrincipalID)
	if err != nil {
		return nil, err
	}
	if role == nil {
		return nil, ErrTargetRoleUnknown
	}
	return permittedActions(*role, owner)
}

func permittedActions(role string, owner bool) ([]string, error) {
	catalog := policycatalog.Default()
	permitted, err := catalog.DelegatedActions(role)
	if err != nil {
		return nil, ErrTargetRoleUnknown
	}
	if owner {
		full, err := catalog.Role(role)
		if err != nil {
			return nil, ErrTargetRoleUnknown
		}
		permitted = full.Actions
	}
	if len(permitted) == 0 {
		return nil, ErrTargetRoleUnknown
	}
	return permitted, nil
}

// lockPerson holds one principal and its go-api user row FOR SHARE and reports
// whether it is an enabled, active user.
func (s *Store) lockPerson(ctx context.Context, tx audit.TxOps, id uuid.UUID) (bool, error) {
	var user bool
	err := tx.QueryRow(ctx, `SELECT kind = 'user' AND disabled_at IS NULL FROM `+s.q+`.principals WHERE id = $1 FOR SHARE`, id).Scan(&user)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var active bool
	err = tx.QueryRow(ctx, `SELECT is_active FROM public.users WHERE id = $1 FOR SHARE`, id).Scan(&active)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return user && active, nil
}

// checkAuthority decides on the current platform role assignments, under the
// shared lock that a grant or revoke waits for: the actor holds the action, and
// the target holds no platform role (G-50). It reports whether the actor holds
// platform_owner now.
func (s *Store) checkAuthority(ctx context.Context, tx audit.TxOps, actor, target uuid.UUID) (bool, error) {
	var allowed, targetHolds, owner bool
	roles := policycatalog.Default().RolesHolding(ImpersonateAction, policycatalog.ScopePlatform)
	if err := tx.QueryRow(ctx, `SELECT
		EXISTS (SELECT 1 FROM `+s.q+`.platform_role_assignments r JOIN `+s.q+`.principals p ON p.id = r.principal_id
			WHERE r.principal_id = $1 AND r.role_key = ANY($2::text[]) AND `+rule.ActiveRule("r", "p")+`),
		EXISTS (SELECT 1 FROM `+s.q+`.platform_role_assignments r JOIN `+s.q+`.principals p ON p.id = r.principal_id
			WHERE r.principal_id = $3 AND `+rule.ActiveRule("r", "p")+`),
		EXISTS (SELECT 1 FROM `+s.q+`.platform_role_assignments r JOIN `+s.q+`.principals p ON p.id = r.principal_id
			WHERE r.principal_id = $1 AND r.role_key = $4 AND `+rule.ActiveRule("r", "p")+`)`,
		actor, roles, target, rule.PlatformOwner).Scan(&allowed, &targetHolds, &owner); err != nil {
		return false, err
	}
	switch {
	case !allowed:
		return false, ErrActorNotAuthorized
	case targetHolds:
		return false, ErrTargetPlatformRole
	}
	return owner, nil
}

// lockBaseAndPriors is step 6a: the base session and the actor's unrevoked
// delegated rows, FOR UPDATE NOWAIT in id order. An Exchange of the base holds
// its row and then needs a key-share lock on the principal, so waiting here
// could close a cycle with the principal lock this transaction holds.
func (s *Store) lockBaseAndPriors(ctx context.Context, tx audit.TxOps, base, actor uuid.UUID) error {
	rows, err := tx.Query(ctx, `
		SELECT s.id = $1::uuid, s.principal_id = $2::uuid, s.actor_principal_id IS NOT NULL,
		       `+s.effectiveRevokedAt()+` IS NOT NULL, `+s.effectiveExpiry()+` <= clock_timestamp()
		FROM `+s.q+`.sessions s
		WHERE s.id = $1::uuid OR (s.actor_principal_id = $2::uuid AND s.revoked_at IS NULL)
		ORDER BY s.id FOR UPDATE OF s NOWAIT`, base.String(), actor.String())
	if err != nil {
		return endOrgErr("locking the base session", err)
	}
	found := false
	var refusal error
	for rows.Next() {
		var isBase, own, delegated, revoked, expired bool
		if err := rows.Scan(&isBase, &own, &delegated, &revoked, &expired); err != nil {
			rows.Close()
			return endOrgErr("reading the base session", err)
		}
		if !isBase {
			continue
		}
		found = true
		switch {
		case !own:
			refusal = ErrBaseNotLive
		case delegated:
			refusal = ErrNestedDelegation
		case revoked || expired:
			refusal = ErrBaseNotLive
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return endOrgErr("locking the base session", err)
	}
	if refusal != nil {
		return refusal
	}
	if !found {
		return ErrBaseNotLive
	}
	priors, err := delegationend.LockOfActor(ctx, tx, s.q, actor, true)
	if err != nil {
		return endOrgErr("locking the prior delegations", err)
	}
	if err := delegationend.End(ctx, tx, s.q, priors, delegationend.ReasonSuperseded); err != nil {
		return err
	}
	return delegationend.Record(ctx, tx, priors)
}

type delegationRow struct {
	id, target, org, base, actor, reason string
	permitted                            []string
}

// insertDelegation writes the row: not insertSession, so no refresh credential
// exists and Exchange has nothing to accept. The token_hash is a digest of a
// value never returned, as for every session. The effective expiry of the base
// and the assignment are read here, on the statement's own clock.
func (s *Store) insertDelegation(ctx context.Context, tx audit.TxOps, row delegationRow, out *Record) error {
	handle, err := newCredential()
	if err != nil {
		return err
	}
	roles := policycatalog.Default().RolesHolding(ImpersonateAction, policycatalog.ScopePlatform)
	var id string
	err = tx.QueryRow(ctx, `
		INSERT INTO `+s.q+`.sessions (id, principal_id, organization_id, token_hash, issued_at, created_at, updated_at, expires_at,
			auth_time, amr, client_ip, user_agent, legacy_token_version,
			actor_principal_id, delegated_from_session_id, delegation_reason, delegation_permitted_actions)
		SELECT $1::uuid, $2::uuid, $3::uuid, $4, t.at, t.at, t.at, e.expires_at,
			s.auth_time, s.amr, $5::inet, $6, `+legacyTokenVersion+`,
			$7::uuid, s.id, $8, $9::text[]
		FROM `+s.q+`.sessions s
		CROSS JOIN (SELECT clock_timestamp() AS at) t
		CROSS JOIN LATERAL (SELECT LEAST(t.at + make_interval(secs => $10::double precision), `+s.effectiveExpiry()+`,
			(SELECT max(COALESCE(r.expires_at, 'infinity'::timestamptz)) FROM `+s.q+`.platform_role_assignments r
			 WHERE r.principal_id = $7::uuid AND r.role_key = ANY($11::text[]))) AS expires_at) e
		LEFT JOIN public.users u ON u.id = $2::uuid
		WHERE s.id = $12::uuid AND e.expires_at > t.at
		RETURNING id::text`,
		row.id, row.target, row.org, hashCredential(handle), clientaddr.Text(ctx), userAgentOf(ctx),
		row.actor, row.reason, row.permitted, MaxDelegation.Seconds(), roles, row.base).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrBaseNotLive
	}
	if err != nil {
		return err
	}
	*out, err = scanRecord(tx.QueryRow(ctx, `SELECT `+s.sessionColumns()+` FROM `+s.q+`.sessions s WHERE s.id = $1`, id))
	return err
}

// EndDelegation ends a delegated session, and its base session stays live. It
// reports false when the session had already ended. A plain session is
// ErrNotDelegated.
func (s *Store) EndDelegation(ctx context.Context, sessionID uuid.UUID, actor Actor) (bool, error) {
	var delegated bool
	err := s.pool.QueryRow(ctx, `SELECT actor_principal_id IS NOT NULL FROM `+s.q+`.sessions WHERE id = $1`, sessionID.String()).Scan(&delegated)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, ErrSessionNotFound
	}
	if err != nil {
		return false, fmt.Errorf("%w: reading the session: %w", ErrStore, err)
	}
	if !delegated {
		return false, ErrNotDelegated
	}
	return s.Revoke(ctx, sessionID, actor)
}
