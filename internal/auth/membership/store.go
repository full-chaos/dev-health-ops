package membership

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

type Config struct {
	Pool   *pgxpool.Pool
	Schema authschema.ValidatedIdentifier
	// OwnsUsers is AUTH_USERS_OWNED. Off, go-api writes memberships and every
	// verb refuses with ErrNotOwned.
	OwnsUsers bool
	// Sessions ends a removed member's sessions of the organization in the
	// removal's transaction; nil ends none.
	Sessions SessionEnder
	Logger   *slog.Logger
	Meter    metric.Meter
}

// SessionEnder is the session domain a removal ends credentials through
// (*session.Store).
type SessionEnder interface {
	EndOrgInTx(ctx context.Context, tx audit.TxOps, principalID, organizationID uuid.UUID) (session.Ended, error)
	RecordEnded(ctx context.Context, ended session.Ended)
}

// Locked is what Authorize sees: the organization and the principals the verb
// locked, in its transaction.
type Locked struct {
	OrgID      uuid.UUID
	Principals []uuid.UUID
}

// Authorize decides, inside the verb's transaction and after its locks, whether
// the actor may make the change. A nil Authorize refuses (ErrDenied).
type Authorize func(ctx context.Context, tx audit.TxOps, l Locked) error

// Change names one membership change.
type Change struct {
	OrgID            uuid.UUID
	PrincipalID      uuid.UUID
	Role             string
	InvitedBy        *uuid.UUID
	ExpectedRevision *int64
	Actor            Actor
	Authorize        Authorize
}

// Transfer moves the ownership of an organization from one member to another.
type Transfer struct {
	OrgID     uuid.UUID
	From, To  uuid.UUID
	Actor     Actor
	Authorize Authorize
}

// Membership is one auth.memberships row with its revision.
type Membership struct {
	ID          uuid.UUID
	OrgID       uuid.UUID
	PrincipalID uuid.UUID
	Role        *string
	Status      Status
	JoinedAt    *time.Time
	Revision    int64
}

type Store struct {
	pool      *pgxpool.Pool
	q         string
	ownsUsers bool
	sessions  SessionEnder
	commit    func(context.Context, audit.Mutation) error
	logger    *slog.Logger
	mutations metric.Int64Counter
}

func New(cfg Config) (*Store, error) {
	if cfg.Pool == nil {
		return nil, errors.New("membership: no connection pool")
	}
	if cfg.Schema.String() == "" {
		return nil, errors.New("membership: no auth schema")
	}
	s := &Store{pool: cfg.Pool, q: authschema.Quote(cfg.Schema), ownsUsers: cfg.OwnsUsers, sessions: cfg.Sessions, logger: cfg.Logger}
	s.commit = func(ctx context.Context, m audit.Mutation) error { return audit.Commit(ctx, cfg.Pool, cfg.Schema, m) }
	if s.logger == nil {
		s.logger = slog.Default()
	}
	meter := cfg.Meter
	if meter == nil {
		meter = otel.Meter("github.com/full-chaos/dev-health-ops/internal/auth/membership")
	}
	var err error
	if s.mutations, err = meter.Int64Counter("auth.membership.mutations",
		metric.WithDescription("Membership mutations, by verb and outcome.")); err != nil {
		return nil, fmt.Errorf("membership: registering the mutation counter: %w", err)
	}
	return s, nil
}

func (s *Store) record(ctx context.Context, v verb, err error) {
	s.mutations.Add(ctx, 1, metric.WithAttributes(attribute.String("verb", string(v)), attribute.String("outcome", outcomeLabel(err))))
}

// refusals are the store's named outcomes: returned as they are, never
// wrapped in ErrStore.
var refusals = []error{ErrNotOwned, ErrRoleInvalid, ErrNotMember, ErrAlreadyMember, ErrSuspended, ErrLastOwner,
	ErrNotOwner, ErrTargetNotMember, ErrRoleMissing, ErrRevisionConflict, ErrOrgNotFound, ErrOrgSuspended,
	ErrPrincipalNotFound, ErrInviterNotFound, ErrDenied, ErrSuperuserRequired, errStateMoved, errSessionsBusy}

func (s *Store) run(ctx context.Context, v verb, m audit.Mutation) (err error) {
	defer func() {
		s.record(ctx, v, err)
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
	s.logger.ErrorContext(ctx, "membership: store failure", "verb", string(v), logging.ErrorAttr(err))
	return fmt.Errorf("%w: %s: %w", ErrStore, v, err)
}

// pairRows is one locked (organization, principal) pair in both tables.
type pairRows struct {
	principal     uuid.UUID
	publicID      *uuid.UUID
	publicRole    *string
	authID        *uuid.UUID
	authStatus    *Status
	authRole      *string
	authJoinedAt  *time.Time
	authInvitedBy *uuid.UUID
}

func (p pairRows) state() state { return effective(p.publicRole, p.authStatus, p.authRole) }

// id is the membership's one id: the auth row's, else go-api's, else new.
func (p pairRows) id() uuid.UUID {
	switch {
	case p.authID != nil:
		return *p.authID
	case p.publicID != nil:
		return *p.publicID
	}
	return uuid.New()
}

// locked is the result of readPairs.
type locked struct {
	pairs        map[uuid.UUID]*pairRows
	activeOwners int
}

type rowsIter interface {
	Next() bool
	Scan(dest ...any) error
	Close()
	Err() error
}

// querier is the read surface readPairs needs: a verb's transaction (locked)
// or the pool (the lock-free plan read).
type querier interface {
	query(ctx context.Context, sql string, args ...any) (rowsIter, error)
	queryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type txQuerier struct{ tx audit.TxOps }

func (q txQuerier) query(ctx context.Context, sql string, args ...any) (rowsIter, error) {
	rows, err := q.tx.Query(ctx, sql, args...)
	return rows, err
}

func (q txQuerier) queryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return q.tx.QueryRow(ctx, sql, args...)
}

type poolQuerier struct{ pool *pgxpool.Pool }

func (q poolQuerier) query(ctx context.Context, sql string, args ...any) (rowsIter, error) {
	return q.pool.Query(ctx, sql, args...)
}

func (q poolQuerier) queryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return q.pool.QueryRow(ctx, sql, args...)
}

// readPairs reads the pairs of one verb. With lock it takes the locks in the
// auth lock order: the one order every writer of these rows uses, in auth
// (membership, invite, organization verbs, the JIT join) and in go-api (admin
// member routes, the session invite accept, the organization delete). They
// cite it by this name.
//  0. auth.workloads rows (workload verbs, before their principals);
//  1. auth.principals rows, one row per statement, in ascending id order;
//  2. public.users rows of the same ids, the same way;
//  3. the auth.organizations row FOR SHARE (organization verbs take it FOR UPDATE);
//  4. invite rows: auth.invites, then public.org_invites, FOR UPDATE;
//  5. public.memberships: the organization's owner rows and the pairs, FOR UPDATE, ORDER BY id;
//  6. auth.memberships pair rows FOR UPDATE, ORDER BY id;
//     6a. a removal: the pair's auth.sessions FOR UPDATE NOWAIT, ORDER
//     BY id, then their refresh credentials, then the pair's
//     public.refresh_tokens FOR UPDATE NOWAIT (session.Store.EndOrgInTx). An
//     Exchange holds its session row and then needs a key-share lock on the
//     principal (the new credential's foreign key), so waiting here would
//     close a cycle with step 1: a locked row rolls the removal back and it
//     is planned again. An organization deprovision (6a) and a suspend (after
//     step 3) do the same for the whole organization through
//     EndOrganizationInTx; a later verb that ends a membership in place,
//     such as a route for Suspend, must end its sessions the same way;
//  7. aggregate_revisions, inside audit.Commit.
//
// Steps 1 and 2 take every id any later statement of the verb references,
// each once in the strongest mode its roles need (LockOrder): a target FOR
// UPDATE, the actor FOR SHARE (Authorize reads its account and role; a
// demotion, disable or superuser change of the actor waits), an inviter and
// any other referenced id (an old inviter a write names again) FOR KEY SHARE,
// the lock its foreign key check takes. No writer takes a principals or users
// lock after step 2, so no two writers wait on each other in a cycle; a
// key-share lock still lets two verbs that only name a row run together. A
// missing target is ErrPrincipalNotFound and a missing inviter
// ErrInviterNotFound, before anything is written; a missing actor is no
// refusal here (Authorize decides on the actor). referenced are the old
// inviters of the planned pairs: a locked pair whose auth row names another
// inviter is errStateMoved (plan again).
func (s *Store) readPairs(ctx context.Context, q querier, orgID uuid.UUID, principals []uuid.UUID, inviter, actor *uuid.UUID, referenced []uuid.UUID, lock bool, step4 func() error) (*locked, error) {
	forUpdate, forShare := "", ""
	if lock {
		forUpdate, forShare = " FOR UPDATE", " FOR SHARE"
	}
	ids := sortedUnique(principals)
	order := LockOrder(ids, inviter, actor, referenced...)
	// The id array goes as one array literal with an explicit ::uuid[] cast: a
	// statement inside audit.Commit has no parameter types to encode a
	// []uuid.UUID with.
	idText := uuidArray(ids)
	inPrincipals, inUsers, err := s.lockRows(ctx, q, order, lock)
	if err != nil {
		return nil, err
	}
	for _, found := range []map[uuid.UUID]bool{inPrincipals, inUsers} {
		for _, id := range ids {
			if !found[id] {
				return nil, ErrPrincipalNotFound
			}
		}
		if inviter != nil && !found[*inviter] {
			return nil, ErrInviterNotFound
		}
	}
	var orgActive bool
	err = q.queryRow(ctx, `SELECT disabled_at IS NULL FROM `+s.q+`.organizations WHERE id = $1`+forShare, orgID).Scan(&orgActive)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrOrgNotFound
	}
	if err != nil {
		return nil, err
	}
	if !orgActive {
		return nil, ErrOrgSuspended
	}
	if step4 != nil {
		if err := step4(); err != nil {
			return nil, err
		}
	}
	l := &locked{pairs: map[uuid.UUID]*pairRows{}}
	for _, id := range ids {
		l.pairs[id] = &pairRows{principal: id}
	}
	rows, err := q.query(ctx, `SELECT id, user_id, role FROM public.memberships
WHERE org_id = $1 AND (role = 'owner' OR user_id = ANY($2::uuid[])) ORDER BY id`+forUpdate, orgID, idText)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var id, user uuid.UUID
		var role string
		if err := rows.Scan(&id, &user, &role); err != nil {
			rows.Close()
			return nil, err
		}
		if role == "owner" {
			l.activeOwners++
		}
		if p, ok := l.pairs[user]; ok {
			p.publicID, p.publicRole = &id, &role
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows, err = q.query(ctx, `SELECT id, principal_id, status, role, joined_at, invited_by FROM `+s.q+`.memberships
WHERE organization_id = $1 AND principal_id = ANY($2::uuid[]) ORDER BY id`+forUpdate, orgID, idText)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var id, principal uuid.UUID
		var status Status
		var role *string
		var joined *time.Time
		var inviter *uuid.UUID
		if err := rows.Scan(&id, &principal, &status, &role, &joined, &inviter); err != nil {
			rows.Close()
			return nil, err
		}
		p := l.pairs[principal]
		p.authID, p.authStatus, p.authRole, p.authJoinedAt, p.authInvitedBy = &id, &status, role, joined, inviter
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if lock {
		held := map[uuid.UUID]bool{}
		for _, r := range order {
			held[r.ID] = true
		}
		for _, p := range l.pairs {
			if p.authInvitedBy != nil && !held[*p.authInvitedBy] {
				return nil, errStateMoved
			}
		}
	}
	return l, nil
}

// referenced are the old inviters the auth rows of a read name: a write may
// name them again (go-api's row of a reactivated membership).
func (l *locked) referenced() []uuid.UUID {
	var out []uuid.UUID
	for _, p := range l.pairs {
		if p.authInvitedBy != nil {
			out = append(out, *p.authInvitedBy)
		}
	}
	return out
}

func uuidArray(ids []uuid.UUID) string {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = id.String()
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func foundIDs(ctx context.Context, q querier, sql string, ids string) (map[uuid.UUID]bool, error) {
	rows, err := q.query(ctx, sql, ids)
	if err != nil {
		return nil, err
	}
	found := map[uuid.UUID]bool{}
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		found[id] = true
	}
	rows.Close()
	return found, rows.Err()
}

func sortedUnique(ids []uuid.UUID) []uuid.UUID {
	seen := map[uuid.UUID]bool{}
	out := make([]uuid.UUID, 0, len(ids))
	for _, id := range ids {
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i][:], out[j][:]) < 0 })
	return out
}

func authorize(ctx context.Context, tx audit.TxOps, auth Authorize, l Locked) error {
	if auth == nil {
		return ErrDenied
	}
	if err := auth(ctx, tx, l); err != nil {
		switch {
		case errors.Is(err, ErrDenied):
			return ErrDenied
		case errors.Is(err, ErrSuperuserRequired):
			return ErrSuperuserRequired
		}
		return err
	}
	return nil
}

// lookupRefusals name a missing or suspended row. They are only returned to an
// actor Authorize accepts; any other actor gets ErrDenied, with no other
// detail, so a refused caller learns nothing about which rows exist.
var lookupRefusals = []error{ErrPrincipalNotFound, ErrInviterNotFound, ErrOrgNotFound, ErrOrgSuspended}

func authorizeLookup(ctx context.Context, tx audit.TxOps, auth Authorize, l Locked, lookupErr error) error {
	for _, refusal := range lookupRefusals {
		if errors.Is(lookupErr, refusal) {
			if err := authorize(ctx, tx, auth, l); err != nil {
				return err
			}
			return lookupErr
		}
	}
	return lookupErr
}

func (s *Store) checkRevision(ctx context.Context, tx audit.TxOps, id uuid.UUID, expected *int64) error {
	if expected == nil {
		return nil
	}
	var rev int64
	err := tx.QueryRow(ctx, `SELECT revision FROM `+s.q+`.aggregate_revisions WHERE aggregate_type = 'membership' AND aggregate_id = $1`,
		provisioning.WireID("membership", id.String())).Scan(&rev)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	if rev != *expected {
		return ErrRevisionConflict
	}
	return nil
}

// write brings both rows of one pair to next: active = go-api's row present
// with the role; any other status = no public row (the 10783 contract). A fresh
// join (Add) gives both rows one new joined_at and the given inviter; any other
// activation keeps the membership's own joined_at and inviter from its auth
// row, so both rows always carry the same join.
func (s *Store) write(ctx context.Context, tx audit.TxOps, orgID uuid.UUID, p *pairRows, id uuid.UUID, next state, fresh bool, invitedBy *uuid.UUID) error {
	var at time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&at); err != nil {
		return err
	}
	joined, inviter := p.authJoinedAt, p.authInvitedBy
	if fresh || joined == nil {
		joined = &at
	}
	if fresh {
		inviter = invitedBy
	}
	if next.status == StatusActive {
		switch {
		case p.publicID == nil:
			// An inviter go-api no longer has (public.users ON DELETE SET NULL) is written as NULL.
			if _, err := tx.Exec(ctx, `INSERT INTO public.memberships (id, user_id, org_id, role, invited_by_id, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, (SELECT u.id FROM public.users u WHERE u.id = $5), $6, $7, $7)`, id, p.principal, orgID, next.role, inviter, *joined, at); err != nil {
				return err
			}
		case *p.publicRole != next.role:
			if _, err := tx.Exec(ctx, `UPDATE public.memberships SET role = $2, updated_at = $3 WHERE id = $1`,
				*p.publicID, next.role, at); err != nil {
				return err
			}
		}
	} else if p.publicID != nil {
		if _, err := tx.Exec(ctx, `DELETE FROM public.memberships WHERE id = $1`, *p.publicID); err != nil {
			return err
		}
	}
	var role *string
	if next.role != "" {
		role = &next.role
	}
	if p.authID == nil {
		var authJoined *time.Time
		if next.status == StatusActive {
			authJoined = joined
		}
		_, err := tx.Exec(ctx, `INSERT INTO `+s.q+`.memberships (id, organization_id, principal_id, status, role, invited_by, joined_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)`, id, orgID, p.principal, string(next.status), role, inviter, authJoined)
		return err
	}
	if next.status == StatusActive {
		_, err := tx.Exec(ctx, `UPDATE `+s.q+`.memberships SET status = $2, role = $3, joined_at = $4, invited_by = $5,
updated_at = $6 WHERE id = $1`, *p.authID, string(next.status), role, *joined, inviter, at)
		return err
	}
	_, err := tx.Exec(ctx, `UPDATE `+s.q+`.memberships SET status = $2, role = $3, updated_at = $4 WHERE id = $1`,
		*p.authID, string(next.status), role, at)
	return err
}

// refusedNext is the planned next state of a change the lock-free read
// refuses: the commit still runs, so Authorize decides before any refusal is
// named. Its audit row and event are never written (the locked read refuses
// again, or finds another next state and plans again), but audit.Commit checks
// them before the transaction, so they must be well formed: a state with no
// role gets a placeholder role.
func refusedNext(prev state) state {
	if prev.role == "" {
		prev.role = organizationRoles[0]
	}
	return prev
}

// errStateMoved reports that the rows changed between the plan read and the
// locks: the verb is planned again.
var errStateMoved = errors.New("membership: the rows changed while the change was planned")

// errSessionsBusy reports that a removal found a session or go-api refresh
// token row of the pair locked by another writer (session.ErrBusy). The whole
// transaction is rolled back, which releases the principal locks the other
// writer may be waiting for, and the verb is planned again.
var errSessionsBusy = errors.New("membership: a session of the removed member is locked by another writer")

// planAttempts bounds the plan-then-lock loop.
const planAttempts = 3

// busyBackoff is the wait before the next attempt after errSessionsBusy.
var busyBackoff = [planAttempts - 1]time.Duration{25 * time.Millisecond, 75 * time.Millisecond}

// pairPlan is one pair's change as planned from a lock-free read: the audit
// row and the event are fixed before the transaction (audit.Mutation), and
// the locked read must find the same rows, or the verb is planned again.
type pairPlan struct {
	rowID *uuid.UUID
	id    uuid.UUID
	prev  state
	next  state
}

func planOf(p *pairRows) pairPlan {
	pl := pairPlan{prev: p.state()}
	switch {
	case p.authID != nil:
		pl.rowID = p.authID
	case p.publicID != nil:
		pl.rowID = p.publicID
	}
	if pl.rowID != nil {
		pl.id = *pl.rowID
	} else {
		pl.id = uuid.New()
	}
	return pl
}

func (pl pairPlan) matches(p *pairRows) bool {
	got := planOf(p)
	if (got.rowID == nil) != (pl.rowID == nil) || (got.rowID != nil && *got.rowID != *pl.rowID) {
		return false
	}
	return got.prev == pl.prev
}

// mutate runs one verb on one pair.
func (s *Store) mutate(ctx context.Context, v verb, c Change) (*Membership, error) {
	if !s.ownsUsers {
		s.record(ctx, v, ErrNotOwned)
		return nil, ErrNotOwned
	}
	if (v == verbAdd || v == verbChangeRole) && !isOrganizationRole(c.Role) {
		s.record(ctx, v, ErrRoleInvalid)
		return nil, ErrRoleInvalid
	}
	ids := []uuid.UUID{c.PrincipalID}
	var err error
	var ended session.Ended
	for attempt := range planAttempts {
		if attempt > 0 && errors.Is(err, errSessionsBusy) {
			if werr := sleepCtx(ctx, busyBackoff[attempt-1]); werr != nil {
				return nil, fmt.Errorf("%w: %s: %w", ErrStore, v, werr)
			}
		}
		ended = session.Ended{}
		var read *locked
		read, err = s.readPairs(ctx, poolQuerier{s.pool}, c.OrgID, ids, c.InvitedBy, c.Actor.PrincipalID, nil, false, nil)
		if err != nil {
			// The locked read below decides the refusal after Authorize, so
			// a refusal is not disclosed to an actor Authorize refuses.
			read = &locked{pairs: map[uuid.UUID]*pairRows{c.PrincipalID: {principal: c.PrincipalID}}}
		}
		plan := planOf(read.pairs[c.PrincipalID])
		plan.next = refusedNext(plan.prev)
		if d, derr := decide(v, plan.prev, c.Role, read.activeOwners); derr == nil {
			plan.next = d.next
		}
		err = s.run(ctx, v, audit.Mutation{
			Apply: func(ctx context.Context, tx audit.TxOps) error {
				l, err := s.readPairs(ctx, txQuerier{tx}, c.OrgID, ids, c.InvitedBy, c.Actor.PrincipalID, read.referenced(), true, nil)
				if err != nil {
					return authorizeLookup(ctx, tx, c.Authorize, Locked{OrgID: c.OrgID, Principals: ids}, err)
				}
				if err := authorize(ctx, tx, c.Authorize, Locked{OrgID: c.OrgID, Principals: ids}); err != nil {
					return err
				}
				p := l.pairs[c.PrincipalID]
				if c.ExpectedRevision != nil {
					row := planOf(p).rowID
					if row == nil {
						return ErrRevisionConflict
					}
					if err := s.checkRevision(ctx, tx, *row, c.ExpectedRevision); err != nil {
						return err
					}
				}
				d, err := decide(v, p.state(), c.Role, l.activeOwners)
				if err != nil {
					return err
				}
				if !plan.matches(p) || d.next != plan.next {
					return errStateMoved
				}
				if d.noop {
					return nil
				}
				if err := s.write(ctx, tx, c.OrgID, p, plan.id, d.next, v == verbAdd, c.InvitedBy); err != nil {
					return err
				}
				if v != verbRemove || s.sessions == nil {
					return nil
				}
				ended, err = s.sessions.EndOrgInTx(ctx, tx, c.PrincipalID, c.OrgID)
				if errors.Is(err, session.ErrBusy) {
					return errSessionsBusy
				}
				return err
			},
			Audit:    auditFor(v, c.OrgID, c.PrincipalID, plan.id, c.Actor, plan.prev, plan.next),
			Events:   []audit.OutboxEvent{eventFor(v, plan.id, plan.next)},
			Refusals: []error{errSessionsBusy},
		})
		if !errors.Is(err, errStateMoved) && !errors.Is(err, errSessionsBusy) {
			break
		}
	}
	if errors.Is(err, errSessionsBusy) {
		s.logger.WarnContext(ctx, "membership: removal gave up: a session of the member stayed locked", "verb", string(v))
		return nil, fmt.Errorf("%w: %s: %w", ErrStore, v, err)
	}
	if err != nil {
		return nil, err
	}
	if s.sessions != nil {
		s.sessions.RecordEnded(ctx, ended)
		if ended != (session.Ended{}) {
			s.logger.InfoContext(ctx, "membership removed; its sessions of the organization ended", "organization_id", c.OrgID.String(),
				"principal_id", c.PrincipalID.String(), "sessions", ended.Sessions, "refresh_tokens", ended.RefreshTokens)
		}
	}
	return s.Get(ctx, c.OrgID, c.PrincipalID)
}

func (s *Store) Add(ctx context.Context, c Change) (*Membership, error) {
	return s.mutate(ctx, verbAdd, c)
}

func (s *Store) ChangeRole(ctx context.Context, c Change) (*Membership, error) {
	return s.mutate(ctx, verbChangeRole, c)
}

func (s *Store) Suspend(ctx context.Context, c Change) (*Membership, error) {
	return s.mutate(ctx, verbSuspend, c)
}

func (s *Store) Reactivate(ctx context.Context, c Change) (*Membership, error) {
	return s.mutate(ctx, verbReactivate, c)
}

func (s *Store) Remove(ctx context.Context, c Change) (*Membership, error) {
	return s.mutate(ctx, verbRemove, c)
}

// TransferOwnership moves the ownership of an organization in one commit: the
// source owner becomes admin and the target member becomes owner (go-api
// transferOwnership). A transfer to the source itself changes nothing.
func (s *Store) TransferOwnership(ctx context.Context, t Transfer) error {
	if !s.ownsUsers {
		s.record(ctx, verbTransfer, ErrNotOwned)
		return ErrNotOwned
	}
	ids := []uuid.UUID{t.From, t.To}
	var err error
	for range planAttempts {
		var from, to pairPlan
		var referenced []uuid.UUID
		if read, rerr := s.readPairs(ctx, poolQuerier{s.pool}, t.OrgID, ids, nil, t.Actor.PrincipalID, nil, false, nil); rerr == nil {
			from, to = planOf(read.pairs[t.From]), planOf(read.pairs[t.To])
			referenced = read.referenced()
		} else {
			from, to = planOf(&pairRows{principal: t.From}), planOf(&pairRows{principal: t.To})
		}
		from.next, to.next = refusedNext(from.prev), refusedNext(to.prev)
		if f, tt, derr := decideTransfer(from.prev, to.prev); derr == nil && t.From != t.To {
			from.next, to.next = f, tt
		}
		err = s.run(ctx, verbTransfer, audit.Mutation{
			Apply: func(ctx context.Context, tx audit.TxOps) error {
				l, err := s.readPairs(ctx, txQuerier{tx}, t.OrgID, ids, nil, t.Actor.PrincipalID, referenced, true, nil)
				if err != nil {
					return authorizeLookup(ctx, tx, t.Authorize, Locked{OrgID: t.OrgID, Principals: sortedUnique(ids)}, err)
				}
				if err := authorize(ctx, tx, t.Authorize, Locked{OrgID: t.OrgID, Principals: sortedUnique(ids)}); err != nil {
					return err
				}
				pf, pt := l.pairs[t.From], l.pairs[t.To]
				nextFrom, nextTo, err := decideTransfer(pf.state(), pt.state())
				if err != nil {
					return err
				}
				if t.From == t.To {
					return nil
				}
				if !from.matches(pf) || !to.matches(pt) || nextFrom != from.next || nextTo != to.next {
					return errStateMoved
				}
				if err := s.write(ctx, tx, t.OrgID, pf, from.id, nextFrom, false, nil); err != nil {
					return err
				}
				return s.write(ctx, tx, t.OrgID, pt, to.id, nextTo, false, nil)
			},
			Audit:  auditFor(verbTransfer, t.OrgID, t.To, to.id, t.Actor, to.prev, to.next),
			Events: []audit.OutboxEvent{eventFor(verbTransfer, from.id, from.next), eventFor(verbTransfer, to.id, to.next)},
		})
		if !errors.Is(err, errStateMoved) {
			break
		}
	}
	return err
}

// ErrStateMoved reports that the rows of a planned add changed before its
// locks: the caller plans the add again (AddPlan).
var ErrStateMoved = errStateMoved

// AddPlan is an Add planned for a caller's own audit.Commit, for a verb whose
// own row decides the add (an invite accept). Its membership id and event are
// fixed before the transaction, as audit.Mutation needs.
type AddPlan struct {
	change     Change
	plan       pairPlan
	referenced []uuid.UUID
}

// MembershipID is the id the planned membership has in both tables.
func (p *AddPlan) MembershipID() uuid.UUID { return p.plan.id }

// Event is the planned membership's provisioning event.
func (p *AddPlan) Event() audit.OutboxEvent { return eventFor(verbAdd, p.plan.id, p.plan.next) }

// PlanAdd plans an add of c from a lock-free read. A refusal the read already
// shows (already a member, a suspended membership) is returned at once.
func (s *Store) PlanAdd(ctx context.Context, c Change) (*AddPlan, error) {
	if !s.ownsUsers {
		return nil, ErrNotOwned
	}
	if !isOrganizationRole(c.Role) {
		return nil, ErrRoleInvalid
	}
	read, err := s.readPairs(ctx, poolQuerier{s.pool}, c.OrgID, []uuid.UUID{c.PrincipalID}, c.InvitedBy, c.Actor.PrincipalID, nil, false, nil)
	if err != nil {
		return nil, err
	}
	plan := planOf(read.pairs[c.PrincipalID])
	d, err := decide(verbAdd, plan.prev, c.Role, read.activeOwners)
	if err != nil {
		return nil, err
	}
	plan.next = d.next
	return &AddPlan{change: c, plan: plan, referenced: read.referenced()}, nil
}

// ApplyAdd writes a planned add inside the caller's audit.Commit, in the one
// lock order: principals, public.users and the organization, then step4 (the
// caller's own row, e.g. the invite, and its checks: they authorize the add),
// then the membership rows. A nil step4 refuses. ErrStateMoved: plan again.
func (s *Store) ApplyAdd(ctx context.Context, tx audit.TxOps, p *AddPlan, step4 func() error) error {
	if step4 == nil {
		return ErrDenied
	}
	c := p.change
	l, err := s.readPairs(ctx, txQuerier{tx}, c.OrgID, []uuid.UUID{c.PrincipalID}, c.InvitedBy, c.Actor.PrincipalID, p.referenced, true, step4)
	if err != nil {
		return err
	}
	pr := l.pairs[c.PrincipalID]
	d, err := decide(verbAdd, pr.state(), c.Role, l.activeOwners)
	if err != nil {
		return err
	}
	if !p.plan.matches(pr) || d.next != p.plan.next {
		return errStateMoved
	}
	return s.write(ctx, tx, c.OrgID, pr, p.plan.id, d.next, true, c.InvitedBy)
}

func (s *Store) Get(ctx context.Context, orgID, principalID uuid.UUID) (*Membership, error) {
	var m Membership
	err := s.pool.QueryRow(ctx, `SELECT m.id, m.organization_id, m.principal_id, m.role, m.status, m.joined_at, COALESCE(r.revision, 0)
FROM `+s.q+`.memberships m LEFT JOIN `+s.q+`.aggregate_revisions r
  ON r.aggregate_type = 'membership' AND r.aggregate_id = 'mbr_' || m.id::text
WHERE m.organization_id = $1 AND m.principal_id = $2`, orgID, principalID).
		Scan(&m.ID, &m.OrgID, &m.PrincipalID, &m.Role, &m.Status, &m.JoinedAt, &m.Revision)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotMember
	}
	if err != nil {
		s.logger.ErrorContext(ctx, "membership: read failure", logging.ErrorAttr(err))
		return nil, fmt.Errorf("%w: read: %w", ErrStore, err)
	}
	return &m, nil
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
