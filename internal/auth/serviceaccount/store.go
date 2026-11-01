package serviceaccount

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
	"github.com/full-chaos/dev-health-ops/internal/auth/membership"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// RowLocker takes steps 1 and 2 of the auth lock order (*membership.Store).
type RowLocker interface {
	LockRows(ctx context.Context, tx audit.TxOps, rows []membership.LockRow) error
}

type Config struct {
	Pool   *pgxpool.Pool
	Schema authschema.ValidatedIdentifier
	Locks  RowLocker
	// OwnsUsers is AUTH_USERS_OWNED. Off, Create, Enable and SetOwner refuse
	// with ErrNotOwned: their row locks on public.users need UPDATE, which the
	// runtime role holds only when auth-service owns users.
	OwnsUsers bool
	Logger    *slog.Logger
	Meter     metric.Meter
}

type Store struct {
	pool      *pgxpool.Pool
	q         string
	locks     RowLocker
	ownsUsers bool
	commit    func(context.Context, audit.Mutation) error
	read      func(context.Context, uuid.UUID) (*Info, error)
	logger    *slog.Logger
	mutations metric.Int64Counter
	// keyResolves counts ResolveKey answers by outcome.
	keyResolves metric.Int64Counter
	// mint makes a key value.
	mint func() (string, error)
}

func New(cfg Config) (*Store, error) {
	switch {
	case cfg.Pool == nil:
		return nil, errors.New("serviceaccount: no connection pool")
	case cfg.Schema.String() == "":
		return nil, errors.New("serviceaccount: no auth schema")
	case cfg.Locks == nil:
		return nil, errors.New("serviceaccount: no row locker")
	}
	s := &Store{pool: cfg.Pool, q: authschema.Quote(cfg.Schema), locks: cfg.Locks, ownsUsers: cfg.OwnsUsers, logger: cfg.Logger, mint: MintKey}
	s.commit = func(ctx context.Context, m audit.Mutation) error { return audit.Commit(ctx, cfg.Pool, cfg.Schema, m) }
	s.read = s.Lookup
	if s.logger == nil {
		s.logger = slog.Default()
	}
	meter := cfg.Meter
	if meter == nil {
		meter = otel.Meter("github.com/full-chaos/dev-health-ops/internal/auth/serviceaccount")
	}
	var err error
	if s.mutations, err = meter.Int64Counter("auth.serviceaccount.mutations",
		metric.WithDescription("Service account mutations, by verb and outcome.")); err != nil {
		return nil, fmt.Errorf("serviceaccount: registering the mutation counter: %w", err)
	}
	if s.keyResolves, err = meter.Int64Counter("auth.serviceaccount.key_resolves",
		metric.WithDescription("Service account key resolves, by outcome.")); err != nil {
		return nil, fmt.Errorf("serviceaccount: registering the key resolve counter: %w", err)
	}
	return s, nil
}

type verb string

const (
	verbCreate   verb = "create"
	verbDisable  verb = "disable"
	verbEnable   verb = "enable"
	verbSetOwner verb = "set_owner"
)

func outcomeLabel(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, ErrNotFound), errors.Is(err, ErrOrgNotFound):
		return "not_found"
	case errors.Is(err, ErrNameTaken), errors.Is(err, ErrAlreadyInState), errors.Is(err, audit.ErrNoStateChange):
		return "taken"
	case errors.Is(err, ErrNotOwned):
		return "not_owned"
	case errors.Is(err, ErrStore):
		return "error"
	}
	for _, r := range keyRefusals {
		if errors.Is(err, r) {
			return "invalid"
		}
	}
	return "error"
}

func (s *Store) record(ctx context.Context, v verb, err error) {
	s.mutations.Add(ctx, 1, metric.WithAttributes(attribute.String("verb", string(v)), attribute.String("outcome", outcomeLabel(err))))
}

func (s *Store) run(ctx context.Context, v verb, m audit.Mutation) (err error) {
	defer func() { s.record(ctx, v, err) }()
	err = s.commit(ctx, m)
	if err == nil {
		return nil
	}
	for _, refusal := range refusals {
		if errors.Is(err, refusal) {
			return refusal
		}
	}
	s.logger.ErrorContext(ctx, "serviceaccount: store failure", "verb", string(v), logging.ErrorAttr(err))
	return fmt.Errorf("%w: %s: %w", ErrStore, v, err)
}

func (s *Store) refuseNotOwned(ctx context.Context, v verb) error {
	s.record(ctx, v, ErrNotOwned)
	return ErrNotOwned
}

func wire(id uuid.UUID) string { return provisioning.WireID("service_account", id.String()) }

func ownerWire(id uuid.UUID) string { return provisioning.WireID("principal", id.String()) }

// auditFor is a verb's one audit row. principal_id names the owner the row
// concerns (the new owner of a SetOwner); the previous owner is the
// principal_id of the account's earlier rows.
func auditFor(eventType, reason string, id, org uuid.UUID, owner *uuid.UUID, a Actor, fields []string, prev, next string) audit.AuditEvent {
	sub, orgText := id.String(), org.String()
	ev := audit.AuditEvent{EventType: eventType, Outcome: audit.OutcomeAllowed, SubjectPrincipalID: &sub, OrganizationID: &orgText,
		RequestID: a.RequestID, Attributes: map[string]any{"target_kind": "service_account", "target_id": sub, "reason_code": reason}}
	if len(fields) > 0 {
		ev.Attributes["changed_fields"] = fields
	}
	if prev != "" {
		ev.Attributes["previous_status"] = prev
	}
	if next != "" {
		ev.Attributes["new_status"] = next
	}
	if owner != nil {
		ev.Attributes["principal_id"] = owner.String()
	}
	if a.PrincipalID != nil {
		actor := a.PrincipalID.String()
		ev.ActorPrincipalID = &actor
	}
	a.Caller.Annotate(ev.Attributes)
	return ev
}

func change(status string, owner *uuid.UUID) map[string]any {
	c := map[string]any{}
	if status != "" {
		c["status"] = status
	}
	if owner != nil {
		c["owner_principal_id"] = ownerWire(*owner)
	}
	return c
}

// lockSet is steps -1, 1 and 2 of the auth lock order for a verb that names
// an owner: the service account (a target, FOR UPDATE; absent on a create),
// the actor FOR SHARE and the owner FOR SHARE (the verb reads its account and
// membership), ascending id, one mode per id.
func (s *Store) lockSet(ctx context.Context, tx audit.TxOps, account *uuid.UUID, owner uuid.UUID, actor *uuid.UUID) error {
	if err := platformrole.LockWritesShared(ctx, tx); err != nil {
		return err
	}
	return s.locks.LockRows(ctx, tx, lockRowsFor(account, owner, actor))
}

func lockRowsFor(account *uuid.UUID, owner uuid.UUID, actor *uuid.UUID) []membership.LockRow {
	var targets []uuid.UUID
	if account != nil {
		targets = append(targets, *account)
	}
	rows := membership.LockOrder(targets, nil, actor, owner)
	for i := range rows {
		if rows[i].ID == owner && rows[i].Mode < membership.LockShare {
			rows[i].Mode = membership.LockShare
		}
	}
	return rows
}

func (s *Store) lockOrg(ctx context.Context, tx audit.TxOps, org uuid.UUID) (bool, error) {
	var active bool
	err := tx.QueryRow(ctx, `SELECT disabled_at IS NULL FROM `+s.q+`.organizations WHERE id = $1 FOR SHARE`, org).Scan(&active)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, ErrOrgNotFound
	}
	return active, err
}

// lockOwner reads the owner under the locks lockSet took and locks its
// membership pair row FOR SHARE (step 6).
func (s *Store) lockOwner(ctx context.Context, tx audit.TxOps, org, owner uuid.UUID) (ownerState, error) {
	var st ownerState
	var kind string
	err := tx.QueryRow(ctx, `SELECT p.kind, p.disabled_at IS NOT NULL, EXISTS (SELECT 1 FROM public.users u WHERE u.id = p.id)
FROM `+s.q+`.principals p WHERE p.id = $1`, owner).Scan(&kind, &st.disabled, &st.isUser)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return ownerState{}, nil
	case err != nil:
		return ownerState{}, err
	}
	st.isUser = st.isUser && kind == "user"
	var status string
	err = tx.QueryRow(ctx, `SELECT status FROM `+s.q+`.memberships WHERE organization_id = $1 AND principal_id = $2 FOR SHARE`, org, owner).Scan(&status)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
	case err != nil:
		return ownerState{}, err
	default:
		st.member = status == "active"
	}
	return st, nil
}

type accountRow struct {
	org    uuid.UUID
	owner  uuid.UUID
	active bool
}

// lockAccount is step 6b: the service_accounts row FOR UPDATE.
func (s *Store) lockAccount(ctx context.Context, tx audit.TxOps, id uuid.UUID) (accountRow, error) {
	var r accountRow
	var owner *uuid.UUID
	err := tx.QueryRow(ctx, `SELECT organization_id, owner_principal_id, status = 'active' FROM `+s.q+`.service_accounts
WHERE principal_id = $1 FOR UPDATE`, id).Scan(&r.org, &owner, &r.active)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, ErrNotFound
	}
	if owner != nil {
		r.owner = *owner
	}
	return r, err
}

// Create inserts an active service account with its principal. The owner must
// be an enabled user and an active member of the organization.
func (s *Store) Create(ctx context.Context, in CreateInput, a Actor) (uuid.UUID, error) {
	if !s.ownsUsers {
		return uuid.Nil, s.refuseNotOwned(ctx, verbCreate)
	}
	if err := ValidateName(in.Name); err != nil {
		s.record(ctx, verbCreate, err)
		return uuid.Nil, err
	}
	id := uuid.New()
	owner := in.Owner
	err := s.run(ctx, verbCreate, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			if err := s.lockSet(ctx, tx, nil, in.Owner, a.PrincipalID); err != nil {
				return err
			}
			if err := a.authorize(ctx, tx, Locked{OrgID: in.OrgID}); err != nil {
				return err
			}
			orgActive, err := s.lockOrg(ctx, tx, in.OrgID)
			if err != nil {
				return err
			}
			st, err := s.lockOwner(ctx, tx, in.OrgID, in.Owner)
			if err != nil {
				return err
			}
			if err := decideCreate(orgActive, st); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `INSERT INTO `+s.q+`.principals (id, kind, display_name) VALUES ($1, 'service_account', $2)`, id, in.Name); err != nil {
				return err
			}
			var inserted uuid.UUID
			err = tx.QueryRow(ctx, `INSERT INTO `+s.q+`.service_accounts (principal_id, organization_id, name, owner_principal_id, created_by)
VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING RETURNING principal_id`, id, in.OrgID, in.Name, in.Owner, a.PrincipalID).Scan(&inserted)
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNameTaken
			}
			return err
		},
		Audit: auditFor("auth.service_account.created", "principal_provisioned", id, in.OrgID, &owner, a,
			[]string{"status", "owner_principal_id"}, "", "active"),
		Events: []audit.OutboxEvent{{AggregateType: "service_account", AggregateID: wire(id), EventType: "service_account.created",
			Change: change("active", &owner)}},
		Refusals: refusals,
	})
	if err != nil {
		return uuid.Nil, err
	}
	return id, nil
}

// Disable sets the account's row and its principal disabled in one
// transaction. It takes the principal row alone, never public.users: a
// service account has no users row, so the kill switch works whether or not
// auth-service owns users. Its Authorize is a plain read in the same
// transaction: a demotion that commits during the transaction counts as after it.
// The audit row names the owner read before the transaction; an owner or an
// organization that moved by the time the account row is locked is
// ErrStateChanged, and Disable reads again and runs once more.
func (s *Store) Disable(ctx context.Context, id uuid.UUID, a Actor) error {
	err := s.disableOnce(ctx, id, a)
	if errors.Is(err, ErrStateChanged) {
		err = s.disableOnce(ctx, id, a)
	}
	return err
}

func (s *Store) disableOnce(ctx context.Context, id uuid.UUID, a Actor) error {
	info, err := s.read(ctx, id)
	if err != nil {
		s.record(ctx, verbDisable, err)
		return err
	}
	owner := info.Owner
	return s.run(ctx, verbDisable, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			if err := platformrole.LockWritesShared(ctx, tx); err != nil {
				return err
			}
			var kind string
			err := tx.QueryRow(ctx, `SELECT kind FROM `+s.q+`.principals WHERE id = $1 FOR UPDATE`, id).Scan(&kind)
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			if err != nil {
				return err
			}
			if kind != "service_account" {
				return ErrNotAccount
			}
			row, err := s.lockAccount(ctx, tx, id)
			if err != nil {
				return err
			}
			if row.org != info.OrgID || row.owner != info.Owner {
				return ErrStateChanged
			}
			if err := a.authorize(ctx, tx, Locked{OrgID: row.org, Owner: row.owner}); err != nil {
				return err
			}
			if err := decideDisable(row.active); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `UPDATE `+s.q+`.service_accounts SET status = 'disabled', disabled_at = clock_timestamp(),
updated_at = clock_timestamp() WHERE principal_id = $1`, id); err != nil {
				return err
			}
			_, err = tx.Exec(ctx, `UPDATE `+s.q+`.principals SET disabled_at = clock_timestamp() WHERE id = $1`, id)
			return err
		},
		Audit: auditFor("auth.service_account.disabled", "principal_disabled", id, info.OrgID, &owner, a,
			[]string{"status", "disabled_at"}, "active", "disabled"),
		Events: []audit.OutboxEvent{{AggregateType: "service_account", AggregateID: wire(id), EventType: "service_account.disabled",
			Change: change("disabled", nil)}},
		Refusals: refusals,
	})
}

// Enable clears the disabled state of the row and the principal. It refuses
// while the owner is disabled, is not a user or is no active member.
func (s *Store) Enable(ctx context.Context, id uuid.UUID, a Actor) error {
	if !s.ownsUsers {
		return s.refuseNotOwned(ctx, verbEnable)
	}
	info, err := s.read(ctx, id)
	if err != nil {
		s.record(ctx, verbEnable, err)
		return err
	}
	owner := info.Owner
	return s.run(ctx, verbEnable, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			if err := s.lockSet(ctx, tx, &id, info.Owner, a.PrincipalID); err != nil {
				return err
			}
			orgActive, err := s.lockOrg(ctx, tx, info.OrgID)
			if err != nil {
				return err
			}
			st, err := s.lockOwner(ctx, tx, info.OrgID, info.Owner)
			if err != nil {
				return err
			}
			row, err := s.lockAccount(ctx, tx, id)
			if err != nil {
				return err
			}
			if row.org != info.OrgID || row.owner != info.Owner {
				return ErrStateChanged
			}
			if err := a.authorize(ctx, tx, Locked{OrgID: row.org, Owner: row.owner}); err != nil {
				return err
			}
			if err := decideEnable(row.active, orgActive, st); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `UPDATE `+s.q+`.service_accounts SET status = 'active', disabled_at = NULL,
updated_at = clock_timestamp() WHERE principal_id = $1`, id); err != nil {
				return err
			}
			_, err = tx.Exec(ctx, `UPDATE `+s.q+`.principals SET disabled_at = NULL WHERE id = $1`, id)
			return err
		},
		Audit: auditFor("auth.service_account.enabled", "principal_enabled", id, info.OrgID, &owner, a,
			[]string{"status", "disabled_at"}, "disabled", "active"),
		Events: []audit.OutboxEvent{{AggregateType: "service_account", AggregateID: wire(id), EventType: "service_account.enabled",
			Change: change("active", nil)}},
		Refusals: refusals,
	})
}

// SetOwner names a new owner, who must pass the checks of Create. A disabled
// account may change owner; Enable then checks the new one.
func (s *Store) SetOwner(ctx context.Context, id, newOwner uuid.UUID, a Actor) error {
	if !s.ownsUsers {
		return s.refuseNotOwned(ctx, verbSetOwner)
	}
	info, err := s.read(ctx, id)
	if err != nil {
		s.record(ctx, verbSetOwner, err)
		return err
	}
	owner := newOwner
	return s.run(ctx, verbSetOwner, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			if err := s.lockSet(ctx, tx, &id, newOwner, a.PrincipalID); err != nil {
				return err
			}
			orgActive, err := s.lockOrg(ctx, tx, info.OrgID)
			if err != nil {
				return err
			}
			st, err := s.lockOwner(ctx, tx, info.OrgID, newOwner)
			if err != nil {
				return err
			}
			row, err := s.lockAccount(ctx, tx, id)
			if err != nil {
				return err
			}
			if row.org != info.OrgID || row.owner != info.Owner {
				return ErrStateChanged
			}
			if err := a.authorize(ctx, tx, Locked{OrgID: row.org, Owner: row.owner}); err != nil {
				return err
			}
			if err := decideSetOwner(row.owner, newOwner, orgActive, st); err != nil {
				return err
			}
			_, err = tx.Exec(ctx, `UPDATE `+s.q+`.service_accounts SET owner_principal_id = $2, updated_at = clock_timestamp()
WHERE principal_id = $1`, id, newOwner)
			return err
		},
		Audit: auditFor("auth.service_account.owner_changed", "service_account_owner_changed", id, info.OrgID, &owner, a,
			[]string{"owner_principal_id"}, "", ""),
		Events: []audit.OutboxEvent{{AggregateType: "service_account", AggregateID: wire(id), EventType: "service_account.owner_changed",
			Change: change("", &owner)}},
		Refusals: refusals,
	})
}

const infoSelect = `SELECT sa.principal_id, sa.organization_id, sa.name, sa.status = 'active', sa.disabled_at, coalesce(sa.owner_principal_id, '00000000-0000-0000-0000-000000000000'::uuid),
coalesce(op.kind = 'user' AND op.disabled_at IS NULL, false),
EXISTS (SELECT 1 FROM %[1]s.memberships m WHERE m.organization_id = sa.organization_id AND m.principal_id = sa.owner_principal_id AND m.status = 'active'),
sa.created_by, sa.last_used_at
FROM %[1]s.service_accounts sa LEFT JOIN %[1]s.principals op ON op.id = sa.owner_principal_id`

func scanInfo(row pgx.Row) (*Info, error) {
	var i Info
	if err := row.Scan(&i.ID, &i.OrgID, &i.Name, &i.Active, &i.DisabledAt, &i.Owner, &i.OwnerEnabled, &i.OwnerMember, &i.CreatedBy, &i.LastUsedAt); err != nil {
		return nil, err
	}
	return &i, nil
}

// Lookup reads one account with its owner's state derived now.
func (s *Store) Lookup(ctx context.Context, id uuid.UUID) (*Info, error) {
	i, err := scanInfo(s.pool.QueryRow(ctx, fmt.Sprintf(infoSelect, s.q)+` WHERE sa.principal_id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("%w: lookup: %w", ErrStore, err)
	}
	return i, nil
}

// List reads the accounts of one organization by name.
func (s *Store) List(ctx context.Context, org uuid.UUID) ([]Info, error) {
	rows, err := s.pool.Query(ctx, fmt.Sprintf(infoSelect, s.q)+` WHERE sa.organization_id = $1 ORDER BY sa.name`, org)
	if err != nil {
		return nil, fmt.Errorf("%w: list: %w", ErrStore, err)
	}
	defer rows.Close()
	var out []Info
	for rows.Next() {
		i, err := scanInfo(rows)
		if err != nil {
			return nil, fmt.Errorf("%w: list: %w", ErrStore, err)
		}
		out = append(out, *i)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: list: %w", ErrStore, err)
	}
	return out, nil
}
