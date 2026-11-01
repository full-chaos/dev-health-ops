package workload

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

type Config struct {
	Pool    *pgxpool.Pool
	Schema  authschema.ValidatedIdentifier
	Catalog *policycatalog.Catalog
	Logger  *slog.Logger
	Meter   metric.Meter
}

type Store struct {
	pool      *pgxpool.Pool
	q         string
	catalog   *policycatalog.Catalog
	commit    func(context.Context, audit.Mutation) error
	logger    *slog.Logger
	mutations metric.Int64Counter
	resolves  metric.Int64Counter
}

func New(cfg Config) (*Store, error) {
	if cfg.Pool == nil {
		return nil, errors.New("workload: no connection pool")
	}
	if cfg.Schema.String() == "" {
		return nil, errors.New("workload: no auth schema")
	}
	s := &Store{pool: cfg.Pool, q: authschema.Quote(cfg.Schema), catalog: cfg.Catalog, logger: cfg.Logger}
	if s.catalog == nil {
		s.catalog = policycatalog.Default()
	}
	s.commit = func(ctx context.Context, m audit.Mutation) error { return audit.Commit(ctx, cfg.Pool, cfg.Schema, m) }
	if s.logger == nil {
		s.logger = slog.Default()
	}
	meter := cfg.Meter
	if meter == nil {
		meter = otel.Meter("github.com/full-chaos/dev-health-ops/internal/auth/workload")
	}
	var err error
	if s.mutations, err = meter.Int64Counter("auth.workload.mutations",
		metric.WithDescription("Workload registry mutations, by verb and outcome.")); err != nil {
		return nil, fmt.Errorf("workload: registering the mutation counter: %w", err)
	}
	if s.resolves, err = meter.Int64Counter("auth.workload.resolves",
		metric.WithDescription("Workload resolves, by outcome (ok, refused, error).")); err != nil {
		return nil, fmt.Errorf("workload: registering the resolve counter: %w", err)
	}
	return s, nil
}

type verb string

const (
	verbRegister       verb = "register"
	verbSetAudiences   verb = "set_audiences"
	verbSetActions     verb = "set_actions"
	verbDisable        verb = "disable"
	verbEnable         verb = "enable"
	verbAddBinding     verb = "add_binding"
	verbDisableBinding verb = "disable_binding"
	verbEnableBinding  verb = "enable_binding"
	verbRebindBinding  verb = "rebind_binding"

	verbBootstrapCreate  verb = "bootstrap_create"
	verbBootstrapRotate  verb = "bootstrap_rotate"
	verbBootstrapRevoke  verb = "bootstrap_revoke"
	verbBootstrapDisable verb = "bootstrap_disable"
)

func outcomeLabel(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, audit.ErrNoStateChange):
		return "noop"
	case errors.Is(err, ErrNotFound), errors.Is(err, ErrBindingNotFound), errors.Is(err, ErrBootstrapNotFound), errors.Is(err, ErrBootstrapNone):
		return "not_found"
	case errors.Is(err, ErrNameTaken), errors.Is(err, ErrBindingTaken), errors.Is(err, ErrBootstrapLive), errors.Is(err, ErrBootstrapRotating),
		errors.Is(err, ErrEnableLiveBootstrap):
		return "taken"
	case errors.Is(err, ErrNameInvalid), errors.Is(err, ErrAudienceInvalid), errors.Is(err, ErrActionInvalid), errors.Is(err, ErrBindingInvalid),
		errors.Is(err, ErrBootstrapValue), errors.Is(err, ErrBootstrapExpiry), errors.Is(err, ErrBootstrapOverlap), errors.Is(err, ErrBootstrapReason),
		errors.Is(err, ErrBootstrapWorkloadDisabled), errors.Is(err, ErrBootstrapPrincipalDisabled),
		errors.Is(err, ErrAssertionKeyInvalid), errors.Is(err, ErrAssertionKeyActiveExists), errors.Is(err, ErrAssertionKeyTerminal),
		errors.Is(err, ErrAssertionKeyTransition), errors.Is(err, ErrAssertionKeyOverlap), errors.Is(err, ErrAssertionKeyNoActive), errors.Is(err, ErrAssertionKeyWorkloadDisabled),
		errors.Is(err, ErrOperatorNotFound), errors.Is(err, ErrOperatorDisabled), errors.Is(err, ErrOperatorIsSubject),
		errors.Is(err, ErrPrincipalNotWorkload), errors.Is(err, ErrEnableStateChanged), errors.Is(err, ErrRebindStateChanged):
		return "invalid"
	}
	return "error"
}

func (s *Store) record(ctx context.Context, v verb, err error) {
	s.mutations.Add(ctx, 1, metric.WithAttributes(attribute.String("verb", string(v)), attribute.String("outcome", outcomeLabel(err))))
}

func (s *Store) run(ctx context.Context, v verb, m audit.Mutation) (err error) {
	defer func() {
		s.record(ctx, v, err)
		if errors.Is(err, audit.ErrNoStateChange) {
			err = nil
		}
	}()
	err = s.commit(ctx, m)
	switch {
	case err == nil, errors.Is(err, audit.ErrNoStateChange), errors.Is(err, ErrNotFound), errors.Is(err, ErrNameTaken),
		errors.Is(err, ErrBindingTaken), errors.Is(err, ErrBindingNotFound), errors.Is(err, ErrRebindStateChanged), isAssertionKeyRefusal(err):
		return err
	}
	s.logger.ErrorContext(ctx, "workload: store failure", "verb", string(v), logging.ErrorAttr(err))
	return fmt.Errorf("%w: %s: %w", ErrStore, v, err)
}

func status(disabled bool) string {
	if disabled {
		return "disabled"
	}
	return "active"
}

func wire(id uuid.UUID) string { return provisioning.WireID("workload", id.String()) }

func (s *Store) auditFor(eventType, reason string, id uuid.UUID, a Actor, fields []string, prev, next string) audit.AuditEvent {
	sub := id.String()
	ev := audit.AuditEvent{EventType: eventType, Outcome: audit.OutcomeAllowed, SubjectPrincipalID: &sub, RequestID: a.RequestID,
		Attributes: map[string]any{"target_kind": "workload", "target_id": sub}}
	if reason != "" {
		ev.Attributes["reason_code"] = reason
	}
	if len(fields) > 0 {
		ev.Attributes["changed_fields"] = fields
	}
	if prev != "" {
		ev.Attributes["previous_status"] = prev
	}
	if next != "" {
		ev.Attributes["new_status"] = next
	}
	if a.PrincipalID != nil {
		actor := a.PrincipalID.String()
		ev.ActorPrincipalID = &actor
	}
	return ev
}

func change(audiences []string, statusValue string) map[string]any {
	c := map[string]any{}
	if statusValue != "" {
		c["status"] = statusValue
	}
	if len(audiences) > 0 {
		c["audiences"] = audiences
	}
	return c
}

// Info is the current registration of a platform workload.
type Info struct {
	PrincipalID    uuid.UUID
	Name           string
	Audiences      []string
	AllowedActions []string
	Disabled       bool
}

// Lookup reads a platform workload by name.
func (s *Store) Lookup(ctx context.Context, name string) (*Info, error) {
	var i Info
	err := s.pool.QueryRow(ctx, `SELECT principal_id, name, audiences, allowed_actions, disabled_at IS NOT NULL
FROM `+s.q+`.workloads WHERE organization_id IS NULL AND name = $1`, name).
		Scan(&i.PrincipalID, &i.Name, &i.Audiences, &i.AllowedActions, &i.Disabled)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		s.logger.ErrorContext(ctx, "workload: read failure", logging.ErrorAttr(err))
		return nil, fmt.Errorf("%w: read: %w", ErrStore, err)
	}
	return &i, nil
}

// PlatformNames lists the names of every platform workload, sorted.
func (s *Store) PlatformNames(ctx context.Context) ([]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT name FROM `+s.q+`.workloads WHERE organization_id IS NULL ORDER BY name`)
	if err != nil {
		s.logger.ErrorContext(ctx, "workload: list failure", logging.ErrorAttr(err))
		return nil, fmt.Errorf("%w: list: %w", ErrStore, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, fmt.Errorf("%w: list: %w", ErrStore, err)
		}
		out = append(out, n)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: list: %w", ErrStore, err)
	}
	return out, nil
}

func (s *Store) validateSpec(sp Spec) error {
	if err := ValidateName(sp.Name); err != nil {
		return err
	}
	if err := ValidateAudiences(sp.Audiences); err != nil {
		return err
	}
	return ValidateActions(s.catalog, sp.AllowedActions)
}

func nonNil(v []string) []string {
	if v == nil {
		return []string{}
	}
	return v
}

// Register inserts an enabled platform workload and its principal. A name that
// is taken is ErrNameTaken; Seed is the idempotent caller.
func (s *Store) Register(ctx context.Context, sp Spec, a Actor) (uuid.UUID, error) {
	if err := s.validateSpec(sp); err != nil {
		s.record(ctx, verbRegister, err)
		return uuid.Nil, err
	}
	id := uuid.New()
	err := s.run(ctx, verbRegister, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			if _, err := tx.Exec(ctx, `INSERT INTO `+s.q+`.principals (id, kind, display_name) VALUES ($1, 'workload', $2)`, id, sp.Name); err != nil {
				return err
			}
			var inserted uuid.UUID
			err := tx.QueryRow(ctx, `INSERT INTO `+s.q+`.workloads (principal_id, name, audiences, allowed_actions) VALUES ($1, $2, $3, $4)
ON CONFLICT DO NOTHING RETURNING principal_id`, id, sp.Name, nonNil(sp.Audiences), nonNil(sp.AllowedActions)).Scan(&inserted)
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNameTaken
			}
			return err
		},
		Audit: s.auditFor("auth.workload.registered", "principal_provisioned", id, a, nil, "", "active"),
		Events: []audit.OutboxEvent{{AggregateType: "workload", AggregateID: wire(id), EventType: "workload.created",
			Change: change(sp.Audiences, "active")}},
	})
	if err != nil {
		return uuid.Nil, err
	}
	return id, nil
}

func (s *Store) lockWorkload(ctx context.Context, tx audit.TxOps, id uuid.UUID) (audiences, actions []string, disabled bool, err error) {
	err = tx.QueryRow(ctx, `SELECT audiences, allowed_actions, disabled_at IS NOT NULL FROM `+s.q+`.workloads
WHERE principal_id = $1 FOR UPDATE`, id).Scan(&audiences, &actions, &disabled)
	if errors.Is(err, pgx.ErrNoRows) {
		err = ErrNotFound
	}
	return
}

// SetAudiences replaces the audience list. An equal list writes nothing.
func (s *Store) SetAudiences(ctx context.Context, name string, audiences []string, a Actor) error {
	if err := ValidateAudiences(audiences); err != nil {
		s.record(ctx, verbSetAudiences, err)
		return err
	}
	info, err := s.Lookup(ctx, name)
	if err != nil {
		s.record(ctx, verbSetAudiences, err)
		return err
	}
	return s.run(ctx, verbSetAudiences, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			cur, _, _, err := s.lockWorkload(ctx, tx, info.PrincipalID)
			if err != nil || sameSet(cur, audiences) {
				return err
			}
			_, err = tx.Exec(ctx, `UPDATE `+s.q+`.workloads SET audiences = $2, updated_at = clock_timestamp() WHERE principal_id = $1`,
				info.PrincipalID, nonNil(audiences))
			return err
		},
		Audit: s.auditFor("auth.workload.audiences_changed", "", info.PrincipalID, a, []string{"audiences"}, "", ""),
		Events: []audit.OutboxEvent{{AggregateType: "workload", AggregateID: wire(info.PrincipalID), EventType: "workload.audiences_changed",
			Change: change(audiences, "")}},
	})
}

// SetAllowedActions replaces the allowed action list. An equal list writes
// nothing.
func (s *Store) SetAllowedActions(ctx context.Context, name string, actions []string, a Actor) error {
	if err := ValidateActions(s.catalog, actions); err != nil {
		s.record(ctx, verbSetActions, err)
		return err
	}
	info, err := s.Lookup(ctx, name)
	if err != nil {
		s.record(ctx, verbSetActions, err)
		return err
	}
	return s.run(ctx, verbSetActions, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			_, cur, _, err := s.lockWorkload(ctx, tx, info.PrincipalID)
			if err != nil || sameSet(cur, actions) {
				return err
			}
			_, err = tx.Exec(ctx, `UPDATE `+s.q+`.workloads SET allowed_actions = $2, updated_at = clock_timestamp() WHERE principal_id = $1`,
				info.PrincipalID, nonNil(actions))
			return err
		},
		Audit:     s.auditFor("auth.workload.allowed_actions_changed", "", info.PrincipalID, a, []string{"allowed_actions"}, "", ""),
		OffStream: audit.OffStreamWorkloadRegistry,
	})
}

// Disable sets disabled_at on the workload and its principal. A repeat is a
// no-op.
func (s *Store) Disable(ctx context.Context, name string, a Actor) error {
	return s.setEnabled(ctx, verbDisable, name, false, a)
}

// Enable clears disabled_at. A repeat is a no-op.
func (s *Store) Enable(ctx context.Context, name string, a Actor) error {
	return s.setEnabled(ctx, verbEnable, name, true, a)
}

func (s *Store) setEnabled(ctx context.Context, v verb, name string, enable bool, a Actor) error {
	info, err := s.Lookup(ctx, name)
	if err != nil {
		s.record(ctx, v, err)
		return err
	}
	event, reason, prev, next := "workload.disabled", "principal_disabled", "active", "disabled"
	if enable {
		event, reason, prev, next = "workload.enabled", "principal_enabled", "disabled", "active"
	}
	return s.run(ctx, v, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			_, _, disabled, err := s.lockWorkload(ctx, tx, info.PrincipalID)
			if err != nil || disabled == !enable {
				return err
			}
			if _, err := tx.Exec(ctx, `UPDATE `+s.q+`.workloads SET disabled_at = CASE WHEN $2::boolean THEN NULL ELSE clock_timestamp() END,
updated_at = clock_timestamp() WHERE principal_id = $1`, info.PrincipalID, enable); err != nil {
				return err
			}
			_, err = tx.Exec(ctx, `UPDATE `+s.q+`.principals SET disabled_at = CASE WHEN $2::boolean THEN NULL ELSE clock_timestamp() END
WHERE id = $1`, info.PrincipalID, enable)
			return err
		},
		Audit: s.auditFor("auth."+event, reason, info.PrincipalID, a, []string{"status", "disabled_at"}, prev, next),
		Events: []audit.OutboxEvent{{AggregateType: "workload", AggregateID: wire(info.PrincipalID), EventType: event,
			Change: change(nil, next)}},
	})
}

// AddBinding binds a workload to a federated identity. The tuple is unique
// across all workloads. Repeating the same binding returns its id and writes
// nothing.
func (s *Store) AddBinding(ctx context.Context, name string, b BindingSpec, a Actor) (uuid.UUID, error) {
	if err := ValidateBinding(b); err != nil {
		s.record(ctx, verbAddBinding, err)
		return uuid.Nil, err
	}
	info, err := s.Lookup(ctx, name)
	if err != nil {
		s.record(ctx, verbAddBinding, err)
		return uuid.Nil, err
	}
	id := uuid.New()
	var uid *string
	if b.ServiceAccountUID != "" {
		uid = &b.ServiceAccountUID
	}
	err = s.run(ctx, verbAddBinding, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			if _, _, _, err := s.lockWorkload(ctx, tx, info.PrincipalID); err != nil {
				return err
			}
			// The commit guard counts an INSERT that conflicts, so an equal
			// binding is looked up first: a repeat writes nothing.
			var existing uuid.UUID
			err := tx.QueryRow(ctx, `SELECT id FROM `+s.q+`.workload_bindings
WHERE trust_domain = $1 AND issuer = $2 AND namespace = $3 AND service_account_name = $4
AND workload_principal_id = $5 AND environment = $6 AND service_account_uid IS NOT DISTINCT FROM $7`,
				b.TrustDomain, b.Issuer, b.Namespace, b.ServiceAccountName, info.PrincipalID, b.Environment, uid).Scan(&existing)
			if err == nil {
				id = existing
				return nil
			}
			if !errors.Is(err, pgx.ErrNoRows) {
				return err
			}
			var inserted uuid.UUID
			err = tx.QueryRow(ctx, `INSERT INTO `+s.q+`.workload_bindings
(id, workload_principal_id, environment, trust_domain, issuer, namespace, service_account_name, service_account_uid)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT DO NOTHING RETURNING id`,
				id, info.PrincipalID, b.Environment, b.TrustDomain, b.Issuer, b.Namespace, b.ServiceAccountName, uid).Scan(&inserted)
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrBindingTaken
			}
			return err
		},
		Audit:     s.auditFor("auth.workload.binding_added", "", info.PrincipalID, a, []string{"binding"}, "", ""),
		OffStream: audit.OffStreamWorkloadRegistry,
	})
	if err != nil {
		return uuid.Nil, err
	}
	return id, nil
}

// DisableBinding makes the exchange refuse the binding at once. A repeat is a
// no-op.
func (s *Store) DisableBinding(ctx context.Context, bindingID uuid.UUID, a Actor) error {
	return s.setBindingEnabled(ctx, verbDisableBinding, bindingID, false, a)
}

// EnableBinding clears disabled_at on a binding. A repeat is a no-op.
func (s *Store) EnableBinding(ctx context.Context, bindingID uuid.UUID, a Actor) error {
	return s.setBindingEnabled(ctx, verbEnableBinding, bindingID, true, a)
}

func (s *Store) setBindingEnabled(ctx context.Context, v verb, bindingID uuid.UUID, enable bool, a Actor) error {
	var workloadID uuid.UUID
	err := s.pool.QueryRow(ctx, `SELECT workload_principal_id FROM `+s.q+`.workload_bindings WHERE id = $1`, bindingID).Scan(&workloadID)
	if errors.Is(err, pgx.ErrNoRows) {
		err = ErrBindingNotFound
	} else if err != nil {
		s.logger.ErrorContext(ctx, "workload: read failure", logging.ErrorAttr(err))
		err = fmt.Errorf("%w: read: %w", ErrStore, err)
	}
	if err != nil {
		s.record(ctx, v, err)
		return err
	}
	eventType, prev, next := "auth.workload.binding_disabled", "active", "disabled"
	if enable {
		eventType, prev, next = "auth.workload.binding_enabled", "disabled", "active"
	}
	return s.run(ctx, v, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			var disabled bool
			err := tx.QueryRow(ctx, `SELECT disabled_at IS NOT NULL FROM `+s.q+`.workload_bindings WHERE id = $1 FOR UPDATE`, bindingID).Scan(&disabled)
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrBindingNotFound
			}
			if err != nil || disabled == !enable {
				return err
			}
			_, err = tx.Exec(ctx, `UPDATE `+s.q+`.workload_bindings SET disabled_at = CASE WHEN $2::boolean THEN NULL ELSE clock_timestamp() END,
updated_at = clock_timestamp() WHERE id = $1`, bindingID, enable)
			return err
		},
		Audit:     s.auditFor(eventType, "", workloadID, a, []string{"binding", "disabled_at"}, prev, next),
		OffStream: audit.OffStreamWorkloadRegistry,
	})
}

// Report is what Seed did.
type Report struct {
	Created, Updated, Unchanged int
	// Unlisted names platform workloads the contract does not list. Seed
	// reports them and never deletes or changes them.
	Unlisted []string
}

// Seed makes the platform workload rows equal the specs: it creates a missing
// row and updates a row whose audiences or allowed actions differ. A run that
// finds every row equal writes nothing.
func (s *Store) Seed(ctx context.Context, specs []Spec, a Actor) (Report, error) {
	return s.seed(ctx, specs, a, false)
}

// PlanSeed reports what Seed would do and writes nothing.
func (s *Store) PlanSeed(ctx context.Context, specs []Spec) (Report, error) {
	return s.seed(ctx, specs, Actor{}, true)
}

func (s *Store) seed(ctx context.Context, specs []Spec, a Actor, dry bool) (Report, error) {
	var r Report
	listed := map[string]bool{}
	for _, sp := range specs {
		listed[sp.Name] = true
		if err := s.validateSpec(sp); err != nil {
			return r, err
		}
		info, err := s.Lookup(ctx, sp.Name)
		if errors.Is(err, ErrNotFound) && dry {
			r.Created++
			continue
		}
		if errors.Is(err, ErrNotFound) {
			if _, err = s.Register(ctx, sp, a); err == nil {
				r.Created++
				continue
			}
			if !errors.Is(err, ErrNameTaken) {
				return r, err
			}
			info, err = s.Lookup(ctx, sp.Name)
		}
		if err != nil {
			return r, err
		}
		changed := false
		if !sameSet(info.Audiences, sp.Audiences) {
			if !dry {
				if err := s.SetAudiences(ctx, sp.Name, sp.Audiences, a); err != nil {
					return r, err
				}
			}
			changed = true
		}
		if !sameSet(info.AllowedActions, sp.AllowedActions) {
			if !dry {
				if err := s.SetAllowedActions(ctx, sp.Name, sp.AllowedActions, a); err != nil {
					return r, err
				}
			}
			changed = true
		}
		if changed {
			r.Updated++
		} else {
			r.Unchanged++
		}
	}
	names, err := s.PlatformNames(ctx)
	if err != nil {
		return r, err
	}
	for _, n := range names {
		if !listed[n] {
			r.Unlisted = append(r.Unlisted, n)
		}
	}
	sort.Strings(r.Unlisted)
	return r, nil
}

type reason string

const (
	reasonUnknown          reason = "unknown_tuple"
	reasonWorkloadDisabled reason = "workload_disabled"
	reasonPrincipalOff     reason = "principal_disabled"
	reasonBindingDisabled  reason = "binding_disabled"
	reasonUIDMismatch      reason = "uid_mismatch"
	reasonNoAudience       reason = "no_audience"
	reasonWrongKind        reason = "wrong_kind"
	reasonBadInput         reason = "bad_input"
)

// ResolveInput is the federated identity the exchange verified.
type ResolveInput struct {
	TrustDomain        string
	Issuer             string
	Namespace          string
	ServiceAccountName string
	ServiceAccountUID  string
}

// Resolve maps a verified federated identity to its registered workload. It
// reads the current rows on every call and caches nothing. Any refusal and
// any store error is a *RefusalError: errors.Is(err, ErrRefused) holds, and the
// Reason (RefusalReason) is for logs and counters, never for the client.
func (s *Store) Resolve(ctx context.Context, in ResolveInput) (Registration, error) {
	reg, why, err := s.resolve(ctx, in)
	switch {
	case err != nil:
		s.resolves.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", "error")))
		s.logger.ErrorContext(ctx, "workload: resolve store failure", logging.ErrorAttr(err))
		return Registration{}, &RefusalError{Reason: ReasonStoreError}
	case why != "":
		s.resolves.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", "refused")))
		s.logger.InfoContext(ctx, "workload: resolve refused", "reason", string(why))
		return Registration{}, &RefusalError{Reason: publicReason(why)}
	}
	s.resolves.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", "ok")))
	return reg, nil
}

func (s *Store) resolve(ctx context.Context, in ResolveInput) (Registration, reason, error) {
	b := BindingSpec{Environment: "x", TrustDomain: in.TrustDomain, Issuer: in.Issuer, Namespace: in.Namespace,
		ServiceAccountName: in.ServiceAccountName, ServiceAccountUID: in.ServiceAccountUID}
	if ValidateBinding(b) != nil {
		return Registration{}, reasonBadInput, nil
	}
	var (
		reg                             Registration
		kind                            string
		wDisabled, pDisabled, bDisabled bool
		boundUID                        *string
		orgID                           *uuid.UUID
	)
	err := s.pool.QueryRow(ctx, `SELECT w.principal_id, w.name, w.organization_id, w.audiences, w.allowed_actions, p.kind,
w.disabled_at IS NOT NULL, p.disabled_at IS NOT NULL, b.id, b.disabled_at IS NOT NULL, b.service_account_uid, COALESCE(r.revision, 0)
FROM `+s.q+`.workload_bindings b
JOIN `+s.q+`.workloads w ON w.principal_id = b.workload_principal_id
JOIN `+s.q+`.principals p ON p.id = w.principal_id
LEFT JOIN `+s.q+`.aggregate_revisions r ON r.aggregate_type = 'workload' AND r.aggregate_id = 'prn_' || w.principal_id::text
WHERE b.trust_domain = $1 AND b.issuer = $2 AND b.namespace = $3 AND b.service_account_name = $4`,
		in.TrustDomain, in.Issuer, in.Namespace, in.ServiceAccountName).
		Scan(&reg.PrincipalID, &reg.Name, &orgID, &reg.Audiences, &reg.AllowedActions, &kind,
			&wDisabled, &pDisabled, &reg.BindingID, &bDisabled, &boundUID, &reg.Revision)
	if errors.Is(err, pgx.ErrNoRows) {
		return Registration{}, reasonUnknown, nil
	}
	if err != nil {
		return Registration{}, "", err
	}
	switch {
	case kind != "workload":
		return Registration{}, reasonWrongKind, nil
	case wDisabled:
		return Registration{}, reasonWorkloadDisabled, nil
	case pDisabled:
		return Registration{}, reasonPrincipalOff, nil
	case bDisabled:
		return Registration{}, reasonBindingDisabled, nil
	case boundUID != nil && *boundUID != in.ServiceAccountUID:
		return Registration{}, reasonUIDMismatch, nil
	case len(reg.Audiences) == 0:
		return Registration{}, reasonNoAudience, nil
	}
	return reg, "", nil
}
