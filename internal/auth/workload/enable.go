package workload

import (
	"bytes"
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

// ReasonWorkloadEnabled is the reason code of the enable verb's audit row.
const ReasonWorkloadEnabled = "workload_enabled"

// EnableInput is who enables a workload and whether its principal row is
// enabled too. Operator must be an existing, enabled principal other than the
// workload's own.
type EnableInput struct {
	Operator        uuid.UUID
	EnablePrincipal bool
	RequestID       *string
}

// EnablePlan is the state the enable verb read and the changes it made (or,
// for PlanEnable, would make). It holds ids and flags only.
type EnablePlan struct {
	PrincipalID       uuid.UUID
	RowDisabled       bool
	PrincipalDisabled bool
	EnableRow         bool
	EnablePrincipal   bool
}

// enableState is what the verb decides on. It is read once without locks and
// again under the locks; the two must be equal.
type enableState struct {
	principalID       uuid.UUID
	rowDisabled       bool
	principalKind     string
	principalDisabled bool
	operatorFound     bool
	operatorDisabled  bool
	liveBootstrap     int
}

var enableRefusals = []error{
	ErrNotFound, ErrOperatorIsSubject, ErrOperatorNotFound, ErrOperatorDisabled,
	ErrBootstrapPrincipalDisabled, ErrPrincipalNotWorkload, ErrEnableStateChanged,
}

func decideEnable(st enableState, in EnableInput) (EnablePlan, error) {
	plan := EnablePlan{PrincipalID: st.principalID, RowDisabled: st.rowDisabled, PrincipalDisabled: st.principalDisabled}
	switch {
	case in.Operator == st.principalID:
		return plan, ErrOperatorIsSubject
	case !st.operatorFound:
		return plan, ErrOperatorNotFound
	case st.operatorDisabled:
		return plan, ErrOperatorDisabled
	case st.principalDisabled && !in.EnablePrincipal:
		return plan, ErrBootstrapPrincipalDisabled
	case in.EnablePrincipal && st.principalKind != "workload":
		return plan, ErrPrincipalNotWorkload
	}
	plan.EnableRow = st.rowDisabled
	plan.EnablePrincipal = in.EnablePrincipal && st.principalDisabled
	return plan, nil
}

// enableAudit is the verb's one audit row. The operator is its actor; a
// principal enabled with the row is named by principal_enabled and
// principal_id, which are absent otherwise.
func (s *Store) enableAudit(plan EnablePlan, in EnableInput) audit.AuditEvent {
	operator := in.Operator
	a := Actor{PrincipalID: &operator, RequestID: in.RequestID}
	var fields []string
	var prev, next string
	if plan.EnableRow {
		fields, prev, next = []string{"status", "disabled_at"}, "disabled", "active"
	}
	ev := s.auditFor("auth.workload.enabled", ReasonWorkloadEnabled, plan.PrincipalID, a, fields, prev, next)
	if plan.EnablePrincipal {
		ev.Attributes["principal_enabled"] = "true"
		ev.Attributes["principal_id"] = plan.PrincipalID.String()
	}
	return ev
}

func enableEvents(plan EnablePlan) []audit.OutboxEvent {
	var events []audit.OutboxEvent
	if plan.EnableRow {
		events = append(events, audit.OutboxEvent{AggregateType: "workload", AggregateID: wire(plan.PrincipalID),
			EventType: "workload.enabled", Change: change(nil, "active")})
	}
	if plan.EnablePrincipal {
		events = append(events, audit.OutboxEvent{AggregateType: "principal",
			AggregateID: provisioning.WireID("principal", plan.PrincipalID.String()),
			EventType:   "principal.enabled", Change: map[string]any{"status": "active"}})
	}
	return events
}

func (s *Store) readEnableState(ctx context.Context, name string, operator uuid.UUID) (enableState, error) {
	var st enableState
	err := s.pool.QueryRow(ctx, `SELECT w.principal_id, w.disabled_at IS NOT NULL, p.kind, p.disabled_at IS NOT NULL,
o.id IS NOT NULL, coalesce(o.disabled_at IS NOT NULL, false)
FROM `+s.q+`.workloads w JOIN `+s.q+`.principals p ON p.id = w.principal_id
LEFT JOIN `+s.q+`.principals o ON o.id = $2
WHERE w.organization_id IS NULL AND w.name = $1`, name, operator).
		Scan(&st.principalID, &st.rowDisabled, &st.principalKind, &st.principalDisabled, &st.operatorFound, &st.operatorDisabled)
	if errors.Is(err, pgx.ErrNoRows) {
		return enableState{}, ErrNotFound
	}
	if err != nil {
		return enableState{}, fault(err)
	}
	return st, nil
}

// lockEnableState reads the state again under locks: the workload row, then
// the two principal rows in id order (the workload's FOR UPDATE, the
// operator's FOR SHARE).
func (s *Store) lockEnableState(ctx context.Context, tx audit.TxOps, principalID, operator uuid.UUID) (enableState, error) {
	st := enableState{principalID: principalID}
	_, _, rowDisabled, err := s.lockWorkload(ctx, tx, principalID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return st, ErrEnableStateChanged
		}
		return st, fault(err)
	}
	st.rowDisabled = rowDisabled
	ids := []uuid.UUID{principalID, operator}
	if bytes.Compare(operator[:], principalID[:]) < 0 {
		ids[0], ids[1] = operator, principalID
	}
	for _, id := range ids {
		if id == principalID {
			if err := tx.QueryRow(ctx, `SELECT kind, disabled_at IS NOT NULL FROM `+s.q+`.principals WHERE id = $1 FOR UPDATE`, id).
				Scan(&st.principalKind, &st.principalDisabled); err != nil {
				return st, fault(err)
			}
			continue
		}
		err := tx.QueryRow(ctx, `SELECT disabled_at IS NOT NULL FROM `+s.q+`.principals WHERE id = $1 FOR SHARE`, id).Scan(&st.operatorDisabled)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
		case err != nil:
			return st, fault(err)
		default:
			st.operatorFound = true
		}
	}
	return st, nil
}

// PlanEnable is EnableWorkload's dry run: the same checks on an unlocked
// read. It writes nothing.
func (s *Store) PlanEnable(ctx context.Context, name string, in EnableInput) (EnablePlan, error) {
	st, err := s.readEnableState(ctx, name, in.Operator)
	if err != nil {
		return EnablePlan{}, err
	}
	return decideEnable(st, in)
}

// EnableWorkload clears the disabled_at of the workload row and, with
// EnablePrincipal, of its workload-kind principal row, in one transaction
// with one audit row naming the operator. It mints no credential. A disabled
// principal without EnablePrincipal is refused, and so is a state that
// changed between the read and the locks. Nothing disabled writes nothing.
func (s *Store) EnableWorkload(ctx context.Context, name string, in EnableInput) (EnablePlan, error) {
	st, err := s.readEnableState(ctx, name, in.Operator)
	if err != nil {
		return EnablePlan{}, s.refuse(ctx, verbEnable, err)
	}
	plan, err := decideEnable(st, in)
	if err != nil {
		return plan, s.refuse(ctx, verbEnable, err)
	}
	if !plan.EnableRow && !plan.EnablePrincipal {
		s.record(ctx, verbEnable, audit.ErrNoStateChange)
		return plan, nil
	}
	err = s.runEnable(ctx, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			locked, err := s.lockEnableState(ctx, tx, st.principalID, in.Operator)
			if err != nil {
				return err
			}
			if locked != st {
				return ErrEnableStateChanged
			}
			if plan.EnableRow {
				tag, err := tx.Exec(ctx, `UPDATE `+s.q+`.workloads SET disabled_at = NULL, updated_at = clock_timestamp()
WHERE principal_id = $1 AND disabled_at IS NOT NULL`, st.principalID)
				if err != nil {
					return fault(err)
				}
				if tag.RowsAffected() != 1 {
					return ErrEnableStateChanged
				}
			}
			if plan.EnablePrincipal {
				tag, err := tx.Exec(ctx, `UPDATE `+s.q+`.principals SET disabled_at = NULL
WHERE id = $1 AND kind = 'workload' AND disabled_at IS NOT NULL`, st.principalID)
				if err != nil {
					return fault(err)
				}
				if tag.RowsAffected() != 1 {
					return ErrEnableStateChanged
				}
			}
			return nil
		},
		Audit:  s.enableAudit(plan, in),
		Events: enableEvents(plan),
	})
	if err != nil {
		return EnablePlan{PrincipalID: st.principalID, RowDisabled: st.rowDisabled, PrincipalDisabled: st.principalDisabled}, err
	}
	return plan, nil
}

func (s *Store) runEnable(ctx context.Context, m audit.Mutation) (err error) {
	defer func() { s.record(ctx, verbEnable, err) }()
	err = s.commit(ctx, m)
	if err == nil {
		return nil
	}
	for _, known := range enableRefusals {
		if errors.Is(err, known) {
			return known
		}
	}
	f := fault(err)
	s.logger.ErrorContext(ctx, "workload: enable store failure", logging.ErrorAttr(f))
	return f
}
