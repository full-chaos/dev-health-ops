package workload

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

// BindingTuple names a binding by its federation tuple, which is unique
// across all workloads.
type BindingTuple struct {
	TrustDomain        string
	Issuer             string
	Namespace          string
	ServiceAccountName string
}

// ValidateTuple refuses an empty, padded or oversized tuple field.
func ValidateTuple(t BindingTuple) error {
	return ValidateBinding(BindingSpec{Environment: "x", TrustDomain: t.TrustDomain, Issuer: t.Issuer,
		Namespace: t.Namespace, ServiceAccountName: t.ServiceAccountName})
}

// BindingInfo is the state of one binding. The pin is read, never printed.
type BindingInfo struct {
	ID          uuid.UUID
	WorkloadID  uuid.UUID
	Workload    string
	Environment string
	Pinned      bool
	pin         string
	Disabled    bool
}

// SamePin reports whether uid equals the binding's current pin.
func (b BindingInfo) SamePin(uid string) bool { return b.Pinned && b.pin == uid }

// LookupBinding reads the binding of a federation tuple.
func (s *Store) LookupBinding(ctx context.Context, t BindingTuple) (BindingInfo, error) {
	if err := ValidateTuple(t); err != nil {
		return BindingInfo{}, err
	}
	var (
		info BindingInfo
		uid  *string
	)
	err := s.pool.QueryRow(ctx, `SELECT b.id, b.workload_principal_id, w.name, b.environment, b.service_account_uid, b.disabled_at IS NOT NULL
FROM `+s.q+`.workload_bindings b JOIN `+s.q+`.workloads w ON w.principal_id = b.workload_principal_id
WHERE b.trust_domain = $1 AND b.issuer = $2 AND b.namespace = $3 AND b.service_account_name = $4`,
		t.TrustDomain, t.Issuer, t.Namespace, t.ServiceAccountName).
		Scan(&info.ID, &info.WorkloadID, &info.Workload, &info.Environment, &uid, &info.Disabled)
	if errors.Is(err, pgx.ErrNoRows) {
		return BindingInfo{}, ErrBindingNotFound
	}
	if err != nil {
		s.logger.ErrorContext(ctx, "workload: read failure", logging.ErrorAttr(err))
		return BindingInfo{}, fmt.Errorf("%w: read: %w", ErrStore, err)
	}
	if uid != nil {
		info.Pinned, info.pin = true, *uid
	}
	return info, nil
}

// RebindBinding sets the ServiceAccount UID pin of the binding of tuple t to
// uid. The UPDATE and its audit row commit in one transaction (the table's
// commit guard refuses an UPDATE without the row). It never changes the
// binding's enabled state. The same pin is a no-op. The caller must take uid
// from the cluster: a recreated ServiceAccount of the same name is a new
// identity, so this is a trust change.
func (s *Store) RebindBinding(ctx context.Context, t BindingTuple, uid string, a Actor) (BindingInfo, bool, error) {
	if err := ValidateBinding(BindingSpec{Environment: "x", TrustDomain: t.TrustDomain, Issuer: t.Issuer,
		Namespace: t.Namespace, ServiceAccountName: t.ServiceAccountName, ServiceAccountUID: uid}); err != nil || uid == "" {
		s.record(ctx, verbRebindBinding, ErrBindingInvalid)
		return BindingInfo{}, false, ErrBindingInvalid
	}
	info, err := s.LookupBinding(ctx, t)
	if err != nil {
		s.record(ctx, verbRebindBinding, err)
		return BindingInfo{}, false, err
	}
	changed := !info.SamePin(uid)
	ev := s.auditFor("auth.workload.binding_rebound", "", info.WorkloadID, a, []string{"binding", "service_account_uid"}, "", "")
	ev.Attributes["binding_id"] = info.ID.String()
	ev.Attributes["binding_disabled"] = fmt.Sprintf("%t", info.Disabled)
	err = s.run(ctx, verbRebindBinding, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			var (
				pin      *string
				disabled bool
			)
			err := tx.QueryRow(ctx, `SELECT service_account_uid, disabled_at IS NOT NULL FROM `+s.q+`.workload_bindings WHERE id = $1 FOR UPDATE`,
				info.ID).Scan(&pin, &disabled)
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrBindingNotFound
			}
			if err != nil {
				return err
			}
			if pin != nil && *pin == uid {
				return nil
			}
			if disabled != info.Disabled || (pin != nil) != info.Pinned || (pin != nil && *pin != info.pin) {
				return ErrRebindStateChanged
			}
			_, err = tx.Exec(ctx, `UPDATE `+s.q+`.workload_bindings SET service_account_uid = $2, updated_at = clock_timestamp() WHERE id = $1`,
				info.ID, uid)
			return err
		},
		Audit:     ev,
		OffStream: audit.OffStreamWorkloadRegistry,
	})
	if err != nil {
		return BindingInfo{}, false, err
	}
	return info, changed, nil
}
