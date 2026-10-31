package usersbackfill

import (
	"context"
	"errors"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// PlatformAdminTarget answers whether no principal holds platform_admin now.
type PlatformAdminTarget interface {
	NoActivePlatformAdmin(ctx context.Context) (bool, error)
}

// ReconcilePlatformAdmin adds the no_active_platform_admin class to rep. With
// gate it counts in GatedTotal; without it the class is printed only. A
// failure names no value.
func ReconcilePlatformAdmin(ctx context.Context, tgt PlatformAdminTarget, gate bool, rep *Reconciliation) error {
	none, err := tgt.NoActivePlatformAdmin(ctx)
	if err != nil {
		return errors.New("usersbackfill: reading the platform admins failed")
	}
	rep.PlatformRolesGate = gate
	rep.platformAdminRead = true
	rep.NoActivePlatformAdmin = 0
	if none {
		rep.NoActivePlatformAdmin = 1
	}
	return nil
}

// NoActivePlatformAdmin is true when no principal holds platform_admin by the
// platform role store's rule, read inside the snapshot transaction.
func (t TxTarget) NoActivePlatformAdmin(ctx context.Context) (bool, error) {
	q := authschema.Quote(t.Schema)
	var none bool
	err := t.Tx.QueryRow(ctx, `SELECT NOT EXISTS (SELECT 1 FROM `+q+`.principals p WHERE `+
		rule.HoldsSQL(q, "p.id", "$1")+`)`, identity.PlatformAdminRole).Scan(&none)
	return none, err
}
