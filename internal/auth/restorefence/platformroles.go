package restorefence

import (
	"context"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

const platformRolesSection = "auth.platform_role_assignments"

// lockoutFires: no active platform_admin is a lockout only when platform roles are owned or in use; an empty table with the
// switch off (public is_superuser is the authority) is not.
func lockoutFires(active int64, owned, inUse bool) bool {
	return active == 0 && (owned || inUse)
}

func platformRolesCountSQL(schema authschema.ValidatedIdentifier) string {
	return `SELECT count(*) FROM ` + authschema.Quote(schema) + `.platform_role_assignments`
}

// platformAdminLockoutSQL reads, in one statement (one clock), the active platform_admin count under the store's rule and
// the ids of the platform_admin assignments that are not active. $1 is the role key.
func platformAdminLockoutSQL(schema authschema.ValidatedIdentifier) string {
	q := authschema.Quote(schema)
	active := rule.ActiveRule("r", "p")
	return `SELECT count(*) FILTER (WHERE ` + active + `), coalesce(array_agg(r.id::text ORDER BY r.id) FILTER (WHERE NOT ` + active + `), '{}'::text[]) ` +
		`FROM ` + q + `.platform_role_assignments r JOIN ` + q + `.principals p ON p.id = r.principal_id WHERE r.role_key = $1`
}

func manifestRowCount(m *Manifest, name string) int {
	if m == nil {
		return 0
	}
	for _, s := range m.Sections {
		if s.Name == name {
			return len(s.Rows)
		}
	}
	return 0
}

// platformAdminLockout runs inside the apply transaction after every section, so the revokes and the deletion of the
// platform role section are already in its snapshot. It writes nothing.
func platformAdminLockout(ctx context.Context, tx audit.TxOps, schema authschema.ValidatedIdentifier, owned, inUse bool) (OperatorAction, bool, error) {
	var active int64
	var inactive []string
	if err := tx.QueryRow(ctx, platformAdminLockoutSQL(schema), rule.PlatformAdmin).Scan(&active, &inactive); err != nil {
		return OperatorAction{}, false, err
	}
	if !lockoutFires(active, owned, inUse) {
		return OperatorAction{}, false, nil
	}
	if inactive == nil {
		inactive = []string{}
	}
	return OperatorAction{Kind: ActionNoActivePlatformAdmin, Source: platformRolesSection, IDs: inactive}, true, nil
}
