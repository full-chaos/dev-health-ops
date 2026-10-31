package restorefence

import "github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"

func lockoutFires(active int64, owned, inUse bool) bool { return false }

func platformAdminLockoutSQL(schema authschema.ValidatedIdentifier) string { return "" }
