package prove

import (
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	pgstorage "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakepg"
)

// The pool this command opens returns pgx's own connect error, which carries the
// effective login (and, from a server that echoes, the password): run() applies the
// boundary built from the resolved configuration to it. Over every form of the
// connection string no login or password the driver resolved survives that boundary,
// and a boundary built from the DSN alone (the defect this pins) leaves some.
func TestOpenPostgresPoolErrorIsRedactedOverEveryConnectionForm(t *testing.T) {
	refusing := fakepg.StartRefusing(t)
	dsnOnlyLeaked := false
	refusing.RunGrid(t, true, func(t *testing.T, dsn string) string {
		_, err := openPostgresPool(context.Background(), dsn)
		if err == nil {
			return ""
		}
		if len(refusing.Leaks(err.Error())) > 0 && len(refusing.Leaks(secrets.NewBoundary(dsn).Redact(err).Error())) > 0 {
			dsnOnlyLeaked = true
		}
		return pgstorage.Boundary(dsn).Redact(err).Error()
	})
	if !dsnOnlyLeaked {
		t.Error("no form left a credential with a DSN-only boundary: the grid no longer reproduces the defect")
	}
}
