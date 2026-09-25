package prove

import (
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	pgstorage "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakepg"
)

// The pool this command opens returns pgx's own connect error, which carries the
// effective login (and, from a server that echoes, the password): run() applies
// the boundary built from the resolved configuration to it, so a login and
// password that only PGUSER and PGPASSWORD supply do not reach the output. A
// boundary built from the URI alone left both (CHAOS-6665).
func TestOpenPostgresPoolErrorIsRedactedForEnvironmentCredentials(t *testing.T) {
	refusing := fakepg.StartRefusing(t)
	_, err := openPostgresPool(context.Background(), refusing.URI)
	if err == nil {
		t.Fatal("the refusing server let the pool open")
	}
	if len(refusing.Leaks(err.Error())) != 2 {
		t.Fatalf("the driver error does not carry the environment's credentials (the repro is void): %v", err)
	}
	if leaks := refusing.Leaks(secrets.NewBoundary(refusing.URI).Redact(err).Error()); len(leaks) == 0 {
		t.Error("the URI-only boundary already redacts them: the defect this pins is gone")
	}
	if leaks := refusing.Leaks(pgstorage.Boundary(refusing.URI).Redact(err).Error()); len(leaks) > 0 {
		t.Errorf("the resolved-configuration boundary left %v in %v", leaks, err)
	}
}
