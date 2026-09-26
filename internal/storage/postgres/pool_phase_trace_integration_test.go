//go:build integration

package postgres

import (
	"context"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/dbphase"
)

// CHAOS-6936. A failed reconciler stage must be able to say whether its budget
// went to waiting for a pool connection or to a statement. That needs the
// tracers on EVERY pool a stage can use, coordinator included (it carried no
// tracer before: the acquire metric's pool label is bounded to
// domain|queue_control, so it was left uninstrumented). This drives the
// production wiring (NewRuntimePools) and reads each pool's own phases back.
func TestEveryRuntimePoolFeedsTheCallersPhaseTrace(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	_, uri, roles := startGrantHarness(t, ctx)
	roleURI := func(role, password string) string {
		parsed, err := url.Parse(uri)
		if err != nil {
			t.Fatal(err)
		}
		parsed.User = url.UserPassword(role, password)
		return parsed.String()
	}
	// The coordinator login is the harness's own superuser: only its identity
	// and separation from the other two are validated here, not its grants.
	adminURI, err := url.Parse(uri)
	if err != nil {
		t.Fatal(err)
	}
	coordinatorRole := adminURI.User.Username()
	pools, err := NewRuntimePools(ctx, RuntimeConfig{
		DomainURI:           roleURI(roles.domain, grantDomainPass),
		QueueControlURI:     roleURI(roles.queue, grantQueuePass),
		CoordinatorURI:      uri,
		DomainRole:          roles.domain,
		QueueRole:           roles.queue,
		CoordinatorRole:     coordinatorRole,
		RiverSchema:         grantSchema,
		QueueControlMode:    config.QueueControlDirect,
		CoordinatorMode:     config.QueueControlDirect,
		DomainMaxConns:      2,
		QueueMaxConns:       2,
		CoordinatorMaxConns: 2,
		RequireCoordinator:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pools.Close)

	for _, tc := range []struct {
		pool string
		run  func(context.Context) error
	}{
		{"domain", func(c context.Context) error {
			_, err := pools.Domain.Exec(c, "-- named domain probe\nSELECT 1")
			return err
		}},
		{"queue_control", func(c context.Context) error {
			_, err := pools.QueueControl.Exec(c, "-- named queue probe\nSELECT 1")
			return err
		}},
		{"coordinator", func(c context.Context) error {
			_, err := pools.Coordinator.Exec(c, "-- named coordinator probe\nSELECT 1")
			return err
		}},
	} {
		t.Run(tc.pool, func(t *testing.T) {
			traced, trace := dbphase.With(ctx)
			if err := tc.run(traced); err != nil {
				t.Fatal(err)
			}
			phases := trace.Phases()
			if len(phases) != 2 || phases[0].Kind != dbphase.KindAcquire || phases[0].Name != tc.pool ||
				phases[1].Kind != dbphase.KindStatement || !strings.HasPrefix(phases[1].Name, "named ") {
				t.Fatalf("phases for the %s pool = %+v, want [acquire:%[1]s, statement:named ...]", tc.pool, phases)
			}
			for _, phase := range phases {
				if !phase.Done || phase.Err != nil {
					t.Fatalf("phase %+v did not finish cleanly", phase)
				}
			}
		})
	}

	// BEGIN and COMMIT are statements to pgx: a transaction's phases are named
	// with no stage code involved.
	traced, trace := dbphase.With(ctx)
	tx, err := pools.Coordinator.Begin(traced)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(traced); err != nil {
		t.Fatal(err)
	}
	if summary := trace.Summary(); !strings.Contains(summary, "acquire:coordinator=") ||
		!strings.Contains(summary, "statement:begin=") || !strings.Contains(summary, "statement:commit=") {
		t.Fatalf("transaction summary = %q, want acquire, begin and commit phases", summary)
	}

	// A context with no trace records nothing and costs nothing.
	if _, err := pools.Domain.Exec(ctx, "SELECT 1"); err != nil {
		t.Fatal(err)
	}
}
