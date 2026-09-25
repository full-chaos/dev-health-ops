package schedulerservice

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
)

// CHAOS-6771: every domain-role readiness query must run on the readiness pool,
// never on the work pool. The work pool here is CLOSED (any acquire fails at
// once) and the readiness pool dials a listener that accepts and never answers,
// so a check that went to the readiness pool runs to its deadline while one that
// went to the work pool returns immediately.
func TestDomainReadinessChecksRunOnTheReadinessPoolNotTheWorkPool(t *testing.T) {
	silent, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = silent.Close() })
	go func() {
		for {
			connection, err := silent.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { _ = connection.Close() })
		}
	}()
	uri := "postgres://role:secret@" + silent.Addr().String() + "/db?connect_timeout=5"
	newPools := func() *postgres.RuntimePools {
		work, err := pgxpool.New(context.Background(), uri)
		if err != nil {
			t.Fatal(err)
		}
		work.Close()
		probe, err := pgxpool.New(context.Background(), uri)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(probe.Close)
		return &postgres.RuntimePools{Domain: work, DomainProbe: probe}
	}
	for name, run := range map[string]func(*postgresSchedulerDatabase, context.Context) error{
		"DomainReady": func(d *postgresSchedulerDatabase, ctx context.Context) error { return d.DomainReady(ctx) },
		"DomainPostureCheck": func(d *postgresSchedulerDatabase, ctx context.Context) error {
			return d.DomainPostureCheck(nil)(ctx)
		},
		"DomainTransactionReady": func(d *postgresSchedulerDatabase, ctx context.Context) error { return d.DomainTransactionReady(ctx) },
		"PostureManifestLockstep": func(d *postgresSchedulerDatabase, ctx context.Context) error {
			_, err := d.PostureManifestLockstep(ctx, "digest")
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			database := &postgresSchedulerDatabase{pools: newPools(), domainRole: "devhealth_domain", riverSchema: "river"}
			const budget = 600 * time.Millisecond
			ctx, cancel := context.WithTimeout(context.Background(), budget)
			defer cancel()
			started := time.Now()
			err := run(database, ctx)
			if err == nil {
				t.Fatal("a check against a database that never answers passed")
			}
			if elapsed := time.Since(started); elapsed < budget-100*time.Millisecond {
				t.Fatalf("%s returned after %s, before its %s deadline: it ran on the closed work pool", name, elapsed, budget)
			}
		})
	}
}
