//go:build integration

package postgres_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

const (
	poolAcquireDomainPass = "pool_acquire_domain_password"
	poolAcquireQueuePass  = "pool_acquire_queue_password"
)

// TestRuntimePoolsObserveRealAcquireLatency is CHAOS-3118 evidence for
// worker_database_pool_acquire_seconds: it opens real domain and
// queue-control pools against an isolated PostgreSQL instance exactly the way
// cmd/dev-health-worker does (postgres.NewRuntimePools followed by
// AttachPoolAcquireObserver, since pgxpool freezes its AcquireTracer before
// the process's MetricsCollector exists), issues real queries that force real
// Acquire calls, and reads the collector's own Prometheus exposition to prove
// non-zero series for both pools.
func TestRuntimePoolsObserveRealAcquireLatency(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer closePostgresInstance(t, instance)

	// CREATE ROLE is cluster-scoped, not database-scoped -- a scratch
	// database does not isolate it (CHAOS-4661). Deriving the role name from
	// this call's own database identity (worker_test on the container path,
	// a unique scratch database on the kiac remote path) is what makes two
	// successive runs, and two concurrent lanes, collision-free. Likewise
	// "worker_test" as a literal DATABASE target only resolves on the
	// container path; dbName is the database this call actually created.
	dbName, err := containers.DatabaseName(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	poolAcquireDomainRole, err := containers.RoleName("pool_acquire_domain", instance)
	if err != nil {
		t.Fatal(err)
	}
	poolAcquireQueueRole, err := containers.RoleName("pool_acquire_queue", instance)
	if err != nil {
		t.Fatal(err)
	}

	admin := openPostgresPool(t, ctx, instance.URI)
	defer admin.Close()
	defer containers.DropRole(admin, poolAcquireDomainRole, t.Logf)
	defer containers.DropRole(admin, poolAcquireQueueRole, t.Logf)
	for _, statement := range []string{
		"CREATE ROLE " + poolAcquireDomainRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + poolAcquireDomainPass + "'",
		"CREATE ROLE " + poolAcquireQueueRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + poolAcquireQueuePass + "'",
		"GRANT CONNECT ON DATABASE " + dbName + " TO " + poolAcquireDomainRole + ", " + poolAcquireQueueRole,
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	domainURI := postgresRoleURI(t, instance.URI, poolAcquireDomainRole, poolAcquireDomainPass)
	queueURI := postgresRoleURI(t, instance.URI, poolAcquireQueueRole, poolAcquireQueuePass)

	pools, err := postgresstore.NewRuntimePools(ctx, postgresstore.RuntimeConfig{
		DomainURI:        domainURI,
		QueueControlURI:  queueURI,
		DomainRole:       poolAcquireDomainRole,
		QueueRole:        poolAcquireQueueRole,
		RiverSchema:      "river",
		QueueControlMode: config.QueueControlDirect,
		DomainMaxConns:   2,
		QueueMaxConns:    2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pools.Close()

	collector, err := jobruntime.NewMetricsCollector(jobruntime.MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	pools.AttachPoolAcquireObserver(collector)

	for i := 0; i < 3; i++ {
		var value int
		if err := pools.Domain.QueryRow(ctx, "SELECT 1").Scan(&value); err != nil {
			t.Fatalf("domain query: %v", err)
		}
		if err := pools.QueueControl.QueryRow(ctx, "SELECT 1").Scan(&value); err != nil {
			t.Fatalf("queue-control query: %v", err)
		}
	}

	text := collector.PrometheusText()
	for _, pool := range []string{"domain", "queue_control"} {
		marker := `worker_database_pool_acquire_seconds_count{pool="` + pool + `",result="acquired"} 3`
		if !strings.Contains(text, marker) {
			t.Fatalf("expected %q in real exposition, got:\n%s", marker, text)
		}
	}
	// Every other bounded result must stay at zero: nothing here times out,
	// gets cancelled, or errors.
	for _, pool := range []string{"domain", "queue_control"} {
		for _, result := range []string{"timeout", "cancelled", "error"} {
			marker := `worker_database_pool_acquire_seconds_count{pool="` + pool + `",result="` + result + `"} 0`
			if !strings.Contains(text, marker) {
				t.Fatalf("expected %q to stay zero, got:\n%s", marker, text)
			}
		}
	}
}
