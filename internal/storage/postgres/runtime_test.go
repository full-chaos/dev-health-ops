package postgres

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	platformconfig "github.com/full-chaos/dev-health-ops/internal/platform/config"
)

func TestRuntimeConfigRequiresSeparatedSessionSafePools(t *testing.T) {
	t.Parallel()

	valid := DefaultRuntimeConfig(
		"postgres://domain_role:domain-secret@pgbouncer.internal/app",
		"postgres://queue_role:queue-secret@postgres.internal/app",
		"domain_role",
		"queue_role",
	)
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid config failed: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*RuntimeConfig)
		want   error
	}{
		{name: "domain missing", mutate: func(c *RuntimeConfig) { c.DomainURI = "" }, want: ErrDomainDatabaseRequired},
		{name: "queue missing", mutate: func(c *RuntimeConfig) { c.QueueControlURI = "" }, want: ErrQueueControlRequired},
		{name: "transaction queue", mutate: func(c *RuntimeConfig) { c.QueueControlMode = platformconfig.QueueControlTransaction }, want: ErrQueueControlTransactionMode},
		{name: "session queue", mutate: func(c *RuntimeConfig) { c.QueueControlMode = platformconfig.QueueControlSession }, want: nil},
		{name: "shared configured role", mutate: func(c *RuntimeConfig) { c.QueueRole = "domain_role" }, want: ErrRuntimeRolesNotSeparated},
		{name: "domain DSN role mismatch", mutate: func(c *RuntimeConfig) { c.DomainRole = "other_role" }, want: ErrRuntimeRoleConfiguration},
		{name: "queue DSN role mismatch", mutate: func(c *RuntimeConfig) { c.QueueControlURI = "postgres://other_role:other@postgres.internal/app" }, want: ErrRuntimeRoleConfiguration},
		{name: "invalid role name", mutate: func(c *RuntimeConfig) { c.QueueRole = "Queue-Bad" }, want: ErrRuntimeRoleConfiguration},
		{name: "same transaction endpoint", mutate: func(c *RuntimeConfig) {
			c.DomainTransactionPooler = true
			c.QueueControlURI = "postgres://queue_role:other@pgbouncer.internal/app"
		}, want: ErrQueueControlTransactionMode},
		{name: "queue over budget", mutate: func(c *RuntimeConfig) { c.QueueMaxConns = 5 }, want: ErrInvalidConfig},
		{name: "domain over budget", mutate: func(c *RuntimeConfig) { c.DomainMaxConns = 17 }, want: ErrInvalidConfig},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			candidate := valid
			test.mutate(&candidate)
			if err := candidate.Validate(); !errors.Is(err, test.want) {
				t.Fatalf("Validate() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestRuntimeConfigSafeSurfaceContainsOnlyBudgets(t *testing.T) {
	t.Parallel()

	const domainSecret = "domain-never-log"
	const queueSecret = "queue-never-log"
	cfg := DefaultRuntimeConfig(
		"postgres://domain_role:"+domainSecret+"@pgbouncer.internal/app",
		"postgres://queue_role:"+queueSecret+"@postgres.internal/app",
		"domain_role",
		"queue_role",
	)
	surface := fmt.Sprint(cfg.SafeAttributes(), cfg.Validate())
	for _, forbidden := range []string{domainSecret, queueSecret, cfg.DomainURI, cfg.QueueControlURI} {
		if strings.Contains(surface, forbidden) {
			t.Fatalf("safe surface exposed %q: %s", forbidden, surface)
		}
	}
	for _, expected := range []string{"domain_database_role:domain_role", "queue_database_role:queue_role", "river_database_schema:river", "domain_max_connections:4", "domain_max_headroom:12", "queue_control_max_connections:2", "queue_control_max_headroom:2", "total_max_connections:6"} {
		if !strings.Contains(surface, expected) {
			t.Fatalf("safe surface missing %q: %s", expected, surface)
		}
	}
}

// validCoordinatorConfig is the opted-in three-pool shape: a coordinator DSN on
// its own direct endpoint, distinct from both the PgBouncer domain endpoint and
// the direct queue endpoint.
func validCoordinatorConfig() RuntimeConfig {
	cfg := DefaultRuntimeConfig(
		"postgres://domain_role:domain-secret@pgbouncer.internal/app",
		"postgres://queue_role:queue-secret@postgres.internal/app",
		"domain_role",
		"queue_role",
	).WithCoordinator()
	cfg.CoordinatorURI = "postgres://coordinator_role:coordinator-secret@postgres.internal/app"
	cfg.CoordinatorRole = "coordinator_role"
	cfg.CoordinatorMaxConns = 2
	return cfg
}

// The coordinator boundary must be validated with the same strictness the other
// two already get. Each new sentinel is reachable here, because a validation
// rule nothing can trigger is indistinguishable from an absent one.
func TestRuntimeConfigValidatesTheCoordinatorBoundaryWhenRequired(t *testing.T) {
	t.Parallel()

	if err := validCoordinatorConfig().Validate(); err != nil {
		t.Fatalf("valid three-pool config failed: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*RuntimeConfig)
		want   error
	}{
		{
			name:   "coordinator DSN missing",
			mutate: func(c *RuntimeConfig) { c.CoordinatorURI = "" },
			want:   ErrCoordinatorDatabaseRequired,
		},
		{
			name:   "coordinator role name invalid",
			mutate: func(c *RuntimeConfig) { c.CoordinatorRole = "Coordinator-Bad" },
			want:   ErrRuntimeRoleConfiguration,
		},
		{
			name:   "coordinator role empty",
			mutate: func(c *RuntimeConfig) { c.CoordinatorRole = "" },
			want:   ErrRuntimeRoleConfiguration,
		},
		{
			// The DSN's user is what PostgreSQL actually authenticates as, so a
			// declared role that disagrees with it means readiness would check a
			// different identity than the queries run as.
			name:   "coordinator DSN user disagrees with declared role",
			mutate: func(c *RuntimeConfig) { c.CoordinatorRole = "other_coordinator" },
			want:   ErrRuntimeRoleConfiguration,
		},
		{
			name:   "coordinator role collides with domain",
			mutate: func(c *RuntimeConfig) { c.CoordinatorRole = "domain_role" },
			want:   ErrRuntimeRolesNotSeparated,
		},
		{
			name:   "coordinator role collides with queue",
			mutate: func(c *RuntimeConfig) { c.CoordinatorRole = "queue_role" },
			want:   ErrRuntimeRolesNotSeparated,
		},
		{
			// Declared names distinct, but both DSNs authenticate as the same
			// login: the collision the name check above cannot see.
			name: "coordinator DSN user collides with the domain DSN user",
			mutate: func(c *RuntimeConfig) {
				c.CoordinatorRole = "domain_role"
				c.CoordinatorURI = "postgres://domain_role:other@postgres.internal/app"
			},
			want: ErrRuntimeRolesNotSeparated,
		},
		{
			name:   "coordinator below budget",
			mutate: func(c *RuntimeConfig) { c.CoordinatorMaxConns = 0 },
			want:   ErrInvalidConfig,
		},
		{
			// 4 is deploymentcontract's ceiling for a direct, server-counted
			// coordinator connection, not the domain role's PgBouncer-pooled 16.
			name:   "coordinator over budget",
			mutate: func(c *RuntimeConfig) { c.CoordinatorMaxConns = 5 },
			want:   ErrInvalidConfig,
		},
		{
			name:   "coordinator DSN unparseable",
			mutate: func(c *RuntimeConfig) { c.CoordinatorURI = "postgres://coordinator_role:x@:notaport/app" },
			want:   ErrInvalidConfig,
		},
		{
			name:   "coordinator uses session endpoint",
			mutate: func(c *RuntimeConfig) { c.CoordinatorMode = platformconfig.QueueControlSession },
			want:   nil,
		},
		{
			name:   "coordinator transaction mode",
			mutate: func(c *RuntimeConfig) { c.CoordinatorMode = platformconfig.QueueControlTransaction },
			want:   ErrCoordinatorTransactionMode,
		},
		{
			// The coordinator holds SHARE ROW EXCLUSIVE table locks and FOR
			// UPDATE row locks across statements in one transaction, so it must
			// not be pointed at the transaction-mode pooler endpoint.
			name: "coordinator shares the transaction-pooler endpoint",
			mutate: func(c *RuntimeConfig) {
				c.DomainTransactionPooler = true
				c.CoordinatorURI = "postgres://coordinator_role:other@pgbouncer.internal/app"
			},
			want: ErrCoordinatorTransactionMode,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			candidate := validCoordinatorConfig()
			test.mutate(&candidate)
			if err := candidate.Validate(); !errors.Is(err, test.want) {
				t.Fatalf("Validate() error = %v, want %v", err, test.want)
			}
		})
	}
}

// A domain-only process must not be forced to configure a coordinator DSN, or
// dev-health-worker and dev-health-stream-runner could not start. Every
// coordinator rule above must therefore be inert when RequireCoordinator is
// false -- including rules that would otherwise fire on the zero value.
func TestRuntimeConfigIgnoresTheCoordinatorBoundaryWhenNotRequired(t *testing.T) {
	t.Parallel()

	cfg := DefaultRuntimeConfig(
		"postgres://domain_role:domain-secret@pgbouncer.internal/app",
		"postgres://queue_role:queue-secret@postgres.internal/app",
		"domain_role",
		"queue_role",
	)
	if cfg.RequireCoordinator {
		t.Fatal("DefaultRuntimeConfig must not opt into the coordinator boundary")
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("domain-only config rejected with no coordinator DSN: %v", err)
	}

	// Values that WOULD be rejected if the boundary were required: an unset
	// DSN, an empty role, and a zero connection budget.
	for _, mutate := range []func(*RuntimeConfig){
		func(c *RuntimeConfig) { c.CoordinatorURI = "" },
		func(c *RuntimeConfig) { c.CoordinatorRole = "" },
		func(c *RuntimeConfig) { c.CoordinatorMaxConns = 0 },
		func(c *RuntimeConfig) { c.CoordinatorRole = "domain_role" },
		func(c *RuntimeConfig) { c.CoordinatorMaxConns = 99 },
	} {
		candidate := cfg
		mutate(&candidate)
		if err := candidate.Validate(); err != nil {
			t.Fatalf("coordinator rule fired for a domain-only process: %v", err)
		}
	}

	// And the pool itself must be absent rather than aliased to the domain pool.
	pools := &RuntimePools{}
	if _, err := pools.CoordinatorPool(); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("CoordinatorPool() error = %v, want ErrUnavailable", err)
	}
}

func TestRuntimeConfigSafeSurfaceReportsTheCoordinatorBudgetWithoutItsSecret(t *testing.T) {
	t.Parallel()

	const coordinatorSecret = "coordinator-never-log"
	cfg := validCoordinatorConfig()
	cfg.CoordinatorURI = "postgres://coordinator_role:" + coordinatorSecret + "@postgres.internal/app"

	surface := fmt.Sprint(cfg.SafeAttributes(), cfg.Validate())
	if strings.Contains(surface, coordinatorSecret) || strings.Contains(surface, cfg.CoordinatorURI) {
		t.Fatalf("safe surface exposed the coordinator DSN: %s", surface)
	}
	for _, expected := range []string{
		"coordinator_required:true",
		"coordinator_configured:true",
		"coordinator_database_role:coordinator_role",
		"coordinator_max_connections:2",
		"coordinator_max_headroom:2",
		// 4 domain + 2 queue + 2 coordinator.
		"total_max_connections:8",
	} {
		if !strings.Contains(surface, expected) {
			t.Fatalf("safe surface missing %q: %s", expected, surface)
		}
	}

	// A domain-only process reports neither a coordinator role nor a
	// coordinator budget, so its logs cannot suggest a pool it does not have.
	domainOnly := DefaultRuntimeConfig(
		"postgres://domain_role:x@pgbouncer.internal/app",
		"postgres://queue_role:y@postgres.internal/app",
		"domain_role", "queue_role",
	)
	domainOnlySurface := fmt.Sprint(domainOnly.SafeAttributes())
	for _, forbidden := range []string{"coordinator_database_role", "coordinator_max_connections"} {
		if strings.Contains(domainOnlySurface, forbidden) {
			t.Fatalf("domain-only safe surface reported %q: %s", forbidden, domainOnlySurface)
		}
	}
	if !strings.Contains(domainOnlySurface, "total_max_connections:6") {
		t.Fatalf("domain-only total must exclude the coordinator: %s", domainOnlySurface)
	}
}
